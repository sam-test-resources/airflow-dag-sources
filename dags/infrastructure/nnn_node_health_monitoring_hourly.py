"""
nnn_node_health_monitoring_hourly
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Owner:      nnn-data-engineering / Network Operations
Domain:     Infrastructure
Schedule:   :05 of every hour (5 * * * *)
SLA:        30 minutes

Processes SNMP/NetFlow telemetry files deposited to S3 by the NNN network
collection agents, calculates per-node health scores, and loads into
Snowflake NETWORK.NODE_HEALTH_HOURLY.

Node types monitored:
  - FTTP Distribution Points (DP)
  - HFC Fibre Nodes (FN)
  - FTTN/FTTC Street Cabinets (cabinet)
  - Fixed Wireless Base Stations (BS)
  - POI routers and aggregation switches

Health score (0–100) components:
  - Availability %   (40 pts): uptime in the hour
  - Error rate       (30 pts): interface error % vs threshold
  - CPU utilisation  (20 pts): headroom vs 80% threshold
  - Memory utilisation (10 pts): headroom vs 85% threshold

Nodes with health score < 60 are flagged into NETWORK.NODE_HEALTH_ALERTS
for the NOC (Network Operations Centre) dashboard.

Steps:
  1. Sense new SNMP/NetFlow files deposited to S3 in the last hour
  2. Parse and aggregate telemetry per node
  3. Compute health scores
  4. Load to NETWORK.NODE_HEALTH_HOURLY
  5. Upsert alerts for unhealthy nodes
"""

from __future__ import annotations

import io
import json
import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_S3, NNN_S3_BUCKET,
    get_execution_date, get_snowflake_hook,
)

log = logging.getLogger(__name__)

HEALTH_THRESHOLDS = {
    "availability_pct":  {"warning": 99.0, "critical": 95.0},
    "error_rate_pct":    {"warning": 0.1,  "critical": 1.0},
    "cpu_utilisation_pct": {"warning": 80.0, "critical": 95.0},
    "mem_utilisation_pct": {"warning": 85.0, "critical": 95.0},
}

default_args = {
    "owner":                    "nnn-data-engineering",
    "depends_on_past":          False,
    "email":                    ["de-alerts@nnnco.com.au", "noc@nnnco.com.au"],
    "email_on_failure":         True,
    "email_on_retry":           False,
    "retries":                  2,
    "retry_delay":              timedelta(minutes=2),
    "retry_exponential_backoff": False,
    "on_failure_callback":      nnn_failure_alert,
    "execution_timeout":        timedelta(minutes=20),
}


def compute_s3_prefix(**context) -> str:
    """Return the S3 prefix for the current collection hour."""
    exec_dt  = get_execution_date(context)
    # Files land 5 minutes into the hour; target the PREVIOUS hour's files
    hour     = (exec_dt.replace(minute=0, second=0) - timedelta(hours=1)).strftime("%Y/%m/%d/%H")
    prefix   = f"network/telemetry/snmp/{hour}/"
    context["ti"].xcom_push(key="s3_prefix", value=prefix)
    context["ti"].xcom_push(key="hour_label", value=hour)
    return prefix


def parse_telemetry_files(**context) -> None:
    """Read all SNMP JSON files for the hour from S3 and aggregate per node."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    prefix = context["ti"].xcom_pull(key="s3_prefix")
    hook   = S3Hook(aws_conn_id=CONN_S3)

    keys = hook.list_keys(bucket_name=NNN_S3_BUCKET, prefix=prefix)
    if not keys:
        log.warning("No SNMP telemetry files found for prefix %s", prefix)
        context["ti"].xcom_push(key="node_rows", value=[])
        return

    records = []
    for key in keys:
        raw = hook.read_key(key, bucket_name=NNN_S3_BUCKET)
        try:
            batch = json.loads(raw)
            records.extend(batch if isinstance(batch, list) else [batch])
        except json.JSONDecodeError:
            log.error("Failed to parse S3 file: %s", key)

    log.info("Parsed %d SNMP records from %d files", len(records), len(keys))

    df = pd.DataFrame(records)
    df.columns = [c.lower() for c in df.columns]

    # Aggregate to one row per node for the hour
    agg = df.groupby("node_id").agg(
        node_type=("node_type", "first"),
        poi_id=("poi_id", "first"),
        state_territory=("state_territory", "first"),
        availability_pct=("is_up", lambda x: x.mean() * 100),
        error_rate_pct=("interface_errors", "mean"),
        cpu_utilisation_pct=("cpu_pct", "mean"),
        mem_utilisation_pct=("mem_pct", "mean"),
        sample_count=("node_id", "count"),
    ).reset_index().round(4)

    context["ti"].xcom_push(key="node_rows", value=agg.to_dict(orient="records"))


def compute_health_scores(**context) -> None:
    """Score each node 0–100 and flag unhealthy ones."""
    rows      = context["ti"].xcom_pull(key="node_rows")
    hour_label = context["ti"].xcom_pull(key="hour_label")
    if not rows:
        context["ti"].xcom_push(key="scored_rows", value=[])
        return

    def score(row):
        pts = 100.0
        # Availability (−40 pts at critical)
        avail = row.get("availability_pct", 100)
        if avail < HEALTH_THRESHOLDS["availability_pct"]["critical"]:   pts -= 40
        elif avail < HEALTH_THRESHOLDS["availability_pct"]["warning"]:  pts -= 20

        # Error rate (−30 pts at critical)
        err = row.get("error_rate_pct", 0)
        if err > HEALTH_THRESHOLDS["error_rate_pct"]["critical"]:       pts -= 30
        elif err > HEALTH_THRESHOLDS["error_rate_pct"]["warning"]:      pts -= 15

        # CPU (−20 pts at critical)
        cpu = row.get("cpu_utilisation_pct", 0)
        if cpu > HEALTH_THRESHOLDS["cpu_utilisation_pct"]["critical"]:  pts -= 20
        elif cpu > HEALTH_THRESHOLDS["cpu_utilisation_pct"]["warning"]: pts -= 10

        # Memory (−10 pts at critical)
        mem = row.get("mem_utilisation_pct", 0)
        if mem > HEALTH_THRESHOLDS["mem_utilisation_pct"]["critical"]:  pts -= 10
        elif mem > HEALTH_THRESHOLDS["mem_utilisation_pct"]["warning"]: pts -= 5

        return max(pts, 0)

    for row in rows:
        row["health_score"]  = score(row)
        row["health_status"] = (
            "CRITICAL" if row["health_score"] < 40 else
            "WARNING"  if row["health_score"] < 60 else
            "HEALTHY"
        )
        row["hour_label"] = hour_label

    unhealthy = sum(1 for r in rows if r["health_status"] != "HEALTHY")
    log.info("Health scored: %d nodes | %d unhealthy", len(rows), unhealthy)
    context["ti"].xcom_push(key="scored_rows", value=rows)


def load_node_health(**context) -> None:
    """Append scored node health rows to NETWORK.NODE_HEALTH_HOURLY."""
    rows = context["ti"].xcom_pull(key="scored_rows")
    if not rows:
        return

    hook = get_snowflake_hook()
    hook.insert_rows(
        table="NETWORK.NODE_HEALTH_HOURLY",
        rows=[list(r.values()) for r in rows],
        target_fields=list(rows[0].keys()),
    )
    log.info("Loaded %d node health rows", len(rows))


def upsert_alerts(**context) -> None:
    """Upsert CRITICAL/WARNING nodes into NETWORK.NODE_HEALTH_ALERTS."""
    rows = context["ti"].xcom_pull(key="scored_rows")
    if not rows:
        return

    alerts = [r for r in rows if r["health_status"] in ("CRITICAL", "WARNING")]
    if not alerts:
        log.info("No unhealthy nodes this hour")
        return

    hook = get_snowflake_hook()
    hook.run("CREATE OR REPLACE TEMPORARY TABLE tmp_node_alerts LIKE NETWORK.NODE_HEALTH_ALERTS")
    hook.insert_rows(
        table="tmp_node_alerts",
        rows=[[r["node_id"], r["hour_label"], r["health_status"],
               r["health_score"], r["node_type"], r["poi_id"]]
              for r in alerts],
        target_fields=["node_id", "hour_label", "health_status",
                       "health_score", "node_type", "poi_id"],
    )
    hook.run("""
        MERGE INTO NETWORK.NODE_HEALTH_ALERTS tgt
        USING tmp_node_alerts src ON (tgt.node_id=src.node_id AND tgt.hour_label=src.hour_label)
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    log.warning("Upserted %d node health alerts", len(alerts))


with DAG(
    dag_id="nnn_node_health_monitoring_hourly",
    description="Hourly SNMP/NetFlow node health scoring from S3 → NETWORK.NODE_HEALTH_HOURLY",
    schedule_interval="5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=2,
    default_args=default_args,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "infrastructure", "network", "hourly", "noc"],
) as dag:

    t_prefix = PythonOperator(task_id="compute_s3_prefix",    python_callable=compute_s3_prefix)
    t_parse  = PythonOperator(task_id="parse_telemetry_files", python_callable=parse_telemetry_files, sla=timedelta(minutes=10))
    t_score  = PythonOperator(task_id="compute_health_scores", python_callable=compute_health_scores, sla=timedelta(minutes=15))
    t_load   = PythonOperator(task_id="load_node_health",      python_callable=load_node_health,      sla=timedelta(minutes=20))
    t_alert  = PythonOperator(task_id="upsert_alerts",         python_callable=upsert_alerts,         sla=timedelta(minutes=25))

    t_prefix >> t_parse >> t_score >> t_load >> t_alert
