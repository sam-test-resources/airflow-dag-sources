"""
nnn_capacity_utilisation_hourly
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Owner:      nnn-data-engineering
Domain:     Network
Schedule:   :30 of every hour (30 * * * *)
SLA:        45 minutes

Consumes CVC (Connectivity Virtual Circuit) throughput events from the
nnn.network.cvc.metrics Kafka topic, calculates utilisation percentage
against contracted CVC capacity per POI, and writes the result to
Snowflake NETWORK.CVC_UTILISATION_HOURLY.

A Snowflake alert (separate from this DAG) pages on-call when any POI
sustains >80% utilisation for 3 consecutive hours.

Steps:
  1. Consume  – read Kafka topic for the previous hour window
  2. Enrich   – join with NETWORK.CVC_CAPACITY_CONTRACTS for contracted limits
  3. Calculate – utilisation %, headroom Gbps, congestion flag (>80%)
  4. Load      – append to Snowflake target (no dedup — hourly partitioned)
  5. Alert check – flag POIs exceeding 80% threshold into NETWORK.CVC_CONGESTION_ALERTS

Upstream: Kafka producer (network telemetry service)
Downstream: nnn_poi_traffic_aggregation_weekly, capacity planning dashboards
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_KAFKA, CONN_SNOWFLAKE, get_execution_date, get_snowflake_hook,
)

log = logging.getLogger(__name__)

default_args = {
    "owner":                    "nnn-data-engineering",
    "depends_on_past":          False,
    "email":                    ["de-alerts@nnnco.com.au"],
    "email_on_failure":         True,
    "email_on_retry":           False,
    "retries":                  2,
    "retry_delay":              timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay":          timedelta(minutes=15),
    "on_failure_callback":      nnn_failure_alert,
    "execution_timeout":        timedelta(minutes=30),
}


def consume_kafka_topic(**context) -> None:
    """Read the previous hour's CVC metrics from Kafka into XCom."""
    from confluent_kafka import Consumer, KafkaException
    from airflow.hooks.base import BaseHook

    exec_dt   = get_execution_date(context)
    hour_start = exec_dt.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    hour_end   = hour_start + timedelta(hours=1)

    creds  = BaseHook.get_connection(CONN_KAFKA)
    config = {
        "bootstrap.servers": creds.host,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms":   "PLAIN",
        "sasl.username":     creds.login,
        "sasl.password":     creds.password,
        "group.id":          f"airflow-cvc-etl-{exec_dt.strftime('%Y%m%d%H')}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    consumer = Consumer(config)
    consumer.subscribe(["nnn.network.cvc.metrics"])

    records, deadline = [], datetime.now(timezone.utc) + timedelta(minutes=10)
    while datetime.now(timezone.utc) < deadline:
        msg = consumer.poll(timeout=2.0)
        if msg is None:
            break
        if msg.error():
            raise KafkaException(msg.error())
        record = msg.value()   # bytes → dict via schema registry in prod
        ts     = record.get("timestamp_utc")
        if hour_start.isoformat() <= ts < hour_end.isoformat():
            records.append(record)

    consumer.close()
    log.info("Consumed %d CVC metric events for hour %s", len(records), hour_start)
    context["ti"].xcom_push(key="cvc_raw", value=records)


def enrich_and_calculate(**context) -> None:
    """Join with contracted CVC capacity and compute utilisation %."""
    records = context["ti"].xcom_pull(key="cvc_raw")
    if not records:
        log.warning("No CVC records consumed — skipping enrich step")
        context["ti"].xcom_push(key="cvc_enriched", value=[])
        return

    exec_dt    = get_execution_date(context)
    hour_label = (exec_dt.replace(minute=0, second=0) - timedelta(hours=1)).isoformat()

    hook = get_snowflake_hook()
    # Fetch contracted CVC limits per POI for the current period
    capacity_rows = hook.get_pandas_df("""
        SELECT poi_id, contracted_cvc_gbps
        FROM   NETWORK.CVC_CAPACITY_CONTRACTS
        WHERE  effective_from <= CURRENT_DATE()
          AND  (effective_to IS NULL OR effective_to > CURRENT_DATE())
    """)
    capacity = dict(zip(capacity_rows["poi_id"], capacity_rows["contracted_cvc_gbps"]))

    df = pd.DataFrame(records)
    df["contracted_gbps"]    = df["poi_id"].map(capacity)
    df["utilisation_pct"]    = (df["throughput_gbps"] / df["contracted_gbps"] * 100).round(2)
    df["headroom_gbps"]      = (df["contracted_gbps"] - df["throughput_gbps"]).round(3)
    df["congestion_flag"]    = df["utilisation_pct"] > 80
    df["hour_label"]         = hour_label

    context["ti"].xcom_push(key="cvc_enriched", value=df.to_dict(orient="records"))
    log.info("Enriched %d rows; %d POIs in congestion",
             len(df), df["congestion_flag"].sum())


def load_utilisation(**context) -> None:
    """Append hourly CVC utilisation rows to Snowflake."""
    rows = context["ti"].xcom_pull(key="cvc_enriched")
    if not rows:
        log.info("No rows to load — exiting cleanly")
        return

    hook = get_snowflake_hook()
    hook.insert_rows(
        table="NETWORK.CVC_UTILISATION_HOURLY",
        rows=[list(r.values()) for r in rows],
        target_fields=list(rows[0].keys()),
    )
    log.info("Loaded %d rows into NETWORK.CVC_UTILISATION_HOURLY", len(rows))


def flag_congestion_alerts(**context) -> None:
    """Write POIs exceeding 80% utilisation into the congestion alerts table."""
    rows = context["ti"].xcom_pull(key="cvc_enriched")
    if not rows:
        return

    congested = [r for r in rows if r.get("congestion_flag")]
    if not congested:
        log.info("No congestion events this hour")
        return

    hook = get_snowflake_hook()
    hook.insert_rows(
        table="NETWORK.CVC_CONGESTION_ALERTS",
        rows=[[r["poi_id"], r["hour_label"], r["utilisation_pct"], r["headroom_gbps"]]
              for r in congested],
        target_fields=["poi_id", "hour_label", "utilisation_pct", "headroom_gbps"],
    )
    log.warning("Flagged %d congested POIs into alert table", len(congested))


with DAG(
    dag_id="nnn_capacity_utilisation_hourly",
    description="Hourly CVC throughput vs contracted capacity → NETWORK.CVC_UTILISATION_HOURLY",
    schedule_interval="30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=2,
    default_args=default_args,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "network", "hourly"],
) as dag:

    t_consume = PythonOperator(
        task_id="consume_kafka_topic",
        python_callable=consume_kafka_topic,
        sla=timedelta(minutes=15),
    )

    t_enrich = PythonOperator(
        task_id="enrich_and_calculate",
        python_callable=enrich_and_calculate,
        sla=timedelta(minutes=25),
    )

    t_load = PythonOperator(
        task_id="load_utilisation",
        python_callable=load_utilisation,
        sla=timedelta(minutes=35),
    )

    t_alert = PythonOperator(
        task_id="flag_congestion_alerts",
        python_callable=flag_congestion_alerts,
    )

    t_consume >> t_enrich >> t_load >> t_alert
