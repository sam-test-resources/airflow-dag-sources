"""
nnn_network_performance_daily
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Owner:      nnn-data-engineering
Domain:     Network
Schedule:   02:00 AEST daily
SLA:        4 hours (must complete before 06:00 AEST for morning ops review)

Pulls per-link utilisation, latency, and packet-loss metrics from the
Network Management System (NMS) REST API, aggregates them to daily
averages/P95 values per Point of Interconnect (POI) and virtual circuit,
and loads results into Snowflake NETWORK.LINK_PERFORMANCE_DAILY.

Steps:
  1. Sensor  – confirm NMS API is reachable
  2. Extract – page through NMS /metrics endpoint for previous day
  3. Transform – compute P95 utilisation, mean latency, packet-loss %
  4. Validate – reject run if record count < 500 (indicates partial pull)
  5. Load    – MERGE into Snowflake target
  6. Publish – update run metadata table for downstream DAGs

Upstream dependencies: none
Downstream consumers:  nnn_capacity_utilisation_hourly (reads same table)
                       nnn_poi_traffic_aggregation_weekly
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_NMS_API, CONN_SNOWFLAKE, get_run_date, assert_row_count,
)

log = logging.getLogger(__name__)

# ── DAG defaults ───────────────────────────────────────────────────────────────

default_args = {
    "owner":                    "nnn-data-engineering",
    "depends_on_past":          False,
    "email":                    ["de-alerts@nnnco.com.au"],
    "email_on_failure":         True,
    "email_on_retry":           False,
    "retries":                  3,
    "retry_delay":              timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":          timedelta(minutes=60),
    "on_failure_callback":      nnn_failure_alert,
    "execution_timeout":        timedelta(hours=2),
}

# ── Task callables ─────────────────────────────────────────────────────────────

def extract_nms_metrics(**context) -> None:
    """Page through NMS /metrics API and write raw JSONL to XCom staging."""
    from airflow.providers.http.hooks.http import HttpHook

    run_date = get_run_date(context)
    hook = HttpHook(method="GET", http_conn_id=CONN_NMS_API)

    records, page = [], 1
    while True:
        resp = hook.run(
            endpoint=f"/api/v2/metrics/link",
            data={"date": run_date, "page": page, "page_size": 500},
            headers={"Accept": "application/json"},
        )
        batch = resp.json().get("data", [])
        if not batch:
            break
        records.extend(batch)
        page += 1
        log.info("Extracted page %d  (total so far: %d records)", page, len(records))

    if len(records) < 500:
        raise ValueError(f"Unexpectedly low record count from NMS: {len(records)}")

    # Persist to XCom (small enough; large scale would use S3 staging)
    context["ti"].xcom_push(key="raw_records", value=records)
    log.info("NMS extract complete: %d records for %s", len(records), run_date)


def transform_metrics(**context) -> list[dict]:
    """Aggregate raw link metrics to daily P95 utilisation and mean latency."""
    records  = context["ti"].xcom_pull(key="raw_records")
    run_date = get_run_date(context)

    df = pd.DataFrame(records)

    # Compute per-link daily aggregates
    agg = (
        df.groupby(["poi_id", "virtual_circuit_id", "link_id"])
        .agg(
            utilisation_mean=("utilisation_pct", "mean"),
            utilisation_p95=("utilisation_pct", lambda x: np.percentile(x, 95)),
            latency_ms_mean=("latency_ms", "mean"),
            latency_ms_p95=("latency_ms", lambda x: np.percentile(x, 95)),
            packet_loss_pct=("packet_loss_pct", "mean"),
            sample_count=("utilisation_pct", "count"),
        )
        .reset_index()
    )
    agg["run_date"] = run_date
    agg = agg.round(4)

    rows = agg.to_dict(orient="records")
    context["ti"].xcom_push(key="transformed_rows", value=rows)
    log.info("Transformed %d link-day rows", len(rows))
    return rows


def load_to_snowflake(**context) -> None:
    """Batch MERGE transformed rows into NETWORK.LINK_PERFORMANCE_DAILY."""
    from nnn_common.utils import get_snowflake_hook

    rows     = context["ti"].xcom_pull(key="transformed_rows")
    run_date = get_run_date(context)
    hook     = get_snowflake_hook()

    # Stage → merge pattern
    hook.run("CREATE OR REPLACE TEMPORARY TABLE tmp_link_perf LIKE NETWORK.LINK_PERFORMANCE_DAILY")
    hook.insert_rows(
        table="tmp_link_perf",
        rows=[list(r.values()) for r in rows],
        target_fields=list(rows[0].keys()),
    )
    hook.run("""
        MERGE INTO NETWORK.LINK_PERFORMANCE_DAILY tgt
        USING tmp_link_perf src
          ON  tgt.poi_id            = src.poi_id
          AND tgt.virtual_circuit_id = src.virtual_circuit_id
          AND tgt.link_id           = src.link_id
          AND tgt.run_date          = src.run_date
        WHEN MATCHED    THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT   *
    """)
    log.info("MERGE complete for run_date=%s (%d rows)", run_date, len(rows))


def validate_load(**context) -> None:
    run_date = get_run_date(context)
    assert_row_count("NETWORK.LINK_PERFORMANCE_DAILY", run_date, min_rows=500)


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="nnn_network_performance_daily",
    description="Daily per-link NMS metric aggregation → NETWORK.LINK_PERFORMANCE_DAILY",
    schedule_interval="0 16 * * *",     # 02:00 AEST = 16:00 UTC prev day
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "network", "daily"],
) as dag:

    check_nms_api = HttpSensor(
        task_id="check_nms_api",
        http_conn_id=CONN_NMS_API,
        endpoint="/health",
        response_check=lambda resp: resp.json().get("status") == "ok",
        poke_interval=60,
        timeout=300,
        on_failure_callback=nnn_failure_alert,
    )

    with TaskGroup("extract") as tg_extract:
        extract = PythonOperator(
            task_id="extract_nms_metrics",
            python_callable=extract_nms_metrics,
            sla=timedelta(hours=1),
        )

    with TaskGroup("transform") as tg_transform:
        transform = PythonOperator(
            task_id="transform_metrics",
            python_callable=transform_metrics,
            sla=timedelta(hours=2),
        )

    with TaskGroup("load") as tg_load:
        load = PythonOperator(
            task_id="load_to_snowflake",
            python_callable=load_to_snowflake,
            sla=timedelta(hours=3),
        )
        validate = PythonOperator(
            task_id="validate_load",
            python_callable=validate_load,
        )
        load >> validate

    update_pipeline_log = SnowflakeOperator(
        task_id="update_pipeline_log",
        snowflake_conn_id=CONN_SNOWFLAKE,
        sql="""
            INSERT INTO OPERATIONS.PIPELINE_RUN_LOG
                (dag_id, run_date, status, completed_at)
            VALUES
                ('nnn_network_performance_daily', '{{ ds }}', 'SUCCESS', CURRENT_TIMESTAMP())
        """,
    )

    check_nms_api >> tg_extract >> tg_transform >> tg_load >> update_pipeline_log
