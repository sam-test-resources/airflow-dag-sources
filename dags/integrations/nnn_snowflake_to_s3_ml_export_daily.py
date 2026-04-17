"""
nnn_snowflake_to_s3_ml_export_daily
--------------------------------------
Owner:      nnn-data-engineering
Domain:     ML Platform / Feature Engineering
Schedule:   Daily at noon AEST (0 12 * * *)
SLA:        2 hours

Exports ML feature tables from Snowflake to the S3 data lake in Parquet format,
then triggers the ML platform training pipeline.  The three feature exports
(customer, network, churn labels) run in parallel to minimise wall-clock time.

Steps:
  1. export_customer_features — UNLOAD ML.CUSTOMER_FEATURES → S3 Parquet (parallel)
  2. export_network_features  — UNLOAD ML.NETWORK_FEATURES → S3 Parquet (parallel)
  3. export_churn_labels      — UNLOAD ML.CHURN_LABELS → S3 Parquet (parallel)
  4. trigger_ml_training      — POST to ML platform API to kick off training job
                                (waits for all 3 exports)

Upstream:   Morning ETL DAGs populating ML.* feature tables
Downstream: ML training platform (consumes S3 Parquet exports)
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_NMS_API,
    NNN_S3_BUCKET,
    get_run_date,
    snowflake_unload_to_s3,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner":                     "nnn-data-engineering",
    "depends_on_past":           False,
    "email":                     ["de-alerts@nnnco.com.au"],
    "email_on_failure":          True,
    "email_on_retry":            False,
    "retries":                   2,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":           timedelta(minutes=30),
    "on_failure_callback":       nnn_failure_alert,
    "execution_timeout":         timedelta(minutes=30),
}

ML_S3_BASE_PREFIX = "ml-exports"


# ---------------------------------------------------------------------------
# Task functions (module-level so the scheduler does not re-define them on
# every DAG parse cycle)
# ---------------------------------------------------------------------------

def export_customer_features(**context) -> None:
    """Unload ML.CUSTOMER_FEATURES for run_date to S3 in Parquet format."""
    run_date  = get_run_date(context)
    s3_prefix = f"{ML_S3_BASE_PREFIX}/customer_features/run_date={run_date}/"

    select_sql = f"""
        SELECT *
        FROM ML.CUSTOMER_FEATURES
        WHERE feature_date = '{run_date}'
    """
    s3_path = snowflake_unload_to_s3(
        select_sql=select_sql,
        s3_prefix=s3_prefix,
        file_format="PARQUET",
        overwrite=True,
    )
    log.info("Customer features unloaded to: %s", s3_path)
    context["ti"].xcom_push(key="customer_features_s3_path", value=s3_path)


def export_network_features(**context) -> None:
    """Unload ML.NETWORK_FEATURES for run_date to S3 in Parquet format."""
    run_date  = get_run_date(context)
    s3_prefix = f"{ML_S3_BASE_PREFIX}/network_features/run_date={run_date}/"

    select_sql = f"""
        SELECT *
        FROM ML.NETWORK_FEATURES
        WHERE feature_date = '{run_date}'
    """
    s3_path = snowflake_unload_to_s3(
        select_sql=select_sql,
        s3_prefix=s3_prefix,
        file_format="PARQUET",
        overwrite=True,
    )
    log.info("Network features unloaded to: %s", s3_path)
    context["ti"].xcom_push(key="network_features_s3_path", value=s3_path)


def export_churn_labels(**context) -> None:
    """Unload ML.CHURN_LABELS for run_date to S3 in Parquet format."""
    run_date  = get_run_date(context)
    s3_prefix = f"{ML_S3_BASE_PREFIX}/churn_labels/run_date={run_date}/"

    select_sql = f"""
        SELECT *
        FROM ML.CHURN_LABELS
        WHERE label_date = '{run_date}'
    """
    s3_path = snowflake_unload_to_s3(
        select_sql=select_sql,
        s3_prefix=s3_prefix,
        file_format="PARQUET",
        overwrite=True,
    )
    log.info("Churn labels unloaded to: %s", s3_path)
    context["ti"].xcom_push(key="churn_labels_s3_path", value=s3_path)


def _build_trigger_payload(**context) -> str:
    """Build the JSON payload for the ML training trigger from XCom data."""
    run_date = get_run_date(context)
    return json.dumps({
        "run_date":     run_date,
        "s3_prefix":    ML_S3_BASE_PREFIX,
        "s3_bucket":    NNN_S3_BUCKET,
        "tables":       ["customer_features", "network_features", "churn_labels"],
        "triggered_at": datetime.now(tz=timezone.utc).isoformat(),
    })


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_snowflake_to_s3_ml_export_daily",
    description="Export ML feature tables from Snowflake to S3 Parquet, then trigger ML training",
    default_args=default_args,
    schedule_interval="0 12 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "ml", "snowflake", "s3", "feature-engineering", "daily"],
) as dag:

    task_export_customer = PythonOperator(  # UNLOAD ML.CUSTOMER_FEATURES to S3 Parquet for run_date
        task_id="export_customer_features",
        python_callable=export_customer_features,
    )

    task_export_network = PythonOperator(  # UNLOAD ML.NETWORK_FEATURES to S3 Parquet for run_date
        task_id="export_network_features",
        python_callable=export_network_features,
    )

    task_export_churn = PythonOperator(  # UNLOAD ML.CHURN_LABELS to S3 Parquet for run_date
        task_id="export_churn_labels",
        python_callable=export_churn_labels,
    )

    task_trigger_training = SimpleHttpOperator(  # POST to ML platform to start training job after all exports
        task_id="trigger_ml_training",
        http_conn_id=CONN_NMS_API,
        endpoint="/api/v1/training/trigger",
        method="POST",
        data=_build_trigger_payload,
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code in (200, 201, 202),
        log_response=True,
    )

    # ------------------------------------------------------------------
    # Dependency chain: fan-out exports → trigger training
    # ------------------------------------------------------------------
    [task_export_customer, task_export_network, task_export_churn] >> task_trigger_training
