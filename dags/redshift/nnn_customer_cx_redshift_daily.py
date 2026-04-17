"""
nnn_customer_cx_redshift_daily
--------------------------------
Owner:      nnn-data-engineering
Domain:     ML / Customer Experience
Schedule:   Daily at 10:00 AEST / 00:00 UTC (0 0 * * *)
SLA:        3 hours

Synchronises Snowflake ML.CX_FEATURE_STORE to the Redshift ML layer
(ml.cx_features) once per day and notifies the ML platform when the feature
set is ready for model serving.

Steps:
  1. unload_cx_features    — UNLOAD CX features from Snowflake to S3 Parquet
  2. copy_to_redshift      — COPY from S3 into ml.cx_features in Redshift
  3. validate_feature_count — assert >= 10,000 feature rows for run_date
  4. notify_ml_platform    — POST to ML platform webhook to signal feature readiness

Upstream:   ML.CX_FEATURE_STORE (Snowflake ML feature pipeline)
Downstream: ML platform model serving (Redshift ml.cx_features)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_ML_PLATFORM,
    assert_redshift_row_count,
    get_run_date,
    redshift_copy_from_s3,
    snowflake_unload_to_s3,
)

# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner": "nnn-data-engineering",
    "depends_on_past": False,
    "email": ["de-alerts@nnnco.com.au"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "on_failure_callback": nnn_failure_alert,
    "execution_timeout": timedelta(hours=2),
}

log = logging.getLogger(__name__)

_S3_PREFIX_TEMPLATE = "redshift-staging/cx_features/run_date={run_date}/"

# Minimum feature rows required for the ML platform to produce reliable scores
_MIN_FEATURE_ROWS = 10_000


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def unload_cx_features(**context: object) -> None:
    """Unload CX feature vectors from Snowflake ``ML.CX_FEATURE_STORE`` to S3.

    Selects all feature rows generated on the run date and stages them as
    Parquet files for the downstream Redshift COPY step.
    """
    run_date = get_run_date(context)
    s3_prefix = _S3_PREFIX_TEMPLATE.format(run_date=run_date)

    select_sql = f"""
        SELECT *
        FROM ML.CX_FEATURE_STORE
        WHERE feature_date = '{run_date}'
    """
    snowflake_unload_to_s3(select_sql=select_sql, s3_prefix=s3_prefix)


def copy_to_redshift(**context: object) -> None:
    """COPY staged CX feature Parquet files from S3 into ``ml.cx_features``.

    A DELETE for today's feature_date is performed first so reruns are
    idempotent without wiping rows from other dates.
    """
    from nnn_common.utils import get_redshift_hook as _get_redshift_hook
    run_date = get_run_date(context)
    s3_prefix = _S3_PREFIX_TEMPLATE.format(run_date=run_date)

    # Remove only today's partition before re-loading (idempotent)
    hook = _get_redshift_hook()
    hook.run(
        "DELETE FROM ml.cx_features WHERE feature_date = %s",
        parameters=(run_date,),
    )

    redshift_copy_from_s3(
        table="ml.cx_features",
        s3_prefix=s3_prefix,
        file_format="PARQUET",
        truncate=False,  # do not wipe other dates' data
    )


def validate_feature_count(**context: object) -> None:
    """Assert that at least 10,000 CX feature rows are present for the run date.

    The ML platform requires a minimum feature population to avoid model
    instability.  If the count falls below the threshold the pipeline halts
    before the ML webhook notification is sent.
    """
    from nnn_common.utils import get_redshift_hook as _get_redshift_hook
    run_date = get_run_date(context)
    hook = _get_redshift_hook()
    row = hook.get_first(
        "SELECT COUNT(*) FROM ml.cx_features WHERE feature_date = %s",
        parameters=(run_date,),
    )
    row_count = row[0] if row else 0
    log.info("Row count for ml.cx_features on %s: %d", run_date, row_count)
    if row_count < _MIN_FEATURE_ROWS:
        raise ValueError(
            f"Redshift data quality check failed: ml.cx_features has {row_count} rows "
            f"for feature_date={run_date} (expected >= {_MIN_FEATURE_ROWS})"
        )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_customer_cx_redshift_daily",
    description="Sync Snowflake ML.CX_FEATURE_STORE to Redshift and notify ML platform",
    default_args=default_args,
    schedule_interval="0 0 * * *",  # 00:00 UTC = 10:00 AEST
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "ml", "daily", "redshift"],
) as dag:

    # Step 1 – export CX features from Snowflake to S3
    t_unload = PythonOperator(  # Unload ML.CX_FEATURE_STORE for feature_date to S3 Parquet
        task_id="unload_cx_features",
        python_callable=unload_cx_features,
        sla=timedelta(hours=1),
        doc_md="Unload CX feature vectors from Snowflake ML.CX_FEATURE_STORE to S3.",
    )

    # Step 2 – COPY from S3 into Redshift ml schema
    t_copy = PythonOperator(  # COPY staged CX feature Parquet files into ml.cx_features in Redshift
        task_id="copy_to_redshift",
        python_callable=copy_to_redshift,
        sla=timedelta(hours=2),
        doc_md="COPY staged CX feature Parquet files into ml.cx_features in Redshift.",
    )

    # Step 3 – data quality gate (minimum feature population check)
    t_validate = PythonOperator(  # Assert >= 10,000 CX feature rows loaded for the run date
        task_id="validate_feature_count",
        python_callable=validate_feature_count,
        sla=timedelta(hours=2, minutes=30),
        doc_md="Assert at least 10,000 CX feature rows loaded for the run date.",
    )

    # Step 4 – notify the ML platform that today's feature set is ready
    # The webhook URL is stored in Airflow Variable ml_webhook_url to avoid
    # hardcoding.  The HTTP connection id is managed via Airflow Connections.
    t_notify = SimpleHttpOperator(  # POST to ML platform webhook signalling CX features are ready
        task_id="notify_ml_platform",
        http_conn_id=CONN_ML_PLATFORM,
        endpoint="{{ var.value.ml_webhook_url }}",
        method="POST",
        headers={"Content-Type": "application/json"},
        data='{"event": "cx_features_ready", "run_date": "{{ ds }}"}',
        response_check=lambda response: response.status_code == 200,
        sla=timedelta(hours=3),
        doc_md="POST to ML platform webhook signalling that CX features are available in Redshift.",
    )

    # Linear dependency chain
    t_unload >> t_copy >> t_validate >> t_notify
