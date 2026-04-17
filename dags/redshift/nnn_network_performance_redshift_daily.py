"""
nnn_network_performance_redshift_daily
----------------------------------------
Owner:      nnn-data-engineering
Domain:     Network / Link Performance
Schedule:   Daily at 06:00 AEST / 20:00 UTC (0 20 * * *)
SLA:        2 hours

Synchronises Snowflake NETWORK.LINK_PERFORMANCE_DAILY to the Redshift
analytics layer (analytics.network_performance) once per day.  After loading,
a minimum row-count gate and a pipeline audit record ensure data completeness.

Steps:
  1. unload_from_snowflake   — UNLOAD previous day's link-performance rows from Snowflake to S3
  2. copy_to_redshift        — COPY from S3 into analytics.network_performance in Redshift
  3. validate_row_count      — assert >= 500 rows loaded for the run date
  4. update_redshift_metadata — insert pipeline-run audit record into analytics.pipeline_run_log

Upstream:   NETWORK.LINK_PERFORMANCE_DAILY (Snowflake network load, ~02:00 AEST)
Downstream: analytics.network_performance → network performance dashboards
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    assert_redshift_row_count,
    get_redshift_hook,
    get_run_date,
    redshift_copy_from_s3,
    snowflake_unload_to_s3,
)

log = logging.getLogger(__name__)

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

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_S3_PREFIX_TEMPLATE = "redshift-staging/network_performance/run_date={run_date}/"
_MIN_NETWORK_ROWS   = 500  # Minimum expected daily link-performance records


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def unload_from_snowflake(**context: object) -> None:
    """Unload yesterday's link-performance rows from Snowflake to S3.

    Selects all columns from ``NETWORK.LINK_PERFORMANCE_DAILY`` for the
    pipeline run date and stages them as Parquet files under the S3 prefix
    used by the downstream COPY step.
    """
    run_date = get_run_date(context)
    s3_prefix = _S3_PREFIX_TEMPLATE.format(run_date=run_date)

    select_sql = f"""
        SELECT *
        FROM NETWORK.LINK_PERFORMANCE_DAILY
        WHERE report_date = '{run_date}'
    """
    snowflake_unload_to_s3(select_sql=select_sql, s3_prefix=s3_prefix)


def copy_to_redshift(**context: object) -> None:
    """COPY staged Parquet files from S3 into ``analytics.network_performance``.

    A DELETE for today's report_date is performed first so reruns are
    idempotent without wiping rows from other dates.
    """
    run_date = get_run_date(context)
    s3_prefix = _S3_PREFIX_TEMPLATE.format(run_date=run_date)

    # Remove only today's partition before re-loading (idempotent)
    hook = get_redshift_hook()
    hook.run(
        "DELETE FROM analytics.network_performance WHERE report_date = %s",
        parameters=(run_date,),
    )

    redshift_copy_from_s3(
        table="analytics.network_performance",
        s3_prefix=s3_prefix,
        file_format="PARQUET",
        truncate=False,  # do not wipe other dates' data
    )


def validate_row_count(**context: object) -> None:
    """Assert that at least 500 rows were loaded for the run date.

    Raises ``ValueError`` if the row count falls below the minimum threshold,
    preventing downstream consumers from ingesting incomplete data.
    """
    run_date = get_run_date(context)
    hook = get_redshift_hook()
    row = hook.get_first(
        "SELECT COUNT(*) FROM analytics.network_performance WHERE report_date = %s",
        parameters=(run_date,),
    )
    row_count = row[0] if row else 0
    log.info("Row count for analytics.network_performance on %s: %d", run_date, row_count)
    if row_count < _MIN_NETWORK_ROWS:
        raise ValueError(
            f"Redshift data quality check failed: analytics.network_performance has {row_count} rows "
            f"for report_date={run_date} (expected >= {_MIN_NETWORK_ROWS})"
        )


def update_redshift_metadata(**context: object) -> None:
    """Insert an audit record into ``analytics.pipeline_run_log``.

    Records the DAG id, run date, row count (from XCom if available), and
    current UTC timestamp so that data-ops teams can track pipeline health.
    """
    run_date = get_run_date(context)
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]

    hook = get_redshift_hook()
    hook.run(
        f"""
        INSERT INTO analytics.pipeline_run_log
            (dag_id, run_id, target_table, run_date, loaded_at)
        VALUES (
            '{dag_id}',
            '{run_id}',
            'analytics.network_performance',
            '{run_date}',
            GETDATE()
        )
        """
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_network_performance_redshift_daily",
    description="Sync Snowflake NETWORK.LINK_PERFORMANCE_DAILY to Redshift daily",
    default_args=default_args,
    schedule_interval="0 20 * * *",  # 20:00 UTC = 06:00 AEST
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "network", "daily", "redshift"],
) as dag:

    # Step 1 – export from Snowflake to S3
    t_unload = PythonOperator(  # Unload NETWORK.LINK_PERFORMANCE_DAILY for report_date to S3 Parquet
        task_id="unload_from_snowflake",
        python_callable=unload_from_snowflake,
        sla=timedelta(hours=1),
        doc_md="Unload link-performance rows from Snowflake NETWORK.LINK_PERFORMANCE_DAILY to S3.",
    )

    # Step 2 – load from S3 into Redshift
    t_copy = PythonOperator(  # COPY staged Parquet files into analytics.network_performance in Redshift
        task_id="copy_to_redshift",
        python_callable=copy_to_redshift,
        sla=timedelta(hours=1, minutes=30),
        doc_md="COPY staged Parquet files into analytics.network_performance in Redshift.",
    )

    # Step 3 – data quality gate
    t_validate = PythonOperator(  # Assert >= 500 rows loaded for the run date
        task_id="validate_row_count",
        python_callable=validate_row_count,
        sla=timedelta(hours=1, minutes=45),
        doc_md="Assert minimum 500 rows loaded for the run date.",
    )

    # Step 4 – audit log
    t_metadata = PythonOperator(  # Insert pipeline-run audit record into analytics.pipeline_run_log
        task_id="update_redshift_metadata",
        python_callable=update_redshift_metadata,
        sla=timedelta(hours=2),
        doc_md="Write pipeline-run audit record into analytics.pipeline_run_log.",
    )

    # Linear dependency chain
    t_unload >> t_copy >> t_validate >> t_metadata
