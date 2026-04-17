"""
nnn_cvc_billing_redshift_daily
--------------------------------
Owner:      nnn-data-engineering
Domain:     Finance / CVC Billing
Schedule:   Daily at 10:00 AEST / 00:00 UTC (0 0 * * *)
SLA:        3 hours

Synchronises Snowflake FINANCE.CVC_BILLING_LINES to the Redshift finance layer
(finance.cvc_billing_lines) once per day.  Billing data is cumulative —
depends_on_past=True ensures missed runs are always backfilled in order.

Steps:
  1. unload_billing_from_snowflake — UNLOAD current day's CVC billing lines to S3
  2. copy_billing_to_redshift      — COPY from S3 into finance.cvc_billing_lines
  3. validate_billing_integrity    — assert row count and no null rsp_id values
  4. refresh_redshift_billing_view — refresh finance.v_cvc_billing_monthly view

Upstream:   FINANCE.CVC_BILLING_LINES (Snowflake billing load, ~08:00 AEST)
Downstream: finance.v_cvc_billing_monthly → finance dashboards
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

# ---------------------------------------------------------------------------
# Default arguments — depends_on_past overridden to True for billing
# ---------------------------------------------------------------------------
default_args = {
    "owner": "nnn-data-engineering",
    "depends_on_past": True,   # billing is cumulative; enforce ordering
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

_S3_PREFIX_TEMPLATE = "redshift-staging/cvc_billing_lines/run_date={run_date}/"

# Monthly rollup view DDL — parameterised at runtime so the view always
# reflects the latest data without a full table rebuild.
_BILLING_VIEW_SQL = """
CREATE OR REPLACE VIEW finance.v_cvc_billing_monthly AS
SELECT
    rsp_id,
    DATE_TRUNC('month', billing_date)   AS billing_month,
    SUM(charge_amount_aud)              AS total_charge_aud,
    SUM(cvc_mb_allocated)               AS total_cvc_mb_allocated,
    SUM(cvc_mb_used)                    AS total_cvc_mb_used,
    COUNT(*)                            AS line_count,
    MAX(loaded_at)                      AS last_loaded_at
FROM finance.cvc_billing_lines
GROUP BY 1, 2
"""


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def unload_billing_from_snowflake(**context: object) -> None:
    """Unload today's CVC billing lines from Snowflake ``FINANCE.CVC_BILLING_LINES`` to S3.

    Only rows matching the pipeline run date are exported so that S3 staging
    files remain small and the downstream COPY is idempotent per run date.
    """
    run_date = get_run_date(context)
    s3_prefix = _S3_PREFIX_TEMPLATE.format(run_date=run_date)

    select_sql = f"""
        SELECT *
        FROM FINANCE.CVC_BILLING_LINES
        WHERE billing_date = '{run_date}'
    """
    snowflake_unload_to_s3(select_sql=select_sql, s3_prefix=s3_prefix)


def copy_billing_to_redshift(**context: object) -> None:
    """COPY staged CVC billing Parquet files from S3 into ``finance.cvc_billing_lines``.

    A DELETE for today's billing_date is performed first so reruns are
    idempotent without wiping rows from other dates.
    """
    run_date = get_run_date(context)
    s3_prefix = _S3_PREFIX_TEMPLATE.format(run_date=run_date)

    # Remove only today's partition before re-loading (idempotent)
    hook = get_redshift_hook()
    hook.run(
        "DELETE FROM finance.cvc_billing_lines WHERE billing_date = %s",
        parameters=(run_date,),
    )

    redshift_copy_from_s3(
        table="finance.cvc_billing_lines",
        s3_prefix=s3_prefix,
        file_format="PARQUET",
        truncate=False,  # do not wipe other dates' data
    )


def validate_billing_integrity(**context: object) -> None:
    """Assert row count and referential integrity for the loaded billing data.

    Two checks are performed using the same Redshift hook connection:
    1. Minimum row count (uses ``assert_redshift_row_count`` helper).
    2. Zero null ``rsp_id`` rows — a null RSP ID cannot be billed.

    Raises ``ValueError`` if either check fails.
    """
    run_date = get_run_date(context)

    # Check 1: minimum row count (use billing_date as the partition column)
    hook = get_redshift_hook()
    row = hook.get_first(
        "SELECT COUNT(*) FROM finance.cvc_billing_lines WHERE billing_date = %s",
        parameters=(run_date,),
    )
    row_count = row[0] if row else 0
    if row_count < 1:
        raise ValueError(
            f"Redshift data quality check failed: finance.cvc_billing_lines has {row_count} rows "
            f"for billing_date={run_date} (expected >= 1)"
        )

    # Check 2: no null rsp_id — use same hook to avoid a second connection
    # PERF: use get_first() (returns a single tuple) instead of get_pandas_df()
    # which loads the result into a DataFrame — wasteful for a single scalar value.
    result = hook.get_first(
        f"""
        SELECT COUNT(*) AS null_rsp_count
        FROM finance.cvc_billing_lines
        WHERE billing_date = '{run_date}'
          AND rsp_id IS NULL
        """
    )
    null_count = int(result[0]) if result else 0
    if null_count > 0:
        raise ValueError(
            f"Billing integrity failure: {null_count} rows with null rsp_id "
            f"found in finance.cvc_billing_lines for run_date={run_date}"
        )


def refresh_redshift_billing_view(**context: object) -> None:
    """Refresh the monthly CVC billing rollup view ``finance.v_cvc_billing_monthly``.

    Redshift views are not materialised — CREATE OR REPLACE is lightweight and
    ensures any column changes in the base table are reflected immediately.
    """
    hook = get_redshift_hook()
    hook.run(_BILLING_VIEW_SQL)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_cvc_billing_redshift_daily",
    description="Sync Snowflake FINANCE.CVC_BILLING_LINES to Redshift daily (billing critical)",
    default_args=default_args,
    schedule_interval="0 0 * * *",  # 00:00 UTC = 10:00 AEST
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "finance", "daily", "redshift"],
) as dag:

    # Step 1 – export billing lines from Snowflake to S3
    t_unload = PythonOperator(  # Unload FINANCE.CVC_BILLING_LINES for billing_date to S3 Parquet
        task_id="unload_billing_from_snowflake",
        python_callable=unload_billing_from_snowflake,
        sla=timedelta(hours=1),
        doc_md="Unload CVC billing lines from Snowflake FINANCE.CVC_BILLING_LINES to S3.",
    )

    # Step 2 – COPY into Redshift finance table
    t_copy = PythonOperator(  # COPY staged CVC billing Parquet files into finance.cvc_billing_lines
        task_id="copy_billing_to_redshift",
        python_callable=copy_billing_to_redshift,
        sla=timedelta(hours=2),
        doc_md="COPY staged CVC billing Parquet files into finance.cvc_billing_lines in Redshift.",
    )

    # Step 3 – data quality and integrity gate
    t_validate = PythonOperator(  # Assert row count >= 1 and zero null rsp_id values
        task_id="validate_billing_integrity",
        python_callable=validate_billing_integrity,
        sla=timedelta(hours=2, minutes=30),
        doc_md="Assert row count and zero null rsp_id for the loaded billing data.",
    )

    # Step 4 – refresh monthly rollup view
    t_view = PythonOperator(  # CREATE OR REPLACE finance.v_cvc_billing_monthly monthly rollup view
        task_id="refresh_redshift_billing_view",
        python_callable=refresh_redshift_billing_view,
        sla=timedelta(hours=3),
        doc_md="Recreate finance.v_cvc_billing_monthly monthly rollup view in Redshift.",
    )

    # Linear dependency chain
    t_unload >> t_copy >> t_validate >> t_view
