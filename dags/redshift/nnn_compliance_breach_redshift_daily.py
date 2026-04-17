"""
nnn_compliance_breach_redshift_daily
--------------------------------------
Owner:      nnn-data-engineering
Domain:     Compliance / SLA Monitoring
Schedule:   Daily at 11:00 AEST / 01:00 UTC (0 1 * * *)
SLA:        2 hours

Synchronises Snowflake COMPLIANCE.SLA_BREACHES to the Redshift compliance layer
(compliance.sla_breaches) once per day.  After loading, the CSG monthly credits
view is refreshed and a Slack alert is raised if total breaches exceed the
high-breach threshold (typically triggers a regulatory review).

Steps:
  1. unload_breaches           — UNLOAD SLA breach records from Snowflake to S3
  2. copy_to_redshift          — COPY from S3 into compliance.sla_breaches
  3. refresh_csg_summary_view  — refresh compliance.v_csg_monthly_credits view
  4. alert_if_high_breach_day  — query breach count; Slack alert if > threshold

Upstream:   COMPLIANCE.SLA_BREACHES (Snowflake compliance ETL)
Downstream: compliance.sla_breaches → CSG credits, compliance dashboards
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_post_slack_message, nnn_sla_miss_alert
from nnn_common.utils import (
    get_redshift_hook,
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

_S3_PREFIX_TEMPLATE = "redshift-staging/sla_breaches/run_date={run_date}/"

# Breach count above which a Slack warning is sent to the compliance channel
_HIGH_BREACH_THRESHOLD = 500   # Threshold that typically triggers a regulatory review

# Monthly CSG credits rollup view
_CSG_VIEW_SQL = """
CREATE OR REPLACE VIEW compliance.v_csg_monthly_credits AS
SELECT
    rsp_id,
    DATE_TRUNC('month', breach_date)        AS breach_month,
    COUNT(*)                                AS total_breaches,
    SUM(csg_credit_amount_aud)              AS total_csg_credits_aud,
    AVG(days_overdue)                       AS avg_days_overdue,
    MAX(loaded_at)                          AS last_loaded_at
FROM compliance.sla_breaches
GROUP BY 1, 2
"""


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def unload_breaches(**context: object) -> None:
    """Unload SLA breach records from Snowflake ``COMPLIANCE.SLA_BREACHES`` to S3.

    Selects all breach rows with ``breach_date`` matching the pipeline run date
    and stages them as Parquet files for the downstream Redshift COPY step.
    """
    run_date = get_run_date(context)
    s3_prefix = _S3_PREFIX_TEMPLATE.format(run_date=run_date)

    select_sql = f"""
        SELECT *
        FROM COMPLIANCE.SLA_BREACHES
        WHERE breach_date = '{run_date}'
    """
    snowflake_unload_to_s3(select_sql=select_sql, s3_prefix=s3_prefix)


def copy_to_redshift(**context: object) -> None:
    """COPY staged breach Parquet files from S3 into ``compliance.sla_breaches``.

    A DELETE for today's breach_date is performed first so reruns are
    idempotent without wiping rows from other dates (truncate=True would
    destroy all historical breach data in the table).
    """
    run_date = get_run_date(context)
    s3_prefix = _S3_PREFIX_TEMPLATE.format(run_date=run_date)

    # Remove only today's partition before re-loading (idempotent)
    hook = get_redshift_hook()
    hook.run(
        "DELETE FROM compliance.sla_breaches WHERE breach_date = %s",
        parameters=(run_date,),
    )

    redshift_copy_from_s3(
        table="compliance.sla_breaches",
        s3_prefix=s3_prefix,
        file_format="PARQUET",
        truncate=False,  # do not wipe other dates' data
    )


def refresh_csg_summary_view(**context: object) -> None:
    """Refresh the CSG monthly credits summary view ``compliance.v_csg_monthly_credits``.

    The view aggregates breaches and CSG credit amounts per RSP per month.
    Refreshing after each daily load ensures finance and compliance dashboards
    always see up-to-date rolling month totals.
    """
    hook = get_redshift_hook()
    hook.run(_CSG_VIEW_SQL)


def alert_if_high_breach_day(**context: object) -> None:
    """Send a Slack warning if today's breach count exceeds the high-breach threshold.

    Queries ``compliance.sla_breaches`` for today's total count using the
    Redshift hook.  If the count exceeds 500, posts a warning message to the
    ``#compliance-alerts`` Slack channel via ``nnn_post_slack_message``.

    No exception is raised — the pipeline continues even if the threshold is
    breached; this is an early-warning notification only.
    """
    run_date = get_run_date(context)
    hook = get_redshift_hook()

    # PERF: use get_first() (returns a single tuple) instead of get_pandas_df()
    # which loads the result into a DataFrame — wasteful for a single scalar value.
    result = hook.get_first(
        f"""
        SELECT COUNT(*) AS breach_count
        FROM compliance.sla_breaches
        WHERE breach_date = '{run_date}'
        """
    )
    breach_count = int(result[0]) if result else 0

    if breach_count > _HIGH_BREACH_THRESHOLD:
        nnn_post_slack_message(
            channel="#compliance-alerts",
            message=(
                f":warning: *High breach day alert* — {breach_count} SLA breaches "
                f"recorded on `{run_date}` (threshold: {_HIGH_BREACH_THRESHOLD}). "
                "Please review `compliance.sla_breaches` and initiate regulatory review if required."
            ),
        )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_compliance_breach_redshift_daily",
    description="Sync Snowflake COMPLIANCE.SLA_BREACHES to Redshift; alert on high-breach days",
    default_args=default_args,
    schedule_interval="0 1 * * *",  # 01:00 UTC = 11:00 AEST
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "compliance", "daily", "redshift"],
) as dag:

    # Step 1 – export breach records from Snowflake to S3
    t_unload = PythonOperator(  # Unload COMPLIANCE.SLA_BREACHES for breach_date to S3 Parquet
        task_id="unload_breaches",
        python_callable=unload_breaches,
        sla=timedelta(hours=1),
        doc_md="Unload SLA breach records from Snowflake COMPLIANCE.SLA_BREACHES to S3.",
    )

    # Step 2 – COPY from S3 into Redshift compliance schema
    t_copy = PythonOperator(  # COPY staged breach Parquet files into compliance.sla_breaches
        task_id="copy_to_redshift",
        python_callable=copy_to_redshift,
        sla=timedelta(hours=1, minutes=30),
        doc_md="COPY staged breach Parquet files into compliance.sla_breaches in Redshift.",
    )

    # Step 3 – refresh CSG monthly credits view
    t_view = PythonOperator(  # CREATE OR REPLACE compliance.v_csg_monthly_credits rollup view
        task_id="refresh_csg_summary_view",
        python_callable=refresh_csg_summary_view,
        sla=timedelta(hours=1, minutes=45),
        doc_md="Recreate compliance.v_csg_monthly_credits per-RSP monthly aggregation view.",
    )

    # Step 4 – conditional Slack alert for high-breach days
    t_alert = PythonOperator(  # Post Slack warning if today's breach count exceeds _HIGH_BREACH_THRESHOLD
        task_id="alert_if_high_breach_day",
        python_callable=alert_if_high_breach_day,
        sla=timedelta(hours=2),
        doc_md="Send Slack warning if today's breach count exceeds 500.",
    )

    # Linear dependency chain
    t_unload >> t_copy >> t_view >> t_alert
