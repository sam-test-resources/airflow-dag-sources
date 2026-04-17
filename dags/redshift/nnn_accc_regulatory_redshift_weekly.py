"""
nnn_accc_regulatory_redshift_weekly
-------------------------------------
Owner:      nnn-data-engineering
Domain:     Compliance / Regulatory
Schedule:   Fridays at 08:00 AEST / 22:00 UTC Thursday (0 22 * * 4)
SLA:        6 hours

Synchronises Snowflake COMPLIANCE.ACCC_METRICS to the Redshift regulatory layer
(regulatory.accc_metrics), creates an anonymised submission view, exports the
anonymised data to S3 CSV, and validates that all 8 Australian states/territories
are represented before the submission is marked ready.

Steps:
  1. unload_accc_metrics      — UNLOAD ACCC metrics from Snowflake to S3
  2. copy_to_redshift         — COPY from S3 into regulatory.accc_metrics in Redshift
  3. create_anonymised_view   — CREATE OR REPLACE regulatory.v_accc_submission
  4. export_regulatory_csv    — UNLOAD the anonymised view to S3 CSV
  5. validate_submission_ready — assert all 8 state rows are present

Upstream:   COMPLIANCE.ACCC_METRICS (refreshed by compliance ETL)
Downstream: ACCC regulatory portal (external consumer)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    get_redshift_hook,
    get_run_date,
    redshift_copy_from_s3,
    redshift_unload_to_s3,
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

_S3_PREFIX_TEMPLATE    = "redshift-staging/accc_metrics/run_date={run_date}/"
_S3_SUBMISSION_TEMPLATE = "compliance/accc/redshift/{week}/submission.csv"

# All 8 Australian states/territories that must appear in the submission
_REQUIRED_STATES = frozenset({"NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"})

# Anonymised submission view DDL — RSP IDs replaced by stable row numbers
# derived from alphabetical ordering, preventing RSP re-identification.
_ANON_VIEW_SQL = """
CREATE OR REPLACE VIEW regulatory.v_accc_submission AS
SELECT
    ROW_NUMBER() OVER (
        PARTITION BY metric_week
        ORDER BY rsp_id
    )                                                        AS anon_rsp_id,
    metric_week,
    state_territory,
    service_class,
    avg_download_mbps,
    avg_upload_mbps,
    avg_latency_ms,
    p95_latency_ms,
    availability_pct,
    total_incidents,
    total_customers_affected,
    measurement_method,
    loaded_at
FROM regulatory.accc_metrics
"""


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def unload_accc_metrics(**context: object) -> None:
    """Unload ACCC regulatory metrics from Snowflake ``COMPLIANCE.ACCC_METRICS`` to S3.

    Selects all metrics for the current reporting week and stages them as
    Parquet files for the downstream Redshift COPY step.
    """
    run_date = get_run_date(context)
    s3_prefix = _S3_PREFIX_TEMPLATE.format(run_date=run_date)

    select_sql = f"""
        SELECT *
        FROM COMPLIANCE.ACCC_METRICS
        WHERE metric_week = '{run_date}'
    """
    snowflake_unload_to_s3(select_sql=select_sql, s3_prefix=s3_prefix)


def copy_to_redshift(**context: object) -> None:
    """COPY staged ACCC metric Parquet files from S3 into ``regulatory.accc_metrics``.

    A DELETE for this week's metric_week is performed first so reruns are
    idempotent without wiping rows from other reporting weeks.
    """
    run_date = get_run_date(context)
    s3_prefix = _S3_PREFIX_TEMPLATE.format(run_date=run_date)

    # Remove only this week's partition before re-loading (idempotent)
    hook = get_redshift_hook()
    hook.run(
        "DELETE FROM regulatory.accc_metrics WHERE metric_week = %s",
        parameters=(run_date,),
    )

    redshift_copy_from_s3(
        table="regulatory.accc_metrics",
        s3_prefix=s3_prefix,
        file_format="PARQUET",
        truncate=False,  # do not wipe other weeks' data
    )


def create_anonymised_view(**context: object) -> None:
    """Create or replace the anonymised ACCC submission view in Redshift.

    The view ``regulatory.v_accc_submission`` replaces RSP IDs with stable
    anonymous integers via ``ROW_NUMBER() OVER (PARTITION BY metric_week ORDER
    BY rsp_id)`` so that the same RSP always receives the same anonymous ID
    within a given reporting week.
    """
    hook = get_redshift_hook()
    hook.run(_ANON_VIEW_SQL)


def export_regulatory_csv(**context: object) -> None:
    """Unload the anonymised ACCC submission view from Redshift to S3 CSV.

    Exports ``regulatory.v_accc_submission`` for the current metric week to
    the S3 path ``compliance/accc/redshift/{week}/submission.csv`` using the
    Redshift UNLOAD command.  The CSV is the official ACCC submission artefact.
    """
    run_date = get_run_date(context)
    s3_prefix = _S3_SUBMISSION_TEMPLATE.format(week=run_date)

    select_sql = f"""
        SELECT
            anon_rsp_id,
            metric_week,
            state_territory,
            service_class,
            avg_download_mbps,
            avg_upload_mbps,
            avg_latency_ms,
            p95_latency_ms,
            availability_pct,
            total_incidents,
            total_customers_affected,
            measurement_method
        FROM regulatory.v_accc_submission
        WHERE metric_week = '{run_date}'
        ORDER BY state_territory, anon_rsp_id
    """
    redshift_unload_to_s3(select_sql=select_sql, s3_prefix=s3_prefix)


def validate_submission_ready(**context: object) -> None:
    """Assert that all 8 Australian states/territories are present in the submission.

    Queries the anonymised view for distinct ``state_territory`` values for the
    current week and raises ``ValueError`` listing any missing states.  All
    8 jurisdictions (NSW, VIC, QLD, SA, WA, TAS, NT, ACT) must be present for
    the submission to be considered complete.
    """
    run_date = get_run_date(context)
    hook = get_redshift_hook()

    # PERF: use get_records() (returns a list of tuples) instead of get_pandas_df()
    # which loads results into a DataFrame — wasteful for this 8-row max result set.
    rows = hook.get_records(
        f"""
        SELECT DISTINCT state_territory
        FROM regulatory.v_accc_submission
        WHERE metric_week = '{run_date}'
        """
    )

    present_states = {row[0].strip() for row in rows} if rows else set()
    missing_states = _REQUIRED_STATES - present_states

    if missing_states:
        raise ValueError(
            f"ACCC submission validation failed for week {run_date}: "
            f"missing state/territory rows: {sorted(missing_states)}. "
            "All 8 jurisdictions must be present before submission."
        )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_accc_regulatory_redshift_weekly",
    description="Weekly ACCC metrics sync from Snowflake to Redshift; anonymised CSV submission export",
    default_args=default_args,
    schedule_interval="0 22 * * 4",  # 22:00 UTC Thursday = 08:00 AEST Friday
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "compliance", "weekly", "redshift"],
) as dag:

    # Step 1 – export ACCC metrics from Snowflake to S3
    t_unload = PythonOperator(  # Unload COMPLIANCE.ACCC_METRICS for metric_week to S3 Parquet
        task_id="unload_accc_metrics",
        python_callable=unload_accc_metrics,
        sla=timedelta(hours=2),
        doc_md="Unload ACCC regulatory metrics from Snowflake COMPLIANCE.ACCC_METRICS to S3.",
    )

    # Step 2 – COPY from S3 into Redshift regulatory schema
    t_copy = PythonOperator(  # COPY staged ACCC metric Parquet files into regulatory.accc_metrics
        task_id="copy_to_redshift",
        python_callable=copy_to_redshift,
        sla=timedelta(hours=3),
        doc_md="COPY staged ACCC metric Parquet files into regulatory.accc_metrics in Redshift.",
    )

    # Step 3 – create anonymised submission view (RSP IDs replaced by row numbers)
    t_view = PythonOperator(  # CREATE OR REPLACE regulatory.v_accc_submission with anonymised RSP IDs
        task_id="create_anonymised_view",
        python_callable=create_anonymised_view,
        sla=timedelta(hours=4),
        doc_md="CREATE OR REPLACE regulatory.v_accc_submission with anonymised RSP identifiers.",
    )

    # Step 4 – unload anonymised view to S3 CSV as the submission artefact
    t_export = PythonOperator(  # Unload anonymised ACCC submission view from Redshift to S3 CSV
        task_id="export_regulatory_csv",
        python_callable=export_regulatory_csv,
        sla=timedelta(hours=5),
        doc_md="Unload anonymised ACCC submission view from Redshift to S3 CSV.",
    )

    # Step 5 – validate all 8 state rows are present before marking submission ready
    t_validate = PythonOperator(  # Assert all 8 Australian states/territories are in the submission
        task_id="validate_submission_ready",
        python_callable=validate_submission_ready,
        sla=timedelta(hours=6),
        doc_md="Assert all 8 Australian states/territories present in the ACCC submission.",
    )

    # Linear dependency chain
    t_unload >> t_copy >> t_view >> t_export >> t_validate
