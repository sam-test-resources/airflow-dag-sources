"""
nnn_rsp_activation_redshift_daily
------------------------------------
Owner:      nnn-data-engineering
Domain:     Wholesale / RSP Activations
Schedule:   Daily at 05:00 AEST / 19:00 UTC (0 19 * * *)
SLA:        2 hours

Synchronises Snowflake WHOLESALE.RSP_ACTIVATIONS to the Redshift wholesale
layer (wholesale.rsp_activations) once per day.  After loading, a per-RSP SLA
summary is computed and written to wholesale.rsp_daily_sla_summary for
RSP-facing SLA reports.

Steps:
  1. extract_activations_to_s3 — UNLOAD RSP activation records from Snowflake to S3
  2. load_to_redshift          — COPY from S3 into wholesale.rsp_activations in Redshift
  3. compute_daily_sla_summary — compute per-RSP SLA rate; upsert into rsp_daily_sla_summary
  4. validate_load             — assert >= 1 activation row loaded for the run date

Upstream:   WHOLESALE.RSP_ACTIVATIONS (Snowflake wholesale provisioning pipeline)
Downstream: wholesale.rsp_daily_sla_summary → RSP SLA reports
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

_S3_PREFIX_TEMPLATE = "redshift-staging/rsp_activations/run_date={run_date}/"

# SLA is considered met when activation is provisioned within 2 business days
_SLA_THRESHOLD_HOURS = 48


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def extract_activations_to_s3(**context: object) -> None:
    """Unload RSP activation records from Snowflake ``WHOLESALE.RSP_ACTIVATIONS`` to S3.

    Selects all activations with an ``activation_date`` matching the pipeline
    run date and exports them as Parquet files to the S3 staging prefix.
    """
    run_date = get_run_date(context)
    s3_prefix = _S3_PREFIX_TEMPLATE.format(run_date=run_date)

    select_sql = f"""
        SELECT *
        FROM WHOLESALE.RSP_ACTIVATIONS
        WHERE activation_date = '{run_date}'
    """
    snowflake_unload_to_s3(select_sql=select_sql, s3_prefix=s3_prefix)


def load_to_redshift(**context: object) -> None:
    """COPY staged activation Parquet files from S3 into ``wholesale.rsp_activations``.

    A DELETE for today's activation_date is performed first so reruns are
    idempotent without wiping rows from other dates.
    """
    run_date = get_run_date(context)
    s3_prefix = _S3_PREFIX_TEMPLATE.format(run_date=run_date)

    # Remove only today's partition before re-loading (idempotent)
    hook = get_redshift_hook()
    hook.run(
        "DELETE FROM wholesale.rsp_activations WHERE activation_date = %s",
        parameters=(run_date,),
    )

    redshift_copy_from_s3(
        table="wholesale.rsp_activations",
        s3_prefix=s3_prefix,
        file_format="PARQUET",
        truncate=False,  # do not wipe other dates' data
    )


def compute_daily_sla_summary(**context: object) -> None:
    """Compute per-RSP SLA metrics for the run date and persist to the summary table.

    Uses a single Redshift hook connection to:
    1. Query ``wholesale.rsp_activations`` via ``get_pandas_df`` for in-Python
       aggregation of SLA pass/fail rates.
    2. Upsert computed rows into ``wholesale.rsp_daily_sla_summary`` using
       Redshift's DELETE+INSERT pattern on a temp table.

    An activation is considered SLA-compliant when ``provisioning_hours`` is
    less than or equal to the SLA threshold (48 hours).
    """
    run_date = get_run_date(context)
    hook = get_redshift_hook()

    # PERF: use get_records() (returns a list of tuples) instead of get_pandas_df()
    # which loads all rows into a DataFrame. Result is bounded to one row per RSP.
    records = hook.get_records(
        f"""
        SELECT
            rsp_id,
            COUNT(*)                                            AS total_activations,
            SUM(CASE WHEN provisioning_hours <= {_SLA_THRESHOLD_HOURS}
                     THEN 1 ELSE 0 END)                        AS sla_met_count,
            AVG(provisioning_hours)                            AS avg_provisioning_hours
        FROM wholesale.rsp_activations
        WHERE activation_date = '{run_date}'
        GROUP BY rsp_id
        """
    )

    if not records:
        return  # nothing to insert; upstream validation will catch true gaps

    # Compute SLA rate per row: (sla_met_count / total_activations) * 100
    # row indices: 0=rsp_id, 1=total_activations, 2=sla_met_count, 3=avg_provisioning_hours
    value_rows = ", ".join(
        f"('{row[0]}', '{run_date}', {row[1]}, "
        f"{row[2]}, {round((row[2] / row[1]) * 100, 4) if row[1] else 0}, {row[3]})"
        for row in records
    )

    # Upsert via DELETE+INSERT on the same hook connection
    hook.run(
        f"""
        CREATE TEMP TABLE tmp_rsp_sla (LIKE wholesale.rsp_daily_sla_summary);

        INSERT INTO tmp_rsp_sla
            (rsp_id, run_date, total_activations, sla_met_count, sla_rate_pct, avg_provisioning_hours)
        VALUES {value_rows};

        DELETE FROM wholesale.rsp_daily_sla_summary
        USING tmp_rsp_sla
        WHERE wholesale.rsp_daily_sla_summary.rsp_id   = tmp_rsp_sla.rsp_id
          AND wholesale.rsp_daily_sla_summary.run_date  = tmp_rsp_sla.run_date;

        INSERT INTO wholesale.rsp_daily_sla_summary
        SELECT * FROM tmp_rsp_sla;
        """
    )


def validate_load(**context: object) -> None:
    """Assert at least one activation row was loaded for the run date.

    A zero-row result indicates either an upstream issue in Snowflake or a
    failed COPY; the pipeline is halted to prevent downstream SLA reporting
    from consuming stale data.
    """
    run_date = get_run_date(context)
    hook = get_redshift_hook()
    row = hook.get_first(
        "SELECT COUNT(*) FROM wholesale.rsp_activations WHERE activation_date = %s",
        parameters=(run_date,),
    )
    row_count = row[0] if row else 0
    log.info("Row count for wholesale.rsp_activations on %s: %d", run_date, row_count)
    if row_count < 1:
        raise ValueError(
            f"Redshift data quality check failed: wholesale.rsp_activations has {row_count} rows "
            f"for activation_date={run_date} (expected >= 1)"
        )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_rsp_activation_redshift_daily",
    description="Sync Snowflake WHOLESALE.RSP_ACTIVATIONS to Redshift and compute SLA summary",
    default_args=default_args,
    schedule_interval="0 19 * * *",  # 19:00 UTC = 05:00 AEST
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "wholesale", "daily", "redshift"],
) as dag:

    # Step 1 – export activations from Snowflake to S3
    t_extract = PythonOperator(  # Unload WHOLESALE.RSP_ACTIVATIONS for activation_date to S3 Parquet
        task_id="extract_activations_to_s3",
        python_callable=extract_activations_to_s3,
        sla=timedelta(hours=1),
        doc_md="Unload RSP activations from Snowflake WHOLESALE.RSP_ACTIVATIONS to S3.",
    )

    # Step 2 – COPY from S3 into Redshift
    t_load = PythonOperator(  # COPY staged activation Parquet files into wholesale.rsp_activations
        task_id="load_to_redshift",
        python_callable=load_to_redshift,
        sla=timedelta(hours=1, minutes=30),
        doc_md="COPY staged activation Parquet files into wholesale.rsp_activations.",
    )

    # Step 3 – per-RSP SLA aggregation written back to Redshift
    t_sla = PythonOperator(  # Compute per-RSP SLA rates and upsert into wholesale.rsp_daily_sla_summary
        task_id="compute_daily_sla_summary",
        python_callable=compute_daily_sla_summary,
        sla=timedelta(hours=1, minutes=45),
        doc_md="Compute per-RSP SLA rates and upsert into wholesale.rsp_daily_sla_summary.",
    )

    # Step 4 – data quality gate
    t_validate = PythonOperator(  # Assert >= 1 activation row loaded for the run date
        task_id="validate_load",
        python_callable=validate_load,
        sla=timedelta(hours=2),
        doc_md="Assert at least one activation row loaded for the run date.",
    )

    # Linear dependency chain
    t_extract >> t_load >> t_sla >> t_validate
