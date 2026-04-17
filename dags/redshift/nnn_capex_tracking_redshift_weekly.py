"""
nnn_capex_tracking_redshift_weekly
------------------------------------
Owner:      nnn-data-engineering
Domain:     Finance / Capital Expenditure
Schedule:   Mondays at 05:00 AEST / 19:00 UTC Sunday (0 19 * * 0)
SLA:        4 hours

Full weekly refresh of Snowflake FINANCE.CAPEX_TRACKING into the Redshift finance
layer (finance.capex_tracking), followed by Earned Value Management (EVM) trend
computation and a board-level summary view refresh.

Steps:
  1. unload_capex      — UNLOAD the full CAPEX tracking dataset from Snowflake to S3
  2. reload_redshift   — TRUNCATE + COPY into finance.capex_tracking in Redshift
  3. compute_evm_trends — compute week-over-week CPI/SPI trends into capex_evm_trends
  4. refresh_board_view — refresh the board-level CAPEX summary view

Upstream:   FINANCE.CAPEX_TRACKING (Snowflake CAPEX load, ~04:00 AEST)
Downstream: finance.v_board_capex_summary → board reporting pack
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

_S3_PREFIX_TEMPLATE = "redshift-staging/capex_tracking/run_date={run_date}/"

# Board-level CAPEX summary view DDL
_BOARD_VIEW_SQL = """
CREATE OR REPLACE VIEW finance.v_board_capex_summary AS
SELECT
    project_id,
    project_name,
    program_area,
    budget_aud,
    actual_spend_aud,
    earned_value_aud,
    planned_value_aud,
    ROUND(earned_value_aud / NULLIF(actual_spend_aud, 0), 4)   AS cpi,
    ROUND(earned_value_aud / NULLIF(planned_value_aud, 0), 4)  AS spi,
    forecast_completion_date,
    project_status,
    week_ending_date,
    MAX(loaded_at) OVER (PARTITION BY project_id)              AS last_loaded_at
FROM finance.capex_tracking
"""


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def unload_capex(**context: object) -> None:
    """Unload the full CAPEX tracking dataset from Snowflake ``FINANCE.CAPEX_TRACKING`` to S3.

    No date filter is applied — this is a full weekly snapshot replacement.
    All active and historical project records are exported as Parquet files.
    """
    run_date = get_run_date(context)
    s3_prefix = _S3_PREFIX_TEMPLATE.format(run_date=run_date)

    select_sql = "SELECT * FROM FINANCE.CAPEX_TRACKING"
    snowflake_unload_to_s3(select_sql=select_sql, s3_prefix=s3_prefix)


def reload_redshift(**context: object) -> None:
    """TRUNCATE and reload ``finance.capex_tracking`` from the S3 weekly snapshot.

    truncate=True implements the full-refresh pattern required for CAPEX data,
    which can have retroactive adjustments applied to any prior week's figures.
    """
    run_date = get_run_date(context)
    s3_prefix = _S3_PREFIX_TEMPLATE.format(run_date=run_date)

    redshift_copy_from_s3(
        table="finance.capex_tracking",
        s3_prefix=s3_prefix,
        file_format="PARQUET",
        truncate=True,  # full weekly refresh — replace all rows
    )


def compute_evm_trends(**context: object) -> None:
    """Compute week-over-week CPI and SPI trends and upsert into ``finance.capex_evm_trends``.

    Uses a single Redshift hook connection to:
    1. Fetch the two most recent weeks of EVM metrics via ``get_pandas_df``.
    2. Compute delta CPI (``cpi_wow_delta``) and delta SPI (``spi_wow_delta``)
       for each project.
    3. Upsert results into ``finance.capex_evm_trends`` via DELETE+INSERT temp
       table pattern to ensure idempotency.
    """
    run_date = get_run_date(context)
    hook = get_redshift_hook()

    # PERF: use get_records() (returns a list of tuples) instead of get_pandas_df()
    # which loads all rows into a DataFrame. Result is bounded to one row per project.
    records = hook.get_records(
        """
        WITH ranked AS (
            SELECT
                project_id,
                week_ending_date,
                ROUND(earned_value_aud / NULLIF(actual_spend_aud, 0), 4)  AS cpi,
                ROUND(earned_value_aud / NULLIF(planned_value_aud, 0), 4) AS spi,
                ROW_NUMBER() OVER (
                    PARTITION BY project_id
                    ORDER BY week_ending_date DESC
                ) AS rn
            FROM finance.capex_tracking
        )
        SELECT
            curr.project_id,
            curr.week_ending_date,
            curr.cpi                          AS cpi_current,
            prev.cpi                          AS cpi_prior,
            ROUND(curr.cpi - prev.cpi, 4)    AS cpi_wow_delta,
            curr.spi                          AS spi_current,
            prev.spi                          AS spi_prior,
            ROUND(curr.spi - prev.spi, 4)    AS spi_wow_delta
        FROM ranked curr
        LEFT JOIN ranked prev
               ON curr.project_id = prev.project_id
              AND prev.rn = 2
        WHERE curr.rn = 1
        """
    )

    if not records:
        return  # no EVM data yet; skip insert

    # row indices: 0=project_id, 1=week_ending_date, 2=cpi_current, 3=cpi_prior,
    #              4=cpi_wow_delta, 5=spi_current, 6=spi_prior, 7=spi_wow_delta
    value_rows = ", ".join(
        f"('{row[0]}', '{row[1]}', "
        f"{row[2]}, {row[3] if row[3] is not None else 'NULL'}, "
        f"{row[4] if row[4] is not None else 'NULL'}, "
        f"{row[5]}, {row[6] if row[6] is not None else 'NULL'}, "
        f"{row[7] if row[7] is not None else 'NULL'}, GETDATE())"
        for row in records
    )

    # Upsert EVM trends using temp table DELETE+INSERT on same connection
    hook.run(
        f"""
        CREATE TEMP TABLE tmp_evm_trends (LIKE finance.capex_evm_trends);

        INSERT INTO tmp_evm_trends
            (project_id, week_ending_date, cpi_current, cpi_prior,
             cpi_wow_delta, spi_current, spi_prior, spi_wow_delta, computed_at)
        VALUES {value_rows};

        DELETE FROM finance.capex_evm_trends
        USING tmp_evm_trends
        WHERE finance.capex_evm_trends.project_id        = tmp_evm_trends.project_id
          AND finance.capex_evm_trends.week_ending_date   = tmp_evm_trends.week_ending_date;

        INSERT INTO finance.capex_evm_trends
        SELECT * FROM tmp_evm_trends;
        """
    )


def refresh_board_view(**context: object) -> None:
    """Refresh the board-level CAPEX summary view ``finance.v_board_capex_summary``.

    The view exposes CPI and SPI alongside budget vs. actuals for the board
    reporting pack.  CREATE OR REPLACE is safe on Redshift views and incurs
    no table lock.
    """
    hook = get_redshift_hook()
    hook.run(_BOARD_VIEW_SQL)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_capex_tracking_redshift_weekly",
    description="Weekly full refresh of Snowflake FINANCE.CAPEX_TRACKING to Redshift with EVM trends",
    default_args=default_args,
    schedule_interval="0 19 * * 0",  # 19:00 UTC Sunday = 05:00 AEST Monday
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "finance", "weekly", "redshift"],
) as dag:

    # Step 1 – export full CAPEX snapshot from Snowflake to S3
    t_unload = PythonOperator(  # UNLOAD full FINANCE.CAPEX_TRACKING snapshot to S3 Parquet
        task_id="unload_capex",
        python_callable=unload_capex,
        sla=timedelta(hours=2),
        doc_md="Unload full CAPEX tracking dataset from Snowflake FINANCE.CAPEX_TRACKING to S3.",
    )

    # Step 2 – TRUNCATE + COPY into Redshift
    t_reload = PythonOperator(  # TRUNCATE and COPY full CAPEX snapshot into finance.capex_tracking
        task_id="reload_redshift",
        python_callable=reload_redshift,
        sla=timedelta(hours=3),
        doc_md="TRUNCATE and COPY full CAPEX snapshot into finance.capex_tracking in Redshift.",
    )

    # Step 3 – EVM trend computation
    t_evm = PythonOperator(  # Compute week-over-week CPI/SPI EVM trends, upsert into capex_evm_trends
        task_id="compute_evm_trends",
        python_callable=compute_evm_trends,
        sla=timedelta(hours=3, minutes=30),
        doc_md="Compute week-over-week CPI/SPI EVM trends and upsert into finance.capex_evm_trends.",
    )

    # Step 4 – refresh board summary view
    t_view = PythonOperator(  # CREATE OR REPLACE finance.v_board_capex_summary for board reporting
        task_id="refresh_board_view",
        python_callable=refresh_board_view,
        sla=timedelta(hours=4),
        doc_md="Recreate finance.v_board_capex_summary for board reporting.",
    )

    # Linear dependency chain
    t_unload >> t_reload >> t_evm >> t_view
