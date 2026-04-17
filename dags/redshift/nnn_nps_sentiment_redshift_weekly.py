"""
nnn_nps_sentiment_redshift_weekly
------------------------------------
Owner:      nnn-data-engineering
Domain:     Customer / NPS Sentiment
Schedule:   Wednesdays at 08:00 AEST / 22:00 UTC Tuesday (0 22 * * 2)
SLA:        3 hours

Synchronises Snowflake CUSTOMER.NPS_RESPONSES to the Redshift customer layer
(customer.nps_responses) weekly, computes the per-RSP NPS scorecard
(promoters% - detractors%), and publishes the scorecard CSV to S3 for RSP
distribution.

Steps:
  1. unload_nps_responses  — UNLOAD NPS responses for the 7-day window from Snowflake to S3
  2. copy_to_redshift      — COPY from S3 into customer.nps_responses in Redshift
  3. compute_nps_scorecard — compute per-RSP NPS score; upsert into customer.nps_weekly_scorecard
  4. publish_to_s3_csv     — UNLOAD weekly scorecard from Redshift to S3 CSV for RSP distribution

Upstream:   CUSTOMER.NPS_RESPONSES (Snowflake NPS survey pipeline)
Downstream: customer.nps_weekly_scorecard → RSP portal, finance reporting
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

_S3_PREFIX_TEMPLATE = "redshift-staging/nps_responses/run_date={run_date}/"
_S3_SCORECARD_PREFIX_TEMPLATE = "nps-scorecards/weekly/week_ending={run_date}/"


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def unload_nps_responses(**context: object) -> None:
    """Unload weekly NPS survey responses from Snowflake ``CUSTOMER.NPS_RESPONSES`` to S3.

    Selects all responses collected in the 7-day window ending on the run date,
    providing a consistent weekly cohort for NPS score computation.
    """
    run_date = get_run_date(context)
    s3_prefix = _S3_PREFIX_TEMPLATE.format(run_date=run_date)

    select_sql = f"""
        SELECT *
        FROM CUSTOMER.NPS_RESPONSES
        WHERE response_date > DATEADD('day', -7, '{run_date}')
          AND response_date <= '{run_date}'
    """
    snowflake_unload_to_s3(select_sql=select_sql, s3_prefix=s3_prefix)


def copy_to_redshift(**context: object) -> None:
    """COPY staged NPS response Parquet files from S3 into ``customer.nps_responses``.

    A DELETE for this week's response window is performed first so reruns are
    idempotent without wiping rows from other reporting weeks.
    """
    run_date = get_run_date(context)
    s3_prefix = _S3_PREFIX_TEMPLATE.format(run_date=run_date)

    # Remove the rolling 7-day window partition before re-loading (idempotent)
    hook = get_redshift_hook()
    hook.run(
        "DELETE FROM customer.nps_responses WHERE response_date > DATEADD('day', -7, %s) AND response_date <= %s",
        parameters=(run_date, run_date),
    )

    redshift_copy_from_s3(
        table="customer.nps_responses",
        s3_prefix=s3_prefix,
        file_format="PARQUET",
        truncate=False,  # do not wipe other weeks' data
    )


def compute_nps_scorecard(**context: object) -> None:
    """Compute per-RSP NPS scorecard for the current week and upsert into Redshift.

    Uses a single Redshift hook to:
    1. Run a window-function query that categorises responses into promoter /
       passive / detractor buckets per RSP.
    2. Compute the NPS score (promoters% - detractors%) per RSP.
    3. Upsert results into ``customer.nps_weekly_scorecard`` via DELETE+INSERT
       temp table pattern.
    """
    run_date = get_run_date(context)
    hook = get_redshift_hook()

    # PERF: use get_records() (returns a list of tuples) instead of get_pandas_df()
    # which loads all rows into a DataFrame. Result is bounded to one row per RSP.
    records = hook.get_records(
        f"""
        WITH categorised AS (
            SELECT
                rsp_id,
                '{run_date}'                                   AS week_ending_date,
                CASE
                    WHEN nps_score >= 9 THEN 'promoter'
                    WHEN nps_score >= 7 THEN 'passive'
                    ELSE 'detractor'
                END                                            AS category
            FROM customer.nps_responses
            WHERE response_date > DATEADD('day', -7, '{run_date}')
              AND response_date <= '{run_date}'
        ),
        counts AS (
            SELECT
                rsp_id,
                week_ending_date,
                COUNT(*)                                                      AS total_responses,
                SUM(CASE WHEN category = 'promoter'  THEN 1 ELSE 0 END)      AS promoter_count,
                SUM(CASE WHEN category = 'passive'   THEN 1 ELSE 0 END)      AS passive_count,
                SUM(CASE WHEN category = 'detractor' THEN 1 ELSE 0 END)      AS detractor_count
            FROM categorised
            GROUP BY rsp_id, week_ending_date
        )
        SELECT
            rsp_id,
            week_ending_date,
            total_responses,
            promoter_count,
            passive_count,
            detractor_count,
            ROUND(
                (promoter_count::FLOAT  / NULLIF(total_responses, 0)) * 100
              - (detractor_count::FLOAT / NULLIF(total_responses, 0)) * 100,
                2
            ) AS nps_score
        FROM counts
        """
    )

    if not records:
        return  # no responses this week — skip

    # row indices: 0=rsp_id, 1=week_ending_date, 2=total_responses, 3=promoter_count,
    #              4=passive_count, 5=detractor_count, 6=nps_score
    value_rows = ", ".join(
        f"('{row[0]}', '{row[1]}', {row[2]}, "
        f"{row[3]}, {row[4]}, {row[5]}, "
        f"{row[6]}, GETDATE())"
        for row in records
    )

    # Upsert into weekly scorecard table on the same connection
    hook.run(
        f"""
        CREATE TEMP TABLE tmp_nps_scorecard (LIKE customer.nps_weekly_scorecard);

        INSERT INTO tmp_nps_scorecard
            (rsp_id, week_ending_date, total_responses, promoter_count,
             passive_count, detractor_count, nps_score, computed_at)
        VALUES {value_rows};

        DELETE FROM customer.nps_weekly_scorecard
        USING tmp_nps_scorecard
        WHERE customer.nps_weekly_scorecard.rsp_id           = tmp_nps_scorecard.rsp_id
          AND customer.nps_weekly_scorecard.week_ending_date  = tmp_nps_scorecard.week_ending_date;

        INSERT INTO customer.nps_weekly_scorecard
        SELECT * FROM tmp_nps_scorecard;
        """
    )


def publish_to_s3_csv(**context: object) -> None:
    """Unload the weekly NPS scorecard from Redshift to S3 as CSV for RSP distribution.

    Uses ``redshift_unload_to_s3`` to export ``customer.nps_weekly_scorecard``
    rows for the current week to a dated S3 prefix.  The CSV files are consumed
    by the RSP portal and finance reporting systems.
    """
    run_date = get_run_date(context)
    s3_prefix = _S3_SCORECARD_PREFIX_TEMPLATE.format(run_date=run_date)

    select_sql = f"""
        SELECT
            rsp_id,
            week_ending_date,
            total_responses,
            promoter_count,
            passive_count,
            detractor_count,
            nps_score,
            computed_at
        FROM customer.nps_weekly_scorecard
        WHERE week_ending_date = '{run_date}'
        ORDER BY nps_score DESC
    """
    redshift_unload_to_s3(select_sql=select_sql, s3_prefix=s3_prefix)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_nps_sentiment_redshift_weekly",
    description="Weekly NPS responses sync from Snowflake to Redshift; scorecard + CSV export",
    default_args=default_args,
    schedule_interval="0 22 * * 2",  # 22:00 UTC Tuesday = 08:00 AEST Wednesday
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "customer", "weekly", "redshift"],
) as dag:

    # Step 1 – export NPS responses from Snowflake to S3
    t_unload = PythonOperator(  # Unload CUSTOMER.NPS_RESPONSES 7-day window to S3 Parquet
        task_id="unload_nps_responses",
        python_callable=unload_nps_responses,
        sla=timedelta(hours=1),
        doc_md="Unload weekly NPS responses from Snowflake CUSTOMER.NPS_RESPONSES to S3.",
    )

    # Step 2 – COPY from S3 into Redshift customer schema
    t_copy = PythonOperator(  # COPY staged NPS response Parquet files into customer.nps_responses
        task_id="copy_to_redshift",
        python_callable=copy_to_redshift,
        sla=timedelta(hours=1, minutes=30),
        doc_md="COPY staged NPS response Parquet files into customer.nps_responses in Redshift.",
    )

    # Step 3 – compute and upsert per-RSP NPS scorecard
    t_scorecard = PythonOperator(  # Compute per-RSP NPS score (promoters% - detractors%); upsert scorecard
        task_id="compute_nps_scorecard",
        python_callable=compute_nps_scorecard,
        sla=timedelta(hours=2),
        doc_md="Compute per-RSP NPS scorecard and upsert into customer.nps_weekly_scorecard.",
    )

    # Step 4 – publish scorecard CSV to S3 for RSP distribution
    t_publish = PythonOperator(  # Unload weekly NPS scorecard from Redshift to S3 CSV for RSP distribution
        task_id="publish_to_s3_csv",
        python_callable=publish_to_s3_csv,
        sla=timedelta(hours=3),
        doc_md="Unload weekly NPS scorecard from Redshift to S3 CSV for RSP distribution.",
    )

    # Linear dependency chain
    t_unload >> t_copy >> t_scorecard >> t_publish
