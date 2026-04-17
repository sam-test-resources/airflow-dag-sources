"""
nnn_poi_capacity_redshift_weekly
----------------------------------
Owner:      nnn-data-engineering
Domain:     Network / POI Capacity Planning
Schedule:   Tuesdays at 06:00 AEST / 20:00 UTC Monday (0 20 * * 1)
SLA:        4 hours

Synchronises Snowflake NETWORK.POI_TRAFFIC_WEEKLY to the Redshift capacity
planning layer (capacity_planning.poi_traffic_weekly), computes rolling
13-week utilisation trends, flags at-risk POIs (trend=UP, utilisation > 60%),
and exports a capacity report CSV to S3 for the capacity planning team.

Steps:
  1. unload_poi_traffic      — UNLOAD weekly POI traffic data from Snowflake to S3
  2. copy_to_redshift        — COPY from S3 into capacity_planning.poi_traffic_weekly
  3. compute_capacity_trends — compute rolling 13-week utilisation trends; upsert into poi_utilisation_trends
  4. flag_at_risk_pois       — insert at-risk POIs into capacity_planning.uplift_recommendations
  5. export_capacity_report  — UNLOAD at-risk POI report from Redshift to S3 CSV

Upstream:   NETWORK.POI_TRAFFIC_WEEKLY (Snowflake weekly POI aggregation)
Downstream: capacity_planning.uplift_recommendations → capacity planning team
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

_S3_PREFIX_TEMPLATE = "redshift-staging/poi_traffic_weekly/run_date={run_date}/"
_S3_CAPACITY_REPORT_TEMPLATE = "capacity-planning/poi-at-risk/week_ending={run_date}/"

# Utilisation threshold above which a POI with an upward trend is at risk
_UTILISATION_AT_RISK_PCT = 60.0
# Rolling window in weeks for trend computation
_ROLLING_WEEKS = 13


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def unload_poi_traffic(**context: object) -> None:
    """Unload weekly POI traffic data from Snowflake ``NETWORK.POI_TRAFFIC_WEEKLY`` to S3.

    Selects POI traffic aggregates for the week ending on the run date.
    Data is staged as Parquet files under a run-date-partitioned S3 prefix.
    """
    run_date = get_run_date(context)
    s3_prefix = _S3_PREFIX_TEMPLATE.format(run_date=run_date)

    select_sql = f"""
        SELECT *
        FROM NETWORK.POI_TRAFFIC_WEEKLY
        WHERE week_ending_date = '{run_date}'
    """
    snowflake_unload_to_s3(select_sql=select_sql, s3_prefix=s3_prefix)


def copy_to_redshift(**context: object) -> None:
    """COPY staged POI traffic Parquet files from S3 into ``capacity_planning.poi_traffic_weekly``.

    A DELETE for this week's week_ending_date is performed first so reruns are
    idempotent without wiping rows from other reporting weeks.
    """
    run_date = get_run_date(context)
    s3_prefix = _S3_PREFIX_TEMPLATE.format(run_date=run_date)

    # Remove only this week's partition before re-loading (idempotent)
    hook = get_redshift_hook()
    hook.run(
        "DELETE FROM capacity_planning.poi_traffic_weekly WHERE week_ending_date = %s",
        parameters=(run_date,),
    )

    redshift_copy_from_s3(
        table="capacity_planning.poi_traffic_weekly",
        s3_prefix=s3_prefix,
        file_format="PARQUET",
        truncate=False,  # do not wipe other weeks' data
    )


def compute_capacity_trends(**context: object) -> None:
    """Compute rolling 13-week utilisation trends per POI and persist to Redshift.

    Uses Redshift window functions to derive:
    - ``avg_utilisation_13w``: rolling 13-week average utilisation percentage.
    - ``trend_direction``: 'UP' if the 4-week avg > 9-week avg, else 'DOWN'.

    Results are upserted into ``capacity_planning.poi_utilisation_trends``
    using a DELETE+INSERT temp table on the same hook connection.
    """
    run_date = get_run_date(context)
    hook = get_redshift_hook()

    # PERF: use get_records() (returns a list of tuples) instead of get_pandas_df()
    # which loads all rows into a DataFrame. Result is bounded to one row per POI per week.
    records = hook.get_records(
        f"""
        WITH windowed AS (
            SELECT
                poi_id,
                poi_name,
                region,
                week_ending_date,
                avg_utilisation_pct,
                AVG(avg_utilisation_pct) OVER (
                    PARTITION BY poi_id
                    ORDER BY week_ending_date
                    ROWS BETWEEN {_ROLLING_WEEKS - 1} PRECEDING AND CURRENT ROW
                ) AS avg_utilisation_13w,
                AVG(avg_utilisation_pct) OVER (
                    PARTITION BY poi_id
                    ORDER BY week_ending_date
                    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                ) AS avg_utilisation_4w,
                AVG(avg_utilisation_pct) OVER (
                    PARTITION BY poi_id
                    ORDER BY week_ending_date
                    ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
                ) AS avg_utilisation_9w
            FROM capacity_planning.poi_traffic_weekly
        )
        SELECT
            poi_id,
            poi_name,
            region,
            week_ending_date,
            avg_utilisation_pct,
            ROUND(avg_utilisation_13w, 4) AS avg_utilisation_13w,
            CASE
                WHEN avg_utilisation_4w > avg_utilisation_9w THEN 'UP'
                ELSE 'DOWN'
            END AS trend_direction
        FROM windowed
        WHERE week_ending_date = '{run_date}'
        """
    )

    if not records:
        return  # no data for this week — nothing to persist

    # row indices: 0=poi_id, 1=poi_name, 2=region, 3=week_ending_date,
    #              4=avg_utilisation_pct, 5=avg_utilisation_13w, 6=trend_direction
    value_rows = ", ".join(
        f"('{row[0]}', '{row[1]}', '{row[2]}', "
        f"'{row[3]}', {row[4]}, "
        f"{row[5]}, '{row[6]}', GETDATE())"
        for row in records
    )

    # Upsert trends via DELETE+INSERT on the same connection
    hook.run(
        f"""
        CREATE TEMP TABLE tmp_poi_trends (LIKE capacity_planning.poi_utilisation_trends);

        INSERT INTO tmp_poi_trends
            (poi_id, poi_name, region, week_ending_date,
             avg_utilisation_pct, avg_utilisation_13w, trend_direction, computed_at)
        VALUES {value_rows};

        DELETE FROM capacity_planning.poi_utilisation_trends
        USING tmp_poi_trends
        WHERE capacity_planning.poi_utilisation_trends.poi_id            = tmp_poi_trends.poi_id
          AND capacity_planning.poi_utilisation_trends.week_ending_date   = tmp_poi_trends.week_ending_date;

        INSERT INTO capacity_planning.poi_utilisation_trends
        SELECT * FROM tmp_poi_trends;
        """
    )

    # Push trend records as JSON for downstream at-risk flagging (bounded to POI count — safe for XCom)
    import json as _json
    trend_list = [
        {"poi_id": r[0], "poi_name": r[1], "region": r[2], "week_ending_date": str(r[3]),
         "avg_utilisation_pct": r[4], "avg_utilisation_13w": r[5], "trend_direction": r[6]}
        for r in records
    ]
    context["ti"].xcom_push(key="trend_df_json", value=_json.dumps(trend_list))


def flag_at_risk_pois(**context: object) -> None:
    """Insert at-risk POIs (trend UP and avg_utilisation_pct > 60) into uplift recommendations.

    Reads the trend computation results from XCom, filters for at-risk POIs,
    and upserts them into ``capacity_planning.uplift_recommendations`` using
    the same hook connection to avoid a second Redshift connection.
    """
    import json

    run_date = get_run_date(context)
    trend_json = context["ti"].xcom_pull(
        task_ids="compute_capacity_trends", key="trend_df_json"
    )

    if not trend_json:
        return  # no trends computed — skip

    # PERF: parse JSON list of dicts directly — no pandas needed for this filter+build step
    all_trends = json.loads(trend_json)
    at_risk = [
        r for r in all_trends
        if r["trend_direction"] == "UP" and r["avg_utilisation_pct"] > _UTILISATION_AT_RISK_PCT
    ]

    if not at_risk:
        return  # no at-risk POIs this week

    hook = get_redshift_hook()
    value_rows = ", ".join(
        f"('{row[\"poi_id\"]}', '{row[\"poi_name\"]}', '{row[\"region\"]}', "
        f"'{row[\"week_ending_date\"]}', {row[\"avg_utilisation_pct\"]}, "
        f"{row[\"avg_utilisation_13w\"]}, 'PENDING', GETDATE())"
        for row in at_risk
    )

    # Upsert uplift recommendations — one record per POI per week
    hook.run(
        f"""
        CREATE TEMP TABLE tmp_uplift (LIKE capacity_planning.uplift_recommendations);

        INSERT INTO tmp_uplift
            (poi_id, poi_name, region, week_ending_date,
             avg_utilisation_pct, avg_utilisation_13w, recommendation_status, created_at)
        VALUES {value_rows};

        DELETE FROM capacity_planning.uplift_recommendations
        USING tmp_uplift
        WHERE capacity_planning.uplift_recommendations.poi_id           = tmp_uplift.poi_id
          AND capacity_planning.uplift_recommendations.week_ending_date  = tmp_uplift.week_ending_date;

        INSERT INTO capacity_planning.uplift_recommendations
        SELECT * FROM tmp_uplift;
        """
    )


def export_capacity_report(**context: object) -> None:
    """Export at-risk POI data from Redshift to S3 CSV for the capacity planning team.

    Unloads all POIs flagged as at-risk (pending uplift recommendation) for the
    current week from ``capacity_planning.uplift_recommendations`` to a
    run-date-partitioned S3 path as CSV.
    """
    run_date = get_run_date(context)
    s3_prefix = _S3_CAPACITY_REPORT_TEMPLATE.format(run_date=run_date)

    select_sql = f"""
        SELECT
            poi_id,
            poi_name,
            region,
            week_ending_date,
            avg_utilisation_pct,
            avg_utilisation_13w,
            recommendation_status,
            created_at
        FROM capacity_planning.uplift_recommendations
        WHERE week_ending_date = '{run_date}'
          AND recommendation_status = 'PENDING'
        ORDER BY avg_utilisation_pct DESC
    """
    redshift_unload_to_s3(select_sql=select_sql, s3_prefix=s3_prefix)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_poi_capacity_redshift_weekly",
    description="Weekly POI traffic sync from Snowflake to Redshift; capacity trend and at-risk export",
    default_args=default_args,
    schedule_interval="0 20 * * 1",  # 20:00 UTC Monday = 06:00 AEST Tuesday
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "network", "weekly", "redshift"],
) as dag:

    # Step 1 – export POI traffic from Snowflake to S3
    t_unload = PythonOperator(  # Unload NETWORK.POI_TRAFFIC_WEEKLY for week_ending_date to S3 Parquet
        task_id="unload_poi_traffic",
        python_callable=unload_poi_traffic,
        sla=timedelta(hours=1),
        doc_md="Unload weekly POI traffic from Snowflake NETWORK.POI_TRAFFIC_WEEKLY to S3.",
    )

    # Step 2 – COPY from S3 into Redshift capacity_planning schema
    t_copy = PythonOperator(  # COPY staged POI traffic Parquet files into capacity_planning.poi_traffic_weekly
        task_id="copy_to_redshift",
        python_callable=copy_to_redshift,
        sla=timedelta(hours=2),
        doc_md="COPY staged POI traffic Parquet files into capacity_planning.poi_traffic_weekly.",
    )

    # Step 3 – compute rolling 13-week trends using Redshift window functions
    t_trends = PythonOperator(  # Compute rolling 13-week utilisation trends; upsert into poi_utilisation_trends
        task_id="compute_capacity_trends",
        python_callable=compute_capacity_trends,
        sla=timedelta(hours=3),
        doc_md="Compute rolling 13-week utilisation trends per POI; upsert into poi_utilisation_trends.",
    )

    # Step 4 – flag at-risk POIs and write uplift recommendations
    t_flag = PythonOperator(  # Insert POIs with trend=UP and utilisation > 60% into uplift_recommendations
        task_id="flag_at_risk_pois",
        python_callable=flag_at_risk_pois,
        sla=timedelta(hours=3, minutes=30),
        doc_md="Flag POIs with upward trend and >60% utilisation into uplift_recommendations.",
    )

    # Step 5 – export at-risk POI report CSV to S3
    t_export = PythonOperator(  # Unload at-risk POI capacity report from Redshift to S3 CSV
        task_id="export_capacity_report",
        python_callable=export_capacity_report,
        sla=timedelta(hours=4),
        doc_md="Unload at-risk POI capacity report from Redshift to S3 CSV.",
    )

    # Linear dependency chain
    t_unload >> t_copy >> t_trends >> t_flag >> t_export
