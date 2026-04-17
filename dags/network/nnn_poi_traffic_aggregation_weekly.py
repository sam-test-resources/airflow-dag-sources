"""
nnn_poi_traffic_aggregation_weekly
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Owner:      nnn-data-engineering
Domain:     Network
Schedule:   Monday 03:00 AEST (0 17 * * 0  UTC Sunday)
SLA:        6 hours

Rolls up 7 days of hourly CVC utilisation into weekly POI-level traffic
statistics used by the Capacity Planning team to trigger CVC uplift requests.

Outputs:
  - Snowflake: NETWORK.POI_TRAFFIC_WEEKLY  (primary target)
  - S3: nnn-data-lake-prod/network/poi_traffic_weekly/run_date=YYYY-MM-DD/report.csv
        (consumed by PowerBI capacity planning dashboard)

Metrics produced per POI per week:
  - Peak throughput Gbps (P95 across all hours)
  - Average utilisation %
  - Hours in congestion (utilisation > 80%)
  - Max sustained congestion window (consecutive congested hours)
  - Recommended CVC uplift flag (triggered at >65% avg or >8 congested hours/week)

Steps:
  1. Aggregate Snowflake source into temp table (SQL only — no Python memory needed)
  2. Compute uplift recommendations via Python
  3. MERGE aggregated rows into NETWORK.POI_TRAFFIC_WEEKLY
  4. Export CSV to S3 for BI consumption
  5. Log run summary
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_SNOWFLAKE, get_run_date, get_snowflake_hook,
    NNN_S3_BUCKET, upload_to_s3,
)

log = logging.getLogger(__name__)

default_args = {
    "owner":                    "nnn-data-engineering",
    "depends_on_past":          True,          # weekly stats are sequential
    "email":                    ["de-alerts@nnnco.com.au", "capacity-planning@nnnco.com.au"],
    "email_on_failure":         True,
    "email_on_retry":           False,
    "retries":                  2,
    "retry_delay":              timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay":          timedelta(minutes=60),
    "on_failure_callback":      nnn_failure_alert,
    "execution_timeout":        timedelta(hours=3),
}

# ── SQL: weekly aggregation from hourly source ─────────────────────────────────

SQL_WEEKLY_AGG = """
CREATE OR REPLACE TEMPORARY TABLE tmp_poi_weekly AS
SELECT
    poi_id,
    DATE_TRUNC('week', hour_label::DATE)                   AS week_start,
    MAX(throughput_gbps)                                   AS peak_throughput_gbps,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY throughput_gbps)
                                                           AS p95_throughput_gbps,
    AVG(utilisation_pct)                                   AS avg_utilisation_pct,
    SUM(CASE WHEN congestion_flag THEN 1 ELSE 0 END)       AS hours_in_congestion,
    MAX(contracted_gbps)                                   AS contracted_gbps
FROM NETWORK.CVC_UTILISATION_HOURLY
WHERE hour_label >= DATEADD(day, -7, '{{ ds }}'::DATE)
  AND hour_label <  '{{ ds }}'::DATE
GROUP BY 1, 2
"""

SQL_MERGE = """
MERGE INTO NETWORK.POI_TRAFFIC_WEEKLY tgt
USING tmp_poi_weekly src
  ON  tgt.poi_id     = src.poi_id
  AND tgt.week_start = src.week_start
WHEN MATCHED    THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT   *
"""


def aggregate_uplift_and_merge(**context) -> None:
    """Aggregate weekly metrics, compute uplift recommendations, and merge to target.

    Runs the full aggregate → uplift → merge pipeline in a SINGLE Snowflake
    session so that the TEMPORARY TABLE created by SQL_WEEKLY_AGG is visible
    to the subsequent uplift read and SQL_MERGE steps.
    Snowflake TEMPORARY TABLEs are session-scoped; using separate tasks (separate
    sessions) would make the temp table invisible to downstream operators.
    """
    run_date = get_run_date(context)
    hook     = get_snowflake_hook()

    # ── Step 1: aggregate into temp table (same session kept open by the hook) ──
    # Render the {{ ds }} Jinja placeholder manually since we are in a PythonOperator
    agg_sql = SQL_WEEKLY_AGG.replace("{{ ds }}", run_date)
    merge_sql = SQL_MERGE  # no Jinja placeholders

    conn   = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(agg_sql)
        log.info("Weekly POI aggregation temp table created for %s", run_date)

        # ── Step 2: compute uplift from temp table (same session) ──────────────
        cursor.execute(
            "SELECT poi_id, week_start, avg_utilisation_pct, "
            "hours_in_congestion, contracted_gbps FROM tmp_poi_weekly"
        )
        rows = cursor.fetchall()
        cols = [d[0].lower() for d in cursor.description]
        df   = __import__("pandas").DataFrame(rows, columns=cols)

        # Uplift rule: avg util > 65%  OR  ≥ 8 congested hours in the week
        df["uplift_recommended"] = (
            (df["avg_utilisation_pct"] > 65) | (df["hours_in_congestion"] >= 8)
        )
        df["recommended_uplift_gbps"] = df.apply(
            lambda r: round(r["contracted_gbps"] * 0.5, 0) if r["uplift_recommended"] else 0,
            axis=1,
        )
        uplift_pois = df[df["uplift_recommended"]]
        log.warning("Uplift recommended for %d POIs this week", len(uplift_pois))

        if not uplift_pois.empty:
            for _, row in uplift_pois.iterrows():
                cursor.execute(
                    "INSERT INTO NETWORK.CVC_UPLIFT_SIGNALS "
                    "(poi_id, week_start, avg_utilisation_pct, hours_in_congestion, recommended_uplift_gbps) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (row["poi_id"], str(row["week_start"]),
                     float(row["avg_utilisation_pct"]), int(row["hours_in_congestion"]),
                     float(row["recommended_uplift_gbps"])),
                )

        # ── Step 3: merge tmp_poi_weekly → permanent target (same session) ─────
        cursor.execute(merge_sql)
        conn.commit()
        log.info("MERGE complete: %d POI-week rows into NETWORK.POI_TRAFFIC_WEEKLY", len(df))

    finally:
        cursor.close()
        conn.close()

    context["ti"].xcom_push(key="uplift_count", value=len(uplift_pois))


def export_csv_to_s3(**context) -> None:
    """Write weekly POI traffic CSV to S3 for BI dashboard consumption."""
    import tempfile
    run_date = get_run_date(context)
    hook     = get_snowflake_hook()

    df = hook.get_pandas_df("""
        SELECT p.poi_id, r.poi_name, r.state_territory,
               p.week_start, p.peak_throughput_gbps, p.p95_throughput_gbps,
               p.avg_utilisation_pct, p.hours_in_congestion, p.contracted_gbps
        FROM   NETWORK.POI_TRAFFIC_WEEKLY p
        JOIN   NETWORK.POI_REGISTRY       r ON r.poi_id = p.poi_id
        WHERE  p.week_start = DATEADD(day, -7, %(run_date)s::DATE)
        ORDER  BY p.avg_utilisation_pct DESC
    """, parameters={"run_date": run_date})

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
        df.to_csv(tmp, index=False)
        tmp_path = tmp.name

    s3_key_path = f"network/poi_traffic_weekly/run_date={run_date}/report.csv"
    upload_to_s3(tmp_path, s3_key_path)
    log.info("Exported %d POI rows to S3", len(df))


with DAG(
    dag_id="nnn_poi_traffic_aggregation_weekly",
    description="Weekly POI-level CVC traffic rollup with uplift recommendations",
    schedule_interval="0 17 * * 0",     # Sunday 17:00 UTC = Monday 03:00 AEST
    start_date=datetime(2024, 1, 7),    # first Sunday
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "network", "weekly", "capacity-planning"],
) as dag:

    # aggregate + uplift calc + merge run in a SINGLE task to share one Snowflake
    # session — Snowflake TEMPORARY TABLEs are session-scoped and cannot cross task
    # boundaries (each task is a separate process with its own connection).
    t_agg_uplift_merge = PythonOperator(
        task_id="aggregate_uplift_and_merge",
        python_callable=aggregate_uplift_and_merge,
        sla=timedelta(hours=4),
    )

    t_export = PythonOperator(
        task_id="export_csv_to_s3",
        python_callable=export_csv_to_s3,
        sla=timedelta(hours=5),
    )

    t_agg_uplift_merge >> t_export
