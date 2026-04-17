"""
nnn_redis_session_analytics_daily
------------------------------------
Owner:      nnn-data-engineering
Domain:     ML / RSP Portal Analytics
Schedule:   Daily at 13:00 AEST (0 13 * * *)
SLA:        1 hour

Scans the Redis RSP portal session cache, extracts session metadata for sessions
created >= yesterday, loads them into Snowflake ML.RSP_SESSION_ANALYTICS, then
computes daily session KPIs into ML.SESSION_KPI_DAILY.

Steps:
  1. scan_redis_sessions   — SCAN keys matching 'session:*', HGETALL each, filter to
                             sessions created >= yesterday; write to temp file, push path
  2. load_to_snowflake     — DELETE existing rows for run_date, INSERT new session
                             records from temp file; skip if none
  3. compute_session_kpis  — run Snowflake SQL to aggregate KPIs into
                             ML.SESSION_KPI_DAILY for run_date

Upstream:   RSP portal session writes to Redis
Downstream: ML.SESSION_KPI_DAILY → ML churn models, portal analytics dashboard
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_REDIS,
    get_execution_date,
    get_run_date,
    get_snowflake_hook,
    snowflake_run,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner":                     "nnn-data-engineering",
    "depends_on_past":           False,
    "email":                     ["de-alerts@nnnco.com.au"],
    "email_on_failure":          True,
    "email_on_retry":            False,
    "retries":                   2,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":           timedelta(minutes=30),
    "on_failure_callback":       nnn_failure_alert,
    "execution_timeout":         timedelta(minutes=30),
}

SESSION_TABLE  = "ML.RSP_SESSION_ANALYTICS"
KPI_TABLE      = "ML.SESSION_KPI_DAILY"
SESSION_PREFIX = "session:*"


# ---------------------------------------------------------------------------
# Task functions (module-level so the scheduler does not re-define them on
# every DAG parse cycle)
# ---------------------------------------------------------------------------

def scan_redis_sessions(**context) -> None:
    """Scan Redis for session:* keys, HGETALL each, filter to sessions >= yesterday.

    PERF: sessions_found can be thousands of dicts — write to a temp JSON file and
    push only the path via XCom to avoid bloating the Airflow metadata DB.
    """
    from airflow.providers.redis.hooks.redis import RedisHook

    execution_dt = get_execution_date(context)
    run_date     = get_run_date(context)

    # Threshold: sessions created on or after yesterday
    yesterday_dt = execution_dt - timedelta(days=1)
    yesterday_ts = yesterday_dt.timestamp()

    hook   = RedisHook(redis_conn_id=CONN_REDIS)
    client = hook.get_conn()  # returns a redis.Redis client

    total_scanned  = 0
    sessions_found = []

    try:
        # PERF: scan_iter provides efficient cursor-based iteration — never use KEYS.
        for key in client.scan_iter(match=SESSION_PREFIX, count=100):
            total_scanned += 1

            try:
                # HGETALL returns dict[bytes, bytes] — decode to strings
                raw = client.hgetall(key)
                if not raw:
                    continue

                session = {
                    k.decode("utf-8"): v.decode("utf-8")
                    for k, v in raw.items()
                }

                # Filter to sessions created >= yesterday
                created_at_raw = session.get("created_at")
                if created_at_raw is None:
                    continue

                created_ts = float(created_at_raw)
                if created_ts < yesterday_ts:
                    continue

                # Enrich with computed fields
                session["session_key"]  = key.decode("utf-8") if isinstance(key, bytes) else key
                session["session_date"] = run_date
                session["extracted_at"] = datetime.now(tz=timezone.utc).isoformat()
                sessions_found.append(session)

            except Exception as exc:
                log.warning("Failed to process key %s: %s", key, exc)
    finally:
        client.close()  # Always release the Redis connection

    log.info(
        "Redis scan complete — scanned %d keys, captured %d sessions for %s",
        total_scanned, len(sessions_found), run_date,
    )

    # PERF: write payload to temp file; push only the path via XCom
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", prefix=f"redis_sessions_{run_date}_", delete=False
    ) as tmp:
        json.dump(sessions_found, tmp)
        tmp_path = tmp.name

    context["ti"].xcom_push(key="sessions_path",  value=tmp_path)
    context["ti"].xcom_push(key="total_scanned",  value=total_scanned)
    context["ti"].xcom_push(key="session_count",  value=len(sessions_found))


def load_to_snowflake(**context) -> None:
    """DELETE run_date rows then INSERT all Redis session records into Snowflake."""
    run_date      = get_run_date(context)
    sessions_path = context["ti"].xcom_pull(task_ids="scan_redis_sessions", key="sessions_path")

    with open(sessions_path) as fh:
        sessions = json.load(fh)

    if not sessions:
        log.info("No session records for %s — skipping load.", run_date)
        os.remove(sessions_path)
        return

    hook   = get_snowflake_hook()
    conn   = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Delete any existing rows for today's session_date (idempotent)
        cursor.execute(
            f"DELETE FROM {SESSION_TABLE} WHERE SESSION_DATE = %(run_date)s",
            {"run_date": run_date},
        )
        log.info("Deleted existing rows for session_date=%s from %s", run_date, SESSION_TABLE)

        # Snowflake connector uses named %(name)s param style, not positional %s.
        insert_sql = f"""
            INSERT INTO {SESSION_TABLE} (
                SESSION_KEY,
                SESSION_ID,
                RSP_ID,
                USER_ID,
                CREATED_AT,
                LAST_ACTIVE_AT,
                DURATION_SECONDS,
                PAGE_VIEWS,
                IS_BOUNCE,
                SESSION_DATE,
                EXTRACTED_AT
            )
            VALUES (
                %(session_key)s, %(session_id)s, %(rsp_id)s, %(user_id)s,
                %(created_at)s, %(last_active_at)s, %(duration_seconds)s,
                %(page_views)s, %(is_bounce)s, %(session_date)s, %(extracted_at)s
            )
        """
        rows = [
            {
                "session_key":      sess.get("session_key"),
                "session_id":       sess.get("session_id"),
                "rsp_id":           sess.get("rsp_id"),
                "user_id":          sess.get("user_id"),
                "created_at":       sess.get("created_at"),
                "last_active_at":   sess.get("last_active_at"),
                "duration_seconds": sess.get("duration_seconds"),
                "page_views":       sess.get("page_views"),
                "is_bounce":        sess.get("is_bounce", "false").lower() == "true",
                "session_date":     sess.get("session_date"),
                "extracted_at":     sess.get("extracted_at"),
            }
            for sess in sessions
        ]
        cursor.executemany(insert_sql, rows)
        conn.commit()
        log.info("Inserted %d session records into %s", len(sessions), SESSION_TABLE)
    finally:
        cursor.close()
        conn.close()

    os.remove(sessions_path)


def compute_session_kpis(**context) -> None:
    """Compute avg session duration, bounce rate, top RSPs by session count into KPI table."""
    run_date = get_run_date(context)

    # Merge-style upsert: delete existing KPI row then recompute from session data
    delete_sql = f"""
        DELETE FROM {KPI_TABLE}
        WHERE KPI_DATE = %(run_date)s
    """
    snowflake_run(delete_sql, parameters={"run_date": run_date})

    insert_kpi_sql = f"""
        INSERT INTO {KPI_TABLE} (
            KPI_DATE,
            TOTAL_SESSIONS,
            UNIQUE_RSPS,
            UNIQUE_USERS,
            AVG_SESSION_DURATION_SECONDS,
            BOUNCE_RATE_PCT,
            TOP_RSP_ID_1,
            TOP_RSP_SESSION_COUNT_1,
            TOP_RSP_ID_2,
            TOP_RSP_SESSION_COUNT_2,
            TOP_RSP_ID_3,
            TOP_RSP_SESSION_COUNT_3,
            COMPUTED_AT
        )
        WITH session_base AS (
            SELECT
                SESSION_DATE,
                RSP_ID,
                COUNT(*)                                            AS session_count,
                AVG(CAST(DURATION_SECONDS AS FLOAT))                AS avg_duration,
                SUM(CASE WHEN IS_BOUNCE THEN 1 ELSE 0 END)         AS bounce_count
            FROM {SESSION_TABLE}
            WHERE SESSION_DATE = %(run_date)s
            GROUP BY SESSION_DATE, RSP_ID
        ),
        totals AS (
            SELECT
                %(run_date)s                                          AS kpi_date,
                SUM(session_count)                                    AS total_sessions,
                COUNT(DISTINCT rsp_id)                                AS unique_rsps,
                (SELECT COUNT(DISTINCT USER_ID) FROM {SESSION_TABLE}
                 WHERE SESSION_DATE = %(run_date)s)                   AS unique_users,
                AVG(avg_duration)                                     AS avg_session_duration_seconds,
                ROUND(100.0 * SUM(bounce_count) / NULLIF(SUM(session_count), 0), 2)
                                                                      AS bounce_rate_pct
            FROM session_base
        ),
        ranked AS (
            SELECT
                RSP_ID,
                session_count,
                ROW_NUMBER() OVER (ORDER BY session_count DESC) AS rn
            FROM session_base
        )
        SELECT
            t.kpi_date,
            t.total_sessions,
            t.unique_rsps,
            t.unique_users,
            t.avg_session_duration_seconds,
            t.bounce_rate_pct,
            MAX(CASE WHEN r.rn = 1 THEN r.rsp_id END)           AS top_rsp_id_1,
            MAX(CASE WHEN r.rn = 1 THEN r.session_count END)     AS top_rsp_session_count_1,
            MAX(CASE WHEN r.rn = 2 THEN r.rsp_id END)           AS top_rsp_id_2,
            MAX(CASE WHEN r.rn = 2 THEN r.session_count END)     AS top_rsp_session_count_2,
            MAX(CASE WHEN r.rn = 3 THEN r.rsp_id END)           AS top_rsp_id_3,
            MAX(CASE WHEN r.rn = 3 THEN r.session_count END)     AS top_rsp_session_count_3,
            CURRENT_TIMESTAMP()                                  AS computed_at
        FROM totals t
        CROSS JOIN ranked r
        GROUP BY
            t.kpi_date, t.total_sessions, t.unique_rsps, t.unique_users,
            t.avg_session_duration_seconds, t.bounce_rate_pct
    """
    snowflake_run(insert_kpi_sql, parameters={"run_date": run_date})
    log.info("Session KPIs computed and inserted into %s for %s", KPI_TABLE, run_date)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_redis_session_analytics_daily",
    description="Extract RSP portal session data from Redis → Snowflake ML analytics",
    default_args=default_args,
    schedule_interval="0 13 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "ml", "redis", "snowflake", "session-analytics", "daily"],
) as dag:

    task_scan = PythonOperator(  # Cursor-scan Redis session:* keys, filter to yesterday's sessions
        task_id="scan_redis_sessions",
        python_callable=scan_redis_sessions,
    )

    task_load = PythonOperator(  # DELETE existing run_date rows then INSERT fresh session records
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
    )

    task_kpis = PythonOperator(  # Compute session KPIs (duration, bounce rate, top RSPs) into ML.SESSION_KPI_DAILY
        task_id="compute_session_kpis",
        python_callable=compute_session_kpis,
    )

    # ------------------------------------------------------------------
    # Dependency chain
    # ------------------------------------------------------------------
    task_scan >> task_load >> task_kpis
