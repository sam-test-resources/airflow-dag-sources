"""
NNN Airflow DAG — GraphQL Partner Portal Activity (Daily)
=========================================================
Owner:      nnn-data-engineering
Domain:     Wholesale / Partner Management
Schedule:   0 4 * * *  (4 AM AEST daily)
SLA:        2 hours (must complete by 6 AM AEST)
Description:
    Fetches partner activity metrics from the NNN Partner Portal GraphQL API
    (served under the NMS API connection, endpoint /graphql). Retrieves
    per-partner activity summaries for the previous day including event type
    breakdowns, counts, and average durations. Loads into Snowflake using a
    delete-insert pattern to ensure idempotency.

Steps:
    1. fetch_graphql_activity  — POST GraphQL query, parse JSON, push to XCom
    2. load_to_snowflake       — DELETE for run_date, then batch INSERT
    3. validate                — assert row count >= 1 for run_date

Upstream:   Partner Portal GraphQL API (CONN_NMS_API at /graphql)
Downstream: WHOLESALE.PARTNER_PORTAL_ACTIVITY → partner analytics, SLA reporting
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_NMS_API,
    assert_row_count,
    get_run_date,
    get_snowflake_hook,
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
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": nnn_failure_alert,
    "execution_timeout": timedelta(minutes=30),
}

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TARGET_TABLE      = "WHOLESALE.PARTNER_PORTAL_ACTIVITY"
GRAPHQL_ENDPOINT  = "/graphql"

# GraphQL query — parameterised by date variable
GRAPHQL_QUERY = """
query GetPartnerActivity($date: String!) {
  partnerActivity(date: $date) {
    partnerId
    activityType
    count
    avgDuration
    totalDuration
    firstEventTs
    lastEventTs
  }
}
"""


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def fetch_graphql_activity(**context) -> int:
    """POST GraphQL query to Partner Portal API and push activity records to XCom."""
    from airflow.providers.http.hooks.http import HttpHook

    run_date: str = get_run_date(context)

    hook = HttpHook(method="POST", http_conn_id=CONN_NMS_API)

    # GraphQL request body with variables — cleaner than string interpolation in query
    request_body = {
        "query": GRAPHQL_QUERY,
        "variables": {"date": run_date},
    }

    response = hook.run(
        endpoint=GRAPHQL_ENDPOINT,
        json=request_body,
        headers={"Content-Type": "application/json"},
    )

    payload = response.json()

    # GraphQL errors are returned in the response body, not HTTP status codes
    if "errors" in payload:
        error_messages = "; ".join(e.get("message", "unknown") for e in payload["errors"])
        raise ValueError(f"GraphQL API returned errors for {run_date}: {error_messages}")

    activity_data = payload.get("data", {}).get("partnerActivity", [])

    if not activity_data:
        raise ValueError(
            f"GraphQL partnerActivity returned no records for {run_date}. "
            "Verify partner portal API data availability."
        )

    # Attach run_date to each record for Snowflake partitioning
    records = [
        {
            "run_date": run_date,
            "partner_id": item.get("partnerId"),
            "activity_type": item.get("activityType"),
            "event_count": item.get("count"),
            "avg_duration": item.get("avgDuration"),
            "total_duration": item.get("totalDuration"),
            "first_event_ts": item.get("firstEventTs"),
            "last_event_ts": item.get("lastEventTs"),
        }
        for item in activity_data
    ]

    # PERF: partner activity records can number in the thousands — write to temp
    # file and push only the file path to avoid bloating the XCom metadata DB.
    import json
    import tempfile

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", prefix=f"graphql_activity_{run_date}_", delete=False
    ) as tmp:
        json.dump(records, tmp)
        tmp_path = tmp.name

    context["ti"].xcom_push(key="records_path", value=tmp_path)
    return len(records)


def load_to_snowflake(**context) -> None:
    """Delete existing rows for run_date then INSERT fresh partner activity records.

    Delete-insert is preferred over MERGE here because the entire day's
    activity is re-fetched as a complete snapshot from the API.
    """
    import json
    import os as _os

    ti = context["ti"]
    run_date: str = get_run_date(context)
    records_path: str = ti.xcom_pull(task_ids="fetch_graphql_activity", key="records_path")
    with open(records_path) as fh:
        records: list[dict] = json.load(fh)

    hook = get_snowflake_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Remove any existing rows for this run date (idempotent reload)
        cursor.execute(
            f"DELETE FROM {TARGET_TABLE} WHERE run_date = %(run_date)s",
            {"run_date": run_date},
        )

        # Batch INSERT all activity records
        insert_sql = f"""
            INSERT INTO {TARGET_TABLE} (
                run_date, partner_id, activity_type, event_count,
                avg_duration, total_duration, first_event_ts, last_event_ts, loaded_at
            ) VALUES (
                %(run_date)s, %(partner_id)s, %(activity_type)s, %(event_count)s,
                %(avg_duration)s, %(total_duration)s,
                TRY_TO_TIMESTAMP_NTZ(%(first_event_ts)s),
                TRY_TO_TIMESTAMP_NTZ(%(last_event_ts)s),
                CURRENT_TIMESTAMP()
            )
        """
        rows = [
            {
                "run_date": r["run_date"],
                "partner_id": r["partner_id"],
                "activity_type": r["activity_type"],
                "event_count": r["event_count"],
                "avg_duration": r["avg_duration"],
                "total_duration": r["total_duration"],
                "first_event_ts": r["first_event_ts"],
                "last_event_ts": r["last_event_ts"],
            }
            for r in records
        ]
        cursor.executemany(insert_sql, rows)
        conn.commit()
    finally:
        cursor.close()
        conn.close()
    _os.remove(records_path)


def validate_row_count(**context) -> None:
    """Assert at least one partner activity row exists for run_date."""
    run_date = get_run_date(context)
    assert_row_count(TARGET_TABLE, run_date, min_rows=1)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_graphql_partner_portal_daily",
    default_args=default_args,
    description="Daily GraphQL partner portal activity → Snowflake WHOLESALE.PARTNER_PORTAL_ACTIVITY",
    schedule_interval="0 4 * * *",  # 4 AM AEST
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "wholesale", "partner", "graphql", "snowflake", "daily"],
) as dag:

    # Step 1: Fetch partner activity from the GraphQL API
    t_fetch = PythonOperator(  # POST GraphQL query, parse JSON, push activity records to XCom
        task_id="fetch_graphql_activity",
        python_callable=fetch_graphql_activity,
        sla=timedelta(hours=2),
    )

    # Step 2: Delete existing rows for run_date then INSERT fresh records
    t_load = PythonOperator(  # DELETE for run_date then batch INSERT partner activity records
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
    )

    # Step 3: Verify at least one row was written for the run date
    t_validate = PythonOperator(  # Assert >= 1 row in WHOLESALE.PARTNER_PORTAL_ACTIVITY for run_date
        task_id="validate_row_count",
        python_callable=validate_row_count,
    )

    t_fetch >> t_load >> t_validate
