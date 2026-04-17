"""
nnn_dynamodb_portal_sessions_daily
------------------------------------
Owner:      nnn-data-engineering
Domain:     Wholesale / RSP Portal Analytics
Schedule:   Daily at 2 AM AEST (0 2 * * *)
SLA:        1 hour

Description:
    Scans the DynamoDB table `nnn-rsp-portal-sessions` for all sessions with
    session_date matching the previous run date. Flattens the DynamoDB item
    format, saves as a JSON file to S3, then uses Redshift COPY to load into
    `analytics.rsp_portal_sessions`.

    DynamoDB Scan is paginated — the paginator pattern is used to handle
    large result sets without hitting the 1 MB per-request limit.

Steps:
    1. scan_dynamodb    — paginated Scan with FilterExpression, upload JSON to S3
    2. load_to_redshift — COPY from S3 JSON into analytics.rsp_portal_sessions
    3. validate         — assert_redshift_row_count for run_date

Upstream:   nnn-rsp-portal-sessions DynamoDB table (portal writes)
Downstream: analytics.rsp_portal_sessions → RSP engagement analytics
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_DYNAMODB,
    NNN_S3_BUCKET,
    assert_redshift_row_count,
    get_redshift_hook,
    get_run_date,
    redshift_copy_from_s3,
    s3_key,
    upload_to_s3,
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
    "execution_timeout": timedelta(minutes=45),  # Paginated scan can take time for large tables
}

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DYNAMO_TABLE_NAME = "nnn-rsp-portal-sessions"
REDSHIFT_TARGET   = "analytics.rsp_portal_sessions"


def _deserialize_dynamo_item(item: dict) -> dict:
    """Convert DynamoDB typed attribute map to a plain Python dict.

    DynamoDB items returned by boto3 in standard mode already have Python
    types when using the resource (not the client). When using the client
    paginator directly, types arrive as {"S": "value"} dicts. This helper
    handles both cases defensively.
    """
    flat: dict = {}
    for key, value in item.items():
        if isinstance(value, dict):
            # Client-style: {"S": "...", "N": "...", "BOOL": True, ...}
            for dtype, val in value.items():
                if dtype == "N":
                    flat[key] = float(val) if "." in str(val) else int(val)
                elif dtype == "BOOL":
                    flat[key] = bool(val)
                elif dtype == "NULL":
                    flat[key] = None
                else:
                    flat[key] = val
                break
        else:
            flat[key] = value
    return flat


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def scan_dynamodb(**context) -> str:
    """Paginate DynamoDB Scan filtered to run_date, serialise to JSON, upload to S3.

    Returns the S3 key path (pushed to XCom implicitly via return value).
    """
    from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook

    run_date: str = get_run_date(context)

    hook = DynamoDBHook(
        aws_conn_id=CONN_DYNAMODB,
        table_name=DYNAMO_TABLE_NAME,
        table_keys=["session_id"],  # partition key
        region_name=None,  # inherits from connection
    )

    # Use boto3 client paginator for robust handling of large tables
    client = hook.conn.meta.client  # DynamoDBHook exposes the boto3 resource

    paginator = client.get_paginator("scan")

    page_iterator = paginator.paginate(
        TableName=DYNAMO_TABLE_NAME,
        FilterExpression="session_date = :rd",
        ExpressionAttributeValues={":rd": {"S": run_date}},
    )

    all_items: list[dict] = []
    for page in page_iterator:
        for raw_item in page.get("Items", []):
            all_items.append(_deserialize_dynamo_item(raw_item))

    if not all_items:
        raise ValueError(
            f"No portal sessions found in DynamoDB for session_date={run_date}. "
            "This may indicate a portal outage or upstream delivery failure."
        )

    # Write items as newline-delimited JSON (Redshift JSON COPY format)
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", prefix=f"portal_sessions_{run_date}_", delete=False
    ) as tmp_file:
        for item in all_items:
            tmp_file.write(json.dumps(item) + "\n")
        local_path = tmp_file.name

    try:
        dest_key = s3_key("analytics/rsp_portal_sessions", "sessions", run_date, ext="json")
        upload_to_s3(local_path, dest_key)   # returns full s3:// URI; we keep the relative key
    finally:
        os.remove(local_path)

    # Push the RELATIVE key (not the full s3:// URI) so redshift_copy_from_s3 can
    # build the correct path: s3://{NNN_S3_BUCKET}/{s3_prefix}
    context["ti"].xcom_push(key="uploaded_s3_key", value=dest_key)
    context["ti"].xcom_push(key="record_count", value=len(all_items))
    return dest_key


def load_to_redshift(**context) -> None:
    """COPY the S3 JSON file into Redshift analytics.rsp_portal_sessions.

    A DELETE for the run_date is performed first to make the load idempotent.
    """
    run_date: str = get_run_date(context)
    s3_key_path: str = context["ti"].xcom_pull(task_ids="scan_dynamodb", key="uploaded_s3_key")

    # Remove existing rows for this date before loading (idempotent)
    hook = get_redshift_hook()
    hook.run(
        f"DELETE FROM {REDSHIFT_TARGET} WHERE session_date = %s",
        parameters=(run_date,),
    )

    # COPY from S3 JSON into Redshift (truncate=False — DELETE already removed today's rows)
    redshift_copy_from_s3(
        table=REDSHIFT_TARGET,
        s3_prefix=s3_key_path,
        file_format="JSON",
        truncate=False,
    )


def validate_row_count(**context) -> None:
    """Assert at least one row exists in Redshift for session_date."""
    run_date = get_run_date(context)
    hook = get_redshift_hook()
    row = hook.get_first(
        f"SELECT COUNT(*) FROM {REDSHIFT_TARGET} WHERE session_date = %s",
        parameters=(run_date,),
    )
    row_count = row[0] if row else 0
    log.info("Row count for %s on %s: %d", REDSHIFT_TARGET, run_date, row_count)
    if row_count < 1:
        raise ValueError(
            f"Redshift data quality check failed: {REDSHIFT_TARGET} has {row_count} rows "
            f"for session_date={run_date} (expected >= 1)"
        )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_dynamodb_portal_sessions_daily",
    default_args=default_args,
    description="Daily DynamoDB RSP portal sessions → S3 → Redshift analytics.rsp_portal_sessions",
    schedule_interval="0 2 * * *",  # 2 AM AEST
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "wholesale", "portal", "dynamodb", "redshift", "daily"],
) as dag:

    # Step 1: Scan DynamoDB for portal sessions, upload JSON to S3
    t_scan = PythonOperator(  # Paginated DynamoDB Scan filtered to run_date; upload JSON to S3
        task_id="scan_dynamodb",
        python_callable=scan_dynamodb,
        sla=timedelta(hours=1),
    )

    # Step 2: COPY from S3 into Redshift
    t_load = PythonOperator(  # DELETE existing rows for run_date then COPY fresh data from S3
        task_id="load_to_redshift",
        python_callable=load_to_redshift,
    )

    # Step 3: Validate row count in Redshift
    t_validate = PythonOperator(  # Assert >= 1 row in analytics.rsp_portal_sessions for run_date
        task_id="validate_row_count",
        python_callable=validate_row_count,
    )

    t_scan >> t_load >> t_validate
