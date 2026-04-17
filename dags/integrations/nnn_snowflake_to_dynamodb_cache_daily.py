"""
nnn_snowflake_to_dynamodb_cache_daily
---------------------------------------
Owner:      nnn-data-engineering
Domain:     Customer / RSP Portal Cache
Schedule:   Daily at 06:00 AEST (0 6 * * *)
SLA:        1 hour

Exports active address-level service-eligibility records from Snowflake and
upserts them into the DynamoDB table used by the RSP portal for low-latency
eligibility lookups.  An audit record is written back to Snowflake on completion.

Steps:
  1. export_from_snowflake   — query CUSTOMER.SERVICE_ELIGIBILITY_CURRENT, write to
                               temp file, push path via XCom
  2. load_to_dynamodb        — batch-write items to DynamoDB in chunks of 25
                               (DynamoDB BatchWriteItem hard limit)
  3. log_cache_refresh_audit — INSERT audit row into CUSTOMER.CACHE_REFRESH_AUDIT

Upstream:   CUSTOMER.SERVICE_ELIGIBILITY_CURRENT refresh (nnn_customer_eligibility_daily)
Downstream: RSP portal read path (real-time)
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
    CONN_DYNAMODB,
    get_run_date,
    get_snowflake_hook,
    snowflake_run,
)

log = logging.getLogger(__name__)

# DynamoDB BatchWriteItem hard limit — must not exceed 25 items per call
DYNAMODB_BATCH_SIZE = 25
DYNAMO_TABLE_NAME   = "nnn-rsp-portal-cache"

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
    "execution_timeout":         timedelta(hours=1),
}


# ---------------------------------------------------------------------------
# Task functions (module-level so the scheduler does not re-define them on
# every DAG parse cycle)
# ---------------------------------------------------------------------------

def export_from_snowflake(**context) -> None:
    """Query Snowflake for all active address eligibility records and write to temp file.

    PERF: eligibility_records can be millions of rows — write to a temp JSON file and
    push only the path via XCom to avoid bloating the Airflow metadata DB.
    INTENTIONAL FULL-TABLE SCAN: SERVICE_ELIGIBILITY_CURRENT is a current-state snapshot
    table (no date partitioning). All active rows are needed for cache refresh.
    """
    run_date = get_run_date(context)

    hook   = get_snowflake_hook()
    conn   = hook.get_conn()
    cursor = conn.cursor()
    try:
        sql = """
            SELECT
                ADDRESS_ID,
                ADDRESS_LINE_1,
                ADDRESS_LINE_2,
                SUBURB,
                STATE,
                POSTCODE,
                TECHNOLOGY_TYPE,
                MAX_DOWNLOAD_SPEED_MBPS,
                MAX_UPLOAD_SPEED_MBPS,
                IS_ELIGIBLE,
                ELIGIBILITY_REASON,
                LAST_UPDATED_AT
            FROM CUSTOMER.SERVICE_ELIGIBILITY_CURRENT
            WHERE IS_ELIGIBLE = TRUE
              AND DATE(LAST_UPDATED_AT) <= %(run_date)s
        """
        cursor.execute(sql, {"run_date": run_date})
        columns = [col[0].lower() for col in cursor.description]
        rows    = cursor.fetchall()
        records = [dict(zip(columns, row)) for row in rows]
    finally:
        cursor.close()
        conn.close()

    # Serialise datetime objects to ISO strings
    for rec in records:
        for k, v in rec.items():
            if hasattr(v, "isoformat"):
                rec[k] = v.isoformat()

    # PERF: write to temp file; push only the path via XCom
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", prefix=f"eligibility_{run_date}_", delete=False
    ) as tmp:
        json.dump(records, tmp)
        tmp_path = tmp.name

    context["ti"].xcom_push(key="eligibility_records_path", value=tmp_path)
    context["ti"].xcom_push(key="record_count", value=len(records))
    log.info("Exported %d eligibility records from Snowflake for %s", len(records), run_date)


def load_to_dynamodb(**context) -> None:
    """Upsert all eligibility records into DynamoDB in BatchWriteItem chunks of 25."""
    from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

    records_path = context["ti"].xcom_pull(
        task_ids="export_from_snowflake", key="eligibility_records_path"
    )
    with open(records_path) as fh:
        records = json.load(fh)

    if not records:
        log.info("No records to load into DynamoDB — skipping.")
        os.remove(records_path)
        return

    hook     = AwsBaseHook(aws_conn_id=CONN_DYNAMODB, resource_type="dynamodb")
    dynamodb = hook.get_resource_type("dynamodb")
    table    = dynamodb.Table(DYNAMO_TABLE_NAME)

    total_written = 0
    for i in range(0, len(records), DYNAMODB_BATCH_SIZE):
        chunk = records[i : i + DYNAMODB_BATCH_SIZE]
        with table.batch_writer() as batch:
            for record in chunk:
                # DynamoDB requires Decimal for numbers; convert to str for safety
                item = {k: str(v) if v is not None else "" for k, v in record.items()}
                item["address_id"] = record["address_id"]  # partition key (string)
                batch.put_item(Item=item)
        total_written += len(chunk)
        log.info(
            "Written chunk %d: %d items so far",
            i // DYNAMODB_BATCH_SIZE + 1, total_written,
        )

    context["ti"].xcom_push(key="items_written", value=total_written)
    log.info("DynamoDB upsert complete — %d items written to %s", total_written, DYNAMO_TABLE_NAME)
    os.remove(records_path)


def log_cache_refresh_audit(**context) -> None:
    """INSERT an audit record into CUSTOMER.CACHE_REFRESH_AUDIT in Snowflake."""
    run_date      = get_run_date(context)
    items_written = context["ti"].xcom_pull(task_ids="load_to_dynamodb", key="items_written") or 0
    refreshed_at  = datetime.now(tz=timezone.utc).isoformat()

    sql = """
        INSERT INTO CUSTOMER.CACHE_REFRESH_AUDIT
            (CACHE_TABLE, RUN_DATE, ITEMS_WRITTEN, REFRESHED_AT, STATUS)
        VALUES
            (%(cache_table)s, %(run_date)s, %(items_written)s, %(refreshed_at)s, %(status)s)
    """
    snowflake_run(
        sql,
        parameters={
            "cache_table":    DYNAMO_TABLE_NAME,
            "run_date":       run_date,
            "items_written":  items_written,
            "refreshed_at":   refreshed_at,
            "status":         "SUCCESS",
        },
    )
    log.info("Audit record inserted: %d items refreshed on %s", items_written, run_date)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_snowflake_to_dynamodb_cache_daily",
    description="Export service-eligibility data from Snowflake → DynamoDB RSP portal cache",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "customer", "dynamodb", "snowflake", "daily"],
) as dag:

    task_export = PythonOperator(  # Query CUSTOMER.SERVICE_ELIGIBILITY_CURRENT; write to temp file
        task_id="export_from_snowflake",
        python_callable=export_from_snowflake,
    )

    task_load = PythonOperator(  # Upsert eligibility records into nnn-rsp-portal-cache in DynamoDB batches
        task_id="load_to_dynamodb",
        python_callable=load_to_dynamodb,
    )

    task_audit = PythonOperator(  # INSERT audit record into CUSTOMER.CACHE_REFRESH_AUDIT in Snowflake
        task_id="log_cache_refresh_audit",
        python_callable=log_cache_refresh_audit,
    )

    # ------------------------------------------------------------------
    # Dependency chain
    # ------------------------------------------------------------------
    task_export >> task_load >> task_audit
