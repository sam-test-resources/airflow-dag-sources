"""
NNN Airflow DAG — MS SQL Legacy Billing Ingest (Daily)
======================================================
Owner:      nnn-data-engineering
Domain:     Finance / Legacy Billing
Schedule:   0 5 * * *  (5 AM AEST daily)
SLA:        2 hours (must complete by 7 AM AEST)
Description:
    Extracts billing transaction records from the legacy MS SQL Server billing
    system (dbo.BillingTransactions) for the previous run date. Loads them
    into Snowflake FINANCE.LEGACY_BILLING_RECORDS using a DELETE + batch
    INSERT pattern to ensure full idempotency (no duplicates on retry).

    The legacy system does not support CDC or incremental exports; a full
    daily extraction scoped to the billing_date column is the agreed pattern.

Steps:
    1. extract_legacy_billing  — query MSSQL dbo.BillingTransactions for run_date
    2. load_to_snowflake       — DELETE existing rows, batch INSERT new rows
    3. validate                — assert_row_count >= 1 for run_date

Upstream:   dbo.BillingTransactions on CONN_MSSQL (legacy billing system)
Downstream: FINANCE.LEGACY_BILLING_RECORDS → Finance reporting, GL reconciliation
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_MSSQL,
    assert_row_count,
    get_mssql_hook,
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
    "execution_timeout": timedelta(hours=1, minutes=30),  # Legacy SQL can be slow on large dates
}

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
log = logging.getLogger(__name__)

MSSQL_TABLE      = "dbo.BillingTransactions"
TARGET_TABLE     = "FINANCE.LEGACY_BILLING_RECORDS"
MSSQL_BATCH_SIZE = 5_000   # Rows per XCom chunk — balance memory vs. call overhead
SF_INSERT_CHUNK  = 1_000   # Rows per Snowflake executemany call


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def extract_legacy_billing(**context) -> int:
    """Query MSSQL dbo.BillingTransactions for all rows matching run_date.

    Results are pushed to XCom as a list of tuples (ordered by billing column
    position) to minimise serialisation overhead vs. dicts.
    Column order must match the Snowflake INSERT in load_to_snowflake.
    """
    run_date: str = get_run_date(context)

    hook = get_mssql_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Query scoped to billing_date — uses the clustered index for performance
        cursor.execute(
            f"""
            SELECT
                transaction_id,
                account_number,
                customer_id,
                billing_date,
                invoice_number,
                line_item_type,
                product_code,
                plan_code,
                charge_amount,
                gst_amount,
                currency,
                payment_method,
                payment_status,
                due_date,
                paid_date,
                rsp_id,
                region,
                created_at,
                updated_at
            FROM {MSSQL_TABLE}
            WHERE billing_date = ?
            ORDER BY transaction_id
            """,
            (run_date,),
        )

        # Fetch all rows — for very large dates consider chunked fetchmany
        rows: list[tuple] = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

    if not rows:
        raise ValueError(
            f"No billing transactions found in {MSSQL_TABLE} for billing_date={run_date}. "
            "This may indicate a processing gap in the legacy billing system."
        )

    # Convert to list of plain tuples (Row objects may not serialise cleanly)
    plain_rows = [tuple(row) for row in rows]

    # PERF: billing rows can be tens of thousands of tuples — too large for XCom
    # (stored in the Airflow metadata DB). Write to a temp file; push only the path.
    import json
    import tempfile

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", prefix=f"billing_{run_date}_", delete=False
    ) as tmp:
        json.dump(plain_rows, tmp)
        tmp_path = tmp.name

    context["ti"].xcom_push(key="rows_path", value=tmp_path)
    context["ti"].xcom_push(key="run_date", value=run_date)
    return len(plain_rows)


def load_to_snowflake(**context) -> None:
    """DELETE existing rows for run_date then batch INSERT the extracted records.

    Using DELETE + INSERT rather than MERGE because the MSSQL extraction is a
    complete snapshot for the date — there is no need to match on primary key.
    This pattern is simpler and faster for full-day refreshes.
    """
    import json
    import os as _os

    ti = context["ti"]
    rows_path: str = ti.xcom_pull(task_ids="extract_legacy_billing", key="rows_path")
    run_date: str = ti.xcom_pull(task_ids="extract_legacy_billing", key="run_date")
    with open(rows_path) as fh:
        rows: list[tuple] = [tuple(r) for r in json.load(fh)]

    hook = get_snowflake_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Remove existing data for this date before loading (idempotent)
        # Snowflake connector uses named %(name)s param style, not positional %s.
        cursor.execute(
            f"DELETE FROM {TARGET_TABLE} WHERE billing_date = %(billing_date)s",
            {"billing_date": run_date},
        )

        insert_sql = f"""
            INSERT INTO {TARGET_TABLE} (
                transaction_id, account_number, customer_id, billing_date,
                invoice_number, line_item_type, product_code, plan_code,
                charge_amount, gst_amount, currency, payment_method,
                payment_status, due_date, paid_date, rsp_id, region,
                created_at, updated_at, loaded_at
            ) VALUES (
                %(transaction_id)s, %(account_number)s, %(customer_id)s, %(billing_date)s,
                %(invoice_number)s, %(line_item_type)s, %(product_code)s, %(plan_code)s,
                %(charge_amount)s, %(gst_amount)s, %(currency)s, %(payment_method)s,
                %(payment_status)s, %(due_date)s, %(paid_date)s, %(rsp_id)s, %(region)s,
                %(created_at)s, %(updated_at)s,
                CURRENT_TIMESTAMP()
            )
        """

        # Column order must match the SELECT in extract_legacy_billing.
        _COLS = [
            "transaction_id", "account_number", "customer_id", "billing_date",
            "invoice_number", "line_item_type", "product_code", "plan_code",
            "charge_amount", "gst_amount", "currency", "payment_method",
            "payment_status", "due_date", "paid_date", "rsp_id", "region",
            "created_at", "updated_at",
        ]

        # Insert in chunks to avoid Snowflake parameter binding limits
        for i in range(0, len(rows), SF_INSERT_CHUNK):
            chunk = [dict(zip(_COLS, row)) for row in rows[i : i + SF_INSERT_CHUNK]]
            cursor.executemany(insert_sql, chunk)

        conn.commit()
    finally:
        cursor.close()
        conn.close()
    _os.remove(rows_path)


def validate_row_count(**context) -> None:
    """Assert at least one billing record exists in Snowflake for run_date."""
    run_date = get_run_date(context)
    assert_row_count(TARGET_TABLE, run_date, min_rows=1)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_mssql_legacy_billing_daily",
    default_args=default_args,
    description="Daily MSSQL legacy billing extraction → Snowflake FINANCE.LEGACY_BILLING_RECORDS",
    schedule_interval="0 5 * * *",  # 5 AM AEST
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "finance", "billing", "mssql", "snowflake", "daily"],
) as dag:

    # Step 1: Extract all billing transactions for run_date from MSSQL
    t_extract = PythonOperator(  # Query dbo.BillingTransactions for all rows matching run_date
        task_id="extract_legacy_billing",
        python_callable=extract_legacy_billing,
        sla=timedelta(hours=2),
    )

    # Step 2: DELETE existing rows for run_date, then batch INSERT extracted rows
    t_load = PythonOperator(  # DELETE for run_date then batch INSERT into FINANCE.LEGACY_BILLING_RECORDS
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
    )

    # Step 3: Verify the load produced at least one row in the target table
    t_validate = PythonOperator(  # Assert >= 1 billing record in Snowflake for run_date
        task_id="validate_row_count",
        python_callable=validate_row_count,
    )

    t_extract >> t_load >> t_validate
