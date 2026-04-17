"""
nnn_snowflake_to_rds_postgres_daily
-------------------------------------
Owner:      nnn-data-engineering
Domain:     Operations / Network Health
Schedule:   Daily at 08:00 AEST (0 8 * * *)
SLA:        1 hour

Exports aggregated daily network health KPIs from Snowflake into the RDS PostgreSQL
database that backs operational dashboards.  A full replace-on-date strategy is
used: existing rows for the run_date are deleted before re-inserting, ensuring
idempotency.

Steps:
  1. export_from_snowflake  — query OPERATIONS.DAILY_NETWORK_HEALTH_SUMMARY for
                              the run_date, write to temp file, push path via XCom
  2. load_to_rds_postgres   — DELETE existing rows for run_date from
                              operational_reporting.network_health_daily, then
                              insert_rows in batches of 1 000
  3. validate               — query Postgres row count for run_date; raise if zero

Upstream:   OPERATIONS.DAILY_NETWORK_HEALTH_SUMMARY (nnn_network_health_summary_daily)
Downstream: Ops dashboards (Grafana / Tableau connected to RDS)
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
    CONN_RDS_POSTGRES,
    get_run_date,
    get_snowflake_hook,
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

PG_TABLE          = "operational_reporting.network_health_daily"
INSERT_BATCH_SIZE = 1_000   # Rows per postgres insert_rows call


# ---------------------------------------------------------------------------
# Task functions (module-level so the scheduler does not re-define them on
# every DAG parse cycle)
# ---------------------------------------------------------------------------

def export_from_snowflake(**context) -> None:
    """Query Snowflake OPERATIONS.DAILY_NETWORK_HEALTH_SUMMARY for run_date; write to temp file.

    PERF: records are scoped to run_date (typically hundreds of rows). Write to a
    temp file and push only the path via XCom to keep XCom lightweight.
    """
    run_date = get_run_date(context)

    hook   = get_snowflake_hook()
    conn   = hook.get_conn()
    cursor = conn.cursor()
    try:
        sql = """
            SELECT
                REPORT_DATE,
                REGION_CODE,
                EXCHANGE_ID,
                TECHNOLOGY_TYPE,
                TOTAL_SERVICES,
                ACTIVE_SERVICES,
                FAULT_COUNT,
                FAULT_RATE_PCT,
                AVG_UTILISATION_PCT,
                P95_LATENCY_MS,
                PACKET_LOSS_PCT,
                SLA_BREACH_COUNT,
                CREATED_AT
            FROM OPERATIONS.DAILY_NETWORK_HEALTH_SUMMARY
            WHERE REPORT_DATE = %(run_date)s
        """
        cursor.execute(sql, {"run_date": run_date})
        columns = [col[0].lower() for col in cursor.description]
        rows    = cursor.fetchall()
        records = [dict(zip(columns, row)) for row in rows]
    finally:
        cursor.close()
        conn.close()

    # Serialise date/datetime objects for JSON storage
    for rec in records:
        for k, v in rec.items():
            if hasattr(v, "isoformat"):
                rec[k] = v.isoformat()

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", prefix=f"net_health_{run_date}_", delete=False
    ) as tmp:
        json.dump(records, tmp)
        tmp_path = tmp.name

    context["ti"].xcom_push(key="health_records_path", value=tmp_path)
    log.info("Exported %d network health records for %s", len(records), run_date)


def load_to_rds_postgres(**context) -> None:
    """Replace rows for run_date in operational_reporting.network_health_daily."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    run_date            = get_run_date(context)
    health_records_path = context["ti"].xcom_pull(
        task_ids="export_from_snowflake", key="health_records_path"
    )

    with open(health_records_path) as fh:
        records = json.load(fh)

    if not records:
        log.info("No records for %s — skipping load.", run_date)
        os.remove(health_records_path)
        return

    hook = PostgresHook(postgres_conn_id=CONN_RDS_POSTGRES)

    # DELETE existing rows for this run_date (idempotent replace)
    hook.run(
        f"DELETE FROM {PG_TABLE} WHERE report_date = %s",
        parameters=(run_date,),
    )
    log.info("Deleted existing rows for report_date=%s from %s", run_date, PG_TABLE)

    # Determine column order from the first record
    columns = list(records[0].keys())

    # Insert in batches of INSERT_BATCH_SIZE
    for i in range(0, len(records), INSERT_BATCH_SIZE):
        batch = records[i : i + INSERT_BATCH_SIZE]
        rows  = [tuple(rec[col] for col in columns) for rec in batch]
        # commit_every=1000 commits after every 1000 rows for efficient batch commits
        hook.insert_rows(table=PG_TABLE, rows=rows, target_fields=columns, commit_every=1000)
        log.info("Inserted batch %d: %d rows", i // INSERT_BATCH_SIZE + 1, len(batch))

    log.info("Load complete — %d rows inserted into %s", len(records), PG_TABLE)
    os.remove(health_records_path)


def validate(**context) -> None:
    """Query Postgres row count for run_date and raise if zero."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    run_date = get_run_date(context)
    hook     = PostgresHook(postgres_conn_id=CONN_RDS_POSTGRES)

    result    = hook.get_first(
        f"SELECT COUNT(*) FROM {PG_TABLE} WHERE report_date = %s",
        parameters=(run_date,),
    )
    row_count = result[0] if result else 0
    log.info("Row count in %s for %s: %d", PG_TABLE, run_date, row_count)

    if row_count == 0:
        raise ValueError(
            f"Validation failed: zero rows found in {PG_TABLE} for report_date={run_date}"
        )
    log.info("Validation passed.")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_snowflake_to_rds_postgres_daily",
    description="Export daily network health KPIs from Snowflake → RDS PostgreSQL",
    default_args=default_args,
    schedule_interval="0 8 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "operations", "postgres", "rds", "snowflake", "daily"],
) as dag:

    task_export = PythonOperator(  # Query OPERATIONS.DAILY_NETWORK_HEALTH_SUMMARY for run_date
        task_id="export_from_snowflake",
        python_callable=export_from_snowflake,
    )

    task_load = PythonOperator(  # DELETE existing run_date rows then batch INSERT into PostgreSQL
        task_id="load_to_rds_postgres",
        python_callable=load_to_rds_postgres,
    )

    task_validate = PythonOperator(  # Query Postgres row count for run_date and raise if zero
        task_id="validate_row_count",
        python_callable=validate,
    )

    # ------------------------------------------------------------------
    # Dependency chain
    # ------------------------------------------------------------------
    task_export >> task_load >> task_validate
