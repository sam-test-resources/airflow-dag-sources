"""
nnn_snowflake_to_gcs_regulatory_weekly
----------------------------------------
Owner:      nnn-data-engineering
Domain:     Compliance / Regulatory
Schedule:   Wednesdays at 20:00 AEST (0 20 * * 3)
SLA:        3 hours

Generates the weekly anonymised regulatory report from the Snowflake
COMPLIANCE.WEEKLY_REGULATORY_EXPORT view, uploads it as a CSV to a Google Cloud
Storage bucket shared with the ACCC, then notifies the ACCC API to confirm delivery.

Steps:
  1. export_from_snowflake — query the view, write CSV to /tmp/
  2. upload_to_gcs         — upload the CSV to GCS under the correct year/month path
  3. notify_accc_api       — POST to the ACCC notification endpoint to confirm upload

Upstream:   COMPLIANCE.WEEKLY_REGULATORY_EXPORT (refreshed by compliance ETL)
Downstream: ACCC regulatory portal (external consumer)
"""
from __future__ import annotations

import csv
import json
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_GCS,
    CONN_NMS_API,
    get_run_date,
    get_run_month,
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
    "execution_timeout":         timedelta(hours=3),
}

GCS_BUCKET     = "nnn-accc-regulatory-prod"  # env suffix applied at runtime if needed
SNOWFLAKE_VIEW = "COMPLIANCE.WEEKLY_REGULATORY_EXPORT"


# ---------------------------------------------------------------------------
# Task functions (module-level so the scheduler does not re-define them on
# every DAG parse cycle)
# ---------------------------------------------------------------------------

def export_from_snowflake(**context) -> None:
    """Query COMPLIANCE.WEEKLY_REGULATORY_EXPORT and write result to a local CSV file."""
    run_date  = get_run_date(context)
    run_month = get_run_month(context)

    hook   = get_snowflake_hook()
    conn   = hook.get_conn()
    cursor = conn.cursor()
    try:
        sql = f"SELECT * FROM {SNOWFLAKE_VIEW}"  # view already filters to latest week
        cursor.execute(sql)
        columns = [col[0] for col in cursor.description]
        rows    = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

    run_year   = run_month[:4]
    local_path = f"/tmp/weekly_report_{run_date}.csv"

    with open(local_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(columns)
        for row in rows:
            # Convert non-string types to strings for CSV serialisation
            writer.writerow([v.isoformat() if hasattr(v, "isoformat") else v for v in row])

    row_count      = len(rows)
    gcs_object_name = f"{run_year}/{run_month}/weekly_report_{run_date}.csv"

    log.info("Exported %d rows from %s to %s", row_count, SNOWFLAKE_VIEW, local_path)

    # Push metadata to XCom for downstream tasks
    context["ti"].xcom_push(key="local_path",       value=local_path)
    context["ti"].xcom_push(key="gcs_object_name",  value=gcs_object_name)
    context["ti"].xcom_push(key="run_date",         value=run_date)
    context["ti"].xcom_push(key="row_count",        value=row_count)


def upload_to_gcs(**context) -> None:
    """Upload the regulatory CSV file to the ACCC GCS bucket."""
    from airflow.providers.google.cloud.hooks.gcs import GCSHook

    local_path      = context["ti"].xcom_pull(task_ids="export_from_snowflake", key="local_path")
    gcs_object_name = context["ti"].xcom_pull(task_ids="export_from_snowflake", key="gcs_object_name")

    hook = GCSHook(gcp_conn_id=CONN_GCS)
    hook.upload(
        bucket_name=GCS_BUCKET,
        object_name=gcs_object_name,
        filename=local_path,
        mime_type="text/csv",
    )

    gcs_uri = f"gs://{GCS_BUCKET}/{gcs_object_name}"
    log.info("Uploaded regulatory report to %s", gcs_uri)

    # Clean up local temp file
    os.remove(local_path)
    log.info("Removed local temp file %s", local_path)

    context["ti"].xcom_push(key="gcs_uri", value=gcs_uri)


def notify_accc_api(**context) -> None:
    """POST to the ACCC notification API to confirm the GCS file is ready."""
    from airflow.providers.http.hooks.http import HttpHook

    gcs_uri   = context["ti"].xcom_pull(task_ids="upload_to_gcs",          key="gcs_uri")
    run_date  = context["ti"].xcom_pull(task_ids="export_from_snowflake",   key="run_date")
    row_count = context["ti"].xcom_pull(task_ids="export_from_snowflake",   key="row_count")

    payload = {
        "file_path":   gcs_uri,
        "report_date": run_date,
        "row_count":   row_count,
        "provider":    "NNN",
        "report_type": "WEEKLY_REGULATORY",
        "notified_at": datetime.now(tz=timezone.utc).isoformat(),
    }

    hook     = HttpHook(http_conn_id=CONN_NMS_API, method="POST")
    response = hook.run(
        endpoint="/api/v1/accc/notify",
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"},
    )

    log.info("ACCC notification response: %d — %s", response.status_code, response.text[:200])
    if response.status_code not in (200, 201, 202):
        raise ValueError(
            f"ACCC notification API returned unexpected status {response.status_code}: {response.text}"
        )
    log.info("ACCC notification sent successfully.")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_snowflake_to_gcs_regulatory_weekly",
    description="Weekly regulatory report: Snowflake → GCS CSV → ACCC notification",
    default_args=default_args,
    schedule_interval="0 20 * * 3",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "compliance", "regulatory", "gcs", "accc", "weekly"],
) as dag:

    task_export = PythonOperator(  # Query COMPLIANCE.WEEKLY_REGULATORY_EXPORT and write to local CSV
        task_id="export_from_snowflake",
        python_callable=export_from_snowflake,
    )

    task_upload = PythonOperator(  # Upload the weekly regulatory CSV to GCS bucket for ACCC
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
    )

    task_notify = PythonOperator(  # POST to ACCC notification API confirming GCS file delivery
        task_id="notify_accc_api",
        python_callable=notify_accc_api,
    )

    # ------------------------------------------------------------------
    # Dependency chain
    # ------------------------------------------------------------------
    task_export >> task_upload >> task_notify
