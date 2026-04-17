"""
nnn_imap_rsp_report_weekly
----------------------------
Owner:      nnn-data-engineering
Domain:     Wholesale / RSP Manual Reporting
Schedule:   Mondays at 11:00 AEST (0 11 * * 1)
SLA:        4 hours

Fetches RSP weekly report Excel attachments from the data-ingest email mailbox,
parses the 'Weekly Summary' sheet, and loads the data into Snowflake
WHOLESALE.RSP_MANUAL_REPORTS.  Processed emails are marked as read to prevent
re-ingestion on subsequent runs.

Steps:
  1. fetch_email_attachments  — search IMAP for unread emails from last 7 days with
                                subject 'RSP Weekly Report', download .xlsx attachments
  2. parse_excel_files        — parse each .xlsx 'Weekly Summary' sheet, validate
                                required columns, convert to list of dicts
  3. load_to_snowflake        — INSERT all rows into WHOLESALE.RSP_MANUAL_REPORTS,
                                mark source emails as read
  4. validate                 — assert row count >= 1 in Snowflake for run_date

Upstream:   RSPs send manual Excel reports via email (external)
Downstream: WHOLESALE.RSP_MANUAL_REPORTS → wholesale analytics
"""
from __future__ import annotations

import email
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_IMAP,
    assert_row_count,
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

REQUIRED_COLUMNS = {
    "rsp_id", "rsp_name", "week_ending", "service_activations",
    "service_disconnections", "fault_tickets_raised", "fault_tickets_resolved",
    "average_resolution_hours", "revenue_aud",
}

SNOWFLAKE_TABLE = "WHOLESALE.RSP_MANUAL_REPORTS"
EXCEL_SHEET     = "Weekly Summary"


# ---------------------------------------------------------------------------
# Task functions (module-level so the scheduler does not re-define them on
# every DAG parse cycle)
# ---------------------------------------------------------------------------

def fetch_email_attachments(**context) -> None:
    """Search IMAP for unread RSP weekly report emails, download .xlsx attachments."""
    from airflow.providers.imap.hooks.imap import ImapHook

    run_date = get_run_date(context)
    hook = ImapHook(imap_conn_id=CONN_IMAP)

    # ImapHook exposes the underlying imaplib.IMAP4_SSL as hook.mail after get_conn
    hook.get_conn()
    mail = hook.mail

    # Select the inbox
    mail.select("INBOX")

    # Search for unread emails with the expected subject
    search_criteria = '(UNSEEN SUBJECT "RSP Weekly Report")'
    _, message_ids_raw = mail.search(None, search_criteria)
    message_ids = message_ids_raw[0].split() if message_ids_raw[0] else []

    log.info("Found %d unread 'RSP Weekly Report' email(s).", len(message_ids))

    file_paths    = []
    email_uid_map = {}  # file_path → email uid for marking read later

    for msg_id in message_ids:
        _, msg_data = mail.fetch(msg_id, "(RFC822)")
        raw_email = msg_data[0][1]
        msg = email.message_from_bytes(raw_email)

        subject = msg.get("Subject", "")
        sender  = msg.get("From", "")
        log.info("Processing email: subject='%s', from='%s'", subject, sender)

        for part in msg.walk():
            content_disposition = str(part.get("Content-Disposition", ""))
            filename = part.get_filename()

            if (
                filename
                and filename.lower().endswith(".xlsx")
                and "attachment" in content_disposition.lower()
            ):
                safe_name  = filename.replace(" ", "_").replace("/", "_")
                local_path = f"/tmp/rsp_report_{run_date}_{safe_name}"

                with open(local_path, "wb") as fh:
                    fh.write(part.get_payload(decode=True))

                file_paths.append(local_path)
                email_uid_map[local_path] = msg_id.decode()
                log.info("Downloaded attachment '%s' to '%s'", filename, local_path)

    hook.mail.logout()

    log.info("Total .xlsx attachments downloaded: %d", len(file_paths))
    context["ti"].xcom_push(key="file_paths",    value=file_paths)
    context["ti"].xcom_push(key="email_uid_map", value=email_uid_map)


def parse_excel_files(**context) -> None:
    """Parse each .xlsx 'Weekly Summary' sheet; validate columns; push records to XCom."""
    import pandas as pd

    run_date   = get_run_date(context)
    file_paths = context["ti"].xcom_pull(task_ids="fetch_email_attachments", key="file_paths") or []

    if not file_paths:
        log.info("No Excel files to parse.")
        context["ti"].xcom_push(key="all_records", value=[])
        return

    all_records = []

    for file_path in file_paths:
        log.info("Parsing: %s", file_path)
        try:
            df = pd.read_excel(file_path, sheet_name=EXCEL_SHEET)
        except Exception as exc:
            raise ValueError(f"Failed to read '{EXCEL_SHEET}' from {file_path}: {exc}") from exc

        # Normalise column names to lowercase with underscores
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

        # Validate required columns are present
        missing = REQUIRED_COLUMNS - set(df.columns)
        if missing:
            raise ValueError(f"File {file_path} is missing required columns: {missing}")

        # Convert to list of dicts and add run metadata
        records = df.to_dict(orient="records")
        for rec in records:
            rec["source_file"] = os.path.basename(file_path)
            rec["run_date"]    = run_date
            rec["ingested_at"] = datetime.now(tz=timezone.utc).isoformat()
            # Normalise any remaining date/datetime objects for XCom serialisation
            for k, v in rec.items():
                if hasattr(v, "isoformat"):
                    rec[k] = v.isoformat()

        all_records.extend(records)
        log.info("Parsed %d rows from %s", len(records), file_path)

    log.info("Total records parsed: %d", len(all_records))
    context["ti"].xcom_push(key="all_records", value=all_records)


def load_to_snowflake(**context) -> None:
    """Batch INSERT RSP report rows into Snowflake; mark source emails as read."""
    from airflow.providers.imap.hooks.imap import ImapHook

    all_records   = context["ti"].xcom_pull(task_ids="parse_excel_files",      key="all_records")   or []
    email_uid_map = context["ti"].xcom_pull(task_ids="fetch_email_attachments", key="email_uid_map") or {}

    if not all_records:
        log.info("No records to insert — skipping load.")
    else:
        hook   = get_snowflake_hook()
        conn   = hook.get_conn()
        cursor = conn.cursor()
        try:
            # Snowflake connector uses named %(name)s param style, not positional %s.
            insert_sql = f"""
                INSERT INTO {SNOWFLAKE_TABLE} (
                    RSP_ID,
                    RSP_NAME,
                    WEEK_ENDING,
                    SERVICE_ACTIVATIONS,
                    SERVICE_DISCONNECTIONS,
                    FAULT_TICKETS_RAISED,
                    FAULT_TICKETS_RESOLVED,
                    AVERAGE_RESOLUTION_HOURS,
                    REVENUE_AUD,
                    SOURCE_FILE,
                    RUN_DATE,
                    INGESTED_AT
                )
                VALUES (
                    %(rsp_id)s, %(rsp_name)s, %(week_ending)s,
                    %(service_activations)s, %(service_disconnections)s,
                    %(fault_tickets_raised)s, %(fault_tickets_resolved)s,
                    %(average_resolution_hours)s, %(revenue_aud)s,
                    %(source_file)s, %(run_date)s, %(ingested_at)s
                )
            """
            rows = [
                {
                    "rsp_id":                  rec.get("rsp_id"),
                    "rsp_name":                rec.get("rsp_name"),
                    "week_ending":             rec.get("week_ending"),
                    "service_activations":     rec.get("service_activations"),
                    "service_disconnections":  rec.get("service_disconnections"),
                    "fault_tickets_raised":    rec.get("fault_tickets_raised"),
                    "fault_tickets_resolved":  rec.get("fault_tickets_resolved"),
                    "average_resolution_hours": rec.get("average_resolution_hours"),
                    "revenue_aud":             rec.get("revenue_aud"),
                    "source_file":             rec.get("source_file"),
                    "run_date":                rec.get("run_date"),
                    "ingested_at":             rec.get("ingested_at"),
                }
                for rec in all_records
            ]
            cursor.executemany(insert_sql, rows)
            conn.commit()
            log.info("Inserted %d rows into %s", len(all_records), SNOWFLAKE_TABLE)
        finally:
            cursor.close()
            conn.close()

    # Mark processed emails as read so they are not re-ingested on reruns
    if email_uid_map:
        imap_hook = ImapHook(imap_conn_id=CONN_IMAP)
        imap_hook.get_conn()
        mail = imap_hook.mail
        mail.select("INBOX")
        for file_path, uid in email_uid_map.items():
            mail.store(uid, "+FLAGS", "\\Seen")
            log.info("Marked email UID %s as read (source: %s)", uid, os.path.basename(file_path))
        mail.logout()

    # Clean up temp Excel files
    for file_path in (context["ti"].xcom_pull(task_ids="fetch_email_attachments", key="file_paths") or []):
        if os.path.exists(file_path):
            os.remove(file_path)
            log.info("Removed temp file: %s", file_path)


def validate(**context) -> None:
    """Assert at least 1 row was loaded into WHOLESALE.RSP_MANUAL_REPORTS for run_date."""
    run_date = get_run_date(context)
    assert_row_count(table=SNOWFLAKE_TABLE, run_date=run_date, min_rows=1)
    log.info("Validation passed for %s on %s", SNOWFLAKE_TABLE, run_date)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_imap_rsp_report_weekly",
    description="Ingest RSP weekly Excel reports from IMAP email into Snowflake",
    default_args=default_args,
    schedule_interval="0 11 * * 1",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "wholesale", "imap", "email", "rsp", "snowflake", "weekly"],
) as dag:

    task_fetch = PythonOperator(  # Search IMAP inbox for unread RSP report emails, save .xlsx attachments
        task_id="fetch_email_attachments",
        python_callable=fetch_email_attachments,
    )

    task_parse = PythonOperator(  # Parse each .xlsx 'Weekly Summary' sheet and validate required columns
        task_id="parse_excel_files",
        python_callable=parse_excel_files,
    )

    task_load = PythonOperator(  # Batch INSERT RSP report rows into Snowflake; mark emails as read
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
    )

    task_validate = PythonOperator(  # Assert >= 1 row in WHOLESALE.RSP_MANUAL_REPORTS for run_date
        task_id="validate_row_count",
        python_callable=validate,
    )

    # ------------------------------------------------------------------
    # Dependency chain
    # ------------------------------------------------------------------
    task_fetch >> task_parse >> task_load >> task_validate
