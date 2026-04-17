"""
NNN Airflow DAG — SFTP RSP Billing Ingest (Daily)
==================================================
Owner:      nnn-data-engineering
Domain:     Wholesale / Billing
Schedule:   0 18 * * *  (6 PM AEST daily; offset = 8 AM UTC)
SLA:        2 hours (must complete by 8 PM AEST)
Description:
    Pulls bulk billing CSV files dropped by RSPs (Retail Service Providers)
    onto the NNN SFTP server, stages them into S3, then COPYs and MERGEs
    the records into Snowflake WHOLESALE.RSP_BILLING_STATEMENTS.

Steps:
    1. list_sftp_files        — list *.csv files in /uploads/billing/{ds}/
    2. download_and_stage     — download each CSV to /tmp/, upload to S3
    3. load_to_snowflake      — COPY into temp table, MERGE into target
    4. validate               — assert row count >= 1

Upstream:   RSP SFTP drop (external)
Downstream: WHOLESALE.RSP_BILLING_STATEMENTS → billing analytics, Finance reports
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_SFTP_RSP,
    NNN_SNOWFLAKE_S3_STAGE,
    assert_row_count,
    get_run_date,
    get_snowflake_hook,
    s3_key,
    upload_to_s3,
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
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": nnn_failure_alert,
    "execution_timeout": timedelta(minutes=60),  # SFTP + S3 + Snowflake can be slow
}

# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def list_sftp_files(ds: str, **context) -> list[str]:
    """List *.csv billing files on the SFTP server for the run date."""
    from airflow.providers.ssh.hooks.ssh import SSHHook

    hook = SSHHook(ssh_conn_id=CONN_SFTP_RSP)
    remote_dir = f"/uploads/billing/{ds}/"

    with hook.get_conn() as ssh_client:
        sftp = ssh_client.open_sftp()
        try:
            all_files = sftp.listdir(remote_dir)
        except FileNotFoundError:
            raise FileNotFoundError(
                f"SFTP directory does not exist: {remote_dir}. "
                "RSP may not have delivered files yet."
            )
        finally:
            sftp.close()

    csv_files = [f for f in all_files if f.endswith(".csv")]
    if not csv_files:
        raise ValueError(f"No *.csv files found in {remote_dir}")

    context["ti"].xcom_push(key="sftp_files", value=csv_files)
    context["ti"].xcom_push(key="remote_dir", value=remote_dir)
    return csv_files


def download_and_stage(ds: str, **context) -> list[str]:
    """Download each CSV from SFTP to /tmp/ then upload to S3."""
    import os
    from airflow.providers.ssh.hooks.ssh import SSHHook

    ti = context["ti"]
    sftp_files: list[str] = ti.xcom_pull(task_ids="list_sftp_files", key="sftp_files")
    remote_dir: str = ti.xcom_pull(task_ids="list_sftp_files", key="remote_dir")

    hook = SSHHook(ssh_conn_id=CONN_SFTP_RSP)
    s3_keys: list[str] = []

    with hook.get_conn() as ssh_client:
        sftp = ssh_client.open_sftp()
        try:
            for fname in sftp_files:
                remote_path = f"{remote_dir}{fname}"
                local_path = f"/tmp/rsp_billing_{ds}_{fname}"
                # Download file from SFTP to local temp location
                sftp.get(remote_path, local_path)

                # Build an S3 key and upload; store the relative key (not the
                # full s3:// URI) so it can be used directly in a Snowflake
                # stage reference: @stage/<relative_key>.
                stem = fname.replace(".csv", "")
                s3_dest = s3_key("wholesale/rsp_billing", stem, ds, ext="csv")
                upload_to_s3(local_path, s3_dest)
                s3_keys.append(s3_dest)  # relative key, not the full s3:// URI

                # Clean up local file immediately to save disk space
                os.remove(local_path)
        finally:
            sftp.close()

    ti.xcom_push(key="s3_keys", value=s3_keys)
    return s3_keys


def load_to_snowflake(**context) -> None:
    """COPY each staged CSV into a temp table, then MERGE into target.

    Temp tables are session-scoped in Snowflake so we do everything in a
    single connection/cursor rather than splitting across tasks.
    """
    ti = context["ti"]
    run_date: str = get_run_date(context)
    s3_keys: list[str] = ti.xcom_pull(task_ids="download_and_stage", key="s3_keys")

    hook = get_snowflake_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Create temp staging table matching target schema
        cursor.execute("""
            CREATE TEMPORARY TABLE tmp_rsp_billing_staging (
                rsp_id          VARCHAR,
                billing_date    DATE,
                service_id      VARCHAR,
                plan_code       VARCHAR,
                charge_amount   NUMBER(18,4),
                currency        VARCHAR,
                loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)

        for s3_path in s3_keys:
            # COPY each CSV file from S3 stage into the temp table
            cursor.execute(f"""
                COPY INTO tmp_rsp_billing_staging (
                    rsp_id, billing_date, service_id, plan_code, charge_amount, currency
                )
                FROM '{NNN_SNOWFLAKE_S3_STAGE}/{s3_path}'
                FILE_FORMAT = (
                    TYPE = CSV
                    SKIP_HEADER = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                    NULL_IF = ('NULL', 'null', '')
                )
                ON_ERROR = 'ABORT_STATEMENT'
            """)

        # MERGE temp staging into the permanent target table
        cursor.execute(f"""
            MERGE INTO WHOLESALE.RSP_BILLING_STATEMENTS AS tgt
            USING (
                SELECT DISTINCT
                    rsp_id,
                    billing_date,
                    service_id,
                    plan_code,
                    charge_amount,
                    currency,
                    loaded_at
                FROM tmp_rsp_billing_staging
            ) AS src
            ON  tgt.rsp_id       = src.rsp_id
            AND tgt.billing_date = src.billing_date
            AND tgt.service_id   = src.service_id
            WHEN MATCHED THEN UPDATE SET
                plan_code      = src.plan_code,
                charge_amount  = src.charge_amount,
                currency       = src.currency,
                loaded_at      = src.loaded_at
            WHEN NOT MATCHED THEN INSERT (
                rsp_id, billing_date, service_id, plan_code, charge_amount, currency, loaded_at
            ) VALUES (
                src.rsp_id, src.billing_date, src.service_id,
                src.plan_code, src.charge_amount, src.currency, src.loaded_at
            )
        """)

        conn.commit()
    finally:
        cursor.close()
        conn.close()


def validate_row_count(**context) -> None:
    """Assert that at least one row was loaded for today's run date."""
    run_date = get_run_date(context)
    hook = get_snowflake_hook()
    row = hook.get_first(
        "SELECT COUNT(*) FROM WHOLESALE.RSP_BILLING_STATEMENTS WHERE billing_date = %(d)s",
        parameters={"d": run_date},
    )
    row_count = row[0] if row else 0
    log.info("Row count for WHOLESALE.RSP_BILLING_STATEMENTS on %s: %d", run_date, row_count)
    if row_count < 1:
        raise ValueError(
            f"Data quality check failed: WHOLESALE.RSP_BILLING_STATEMENTS has {row_count} rows "
            f"for billing_date={run_date} (expected >= 1)"
        )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_sftp_rsp_billing_ingest_daily",
    default_args=default_args,
    description="Ingest RSP bulk billing CSVs from SFTP into Snowflake WHOLESALE.RSP_BILLING_STATEMENTS",
    schedule_interval="0 18 * * *",  # 6 PM AEST = 8 AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "wholesale", "billing", "sftp", "snowflake", "daily"],
) as dag:

    # Step 1: List CSV files dropped by RSPs on the SFTP server
    t_list = PythonOperator(
        task_id="list_sftp_files",
        python_callable=list_sftp_files,
        op_kwargs={"ds": "{{ ds }}"},
        sla=timedelta(hours=2),
    )

    # Step 2: Download each CSV from SFTP and upload to S3 for staging
    t_stage = PythonOperator(
        task_id="download_and_stage",
        python_callable=download_and_stage,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # Step 3: COPY from S3 into Snowflake temp table, then MERGE into target
    t_load = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
    )

    # Step 4: Validate that rows exist for today's billing date
    t_validate = PythonOperator(
        task_id="validate_row_count",
        python_callable=validate_row_count,
    )

    t_list >> t_stage >> t_load >> t_validate
