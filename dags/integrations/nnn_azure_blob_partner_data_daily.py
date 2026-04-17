"""
nnn_azure_blob_partner_data_daily
----------------------------------
Owner:      nnn-data-engineering
Domain:     Wholesale / Partner Data Ingestion
Schedule:   Daily at 10:00 AEST (0 10 * * *)
SLA:        2 hours

Ingests daily partner data drops from Azure Blob Storage into Snowflake.
Each partner drops a CSV file into the shared container under their partner_id
prefix.  Files are staged via S3 before COPY INTO Snowflake to leverage the
existing Snowflake S3 integration.

Steps:
  1. list_azure_blobs           — list all blobs under partner-data-drops/{run_date}/
  2. download_and_upload_to_s3  — download each blob, write to /tmp/, upload to S3
  3. load_to_snowflake          — COPY INTO WHOLESALE.PARTNER_DATA_INGEST from each S3 key
  4. validate                   — assert row count >= 1 for run_date

Upstream:   Partner CSV drops in Azure Blob Storage (external)
Downstream: WHOLESALE.PARTNER_DATA_INGEST → downstream wholesale analytics
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_AZURE_BLOB,
    CONN_S3,
    NNN_S3_BUCKET,
    NNN_SNOWFLAKE_S3_STAGE,
    assert_row_count,
    get_run_date,
    snowflake_run,
    upload_to_s3,
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

AZURE_CONTAINER  = "partner-data-drops"
SNOWFLAKE_TABLE  = "WHOLESALE.PARTNER_DATA_INGEST"
S3_DOMAIN        = "wholesale"
S3_TABLE         = "partner_data_ingest"


# ---------------------------------------------------------------------------
# Task functions (module-level so the scheduler does not re-define them on
# every DAG parse cycle)
# ---------------------------------------------------------------------------

def list_azure_blobs(**context) -> None:
    """List all blobs under partner-data-drops/{run_date}/ and push names to XCom."""
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

    run_date = get_run_date(context)
    hook     = WasbHook(wasb_conn_id=CONN_AZURE_BLOB)

    # WasbHook.get_blobs_list returns a list of blob name strings
    blobs = hook.get_blobs_list(
        container_name=AZURE_CONTAINER,
        prefix=f"{run_date}/",
    )

    if not blobs:
        log.info(
            "No blobs found in '%s' with prefix '%s/' — nothing to ingest.",
            AZURE_CONTAINER, run_date,
        )
    else:
        log.info("Found %d blob(s) for %s: %s", len(blobs), run_date, blobs)

    context["ti"].xcom_push(key="blob_names", value=blobs)
    context["ti"].xcom_push(key="run_date",   value=run_date)


def download_and_upload_to_s3(**context) -> None:
    """Download partner blobs from Azure to /tmp/, then upload to S3 staging area."""
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

    blob_names = context["ti"].xcom_pull(task_ids="list_azure_blobs", key="blob_names") or []
    run_date   = context["ti"].xcom_pull(task_ids="list_azure_blobs", key="run_date")

    if not blob_names:
        log.info("No blobs to download — skipping.")
        context["ti"].xcom_push(key="s3_keys", value=[])
        return

    hook    = WasbHook(wasb_conn_id=CONN_AZURE_BLOB)
    s3_keys = []

    for blob_name in blob_names:
        # blob_name format: {partner_id}/{run_date}/data.csv
        safe_name  = blob_name.replace("/", "_")
        local_path = f"/tmp/partner_{safe_name}"

        # Download bytes from Azure Blob
        blob_data = hook.read_file(container_name=AZURE_CONTAINER, blob_name=blob_name)
        with open(local_path, "wb") as fh:
            fh.write(blob_data if isinstance(blob_data, bytes) else blob_data.encode())
        log.info("Downloaded blob '%s' to '%s'", blob_name, local_path)

        # Upload to S3 under the wholesale/partner_data_ingest prefix
        s3_key_path = f"{S3_DOMAIN}/{S3_TABLE}/run_date={run_date}/{safe_name}"
        upload_to_s3(local_path=local_path, s3_key_path=s3_key_path)
        s3_keys.append(s3_key_path)
        log.info("Uploaded to S3: s3://%s/%s", NNN_S3_BUCKET, s3_key_path)

        # Clean up temp file
        os.remove(local_path)

    context["ti"].xcom_push(key="s3_keys", value=s3_keys)
    log.info("Uploaded %d file(s) to S3.", len(s3_keys))


def load_to_snowflake(**context) -> None:
    """COPY INTO WHOLESALE.PARTNER_DATA_INGEST from S3 for each partner file."""
    run_date = get_run_date(context)
    s3_keys  = context["ti"].xcom_pull(task_ids="download_and_upload_to_s3", key="s3_keys") or []

    if not s3_keys:
        log.info("No S3 keys to load — skipping Snowflake COPY.")
        return

    for s3_key in s3_keys:
        copy_sql = f"""
            COPY INTO {SNOWFLAKE_TABLE} (
                PARTNER_ID,
                RUN_DATE,
                SERVICE_ID,
                SERVICE_TYPE,
                QUANTITY,
                UNIT_PRICE,
                TOTAL_AMOUNT,
                CURRENCY,
                REPORTING_PERIOD,
                LOADED_AT
            )
            FROM '@{NNN_SNOWFLAKE_S3_STAGE}/{s3_key}'
            FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
            ON_ERROR = 'ABORT_STATEMENT'
            PURGE = FALSE
        """
        snowflake_run(copy_sql)
        log.info("COPY INTO complete for s3_key: %s", s3_key)

    log.info("All %d file(s) loaded into %s", len(s3_keys), SNOWFLAKE_TABLE)


def validate(**context) -> None:
    """Assert that at least 1 row was loaded into WHOLESALE.PARTNER_DATA_INGEST for run_date."""
    run_date = get_run_date(context)
    assert_row_count(table=SNOWFLAKE_TABLE, run_date=run_date, min_rows=1)
    log.info("Validation passed for %s on %s", SNOWFLAKE_TABLE, run_date)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_azure_blob_partner_data_daily",
    description="Ingest partner CSV files from Azure Blob Storage into Snowflake via S3 staging",
    default_args=default_args,
    schedule_interval="0 10 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "wholesale", "azure", "snowflake", "partner", "daily"],
) as dag:

    task_list = PythonOperator(  # List today's partner blobs in Azure, push names to XCom
        task_id="list_azure_blobs",
        python_callable=list_azure_blobs,
    )

    task_download = PythonOperator(  # Download each Azure blob and stage to S3
        task_id="download_and_upload_to_s3",
        python_callable=download_and_upload_to_s3,
    )

    task_load = PythonOperator(  # COPY each S3-staged CSV into WHOLESALE.PARTNER_DATA_INGEST
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
    )

    task_validate = PythonOperator(  # Assert >= 1 row in WHOLESALE.PARTNER_DATA_INGEST for run_date
        task_id="validate_row_count",
        python_callable=validate,
    )

    # ------------------------------------------------------------------
    # Dependency chain
    # ------------------------------------------------------------------
    task_list >> task_download >> task_load >> task_validate
