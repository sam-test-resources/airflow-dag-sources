"""
nnn_common.utils
~~~~~~~~~~~~~~~~
Shared helpers used across all NNN Airflow DAGs.

Conventions:
  - Connection IDs are always  nnn_<system>_<env>  (env = prod|staging|dev)
  - Snowflake target schemas:  NETWORK, CUSTOMER, WHOLESALE, FIELD_OPS,
                               FINANCE, COMPLIANCE, INFRASTRUCTURE, ML, OPERATIONS
  - Redshift schemas:          analytics, finance, wholesale, customer,
                               ml, compliance, regulatory, capacity_planning
  - All local dates are AEST (UTC+10 / UTC+11 DST)
  - S3 bucket: nnn-data-lake-<env>
"""

import os
import logging
from datetime import datetime, timezone, timedelta

from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)

NNN_ENV        = os.getenv("NNN_ENV", "prod")
NNN_S3_BUCKET  = f"nnn-data-lake-{NNN_ENV}"
NNN_REGION     = "ap-southeast-2"          # Sydney

# IAM role ARN used by Redshift for S3 COPY / UNLOAD operations
NNN_REDSHIFT_IAM_ROLE = os.getenv(
    "NNN_REDSHIFT_IAM_ROLE",
    f"arn:aws:iam::123456789012:role/nnn-redshift-s3-{NNN_ENV}",
)

# Snowflake external stage name (pre-configured to point at NNN_S3_BUCKET)
NNN_SNOWFLAKE_S3_STAGE = f"nnn_s3_stage_{NNN_ENV}"

# ── Connection ID helpers ──────────────────────────────────────────────────────

def conn(system: str) -> str:
    """Return the Airflow connection ID for a given system in the current env."""
    return f"nnn_{system}_{NNN_ENV}"


# ── Source / operational system connections ────────────────────────────────────

CONN_SNOWFLAKE    = conn("snowflake")
CONN_ORACLE_EBS   = conn("oracle_ebs")
CONN_ORACLE_OSS   = conn("oracle_oss")
CONN_POSTGRES_NI  = conn("postgres_ni")     # Network Inventory (on-prem)
CONN_SERVICENOW   = conn("servicenow")
CONN_SALESFORCE   = conn("salesforce")
CONN_SAP          = conn("sap_erp")
CONN_MEDALLIA     = conn("medallia")
CONN_S3           = conn("s3")
CONN_KAFKA        = conn("kafka")
CONN_NMS_API      = conn("nms_api")         # Network Management System REST API
CONN_SQ_API       = conn("sq_api")          # Service Qualification API
CONN_POWERBI      = conn("powerbi")

# ── AWS-native connections ─────────────────────────────────────────────────────

CONN_REDSHIFT     = conn("redshift")        # Amazon Redshift analytics DW
CONN_DYNAMODB     = conn("dynamodb")        # Amazon DynamoDB
CONN_KINESIS      = conn("kinesis")         # Amazon Kinesis Data Streams
CONN_SQS          = conn("sqs")             # Amazon SQS
CONN_FIREHOSE     = conn("firehose")        # Amazon Kinesis Data Firehose
CONN_GLUE         = conn("glue")            # AWS Glue Data Catalog

# ── New diverse source connections ─────────────────────────────────────────────

CONN_SFTP_RSP     = conn("sftp_rsp")        # SFTP server for RSP bulk file drops
CONN_MONGODB      = conn("mongodb")         # MongoDB (NOC operational tools)
CONN_ELASTICSEARCH = conn("elasticsearch") # Elasticsearch (fault log analytics)
CONN_MQTT         = conn("mqtt")            # MQTT broker (FW base station IoT)
CONN_RABBITMQ     = conn("rabbitmq")        # RabbitMQ (order management events)
CONN_MSSQL        = conn("mssql_legacy")    # MS SQL Server (legacy billing system)
CONN_REDIS        = conn("redis")           # Redis (RSP portal session cache)
CONN_RDS_POSTGRES = conn("rds_postgres")    # RDS PostgreSQL (operational reporting)
CONN_AZURE_BLOB   = conn("azure_blob")      # Azure Blob Storage (partner data)
CONN_GCS          = conn("gcs")             # Google Cloud Storage (regulatory)
CONN_IMAP         = conn("imap_reports")    # IMAP email (RSP manual report delivery)
CONN_ML_PLATFORM  = conn("ml_platform_http")  # Internal ML platform REST API (training trigger)


# ── Date/time helpers ──────────────────────────────────────────────────────────

AEST = timezone(timedelta(hours=10))


def get_execution_date(context: dict) -> datetime:
    """Return execution_date as an AEST-aware datetime."""
    ed = context["logical_date"]
    return ed.astimezone(AEST)


def get_run_date(context: dict) -> str:
    """Return execution date as YYYY-MM-DD string in AEST."""
    return get_execution_date(context).strftime("%Y-%m-%d")


def get_run_month(context: dict) -> str:
    """Return execution month as YYYY-MM string in AEST."""
    return get_execution_date(context).strftime("%Y-%m")


# ── Snowflake helpers ──────────────────────────────────────────────────────────

def get_snowflake_hook():
    """Return a SnowflakeHook using the standard NNN connection."""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    return SnowflakeHook(snowflake_conn_id=CONN_SNOWFLAKE)


def snowflake_run(sql: str, parameters: dict | None = None) -> None:
    """Execute a SQL statement on Snowflake and log row counts."""
    hook = get_snowflake_hook()
    hook.run(sql, parameters=parameters)
    log.info("Snowflake SQL executed successfully")


def snowflake_unload_to_s3(
    select_sql: str,
    s3_prefix: str,
    file_format: str = "PARQUET",
    overwrite: bool = True,
) -> str:
    """COPY INTO an S3 external stage from a Snowflake SELECT.

    Uses NNN_SNOWFLAKE_S3_STAGE (a named external stage pointing at NNN_S3_BUCKET).
    Returns the full S3 prefix written.

    Args:
        select_sql:  SQL SELECT to unload (without surrounding parens).
        s3_prefix:   Sub-path within the stage, e.g. 'network/perf/run_date=2024-01-01/'.
        file_format: PARQUET (default), CSV, or JSON.
        overwrite:   Drop existing files in prefix before writing (default True).
    """
    overwrite_clause = "OVERWRITE = TRUE" if overwrite else ""
    sql = f"""
        COPY INTO @{NNN_SNOWFLAKE_S3_STAGE}/{s3_prefix}
        FROM ({select_sql})
        FILE_FORMAT = (TYPE = {file_format})
        {overwrite_clause}
        HEADER = TRUE
    """
    snowflake_run(sql)
    full_s3 = f"s3://{NNN_S3_BUCKET}/{s3_prefix}"
    log.info("Snowflake UNLOAD complete → %s", full_s3)
    return full_s3


# ── Redshift helpers ───────────────────────────────────────────────────────────

def get_redshift_hook():
    """Return a RedshiftSQLHook using the standard NNN Redshift connection."""
    from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
    return RedshiftSQLHook(redshift_conn_id=CONN_REDSHIFT)


def redshift_copy_from_s3(
    table: str,
    s3_prefix: str,
    file_format: str = "PARQUET",
    iam_role: str | None = None,
    truncate: bool = True,
    extra_options: str = "",
) -> None:
    """Load data from S3 into a Redshift table using the COPY command.

    Args:
        table:        Fully qualified Redshift table (schema.table).
        s3_prefix:    S3 key prefix (no leading slash).
        file_format:  PARQUET (default), CSV, or JSON.
        iam_role:     IAM role ARN; defaults to NNN_REDSHIFT_IAM_ROLE env var.
        truncate:     TRUNCATE table before COPY (idempotent reloads).
        extra_options: Additional COPY options appended verbatim.
    """
    hook = get_redshift_hook()
    role = iam_role or NNN_REDSHIFT_IAM_ROLE

    if truncate:
        hook.run(f"TRUNCATE {table}")
        log.info("Truncated Redshift table %s before COPY", table)

    copy_sql = f"""
        COPY {table}
        FROM 's3://{NNN_S3_BUCKET}/{s3_prefix}'
        IAM_ROLE '{role}'
        FORMAT AS {file_format}
        TRUNCATECOLUMNS
        BLANKSASNULL
        EMPTYASNULL
        {extra_options}
    """
    hook.run(copy_sql)
    log.info("Redshift COPY complete: s3://%s/%s → %s", NNN_S3_BUCKET, s3_prefix, table)


def redshift_unload_to_s3(
    select_sql: str,
    s3_prefix: str,
    iam_role: str | None = None,
    parallel: bool = True,
    header: bool = True,
) -> str:
    """Export data from Redshift to S3 as gzipped CSV using UNLOAD.

    Returns the full S3 prefix written.
    """
    hook     = get_redshift_hook()
    role     = iam_role or NNN_REDSHIFT_IAM_ROLE
    parallel_opt = "" if parallel else "PARALLEL OFF"
    header_opt   = "HEADER" if header else ""

    unload_sql = f"""
        UNLOAD ('{select_sql}')
        TO 's3://{NNN_S3_BUCKET}/{s3_prefix}'
        IAM_ROLE '{role}'
        FORMAT AS CSV
        GZIP
        ALLOWOVERWRITE
        {parallel_opt}
        {header_opt}
    """
    hook.run(unload_sql)
    full_s3 = f"s3://{NNN_S3_BUCKET}/{s3_prefix}"
    log.info("Redshift UNLOAD complete → %s", full_s3)
    return full_s3


def redshift_run(sql: str, parameters: dict | None = None) -> None:
    """Execute a SQL statement on Redshift."""
    get_redshift_hook().run(sql, parameters=parameters)
    log.info("Redshift SQL executed successfully")


# ── MS SQL Server helper ───────────────────────────────────────────────────────

def get_mssql_hook():
    """Return a MsSqlHook using the NNN legacy billing connection."""
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
    return MsSqlHook(mssql_conn_id=CONN_MSSQL)


# ── S3 helpers ─────────────────────────────────────────────────────────────────

def s3_key(domain: str, table: str, run_date: str, ext: str = "parquet") -> str:
    """Canonical S3 key: <domain>/<table>/run_date=YYYY-MM-DD/data.<ext>"""
    return f"{domain}/{table}/run_date={run_date}/data.{ext}"


def upload_to_s3(local_path: str, s3_key_path: str) -> str:
    """Upload a local file to the NNN data lake and return the full S3 URI."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    hook = S3Hook(aws_conn_id=CONN_S3)
    hook.load_file(local_path, s3_key_path, bucket_name=NNN_S3_BUCKET, replace=True)
    uri = f"s3://{NNN_S3_BUCKET}/{s3_key_path}"
    log.info("Uploaded %s → %s", local_path, uri)
    return uri


def download_from_s3(s3_key_path: str, local_path: str) -> str:
    """Download a file from the NNN data lake to a local path."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    hook = S3Hook(aws_conn_id=CONN_S3)
    obj  = hook.get_key(s3_key_path, bucket_name=NNN_S3_BUCKET)
    with open(local_path, "wb") as f:
        f.write(obj.get()["Body"].read())
    log.info("Downloaded s3://%s/%s → %s", NNN_S3_BUCKET, s3_key_path, local_path)
    return local_path


# ── Validation helpers ─────────────────────────────────────────────────────────

def assert_row_count(table: str, run_date: str, min_rows: int = 1) -> None:
    """Raise ValueError if Snowflake table has fewer than min_rows for run_date."""
    hook = get_snowflake_hook()
    row = hook.get_first(
        f"SELECT COUNT(*) FROM {table} WHERE run_date = %(d)s",
        parameters={"d": run_date},
    )
    rows = row[0] if row else 0
    log.info("Row count for %s on %s: %d", table, run_date, rows)
    if rows < min_rows:
        raise ValueError(
            f"Data quality check failed: {table} has {rows} rows "
            f"for {run_date} (expected >= {min_rows})"
        )


def assert_redshift_row_count(table: str, run_date: str, min_rows: int = 1) -> None:
    """Raise ValueError if Redshift table has fewer than min_rows for run_date."""
    hook = get_redshift_hook()
    row = hook.get_first(
        f"SELECT COUNT(*) FROM {table} WHERE run_date = %s",
        parameters=(run_date,),
    )
    rows = row[0] if row else 0
    log.info("Redshift row count for %s on %s: %d", table, run_date, rows)
    if rows < min_rows:
        raise ValueError(
            f"Redshift data quality check failed: {table} has {rows} rows "
            f"for {run_date} (expected >= {min_rows})"
        )
