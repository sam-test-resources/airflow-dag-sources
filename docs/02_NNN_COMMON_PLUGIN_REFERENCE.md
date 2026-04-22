# `nnn_common` Plugin Reference

**Audience:** Developers writing or reviewing NNN Airflow DAGs.  
**Location:** `plugins/nnn_common/`  
**PYTHONPATH note:** Airflow (and Astronomer) automatically adds `plugins/` to `sys.path`. No manual path manipulation needed in production.

---

## Table of Contents

1. [Plugin Structure](#1-plugin-structure)
2. [nnn_common.alerts](#2-nnn_commonalerts)
   - [nnn_failure_alert](#nnn_failure_alert)
   - [nnn_sla_miss_alert](#nnn_sla_miss_alert)
   - [nnn_post_slack_message](#nnn_post_slack_message)
3. [nnn_common.utils — Environment & Constants](#3-nnn_commonutils--environment--constants)
4. [nnn_common.utils — Connection ID Constants](#4-nnn_commonutils--connection-id-constants)
5. [nnn_common.utils — Date / Context Helpers](#5-nnn_commonutils--date--context-helpers)
6. [nnn_common.utils — Snowflake Helpers](#6-nnn_commonutils--snowflake-helpers)
7. [nnn_common.utils — Redshift Helpers](#7-nnn_commonutils--redshift-helpers)
8. [nnn_common.utils — S3 / File Helpers](#8-nnn_commonutils--s3--file-helpers)
9. [nnn_common.utils — Data Quality Helpers](#9-nnn_commonutils--data-quality-helpers)
10. [nnn_common.utils — Miscellaneous Hooks](#10-nnn_commonutils--miscellaneous-hooks)
11. [Extending the Plugin](#11-extending-the-plugin)

---

## 1. Plugin Structure

```
plugins/
└── nnn_common/
    ├── __init__.py          (empty)
    ├── alerts.py            alert callbacks for on_failure_callback and sla_miss_callback
    └── utils.py             connection constants, hook factories, S3/Snowflake/Redshift helpers
```

Import in DAG files:

```python
from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils  import CONN_SNOWFLAKE, get_run_date, get_snowflake_hook
```

---

## 2. `nnn_common.alerts`

### `nnn_failure_alert`

```python
def nnn_failure_alert(context: dict) -> None
```

**Used as:** `"on_failure_callback": nnn_failure_alert` in `default_args`

**Behaviour:**
1. Extracts `dag_id`, `task_id`, `run_id`, `logical_date`, and the exception message from `context`.
2. Posts a structured Slack message to `#de-alerts` via `NNN_SLACK_WEBHOOK_URL`.
3. Triggers a **PagerDuty P2 alert** via `NNN_PAGERDUTY_ROUTING_KEY`.  
   Exception: for DAGs whose `dag_id` contains `billing` or `compliance`, triggers **PagerDuty P1**.

**Example Slack message:**
```
🚨 *DAG Failure* | nnn_cvc_billing_daily
Task: `load_snowflake` failed on run 2024-06-01T05:00:00+00:00
Error: OperationalError: Connection timeout after 30s
Airflow: https://airflow.nnnco.com.au/log?dag_id=nnn_cvc_billing_daily&task_id=load_snowflake
```

**Never call directly** — Airflow calls this automatically on task failure.

---

### `nnn_sla_miss_alert`

```python
def nnn_sla_miss_alert(
    dag: DAG,
    task_list: list,
    blocking_task_list: list,
    slas: list,
    blocking_tis: list,
) -> None
```

**Used as:** `sla_miss_callback=nnn_sla_miss_alert` on the `DAG(...)` constructor (not in `default_args`).

**Behaviour:**
1. Posts a Slack warning to `#de-alerts-sla`.
2. Triggers **PagerDuty P1** for DAGs tagged `REGULATORY` or `CRITICAL`; P2 otherwise.

**Which DAGs use it:** All DAGs with a defined SLA (see README for the full list). Omit from near-real-time DAGs with no SLA (e.g., `nnn_rabbitmq_order_events_15min`).

```python
# Correct: DAG with SLA
with DAG(
    ...,
    sla_miss_callback=nnn_sla_miss_alert,
) as dag:

# Correct: near-real-time DAG, no SLA
with DAG(
    dag_id="nnn_rabbitmq_order_events_15min",
    # sla_miss_callback intentionally omitted
) as dag:
```

---

### `nnn_post_slack_message`

```python
def nnn_post_slack_message(message: str) -> None
```

**Parameters:**
- `message` (`str`) — Plain text or Slack markdown message body.

**Returns:** `None`

**Use case:** Sending custom Slack notifications from inside a PythonOperator callable (e.g., reporting a summary count at the end of a load task). Not intended for failure alerting — use `on_failure_callback` for that.

```python
def load_to_snowflake(**context) -> None:
    # ... insert logic ...
    nnn_post_slack_message(
        f":white_check_mark: Loaded {len(records)} partner records into "
        f"WHOLESALE.PARTNER_DATA_INGEST for {run_date}"
    )
```

---

## 3. `nnn_common.utils` — Environment & Constants

### `NNN_ENV`

```python
NNN_ENV: str  # "prod" | "staging" | "dev"
```

Read from the `NNN_ENV` environment variable. Defaults to `"prod"` if unset. Affects which connection IDs `conn()` generates.

```python
import os
NNN_ENV = os.getenv("NNN_ENV", "prod")
```

### `NNN_S3_BUCKET`

```python
NNN_S3_BUCKET: str  # "nnn-data-lake-prod" | "nnn-data-lake-staging" | "nnn-data-lake-dev"
```

The data lake bucket for the current environment. Always reference this constant — never hardcode the bucket name in DAG code.

```python
s3_uri = f"s3://{NNN_S3_BUCKET}/network/link_performance/run_date={run_date}/"
```

### `NNN_REGION`

```python
NNN_REGION: str  # "ap-southeast-2"
```

AWS region for all NNN services. Always Sydney.

### `NNN_REDSHIFT_IAM_ROLE`

```python
NNN_REDSHIFT_IAM_ROLE: str  # "arn:aws:iam::123456789:role/NNN-Redshift-S3-Role-prod"
```

IAM role ARN passed to Redshift `COPY` and `UNLOAD` commands. `redshift_copy_from_s3()` and `redshift_unload_to_s3()` use this automatically — you do not need to pass it manually.

### `NNN_SNOWFLAKE_S3_STAGE`

```python
NNN_SNOWFLAKE_S3_STAGE: str  # "nnn_s3_stage_prod"
```

The Snowflake external stage that points to `NNN_S3_BUCKET`. Used in `COPY INTO` statements:

```python
copy_sql = f"""
    COPY INTO WHOLESALE.PARTNER_DATA_INGEST
    FROM '@{NNN_SNOWFLAKE_S3_STAGE}/{s3_key}'
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
"""
```

### `AEST`

```python
AEST: timezone  # datetime.timezone(timedelta(hours=10))
```

UTC+10 fixed-offset timezone object. Used internally by `get_execution_date()` and `get_run_date()` to convert Airflow's UTC execution timestamp to the AEST business date. **Do not use `AEST` in `start_date`** — pass `datetime(2024, 1, 1)` with no tzinfo.

### `conn(system: str) -> str`

```python
def conn(system: str) -> str
```

Returns `f"nnn_{system}_{NNN_ENV}"`. The single source of truth for connection ID generation.

```python
conn("snowflake")    # → "nnn_snowflake_prod"
conn("redis")        # → "nnn_redis_prod"
```

---

## 4. `nnn_common.utils` — Connection ID Constants

All constants follow the `CONN_<SYSTEM>` naming pattern. The value is the Airflow Connection ID for the current environment.

| Constant | Airflow Connection ID (prod) | System |
|---|---|---|
| `CONN_SNOWFLAKE` | `nnn_snowflake_prod` | Snowflake analytics DW |
| `CONN_ORACLE_EBS` | `nnn_oracle_ebs_prod` | Oracle EBS billing / AR |
| `CONN_ORACLE_OSS` | `nnn_oracle_oss_prod` | Oracle OSS provisioning |
| `CONN_POSTGRES_NI` | `nnn_postgres_ni_prod` | Network Inventory (on-prem) |
| `CONN_SERVICENOW` | `nnn_servicenow_prod` | ServiceNow ITSM |
| `CONN_SALESFORCE` | `nnn_salesforce_prod` | Salesforce CRM |
| `CONN_SAP` | `nnn_sap_erp_prod` | SAP ERP (PS module) |
| `CONN_MEDALLIA` | `nnn_medallia_prod` | Medallia NPS platform |
| `CONN_NMS_API` | `nnn_nms_api_prod` | NMS REST API |
| `CONN_SQ_API` | `nnn_sq_api_prod` | Service Qualification API |
| `CONN_POWERBI` | `nnn_powerbi_prod` | PowerBI REST API |
| `CONN_ML_PLATFORM` | `nnn_ml_platform_http_prod` | Internal ML platform API |
| `CONN_REDSHIFT` | `nnn_redshift_prod` | Amazon Redshift |
| `CONN_S3` | `nnn_s3_prod` | AWS S3 data lake |
| `CONN_DYNAMODB` | `nnn_dynamodb_prod` | Amazon DynamoDB |
| `CONN_KINESIS` | `nnn_kinesis_prod` | Amazon Kinesis Data Streams |
| `CONN_SQS` | `nnn_sqs_prod` | Amazon SQS |
| `CONN_FIREHOSE` | `nnn_firehose_prod` | Amazon Kinesis Firehose |
| `CONN_GLUE` | `nnn_glue_prod` | AWS Glue Data Catalog |
| `CONN_SFTP_RSP` | `nnn_sftp_rsp_prod` | SFTP (RSP file drops) |
| `CONN_MONGODB` | `nnn_mongodb_prod` | MongoDB (NOC tools) |
| `CONN_ELASTICSEARCH` | `nnn_elasticsearch_prod` | Elasticsearch |
| `CONN_MQTT` | `nnn_mqtt_prod` | MQTT broker (lineage only) |
| `CONN_RABBITMQ` | `nnn_rabbitmq_prod` | RabbitMQ |
| `CONN_MSSQL` | `nnn_mssql_legacy_prod` | MS SQL Server (legacy billing) |
| `CONN_REDIS` | `nnn_redis_prod` | Redis (session cache) |
| `CONN_RDS_POSTGRES` | `nnn_rds_postgres_prod` | RDS PostgreSQL (ops reporting) |
| `CONN_AZURE_BLOB` | `nnn_azure_blob_prod` | Azure Blob Storage |
| `CONN_GCS` | `nnn_gcs_prod` | Google Cloud Storage |
| `CONN_IMAP` | `nnn_imap_reports_prod` | IMAP email |
| `CONN_KAFKA` | `nnn_kafka_prod` | Kafka broker |

---

## 5. `nnn_common.utils` — Date / Context Helpers

### `get_execution_date`

```python
def get_execution_date(context: dict) -> datetime
```

**Parameters:**
- `context` (`dict`) — The Airflow task context dict (passed as `**context` to a PythonOperator callable).

**Returns:** `datetime` — The DAG run's logical (execution) date as a **timezone-aware datetime in AEST** (UTC+10 fixed offset).

**When to use:** When you need the full timestamp (not just the date), e.g., computing hour windows for hourly DAGs.

```python
def load_telemetry(**context) -> None:
    execution_dt: datetime = get_execution_date(context)
    execution_hour: int = execution_dt.hour     # 0–23, AEST
    window_start = execution_dt.replace(minute=0, second=0, microsecond=0)
    window_end   = window_start + timedelta(hours=1)
```

**Implementation note:** Reads `context["execution_date"]` (Airflow's UTC timestamp) and converts it to AEST via `datetime.astimezone(AEST)`.

---

### `get_run_date`

```python
def get_run_date(context: dict) -> str
```

**Parameters:**
- `context` (`dict`) — Airflow task context.

**Returns:** `str` — The business date in `YYYY-MM-DD` format (AEST).

**When to use:** The most commonly used helper. Use for all date-partitioned queries and S3 key construction.

```python
def extract(**context) -> None:
    run_date = get_run_date(context)          # e.g., "2024-06-01"
    sql = "SELECT * FROM tbl WHERE date = %(run_date)s"
    cursor.execute(sql, {"run_date": run_date})
    
    s3_prefix = f"network/link_performance/run_date={run_date}/"
```

**Important:** For a DAG scheduled at `0 2 * * *` (02:00 AEST), the execution_date in Airflow is the **previous day's midnight UTC** (i.e., the day the interval *started*). `get_run_date()` handles this conversion correctly — you get the AEST calendar date of the scheduled run, which is almost always what you want.

---

### `get_run_month`

```python
def get_run_month(context: dict) -> str
```

**Returns:** `str` — The business month in `YYYY-MM` format (AEST).

**When to use:** Monthly DAGs and S3 partitioning by month.

```python
run_month = get_run_month(context)   # e.g., "2024-06"
gcs_path  = f"{run_month[:4]}/{run_month}/weekly_report_{run_date}.csv"
```

---

## 6. `nnn_common.utils` — Snowflake Helpers

### `get_snowflake_hook`

```python
def get_snowflake_hook() -> SnowflakeHook
```

**Returns:** `SnowflakeHook` configured with `CONN_SNOWFLAKE`.

**When to use:** Any time you need a raw Snowflake connection for DML operations (INSERT, MERGE, DELETE) or cursor-level control.

```python
hook   = get_snowflake_hook()
conn   = hook.get_conn()
cursor = conn.cursor()
try:
    cursor.execute("SELECT 1")
    cursor.execute("INSERT INTO ... VALUES (...)", {"param": value})
    conn.commit()
finally:
    cursor.close()
    conn.close()
```

**Critical:** Always close `cursor` and `conn` in a `finally` block. Snowflake connections are not automatically released.

**Do not call** `hook.get_conn()` multiple times across separate tasks — TEMPORARY tables are session-scoped and will not survive a new connection.

---

### `snowflake_run`

```python
def snowflake_run(sql: str, parameters: dict | None = None) -> None
```

**Parameters:**
- `sql` (`str`) — SQL statement to execute.
- `parameters` (`dict | None`) — Named parameter bindings (`%(name)s` style). Default: `None`.

**Returns:** `None`

**When to use:** Simple fire-and-forget DDL/DML where you don't need to inspect results — DELETE, INSERT INTO ... SELECT, MERGE, CREATE TABLE AS. Handles connection lifecycle internally.

```python
# Delete + reinsert KPI pattern
snowflake_run(
    "DELETE FROM ML.SESSION_KPI_DAILY WHERE KPI_DATE = %(run_date)s",
    parameters={"run_date": run_date},
)
snowflake_run(
    "INSERT INTO ML.SESSION_KPI_DAILY SELECT ... FROM ML.RSP_SESSION_ANALYTICS WHERE ...",
    parameters={"run_date": run_date},
)
```

**When NOT to use:** When you need to fetch rows (use `hook.get_conn().cursor().fetchall()`) or when you need multiple statements to share a session for TEMPORARY TABLE operations (use `get_snowflake_hook().get_conn()` directly).

---

### `snowflake_unload_to_s3`

```python
def snowflake_unload_to_s3(
    select_sql: str,
    s3_prefix: str,
    file_format: str = "PARQUET",
    overwrite: bool = True,
) -> str
```

**Parameters:**
- `select_sql` (`str`) — The `SELECT` statement whose results are exported. Must be a complete SQL statement.
- `s3_prefix` (`str`) — Relative S3 path under `NNN_S3_BUCKET`. Example: `"ml-exports/customer_features/run_date=2024-06-01/"`. **Do not include** `s3://bucket/`.
- `file_format` (`str`) — `"PARQUET"` (default) or `"CSV"`.
- `overwrite` (`bool`) — If `True`, removes existing files at the prefix before writing. Default: `True`.

**Returns:** `str` — Full S3 URI of the exported data, e.g., `"s3://nnn-data-lake-prod/ml-exports/customer_features/run_date=2024-06-01/"`.

**How it works internally:** Issues a Snowflake `COPY INTO '@<stage>/<s3_prefix>' FROM (<select_sql>) FILE_FORMAT = (<format>)` command using `NNN_SNOWFLAKE_S3_STAGE`.

```python
s3_path = snowflake_unload_to_s3(
    select_sql=f"SELECT * FROM ML.CUSTOMER_FEATURES WHERE feature_date = '{run_date}'",
    s3_prefix=f"ml-exports/customer_features/run_date={run_date}/",
    file_format="PARQUET",
    overwrite=True,
)
# s3_path → "s3://nnn-data-lake-prod/ml-exports/customer_features/run_date=2024-06-01/"
```

**Important:** The returned full S3 URI must **not** be passed directly to `redshift_copy_from_s3()` as the `s3_prefix` argument — that function prepends the bucket internally. Pass only the relative prefix.

```python
# WRONG — double-prefix bug
redshift_copy_from_s3(table="analytics.foo", s3_prefix=s3_path)     # s3_path is "s3://bucket/..."

# CORRECT — keep relative prefix in a separate variable
s3_prefix = f"ml-exports/customer_features/run_date={run_date}/"
s3_path   = snowflake_unload_to_s3(select_sql=sql, s3_prefix=s3_prefix, ...)
redshift_copy_from_s3(table="analytics.foo", s3_prefix=s3_prefix)   # relative only
```

---

## 7. `nnn_common.utils` — Redshift Helpers

### `get_redshift_hook`

```python
def get_redshift_hook() -> RedshiftSQLHook
```

**Returns:** `RedshiftSQLHook` configured with `CONN_REDSHIFT`.

**When to use:** All Redshift DDL/DML operations and queries.

```python
hook = get_redshift_hook()

# Simple DML
hook.run("DELETE FROM analytics.foo WHERE date_col = %s", parameters=(run_date,))

# Query with result
row = hook.get_first("SELECT COUNT(*) FROM analytics.foo WHERE date_col = %s", parameters=(run_date,))
count = row[0] if row else 0
```

**Note:** `RedshiftSQLHook.run()` uses **positional `%s` parameters with tuple** — opposite to Snowflake's named `%(name)s` with dict. Do not mix them.

---

### `redshift_copy_from_s3`

```python
def redshift_copy_from_s3(
    table: str,
    s3_prefix: str,
    file_format: str = "PARQUET",
    iam_role: str = NNN_REDSHIFT_IAM_ROLE,
    truncate: bool = False,
    extra_options: str = "",
) -> None
```

**Parameters:**
- `table` (`str`) — Fully qualified Redshift table: `"schema.table_name"`.
- `s3_prefix` (`str`) — **Relative** S3 path under `NNN_S3_BUCKET`. Must NOT include `s3://`.
- `file_format` (`str`) — `"PARQUET"` (default), `"CSV"`, or `"JSON"`.
- `iam_role` (`str`) — IAM role ARN. Defaults to `NNN_REDSHIFT_IAM_ROLE`; override only in exceptional cases.
- `truncate` (`bool`) — If `True`, runs `TRUNCATE table` before COPY. **Only use for full-refresh tables with no date partitioning** (e.g., `finance.capex_tracking`). For date-partitioned tables, always use `truncate=False` with a preceding DELETE.
- `extra_options` (`str`) — Additional COPY options appended verbatim, e.g., `"EMPTYASNULL BLANKSASNULL"`.

**Returns:** `None`

**Correct idempotent pattern for date-partitioned tables:**

```python
def load_to_redshift(**context) -> None:
    run_date     = get_run_date(context)
    s3_key_path  = context["ti"].xcom_pull(task_ids="export_snowflake", key="s3_prefix")

    hook = get_redshift_hook()
    # Step 1: Remove today's rows first (idempotent)
    hook.run(
        "DELETE FROM analytics.network_performance_daily WHERE report_date = %s",
        parameters=(run_date,),
    )
    # Step 2: COPY fresh data from S3
    redshift_copy_from_s3(
        table="analytics.network_performance_daily",
        s3_prefix=s3_key_path,   # relative path only
        file_format="PARQUET",
        truncate=False,           # NEVER True for date-partitioned tables
    )
```

---

### `redshift_unload_to_s3`

```python
def redshift_unload_to_s3(
    select_sql: str,
    s3_prefix: str,
    iam_role: str = NNN_REDSHIFT_IAM_ROLE,
    parallel: bool = True,
    header: bool = False,
) -> str
```

**Returns:** `str` — Full S3 URI of the exported files.

**When to use:** Exporting data from Redshift to S3 (e.g., for regulatory reporting or reverse ETL).

```python
s3_uri = redshift_unload_to_s3(
    select_sql=f"SELECT * FROM analytics.nps_sentiment WHERE week_ending = '{run_date}'",
    s3_prefix=f"redshift-exports/nps/{run_date}/",
    header=True,    # include column headers in CSV
)
```

---

### `redshift_run`

```python
def redshift_run(sql: str, parameters: tuple | None = None) -> None
```

Convenience wrapper for Redshift DML. Uses **positional `%s`** with `tuple` (Redshift / psycopg2 style).

```python
redshift_run(
    "DELETE FROM compliance.sla_breaches WHERE breach_date = %s",
    parameters=(run_date,),
)
```

---

### `assert_redshift_row_count`

```python
def assert_redshift_row_count(
    table: str,
    run_date: str,
    min_rows: int = 1,
    date_column: str = "run_date",
) -> None
```

**Parameters:**
- `table` (`str`) — Fully qualified Redshift table.
- `run_date` (`str`) — The date to check (`YYYY-MM-DD`).
- `min_rows` (`int`) — Minimum acceptable row count. Default: `1`.
- `date_column` (`str`) — Column to filter on. Default: `"run_date"`. Override when the table uses a different column name.

**Raises:** `ValueError` if `COUNT(*) < min_rows`.

```python
# Default — uses 'run_date' column
assert_redshift_row_count(table="analytics.network_performance_daily", run_date=run_date)

# Override column name when table uses a different date column
assert_redshift_row_count(
    table="wholesale.rsp_activations_daily",
    run_date=run_date,
    date_column="activation_date",
    min_rows=10,
)
```

---

## 8. `nnn_common.utils` — S3 / File Helpers

### `s3_key`

```python
def s3_key(domain: str, table: str, run_date: str, ext: str = "parquet") -> str
```

**Returns:** `str` — Canonical relative S3 key: `"<domain>/<table>/run_date=<run_date>/data.<ext>"`

**When to use:** Constructing standard data-lake keys for regular ETL outputs. Always prefer this over manual string formatting.

```python
key = s3_key("analytics", "rsp_portal_sessions", run_date, ext="json")
# → "analytics/rsp_portal_sessions/run_date=2024-06-01/data.json"
upload_to_s3(local_path, key)
```

---

### `upload_to_s3`

```python
def upload_to_s3(local_path: str, s3_key_path: str) -> str
```

**Parameters:**
- `local_path` (`str`) — Absolute local filesystem path of the file to upload.
- `s3_key_path` (`str`) — **Relative** key under `NNN_S3_BUCKET`. Do not include `s3://bucket/`.

**Returns:** `str` — Full S3 URI: `f"s3://{NNN_S3_BUCKET}/{s3_key_path}"`.

**Critical — do not pass the return value as `s3_prefix` to `redshift_copy_from_s3()`:** That function prepends the bucket internally. Pass only `s3_key_path` (the relative key):

```python
dest_key = s3_key("analytics", "sessions", run_date, ext="json")
upload_to_s3(local_path, dest_key)           # returns full URI — discard or log it
redshift_copy_from_s3(table="...", s3_prefix=dest_key, file_format="JSON")  # relative key
```

---

### `download_from_s3`

```python
def download_from_s3(s3_key_path: str, local_path: str) -> str
```

**Parameters:**
- `s3_key_path` (`str`) — Relative key under `NNN_S3_BUCKET`.
- `local_path` (`str`) — Target filesystem path to write the file.

**Returns:** `str` — The `local_path` written.

```python
s3_key_path = _build_s3_telemetry_key(run_date, execution_hour)
download_from_s3(s3_key_path, local_path)

with open(local_path) as f:
    for line in f:
        record = json.loads(line)
```

---

## 9. `nnn_common.utils` — Data Quality Helpers

### `assert_row_count`

```python
def assert_row_count(
    table: str,
    run_date: str,
    min_rows: int = 1,
) -> None
```

Snowflake equivalent of `assert_redshift_row_count`. Queries `SELECT COUNT(*) FROM <table> WHERE run_date = '<run_date>'`.

**Raises:** `ValueError` if count < `min_rows`.

```python
def validate(**context) -> None:
    run_date = get_run_date(context)
    assert_row_count(table="WHOLESALE.PARTNER_DATA_INGEST", run_date=run_date, min_rows=1)
    log.info("Validation passed for %s on %s", "WHOLESALE.PARTNER_DATA_INGEST", run_date)
```

**Limitation:** Assumes the table has a `run_date` column. For tables with different date column names, write an inline count query using `get_snowflake_hook()` directly.

---

## 10. `nnn_common.utils` — Miscellaneous Hooks

### `get_mssql_hook`

```python
def get_mssql_hook() -> MsSqlHook
```

**Returns:** `MsSqlHook` configured with `CONN_MSSQL`.

```python
def extract_legacy_billing(**context) -> None:
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

    hook   = get_mssql_hook()
    result = hook.get_records(
        "SELECT TOP 1000 * FROM dbo.BillingRecords WHERE BillDate = %s",
        parameters=(run_date,),
    )
```

> **Note:** MS SQL Server uses positional `%s` with tuple (same as Redshift/psycopg2), not Snowflake's `%(name)s` with dict.

---

## 11. Extending the Plugin

### Adding a new helper function

1. Add the function to `plugins/nnn_common/utils.py`.
2. Keep the function **pure and stateless** — no Airflow context, no global mutable state.
3. Use lazy imports (import inside the function body) for heavy provider packages.
4. Add unit tests in `tests/test_utils.py`.
5. Add a row to the helper function table in `README.md` and to this document.

### Adding a new connection constant

```python
# In plugins/nnn_common/utils.py, inside the connection constants block:
CONN_HUBSPOT = conn("hubspot_api")    # ← follow alphabetical grouping
```

Then:
- Add the Airflow connection in UI / env var for every environment.
- Document in `README.md` and in the connection table in this document (Section 4).

### Plugin import errors

If you see `ModuleNotFoundError: No module named 'nnn_common'` locally:

```bash
export PYTHONPATH="$(pwd)/plugins:$PYTHONPATH"
```

In Docker/Compose-based Airflow setups, mount `plugins/` and ensure `PYTHONPATH` is set in the container.
