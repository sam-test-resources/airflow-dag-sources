# New DAG Development Guide

**Audience:** Developers adding a new pipeline to the NNN Airflow DAG repository.  
**Prerequisite reading:** `README.md` (conventions overview), `plugins/nnn_common/utils.py` (shared helpers).

---

## Table of Contents

1. [Local Environment Setup](#1-local-environment-setup)
2. [The Anatomy of an NNN DAG](#2-the-anatomy-of-an-nnn-dag)
3. [Choosing the Right Pattern](#3-choosing-the-right-pattern)
4. [Step-by-Step: Building a New DAG](#4-step-by-step-building-a-new-dag)
5. [Adding a New Connection](#5-adding-a-new-connection)
6. [Testing Your DAG](#6-testing-your-dag)
7. [Pre-Merge Checklist](#7-pre-merge-checklist)
8. [Common Mistakes](#8-common-mistakes)

---

## 1. Local Environment Setup

### Python environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install apache-airflow[amazon,snowflake,postgres,mongo,redis,rabbitmq,microsoft.azure,google,elasticsearch,imap]==2.9.*
pip install pandas openpyxl snowflake-connector-python elasticsearch
```

### PYTHONPATH

The shared plugin lives in `plugins/`. Add it to your path so imports resolve locally:

```bash
export PYTHONPATH="$(pwd)/plugins:$PYTHONPATH"
```

Or add to `.env`:

```
PYTHONPATH=/path/to/nnn_airflow_dags/plugins
```

### Airflow configuration (local only)

```bash
export AIRFLOW_HOME=$(pwd)/.airflow
airflow db init
airflow users create --username admin --password admin --role Admin --email admin@localhost --firstname A --lastname B
```

Point `dags_folder` at this repo's `dags/` directory in `airflow.cfg` or via:

```bash
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
```

---

## 2. The Anatomy of an NNN DAG

Every DAG file is structured in this **exact order**. Deviating from this layout breaks the scaffolding agent's convention extractor.

```
① from __future__ import annotations          ← always first line
② stdlib imports
③ airflow core imports
④ nnn_common imports
⑤ log = logging.getLogger(__name__)
⑥ default_args = { ... }
⑦ module-level constants
⑧ task functions (def callable(**context))    ← ALWAYS module-level, never inside with DAG()
⑨ with DAG(...) as dag:
      task definitions (PythonOperator, etc.)  ← only operator instantiation here
      dependency chain                         ← task_a >> task_b >> task_c
```

### Annotated example skeleton

```python
"""
nnn_<domain>_<description>_<frequency>
-----------------------------------------
Owner:      nnn-data-engineering
Domain:     <Domain> / <Subdomain>
Schedule:   <Human description> (<cron>)
SLA:        <duration or "None">

<One-paragraph description of what this DAG does and why.>

Steps:
  1. <task_id_1>  — <what it does>
  2. <task_id_2>  — <what it does>

Upstream:   <what feeds this DAG>
Downstream: <what consumes this DAG's output>
"""
from __future__ import annotations                        # ① always first

import logging                                            # ② stdlib
from datetime import datetime, timedelta

from airflow import DAG                                   # ③ airflow core
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert   # ④ nnn_common
from nnn_common.utils import (
    CONN_SNOWFLAKE,
    get_run_date,
    get_snowflake_hook,
)

log = logging.getLogger(__name__)                         # ⑤ module logger

# ⑥ Default arguments — copy this block verbatim, adjust only execution_timeout
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

TARGET_TABLE = "SCHEMA.TABLE_NAME"                        # ⑦ module constants

# ⑧ Task functions — MUST be at module level
def extract(**context) -> None:
    """Fetch data from source and push to XCom or temp file."""
    from airflow.providers.some.hooks.hook import SomeHook  # provider hooks import INSIDE function
    run_date = get_run_date(context)
    # ... logic ...

def load(**context) -> None:
    """Insert/merge data into target system."""
    # ... logic ...

def validate(**context) -> None:
    """Assert data quality — raise ValueError if check fails."""
    # ... logic ...

# ⑨ DAG definition block — operator instantiation and wiring ONLY
with DAG(
    dag_id="nnn_<domain>_<description>_<frequency>",
    description="<one-line description>",
    default_args=default_args,
    schedule_interval="<cron>",
    start_date=datetime(2024, 1, 1),   # always naive datetime, never tzinfo=AEST
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,   # omit only if there is no SLA
    tags=["nnn", "<domain>", "<source-system>", "<target-system>", "<frequency>"],
) as dag:

    t_extract = PythonOperator(         # inline comment explains the task's role
        task_id="extract",
        python_callable=extract,
    )

    t_load = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    t_validate = PythonOperator(
        task_id="validate",
        python_callable=validate,
    )

    t_extract >> t_load >> t_validate
```

### Key structural rules

| Rule | Why it matters |
|---|---|
| Task functions at **module level** (not inside `with DAG()`) | Functions re-defined inside the context manager are recreated on every scheduler parse, wasting memory and making profiling confusing |
| Provider **hooks** imported **inside the function** | Avoids `ImportError` crashes when a provider package is not installed in the environment — the import only runs in the worker |
| Provider **operators** (sensors, HTTP operators) imported at **module level** or inside `with DAG()` | Operators must be available when Airflow parses the DAG file |
| `start_date=datetime(2024, 1, 1)` — **no `tzinfo`** | Airflow's scheduler expects naive datetimes; adding `tzinfo=AEST` causes silent scheduling bugs |
| One inline comment per `PythonOperator` definition | The scaffolding agent reads these comments to understand task intent |

---

## 3. Choosing the Right Pattern

### Source system → pattern → example DAG

| Source system | Pattern | Example DAG to copy |
|---|---|---|
| REST API (HTTP/S) | `HttpHook` inside PythonOperator | `nnn_network_performance_daily.py` |
| SFTP / SSH file drop | `SSHHook` + sftp method | `nnn_sftp_rsp_billing_ingest_daily.py` |
| MongoDB | `MongoHook` | `nnn_mongodb_noc_incidents_sync_hourly.py` |
| Elasticsearch (read) | `ElasticsearchPythonHook` | `nnn_elasticsearch_fault_logs_daily.py` |
| AWS DynamoDB (read) | `DynamoDBHook` paginator | `nnn_dynamodb_portal_sessions_daily.py` |
| AWS Kinesis Data Stream | `AwsBaseHook` + kinesis client | `nnn_kinesis_clickstream_hourly.py` |
| AWS SQS | `SqsHook` | `nnn_sqs_provisioning_events_15min.py` |
| MQTT (IoT) | S3 sensor + PythonOperator | `nnn_mqtt_basestation_telemetry_hourly.py` |
| SOAP/XML API | `HttpHook` + xml.etree | `nnn_soap_mediation_cdr_daily.py` |
| GraphQL API | `HttpHook` + JSON body | `nnn_graphql_partner_portal_daily.py` |
| MS SQL Server | `get_mssql_hook()` | `nnn_mssql_legacy_billing_daily.py` |
| RabbitMQ | `RabbitMQHook` + pika | `nnn_rabbitmq_order_events_15min.py` |
| IMAP email + attachment | `ImapHook` | `nnn_imap_rsp_report_weekly.py` |
| Redis cache | `RedisHook` + scan_iter | `nnn_redis_session_analytics_daily.py` |
| Azure Blob Storage | `WasbHook` | `nnn_azure_blob_partner_data_daily.py` |
| Kafka | `confluent_kafka.Consumer` | `nnn_capacity_utilisation_hourly.py` |
| Oracle EBS/OSS | `OracleHook` | `nnn_cvc_billing_daily.py` |
| Snowflake (source) | `get_snowflake_hook()` | `nnn_snowflake_to_rds_postgres_daily.py` |

### Target system → pattern

| Target system | Pattern | Example DAG |
|---|---|---|
| Snowflake (INSERT/MERGE) | `get_snowflake_hook().get_conn()` + explicit cursor | `nnn_sftp_rsp_billing_ingest_daily.py` |
| Snowflake (COPY from S3) | `snowflake_run()` with COPY INTO | `nnn_azure_blob_partner_data_daily.py` |
| Redshift (COPY from S3) | `snowflake_unload_to_s3()` → `redshift_copy_from_s3()` | `nnn_network_performance_redshift_daily.py` |
| Redshift (direct insert) | `redshift_copy_from_s3()` | `nnn_dynamodb_portal_sessions_daily.py` |
| S3 (export) | `upload_to_s3()` or `snowflake_unload_to_s3()` | `nnn_snowflake_to_s3_ml_export_daily.py` |
| DynamoDB (write) | `AwsBaseHook` resource + `batch_writer()` | `nnn_snowflake_to_dynamodb_cache_daily.py` |
| Elasticsearch (index) | `ElasticsearchPythonHook` + `bulk()` | `nnn_snowflake_to_elasticsearch_daily.py` |
| RDS PostgreSQL | `PostgresHook` | `nnn_snowflake_to_rds_postgres_daily.py` |
| GCS | `GCSHook` | `nnn_snowflake_to_gcs_regulatory_weekly.py` |
| AWS Glue Catalog | `AwsBaseHook` + glue client | `nnn_s3_to_glue_catalog_daily.py` |

### Frequency → schedule and retry settings

| Frequency | `schedule_interval` | `retries` | `retry_delay` | `retry_exponential_backoff` |
|---|---|---|---|---|
| Monthly (last day) | `0 22 28-31 * *` + `ShortCircuitOperator` | `2` | `5 min` | `True` |
| Weekly | `0 <hh> * * <d>` | `2` | `5 min` | `True` |
| Daily | `0 <hh> * * *` | `2` | `5 min` | `True` |
| Hourly | `<mm> * * * *` | `2` | `5 min` | `True` |
| 15-min (near-RT) | `*/15 * * * *` | `3` | `1 min` | `False` |

---

## 4. Step-by-Step: Building a New DAG

We'll work through a complete example: *"Export daily churn predictions from Snowflake to Salesforce Contact objects"*.

### Step 1 — Name the DAG

Follow the pattern: `nnn_<domain>_<description>_<frequency>`

```
nnn_ml_churn_predictions_to_salesforce_daily
```

File: `dags/integrations/nnn_ml_churn_predictions_to_salesforce_daily.py`

### Step 2 — Write the module docstring

Fill in every field. This is the first thing the scaffolding agent and reviewers read.

```python
"""
nnn_ml_churn_predictions_to_salesforce_daily
----------------------------------------------
Owner:      nnn-data-engineering
Domain:     ML / Customer Retention
Schedule:   Daily at 09:00 AEST (0 9 * * *)
SLA:        2 hours

Exports daily churn propensity scores from ML.CHURN_PREDICTIONS (refreshed
overnight by the ML training pipeline) and upserts them into Salesforce
Contact records via the bulk upsert API, keyed on Service_ID__c.

Steps:
  1. export_predictions   — query ML.CHURN_PREDICTIONS for run_date, write to temp file
  2. upsert_salesforce    — bulk upsert Contact.Churn_Score__c and Churn_Risk_Band__c
  3. validate             — assert >= 1 Contact updated in Salesforce for run_date

Upstream:   nnn_snowflake_to_s3_ml_export_daily (ML.CHURN_PREDICTIONS refresh)
Downstream: Salesforce → Sales team churn intervention workflow
"""
```

### Step 3 — Identify required connections

Look at the source and target, find the right constant in `nnn_common/utils.py`:

```python
from nnn_common.utils import (
    CONN_SNOWFLAKE,      # already exists
    CONN_SALESFORCE,     # already exists
    get_run_date,
    get_snowflake_hook,
)
```

If the connection you need does **not** exist in `utils.py`, add it — see [Section 5](#5-adding-a-new-connection).

### Step 4 — Define `default_args` and constants

Copy the standard block. Only adjust `execution_timeout`:

```python
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
    "execution_timeout":         timedelta(hours=2),   # adjusted for Salesforce bulk API
}

SF_OBJECT    = "Contact"
SF_EXT_ID    = "Service_ID__c"        # external ID field for upsert matching
SF_CHUNK_SIZE = 200                    # Salesforce bulk API batch size
```

### Step 5 — Write task functions at module level

#### Extract (Snowflake → temp file)

Large payloads must go to a temp file, not XCom directly:

```python
def export_predictions(**context) -> None:
    """Query ML.CHURN_PREDICTIONS for run_date and write to temp JSON file."""
    import json
    import os
    import tempfile

    run_date = get_run_date(context)

    hook   = get_snowflake_hook()
    conn   = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT SERVICE_ID, CHURN_SCORE, CHURN_RISK_BAND, PREDICTED_AT
            FROM ML.CHURN_PREDICTIONS
            WHERE PREDICTION_DATE = %(run_date)s
            """,
            {"run_date": run_date},
        )
        columns = [col[0].lower() for col in cursor.description]
        rows    = cursor.fetchall()
        records = [dict(zip(columns, row)) for row in rows]
    finally:
        cursor.close()
        conn.close()

    if not records:
        raise ValueError(
            f"No churn predictions found for {run_date}. "
            "Check that nnn_snowflake_to_s3_ml_export_daily completed."
        )

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", prefix=f"churn_{run_date}_", delete=False
    ) as tmp:
        json.dump(records, tmp)
        tmp_path = tmp.name

    context["ti"].xcom_push(key="predictions_path", value=tmp_path)
    context["ti"].xcom_push(key="record_count",     value=len(records))
    log.info("Exported %d churn predictions for %s", len(records), run_date)
```

#### Load (temp file → Salesforce bulk upsert)

```python
def upsert_salesforce(**context) -> None:
    """Bulk upsert churn scores into Salesforce Contact records."""
    import json
    import os
    from simple_salesforce import Salesforce, SFType

    from airflow.providers.salesforce.hooks.salesforce import SalesforceHook

    predictions_path = context["ti"].xcom_pull(task_ids="export_predictions", key="predictions_path")
    with open(predictions_path) as fh:
        records = json.load(fh)

    hook = SalesforceHook(salesforce_conn_id=CONN_SALESFORCE)
    sf   = hook.get_conn()

    updated_count = 0
    for i in range(0, len(records), SF_CHUNK_SIZE):
        chunk = records[i : i + SF_CHUNK_SIZE]
        sf_records = [
            {
                SF_EXT_ID:           r["service_id"],
                "Churn_Score__c":    r["churn_score"],
                "Churn_Risk_Band__c": r["churn_risk_band"],
                "Churn_Predicted_At__c": r["predicted_at"],
            }
            for r in chunk
        ]
        result = sf.bulk.Contact.upsert(sf_records, SF_EXT_ID)
        errors = [r for r in result if r["success"] is False]
        if errors:
            log.warning("Salesforce upsert errors in chunk %d: %s", i // SF_CHUNK_SIZE, errors[:3])
        updated_count += sum(1 for r in result if r["success"])

    context["ti"].xcom_push(key="updated_count", value=updated_count)
    log.info("Upserted %d Contact records in Salesforce", updated_count)
    os.remove(predictions_path)
```

#### Validate

```python
def validate(**context) -> None:
    """Assert at least 1 Contact was updated in Salesforce for run_date."""
    updated_count = context["ti"].xcom_pull(task_ids="upsert_salesforce", key="updated_count") or 0
    run_date      = get_run_date(context)
    if updated_count < 1:
        raise ValueError(
            f"Salesforce upsert returned 0 successful updates for {run_date}. "
            "Investigate the bulk API response."
        )
    log.info("Validation passed — %d Contacts updated for %s", updated_count, run_date)
```

### Step 6 — Define the DAG block

```python
with DAG(
    dag_id="nnn_ml_churn_predictions_to_salesforce_daily",
    description="Export daily churn scores from Snowflake ML schema → Salesforce Contact bulk upsert",
    default_args=default_args,
    schedule_interval="0 9 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "ml", "salesforce", "churn", "daily"],
) as dag:

    t_export = PythonOperator(        # Query ML.CHURN_PREDICTIONS for run_date; write to temp JSON file
        task_id="export_predictions",
        python_callable=export_predictions,
    )

    t_upsert = PythonOperator(        # Bulk upsert churn scores into Salesforce Contact records
        task_id="upsert_salesforce",
        python_callable=upsert_salesforce,
    )

    t_validate = PythonOperator(      # Assert >= 1 Contact updated in Salesforce for run_date
        task_id="validate",
        python_callable=validate,
    )

    t_export >> t_upsert >> t_validate
```

### Step 7 — Validate imports and syntax

```bash
# Parse check — catches syntax errors and broken imports
python -c "import dags.integrations.nnn_ml_churn_predictions_to_salesforce_daily"

# Airflow DAG load check
airflow dags list | grep churn

# Dry-run a specific task
airflow tasks test nnn_ml_churn_predictions_to_salesforce_daily export_predictions 2024-06-01
```

---

## 5. Adding a New Connection

### 5.1 — Register the constant in `utils.py`

Open `plugins/nnn_common/utils.py` and find the connection constants block (~line 40). Add your new constant following the alphabetical grouping:

```python
# External CRM / Partner systems
CONN_SALESFORCE    = conn("salesforce")       # already exists
CONN_HUBSPOT       = conn("hubspot_api")      # ← add new entry like this
```

The `conn()` helper appends `_prod` / `_staging` / `_dev` based on `NNN_ENV`.

### 5.2 — Configure in Airflow

**Via UI:** Admin → Connections → + Add Connection  
Set **Connection ID** to exactly `nnn_<system>_prod` (the value `conn()` generates).

**Via environment variable:**
```bash
export AIRFLOW_CONN_NNN_HUBSPOT_API_PROD='{"conn_type": "http", "host": "api.hubapi.com", "schema": "https", "extra": {"Authorization": "Bearer <key>"}}'
```

### 5.3 — Document it

Add a row to both the connection constants table **and** the Required Airflow Connections table in `README.md`.

---

## 6. Testing Your DAG

### DAG integrity (no execution)

```bash
# Import check — will show Python errors and import failures
python dags/integrations/nnn_my_new_dag.py

# Airflow parse check
airflow dags list 2>&1 | grep -E "ERROR|nnn_my_new_dag"
```

### Task dry-run (executes real code against real systems)

```bash
# Run a single task for a specific logical date
airflow tasks test nnn_my_new_dag extract 2024-06-01

# Run in sequence
airflow tasks test nnn_my_new_dag extract 2024-06-01
airflow tasks test nnn_my_new_dag load    2024-06-01
airflow tasks test nnn_my_new_dag validate 2024-06-01
```

> **Warning:** `airflow tasks test` executes against real connections. Use staging connections (`NNN_ENV=staging`) when running against non-production targets.

### Inspecting XCom after a test run

```bash
airflow tasks render nnn_my_new_dag extract 2024-06-01
# or query the Airflow metadata DB:
airflow db shell
SELECT key, value FROM xcom WHERE dag_id='nnn_my_new_dag' ORDER BY timestamp DESC LIMIT 20;
```

### Unit testing task functions

Because task functions are at module level, you can import and call them directly with a mock context:

```python
# tests/test_nnn_my_new_dag.py
import json
import pytest
from unittest.mock import MagicMock, patch

from dags.integrations.nnn_my_new_dag import export_predictions

def make_context(run_date="2024-06-01"):
    ti = MagicMock()
    return {"ti": ti, "ds": run_date, "execution_date": MagicMock(strftime=lambda f: run_date)}

@patch("dags.integrations.nnn_my_new_dag.get_snowflake_hook")
def test_export_predictions_no_records_raises(mock_hook):
    cursor = MagicMock()
    cursor.description = [("service_id",), ("churn_score",)]
    cursor.fetchall.return_value = []
    mock_hook.return_value.get_conn.return_value.cursor.return_value = cursor

    ctx = make_context()
    with pytest.raises(ValueError, match="No churn predictions found"):
        export_predictions(**ctx)
```

---

## 7. Pre-Merge Checklist

Copy this checklist into your PR description:

```
### DAG Development Checklist

Structure
- [ ] File is in the correct domain subdirectory under dags/
- [ ] DAG ID matches filename (dag_id="nnn_..." matches file name nnn_....py)
- [ ] Module docstring complete (Owner, Domain, Schedule, SLA, Steps, Upstream, Downstream)
- [ ] from __future__ import annotations is the first line
- [ ] All task callable functions are at MODULE LEVEL (not inside with DAG() block)
- [ ] Dependency chain defined at the bottom of the with DAG() block
- [ ] One inline comment per PythonOperator explaining its role

Conventions
- [ ] default_args uses the standard NNN shape (all 10 keys present)
- [ ] start_date=datetime(2024, 1, 1) — no tzinfo argument
- [ ] catchup=False
- [ ] max_active_runs=1
- [ ] on_failure_callback=nnn_failure_alert in default_args
- [ ] sla_miss_callback=nnn_sla_miss_alert on DAG (if SLA is defined)
- [ ] Tags list includes "nnn", domain, source, target, frequency
- [ ] All print() replaced with log.info() / log.warning()
- [ ] Provider hooks imported INSIDE the function that uses them

Data handling
- [ ] All Snowflake SQL uses named %(param)s params with dict arguments (never positional %s with tuples)
- [ ] Large payloads (>100 rows) written to temp file; only path pushed via XCom
- [ ] Idempotent: running the task twice for the same date produces the same result
- [ ] DELETE + INSERT (or MERGE) pattern used — not append-only INSERT
- [ ] Temp files cleaned up (os.remove) after use
- [ ] Snowflake connections closed in finally block (cursor.close(); conn.close())

Connections
- [ ] All connection IDs use CONN_* constants from nnn_common.utils
- [ ] Any new CONN_* constant added to utils.py AND documented in README.md
- [ ] Connection configured in Airflow for all environments (prod, staging, dev)

Testing
- [ ] python dags/integrations/nnn_my_dag.py passes (no import errors)
- [ ] airflow tasks test run against staging for at least the primary extract/load tasks
- [ ] Validation task confirmed to raise ValueError on bad data
```

---

## 8. Common Mistakes

| Mistake | Symptom | Fix |
|---|---|---|
| Task function inside `with DAG()` block | Function re-defined on every scheduler parse; tasks may not show in UI until restart | Move all `def callable(**context)` to module level, above the `with DAG()` |
| `start_date=datetime(2024, 1, 1, tzinfo=AEST)` | Scheduler silently misaligns run dates; catchup may trigger unexpectedly | Use `datetime(2024, 1, 1)` — naive, no tzinfo |
| `print(...)` in task code | Output bypasses Airflow's structured task log system; logs not searchable | Replace with `log.info(...)` / `log.warning(...)` |
| Snowflake positional `%s` params | `ProgrammingError: not all arguments converted` at runtime | Use `%(name)s` with a `dict`, not `%s` with a tuple |
| Pushing large lists directly to XCom | Airflow metadata DB bloat; XCom serialisation timeout for >10 MB | Write to `tempfile.NamedTemporaryFile`, push the path |
| `upload_to_s3()` return value passed as `s3_prefix` | `redshift_copy_from_s3` receives `s3://bucket/path` but prepends bucket again → double-prefix error | `upload_to_s3()` returns a full URI; pass only the relative `dest_key` to `redshift_copy_from_s3()` |
| Provider hook import at module level | `ImportError` on Airflow workers that don't have that provider installed | Import inside the function: `from airflow.providers.x.hooks.y import Hook` |
| Snowflake TEMPORARY TABLE in one task, used in another | `Object 'TMP_TABLE' does not exist` — temp tables are session-scoped | All MERGE operations using a temp table must be in a single `PythonOperator` with one shared connection |
| `cursor.fetchall()` on a multi-million-row table | Worker OOM kill | Use `cursor.fetchmany(batch_size)` or `snowflake_unload_to_s3()` for large exports |
| `client.keys()` on Redis | Blocks Redis for seconds on large keyspaces | Use `client.scan_iter(match="prefix:*", count=100)` instead |
| `truncate=True` in `redshift_copy_from_s3()` for date-partitioned tables | TRUNCATES ALL historical rows, not just today's | Use `truncate=False` with a preceding `DELETE FROM table WHERE date_col = %s` |
