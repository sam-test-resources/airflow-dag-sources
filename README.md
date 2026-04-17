# NNN Airflow DAGs

Production Airflow DAG repository for the **NNN (National NextGen Network)** Data Engineering team.

This repository contains **50 production-grade DAG files** organised by business domain, plus shared plugin utilities. It is used as a **convention reference** by the DAG Scaffolding Agent: the agent clones this repo, analyses its patterns, and applies the same conventions to every newly generated DAG.

---

## Repository Structure

```
nnn_airflow_dags/
├── dags/
│   ├── network/          # Network performance, capacity, outages, traffic
│   ├── customer/         # RSP activations, service qualification, NPS, CX features
│   ├── wholesale/        # CVC billing, invoicing, RSP reconciliation
│   ├── field_ops/        # Technician scheduling, premises activation, FSA reporting
│   ├── finance/          # Revenue recognition, CAPEX tracking
│   ├── compliance/       # ACCC reporting, SLA compliance
│   ├── infrastructure/   # Node health monitoring, PON splitter audit
│   ├── redshift/         # Snowflake → S3 → Redshift analytics warehouse loads (10 DAGs)
│   └── integrations/     # Diverse source/target integration patterns (20 DAGs)
└── plugins/
    └── nnn_common/       # Shared callbacks, helpers, and connection constants
        ├── __init__.py
        ├── alerts.py     # nnn_failure_alert, nnn_sla_miss_alert, nnn_post_slack_message
        └── utils.py      # Connection IDs, S3/Snowflake/Redshift helpers, date utils
```

---

## DAG Inventory

### Network Domain

| DAG File | DAG ID | Schedule (AEST) | SLA | Source(s) | Target(s) |
|---|---|---|---|---|---|
| `nnn_network_performance_daily.py` | `nnn_network_performance_daily` | Daily 02:00 | 4 h | NMS REST API | `NETWORK.LINK_PERFORMANCE_DAILY` |
| `nnn_capacity_utilisation_hourly.py` | `nnn_capacity_utilisation_hourly` | Every :30 | 45 min | Kafka `nnn.network.cvc.metrics` | `NETWORK.CVC_UTILISATION_HOURLY` |
| `nnn_outage_incident_etl.py` | `nnn_outage_incident_etl` | Every 15 min | — | ServiceNow incidents | `OPERATIONS.OUTAGE_INCIDENTS` |
| `nnn_poi_traffic_aggregation_weekly.py` | `nnn_poi_traffic_aggregation_weekly` | Monday 03:00 | — | Snowflake rollup | `NETWORK.POI_TRAFFIC_WEEKLY`, S3 CSV |

### Customer Domain

| DAG File | DAG ID | Schedule (AEST) | SLA | Source(s) | Target(s) |
|---|---|---|---|---|---|
| `nnn_rsp_activation_daily.py` | `nnn_rsp_activation_daily` | Daily 01:00 | — | Oracle OSS | `WHOLESALE.RSP_ACTIVATIONS`, Snowflake dynamic table |
| `nnn_service_qualification_sync.py` | `nnn_service_qualification_sync` | Every 6 h | 1 h | SQ API | Postgres `public.nnn_service_areas`, `CUSTOMER.SQ_SYNC_AUDIT` |
| `nnn_customer_experience_features.py` | `nnn_customer_experience_features` | Daily 04:00 | — | Snowflake (4 parallel extracts) | `ML.CX_FEATURE_STORE` |
| `nnn_nps_survey_etl_weekly.py` | `nnn_nps_survey_etl_weekly` | Tuesday 06:00 | — | Medallia API | `CUSTOMER.NPS_RESPONSES` |

### Wholesale Domain

| DAG File | DAG ID | Schedule (AEST) | SLA | Source(s) | Target(s) |
|---|---|---|---|---|---|
| `nnn_cvc_billing_daily.py` | `nnn_cvc_billing_daily` | Daily 05:00 | 4 h (CRITICAL) | Snowflake P95 | `FINANCE.CVC_BILLING_LINES`, Oracle EBS AR staging |
| `nnn_wholesale_invoice_monthly.py` | `nnn_wholesale_invoice_monthly` | Last day of month 10:00 PM | 8 h | Snowflake CVC/AVC/CSG | Snowflake, Oracle EBS AR, PDF → S3 |
| `nnn_rsp_reconciliation_weekly.py` | `nnn_rsp_reconciliation_weekly` | Saturday 02:00 | 6 h | Oracle EBS + Salesforce | `WHOLESALE.RSP_RECONCILIATION`, `WHOLESALE.RECON_DISCREPANCIES` |

### Field Operations Domain

| DAG File | DAG ID | Schedule (AEST) | SLA | Source(s) | Target(s) |
|---|---|---|---|---|---|
| `nnn_technician_scheduling_daily.py` | `nnn_technician_scheduling_daily` | Daily 03:00 | — | ServiceNow work orders, Snowflake roster | `FIELD_OPS.TECHNICIAN_SCHEDULE`, ServiceNow write-back |
| `nnn_premises_activation_etl.py` | `nnn_premises_activation_etl` | Daily 01:30 | — | Oracle OSS (incremental watermark), Postgres NI | `INFRASTRUCTURE.PREMISES`, `INFRASTRUCTURE.ROLLOUT_METRICS` |
| `nnn_fsa_completion_reporting.py` | `nnn_fsa_completion_reporting` | Daily 07:00 | — | ServiceNow FSA | `FIELD_OPS.FSA_PERFORMANCE`, PowerBI dataset refresh |

### Finance Domain

| DAG File | DAG ID | Schedule (AEST) | SLA | Source(s) | Target(s) |
|---|---|---|---|---|---|
| `nnn_revenue_recognition_monthly.py` | `nnn_revenue_recognition_monthly` | 2nd of month 09:00 | 12 h | Oracle EBS AR lines | `FINANCE.REVENUE_RECOGNITION`, Oracle EBS GL |
| `nnn_capex_project_tracking_weekly.py` | `nnn_capex_project_tracking_weekly` | Monday 04:00 | — | SAP PS OData | `FINANCE.CAPEX_TRACKING` |

### Compliance Domain

| DAG File | DAG ID | Schedule (AEST) | SLA | Source(s) | Target(s) |
|---|---|---|---|---|---|
| `nnn_accc_reporting_weekly.py` | `nnn_accc_reporting_weekly` | Thursday 05:00 | 8 h (REGULATORY) | Snowflake multi-schema | `COMPLIANCE.ACCC_METRICS`, S3 JSON + XLSX |
| `nnn_sla_compliance_daily.py` | `nnn_sla_compliance_daily` | Daily 06:00 | — | Snowflake (SLA breach detection) | `COMPLIANCE.SLA_BREACHES`, CSG credits |

### Infrastructure Domain

| DAG File | DAG ID | Schedule (AEST) | SLA | Source(s) | Target(s) |
|---|---|---|---|---|---|
| `nnn_node_health_monitoring_hourly.py` | `nnn_node_health_monitoring_hourly` | Every :05 | — | S3 SNMP/NetFlow JSON files | `NETWORK.NODE_HEALTH_HOURLY`, `NETWORK.NODE_HEALTH_ALERTS` |
| `nnn_pon_splitter_audit_weekly.py` | `nnn_pon_splitter_audit_weekly` | Sunday 01:00 | — | Postgres NI, S3 field survey | `INFRASTRUCTURE.PON_AUDIT_WEEKLY`, `INFRASTRUCTURE.SPLITTER_REMEDIATION_QUEUE`, S3 XLSX |

---

### Redshift Domain (Snowflake → S3 → Redshift)

All Redshift DAGs use the **Snowflake UNLOAD → S3 → Redshift COPY** pattern:
1. Unload data from Snowflake to S3 in Parquet format via `snowflake_unload_to_s3()`
2. COPY into the Redshift analytics warehouse via `redshift_copy_from_s3()`
3. Validate row counts via `assert_redshift_row_count()`

| DAG File | DAG ID | Schedule (AEST) | SLA | Snowflake Source | Redshift Target |
|---|---|---|---|---|---|
| `nnn_network_performance_redshift_daily.py` | `nnn_network_performance_redshift_daily` | Daily 20:00 | 2 h | `NETWORK.LINK_PERFORMANCE_DAILY` | `analytics.network_performance_daily` |
| `nnn_cvc_billing_redshift_daily.py` | `nnn_cvc_billing_redshift_daily` | Daily 00:00 | 3 h | `FINANCE.CVC_BILLING_LINES` | `finance.cvc_billing_daily` |
| `nnn_rsp_activation_redshift_daily.py` | `nnn_rsp_activation_redshift_daily` | Daily 19:00 | 2 h | `WHOLESALE.RSP_ACTIVATIONS` | `wholesale.rsp_activations_daily` |
| `nnn_customer_cx_redshift_daily.py` | `nnn_customer_cx_redshift_daily` | Daily 00:00 | 2 h | `ML.CX_FEATURE_STORE` | `ml.customer_cx_features` |
| `nnn_compliance_breach_redshift_daily.py` | `nnn_compliance_breach_redshift_daily` | Daily 01:00 | 2 h | `COMPLIANCE.SLA_BREACHES` | `compliance.sla_breaches_daily` |
| `nnn_infrastructure_health_redshift_hourly.py` | `nnn_infrastructure_health_redshift_hourly` | Every :20 | 30 min | `NETWORK.NODE_HEALTH_HOURLY` | `analytics.node_health_hourly` |
| `nnn_capex_tracking_redshift_weekly.py` | `nnn_capex_tracking_redshift_weekly` | Sunday 19:00 | 4 h | `FINANCE.CAPEX_TRACKING` | `finance.capex_tracking_weekly` |
| `nnn_nps_sentiment_redshift_weekly.py` | `nnn_nps_sentiment_redshift_weekly` | Wednesday 22:00 | 3 h | `CUSTOMER.NPS_RESPONSES` | `customer.nps_sentiment_weekly` |
| `nnn_poi_capacity_redshift_weekly.py` | `nnn_poi_capacity_redshift_weekly` | Monday 20:00 | 4 h | `NETWORK.POI_TRAFFIC_WEEKLY` | `analytics.poi_capacity_weekly` |
| `nnn_accc_regulatory_redshift_weekly.py` | `nnn_accc_regulatory_redshift_weekly` | Thursday 22:00 | 4 h | `COMPLIANCE.ACCC_METRICS` | `regulatory.accc_metrics_weekly` |

---

### Integrations Domain (Diverse Source & Target Patterns)

These 20 DAGs demonstrate the full breadth of integration patterns, covering every major source and target system type used by NNN.

#### Ingest to Snowflake / Redshift

| DAG File | DAG ID | Schedule | SLA | Source | Target |
|---|---|---|---|---|---|
| `nnn_sftp_rsp_billing_ingest_daily.py` | `nnn_sftp_rsp_billing_ingest_daily` | Daily 18:00 | 2 h | SFTP CSV files | `WHOLESALE.RSP_BILLING_STATEMENTS` |
| `nnn_mongodb_noc_incidents_sync_hourly.py` | `nnn_mongodb_noc_incidents_sync_hourly` | Every :05 | 30 min | MongoDB `noc_tools.incidents` | `OPERATIONS.NOC_INCIDENTS` |
| `nnn_elasticsearch_fault_logs_daily.py` | `nnn_elasticsearch_fault_logs_daily` | Daily 01:00 | 90 min | Elasticsearch `nnn-fault-logs-*` | `OPERATIONS.FAULT_LOG_SUMMARY` |
| `nnn_dynamodb_portal_sessions_daily.py` | `nnn_dynamodb_portal_sessions_daily` | Daily 02:00 | 1 h | AWS DynamoDB `nnn-rsp-portal-sessions` | Redshift `analytics.rsp_portal_sessions` |
| `nnn_kinesis_clickstream_hourly.py` | `nnn_kinesis_clickstream_hourly` | Every :45 | 45 min | AWS Kinesis `nnn-rsp-portal-clickstream` | `ML.RSP_PORTAL_CLICKSTREAM` |
| `nnn_sqs_provisioning_events_15min.py` | `nnn_sqs_provisioning_events_15min` | Every 15 min | — | AWS SQS `nnn-provisioning-events` | `OPERATIONS.PROVISIONING_EVENTS` |
| `nnn_mqtt_basestation_telemetry_hourly.py` | `nnn_mqtt_basestation_telemetry_hourly` | Every :15 | 30 min | MQTT → Firehose → S3 | `NETWORK.FW_BASESTATION_TELEMETRY` |
| `nnn_soap_mediation_cdr_daily.py` | `nnn_soap_mediation_cdr_daily` | Daily 03:00 | 3 h | SOAP/XML mediation CDR API | `NETWORK.MEDIATION_CDR` |
| `nnn_graphql_partner_portal_daily.py` | `nnn_graphql_partner_portal_daily` | Daily 04:00 | 2 h | GraphQL partner portal API | `WHOLESALE.PARTNER_PORTAL_ACTIVITY` |
| `nnn_mssql_legacy_billing_daily.py` | `nnn_mssql_legacy_billing_daily` | Daily 05:00 | 2 h | MS SQL Server legacy billing | `FINANCE.LEGACY_BILLING_RECORDS` |
| `nnn_azure_blob_partner_data_daily.py` | `nnn_azure_blob_partner_data_daily` | Daily 10:00 | 2 h | Azure Blob Storage `partner-data-drops` | `WHOLESALE.PARTNER_DATA_INGEST` |
| `nnn_rabbitmq_order_events_15min.py` | `nnn_rabbitmq_order_events_15min` | Every 15 min | — | RabbitMQ `nnn.orders.provisioned` | `OPERATIONS.ORDER_EVENTS` |
| `nnn_imap_rsp_report_weekly.py` | `nnn_imap_rsp_report_weekly` | Monday 11:00 | 4 h | IMAP email `.xlsx` attachments | `WHOLESALE.RSP_MANUAL_REPORTS` |
| `nnn_redis_session_analytics_daily.py` | `nnn_redis_session_analytics_daily` | Daily 13:00 | 1 h | Redis `session:*` hash keys | `ML.RSP_SESSION_ANALYTICS` |

#### Export from Snowflake to External Systems

| DAG File | DAG ID | Schedule | SLA | Source | Target |
|---|---|---|---|---|---|
| `nnn_snowflake_to_dynamodb_cache_daily.py` | `nnn_snowflake_to_dynamodb_cache_daily` | Daily 06:00 | 1 h | `CUSTOMER.SERVICE_ELIGIBILITY_CURRENT` | DynamoDB `nnn-rsp-portal-cache` |
| `nnn_snowflake_to_elasticsearch_daily.py` | `nnn_snowflake_to_elasticsearch_daily` | Daily 07:00 | 2 h | `CUSTOMER.SERVICE_AVAILABILITY_CURRENT` | Elasticsearch `nnn-service-availability` |
| `nnn_snowflake_to_rds_postgres_daily.py` | `nnn_snowflake_to_rds_postgres_daily` | Daily 08:00 | 1 h | `OPERATIONS.DAILY_NETWORK_HEALTH_SUMMARY` | RDS Postgres `operational_reporting.network_health_daily` |
| `nnn_s3_to_glue_catalog_daily.py` | `nnn_s3_to_glue_catalog_daily` | Daily 09:00 | 1 h | S3 data lake new partitions | AWS Glue Data Catalog |
| `nnn_snowflake_to_gcs_regulatory_weekly.py` | `nnn_snowflake_to_gcs_regulatory_weekly` | Wednesday 20:00 | 3 h | `COMPLIANCE.WEEKLY_REGULATORY_EXPORT` | GCS `nnn-accc-regulatory-{env}` |
| `nnn_snowflake_to_s3_ml_export_daily.py` | `nnn_snowflake_to_s3_ml_export_daily` | Daily 12:00 | 2 h | `ML.CUSTOMER_FEATURES`, `ML.NETWORK_FEATURES`, `ML.CHURN_LABELS` | S3 Parquet + ML training trigger |

---

## NNN Team Conventions

All DAGs in this repository follow a strict set of conventions. The DAG Scaffolding Agent learns these patterns from the files above and applies them to every generated DAG.

### DAG and Task Naming

| Object | Pattern | Example |
|---|---|---|
| DAG ID | `nnn_<domain>_<description>_<frequency>` | `nnn_cvc_billing_daily` |
| Task ID | `<verb>_<noun>` (snake_case) | `extract_network_metrics`, `load_snowflake` |
| File name | `<dag_id>.py` | `nnn_cvc_billing_daily.py` |
| Task group ID | `<noun>_<phase>` | `network_extract`, `snowflake_load` |

### `default_args` Structure

Every DAG uses this exact `default_args` shape:

```python
from nnn_common.alerts import nnn_failure_alert

default_args = {
    "owner":                    "nnn-data-engineering",
    "depends_on_past":          False,
    "email":                    ["de-alerts@nnnco.com.au"],
    "email_on_failure":         True,
    "email_on_retry":           False,
    "retries":                  2,           # 3 for near-real-time DAGs
    "retry_delay":              timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":          timedelta(minutes=30),
    "on_failure_callback":      nnn_failure_alert,
    "execution_timeout":        timedelta(minutes=30),  # increase for heavy DAGs
}
```

Near-real-time DAGs (15-min, hourly) use `retries=3`, `retry_delay=timedelta(minutes=1)`, `retry_exponential_backoff=False`.

### SLA Miss Callback

DAGs with SLA requirements attach `sla_miss_callback` at the DAG level:

```python
from nnn_common.alerts import nnn_sla_miss_alert

with DAG(
    ...
    sla_miss_callback=nnn_sla_miss_alert,
) as dag:
```

Billing and compliance DAGs trigger a **PagerDuty P1 (critical)** page on SLA miss. All other DAGs trigger P2.

### Connection ID Convention

All connection IDs follow the pattern `nnn_<system>_<env>` where `env` is `prod`, `staging`, or `dev`.

```python
from nnn_common.utils import conn

CONN_SNOWFLAKE    = conn("snowflake")    # → nnn_snowflake_prod
CONN_ORACLE_EBS   = conn("oracle_ebs")  # → nnn_oracle_ebs_prod
CONN_REDSHIFT     = conn("redshift")    # → nnn_redshift_prod
```

### Snowflake Schemas

| Schema | Purpose |
|---|---|
| `NETWORK` | Network performance, capacity, node health, CDR, FW telemetry |
| `CUSTOMER` | NPS surveys, RSP activations, service availability |
| `WHOLESALE` | CVC billing, invoicing, RSP reconciliation, partner data |
| `FIELD_OPS` | Technician scheduling, FSA performance |
| `FINANCE` | Revenue recognition, CAPEX tracking, CVC/legacy billing |
| `COMPLIANCE` | ACCC reporting, SLA breach records, CSG credits |
| `INFRASTRUCTURE` | Premises rollout, PON splitter audit |
| `ML` | Feature stores, clickstream, session analytics |
| `OPERATIONS` | ETL watermarks, outage incidents, NOC incidents, provisioning events |

### Redshift Schemas

| Schema | Purpose |
|---|---|
| `analytics` | Network performance, RSP portal sessions, node health, POI capacity |
| `finance` | CVC billing, CAPEX tracking |
| `wholesale` | RSP activations |
| `customer` | NPS sentiment |
| `ml` | Customer CX features |
| `compliance` | SLA breaches |
| `regulatory` | ACCC metrics |

### Incremental Load (Watermark Pattern)

DAGs that load incrementally use Snowflake's `OPERATIONS.ETL_WATERMARKS` table:

```python
# Read watermark
row = hook.get_first(
    "SELECT TO_CHAR(last_run_ts, 'YYYY-MM-DD HH24:MI:SS') FROM OPERATIONS.ETL_WATERMARKS "
    "WHERE dag_id = 'my_dag_id'"
)
watermark = row[0] if row else fallback_ts

# Update watermark (trigger_rule="all_done" so it always advances)
hook.run("""
    MERGE INTO OPERATIONS.ETL_WATERMARKS tgt
    USING (SELECT 'my_dag_id' AS dag_id, CURRENT_TIMESTAMP() AS ts) src
      ON tgt.dag_id = src.dag_id
    WHEN MATCHED     THEN UPDATE SET last_run_ts = src.ts
    WHEN NOT MATCHED THEN INSERT (dag_id, last_run_ts) VALUES (src.dag_id, src.ts)
""")
```

### Snowflake MERGE / Temp Table Pattern

**Critical**: Snowflake TEMPORARY TABLEs are session-scoped. Each Airflow task runs in a separate OS process with a separate connection. Never create a temp table in one task and reference it in another. Always use a **single PythonOperator** with explicit connection management:

```python
def upsert_records(**context) -> None:
    hook   = get_snowflake_hook()
    conn   = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("CREATE OR REPLACE TEMPORARY TABLE tmp_staging LIKE target_table")
        cursor.executemany("INSERT INTO tmp_staging VALUES (%s, %s, ...)", rows)
        cursor.execute("""
            MERGE INTO target_table tgt
            USING tmp_staging src ON tgt.key_col = src.key_col
            WHEN MATCHED     THEN UPDATE SET ...
            WHEN NOT MATCHED THEN INSERT *
        """)
        conn.commit()
    finally:
        cursor.close()
        conn.close()
```

### Redshift MERGE Pattern (No Native MERGE pre-2023)

```python
def upsert_redshift(**context) -> None:
    hook = get_redshift_hook()
    hook.run("CREATE TEMP TABLE tmp_staging (LIKE target_schema.target_table)")
    hook.run("INSERT INTO tmp_staging SELECT * FROM target_schema.target_table WHERE key IN (...)")
    hook.run("DELETE FROM target_schema.target_table USING tmp_staging WHERE ...")
    hook.run("INSERT INTO target_schema.target_table SELECT * FROM tmp_staging")
    hook.run("DROP TABLE tmp_staging")
```

### TaskGroup Usage

Complex DAGs decompose work into named `TaskGroup` blocks:

```python
from airflow.utils.task_group import TaskGroup

with TaskGroup("extract") as extract_group:
    ...
with TaskGroup("load") as load_group:
    ...

extract_group >> load_group
```

### ShortCircuitOperator for Conditional Execution

Monthly DAGs that use `28-31 * *` cron patterns guard against mid-month runs:

```python
import calendar
from airflow.operators.python import ShortCircuitOperator

def is_last_day_of_month(**context) -> bool:
    d = get_execution_date(context)
    return d.day == calendar.monthrange(d.year, d.month)[1]

t_check = ShortCircuitOperator(
    task_id="is_last_day_of_month",
    python_callable=is_last_day_of_month,
)
```

### BranchPythonOperator for Threshold Alerts

```python
branch = BranchPythonOperator(
    task_id="check_threshold",
    python_callable=lambda **ctx: "send_alert" if ctx["ti"].xcom_pull(key="count") >= 50 else "no_alert",
)
branch >> [alert_task, no_alert]
```

### Operator Selection by Source / Target System

| System | Operator / Hook | Notes |
|---|---|---|
| NMS REST API | `HttpHook` | Used for NMS, SQ API, ServiceNow, partner GraphQL, SOAP endpoints |
| Kafka | `PythonOperator` + `confluent_kafka.Consumer` | Explicit offset management |
| Oracle EBS / OSS | `PythonOperator` + `OracleHook` | Import inside function |
| Snowflake (write) | `PythonOperator` + `SnowflakeHook` | Prefer explicit cursor for MERGE |
| Snowflake (read) | `get_snowflake_hook().get_pandas_df()` or `get_first()` | — |
| Salesforce | `PythonOperator` + `simple_salesforce.Salesforce` | Import inside function |
| SAP PS OData | `PythonOperator` + `HttpHook` | — |
| Medallia API | `PythonOperator` + `HttpHook` | — |
| S3 file landing | `S3KeySensor` (wait) + `PythonOperator` (process) | `mode="reschedule"` |
| S3 file ops | `upload_to_s3()` / `download_from_s3()` from utils | — |
| Postgres (NI / RDS) | `PythonOperator` + `PostgresHook` | Specify conn_id explicitly |
| PowerBI | `SimpleHttpOperator` | Dataset refresh trigger only |
| Redshift (load) | `redshift_copy_from_s3()` from utils | Snowflake → S3 → Redshift pattern |
| Redshift (export) | `redshift_unload_to_s3()` from utils | — |
| MS SQL Server | `get_mssql_hook()` from utils | Legacy billing system |
| SFTP | `SSHHook` (sftp method) | Import `from airflow.providers.ssh.hooks.ssh` |
| MongoDB | `MongoHook` | Import `from airflow.providers.mongo.hooks.mongo` |
| Elasticsearch | `ElasticsearchPythonHook` | Import `from airflow.providers.elasticsearch.hooks.elasticsearch` |
| DynamoDB | `AwsBaseHook` + boto3 `dynamodb` resource | `get_client_type("dynamodb")` or resource |
| Kinesis | `AwsBaseHook` + boto3 `kinesis` client | `get_client_type("kinesis")` |
| SQS | `SqsHook` | Import `from airflow.providers.amazon.aws.hooks.sqs` |
| MQTT (IoT) | Via S3 staging (Firehose buffers MQTT → S3) | DAG reads from S3 |
| RabbitMQ | `RabbitMQHook` + pika channel | Import `from airflow.providers.rabbitmq.hooks.rabbitmq` |
| Redis | `RedisHook` | Import `from airflow.providers.redis.hooks.redis` |
| Azure Blob | `WasbHook` | Import `from airflow.providers.microsoft.azure.hooks.wasb` |
| Google Cloud Storage | `GCSHook` | Import `from airflow.providers.google.cloud.hooks.gcs` |
| AWS Glue | `AwsBaseHook` + boto3 `glue` client | `get_client_type("glue")` |
| IMAP email | `ImapHook` | Import `from airflow.providers.imap.hooks.imap` |

### Miscellaneous Rules

- `catchup=False` on all DAGs — no historical backfill by default.
- `max_active_runs=1` on DAGs with stateful targets (Snowflake MERGE, Oracle EBS write-back).
- All timestamps stored in UTC; business logic converts to AEST via `get_execution_date()`.
- No hardcoded credentials or environment-specific literals.
- Provider **hooks** (`SnowflakeHook`, `S3Hook`, `WasbHook`, etc.) go **inside the PythonOperator function** that uses them — not at module level.  Provider **operators** (`S3KeySensor`, `SimpleHttpOperator`, etc.) that define tasks in the DAG block are imported at module level or inside the `with DAG()` block.
- Every task has an inline comment explaining its role.
- Module-level docstring on every DAG file: Owner, Domain, Schedule, SLA, Steps, Upstream, Downstream.
- `from __future__ import annotations` at the top of every DAG file.

---

## Shared Plugin: `nnn_common`

Located at `plugins/nnn_common/`. Add `plugins/` to `PYTHONPATH` (Astronomer and most managed Airflow platforms do this automatically).

### `nnn_common.alerts`

| Symbol | Type | Description |
|---|---|---|
| `nnn_failure_alert(context)` | `on_failure_callback` | Posts Slack + PagerDuty P2 on task failure |
| `nnn_sla_miss_alert(dag, ...)` | `sla_miss_callback` | Posts Slack + PagerDuty P1/P2 on SLA miss |
| `nnn_post_slack_message(message)` | `None` | Public helper to post a plain-text Slack message from DAG logic |

### `nnn_common.utils` — Connection Constants

| Symbol | Connection ID (prod) | System |
|---|---|---|
| `CONN_SNOWFLAKE` | `nnn_snowflake_prod` | Snowflake analytics DW |
| `CONN_ORACLE_EBS` | `nnn_oracle_ebs_prod` | Oracle EBS billing/AR |
| `CONN_ORACLE_OSS` | `nnn_oracle_oss_prod` | Oracle OSS provisioning |
| `CONN_POSTGRES_NI` | `nnn_postgres_ni_prod` | Network Inventory (on-prem) |
| `CONN_SERVICENOW` | `nnn_servicenow_prod` | ServiceNow ITSM |
| `CONN_SALESFORCE` | `nnn_salesforce_prod` | Salesforce CRM |
| `CONN_SAP` | `nnn_sap_erp_prod` | SAP ERP (PS module) |
| `CONN_MEDALLIA` | `nnn_medallia_prod` | Medallia NPS platform |
| `CONN_S3` | `nnn_s3_prod` | AWS S3 (data lake) |
| `CONN_KAFKA` | `nnn_kafka_prod` | Kafka broker |
| `CONN_NMS_API` | `nnn_nms_api_prod` | NMS REST API |
| `CONN_SQ_API` | `nnn_sq_api_prod` | Service Qualification API |
| `CONN_POWERBI` | `nnn_powerbi_prod` | PowerBI REST API |
| `CONN_REDSHIFT` | `nnn_redshift_prod` | Amazon Redshift analytics DW |
| `CONN_DYNAMODB` | `nnn_dynamodb_prod` | Amazon DynamoDB |
| `CONN_KINESIS` | `nnn_kinesis_prod` | Amazon Kinesis Data Streams |
| `CONN_SQS` | `nnn_sqs_prod` | Amazon SQS |
| `CONN_FIREHOSE` | `nnn_firehose_prod` | Amazon Kinesis Firehose |
| `CONN_GLUE` | `nnn_glue_prod` | AWS Glue Data Catalog |
| `CONN_SFTP_RSP` | `nnn_sftp_rsp_prod` | SFTP (RSP bulk file drops) |
| `CONN_MONGODB` | `nnn_mongodb_prod` | MongoDB (NOC operational tools) |
| `CONN_ELASTICSEARCH` | `nnn_elasticsearch_prod` | Elasticsearch (fault log analytics) |
| `CONN_MQTT` | `nnn_mqtt_prod` | MQTT broker (FW base station IoT) |
| `CONN_RABBITMQ` | `nnn_rabbitmq_prod` | RabbitMQ (order management events) |
| `CONN_MSSQL` | `nnn_mssql_legacy_prod` | MS SQL Server (legacy billing) |
| `CONN_REDIS` | `nnn_redis_prod` | Redis (RSP portal session cache) |
| `CONN_RDS_POSTGRES` | `nnn_rds_postgres_prod` | RDS PostgreSQL (operational reporting) |
| `CONN_AZURE_BLOB` | `nnn_azure_blob_prod` | Azure Blob Storage (partner data) |
| `CONN_GCS` | `nnn_gcs_prod` | Google Cloud Storage (regulatory) |
| `CONN_IMAP` | `nnn_imap_reports_prod` | IMAP email (RSP manual reports) |
| `CONN_ML_PLATFORM` | `nnn_ml_platform_http_prod` | Internal ML platform REST API (training trigger / feature readiness webhook) |

### `nnn_common.utils` — Helper Functions & Constants

| Symbol | Returns | Description |
|---|---|---|
| `conn(system)` | `str` | Returns `nnn_<system>_<env>` connection ID |
| `NNN_ENV` | `str` | Runtime environment: `prod` / `staging` / `dev` |
| `NNN_S3_BUCKET` | `str` | `nnn-data-lake-<env>` |
| `NNN_REGION` | `str` | `ap-southeast-2` (Sydney) |
| `NNN_REDSHIFT_IAM_ROLE` | `str` | IAM role ARN for Redshift S3 COPY/UNLOAD |
| `NNN_SNOWFLAKE_S3_STAGE` | `str` | Snowflake external S3 stage name |
| `AEST` | `timezone` | UTC+10 fixed offset |
| `get_execution_date(context)` | `datetime` | Logical date as AEST-aware datetime |
| `get_run_date(context)` | `str` | `YYYY-MM-DD` in AEST |
| `get_run_month(context)` | `str` | `YYYY-MM` in AEST |
| `get_snowflake_hook()` | `SnowflakeHook` | Hook using `CONN_SNOWFLAKE` |
| `snowflake_run(sql, parameters)` | `None` | Execute SQL on Snowflake with logging |
| `snowflake_unload_to_s3(select_sql, s3_prefix, file_format, overwrite)` | `str` | COPY INTO S3 stage; returns full S3 URI |
| `get_redshift_hook()` | `RedshiftSQLHook` | Hook using `CONN_REDSHIFT` |
| `redshift_copy_from_s3(table, s3_prefix, file_format, iam_role, truncate, extra_options)` | `None` | COPY from S3 into Redshift table |
| `redshift_unload_to_s3(select_sql, s3_prefix, iam_role, parallel, header)` | `str` | UNLOAD from Redshift to S3 as gzipped CSV |
| `redshift_run(sql, parameters)` | `None` | Execute SQL on Redshift with logging |
| `get_mssql_hook()` | `MsSqlHook` | Hook using `CONN_MSSQL` |
| `s3_key(domain, table, run_date, ext)` | `str` | Canonical S3 key: `<domain>/<table>/run_date=<date>/data.<ext>` |
| `upload_to_s3(local_path, s3_key_path)` | `str` | Upload file to data lake; returns S3 URI |
| `download_from_s3(s3_key_path, local_path)` | `str` | Download file from data lake |
| `assert_row_count(table, run_date, min_rows)` | `None` | Raise `ValueError` if Snowflake partition is too small |
| `assert_redshift_row_count(table, run_date, min_rows)` | `None` | Raise `ValueError` if Redshift partition is too small |

---

## Required Airflow Connections

Configure in Airflow UI (Admin → Connections) or via `AIRFLOW_CONN_<UPPERCASE_ID>` environment variables.

### Core / On-Prem Systems

| Connection ID | Type | Notes |
|---|---|---|
| `nnn_snowflake_prod` | Snowflake | Account, warehouse, role, database |
| `nnn_oracle_ebs_prod` | Oracle | EBS AR/GL host, port, SID |
| `nnn_oracle_oss_prod` | Oracle | OSS provisioning database |
| `nnn_postgres_ni_prod` | Postgres | Network Inventory on-prem |
| `nnn_servicenow_prod` | HTTP | ServiceNow base URL + basic auth |
| `nnn_salesforce_prod` | HTTP | Salesforce REST API + OAuth |
| `nnn_sap_erp_prod` | HTTP | SAP PS OData endpoint + basic auth |
| `nnn_medallia_prod` | HTTP | Medallia Reporting API + OAuth |
| `nnn_nms_api_prod` | HTTP | NMS REST API + API key header |
| `nnn_sq_api_prod` | HTTP | Service Qualification API + API key |
| `nnn_powerbi_prod` | HTTP | PowerBI REST API + Azure AD token |
| `nnn_mssql_legacy_prod` | MsSql | Legacy billing SQL Server host/port/DB |

### AWS Systems

| Connection ID | Type | Notes |
|---|---|---|
| `nnn_s3_prod` | Amazon S3 | IAM role or access key for `nnn-data-lake-prod` |
| `nnn_redshift_prod` | Amazon Redshift | Cluster endpoint, port, DB, user/password |
| `nnn_dynamodb_prod` | Amazon Web Services | IAM credentials for DynamoDB |
| `nnn_kinesis_prod` | Amazon Web Services | IAM credentials for Kinesis Data Streams |
| `nnn_sqs_prod` | Amazon Web Services | IAM credentials for SQS |
| `nnn_firehose_prod` | Amazon Web Services | IAM credentials for Kinesis Firehose |
| `nnn_glue_prod` | Amazon Web Services | IAM credentials for Glue Data Catalog |

### Messaging / Streaming Systems

| Connection ID | Type | Notes |
|---|---|---|
| `nnn_kafka_prod` | — | Kafka bootstrap servers (host, SASL creds in extra JSON) |
| `nnn_mqtt_prod` | — | MQTT broker host/port (telemetry buffered to S3 via Firehose) |
| `nnn_rabbitmq_prod` | — | RabbitMQ host, port, vhost, login, password |
| `nnn_redis_prod` | Redis | Redis host, port, DB number |

### File Transfer / Storage Systems

| Connection ID | Type | Notes |
|---|---|---|
| `nnn_sftp_rsp_prod` | SFTP / SSH | SFTP host, port, login, private key |
| `nnn_imap_reports_prod` | IMAP | IMAP host, port, login, password |
| `nnn_azure_blob_prod` | Azure Blob Storage (WASB) | Storage account name + access key or SAS |
| `nnn_gcs_prod` | Google Cloud | Service account key JSON |

### NoSQL / Search Systems

| Connection ID | Type | Notes |
|---|---|---|
| `nnn_mongodb_prod` | MongoDB | MongoDB URI (host, port, DB, auth) |
| `nnn_elasticsearch_prod` | Elasticsearch | ES host, port, login, password |

### Operational Reporting

| Connection ID | Type | Notes |
|---|---|---|
| `nnn_rds_postgres_prod` | Postgres | RDS Postgres host, port, DB, user/password |
| `nnn_ml_platform_http_prod` | HTTP | Internal ML platform REST API + API key |

Replace `_prod` with `_staging` or `_dev` for non-production environments.

---

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `NNN_ENV` | No | `prod` | Runtime environment: `prod`, `staging`, `dev` |
| `NNN_SLACK_WEBHOOK_URL` | Yes (prod) | — | Slack incoming webhook for `#de-alerts` |
| `NNN_PAGERDUTY_ROUTING_KEY` | Yes (prod) | — | PagerDuty Events v2 integration key |
| `NNN_REDSHIFT_IAM_ROLE` | No | (ARN default) | IAM role ARN for Redshift S3 COPY/UNLOAD |

---

## Usage as DAG Scaffolding Agent Input

This repository is the **convention learning source** for the DAG Scaffolding Agent workflow. The agent accepts two inputs:

1. **Pipeline requirement** — plain-English description (inline text or `.txt` / `.docx` file).
2. **GitHub repository URL** — URL of this repository or a fork.

The workflow:
1. Clones this repo into its working directory.
2. Scans all `.py` files in `dags/` across all subdirectories to extract naming conventions, `default_args`, operator patterns, callback usage, and source/target integration patterns.
3. Generates a new production-ready DAG following these conventions exactly.
4. Self-reviews for requirements coverage and Airflow best practices.
5. Applies any blocking fixes and packages all outputs for download.

**Example scaffolding request:**

```
GitHub repo: https://github.com/your-org/nnn_airflow_dags

Requirement:
Build a daily DAG that extracts customer churn propensity scores from
the ML.CHURN_PREDICTIONS Snowflake table and loads them into Salesforce
as custom fields on the Contact object. Run at 08:00 AEST. SLA: 2 hours.
Alert via Slack on failure.
```

---

## Domain Glossary

| Term | Meaning |
|---|---|
| RSP | Retail Service Provider — ISPs that sell NNN connectivity to end customers |
| CVC | Connectivity Virtual Circuit — bandwidth capacity purchased by RSPs |
| AVC | Access Virtual Circuit — individual end-user service connection |
| POI | Point of Interconnect — handover point between NNN and RSP network |
| FSA | Field Service Appointment — scheduled technician visit |
| CSG | Customer Service Guarantee — regulatory obligation; breaches attract financial penalties ($25/day) |
| ACCC | Australian Competition and Consumer Commission — reports required under broadband performance obligations |
| NMS | Network Management System — internal tool monitoring link-level metrics |
| SQ API | Service Qualification API — determines which NNN products are available at an address |
| PON | Passive Optical Network — fibre access technology |
| FW | Fixed Wireless — base-station-based broadband technology |
| MSF | Mass Service Failure — outage affecting >2500 premises; triggers ACCC regulatory reporting |
| EVM | Earned Value Management — project performance metrics (SPI, CPI, EAC) |
| AASB 15 | Australian accounting standard for revenue recognition (connection fee amortisation) |
| CDR | Call/Connection Data Record — usage/session records from mediation systems |
| NOC | Network Operations Centre — 24/7 network monitoring team |
| IoT | Internet of Things — Fixed Wireless base station sensor telemetry |
