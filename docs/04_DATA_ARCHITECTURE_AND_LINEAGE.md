# Data Architecture & Lineage

**Audience:** Developers needing to understand how data flows through the NNN platform, which DAGs depend on which, and where each table lives.  
**Purpose:** Prevent circular dependencies, identify upstream blockers, plan SLA chains, and find the right source table for a new DAG.

---

## Table of Contents

1. [Overall Architecture](#1-overall-architecture)
2. [S3 Data Lake — Structure & Conventions](#2-s3-data-lake--structure--conventions)
3. [Snowflake Schema & Table Inventory](#3-snowflake-schema--table-inventory)
4. [Redshift Schema & Table Inventory](#4-redshift-schema--table-inventory)
5. [DAG Dependency Map](#5-dag-dependency-map)
6. [SLA Dependency Chains](#6-sla-dependency-chains)
7. [External System Data Flows](#7-external-system-data-flows)
8. [Table Ownership Matrix](#8-table-ownership-matrix)

---

## 1. Overall Architecture

```
External Sources                  NNN Platform                        Consumers
────────────────     ─────────────────────────────────────────    ──────────────────

RSP SFTP drops  ──→┐
Azure Blob      ──→│
IMAP email      ──→│  dags/integrations/   ──→  Snowflake    ──→  dags/redshift/  ──→  Redshift
GraphQL APIs    ──→│  (ingest DAGs)              (source of         (export DAGs)        (analytics)
SOAP/REST APIs  ──→│                              truth)
MongoDB / ES    ──→│
                    │
DynamoDB        ──→┤                        ──→  S3 Data Lake ──→  Athena
RabbitMQ        ──→│                             (raw + processed    Redshift Spectrum
SQS             ──→│                              Parquet)           Glue Catalog
Kinesis         ──→│
MQTT/Firehose   ──→│
                    │
Redis           ──→┘  (read cache,
MS SQL Server   ──→    legacy extract)
Oracle EBS/OSS  ──→  dags/wholesale,        ──→  Oracle EBS   ──→  Finance systems
ServiceNow      ──→  finance, network,           (write-back)
                     field_ops, compliance
                     (core ETL DAGs)

                     dags/integrations/     ──→  DynamoDB     ──→  RSP portal (read-path)
                     (export DAGs)          ──→  Elasticsearch──→  Customer availability API
                                            ──→  RDS Postgres ──→  Grafana / Tableau
                                            ──→  GCS          ──→  ACCC regulatory portal
                                            ──→  Salesforce   ──→  Sales team CRM
```

---

## 2. S3 Data Lake — Structure & Conventions

**Bucket:** `s3://nnn-data-lake-{env}/`  (`prod` / `staging` / `dev`)

### Standard partition layout

All data in the lake follows the Hive-style partition convention:

```
s3://nnn-data-lake-prod/
├── <domain>/
│   └── <table>/
│       └── run_date=YYYY-MM-DD/
│           └── data.<ext>           ← single file (small datasets)
│               OR
│           └── part-00000.parquet   ← multiple files (Snowflake UNLOAD)
│           └── part-00001.parquet
```

### Domain prefixes

| S3 Domain Prefix | Corresponds to | Example tables |
|---|---|---|
| `network/` | NETWORK Snowflake schema | `link_utilisation`, `fault_events`, `node_inventory` |
| `customer/` | CUSTOMER schema | `service_eligibility`, `service_availability` |
| `wholesale/` | WHOLESALE schema | `partner_data_ingest`, `rsp_manual_reports`, `wholesale_orders` |
| `finance/` | FINANCE schema | `invoice_summary`, `payment_events`, `revenue_daily` |
| `operations/` | OPERATIONS schema | `order_events`, `daily_network_health_summary`, `ticket_summary` |
| `ml-exports/` | ML schema exports | `customer_features`, `network_features`, `churn_labels` |
| `analytics/` | Redshift staging | `rsp_portal_sessions`, portal analytics |
| `mqtt/` | IoT / MQTT data | `fw-telemetry/<run_date>T<hour>/data.json` |
| `redshift-exports/` | Redshift → S3 unloads | Regulatory, NPS |
| `compliance/` | COMPLIANCE schema | ACCC metrics, SLA breaches |

### MQTT special layout

The MQTT telemetry path includes the hour component (ISO-8601 partial datetime):

```
s3://nnn-data-lake-prod/mqtt/fw-telemetry/2024-06-01T09/data.json
```

### S3 key helpers

Always use `s3_key()` from `nnn_common.utils` to build paths — never hardcode:

```python
key = s3_key(domain="analytics", table="rsp_portal_sessions", run_date=run_date, ext="json")
# → "analytics/rsp_portal_sessions/run_date=2024-06-01/data.json"
```

---

## 3. Snowflake Schema & Table Inventory

**Database:** `NNN_DW` (production) / `NNN_DW_STAGING` / `NNN_DW_DEV`

### NETWORK schema

| Table | Description | Source DAG | Partition Key |
|---|---|---|---|
| `NETWORK.LINK_PERFORMANCE_DAILY` | Link-level latency, loss, utilisation KPIs | `nnn_network_performance_daily` | `REPORT_DATE` |
| `NETWORK.CVC_UTILISATION_HOURLY` | CVC capacity utilisation per hour per POI | `nnn_capacity_utilisation_hourly` | `METRIC_HOUR` |
| `NETWORK.POI_TRAFFIC_WEEKLY` | Weekly aggregated POI traffic volumes | `nnn_poi_traffic_aggregation_weekly` | `WEEK_ENDING` |
| `NETWORK.NODE_HEALTH_HOURLY` | SNMP/NetFlow node health metrics | `nnn_node_health_monitoring_hourly` | `METRIC_HOUR` |
| `NETWORK.NODE_HEALTH_ALERTS` | Active threshold breach alerts | `nnn_node_health_monitoring_hourly` | `ALERT_DATE` |
| `NETWORK.FW_BASESTATION_TELEMETRY` | Fixed Wireless base station IoT telemetry | `nnn_mqtt_basestation_telemetry_hourly` | `RUN_DATE`, `TELEMETRY_TS` |
| `NETWORK.MEDIATION_CDR` | Call/Connection Data Records from SOAP mediation | `nnn_soap_mediation_cdr_daily` | `RUN_DATE` |

### CUSTOMER schema

| Table | Description | Source DAG | Partition Key |
|---|---|---|---|
| `CUSTOMER.NPS_RESPONSES` | NPS survey responses from Medallia | `nnn_nps_survey_etl_weekly` | `SURVEY_DATE` |
| `CUSTOMER.SQ_SYNC_AUDIT` | Service qualification sync audit records | `nnn_service_qualification_sync` | `SYNC_DATE` |
| `CUSTOMER.SERVICE_ELIGIBILITY_CURRENT` | Current-state address eligibility (full snapshot) | `nnn_rsp_activation_daily` (upstream) | None (full-replace) |
| `CUSTOMER.SERVICE_AVAILABILITY_CURRENT` | Current-state address availability (full snapshot) | Various ETL | None (full-replace) |
| `CUSTOMER.CACHE_REFRESH_AUDIT` | Audit trail for DynamoDB portal cache refreshes | `nnn_snowflake_to_dynamodb_cache_daily` | `RUN_DATE` |

### WHOLESALE schema

| Table | Description | Source DAG | Partition Key |
|---|---|---|---|
| `WHOLESALE.RSP_ACTIVATIONS` | RSP service activation records from Oracle OSS | `nnn_rsp_activation_daily` | `ACTIVATION_DATE` |
| `WHOLESALE.RSP_BILLING_STATEMENTS` | RSP billing CSV from SFTP | `nnn_sftp_rsp_billing_ingest_daily` | `BILLING_DATE` |
| `WHOLESALE.PARTNER_DATA_INGEST` | Partner CSV files from Azure Blob | `nnn_azure_blob_partner_data_daily` | `RUN_DATE` |
| `WHOLESALE.RSP_MANUAL_REPORTS` | Manual weekly Excel reports from IMAP email | `nnn_imap_rsp_report_weekly` | `RUN_DATE` |
| `WHOLESALE.PARTNER_PORTAL_ACTIVITY` | GraphQL partner portal activity | `nnn_graphql_partner_portal_daily` | `RUN_DATE` |

### FINANCE schema

| Table | Description | Source DAG | Partition Key |
|---|---|---|---|
| `FINANCE.CVC_BILLING_LINES` | CVC billing line items | `nnn_cvc_billing_daily` | `BILLING_DATE` |
| `FINANCE.REVENUE_RECOGNITION` | AASB 15 revenue recognition records | `nnn_revenue_recognition_monthly` | `RECOGNITION_DATE` |
| `FINANCE.CAPEX_TRACKING` | SAP PS CAPEX project spend | `nnn_capex_project_tracking_weekly` | `WEEK_ENDING` |
| `FINANCE.LEGACY_BILLING_RECORDS` | Legacy billing from MS SQL Server | `nnn_mssql_legacy_billing_daily` | `BILLING_DATE` |

### COMPLIANCE schema

| Table | Description | Source DAG | Partition Key |
|---|---|---|---|
| `COMPLIANCE.ACCC_METRICS` | ACCC regulatory performance metrics | `nnn_accc_reporting_weekly` | `WEEK_ENDING` |
| `COMPLIANCE.SLA_BREACHES` | CSG SLA breach records | `nnn_sla_compliance_daily` | `BREACH_DATE` |
| `COMPLIANCE.WEEKLY_REGULATORY_EXPORT` | View: anonymised weekly regulatory data | (view, no DAG writes it) | — |

### FIELD_OPS schema

| Table | Description | Source DAG | Partition Key |
|---|---|---|---|
| `FIELD_OPS.TECHNICIAN_SCHEDULE` | Daily technician schedules | `nnn_technician_scheduling_daily` | `SCHEDULE_DATE` |
| `FIELD_OPS.FSA_PERFORMANCE` | Field service appointment completion metrics | `nnn_fsa_completion_reporting` | `REPORT_DATE` |

### INFRASTRUCTURE schema

| Table | Description | Source DAG | Partition Key |
|---|---|---|---|
| `INFRASTRUCTURE.PREMISES` | Premises rollout and activation records | `nnn_premises_activation_etl` | `ACTIVATION_DATE` |
| `INFRASTRUCTURE.ROLLOUT_METRICS` | Daily rollout KPIs | `nnn_premises_activation_etl` | `METRIC_DATE` |
| `INFRASTRUCTURE.PON_AUDIT_WEEKLY` | PON splitter audit results | `nnn_pon_splitter_audit_weekly` | `AUDIT_DATE` |
| `INFRASTRUCTURE.SPLITTER_REMEDIATION_QUEUE` | Splitters flagged for remediation | `nnn_pon_splitter_audit_weekly` | `FLAGGED_DATE` |

### ML schema

| Table | Description | Source DAG | Partition Key |
|---|---|---|---|
| `ML.CX_FEATURE_STORE` | Customer experience ML features | `nnn_customer_experience_features` | `FEATURE_DATE` |
| `ML.RSP_PORTAL_CLICKSTREAM` | Hourly RSP portal clickstream (from Kinesis) | `nnn_kinesis_clickstream_hourly` | `ARRIVAL_TS` window |
| `ML.RSP_SESSION_ANALYTICS` | Daily Redis portal session extracts | `nnn_redis_session_analytics_daily` | `SESSION_DATE` |
| `ML.SESSION_KPI_DAILY` | Computed session KPIs (bounce rate, top RSPs) | `nnn_redis_session_analytics_daily` | `KPI_DATE` |
| `ML.CUSTOMER_FEATURES` | Customer churn/propensity features | ML training pipeline (external) | `FEATURE_DATE` |
| `ML.NETWORK_FEATURES` | Network performance ML features | ML training pipeline (external) | `FEATURE_DATE` |
| `ML.CHURN_LABELS` | Churn label ground truth | ML training pipeline (external) | `LABEL_DATE` |
| `ML.CHURN_PREDICTIONS` | Daily churn propensity scores | ML training pipeline (external) | `PREDICTION_DATE` |

### OPERATIONS schema

| Table | Description | Source DAG | Partition Key |
|---|---|---|---|
| `OPERATIONS.OUTAGE_INCIDENTS` | ServiceNow outage incidents | `nnn_outage_incident_etl` | `INCIDENT_DATE` |
| `OPERATIONS.NOC_INCIDENTS` | MongoDB NOC tool incidents | `nnn_mongodb_noc_incidents_sync_hourly` | `RUN_DATE` |
| `OPERATIONS.FAULT_LOG_SUMMARY` | Elasticsearch fault log daily summary | `nnn_elasticsearch_fault_logs_daily` | `LOG_DATE` |
| `OPERATIONS.ORDER_EVENTS` | RabbitMQ order management events | `nnn_rabbitmq_order_events_15min` | `EVENT_TIMESTAMP` |
| `OPERATIONS.PROVISIONING_EVENTS` | SQS provisioning events | `nnn_sqs_provisioning_events_15min` | `RECEIVED_AT` |
| `OPERATIONS.DAILY_NETWORK_HEALTH_SUMMARY` | Aggregated daily network health KPIs | `nnn_network_performance_daily` (upstream) | `REPORT_DATE` |
| `OPERATIONS.ETL_WATERMARKS` | Incremental load watermarks for all DAGs | All incremental DAGs | `dag_id` |

---

## 4. Redshift Schema & Table Inventory

**Cluster:** `nnn-analytics-redshift.ap-southeast-2.redshift.amazonaws.com`

| Schema | Table | Loaded by | Partition Key | Source (Snowflake) |
|---|---|---|---|---|
| `analytics` | `network_performance_daily` | `nnn_network_performance_redshift_daily` | `report_date` | `NETWORK.LINK_PERFORMANCE_DAILY` |
| `analytics` | `node_health_hourly` | `nnn_infrastructure_health_redshift_hourly` | `metric_hour` | `NETWORK.NODE_HEALTH_HOURLY` |
| `analytics` | `rsp_portal_sessions` | `nnn_dynamodb_portal_sessions_daily` | `session_date` | DynamoDB `nnn-rsp-portal-sessions` |
| `analytics` | `poi_capacity_weekly` | `nnn_poi_capacity_redshift_weekly` | `week_ending` | `NETWORK.POI_TRAFFIC_WEEKLY` |
| `finance` | `cvc_billing_daily` | `nnn_cvc_billing_redshift_daily` | `billing_date` | `FINANCE.CVC_BILLING_LINES` |
| `finance` | `capex_tracking_weekly` | `nnn_capex_tracking_redshift_weekly` | (full-replace) | `FINANCE.CAPEX_TRACKING` |
| `wholesale` | `rsp_activations_daily` | `nnn_rsp_activation_redshift_daily` | `activation_date` | `WHOLESALE.RSP_ACTIVATIONS` |
| `customer` | `nps_sentiment_weekly` | `nnn_nps_sentiment_redshift_weekly` | `survey_date` | `CUSTOMER.NPS_RESPONSES` |
| `ml` | `customer_cx_features` | `nnn_customer_cx_redshift_daily` | `feature_date` | `ML.CX_FEATURE_STORE` |
| `compliance` | `sla_breaches_daily` | `nnn_compliance_breach_redshift_daily` | `breach_date` | `COMPLIANCE.SLA_BREACHES` |
| `regulatory` | `accc_metrics_weekly` | `nnn_accc_regulatory_redshift_weekly` | `week_ending` | `COMPLIANCE.ACCC_METRICS` |
| `operational_reporting` | `network_health_daily` | `nnn_snowflake_to_rds_postgres_daily` | `report_date` | `OPERATIONS.DAILY_NETWORK_HEALTH_SUMMARY` |

---

## 5. DAG Dependency Map

A DAG is **upstream** of another if its output table or S3 path is consumed by the other DAG. Missing an upstream run causes a downstream DAG to fail validation or produce stale data.

```
nnn_rsp_activation_daily
  └──→ nnn_rsp_activation_redshift_daily          (reads WHOLESALE.RSP_ACTIVATIONS)

nnn_network_performance_daily
  └──→ nnn_network_performance_redshift_daily     (reads NETWORK.LINK_PERFORMANCE_DAILY)
  └──→ nnn_snowflake_to_rds_postgres_daily        (reads OPERATIONS.DAILY_NETWORK_HEALTH_SUMMARY)

nnn_node_health_monitoring_hourly
  └──→ nnn_infrastructure_health_redshift_hourly  (reads NETWORK.NODE_HEALTH_HOURLY)

nnn_cvc_billing_daily
  └──→ nnn_cvc_billing_redshift_daily             (reads FINANCE.CVC_BILLING_LINES)

nnn_nps_survey_etl_weekly
  └──→ nnn_nps_sentiment_redshift_weekly          (reads CUSTOMER.NPS_RESPONSES)

nnn_poi_traffic_aggregation_weekly
  └──→ nnn_poi_capacity_redshift_weekly           (reads NETWORK.POI_TRAFFIC_WEEKLY)

nnn_accc_reporting_weekly
  └──→ nnn_accc_regulatory_redshift_weekly        (reads COMPLIANCE.ACCC_METRICS)

nnn_sla_compliance_daily
  └──→ nnn_compliance_breach_redshift_daily       (reads COMPLIANCE.SLA_BREACHES)

nnn_customer_experience_features
  └──→ nnn_customer_cx_redshift_daily             (reads ML.CX_FEATURE_STORE)

nnn_snowflake_to_s3_ml_export_daily               (reads ML.CUSTOMER_FEATURES, NETWORK_FEATURES, CHURN_LABELS)
  └── ML training platform (external)
      └── ML.CHURN_PREDICTIONS refreshed
          └──→ (future) nnn_ml_churn_to_salesforce_daily

nnn_redis_session_analytics_daily
  └── writes ML.RSP_SESSION_ANALYTICS + ML.SESSION_KPI_DAILY  (no current downstream DAG)

nnn_snowflake_to_dynamodb_cache_daily             (reads CUSTOMER.SERVICE_ELIGIBILITY_CURRENT)
  └── RSP portal read-path (external, real-time)

nnn_snowflake_to_elasticsearch_daily              (reads CUSTOMER.SERVICE_AVAILABILITY_CURRENT)
  └── Customer-facing availability checker API (external)

nnn_dynamodb_portal_sessions_daily                (reads DynamoDB nnn-rsp-portal-sessions)
  └── analytics.rsp_portal_sessions in Redshift

nnn_kinesis_clickstream_hourly                    (reads Kinesis nnn-rsp-portal-clickstream)
  └── ML.RSP_PORTAL_CLICKSTREAM (used by ML feature pipelines)

[Multiple ingest DAGs]
  └──→ nnn_s3_to_glue_catalog_daily               (registers all S3 partitions in Glue)
                                                   └── Athena / Redshift Spectrum consumers
```

### Cross-DAG dependency summary table

| Downstream DAG | Depends on (Upstream DAG / source) | Failure impact |
|---|---|---|
| `nnn_network_performance_redshift_daily` | `nnn_network_performance_daily` | Stale Grafana/Tableau dashboards |
| `nnn_cvc_billing_redshift_daily` | `nnn_cvc_billing_daily` | Finance reporting gap |
| `nnn_rsp_activation_redshift_daily` | `nnn_rsp_activation_daily` | Missing activations in wholesale Redshift |
| `nnn_compliance_breach_redshift_daily` | `nnn_sla_compliance_daily` | Compliance dashboard gap |
| `nnn_accc_regulatory_redshift_weekly` | `nnn_accc_reporting_weekly` | Regulatory reporting failure (P1) |
| `nnn_snowflake_to_dynamodb_cache_daily` | Manual upstream ETL refreshing `SERVICE_ELIGIBILITY_CURRENT` | RSP portal shows stale eligibility data |
| `nnn_snowflake_to_s3_ml_export_daily` | ML pipeline writing `ML.CUSTOMER_FEATURES` etc. | ML training job skipped |
| `nnn_s3_to_glue_catalog_daily` | All daily ETL DAGs writing Parquet to S3 | Athena queries return no new data |

---

## 6. SLA Dependency Chains

The SLA of a downstream DAG implicitly includes the time needed for its upstream DAGs to complete. When setting SLAs, consider the full chain duration.

### Morning data availability chain (business-critical)

```
00:00  nnn_cvc_billing_daily         SLA: 4 h → must complete by 04:00
       nnn_rsp_activation_daily
       (Various overnight ETLs)

01:00  nnn_compliance_breach_redshift_daily   SLA: 2 h → by 03:00
       nnn_dynamodb_portal_sessions_daily     SLA: 1 h → by 03:00

02:00  nnn_network_performance_daily (starts)

05:00  nnn_sftp_rsp_billing_ingest_daily      SLA: 2 h → by 07:00

06:00  nnn_snowflake_to_dynamodb_cache_daily  SLA: 1 h → by 07:00  ← portal cache ready

07:00  nnn_snowflake_to_elasticsearch_daily   SLA: 2 h → by 09:00  ← availability API ready
       nnn_snowflake_to_rds_postgres_daily    SLA: 1 h → by 09:00  ← ops dashboards ready

08:00  nnn_cvc_billing_redshift_daily         SLA: 3 h → by 11:00

09:00  nnn_s3_to_glue_catalog_daily           SLA: 1 h → by 10:00  ← Athena ready
       nnn_azure_blob_partner_data_daily      SLA: 2 h → by 12:00

12:00  nnn_snowflake_to_s3_ml_export_daily    SLA: 2 h → by 14:00  ← ML training trigger
```

### Near-real-time chains (no SLA, but monitoring required)

```
*/15  nnn_sqs_provisioning_events_15min       No SLA — monitor queue depth
*/15  nnn_rabbitmq_order_events_15min         No SLA — monitor queue depth

:15   nnn_mqtt_basestation_telemetry_hourly   SLA: 30 min → must complete by :45

:45   nnn_kinesis_clickstream_hourly          SLA: 45 min → must complete by next :30
```

### Regulatory deadline chain (weekly)

```
Wed 20:00  nnn_snowflake_to_gcs_regulatory_weekly  SLA: 3 h → by Wed 23:00
                                                    ACCC portal deadline: Thursday morning
Thu 05:00  nnn_accc_reporting_weekly               SLA: 8 h → by Thu 13:00
Thu 22:00  nnn_accc_regulatory_redshift_weekly     SLA: 4 h → by Fri 02:00
```

---

## 7. External System Data Flows

### Inbound (external → NNN Snowflake)

| External System | Data | Frequency | Ingested by | Format |
|---|---|---|---|---|
| Partner SFTP | RSP billing statements | Daily | `nnn_sftp_rsp_billing_ingest_daily` | CSV |
| Azure Blob Storage | Partner data drops | Daily | `nnn_azure_blob_partner_data_daily` | CSV |
| IMAP email | RSP manual Excel reports | Weekly (Mon) | `nnn_imap_rsp_report_weekly` | .xlsx |
| SOAP Mediation API | CDR records | Daily | `nnn_soap_mediation_cdr_daily` | XML |
| GraphQL Partner Portal | Partner activity | Daily | `nnn_graphql_partner_portal_daily` | JSON |
| MS SQL Server (legacy) | Billing records | Daily | `nnn_mssql_legacy_billing_daily` | Rows |
| MongoDB (NOC tools) | Incident records | Hourly | `nnn_mongodb_noc_incidents_sync_hourly` | BSON |
| Elasticsearch | Fault logs | Daily | `nnn_elasticsearch_fault_logs_daily` | JSON |
| AWS DynamoDB | RSP portal sessions | Daily | `nnn_dynamodb_portal_sessions_daily` | DynamoDB items |
| AWS Kinesis | RSP portal clickstream | Hourly | `nnn_kinesis_clickstream_hourly` | Base64 JSON |
| AWS SQS | Provisioning events | 15 min | `nnn_sqs_provisioning_events_15min` | JSON |
| MQTT → Firehose → S3 | FW base station telemetry | Hourly | `nnn_mqtt_basestation_telemetry_hourly` | JSON Lines |
| RabbitMQ | Order management events | 15 min | `nnn_rabbitmq_order_events_15min` | JSON |
| Redis | RSP portal session cache | Daily | `nnn_redis_session_analytics_daily` | Hash keys |

### Outbound (Snowflake → external)

| Target System | Data | Frequency | Exported by | Format |
|---|---|---|---|---|
| AWS DynamoDB | Service eligibility (portal cache) | Daily | `nnn_snowflake_to_dynamodb_cache_daily` | DynamoDB items |
| Elasticsearch | Service availability index | Daily | `nnn_snowflake_to_elasticsearch_daily` | Bulk JSON |
| RDS PostgreSQL | Network health KPIs | Daily | `nnn_snowflake_to_rds_postgres_daily` | Rows |
| AWS Glue Catalog | S3 partition registrations | Daily | `nnn_s3_to_glue_catalog_daily` | Metadata |
| Google Cloud Storage | Weekly regulatory report | Weekly | `nnn_snowflake_to_gcs_regulatory_weekly` | CSV |
| S3 (ML exports) | ML feature tables | Daily | `nnn_snowflake_to_s3_ml_export_daily` | Parquet |
| Redshift | Analytics tables (10 streams) | Daily/Hourly/Weekly | `dags/redshift/` | Parquet via S3 |
| ACCC API | Regulatory delivery notification | Weekly | `nnn_snowflake_to_gcs_regulatory_weekly` | JSON POST |

---

## 8. Table Ownership Matrix

Use this to find who to contact when a table has data quality issues.

| Table / System | Owner DAG | Business Owner | On-call Escalation |
|---|---|---|---|
| `FINANCE.CVC_BILLING_LINES` | `nnn_cvc_billing_daily` | Finance / Billing team | PagerDuty P1 (SLA 4 h CRITICAL) |
| `COMPLIANCE.ACCC_METRICS` | `nnn_accc_reporting_weekly` | Compliance team | PagerDuty P1 (REGULATORY) |
| `COMPLIANCE.SLA_BREACHES` | `nnn_sla_compliance_daily` | Compliance team | PagerDuty P2 |
| `WHOLESALE.RSP_ACTIVATIONS` | `nnn_rsp_activation_daily` | Wholesale team | PagerDuty P2 |
| `WHOLESALE.PARTNER_DATA_INGEST` | `nnn_azure_blob_partner_data_daily` | Wholesale / Partnerships | PagerDuty P2 |
| `ML.RSP_PORTAL_CLICKSTREAM` | `nnn_kinesis_clickstream_hourly` | ML Platform team | PagerDuty P2 |
| `ML.CX_FEATURE_STORE` | `nnn_customer_experience_features` | ML Platform team | PagerDuty P2 |
| `NETWORK.FW_BASESTATION_TELEMETRY` | `nnn_mqtt_basestation_telemetry_hourly` | Network / NOC | PagerDuty P2 |
| `CUSTOMER.SERVICE_ELIGIBILITY_CURRENT` | Upstream ETL (not in this repo) | Customer team | Check upstream DAG status first |
| `Redshift analytics.*` | Respective `nnn_*_redshift_*` DAG | Data Platform team | Slack `#de-alerts` first |
| `DynamoDB nnn-rsp-portal-cache` | `nnn_snowflake_to_dynamodb_cache_daily` | Customer / RSP Portal team | PagerDuty P2 |
| `Elasticsearch nnn-service-availability` | `nnn_snowflake_to_elasticsearch_daily` | Customer team | PagerDuty P2 |
| `GCS nnn-accc-regulatory-prod` | `nnn_snowflake_to_gcs_regulatory_weekly` | Compliance team | PagerDuty P1 |
| `OPERATIONS.ETL_WATERMARKS` | All incremental DAGs | Data Engineering | Check individual DAG logs |

### ETL Watermarks

`OPERATIONS.ETL_WATERMARKS` tracks the last successful run timestamp for every incremental DAG. Query it to diagnose data gaps:

```sql
SELECT dag_id, last_run_ts, DATEDIFF('hour', last_run_ts, CURRENT_TIMESTAMP()) AS hours_since_last_run
FROM OPERATIONS.ETL_WATERMARKS
WHERE hours_since_last_run > 25     -- daily DAGs that haven't run in 25+ hours
ORDER BY hours_since_last_run DESC;
```
