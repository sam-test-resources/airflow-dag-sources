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
NMS REST API    ──→│
MongoDB (NOC)   ──→│  dags/integrations/   ──→  Snowflake    ──→  dags/redshift/  ──→  Redshift
MS SQL Server   ──→│  (ingest DAGs)              (source of         (3 streams)         (analytics)
RabbitMQ        ──→│                              truth)
                    │
Kinesis         ──→┤                        ──→  S3 Data Lake ──→  ML training platform
MQTT/Firehose   ──→┘                             (raw + processed    (external)
                                                  Parquet)

Oracle EBS/OSS  ──→  dags/customer/         ──→  Oracle EBS   ──→  Finance systems
ServiceNow      ──→  dags/finance/               (write-back)
                     dags/network/
                     dags/compliance/
                     dags/infrastructure/
                     dags/wholesale/
                     (core ETL DAGs)

                     dags/integrations/     ──→  S3 (ML exports) ──→  ML training jobs
                     (export DAGs)          ──→  Redshift (3 streams)
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
| `wholesale/` | WHOLESALE schema | `rsp_billing_statements`, `wholesale_orders` |
| `finance/` | FINANCE schema | `invoice_summary`, `payment_events`, `revenue_daily` |
| `operations/` | OPERATIONS schema | `order_events`, `ticket_summary` |
| `ml-exports/` | ML schema exports | `customer_features`, `network_features`, `churn_labels` |
| `analytics/` | Redshift staging | `network_performance`, `cvc_billing`, `capex_tracking` |
| `mqtt/` | IoT / MQTT data | `fw-telemetry/<run_date>T<hour>/data.json` |
| `compliance/` | COMPLIANCE schema | ACCC metrics |

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
| `NETWORK.CVC_UTILISATION_HOURLY` | CVC capacity utilisation per hour per POI | `nnn_capacity_utilisation_hourly` | `METRIC_HOUR` |
| `NETWORK.NODE_HEALTH_HOURLY` | SNMP/NetFlow node health metrics | `nnn_node_health_monitoring_hourly` | `METRIC_HOUR` |
| `NETWORK.NODE_HEALTH_ALERTS` | Active threshold breach alerts | `nnn_node_health_monitoring_hourly` | `ALERT_DATE` |
| `NETWORK.FW_BASESTATION_TELEMETRY` | Fixed Wireless base station IoT telemetry | `nnn_mqtt_basestation_telemetry_hourly` | `RUN_DATE`, `TELEMETRY_TS` |

### CUSTOMER schema

| Table | Description | Source DAG | Partition Key |
|---|---|---|---|
| `CUSTOMER.NPS_RESPONSES` | NPS survey responses from Medallia | `nnn_nps_survey_etl_weekly` | `SURVEY_DATE` |
| `CUSTOMER.SERVICE_ELIGIBILITY_CURRENT` | Current-state address eligibility (full snapshot) | `nnn_rsp_activation_daily` (upstream) | None (full-replace) |
| `CUSTOMER.SERVICE_AVAILABILITY_CURRENT` | Current-state address availability (full snapshot) | Various ETL | None (full-replace) |

### WHOLESALE schema

| Table | Description | Source DAG | Partition Key |
|---|---|---|---|
| `WHOLESALE.RSP_ACTIVATIONS` | RSP service activation records from Oracle OSS | `nnn_rsp_activation_daily` | `ACTIVATION_DATE` |
| `WHOLESALE.RSP_BILLING_STATEMENTS` | RSP billing CSV from SFTP | `nnn_sftp_rsp_billing_ingest_daily` | `BILLING_DATE` |

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
| `COMPLIANCE.WEEKLY_REGULATORY_EXPORT` | View: anonymised weekly regulatory data | (view, no DAG writes it) | — |

### INFRASTRUCTURE schema

| Table | Description | Source DAG | Partition Key |
|---|---|---|---|
| `INFRASTRUCTURE.PON_AUDIT_WEEKLY` | PON splitter audit results | `nnn_pon_splitter_audit_weekly` | `AUDIT_DATE` |
| `INFRASTRUCTURE.SPLITTER_REMEDIATION_QUEUE` | Splitters flagged for remediation | `nnn_pon_splitter_audit_weekly` | `FLAGGED_DATE` |

### ML schema

| Table | Description | Source DAG | Partition Key |
|---|---|---|---|
| `ML.RSP_PORTAL_CLICKSTREAM` | Hourly RSP portal clickstream (from Kinesis) | `nnn_kinesis_clickstream_hourly` | `ARRIVAL_TS` window |
| `ML.CUSTOMER_FEATURES` | Customer churn/propensity features | ML training pipeline (external) | `FEATURE_DATE` |
| `ML.NETWORK_FEATURES` | Network performance ML features | ML training pipeline (external) | `FEATURE_DATE` |
| `ML.CHURN_LABELS` | Churn label ground truth | ML training pipeline (external) | `LABEL_DATE` |
| `ML.CHURN_PREDICTIONS` | Daily churn propensity scores | ML training pipeline (external) | `PREDICTION_DATE` |

### OPERATIONS schema

| Table | Description | Source DAG | Partition Key |
|---|---|---|---|
| `OPERATIONS.OUTAGE_INCIDENTS` | ServiceNow outage incidents | `nnn_outage_incident_etl` | `INCIDENT_DATE` |
| `OPERATIONS.NOC_INCIDENTS` | MongoDB NOC tool incidents | `nnn_mongodb_noc_incidents_sync_hourly` | `RUN_DATE` |
| `OPERATIONS.ORDER_EVENTS` | RabbitMQ order management events | `nnn_rabbitmq_order_events_15min` | `EVENT_TIMESTAMP` |
| `OPERATIONS.ETL_WATERMARKS` | Incremental load watermarks for all DAGs | All incremental DAGs | `dag_id` |

---

## 4. Redshift Schema & Table Inventory

**Cluster:** `nnn-analytics-redshift.ap-southeast-2.redshift.amazonaws.com`

| Schema | Table | Loaded by | Partition Key | Source (Snowflake) |
|---|---|---|---|---|
| `analytics` | `network_performance_daily` | `nnn_network_performance_redshift_daily` | `report_date` | `NETWORK.LINK_PERFORMANCE_DAILY` |
| `finance` | `cvc_billing_daily` | `nnn_cvc_billing_redshift_daily` | `billing_date` | `FINANCE.CVC_BILLING_LINES` |
| `finance` | `capex_tracking_weekly` | `nnn_capex_tracking_redshift_weekly` | (full-replace) | `FINANCE.CAPEX_TRACKING` |

---

## 5. DAG Dependency Map

A DAG is **upstream** of another if its output table or S3 path is consumed by the other DAG. Missing an upstream run causes a downstream DAG to fail validation or produce stale data.

```
nnn_capex_project_tracking_weekly
  └──→ nnn_capex_tracking_redshift_weekly          (reads FINANCE.CAPEX_TRACKING)

nnn_snowflake_to_s3_ml_export_daily                (reads ML.CUSTOMER_FEATURES, NETWORK_FEATURES, CHURN_LABELS)
  └── ML training platform (external)
      └── ML.CHURN_PREDICTIONS refreshed
          └──→ (future) nnn_ml_churn_to_salesforce_daily

nnn_kinesis_clickstream_hourly                     (reads Kinesis nnn-rsp-portal-clickstream)
  └── ML.RSP_PORTAL_CLICKSTREAM (used by ML feature pipelines)
```

### Cross-DAG dependency summary table

| Downstream DAG | Depends on (Upstream DAG / source) | Failure impact |
|---|---|---|
| `nnn_capex_tracking_redshift_weekly` | `nnn_capex_project_tracking_weekly` | Finance CAPEX reporting gap in Redshift |
| `nnn_snowflake_to_s3_ml_export_daily` | ML pipeline writing `ML.CUSTOMER_FEATURES` etc. | ML training job skipped |

---

## 6. SLA Dependency Chains

The SLA of a downstream DAG implicitly includes the time needed for its upstream DAGs to complete. When setting SLAs, consider the full chain duration.

### Morning data availability chain (business-critical)

```
00:00  nnn_rsp_activation_daily        SLA: 4 h → must complete by 04:00
       (Various overnight ETLs)

05:00  nnn_sftp_rsp_billing_ingest_daily      SLA: 2 h → by 07:00

08:00  nnn_cvc_billing_redshift_daily         SLA: 3 h → by 11:00

12:00  nnn_snowflake_to_s3_ml_export_daily    SLA: 2 h → by 14:00  ← ML training trigger
```

### Near-real-time chains (no SLA, but monitoring required)

```
*/15  nnn_rabbitmq_order_events_15min         No SLA — monitor queue depth

:15   nnn_mqtt_basestation_telemetry_hourly   SLA: 30 min → must complete by :45

:45   nnn_kinesis_clickstream_hourly          SLA: 45 min → must complete by next :30
```

### Regulatory deadline chain (weekly)

```
Thu 05:00  nnn_accc_reporting_weekly          SLA: 8 h → by Thu 13:00
           (reads COMPLIANCE.ACCC_METRICS for ACCC Part 8 submission)
```

---

## 7. External System Data Flows

### Inbound (external → NNN Snowflake)

| External System | Data | Frequency | Ingested by | Format |
|---|---|---|---|---|
| Partner SFTP | RSP billing statements | Daily | `nnn_sftp_rsp_billing_ingest_daily` | CSV |
| NMS REST API | Outage and incident records | Daily | `nnn_outage_incident_etl` | JSON |
| Oracle EBS/OSS | RSP activation records | Daily | `nnn_rsp_activation_daily` | Rows |
| MS SQL Server (legacy) | Billing records | Daily | `nnn_mssql_legacy_billing_daily` | Rows |
| MongoDB (NOC tools) | Incident records | Hourly | `nnn_mongodb_noc_incidents_sync_hourly` | BSON |
| AWS Kinesis | RSP portal clickstream | Hourly | `nnn_kinesis_clickstream_hourly` | Base64 JSON |
| MQTT → Firehose → S3 | FW base station telemetry | Hourly | `nnn_mqtt_basestation_telemetry_hourly` | JSON Lines |
| RabbitMQ | Order management events | 15 min | `nnn_rabbitmq_order_events_15min` | JSON |

### Outbound (Snowflake → external)

| Target System | Data | Frequency | Exported by | Format |
|---|---|---|---|---|
| S3 (ML exports) | ML feature tables | Daily | `nnn_snowflake_to_s3_ml_export_daily` | Parquet |
| Redshift | Analytics tables (3 streams) | Daily/Weekly | `dags/redshift/` | Parquet via S3 |

---

## 8. Table Ownership Matrix

Use this to find who to contact when a table has data quality issues.

| Table / System | Owner DAG | Business Owner | On-call Escalation |
|---|---|---|---|
| `FINANCE.CVC_BILLING_LINES` | `nnn_cvc_billing_daily` | Finance / Billing team | PagerDuty P1 (SLA 4 h CRITICAL) |
| `COMPLIANCE.ACCC_METRICS` | `nnn_accc_reporting_weekly` | Compliance team | PagerDuty P1 (REGULATORY) |
| `WHOLESALE.RSP_ACTIVATIONS` | `nnn_rsp_activation_daily` | Wholesale team | PagerDuty P2 |
| `ML.RSP_PORTAL_CLICKSTREAM` | `nnn_kinesis_clickstream_hourly` | ML Platform team | PagerDuty P2 |
| `NETWORK.FW_BASESTATION_TELEMETRY` | `nnn_mqtt_basestation_telemetry_hourly` | Network / NOC | PagerDuty P2 |
| `CUSTOMER.SERVICE_ELIGIBILITY_CURRENT` | Upstream ETL (not in this repo) | Customer team | Check upstream DAG status first |
| `Redshift analytics.*` | Respective `nnn_*_redshift_*` DAG | Data Platform team | Slack `#de-alerts` first |
| `OPERATIONS.ETL_WATERMARKS` | All incremental DAGs | Data Engineering | Check individual DAG logs |

### ETL Watermarks

`OPERATIONS.ETL_WATERMARKS` tracks the last successful run timestamp for every incremental DAG. Query it to diagnose data gaps:

```sql
SELECT dag_id, last_run_ts, DATEDIFF('hour', last_run_ts, CURRENT_TIMESTAMP()) AS hours_since_last_run
FROM OPERATIONS.ETL_WATERMARKS
WHERE hours_since_last_run > 25     -- daily DAGs that haven't run in 25+ hours
ORDER BY hours_since_last_run DESC;
```
