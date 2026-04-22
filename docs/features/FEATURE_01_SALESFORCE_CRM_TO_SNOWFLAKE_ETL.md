# Feature 01 — Salesforce CRM to Snowflake ETL

| Field | Value |
|---|---|
| **Status** | Ready for Development |
| **Priority** | P1 — High |
| **Owner** | NNN Data Engineering |
| **Stakeholders** | Customer Experience Team, Revenue Analytics, Sales Operations |
| **Target Domain** | `CUSTOMER` schema in Snowflake |
| **Estimated Effort** | 3 sprints (3 weeks) |

---

## 1. Executive Summary

NNN uses Salesforce as its primary CRM system for managing RSP (Retail Service Provider)
accounts, customer cases, and sales opportunities. Currently this data is isolated inside
Salesforce and is not available in the Snowflake data warehouse. This feature adds a daily
ETL pipeline that extracts core Salesforce objects and loads them into the Snowflake
`CUSTOMER` schema, enabling cross-domain analytics such as CX correlation, churn
modelling, and wholesale revenue attribution by RSP.

---

## 2. Business Context & Motivation

### Problems Being Solved

- **No RSP-level CX visibility**: The NPS survey ETL (`nnn_nps_survey_etl_weekly`) loads
  survey scores but cannot join them to RSP account data because account hierarchies live
  only in Salesforce.
- **Churn model missing a key signal**: The ML churn features DAG
  (`nnn_customer_experience_features`) lacks Salesforce case volume and case resolution
  time, which are strong predictors of churn.
- **Manual reporting**: The Revenue Analytics team exports Salesforce reports manually
  every Monday and uploads to SharePoint. This is error-prone and always 3–5 days stale.
- **RSP reconciliation gaps**: `nnn_rsp_reconciliation_weekly` cannot validate service
  counts against contracted entitlements because contract data lives in Salesforce
  Opportunities.

### Expected Outcomes

- A single source of truth for RSP account and contract data in Snowflake.
- Automated daily refresh — no manual Salesforce exports.
- Salesforce case metrics available as ML features for churn prediction.
- Revenue Analytics team can self-serve from Redshift instead of SharePoint exports.

---

## 3. Source System Overview

| Attribute | Detail |
|---|---|
| **System** | Salesforce (Sales Cloud + Service Cloud) |
| **Access method** | Salesforce REST API v57.0 (SOQL queries) |
| **Authentication** | OAuth 2.0 — JWT Bearer Token Flow (server-to-server; no user login) |
| **Airflow connection** | `nnn_salesforce_prod` (type: HTTP, with OAuth credentials stored) |
| **Rate limits** | 100,000 API calls per 24 hours; bulk query API preferred for large objects |
| **Bulk API** | Salesforce Bulk API 2.0 available for objects > 10,000 records |
| **Environment** | Production org; a dedicated integration user with read-only profile |

### Salesforce Objects in Scope

| Salesforce Object | API Name | Approx. Record Count | Update Frequency |
|---|---|---|---|
| Account (RSP) | `Account` | ~400 records | Low — weekly changes |
| Contact | `Contact` | ~2,000 records | Low — weekly changes |
| Case (Support) | `Case` | ~150,000 total; ~500/day new | High — real-time in SF |
| Case Comment | `CaseComment` | ~600,000 total | High |
| Opportunity (Contract) | `Opportunity` | ~1,200 records | Low |
| Opportunity Line Item | `OpportunityLineItem` | ~5,000 records | Low |
| Task (Activity) | `Task` | ~80,000 total; ~200/day | Medium |

---

## 4. Target Schema Design

All tables land in the Snowflake `CUSTOMER` schema. Column names use `snake_case`.
All tables include `_sf_id` (Salesforce record ID), `_loaded_at` (pipeline load timestamp),
and `run_date` (pipeline execution date) for lineage and idempotent reloads.

### 4.1 `CUSTOMER.SF_ACCOUNTS`

Represents RSP organisations that NNN has a wholesale relationship with.

| Column | Type | Description |
|---|---|---|
| `sf_account_id` | VARCHAR(18) PK | Salesforce Account ID |
| `account_name` | VARCHAR | RSP trading name |
| `account_type` | VARCHAR | Account segment: `RSP`, `Wholesaler`, `Partner` |
| `abn` | VARCHAR(11) | Australian Business Number |
| `billing_state` | VARCHAR | State/territory |
| `nnn_rsp_code` | VARCHAR | Internal RSP code — join key to NNN billing systems |
| `account_status` | VARCHAR | `Active`, `Inactive`, `Churned` |
| `contracted_services` | INTEGER | Count of contracted service lines (from Opportunity) |
| `account_owner_name` | VARCHAR | Assigned NNN account manager name |
| `created_date` | DATE | Date account was created in Salesforce |
| `last_modified_date` | TIMESTAMP_NTZ | Last modification in Salesforce |
| `run_date` | DATE | Pipeline execution date |
| `_loaded_at` | TIMESTAMP_NTZ | Row load timestamp |

### 4.2 `CUSTOMER.SF_CASES`

All support cases raised by RSPs or customers. High-volume table; partitioned by `run_date`.

| Column | Type | Description |
|---|---|---|
| `sf_case_id` | VARCHAR(18) PK | Salesforce Case ID |
| `case_number` | VARCHAR | Human-readable case reference |
| `sf_account_id` | VARCHAR(18) FK | Parent RSP account |
| `subject` | VARCHAR | Case subject line |
| `case_type` | VARCHAR | `Technical`, `Billing`, `Provisioning`, `Complaint` |
| `status` | VARCHAR | `New`, `In Progress`, `Escalated`, `Resolved`, `Closed` |
| `priority` | VARCHAR | `Low`, `Medium`, `High`, `Critical` |
| `origin` | VARCHAR | `Phone`, `Email`, `Web`, `Chat` |
| `created_date` | TIMESTAMP_NTZ | When case was opened |
| `closed_date` | TIMESTAMP_NTZ | When case was closed (NULL if open) |
| `resolution_hours` | DECIMAL(10,2) | Hours from opened to closed |
| `first_response_hours` | DECIMAL(10,2) | Hours from opened to first agent response |
| `escalated_flag` | BOOLEAN | TRUE if case was escalated at any point |
| `run_date` | DATE | Pipeline execution date |
| `_loaded_at` | TIMESTAMP_NTZ | Row load timestamp |

### 4.3 `CUSTOMER.SF_OPPORTUNITIES`

Wholesale contracts and service agreements.

| Column | Type | Description |
|---|---|---|
| `sf_opportunity_id` | VARCHAR(18) PK | Salesforce Opportunity ID |
| `sf_account_id` | VARCHAR(18) FK | Parent RSP account |
| `opportunity_name` | VARCHAR | Contract or deal name |
| `stage` | VARCHAR | `Prospecting`, `Negotiation`, `Closed Won`, `Closed Lost` |
| `contract_type` | VARCHAR | `Annual`, `Multi-Year`, `Month-to-Month` |
| `contract_start_date` | DATE | Contract commencement |
| `contract_end_date` | DATE | Contract expiry |
| `mrr_aud` | DECIMAL(15,2) | Monthly recurring revenue in AUD |
| `arr_aud` | DECIMAL(15,2) | Annual recurring revenue in AUD |
| `service_count` | INTEGER | Number of service lines in contract |
| `close_date` | DATE | Opportunity close/expected-close date |
| `run_date` | DATE | Pipeline execution date |
| `_loaded_at` | TIMESTAMP_NTZ | Row load timestamp |

### 4.4 `CUSTOMER.SF_CONTACTS`

RSP-side contacts associated with NNN accounts.

| Column | Type | Description |
|---|---|---|
| `sf_contact_id` | VARCHAR(18) PK | Salesforce Contact ID |
| `sf_account_id` | VARCHAR(18) FK | Parent RSP account |
| `first_name` | VARCHAR | |
| `last_name` | VARCHAR | |
| `title` | VARCHAR | Job title |
| `email` | VARCHAR | Work email (PII — masked in non-prod) |
| `phone` | VARCHAR | Work phone (PII — masked in non-prod) |
| `contact_role` | VARCHAR | `Technical`, `Billing`, `Executive Sponsor` |
| `active_flag` | BOOLEAN | |
| `run_date` | DATE | Pipeline execution date |
| `_loaded_at` | TIMESTAMP_NTZ | Row load timestamp |

---

## 5. Data Flow

```
Salesforce REST API (SOQL / Bulk API 2.0)
    │
    ▼  [Extract — paginated SOQL or bulk job]
S3 Data Lake
nnn-data-lake-prod/salesforce/<object>/run_date=YYYY-MM-DD/data.parquet
    │
    ▼  [Transform — resolve FKs, compute derived fields, mask PII in non-prod]
Snowflake STAGING Schema (temp tables)
    │
    ▼  [MERGE — upsert on SF record ID + run_date]
Snowflake CUSTOMER Schema
    ├── SF_ACCOUNTS
    ├── SF_CASES
    ├── SF_OPPORTUNITIES
    └── SF_CONTACTS
    │
    ▼  [Downstream consumers]
    ├── CUSTOMER.CUSTOMER_HEALTH_SCORE (existing view — will JOIN SF_CASES)
    ├── ML.CHURN_FEATURES (existing table — new columns from SF_CASES)
    └── Redshift analytics.rsp_performance (via existing Snowflake→Redshift DAG)
```

---

## 6. Load Strategy

| Object | Strategy | Reason |
|---|---|---|
| `SF_ACCOUNTS` | Full reload daily | Low volume (~400); simplest correctness guarantee |
| `SF_CONTACTS` | Full reload daily | Low volume (~2,000) |
| `SF_OPPORTUNITIES` | Full reload daily | Low volume (~1,200) |
| `SF_CASES` | Incremental — extract WHERE `LastModifiedDate > last_watermark` | High volume; only modified records change daily |
| `SF_CASE_COMMENTS` | Incremental — extract WHERE `LastModifiedDate > last_watermark` | High volume |

**Incremental watermark** for cases: stored in `OPERATIONS.PIPELINE_RUN_LOG` as
`last_sf_modified_ts`. Updated to `MAX(LastModifiedDate)` of extracted records only
after a successful load. On first run or after a manual clear, defaults to 90 days ago
(configurable).

**Idempotency**: For incremental loads, a MERGE on `(sf_case_id)` is used. Re-running
the same window updates existing rows and inserts any missed new rows — never duplicates.

---

## 7. Transformation Rules

| Rule ID | Object | Rule |
|---|---|---|
| T-01 | All | `LastModifiedDate` from Salesforce (UTC) converted to AEST for `last_modified_date` column |
| T-02 | `SF_CASES` | `resolution_hours` = `(ClosedDate - CreatedDate)` in decimal hours; NULL if case is open |
| T-03 | `SF_CASES` | `first_response_hours` derived from the earliest `CaseComment` created by an internal user |
| T-04 | `SF_CASES` | `escalated_flag` = TRUE if `IsEscalated = true` at any point in case history |
| T-05 | `SF_ACCOUNTS` | `nnn_rsp_code` mapped from Salesforce custom field `NNN_RSP_Code__c`. This field is confirmed to match the `rsp_code` field in NNN billing systems and is the authoritative join key across all domains. Accounts where `NNN_RSP_Code__c` is blank are loaded with `nnn_rsp_code = NULL` and flagged for review by the Sales Operations team |
| T-06 | `SF_CONTACTS` | In non-prod environments, `email` and `phone` replaced with `masked@nnnco.com.au` and `+61200000000` respectively |
| T-07 | `SF_OPPORTUNITIES` | `mrr_aud` = `Amount / contract_duration_months`; `arr_aud` = `mrr_aud * 12` |
| T-08 | All | Records with `IsDeleted = true` in Salesforce are soft-deleted: `active_flag` set to FALSE, not physically removed |

---

## 8. Data Quality Requirements

| Check | Object | Threshold | Action on Failure |
|---|---|---|---|
| Row count minimum | `SF_CASES` (daily new/modified) | >= 10 records per run | Fail the pipeline |
| Null rate on FK | `SF_CASES.sf_account_id` | 0% null | Fail the pipeline |
| Null rate on key date | `SF_CASES.created_date` | 0% null | Fail the pipeline |
| Value constraint | `SF_CASES.resolution_hours` | >= 0 when not NULL | Warn only |
| Account referential integrity | `SF_CASES.sf_account_id` must exist in `SF_ACCOUNTS` | 0% orphans | Warn only |
| Duplicate key | `SF_CASES` on `sf_case_id` | 0 duplicates | Fail the pipeline |

---

## 9. Schedule & SLA

| Attribute | Value |
|---|---|
| **Schedule** | Daily at 01:00 AEST (15:00 UTC) |
| **SLA** | Must complete within 2 hours (before 03:00 AEST) |
| **Retries** | 3 attempts, 10-minute delay between retries |
| **SLA miss behaviour** | `nnn_sla_miss_alert` fires PagerDuty `error` (P2) |
| **Dependency** | None — this DAG is a source DAG; no upstream Airflow dependency |
| **Downstream trigger** | On success, signals readiness for downstream ML feature pipelines that consume `CUSTOMER.SF_CASES` |

---

## 10. Non-Functional Requirements

| Requirement | Detail |
|---|---|
| **Volume** | `SF_CASES` incremental extract expected 300–700 modified records/day; full table ~150,000 rows |
| **Latency** | Data in Snowflake must reflect Salesforce state as of midnight AEST |
| **PII handling** | `email` and `phone` in `SF_CONTACTS` masked in staging/dev environments; production access restricted to `nnn-data-engineering` role |
| **Auditability** | Every loaded row carries `_loaded_at` and `run_date`; all Salesforce record IDs preserved |
| **Re-runability** | Any DAG run can be safely re-triggered for the same date without creating duplicates |
| **Connection credential rotation** | OAuth JWT private key stored in Airflow connection; rotation does not require DAG code changes |

---

## 11. New Airflow DAG

| Attribute | Value |
|---|---|
| **DAG ID** | `nnn_salesforce_crm_daily` |
| **File** | `dags/integrations/nnn_salesforce_crm_daily.py` |
| **Schedule** | `0 15 * * *` (01:00 AEST) |
| **Tags** | `nnn`, `salesforce`, `customer`, `daily` |
| **Max active runs** | 1 |

**Task sequence:**

```
check_salesforce_api
    │
    ▼
extract_accounts ──────────────────────────────────┐
extract_contacts ──────────────────────────────────┤  [parallel]
extract_opportunities ─────────────────────────────┤
extract_cases_incremental ─────────────────────────┘
    │
    ▼
transform_and_stage          [single task — dbt model or PythonOperator]
    │
    ▼
merge_accounts ────────────────────────────────────┐
merge_contacts ────────────────────────────────────┤  [parallel]
merge_opportunities ───────────────────────────────┤
merge_cases ───────────────────────────────────────┘
    │
    ▼
validate_load                [row counts + null checks per table]
    │
    ▼
update_pipeline_log
```

---

## 12. Acceptance Criteria

| # | Criterion |
|---|---|
| AC-01 | All 4 target tables are created in Snowflake `CUSTOMER` schema with the defined columns |
| AC-02 | Daily run extracts all Accounts, Contacts, and Opportunities (full reload) |
| AC-03 | Daily run extracts only Cases modified since last successful watermark |
| AC-04 | Re-running the DAG for the same date produces identical row counts (no duplicates) |
| AC-05 | `resolution_hours` is NULL for all open cases and a positive decimal for closed cases |
| AC-06 | `nnn_rsp_code` successfully joins to RSP identifiers in `CUSTOMER.RSP_ACTIVATIONS` |
| AC-07 | In staging environment, `email` and `phone` in SF_CONTACTS are masked |
| AC-08 | A Salesforce API outage causes the pipeline to retry 3 times before alerting PagerDuty |
| AC-09 | Soft-deleted Salesforce records have `active_flag = FALSE` in Snowflake (not physically deleted) |
| AC-10 | DAG completes within the 2-hour SLA window on a representative data volume |

---

## 13. Out of Scope

- Salesforce objects not listed in Section 3 (e.g. Lead, Campaign, Quote, Contract object).
- Real-time / streaming sync from Salesforce platform events.
- Write-back from Snowflake to Salesforce.
- Einstein Analytics integration.
- Salesforce Sandbox environment ETL (production only).

---

## 14. Dependencies & Prerequisites

- Salesforce connected app name is `NNN_Airflow_Integration`; owned and maintained by the Salesforce Admin team. JWT private key is stored in the `nnn_salesforce_prod` Airflow connection.
- `nnn_salesforce_prod` Airflow connection configured with OAuth JWT Bearer Token credentials (client ID, private key, Salesforce username).
- `CUSTOMER` schema already exists in Snowflake (confirmed — used by existing DAGs).
- `OPERATIONS.PIPELINE_RUN_LOG` table exists for watermark storage.
- PII data classification review completed and approved by Privacy Officer before go-live. `SF_CASE_COMMENTS` body text is excluded from Snowflake entirely due to free-text PII risk — comment counts and timestamps are captured on `SF_CASES` instead.
- On first run the incremental watermark for `SF_CASES` defaults to `2020-01-01`. Cases created before 2020 are not backfilled — the Analytics team has confirmed pre-2020 history is not required for current use cases.
