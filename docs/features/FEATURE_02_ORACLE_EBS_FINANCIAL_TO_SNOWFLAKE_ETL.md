# Feature 02 — Oracle EBS Financial Data to Snowflake ETL

| Field | Value |
|---|---|
| **Status** | Ready for Development |
| **Priority** | P1 — High |
| **Owner** | NNN Data Engineering |
| **Stakeholders** | Finance, Revenue Assurance, CFO Office |
| **Target Domain** | `FINANCE` schema in Snowflake |
| **Estimated Effort** | 4 sprints (4 weeks) |

---

## 1. Executive Summary

NNN's financial source of truth is Oracle E-Business Suite (EBS), which manages
Accounts Payable (AP), Accounts Receivable (AR), General Ledger (GL), and fixed asset
tracking. Financial data currently reaches Snowflake only through manual extracts
produced by the Finance team and uploaded to S3 on an ad-hoc basis. This feature
replaces those manual extracts with a fully automated, scheduled ETL pipeline that pulls
Oracle EBS financial data daily and makes it available in the Snowflake `FINANCE` schema
for reporting, revenue assurance, and the existing CAPEX tracking DAG.

---

## 2. Business Context & Motivation

### Problems Being Solved

- **Manual financial extract bottleneck**: Finance runs 3–5 manual Oracle EBS reports every
  Monday morning, exports them to CSV, and uploads to SharePoint. This process takes 2–3
  hours and is delayed if any team member is unavailable.
- **Stale CAPEX data**: `nnn_capex_project_tracking_weekly` reads from a manually maintained
  S3 file. Project actuals in Oracle EBS are never synced automatically.
- **No GL-to-Snowflake link**: Revenue figures in `FINANCE.REVENUE_RECOGNITION` are derived
  from billing system exports (Oracle OSS), but are never reconciled against Oracle EBS
  GL journal entries. Revenue assurance has no automated variance detection.
- **Audit gaps**: There is no timestamped record of when financial data was loaded or
  which Oracle EBS period it came from.

### Expected Outcomes

- Daily automated GL, AP, and AR extracts replacing all manual Monday uploads.
- `nnn_capex_project_tracking_weekly` reads directly from Snowflake instead of S3 flat files.
- Finance team can reconcile EBS GL balances against billing revenue in Snowflake with a single query.
- All loads are timestamped, auditable, and re-runnable without human intervention.

---

## 3. Source System Overview

| Attribute | Detail |
|---|---|
| **System** | Oracle E-Business Suite R12.2 |
| **Access method** | Direct JDBC connection to Oracle DB via read-only replica |
| **Authentication** | Oracle DB user — read-only account `NNN_ETL_READER` on replica |
| **Airflow connection** | `nnn_oracle_ebs_prod` (type: Oracle / JDBC; host: EBS read replica) |
| **Key schemas** | `AR` (Receivables), `AP` (Payables), `GL` (General Ledger), `FA` (Fixed Assets), `PA` (Projects) |
| **DB version** | Oracle Database 19c |
| **Replica lag** | Read replica is approximately 5 minutes behind primary; acceptable for daily ETL |
| **Access restriction** | Only the read-only replica is accessible from the Airflow worker network; primary DB is not reachable |

### Key Oracle EBS Tables in Scope

| EBS Table | Module | Description |
|---|---|---|
| `GL.GL_JE_HEADERS` | General Ledger | Journal entry batch headers |
| `GL.GL_JE_LINES` | General Ledger | Individual GL debit/credit line items |
| `GL.GL_CODE_COMBINATIONS` | General Ledger | Chart of accounts — cost centre / account segment mapping |
| `GL.GL_PERIODS` | General Ledger | Accounting period definitions |
| `AR.RA_CUSTOMER_TRX_ALL` | Accounts Receivable | Invoice transactions |
| `AR.RA_CUSTOMER_TRX_LINES_ALL` | Accounts Receivable | Invoice line items |
| `AR.AR_CASH_RECEIPTS_ALL` | Accounts Receivable | Payments received |
| `AP.AP_INVOICES_ALL` | Accounts Payable | Supplier invoices |
| `AP.AP_INVOICE_LINES_ALL` | Accounts Payable | Supplier invoice line items |
| `AP.AP_CHECKS_ALL` | Accounts Payable | Payment runs |
| `PA.PA_PROJECTS_ALL` | Projects | CAPEX project master |
| `PA.PA_EXPENDITURES_ALL` | Projects | Project expenditure transactions |
| `FA.FA_ADDITIONS` | Fixed Assets | Asset additions and disposals |

---

## 4. Target Schema Design

All tables are in Snowflake `FINANCE` schema. All tables carry `_ebs_last_update_date`
(the Oracle EBS `LAST_UPDATE_DATE` of the source record), `run_date`, and `_loaded_at`
for auditability.

### 4.1 `FINANCE.GL_JOURNAL_LINES`

Daily GL journal entry lines — the core financial ledger in Snowflake.

| Column | Type | Description |
|---|---|---|
| `je_line_id` | NUMBER PK | Surrogate: `header_id + line_num` concatenated |
| `je_header_id` | NUMBER | Oracle GL_JE_HEADERS.JE_HEADER_ID |
| `period_name` | VARCHAR | Accounting period, e.g. `JAN-25` |
| `ledger_id` | NUMBER | Oracle ledger (entity) ID |
| `currency_code` | VARCHAR(3) | Transaction currency, e.g. `AUD` |
| `accounted_dr` | DECIMAL(18,2) | Debit amount in functional currency (AUD) |
| `accounted_cr` | DECIMAL(18,2) | Credit amount in functional currency (AUD) |
| `cost_centre` | VARCHAR | Segment 2 from GL_CODE_COMBINATIONS |
| `account_code` | VARCHAR | Segment 3 — natural account |
| `account_description` | VARCHAR | Human-readable account name |
| `project_code` | VARCHAR | Segment 4 — CAPEX project code (NULL for opex) |
| `je_source` | VARCHAR | Origin system: `Manual`, `Receivables`, `Payables`, `Projects` |
| `je_category` | VARCHAR | `Revenue`, `Cost of Sales`, `CAPEX`, `Accrual`, etc. |
| `effective_date` | DATE | Accounting date of the transaction |
| `posted_flag` | BOOLEAN | TRUE if the journal has been posted to the ledger |
| `run_date` | DATE | Pipeline execution date |
| `_ebs_last_update_date` | TIMESTAMP_NTZ | Oracle LAST_UPDATE_DATE — used for incremental detection |
| `_loaded_at` | TIMESTAMP_NTZ | Row load timestamp |

### 4.2 `FINANCE.AR_INVOICES`

Customer (RSP) invoices raised by NNN — the AR sub-ledger.

| Column | Type | Description |
|---|---|---|
| `trx_id` | NUMBER PK | Oracle RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID |
| `invoice_number` | VARCHAR | Human-readable invoice reference |
| `customer_id` | NUMBER | Oracle customer ID (joins to `CUSTOMER.SF_ACCOUNTS.nnn_rsp_code` via mapping) |
| `customer_name` | VARCHAR | RSP name as recorded in Oracle |
| `invoice_date` | DATE | Invoice issue date |
| `due_date` | DATE | Payment due date |
| `currency_code` | VARCHAR(3) | `AUD` |
| `invoice_amount` | DECIMAL(18,2) | Total invoice value |
| `tax_amount` | DECIMAL(18,2) | GST component |
| `amount_due_remaining` | DECIMAL(18,2) | Unpaid balance |
| `payment_status` | VARCHAR | `Open`, `Partially Paid`, `Paid`, `Overdue` |
| `payment_received_date` | DATE | Date payment cleared (NULL if outstanding) |
| `invoice_type` | VARCHAR | `Wholesale Services`, `CVC`, `ACCC Levy`, etc. |
| `run_date` | DATE | Pipeline execution date |
| `_ebs_last_update_date` | TIMESTAMP_NTZ | |
| `_loaded_at` | TIMESTAMP_NTZ | |

### 4.3 `FINANCE.AP_INVOICES`

Supplier invoices received by NNN — the AP sub-ledger.

| Column | Type | Description |
|---|---|---|
| `invoice_id` | NUMBER PK | Oracle AP_INVOICES_ALL.INVOICE_ID |
| `invoice_number` | VARCHAR | Supplier invoice reference |
| `vendor_id` | NUMBER | Oracle vendor (supplier) ID |
| `vendor_name` | VARCHAR | Supplier name |
| `vendor_site` | VARCHAR | Supplier billing site |
| `invoice_date` | DATE | |
| `payment_due_date` | DATE | |
| `currency_code` | VARCHAR(3) | |
| `invoice_amount` | DECIMAL(18,2) | Total supplier invoice amount |
| `amount_paid` | DECIMAL(18,2) | |
| `amount_remaining` | DECIMAL(18,2) | |
| `payment_status` | VARCHAR | `Unpaid`, `Partially Paid`, `Paid` |
| `expense_category` | VARCHAR | Mapped from GL account: `Network Capex`, `Opex`, `IT`, `Labour` |
| `run_date` | DATE | |
| `_ebs_last_update_date` | TIMESTAMP_NTZ | |
| `_loaded_at` | TIMESTAMP_NTZ | |

### 4.4 `FINANCE.CAPEX_PROJECT_ACTUALS`

Project expenditure lines from Oracle Projects — replaces the manually maintained S3 file
currently read by `nnn_capex_project_tracking_weekly`.

| Column | Type | Description |
|---|---|---|
| `expenditure_id` | NUMBER PK | Oracle PA_EXPENDITURES_ALL.EXPENDITURE_ID |
| `project_id` | NUMBER FK | Oracle PA_PROJECTS_ALL.PROJECT_ID |
| `project_number` | VARCHAR | Human-readable project code |
| `project_name` | VARCHAR | |
| `project_status` | VARCHAR | `Active`, `Closed`, `On Hold` |
| `task_id` | NUMBER | Oracle project task ID |
| `task_name` | VARCHAR | |
| `expenditure_type` | VARCHAR | `Equipment`, `Labour`, `Contractor`, `Civil Works` |
| `expenditure_date` | DATE | Transaction date |
| `amount_aud` | DECIMAL(18,2) | Committed or actual cost in AUD |
| `transaction_type` | VARCHAR | `Actual`, `Committed`, `Forecast` |
| `gl_account_code` | VARCHAR | Linked GL account for posted actuals |
| `run_date` | DATE | |
| `_ebs_last_update_date` | TIMESTAMP_NTZ | |
| `_loaded_at` | TIMESTAMP_NTZ | |

### 4.5 `FINANCE.GL_PERIOD_STATUS`

Accounting period open/close status — used by Finance to guard against loading
data from an open, unlocked period.

| Column | Type | Description |
|---|---|---|
| `ledger_id` | NUMBER | |
| `period_name` | VARCHAR | e.g. `JAN-25` |
| `period_year` | NUMBER | |
| `period_num` | NUMBER | 1–12 |
| `start_date` | DATE | |
| `end_date` | DATE | |
| `closing_status` | VARCHAR | `O` = Open, `C` = Closed, `P` = Permanently Closed |
| `run_date` | DATE | |
| `_loaded_at` | TIMESTAMP_NTZ | |

---

## 5. Data Flow

```
Oracle EBS Read Replica (Oracle DB 19c)
    │
    ▼  [Extract — JDBC SELECT with date-range filter on LAST_UPDATE_DATE]
S3 Data Lake
nnn-data-lake-prod/oracle_ebs/<module>/run_date=YYYY-MM-DD/data.parquet
    │
    ▼  [Transform — decode segments, map account codes, compute derived fields]
Snowflake STAGING (temp tables, dropped after each run)
    │
    ▼  [MERGE on primary key — upsert changed/new records only]
Snowflake FINANCE Schema
    ├── GL_JOURNAL_LINES
    ├── AR_INVOICES
    ├── AP_INVOICES
    ├── CAPEX_PROJECT_ACTUALS
    └── GL_PERIOD_STATUS
    │
    ▼  [Downstream consumers]
    ├── nnn_capex_project_tracking_weekly (reads CAPEX_PROJECT_ACTUALS instead of S3 file)
    ├── nnn_revenue_recognition_monthly (reconciliation joins GL_JOURNAL_LINES)
    └── FINANCE.V_REVENUE_GL_RECONCILIATION (new view — see Section 7)
```

---

## 6. Load Strategy

| Table | Strategy | Filter Column |
|---|---|---|
| `GL_JOURNAL_LINES` | Incremental — `LAST_UPDATE_DATE > last_watermark` | `GL_JE_LINES.LAST_UPDATE_DATE` |
| `AR_INVOICES` | Incremental | `RA_CUSTOMER_TRX_ALL.LAST_UPDATE_DATE` |
| `AP_INVOICES` | Incremental | `AP_INVOICES_ALL.LAST_UPDATE_DATE` |
| `CAPEX_PROJECT_ACTUALS` | Incremental | `PA_EXPENDITURES_ALL.LAST_UPDATE_DATE` |
| `GL_PERIOD_STATUS` | Full reload | Small lookup table; ~36 rows (3 years of periods) |

**Watermark storage**: `OPERATIONS.PIPELINE_RUN_LOG` with a dedicated row per module
(e.g. `dag_id = 'nnn_oracle_ebs_financial_daily'`, `module = 'GL'`).

**GL period locking guard**: Before extracting `GL_JOURNAL_LINES`, the pipeline reads
`GL_PERIOD_STATUS` and checks whether any period with data in the extract window has
`closing_status = 'O'` (open). If so, the pipeline runs but appends a `_period_open_flag`
warning. Finance is notified via Slack that data for the open period may be revised.

---

## 7. Transformation Rules

| Rule ID | Target Table | Rule |
|---|---|---|
| T-01 | `GL_JOURNAL_LINES` | Decode `GL_CODE_COMBINATIONS` segments: Segment 1 = entity, Segment 2 = cost centre, Segment 3 = account (CAPEX range `1500000–1599999`; OPEX range `6000000–6999999`), Segment 4 = project |
| T-02 | `GL_JOURNAL_LINES` | `je_category` mapped from Oracle category name to NNN taxonomy using the GL Account Code Taxonomy file supplied by Finance (e.g. `'Purchase Invoices'` → `'Accounts Payable'`, `'Project Accounting'` → `'CAPEX'`). The taxonomy file is stored at `OPERATIONS.GL_CATEGORY_TAXONOMY` in Snowflake |
| T-03 | `GL_JOURNAL_LINES` | Only posted journals (`JE_LINES.STATUS = 'P'`) are loaded; draft entries are excluded |
| T-04 | `AR_INVOICES` | `payment_status` derived: `'Paid'` if `AMOUNT_DUE_REMAINING = 0`; `'Overdue'` if `DUE_DATE < SYSDATE AND AMOUNT_DUE_REMAINING > 0`; else `'Open'` |
| T-05 | `AR_INVOICES` | `invoice_type` mapped from Oracle transaction type name to NNN billing category taxonomy |
| T-06 | `AP_INVOICES` | `expense_category` derived from the GL account code linked to the invoice distribution |
| T-07 | `CAPEX_PROJECT_ACTUALS` | `transaction_type` = `'Actual'` if expenditure is posted to GL; `'Committed'` if PO-backed; `'Forecast'` if project plan item |
| T-08 | All | All timestamps converted from Oracle DB timezone (UTC) to AEST before storage |
| T-09 | All | Records with a negative amount that represent reversals are retained and flagged with `is_reversal = TRUE` |

---

## 8. New View: `FINANCE.V_REVENUE_GL_RECONCILIATION`

A Snowflake view joining `FINANCE.AR_INVOICES` against `FINANCE.GL_JOURNAL_LINES`
to enable Finance to detect variances between the AR sub-ledger and the GL posting.

| Column | Description |
|---|---|
| `period_name` | Accounting period |
| `invoice_type` | Revenue category |
| `ar_total_invoiced` | Sum of `AR_INVOICES.invoice_amount` for the period |
| `gl_total_posted` | Sum of `GL_JOURNAL_LINES.accounted_cr` for revenue accounts |
| `variance_aud` | `ar_total_invoiced - gl_total_posted` |
| `variance_pct` | Percentage variance |
| `reconciled_flag` | TRUE if `abs(variance_pct) < 0.01` (within 1%) |

---

## 9. Schedule & SLA

| Attribute | Value |
|---|---|
| **Schedule** | Daily at 02:00 AEST (16:00 UTC) |
| **SLA** | Must complete within 3 hours (before 05:00 AEST) |
| **Retries** | 3 attempts, 15-minute delay |
| **SLA miss behaviour** | `nnn_sla_miss_alert` — PagerDuty `critical` (P1) for Finance data |
| **Month-end special run** | On the last calendar day of each month, a second manual trigger is expected after Oracle month-end close (5th business day); documented in runbook |

---

## 10. Non-Functional Requirements

| Requirement | Detail |
|---|---|
| **Volume** | GL extract: ~10,000–30,000 new/modified lines per day; AR/AP: ~500–2,000 records |
| **Data sensitivity** | All financial data classified as Confidential; access restricted to `FINANCE_READER` Snowflake role |
| **Auditability** | Every loaded row carries the source Oracle `LAST_UPDATE_DATE` and pipeline `_loaded_at` |
| **No write to Oracle** | The EBS read replica is read-only; the ETL must never attempt DML against Oracle |
| **Network path** | Oracle DB is on-prem; Airflow workers connect via site-to-site VPN. VPN stability is a dependency |
| **Re-runability** | Re-triggering the same run date re-extracts based on the same `LAST_UPDATE_DATE` window and MERGEs without creating duplicates |

---

## 11. New Airflow DAG

| Attribute | Value |
|---|---|
| **DAG ID** | `nnn_oracle_ebs_financial_daily` |
| **File** | `dags/integrations/nnn_oracle_ebs_financial_daily.py` |
| **Schedule** | `0 16 * * *` (02:00 AEST) |
| **Tags** | `nnn`, `oracle`, `finance`, `daily` |

**Task sequence:**

```
check_oracle_connectivity
    │
    ▼
extract_gl_periods          [refresh lookup — always runs]
    │
    ▼
check_period_lock_status    [warn if any period in window is open]
    │
    ▼
extract_gl_lines ──────────────────────────────────┐
extract_ar_invoices ───────────────────────────────┤  [parallel]
extract_ap_invoices ───────────────────────────────┤
extract_capex_actuals ─────────────────────────────┘
    │
    ▼
transform_and_stage
    │
    ▼
merge_gl_lines ─────────────────────────────────────┐
merge_ar_invoices ──────────────────────────────────┤  [parallel]
merge_ap_invoices ──────────────────────────────────┤
merge_capex_actuals ────────────────────────────────┘
    │
    ▼
validate_load
    │
    ▼
update_pipeline_log
```

---

## 12. Acceptance Criteria

| # | Criterion |
|---|---|
| AC-01 | All 4 target tables created in Snowflake `FINANCE` schema with defined columns and types |
| AC-02 | GL lines with `STATUS != 'P'` (not posted) are excluded from `GL_JOURNAL_LINES` |
| AC-03 | Re-running the DAG for the same date produces identical row counts (MERGE is idempotent) |
| AC-04 | `CAPEX_PROJECT_ACTUALS` contains the same project actuals as the currently maintained S3 manual file (verified by Finance on a sample period) |
| AC-05 | `V_REVENUE_GL_RECONCILIATION` variance is < 1% for the most recent closed period |
| AC-06 | Oracle connection failure causes the pipeline to retry 3 times and alert PagerDuty at P1 |
| AC-07 | `GL_PERIOD_STATUS` is always loaded, even if incremental extracts fail (separate task, no dependency) |
| AC-08 | `nnn_capex_project_tracking_weekly` reads from `FINANCE.CAPEX_PROJECT_ACTUALS` and produces the same weekly report as the old S3-based version |
| AC-09 | Access to `FINANCE` schema tables is restricted to the `FINANCE_READER` role; DE team confirms other roles return access denied |
| AC-10 | All timestamps in target tables are in AEST, not UTC |

---

## 13. Out of Scope

- Oracle OSS (Order & Service Suite) — that system is covered by a separate initiative.
- Oracle EBS modules not listed: Inventory, HR, Purchasing (PR/PO) below the AP invoice level.
- Real-time sub-minute financial event streaming.
- Write-back or any DML against Oracle EBS.
- Multi-currency restatement — all amounts stored in original transaction currency plus AUD equivalent.

---

## 14. Dependencies & Prerequisites

- Oracle EBS read replica accessible from Airflow worker network via site-to-site VPN (confirmed). Replica lag is confirmed at < 5 minutes; month-end data is visible on the replica within 1 business day of being posted to the primary — this is sufficient for the daily ETL schedule.
- `nnn_oracle_ebs_prod` Airflow connection configured with JDBC URL, username `NNN_ETL_READER`, and password. `NNN_ETL_READER` has confirmed SELECT grants on `GL`, `AR`, `AP`, and `PA` schemas. `FA` schema access is confirmed via the same user.
- `FINANCE` schema exists in Snowflake (confirmed — used by `nnn_revenue_recognition_monthly`).
- `OPERATIONS.GL_CATEGORY_TAXONOMY` lookup table must be created and populated from the Finance-supplied GL account code taxonomy file before first run (required for T-02).
- Historical backfill covers data from `2022-01-01` onwards — approximately 3 fiscal years, estimated at 500,000 GL lines. Data prior to 2022 is not required by Finance Analytics.
- `nnn_capex_project_tracking_weekly` DAG updated to read from Snowflake after go-live; old S3-file path deprecated.
