# Feature 03 — ServiceNow ITSM to Snowflake ETL

| Field | Value |
|---|---|
| **Status** | Ready for Development |
| **Priority** | P2 — Medium-High |
| **Owner** | NNN Data Engineering |
| **Stakeholders** | Network Operations, Field Ops, SLA Compliance Team, ACCC Reporting |
| **Target Domain** | `COMPLIANCE` and `FIELD_OPS` schemas in Snowflake |
| **Estimated Effort** | 3 sprints (3 weeks) |

---

## 1. Executive Summary

NNN uses ServiceNow as its IT Service Management (ITSM) platform for tracking network
incidents, change requests, problems, field technician work orders, and SLA commitments.
This data is currently siloed in ServiceNow and only accessible through manually scheduled
ServiceNow reports. This feature builds a daily ETL pipeline to extract core ITSM records
and load them into Snowflake, enabling automated ACCC SLA reporting, incident trend
analysis, and field operations performance measurement.

---

## 2. Business Context & Motivation

### Problems Being Solved

- **ACCC SLA reporting is manual**: `nnn_accc_reporting_weekly` currently reads from a
  manually assembled CSV produced by the Network Operations team. ServiceNow holds the
  ground truth on fault rectification times and SLA breaches — but extracting it requires
  a ServiceNow admin to run a report each week.
- **Incident data gap in network analytics**: `nnn_outage_incident_etl` ingests raw alarm
  events from the NMS API but does not have ServiceNow incident records, which include
  root cause classification, customer impact scope, and resolution notes — all required
  for ACCC Part 8 reporting.
- **No field ops pipeline view**: `nnn_technician_scheduling_daily` manages dispatch
  scheduling but has no feedback loop from ServiceNow Work Orders (actual completion,
  time on site, re-work rate). Field ops KPIs are calculated manually in spreadsheets.
- **Duplicate problem records**: Without a Snowflake-visible incident-to-problem link,
  the Network team cannot detect when the same underlying network fault is generating
  multiple customer incidents.

### Expected Outcomes

- `nnn_accc_reporting_weekly` reads from Snowflake `COMPLIANCE.SERVICENOW_INCIDENTS`
  instead of a manual CSV — eliminating 3–4 hours of manual work per week.
- Incident root cause and customer impact data available for network capacity planning.
- Field ops team can self-serve work order completion metrics from Redshift.
- Automated detection of incident-to-problem clusters in Snowflake views.

---

## 3. Source System Overview

| Attribute | Detail |
|---|---|
| **System** | ServiceNow (ITSM — Network Operations instance) |
| **Access method** | ServiceNow Table API (REST JSON) |
| **Authentication** | OAuth 2.0 — client credentials (service account) |
| **Airflow connection** | `nnn_servicenow_prod` (type: HTTP; base URL: `https://nnnco.service-now.com`) |
| **API version** | ServiceNow Tokyo release; Table API v1 |
| **Rate limits** | 25,000 API calls per 24 hours; bulk export via `sysparm_limit=1000` pagination |
| **Integration user** | `nnn_etl_integration` — read-only role, all ITSM modules |

### ServiceNow Tables in Scope

| ServiceNow Table | API Name | Description | Approx. Record Count |
|---|---|---|---|
| Incident | `incident` | Network and customer incidents | ~200,000 total; ~150/day |
| Problem | `problem` | Root cause records linked to incidents | ~8,000 total |
| Change Request | `change_request` | Planned network changes | ~12,000 total |
| Work Order | `wm_order` | Field technician dispatch orders | ~50,000 total |
| Work Order Task | `wm_task` | Individual tasks within a work order | ~180,000 total |
| Configuration Item | `cmdb_ci_network_adapter` | Network device CI records (reference) | ~5,000 |
| SLA Definition | `contract_sla` | SLA commitment definitions | ~80 records (lookup) |
| Task SLA | `task_sla` | Per-incident SLA tracking records | ~400,000 total |

---

## 4. Target Schema Design

Incidents and compliance data land in `COMPLIANCE` schema; field ops data lands in
`FIELD_OPS` schema. All tables carry `_snow_sys_id` (ServiceNow internal record ID),
`run_date`, and `_loaded_at`.

### 4.1 `COMPLIANCE.SERVICENOW_INCIDENTS`

The primary table for ACCC fault reporting and incident trend analysis.

| Column | Type | Description |
|---|---|---|
| `snow_sys_id` | VARCHAR(32) PK | ServiceNow sys_id |
| `incident_number` | VARCHAR | Human-readable `INC0123456` |
| `category` | VARCHAR | `Network`, `Customer Equipment`, `Provisioning`, `Billing` |
| `subcategory` | VARCHAR | e.g. `PON Fault`, `CVC Congestion`, `Layer 2` |
| `priority` | INTEGER | 1 (Critical) to 5 (Planning) |
| `urgency` | INTEGER | 1–3 |
| `impact` | INTEGER | 1–3 |
| `state` | VARCHAR | `New`, `In Progress`, `Resolved`, `Closed`, `Cancelled` |
| `opened_at` | TIMESTAMP_NTZ | Incident creation time (AEST) |
| `resolved_at` | TIMESTAMP_NTZ | Resolution time (NULL if open) |
| `closed_at` | TIMESTAMP_NTZ | Close time |
| `resolution_minutes` | INTEGER | Elapsed minutes from `opened_at` to `resolved_at` |
| `assigned_group` | VARCHAR | Resolver group name |
| `assignee_name` | VARCHAR | Individual resolver |
| `affected_ci` | VARCHAR | Configuration item (network device or service) |
| `affected_poi` | VARCHAR | Point of Interconnect ID (NNN-specific field) |
| `customer_impact_count` | INTEGER | Number of services impacted |
| `root_cause_category` | VARCHAR | Post-resolution: `Hardware`, `Software`, `Human Error`, `External` |
| `problem_id` | VARCHAR(32) | FK to `COMPLIANCE.SERVICENOW_PROBLEMS.snow_sys_id` (NULL if no linked problem) |
| `accc_reportable_flag` | BOOLEAN | TRUE if fault meets ACCC Part 8 reporting criteria |
| `sla_breached_flag` | BOOLEAN | TRUE if any SLA record for this incident is breached |
| `run_date` | DATE | |
| `_snow_last_updated` | TIMESTAMP_NTZ | ServiceNow `sys_updated_on` — watermark column |
| `_loaded_at` | TIMESTAMP_NTZ | |

### 4.2 `COMPLIANCE.SERVICENOW_TASK_SLAS`

Per-incident SLA measurement records — the source for ACCC SLA breach reporting.

| Column | Type | Description |
|---|---|---|
| `task_sla_id` | VARCHAR(32) PK | ServiceNow task_sla sys_id |
| `incident_sys_id` | VARCHAR(32) FK | Parent incident |
| `sla_definition_name` | VARCHAR | e.g. `'NBN Fault Rectification - Priority 1'` |
| `stage` | VARCHAR | `In Progress`, `Completed`, `Breached` |
| `has_breached` | BOOLEAN | |
| `business_duration_minutes` | INTEGER | Elapsed business-hours minutes |
| `actual_elapsed_minutes` | INTEGER | Calendar elapsed minutes |
| `breach_time` | TIMESTAMP_NTZ | When SLA was or would be breached |
| `pause_duration_minutes` | INTEGER | Time SLA was paused (e.g. awaiting customer) |
| `run_date` | DATE | |
| `_loaded_at` | TIMESTAMP_NTZ | |

### 4.3 `COMPLIANCE.SERVICENOW_PROBLEMS`

Root cause analysis records linked to one or more incidents.

| Column | Type | Description |
|---|---|---|
| `snow_sys_id` | VARCHAR(32) PK | |
| `problem_number` | VARCHAR | `PRB0012345` |
| `state` | VARCHAR | `Open`, `Root Cause Analysis`, `Fix in Progress`, `Resolved`, `Closed` |
| `priority` | INTEGER | |
| `category` | VARCHAR | |
| `known_error_flag` | BOOLEAN | TRUE if promoted to Known Error Database |
| `root_cause` | VARCHAR | Free-text root cause description |
| `workaround` | VARCHAR | Active workaround description |
| `opened_at` | TIMESTAMP_NTZ | |
| `resolved_at` | TIMESTAMP_NTZ | |
| `linked_incident_count` | INTEGER | Number of incidents linked to this problem |
| `run_date` | DATE | |
| `_snow_last_updated` | TIMESTAMP_NTZ | |
| `_loaded_at` | TIMESTAMP_NTZ | |

### 4.4 `FIELD_OPS.SERVICENOW_WORK_ORDERS`

Technician dispatch orders — the operational mirror of `FIELD_OPS.TECHNICIAN_DISPATCH`.

| Column | Type | Description |
|---|---|---|
| `work_order_id` | VARCHAR(32) PK | ServiceNow wm_order sys_id |
| `work_order_number` | VARCHAR | `WO0098765` |
| `incident_sys_id` | VARCHAR(32) FK | Triggering incident (NULL for planned work) |
| `change_sys_id` | VARCHAR(32) FK | Triggering change request (NULL for reactive) |
| `state` | VARCHAR | `Draft`, `Pending Dispatch`, `Dispatched`, `Work In Progress`, `Closed Complete`, `Closed Incomplete` |
| `work_type` | VARCHAR | `Corrective`, `Preventive`, `Installation`, `Audit` |
| `assigned_technician` | VARCHAR | Technician employee ID |
| `assigned_team` | VARCHAR | Field team name |
| `location_fsa` | VARCHAR | Field Service Area code |
| `location_address` | VARCHAR | Site address |
| `scheduled_start` | TIMESTAMP_NTZ | Planned start time |
| `scheduled_end` | TIMESTAMP_NTZ | Planned end time |
| `actual_start` | TIMESTAMP_NTZ | Actual arrival on site |
| `actual_end` | TIMESTAMP_NTZ | Actual completion |
| `time_on_site_minutes` | INTEGER | `actual_end - actual_start` in minutes |
| `rework_flag` | BOOLEAN | TRUE if a follow-up work order exists within 30 days for same location |
| `closure_code` | VARCHAR | `Resolved`, `Escalated`, `No Access`, `Parts Required` |
| `run_date` | DATE | |
| `_snow_last_updated` | TIMESTAMP_NTZ | |
| `_loaded_at` | TIMESTAMP_NTZ | |

---

## 5. Data Flow

```
ServiceNow Table API (REST JSON — paginated SOQL-style queries)
    │
    ▼  [Extract — incremental by sys_updated_on > last_watermark]
S3 Data Lake
nnn-data-lake-prod/servicenow/<table>/run_date=YYYY-MM-DD/data.parquet
    │
    ▼  [Transform — denormalize reference fields, compute derived metrics, classify ACCC flags]
Snowflake STAGING (temp tables)
    │
    ▼  [MERGE on snow_sys_id]
Snowflake
    ├── COMPLIANCE.SERVICENOW_INCIDENTS
    ├── COMPLIANCE.SERVICENOW_TASK_SLAS
    ├── COMPLIANCE.SERVICENOW_PROBLEMS
    └── FIELD_OPS.SERVICENOW_WORK_ORDERS
    │
    ▼  [Downstream consumers]
    ├── nnn_accc_reporting_weekly (reads COMPLIANCE.SERVICENOW_INCIDENTS + TASK_SLAS)
    ├── nnn_sla_compliance_daily (reads SERVICENOW_TASK_SLAS for SLA breach monitoring)
    └── nnn_fsa_completion_reporting (reads FIELD_OPS.SERVICENOW_WORK_ORDERS)
```

---

## 6. Load Strategy

| Table | Strategy | Watermark Column |
|---|---|---|
| `COMPLIANCE.SERVICENOW_INCIDENTS` | Incremental | `incident.sys_updated_on` |
| `COMPLIANCE.SERVICENOW_TASK_SLAS` | Incremental | `task_sla.sys_updated_on` |
| `COMPLIANCE.SERVICENOW_PROBLEMS` | Incremental | `problem.sys_updated_on` |
| `FIELD_OPS.SERVICENOW_WORK_ORDERS` | Incremental | `wm_order.sys_updated_on` |
| `contract_sla` (SLA definitions) | Full reload | Static lookup; ~80 rows |

**Watermark**: Last successful `sys_updated_on` value per table stored in
`OPERATIONS.PIPELINE_RUN_LOG`. On first run, defaults to 365 days ago to backfill one
year of ITSM history (a reasonable ACCC reporting lookback).

**ACCC flag computation**: `accc_reportable_flag` on incidents is re-evaluated on every
incremental load because the classification rules (priority, category, customer impact
count) can be retroactively adjusted by NOC operators in ServiceNow for up to 48 hours
after incident closure.

---

## 7. Transformation Rules

| Rule ID | Target | Rule |
|---|---|---|
| T-01 | `SERVICENOW_INCIDENTS` | `resolution_minutes` = NULL for open incidents; computed as `DATEDIFF('minute', opened_at, resolved_at)` for resolved |
| T-02 | `SERVICENOW_INCIDENTS` | `accc_reportable_flag` = TRUE when `priority IN (1,2)` AND `category = 'Network'` AND `customer_impact_count >= 1`. `customer_impact_count` is defined as the number of end-user NBN services impacted (not RSP accounts) — sourced from ServiceNow field `u_affected_service_count`. Cancelled incidents always have `accc_reportable_flag = FALSE` regardless of other fields |
| T-03 | `SERVICENOW_INCIDENTS` | `affected_poi` extracted from ServiceNow custom field `u_affected_poi`; NULL if not set |
| T-04 | `SERVICENOW_INCIDENTS` | `sla_breached_flag` derived by joining to `SERVICENOW_TASK_SLAS.has_breached` — TRUE if any linked task SLA is breached |
| T-05 | `SERVICENOW_TASK_SLAS` | `pause_duration_minutes` subtracted from `actual_elapsed_minutes` to produce net elapsed time excluding hold periods. Business hours for SLA calculations are defined as AEST Monday–Friday 08:00–18:00, excluding Australian public holidays. This matches the definition used in NNN's current ACCC SLA compliance reports |
| T-06 | `FIELD_OPS.SERVICENOW_WORK_ORDERS` | `rework_flag` computed in Snowflake post-load via a follow-up SQL step that looks for any other work order at the same `location_address` within 30 days |
| T-07 | `FIELD_OPS.SERVICENOW_WORK_ORDERS` | `time_on_site_minutes` = NULL if `actual_start` or `actual_end` is NULL (work order not yet completed) |
| T-08 | All | ServiceNow stores all timestamps in UTC; convert to AEST before loading |
| T-09 | `SERVICENOW_INCIDENTS` | Reference field `assigned_group.name` (ServiceNow dot-walking) denormalised into `assigned_group` column to avoid a separate lookup join |

---

## 8. New Views

### `COMPLIANCE.V_ACCC_SLA_BREACH_SUMMARY`

Weekly aggregation of SLA breaches by priority and category — input to the ACCC Part 8
automated report. Replaces the manual ServiceNow export used by `nnn_accc_reporting_weekly`.

| Column | Description |
|---|---|
| `report_week` | ISO week starting Monday |
| `incident_priority` | 1–5 |
| `incident_category` | Network, Provisioning, etc. |
| `total_incidents` | Count of ACCC-reportable incidents |
| `sla_breached_count` | Count where `sla_breached_flag = TRUE` |
| `sla_compliance_rate_pct` | `(1 - sla_breached_count / total_incidents) * 100` |
| `avg_resolution_minutes` | Mean resolution time |
| `p95_resolution_minutes` | 95th percentile resolution time |

### `FIELD_OPS.V_TECHNICIAN_PERFORMANCE`

Daily field ops KPIs per technician and FSA — feeds `nnn_fsa_completion_reporting`.

| Column | Description |
|---|---|
| `run_date` | |
| `assigned_team` | |
| `location_fsa` | |
| `completed_orders` | Work orders closed with `Resolved` |
| `incomplete_orders` | Closed with `Escalated`, `No Access`, `Parts Required` |
| `avg_time_on_site_minutes` | |
| `rework_rate_pct` | `rework_flag = TRUE` / total completed |

---

## 9. Schedule & SLA

| Attribute | Value |
|---|---|
| **Schedule** | Daily at 03:00 AEST (17:00 UTC) |
| **SLA** | Must complete within 2 hours (before 05:00 AEST) |
| **Retries** | 3 attempts, 10-minute delay |
| **SLA miss behaviour** | `nnn_sla_miss_alert` — PagerDuty `critical` (P1) given ACCC reporting dependency |
| **ACCC reporting dependency** | `nnn_accc_reporting_weekly` must not run until this DAG succeeds for the week |

---

## 10. Non-Functional Requirements

| Requirement | Detail |
|---|---|
| **Volume** | ~150 new/modified incidents per day; ~100 work order updates per day |
| **API call budget** | One full day's extract uses approximately 400–600 API calls (well within 25,000 daily limit) |
| **Data latency** | Data reflects ServiceNow state as of midnight AEST |
| **PII handling** | `assignee_name` and `location_address` are operational data — no masking required in staging; masked in development |
| **Idempotency** | MERGE on `snow_sys_id` — safe to re-run |
| **Historical backfill** | First run backfills 365 days automatically; further historical backfill requires manual trigger with custom date range parameter |

---

## 11. New Airflow DAG

| Attribute | Value |
|---|---|
| **DAG ID** | `nnn_servicenow_itsm_daily` |
| **File** | `dags/integrations/nnn_servicenow_itsm_daily.py` |
| **Schedule** | `0 17 * * *` (03:00 AEST) |
| **Tags** | `nnn`, `servicenow`, `compliance`, `field_ops`, `daily` |

**Task sequence:**

```
check_servicenow_api
    │
    ▼
extract_incidents ──────────────────────────────────┐
extract_task_slas ──────────────────────────────────┤  [parallel]
extract_problems ───────────────────────────────────┤
extract_work_orders ────────────────────────────────┘
    │
    ▼
transform_and_stage
    │
    ▼
merge_incidents ────────────────────────────────────┐
merge_task_slas ────────────────────────────────────┤  [parallel]
merge_problems ─────────────────────────────────────┤
merge_work_orders ──────────────────────────────────┘
    │
    ▼
compute_rework_flags        [post-load SQL — derives rework_flag across work orders]
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
| AC-01 | All 4 target tables created with the defined columns and populated after the first run |
| AC-02 | `accc_reportable_flag` correctly identifies P1/P2 network incidents with customer impact (validated against a sample of manually classified incidents provided by the NOC team) |
| AC-03 | `nnn_accc_reporting_weekly` produces the same SLA breach counts when reading from Snowflake vs the current manual ServiceNow report (within 0.5% tolerance) |
| AC-04 | Re-running the pipeline for the same date does not create duplicate rows |
| AC-05 | Incidents updated in ServiceNow after the last pipeline run appear in Snowflake the following day |
| AC-06 | `rework_flag` is correctly set to TRUE for work orders where a follow-up order exists at the same address within 30 days |
| AC-07 | `V_ACCC_SLA_BREACH_SUMMARY` produces values matching the manually maintained ACCC spreadsheet for the last 4 completed weeks |
| AC-08 | A ServiceNow API 429 (rate limit) response causes the pipeline to wait and retry, not fail immediately |
| AC-09 | All timestamps in target tables are stored in AEST |
| AC-10 | Historical backfill of 365 days completes within 4 hours on first run |

---

## 13. Out of Scope

- ServiceNow modules not listed: HR Service Delivery, IT Asset Management (ITAM), Knowledge Base articles.
- Change Request task lines (sub-tasks within a change) — parent change request record only.
- Real-time incident streaming from ServiceNow event subscriptions.
- Bi-directional sync (writing back to ServiceNow from Snowflake).
- ServiceNow Dev/Test instance — production instance only.

---

## 14. Dependencies & Prerequisites

- `nnn_servicenow_prod` Airflow connection configured with OAuth client ID and secret.
- ServiceNow integration user `nnn_etl_integration` provisioned with read access to all in-scope tables. No additional custom `u_` fields beyond `u_affected_poi` and `u_affected_service_count` are required for ACCC Part 8 compliance — confirmed with the ACCC Compliance Lead.
- `COMPLIANCE` and `FIELD_OPS` schemas already exist in Snowflake (confirmed — used by existing DAGs).
- NOC team to provide a sample of 50 manually classified ACCC-reportable incidents for AC-02 and AC-07 validation before go-live.
- `nnn_accc_reporting_weekly` DAG updated to read from Snowflake after go-live.
