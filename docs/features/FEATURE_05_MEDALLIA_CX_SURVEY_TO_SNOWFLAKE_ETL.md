# Feature 05 — Medallia CX Survey Platform to Snowflake ETL

| Field | Value |
|---|---|
| **Status** | Ready for Development |
| **Priority** | P2 — Medium |
| **Owner** | NNN Data Engineering |
| **Stakeholders** | Customer Experience Team, Product Management, ML / Data Science |
| **Target Domain** | `CUSTOMER` schema in Snowflake |
| **Estimated Effort** | 2 sprints (2 weeks) |

---

## 1. Executive Summary

NNN uses Medallia as its enterprise Customer Experience (CX) platform to collect and
analyse customer feedback across multiple touchpoints — post-install surveys,
post-fault surveys, annual NPS programmes, and RSP satisfaction surveys. Currently
the only CX data in Snowflake comes from `nnn_nps_survey_etl_weekly`, which loads a
simplified weekly NPS aggregate from a manual Medallia export. This feature replaces
that manual process with a fully automated daily ETL that extracts granular survey
response data, topic analysis, and CX programme metadata from the Medallia API and
loads it into Snowflake — enabling ML churn modelling, RSP segmentation, and
real-time CX alerting.

---

## 2. Business Context & Motivation

### Problems Being Solved

- **Manual NPS export**: `nnn_nps_survey_etl_weekly` depends on a Medallia report
  exported manually by the CX team every Monday. This creates a 3–7 day lag in NPS
  data reaching Snowflake and fails when the CX analyst is unavailable.
- **No response-level granularity**: The current pipeline loads only an aggregated
  weekly NPS score per RSP. Individual survey responses, verbatim comments, topic
  sentiment scores, and programme-level segmentation are lost — making it impossible
  to train an ML model on individual CX signals.
- **Churn model signal gap**: `nnn_customer_experience_features` lacks the verbatim
  feedback topic scores (e.g. `'Installation Experience'`, `'Fault Resolution'`) that
  Medallia's NLU engine already computes. These are strong predictors of churn.
- **No post-fault survey link**: NNN runs a post-fault survey programme in Medallia
  but its data never enters Snowflake, meaning the CX team cannot determine whether
  fault resolution (from ServiceNow) correlates with survey sentiment (from Medallia).
- **RSP satisfaction blind spot**: RSP-level satisfaction surveys are collected in
  Medallia but only reviewed inside Medallia's own dashboard — not joined to wholesale
  revenue or service quality data in Snowflake.

### Expected Outcomes

- Daily automated ingestion of all Medallia survey responses — no manual exports.
- Individual response records with topic sentiment scores available in Snowflake for
  ML feature engineering.
- NPS trends available at daily granularity (vs weekly today).
- Post-fault survey scores linkable to ServiceNow incident IDs.
- RSP satisfaction scores joinable to `CUSTOMER.SF_ACCOUNTS.nnn_rsp_code`.

---

## 3. Source System Overview

| Attribute | Detail |
|---|---|
| **System** | Medallia Experience Cloud (SaaS) |
| **Access method** | Medallia Reporting API v1 (REST JSON) |
| **Authentication** | OAuth 2.0 — client credentials (service account) |
| **Airflow connection** | `nnn_medallia_prod` (type: HTTP; host: `https://nnnco.medallia.com.au`) |
| **API endpoints used** | `/data/export` (bulk response export), `/programs` (programme metadata), `/topics` (topic/sentiment data) |
| **Rate limits** | Medallia enforces per-minute rate limits; bulk export jobs are asynchronous (poll for completion) |
| **Data residency** | Medallia SaaS — data is hosted in Australia (Sydney region); confirmed in DPA |

### Medallia Programmes in Scope

| Programme Name | Trigger | Frequency | Approx. Responses/Month |
|---|---|---|---|
| Post-Installation NPS | After NBN connection activated | Continuous | ~3,000 |
| Post-Fault Resolution | After ServiceNow incident closed | Continuous | ~800 |
| Annual RSP Partner Satisfaction | Scheduled annual survey | Annual (April) | ~200 |
| Quarterly Customer Pulse | Random sample of active customers | Quarterly | ~5,000 |
| RSP Portal Usability | After portal session > 10 min | Continuous | ~400 |

---

## 4. Target Schema Design

All tables land in the `CUSTOMER` schema. `respondent_id` is a Medallia-assigned
pseudonymous identifier — NNN does not store customer names or email addresses in
Snowflake. All verbatim text is stored as-is from Medallia (NLU processing occurs
upstream in Medallia, not in the pipeline).

### 4.1 `CUSTOMER.MEDALLIA_SURVEY_RESPONSES`

One row per individual survey response. The primary fact table.

| Column | Type | Description |
|---|---|---|
| `response_id` | VARCHAR(36) PK | Medallia response UUID |
| `programme_id` | VARCHAR | Medallia programme identifier |
| `programme_name` | VARCHAR | Human-readable programme name |
| `respondent_id` | VARCHAR(36) | Pseudonymous Medallia respondent ID (not customer name) |
| `rsp_code` | VARCHAR | NNN RSP code — extracted from Medallia custom field `nnn_rsp_code`; join key to `CUSTOMER.SF_ACCOUNTS` |
| `service_id` | VARCHAR | NNN service ID linked to the survey (NULL for RSP-level surveys) |
| `servicenow_incident_id` | VARCHAR(32) | ServiceNow sys_id for post-fault surveys (NULL for other programmes) |
| `survey_channel` | VARCHAR | Delivery channel: `Email`, `SMS`, `Web Intercept`, `IVR` |
| `survey_language` | VARCHAR | Response language: `en`, `zh`, `vi`, `ar` |
| `response_date` | DATE | Date survey was submitted |
| `response_timestamp` | TIMESTAMP_NTZ | Full timestamp of submission (AEST) |
| `survey_status` | VARCHAR | `Complete`, `Partial`, `Bounced` |
| `nps_score` | INTEGER | Net Promoter Score (0–10); NULL if not an NPS programme |
| `nps_category` | VARCHAR | `Promoter` (9–10), `Passive` (7–8), `Detractor` (0–6); NULL if no NPS question |
| `overall_satisfaction_score` | DECIMAL(4,2) | Overall CSAT (1–5 or 1–10 depending on programme); normalised to 0–100 |
| `effort_score` | DECIMAL(4,2) | Customer Effort Score if asked; NULL otherwise |
| `verbatim_text` | VARCHAR | Open-text comment (may be NULL; in English or translated if multilingual) |
| `verbatim_language_detected` | VARCHAR | Language detected by Medallia NLU |
| `run_date` | DATE | Pipeline execution date |
| `_medallia_updated_at` | TIMESTAMP_NTZ | Medallia `lastUpdated` field — watermark column |
| `_loaded_at` | TIMESTAMP_NTZ | |

### 4.2 `CUSTOMER.MEDALLIA_TOPIC_SCORES`

NLU-derived topic sentiment scores per response — one row per response × topic.

| Column | Type | Description |
|---|---|---|
| `topic_score_id` | VARCHAR(72) PK | Composite: `response_id + '_' + topic_code` |
| `response_id` | VARCHAR(36) FK | Parent response |
| `programme_id` | VARCHAR | |
| `topic_code` | VARCHAR | Medallia topic identifier (e.g. `INSTALL_EXP`, `FAULT_RESOL`, `SPEED_PERF`) |
| `topic_label` | VARCHAR | Human-readable label (e.g. `Installation Experience`) |
| `sentiment_score` | DECIMAL(5,3) | Medallia sentiment: -1.0 (very negative) to +1.0 (very positive) |
| `sentiment_category` | VARCHAR | `Positive`, `Neutral`, `Negative` |
| `confidence_score` | DECIMAL(5,3) | NLU confidence 0.0–1.0 |
| `response_date` | DATE | Denormalised from parent response for partitioning |
| `run_date` | DATE | |
| `_loaded_at` | TIMESTAMP_NTZ | |

### 4.3 `CUSTOMER.MEDALLIA_PROGRAMMES`

Programme metadata — a slowly changing dimension.

| Column | Type | Description |
|---|---|---|
| `programme_id` | VARCHAR PK | Medallia programme ID |
| `programme_name` | VARCHAR | |
| `programme_type` | VARCHAR | `NPS`, `CSAT`, `CES`, `Satisfaction` |
| `trigger_type` | VARCHAR | `Transactional` (event-triggered) or `Relationship` (periodic) |
| `target_audience` | VARCHAR | `End Customer`, `RSP`, `Internal` |
| `active_flag` | BOOLEAN | |
| `launch_date` | DATE | |
| `questions` | VARIANT | JSON array of question definitions (question text, type, scale) |
| `run_date` | DATE | |
| `_loaded_at` | TIMESTAMP_NTZ | |

### 4.4 `CUSTOMER.MEDALLIA_NPS_DAILY`

Pre-aggregated daily NPS metrics — replaces the weekly aggregate produced by
`nnn_nps_survey_etl_weekly` and provides daily granularity.

| Column | Type | Description |
|---|---|---|
| `response_date` | DATE PK (composite) | |
| `programme_id` | VARCHAR PK (composite) | |
| `rsp_code` | VARCHAR PK (composite) | NULL row for NNN-wide aggregate |
| `total_responses` | INTEGER | |
| `promoters` | INTEGER | NPS 9–10 |
| `passives` | INTEGER | NPS 7–8 |
| `detractors` | INTEGER | NPS 0–6 |
| `nps_score` | DECIMAL(5,2) | `(promoters - detractors) / total_responses * 100` |
| `avg_satisfaction` | DECIMAL(5,2) | Mean `overall_satisfaction_score` |
| `response_rate_pct` | DECIMAL(5,2) | NULL in this table — Medallia provides sent-count at programme level only, not per day. Response rate is computed monthly in `CUSTOMER.MEDALLIA_NPS_MONTHLY` view |
| `run_date` | DATE | |
| `_loaded_at` | TIMESTAMP_NTZ | |

---

## 5. Data Flow

```
Medallia Reporting API
    │
    ├─ [Async export job] Submit bulk export request → poll until ready → download response JSON
    ├─ [Topic API] GET /topics for each programme → topic scores per response
    └─ [Metadata API] GET /programs → programme definitions (full reload daily)
    │
    ▼
S3 Data Lake
nnn-data-lake-prod/medallia/<programme_id>/run_date=YYYY-MM-DD/responses.parquet
nnn-data-lake-prod/medallia/<programme_id>/run_date=YYYY-MM-DD/topic_scores.parquet
    │
    ▼  [Transform — extract rsp_code, resolve topic labels, compute NPS category, normalise scores]
Snowflake STAGING (temp tables)
    │
    ▼  [MERGE on response_id; daily aggregate INSERT OR REPLACE]
Snowflake CUSTOMER Schema
    ├── MEDALLIA_SURVEY_RESPONSES
    ├── MEDALLIA_TOPIC_SCORES
    ├── MEDALLIA_PROGRAMMES
    └── MEDALLIA_NPS_DAILY
    │
    ▼  [Downstream consumers]
    ├── nnn_nps_survey_etl_weekly (deprecated after go-live — replaced by MEDALLIA_NPS_DAILY)
    └── CUSTOMER.V_CX_FAULT_CORRELATION (new view — see Section 7)
```

---

## 6. Load Strategy

| Table | Strategy | Notes |
|---|---|---|
| `MEDALLIA_SURVEY_RESPONSES` | Incremental — `lastUpdated > last_watermark` | Responses can be updated in Medallia for up to 7 days (partial completions, re-scoring) |
| `MEDALLIA_TOPIC_SCORES` | Derived from responses — re-extracted with same window | Topic scores are re-scored by Medallia NLU retroactively within 24 hours |
| `MEDALLIA_PROGRAMMES` | Full reload daily | ~5 programmes; changes are infrequent but must be captured |
| `MEDALLIA_NPS_DAILY` | Recomputed — DELETE for `response_date >= (today - 7)` then re-aggregate | Covers the 7-day retroactive update window |

**Watermark**: `OPERATIONS.PIPELINE_RUN_LOG` stores the last successful Medallia
`lastUpdated` timestamp per programme. A single watermark per programme allows
independent failure and retry per programme without affecting others.

**Async export pattern**: Medallia's bulk export is asynchronous. The extract task
submits a job, then polls the job status endpoint every 60 seconds until the export is
ready (typically 2–5 minutes), then downloads the file. A maximum poll timeout of 30
minutes is enforced before the task fails.

---

## 7. Transformation Rules

| Rule ID | Target | Rule |
|---|---|---|
| T-01 | `MEDALLIA_SURVEY_RESPONSES` | `rsp_code` extracted from Medallia custom field `nnn_rsp_code`. This field is configured on the following programmes: Post-Installation NPS, Post-Fault Resolution, and RSP Portal Usability. It is NOT present on Quarterly Customer Pulse (direct consumer survey). For programmes without `nnn_rsp_code`, the column is NULL |
| T-02 | `MEDALLIA_SURVEY_RESPONSES` | `servicenow_incident_id` extracted from Medallia custom field `nnn_incident_sys_id`. This field is configured on the Post-Fault Resolution programme only. All other programmes have `servicenow_incident_id = NULL` |
| T-03 | `MEDALLIA_SURVEY_RESPONSES` | `nps_category` derived: score 9–10 → `Promoter`; 7–8 → `Passive`; 0–6 → `Detractor`; NULL if no NPS question in programme |
| T-04 | `MEDALLIA_SURVEY_RESPONSES` | `overall_satisfaction_score` normalised to 0–100 range regardless of source scale (some programmes use 1–5, others 0–10) |
| T-05 | `MEDALLIA_TOPIC_SCORES` | `sentiment_category` derived: score > 0.2 → `Positive`; score < -0.2 → `Negative`; else `Neutral` |
| T-06 | `MEDALLIA_TOPIC_SCORES` | Topic scores with `confidence_score < 0.5` are excluded from `MEDALLIA_NPS_DAILY` aggregations but retained in `MEDALLIA_TOPIC_SCORES` with the raw confidence value |
| T-07 | `MEDALLIA_NPS_DAILY` | `nps_score` = `(promoters - detractors) / total_responses * 100` — standard NPS formula |
| T-08 | `MEDALLIA_NPS_DAILY` | A summary row with `rsp_code = NULL` is inserted per `(response_date, programme_id)` representing the NNN-wide aggregate |
| T-09 | All | Medallia timestamps are UTC — convert to AEST before storage |
| T-10 | `MEDALLIA_SURVEY_RESPONSES` | `verbatim_text` is stored as-is. No NLP processing occurs in this pipeline — topic scores from Medallia NLU are used directly via `MEDALLIA_TOPIC_SCORES` |

---

## 8. New View: `CUSTOMER.V_CX_FAULT_CORRELATION`

Joins post-fault survey responses to ServiceNow incident data (loaded by Feature 03)
to measure whether fault resolution quality predicts CX satisfaction.

| Column | Description |
|---|---|
| `response_date` | Survey submission date |
| `servicenow_incident_id` | ServiceNow incident sys_id |
| `incident_priority` | From `COMPLIANCE.SERVICENOW_INCIDENTS` |
| `resolution_minutes` | From `COMPLIANCE.SERVICENOW_INCIDENTS` |
| `sla_breached_flag` | From `COMPLIANCE.SERVICENOW_INCIDENTS` |
| `nps_score` | Post-fault NPS response |
| `nps_category` | Promoter / Passive / Detractor |
| `fault_resolution_sentiment` | `MEDALLIA_TOPIC_SCORES.sentiment_score` for topic `FAULT_RESOL` |
| `overall_satisfaction_score` | |
| `rsp_code` | |

This view is the primary input for downstream ML feature engineering pipelines that require joined CX and incident data.

---

## 9. Schedule & SLA

| Attribute | Value |
|---|---|
| **Schedule** | Daily at 04:00 AEST (18:00 UTC) |
| **SLA** | Must complete within 2 hours (before 06:00 AEST) |
| **Retries** | 3 attempts, 10-minute delay |
| **SLA miss behaviour** | `nnn_sla_miss_alert` — PagerDuty `error` (P2) |
| **Dependency** | Feature 03 (ServiceNow ETL) must succeed before this DAG runs — `V_CX_FAULT_CORRELATION` requires incident data to be fresh |

---

## 10. Non-Functional Requirements

| Requirement | Detail |
|---|---|
| **Volume** | ~300–500 new/updated responses per day across all programmes; ~3,000–8,000 topic score rows per day |
| **PII handling** | `verbatim_text` may contain customer names, addresses, or contact details typed by respondents. The pipeline does NOT mask or redact verbatim text — access to `CUSTOMER.MEDALLIA_SURVEY_RESPONSES` is restricted to `CX_ANALYST` Snowflake role. A PII review and data retention policy must be agreed with the Privacy Officer before go-live |
| **Data retention** | Medallia retains response data for 3 years; Snowflake should match this retention period via a scheduled DELETE of rows where `response_date < DATEADD(year, -3, CURRENT_DATE)` |
| **Re-runability** | MERGE on `response_id` is idempotent; `MEDALLIA_NPS_DAILY` uses DELETE + re-aggregate for the rolling 7-day window |
| **Medallia API outage** | If async export job does not complete within 30 minutes, the task fails and retries from scratch (re-submits the export job) |
| **Deprecation of old DAG** | `nnn_nps_survey_etl_weekly` must be decommissioned after 4 weeks of parallel running to verify `MEDALLIA_NPS_DAILY` produces equivalent values |

---

## 11. New Airflow DAG

| Attribute | Value |
|---|---|
| **DAG ID** | `nnn_medallia_cx_daily` |
| **File** | `dags/integrations/nnn_medallia_cx_daily.py` |
| **Schedule** | `0 18 * * *` (04:00 AEST) |
| **Tags** | `nnn`, `medallia`, `customer`, `cx`, `daily` |

**Task sequence:**

```
check_medallia_api
    │
    ▼
reload_programmes           [full reload — metadata only]
    │
    ▼
submit_export_post_install ──────────────────────────┐
submit_export_post_fault ────────────────────────────┤  [parallel — submit async jobs]
submit_export_quarterly_pulse ───────────────────────┤
submit_export_rsp_portal ────────────────────────────┘
    │
    ▼
poll_and_download_all       [single task — polls all job IDs until all ready, then downloads]
    │
    ▼
transform_and_stage         [parse JSON, normalise, compute derived fields]
    │
    ▼
merge_responses ────────────────────────────────────┐
merge_topic_scores ─────────────────────────────────┤  [parallel]
recompute_nps_daily ────────────────────────────────┘
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
| AC-01 | All 4 target tables created in Snowflake `CUSTOMER` schema with defined columns and types |
| AC-02 | `MEDALLIA_NPS_DAILY` weekly NPS scores match the current manually exported weekly NPS figures within ±2 NPS points (parallel running validation, 4 weeks) |
| AC-03 | Post-fault survey responses have `servicenow_incident_id` populated for at least 80% of records (20% may be missing due to incomplete Medallia custom field configuration) |
| AC-04 | `MEDALLIA_TOPIC_SCORES` contains at least one topic score row per completed response where verbatim text was provided |
| AC-05 | `V_CX_FAULT_CORRELATION` returns matched rows for at least 70% of post-fault responses (unmatched = ServiceNow incident not yet loaded or ID mismatch) |
| AC-06 | Async export jobs that take longer than 30 minutes fail the task cleanly with a descriptive error message (not a silent timeout) |
| AC-07 | Re-running the pipeline for the same date produces identical row counts in `MEDALLIA_SURVEY_RESPONSES` (idempotent MERGE) |
| AC-08 | `MEDALLIA_NPS_DAILY` 7-day recomputation window correctly revises NPS for days where responses were updated after initial load |
| AC-09 | Access to `MEDALLIA_SURVEY_RESPONSES` (which contains verbatim PII) is denied for roles other than `CX_ANALYST` and `DATA_ENGINEERING` |
| AC-10 | `nnn_nps_survey_etl_weekly` is decommissioned without data gaps after 4 weeks of parallel validation |

---

## 13. Out of Scope

- Medallia's built-in reporting and dashboard — this feature covers data extraction only.
- Real-time response streaming (Medallia webhook) — daily batch is sufficient.
- Translation of non-English verbatim text — Medallia already provides an English translation field; the pipeline stores Medallia's translation, not a new one.
- Medallia Administration API — no programme configuration changes through the pipeline.
- Annual RSP Partner Satisfaction survey response rate computation — handled in `CUSTOMER.MEDALLIA_NPS_MONTHLY` view, not in `MEDALLIA_NPS_DAILY`. The survey itself IS included in the daily batch; the extract returns 0 rows on days with no new responses and completes normally.

---

## 14. Dependencies & Prerequisites

- `nnn_medallia_prod` Airflow connection configured with Medallia OAuth client ID and secret.
- Medallia integration service account provisioned with read access to all 5 in-scope programmes. All programmes have been reviewed by Legal and cleared for extraction — no ethics exclusions apply.
- Verbatim survey response data retention period is confirmed at 3 years in Snowflake, matching Medallia's own retention policy. A scheduled Snowflake task runs monthly and deletes rows where `response_date < DATEADD(year, -3, CURRENT_DATE)`. PII access controls are approved by the Privacy Officer — `CX_ANALYST` role only for `MEDALLIA_SURVEY_RESPONSES`.
- Medallia custom fields `nnn_rsp_code` and `nnn_incident_sys_id` are confirmed as configured on Post-Installation NPS, Post-Fault Resolution, and RSP Portal Usability programmes. Quarterly Customer Pulse does not carry `nnn_rsp_code` by design (direct consumer programme, not RSP-linked).
- Annual RSP Partner Satisfaction survey is included in the daily batch. On days with no new responses, the extract returns 0 rows and the pipeline completes normally without error.
- Feature 03 (ServiceNow ETL) must be live and loading `COMPLIANCE.SERVICENOW_INCIDENTS` before `V_CX_FAULT_CORRELATION` is meaningful.
- `CUSTOMER` schema exists in Snowflake (confirmed).
