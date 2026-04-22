# Feature 04 — SAP ERP Asset & Workforce Data to Snowflake ETL

| Field | Value |
|---|---|
| **Status** | Ready for Development |
| **Priority** | P2 — Medium-High |
| **Owner** | NNN Data Engineering |
| **Stakeholders** | Network Engineering, Field Operations, Finance (CAPEX), HR Analytics |
| **Target Domain** | `INFRASTRUCTURE` and `FIELD_OPS` schemas in Snowflake |
| **Estimated Effort** | 4 sprints (4 weeks) |

---

## 1. Executive Summary

NNN uses SAP ERP (S/4HANA) to manage physical network asset lifecycle, procurement
orders, preventive maintenance schedules, and the field workforce master. This data
currently has no automated path into Snowflake, which means network capacity planning
relies on manually maintained asset spreadsheets and HR Analytics cannot correlate
workforce headcount with field ops delivery metrics. This feature adds a weekly ETL
pipeline that extracts SAP asset, maintenance, procurement, and workforce data into
Snowflake, enabling cross-domain analysis across infrastructure health, CAPEX spend,
and field operations throughput.

---

## 2. Business Context & Motivation

### Problems Being Solved

- **Network asset data is stale**: `nnn_node_health_monitoring_hourly` reports on
  real-time performance of network nodes, but Snowflake has no record of the physical
  asset attributes (age, vendor, rated capacity, warranty status, location) that explain
  why a node is degrading. Asset data lives only in SAP.
- **CAPEX reconciliation gap**: `nnn_capex_project_tracking_weekly` tracks project
  actuals from Oracle EBS, but the underlying purchase orders and goods receipts (which
  confirm whether CAPEX-procured equipment has actually been delivered and installed) are
  managed in SAP. Without this link, Finance cannot close the CAPEX-to-asset gap.
- **Field workforce planning is disconnected**: `nnn_technician_scheduling_daily` assigns
  technicians to jobs, but has no connection to the SAP HR master data that holds
  technician certifications, employment type (permanent vs contractor), and FSA
  assignments. Scheduling decisions cannot be validated against actual workforce capacity.
- **Preventive maintenance data missing**: Network teams cannot correlate planned SAP
  maintenance schedules against actual NMS-observed fault rates (from
  `nnn_network_performance_daily`) to identify whether preventive maintenance is effective.

### Expected Outcomes

- Snowflake `INFRASTRUCTURE` schema enriched with physical asset attributes joinable
  to NMS performance data.
- Oracle EBS CAPEX actuals linkable to SAP goods receipts — closing the CAPEX cycle
  in Snowflake.
- Field workforce master data available in `FIELD_OPS` schema for scheduling
  capacity planning.
- SAP preventive maintenance schedule available for comparison against observed fault rates.

---

## 3. Source System Overview

| Attribute | Detail |
|---|---|
| **System** | SAP S/4HANA 2023 (on-premise, Sydney data centre) |
| **Access method** | SAP OData API v4 (preferred) for structured entities; RFC/BAPI fallback for complex structures |
| **Authentication** | SAP Basic Authentication with a dedicated integration user `NNN_ETL_USER` (read-only) |
| **Airflow connection** | `nnn_sap_erp_prod` (type: HTTP; base URL: `https://sap-s4.nnnco.internal`) |
| **Network path** | SAP is on-prem in Sydney DC; Airflow workers connect via internal network (no VPN needed — same DC) |
| **SAP modules in scope** | PM (Plant Maintenance), MM (Materials Management), HR (Human Resources — restricted) |
| **Extraction frequency** | Weekly — SAP data changes slowly; daily extracts are not justified by data velocity |

### SAP Entities in Scope

| SAP Entity / Table | Module | OData Entity / BAPI | Description |
|---|---|---|---|
| Equipment Master | PM | `A_Equipment` | Physical network asset records |
| Functional Location | PM | `A_FunctionalLocation` | Logical site/rack hierarchy |
| Maintenance Order | PM | `A_MaintenanceOrder` | Corrective and preventive maintenance orders |
| Maintenance Notification | PM | `A_MaintenanceNotification` | Faults and inspection findings |
| Goods Receipt | MM | `A_GoodsMovement` (type 101) | Equipment delivered against purchase order |
| Purchase Order | MM | `A_PurchaseOrder` | Procurement orders for network equipment |
| Purchase Order Item | MM | `A_PurchaseOrderItem` | Line items per PO |
| Employee Master | HR | `A_BusinessPartner` (employee role) | Field technician workforce master |
| Organisational Unit | HR | `A_OrganizationalUnit` | Team / FSA hierarchy |

---

## 4. Target Schema Design

Network and infrastructure data lands in `INFRASTRUCTURE` schema; workforce data in
`FIELD_OPS` schema. All tables carry `_sap_changed_at` (SAP `ChangedDateTime`),
`run_date`, and `_loaded_at`.

### 4.1 `INFRASTRUCTURE.SAP_NETWORK_ASSETS`

Physical network equipment — the asset dimension that enriches NMS performance data.

| Column | Type | Description |
|---|---|---|
| `equipment_id` | VARCHAR(18) PK | SAP Equipment Number |
| `equipment_description` | VARCHAR | Asset description |
| `equipment_category` | VARCHAR | `OLT`, `ONU`, `Splitter`, `Router`, `Switch`, `Fibre Cable` |
| `asset_class` | VARCHAR | SAP asset class code |
| `functional_location` | VARCHAR | SAP Functional Location ID — physical site hierarchy |
| `site_code` | VARCHAR | NNN site/exchange code |
| `poi_id` | VARCHAR | Point of Interconnect — join key to `NETWORK.LINK_PERFORMANCE_DAILY` |
| `vendor_name` | VARCHAR | Equipment manufacturer |
| `model_number` | VARCHAR | Hardware model |
| `serial_number` | VARCHAR | Physical serial number |
| `manufacture_date` | DATE | |
| `installation_date` | DATE | Date asset commissioned at site |
| `warranty_expiry_date` | DATE | |
| `asset_age_years` | DECIMAL(5,2) | Derived: years since `installation_date` |
| `rated_capacity_gbps` | DECIMAL(10,2) | Vendor-rated port capacity |
| `lifecycle_status` | VARCHAR | `Active`, `End of Life`, `Decommissioned`, `Spare` |
| `capitalized_value_aud` | DECIMAL(15,2) | Book value at capitalisation (from FA module) |
| `useful_life_years` | INTEGER | Accounting useful life |
| `run_date` | DATE | |
| `_sap_changed_at` | TIMESTAMP_NTZ | SAP last change timestamp — watermark column |
| `_loaded_at` | TIMESTAMP_NTZ | |

### 4.2 `INFRASTRUCTURE.SAP_MAINTENANCE_ORDERS`

Corrective and preventive maintenance orders against network assets.

| Column | Type | Description |
|---|---|---|
| `maintenance_order_id` | VARCHAR(12) PK | SAP Order Number |
| `equipment_id` | VARCHAR(18) FK | Parent asset from `SAP_NETWORK_ASSETS` |
| `order_type` | VARCHAR | `PM01` = Corrective, `PM02` = Preventive, `PM03` = Inspection |
| `description` | VARCHAR | Work description |
| `priority` | VARCHAR | `Very High`, `High`, `Medium`, `Low` |
| `status` | VARCHAR | `Created`, `Released`, `In Progress`, `Completed`, `Closed` |
| `planned_start` | DATE | |
| `planned_end` | DATE | |
| `actual_start` | DATE | |
| `actual_finish` | DATE | |
| `total_planned_cost_aud` | DECIMAL(15,2) | |
| `total_actual_cost_aud` | DECIMAL(15,2) | |
| `responsible_work_centre` | VARCHAR | SAP work centre (team/group) |
| `notification_id` | VARCHAR(12) FK | Triggering notification (if corrective) |
| `run_date` | DATE | |
| `_sap_changed_at` | TIMESTAMP_NTZ | |
| `_loaded_at` | TIMESTAMP_NTZ | |

### 4.3 `INFRASTRUCTURE.SAP_PROCUREMENT_ORDERS`

Purchase orders for network equipment — links CAPEX spend to physical assets.

| Column | Type | Description |
|---|---|---|
| `po_number` | VARCHAR(10) PK | SAP Purchase Order number |
| `po_item` | INTEGER | PO line item number |
| `vendor_id` | VARCHAR(10) | SAP vendor code |
| `vendor_name` | VARCHAR | |
| `material_number` | VARCHAR | SAP material/part number |
| `material_description` | VARCHAR | |
| `po_quantity` | DECIMAL(13,3) | Ordered quantity |
| `unit_of_measure` | VARCHAR | `EA` (each), `M` (metres), `KG`, etc. |
| `net_price_aud` | DECIMAL(15,2) | |
| `total_value_aud` | DECIMAL(15,2) | `po_quantity * net_price_aud` |
| `delivery_date` | DATE | Promised delivery date |
| `goods_receipt_date` | DATE | Actual goods receipt date (NULL if not received) |
| `goods_receipt_quantity` | DECIMAL(13,3) | Quantity confirmed received |
| `invoice_receipt_flag` | BOOLEAN | TRUE if supplier invoice matched |
| `oracle_project_code` | VARCHAR | Oracle Projects code for CAPEX cross-reference |
| `po_status` | VARCHAR | `Open`, `Partially Received`, `Completed`, `Cancelled` |
| `run_date` | DATE | |
| `_sap_changed_at` | TIMESTAMP_NTZ | |
| `_loaded_at` | TIMESTAMP_NTZ | |

### 4.4 `FIELD_OPS.SAP_WORKFORCE_MASTER`

Field technician employee master — dimension for scheduling and performance analysis.

| Column | Type | Description |
|---|---|---|
| `employee_id` | VARCHAR(10) PK | SAP Personnel Number |
| `first_name` | VARCHAR | (PII — masked in non-prod) |
| `last_name` | VARCHAR | (PII — masked in non-prod) |
| `preferred_name` | VARCHAR | Display name — not masked |
| `employment_type` | VARCHAR | `Permanent`, `Contractor`, `Part-Time` |
| `job_title` | VARCHAR | e.g. `Senior Field Technician`, `Network Engineer` |
| `team_name` | VARCHAR | SAP Org Unit name |
| `fsa_assignment` | VARCHAR | Field Service Area primary assignment |
| `certifications` | ARRAY | List of active certification codes (e.g. `['ACMA', 'NBN_OLT', 'FTTN']`) |
| `certification_expiry_dates` | VARIANT | JSON object: `{cert_code: expiry_date}` |
| `hire_date` | DATE | |
| `active_flag` | BOOLEAN | FALSE if on extended leave or terminated |
| `manager_employee_id` | VARCHAR(10) | FK to same table (team lead) |
| `cost_centre` | VARCHAR | GL cost centre for labour cost allocation |
| `run_date` | DATE | |
| `_sap_changed_at` | TIMESTAMP_NTZ | |
| `_loaded_at` | TIMESTAMP_NTZ | |

---

## 5. Data Flow

```
SAP S/4HANA OData API v4 (on-premise, Sydney DC)
    │
    ▼  [Extract — OData $filter on ChangedDateTime > last_watermark; paginated with $top/$skip]
S3 Data Lake
nnn-data-lake-prod/sap_erp/<entity>/run_date=YYYY-MM-DD/data.parquet
    │
    ▼  [Transform — resolve SAP codes to descriptions, derive age/status fields, mask PII]
Snowflake STAGING (temp tables)
    │
    ▼  [MERGE on SAP primary key]
Snowflake
    ├── INFRASTRUCTURE.SAP_NETWORK_ASSETS
    ├── INFRASTRUCTURE.SAP_MAINTENANCE_ORDERS
    ├── INFRASTRUCTURE.SAP_PROCUREMENT_ORDERS
    └── FIELD_OPS.SAP_WORKFORCE_MASTER
    │
    ▼  [Downstream consumers]
    ├── nnn_node_health_monitoring_hourly (joins SAP_NETWORK_ASSETS via poi_id)
    ├── nnn_capex_project_tracking_weekly (joins SAP_PROCUREMENT_ORDERS via oracle_project_code)
    └── INFRASTRUCTURE.V_ASSET_HEALTH_COMBINED (new view — see Section 7)
```

---

## 6. Load Strategy

| Table | Strategy | Watermark Column |
|---|---|---|
| `SAP_NETWORK_ASSETS` | Incremental | `Equipment.ChangedDateTime` |
| `SAP_MAINTENANCE_ORDERS` | Incremental | `MaintenanceOrder.ChangedDateTime` |
| `SAP_PROCUREMENT_ORDERS` | Incremental | `PurchaseOrder.LastChangeDateTime` |
| `FIELD_OPS.SAP_WORKFORCE_MASTER` | Full reload weekly | Small (~500 rows); full reload ensures deactivations are captured |

**Schedule**: Weekly on Sunday at 01:00 AEST — chosen because SAP is a batch-update
system with low weekend change volume, and Monday-morning reports need fresh data.

**Decommissioned assets**: SAP does not delete Equipment master records; it sets a
`DeletionFlag`. The ETL must honour this by setting `lifecycle_status = 'Decommissioned'`
rather than physically deleting from Snowflake.

---

## 7. Transformation Rules

| Rule ID | Target | Rule |
|---|---|---|
| T-01 | `SAP_NETWORK_ASSETS` | `equipment_category` derived from SAP equipment category code using the lookup table `OPERATIONS.SAP_EQUIPMENT_CATEGORY_MAPPING` (e.g. SAP code `Z001` → `OLT`, `Z002` → `ONU`, `Z003` → `Splitter`). The full code list is extracted from SAP configuration table `T370` during the initial setup task and stored in Snowflake — it does not require manual maintenance |
| T-02 | `SAP_NETWORK_ASSETS` | `poi_id` extracted from SAP Functional Location description field using regex pattern `POI-[A-Z]{3}[0-9]{4}` (e.g. `POI-SYD0042`). This naming convention is confirmed to be applied consistently by the Network Engineering team across all active functional locations. Assets without a matching POI pattern have `poi_id = NULL` |
| T-03 | `SAP_NETWORK_ASSETS` | `asset_age_years` = `DATEDIFF('day', installation_date, CURRENT_DATE) / 365.25`; NULL if `installation_date` is NULL |
| T-04 | `SAP_NETWORK_ASSETS` | `lifecycle_status` = `'Decommissioned'` if SAP `DeletionFlag = 'X'`; else derived from SAP user status field |
| T-05 | `SAP_MAINTENANCE_ORDERS` | `order_type` description resolved from SAP order type code using standard PM type mapping |
| T-06 | `SAP_PROCUREMENT_ORDERS` | `po_status` derived: `'Completed'` if `goods_receipt_quantity >= po_quantity`; `'Partially Received'` if `0 < goods_receipt_quantity < po_quantity`; `'Open'` if `goods_receipt_quantity = 0`; `'Cancelled'` if SAP deletion indicator set |
| T-07 | `SAP_PROCUREMENT_ORDERS` | `oracle_project_code` mapped from SAP WBS element field — requires a cross-reference table maintained by Finance (Oracle WBS → SAP WBS mapping) |
| T-08 | `FIELD_OPS.SAP_WORKFORCE_MASTER` | `certifications` extracted from SAP Qualifications infotype (IT0024); active qualifications only (end date >= today) |
| T-09 | `FIELD_OPS.SAP_WORKFORCE_MASTER` | `first_name` and `last_name` masked in non-production environments (replaced with `Employee` and employee ID) |
| T-10 | All | SAP timestamps are in local system time (AEST); stored in Snowflake as AEST with `_AEST` suffix on timestamp columns where timezone is ambiguous |

---

## 8. New Views

### `INFRASTRUCTURE.V_ASSET_HEALTH_COMBINED`

Joins NMS performance data to SAP asset attributes — the primary view for network
capacity planning and asset refresh prioritisation.

| Column | Description |
|---|---|
| `poi_id` | Point of Interconnect |
| `equipment_id` | SAP asset ID |
| `equipment_category` | OLT, ONU, etc. |
| `asset_age_years` | From SAP |
| `warranty_expiry_date` | From SAP |
| `lifecycle_status` | From SAP |
| `utilisation_p95_30d` | 30-day P95 utilisation from `NETWORK.LINK_PERFORMANCE_DAILY` |
| `latency_p95_30d` | 30-day P95 latency |
| `fault_count_90d` | Count of corrective maintenance orders in last 90 days |
| `last_preventive_maintenance` | Most recent preventive order actual finish date |
| `refresh_risk_score` | Derived score (0–100): weighted combination of age, utilisation, fault count, warranty status |

---

## 9. Schedule & SLA

| Attribute | Value |
|---|---|
| **Schedule** | Weekly — Sunday at 01:00 AEST (Saturday 15:00 UTC) |
| **SLA** | Must complete within 4 hours (before 05:00 AEST Sunday) |
| **Retries** | 3 attempts, 20-minute delay |
| **SLA miss behaviour** | `nnn_sla_miss_alert` — PagerDuty `error` (P2) |
| **Downstream dependency** | `nnn_capex_project_tracking_weekly` runs Monday 06:00 AEST — must succeed before then |

---

## 10. Non-Functional Requirements

| Requirement | Detail |
|---|---|
| **Volume** | Asset master: ~5,000 records; maintenance orders: ~500 modified/week; POs: ~200 new/week; workforce: ~500 records (full reload) |
| **PII handling** | `first_name`, `last_name` in `SAP_WORKFORCE_MASTER` classified as PII; masked in non-prod; access in prod restricted to `FIELD_OPS_ANALYST` role |
| **SAP API availability** | SAP maintenance window is Saturday 22:00–Sunday 00:30 AEST. The pipeline runs at Sunday 01:00 AEST, which is after the maintenance window — no scheduling conflict. If SAP is unavailable at runtime, the pipeline retries 3 times with a 20-minute delay before alerting PagerDuty |
| **SAP load** | OData extraction must use `$top=500` pagination; no bulk dump that could saturate SAP application server threads |
| **Cross-reference table** | Oracle-to-SAP WBS code mapping (for T-07) must be maintained in Snowflake as a lookup table (`OPERATIONS.ORACLE_SAP_WBS_MAPPING`) by the Finance team |
| **Re-runability** | MERGE-based — all loads are idempotent |

---

## 11. New Airflow DAG

| Attribute | Value |
|---|---|
| **DAG ID** | `nnn_sap_erp_asset_workforce_weekly` |
| **File** | `dags/integrations/nnn_sap_erp_asset_workforce_weekly.py` |
| **Schedule** | `0 15 * * 6` (Sunday 01:00 AEST = Saturday 15:00 UTC) |
| **Tags** | `nnn`, `sap`, `infrastructure`, `field_ops`, `weekly` |

**Task sequence:**

```
check_sap_api
    │
    ▼
extract_network_assets ─────────────────────────────┐
extract_maintenance_orders ─────────────────────────┤  [parallel]
extract_procurement_orders ─────────────────────────┤
extract_workforce_master ───────────────────────────┘
    │
    ▼
transform_and_stage
    │
    ▼
merge_network_assets ───────────────────────────────┐
merge_maintenance_orders ───────────────────────────┤  [parallel]
merge_procurement_orders ───────────────────────────┤
reload_workforce_master ────────────────────────────┘
    │
    ▼
validate_load
    │
    ▼
refresh_asset_health_view
    │
    ▼
update_pipeline_log
```

---

## 12. Acceptance Criteria

| # | Criterion |
|---|---|
| AC-01 | `SAP_NETWORK_ASSETS` row count matches the count of active equipment records in SAP (verified with SAP admin running a matching report) |
| AC-02 | `poi_id` is populated for at least 90% of OLT and ONU asset records (remaining 10% may not have a POI assignment yet) |
| AC-03 | `V_ASSET_HEALTH_COMBINED` joins correctly to `NETWORK.LINK_PERFORMANCE_DAILY` with no more than 5% unmatched POIs |
| AC-04 | `SAP_PROCUREMENT_ORDERS.oracle_project_code` joins to at least 80% of Oracle EBS CAPEX project codes (validated with Finance) |
| AC-05 | Decommissioned SAP assets have `lifecycle_status = 'Decommissioned'` in Snowflake within 1 week of being flagged in SAP |
| AC-06 | `SAP_WORKFORCE_MASTER` technician records match headcount in the HR system (± 2 for contractors in transition) |
| AC-07 | `certifications` array is populated for technicians with active SAP qualifications |
| AC-08 | PII columns (`first_name`, `last_name`) are masked in the staging Snowflake environment |
| AC-09 | Re-running the weekly DAG produces no duplicate rows |
| AC-10 | `nnn_capex_project_tracking_weekly` produces the same CAPEX total when reading SAP goods receipts vs the current manually assembled input file (validated by Finance) |

---

## 13. Out of Scope

- SAP HR modules beyond workforce master: payroll, leave management, performance reviews.
- SAP FI/CO (Financials/Controlling) — financial data is sourced from Oracle EBS (Feature 02).
- SAP SD (Sales and Distribution) — sales is managed in Salesforce (Feature 01).
- Real-time SAP event streaming (IDocs, SAP Event Mesh) — weekly batch is sufficient for current use cases.
- SAP non-production systems (Dev/QA) — production instance only.
- Bi-directional sync or write-back to SAP.

---

## 14. Dependencies & Prerequisites

- `nnn_sap_erp_prod` Airflow connection configured with SAP OData base URL `https://sap-s4.nnnco.internal` and `NNN_ETL_USER` basic auth credentials. `NNN_ETL_USER` is confirmed to have read-only access to PM, MM, and HR OData services.
- `OPERATIONS.ORACLE_SAP_WBS_MAPPING` lookup table created and populated by Finance before first run (required for T-07).
- `OPERATIONS.SAP_EQUIPMENT_CATEGORY_MAPPING` lookup table seeded from SAP configuration table `T370` during the initial setup task — no manual file required from Network Engineering.
- `INFRASTRUCTURE` and `FIELD_OPS` schemas exist in Snowflake (confirmed — used by existing DAGs).
- NNN HR policy permits loading employee name and certification data into Snowflake. PII masking in non-production environments is the agreed control. Production access is restricted to the `FIELD_OPS_ANALYST` Snowflake role.
- `nnn_capex_project_tracking_weekly` DAG updated to JOIN `SAP_PROCUREMENT_ORDERS` after go-live.
