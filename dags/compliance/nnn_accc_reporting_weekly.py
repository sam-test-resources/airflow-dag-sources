"""
nnn_accc_reporting_weekly
~~~~~~~~~~~~~~~~~~~~~~~~~~
Owner:      nnn-data-engineering / Regulatory Affairs
Domain:     Compliance / Regulatory
Schedule:   Thursday 05:00 AEST (19:00 UTC Wednesday) — report due to ACCC by Friday
SLA:        8 hours  ← REGULATORY DEADLINE

Produces NNN's weekly regulatory performance report submitted to the
Australian Competition and Consumer Commission (ACCC) under the
Wholesale Broadband Agreement (WBA) monitoring framework.

ACCC metrics required (as per WBA Schedule 1B):
  - Activation performance:  % completed within SLA by RSP and technology type
  - Fault repair performance: % resolved within 2/5/20 business days tiers
  - Major Service Failure (MSF) count and affected premises
  - CVC utilisation > 80% (congestion incidents by POI and state)
  - Service qualification accuracy

All metrics are broken down by:
  - State/Territory (NSW, VIC, QLD, SA, WA, TAS, NT, ACT)
  - Technology type (FTTP, FTTC, FTTN, HFC, FW)
  - RSP (anonymised in public submissions — full RSP IDs in internal version)

Outputs:
  1. Snowflake COMPLIANCE.ACCC_METRICS          (detailed, internal)
  2. Snowflake COMPLIANCE.ACCC_METRICS_PUBLISHED (anonymised RSPs, for submission)
  3. S3: nnn-data-lake-prod/compliance/accc/YYYY-WW/accc_report.json  (submission file)
  4. S3: nnn-data-lake-prod/compliance/accc/YYYY-WW/accc_report.xlsx  (human-readable)
"""

from __future__ import annotations

import json
import logging
import tempfile
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    get_run_date, get_snowflake_hook, upload_to_s3,
)

log = logging.getLogger(__name__)

default_args = {
    "owner":                    "nnn-data-engineering",
    "depends_on_past":          True,
    "email":                    ["de-alerts@nnnco.com.au", "regulatory@nnnco.com.au"],
    "email_on_failure":         True,
    "email_on_retry":           False,
    "retries":                  3,
    "retry_delay":              timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay":          timedelta(minutes=60),
    "on_failure_callback":      nnn_failure_alert,
    "execution_timeout":        timedelta(hours=4),
}

SQL_ACTIVATION_METRICS = """
CREATE OR REPLACE TEMPORARY TABLE tmp_accc_activations AS
SELECT
    state_territory,
    technology_type,
    rsp_id,
    COUNT(*)                                          AS total_activations,
    SUM(CASE WHEN sla_met THEN 1 ELSE 0 END)          AS sla_met_count,
    ROUND(AVG(days_to_activate), 2)                   AS avg_days_to_activate,
    ROUND(SUM(CASE WHEN sla_met THEN 1.0 ELSE 0.0 END) / COUNT(*) * 100, 2)
                                                      AS sla_compliance_pct
FROM WHOLESALE.RSP_ACTIVATIONS
WHERE run_date BETWEEN DATEADD(week, -1, '{{ ds }}'::DATE) AND '{{ ds }}'::DATE
  AND status = 'ACTIVE'
GROUP BY 1, 2, 3
"""

SQL_FAULT_METRICS = """
CREATE OR REPLACE TEMPORARY TABLE tmp_accc_faults AS
SELECT
    i.state_territory,
    i.technology_type,
    i.rsp_id,
    COUNT(*)                                                          AS total_faults,
    SUM(CASE WHEN DATEDIFF('day',i.opened_at,i.resolved_at) <= 2 THEN 1 ELSE 0 END)
                                                                      AS resolved_2bd,
    SUM(CASE WHEN DATEDIFF('day',i.opened_at,i.resolved_at) <= 5 THEN 1 ELSE 0 END)
                                                                      AS resolved_5bd,
    SUM(CASE WHEN DATEDIFF('day',i.opened_at,i.resolved_at) <= 20 THEN 1 ELSE 0 END)
                                                                      AS resolved_20bd
FROM OPERATIONS.OUTAGE_INCIDENTS i
WHERE i.resolved_at BETWEEN DATEADD(week, -1, '{{ ds }}'::DATE) AND '{{ ds }}'::DATE
GROUP BY 1, 2, 3
"""

SQL_MSF_METRICS = """
CREATE OR REPLACE TEMPORARY TABLE tmp_accc_msf AS
SELECT
    state_territory,
    COUNT(*)                AS msf_event_count,
    SUM(premises_count)     AS total_affected_premises,
    AVG(DATEDIFF('minute', opened_at, resolved_at))  AS avg_resolution_mins
FROM OPERATIONS.OUTAGE_INCIDENTS
WHERE msf_eligible = TRUE
  AND opened_at BETWEEN DATEADD(week, -1, '{{ ds }}'::DATE) AND '{{ ds }}'::DATE
GROUP BY state_territory
"""

SQL_CVC_CONGESTION = """
CREATE OR REPLACE TEMPORARY TABLE tmp_accc_cvc AS
SELECT
    p.state_territory,
    c.poi_id,
    COUNT(DISTINCT c.hour_label)   AS congested_hours,
    MAX(c.utilisation_pct)         AS peak_utilisation_pct
FROM NETWORK.CVC_UTILISATION_HOURLY c
JOIN NETWORK.POI_REGISTRY           p ON p.poi_id = c.poi_id
WHERE c.congestion_flag = TRUE
  AND c.hour_label BETWEEN DATEADD(week, -1, '{{ ds }}'::DATE) AND '{{ ds }}'::DATE
GROUP BY 1, 2
"""


def merge_and_publish(**context) -> None:
    """Build all 4 ACCC metric temp tables and load to COMPLIANCE.ACCC_METRICS.

    All SQL runs in a SINGLE Snowflake session so that TEMPORARY TABLEs created
    by the metric SQLs are visible when the INSERT INTO runs.
    Previously these were 4 separate SnowflakeOperator tasks (each in its own
    session), which caused the temp tables to be invisible to this function.
    """
    run_date = get_run_date(context)
    hook     = get_snowflake_hook()

    # Render {{ ds }} Jinja placeholder (not available in PythonOperator callables)
    def render(sql: str) -> str:
        return sql.replace("{{ ds }}", run_date)

    insert_sql = f"""
        INSERT INTO COMPLIANCE.ACCC_METRICS
        SELECT
            '{run_date}'::DATE AS report_date,
            a.state_territory, a.technology_type, a.rsp_id,
            a.total_activations, a.sla_met_count, a.sla_compliance_pct, a.avg_days_to_activate,
            f.total_faults, f.resolved_2bd, f.resolved_5bd, f.resolved_20bd,
            m.msf_event_count, m.total_affected_premises,
            v.congested_hours, v.peak_utilisation_pct
        FROM tmp_accc_activations a
        LEFT JOIN tmp_accc_faults    f ON (f.state_territory=a.state_territory AND f.technology_type=a.technology_type AND f.rsp_id=a.rsp_id)
        LEFT JOIN tmp_accc_msf       m ON m.state_territory=a.state_territory
        LEFT JOIN tmp_accc_cvc       v ON v.state_territory=a.state_territory
    """

    conn   = hook.get_conn()
    cursor = conn.cursor()
    try:
        for label, sql in [
            ("activation_metrics",  SQL_ACTIVATION_METRICS),
            ("fault_metrics",       SQL_FAULT_METRICS),
            ("msf_metrics",         SQL_MSF_METRICS),
            ("cvc_congestion",      SQL_CVC_CONGESTION),
        ]:
            cursor.execute(render(sql))
            log.info("Executed %s temp table (session shared)", label)

        cursor.execute(insert_sql)
        conn.commit()
        log.info("ACCC metrics loaded for %s", run_date)
    finally:
        cursor.close()
        conn.close()


def export_submission_files(**context) -> None:
    """Generate anonymised JSON + XLSX files for ACCC submission."""
    import openpyxl
    run_date  = get_run_date(context)
    week_num  = datetime.strptime(run_date, "%Y-%m-%d").strftime("%Y-W%V")
    hook      = get_snowflake_hook()

    df = hook.get_pandas_df("""
        SELECT report_date, state_territory, technology_type,
               ROW_NUMBER() OVER (ORDER BY rsp_id) AS rsp_anon_id,  -- anonymise RSP
               total_activations, sla_compliance_pct,
               total_faults, resolved_2bd, resolved_5bd,
               msf_event_count, total_affected_premises,
               congested_hours, peak_utilisation_pct
        FROM COMPLIANCE.ACCC_METRICS
        WHERE report_date = %(d)s
    """, parameters={"d": run_date})

    with tempfile.TemporaryDirectory() as tmpdir:
        # JSON submission
        json_path = f"{tmpdir}/accc_report.json"
        df.to_json(json_path, orient="records", indent=2)
        upload_to_s3(json_path, f"compliance/accc/{week_num}/accc_report.json")

        # XLSX human-readable
        xlsx_path = f"{tmpdir}/accc_report.xlsx"
        df.to_excel(xlsx_path, index=False, sheet_name="ACCC Weekly Metrics")
        upload_to_s3(xlsx_path, f"compliance/accc/{week_num}/accc_report.xlsx")

    log.info("ACCC submission files exported for week %s", week_num)


with DAG(
    dag_id="nnn_accc_reporting_weekly",
    description="Weekly ACCC regulatory performance report under WBA monitoring framework",
    schedule_interval="0 19 * * 3",    # Wednesday 19:00 UTC = Thursday 05:00 AEST
    start_date=datetime(2024, 1, 4),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "compliance", "regulatory", "accc", "weekly"],
) as dag:

    # merge_and_publish now builds all 4 metric temp tables AND runs the INSERT in
    # a single Snowflake session — no separate tg_build TaskGroup needed.
    t_merge  = PythonOperator(task_id="build_metrics_and_publish", python_callable=merge_and_publish,       sla=timedelta(hours=5))
    t_export = PythonOperator(task_id="export_submission_files",   python_callable=export_submission_files, sla=timedelta(hours=8))

    t_merge >> t_export
