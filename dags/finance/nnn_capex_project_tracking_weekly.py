"""
nnn_capex_project_tracking_weekly
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Owner:      nnn-data-engineering / Finance
Domain:     Finance / Infrastructure
Schedule:   Monday 04:00 AEST (18:00 UTC Sunday)
SLA:        6 hours

Extracts CAPEX project spend and milestone data from SAP Project Systems (PS)
module, compares actuals vs approved budget, calculates forecast-at-completion,
and loads into Snowflake FINANCE.CAPEX_TRACKING for reporting to the NNN
Board Finance Committee.

Project categories tracked:
  - NETWORK_ROLLOUT  : new build (FTTP upgrade, HFC, FW towers)
  - MAINTENANCE_CAPEX: asset replacement and life extension
  - TECHNOLOGY_UPLIFT: platform/system upgrades
  - CORPORATE_PROJECTS: non-network CAPEX

Budget variance thresholds (trigger email alerts):
  - WARNING:  actual > 90% of approved budget
  - CRITICAL: actual > 100% of approved budget (budget overrun)

Forecast method: Earned Value Management (EVM)
  - SPI (Schedule Performance Index) = EV / PV
  - CPI (Cost Performance Index)     = EV / AC
  - EAC (Estimate at Completion)     = BAC / CPI

Steps:
  1. Extract SAP PS project actuals and budget (REST API via SAP Gateway)
  2. Calculate EVM metrics per project
  3. Flag at-risk and over-budget projects
  4. Merge into Snowflake FINANCE.CAPEX_TRACKING
  5. Email alert for CRITICAL projects
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import CONN_SAP, CONN_SNOWFLAKE, get_run_date, get_snowflake_hook

log = logging.getLogger(__name__)

default_args = {
    "owner":                    "nnn-data-engineering",
    "depends_on_past":          False,
    "email":                    ["de-alerts@nnnco.com.au", "finance-projects@nnnco.com.au"],
    "email_on_failure":         True,
    "email_on_retry":           False,
    "retries":                  2,
    "retry_delay":              timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay":          timedelta(minutes=60),
    "on_failure_callback":      nnn_failure_alert,
    "execution_timeout":        timedelta(hours=3),
}


def extract_sap_capex(**context) -> None:
    """Pull project budget and actuals from SAP PS via OData Gateway."""
    from airflow.providers.http.hooks.http import HttpHook

    run_date = get_run_date(context)
    hook     = HttpHook(method="GET", http_conn_id=CONN_SAP)

    # SAP OData endpoint for project accounting
    resp = hook.run(
        endpoint="/sap/opu/odata/sap/ZPS_PROJECT_ACTUALS_SRV/ProjectSet",
        data={
            "$filter": f"KeyDate le datetime'{run_date}T00:00:00' and Status ne 'CLSD'",
            "$select": (
                "ProjectId,ProjectName,Category,ApprovedBudget,ActualCost,"
                "CommittedCost,PlannedValue,EarnedValue,PlannedCompletion,"
                "ForecastCompletion,ProjectManager,CostCentre"
            ),
            "$format": "json",
        },
        headers={"Accept": "application/json"},
    )

    projects = resp.json().get("d", {}).get("results", [])
    log.info("Extracted %d SAP CAPEX projects as of %s", len(projects), run_date)
    context["ti"].xcom_push(key="sap_projects", value=projects)


def calculate_evm_metrics(**context) -> None:
    """Calculate EVM metrics (SPI, CPI, EAC) and budget risk classification."""
    projects = context["ti"].xcom_pull(key="sap_projects")
    run_date = get_run_date(context)

    if not projects:
        context["ti"].xcom_push(key="capex_json", value="[]")
        return

    df = pd.DataFrame(projects)
    df.columns = [c.lower() for c in df.columns]

    # Convert SAP number strings to float
    for col in ["approvedbudget", "actualcost", "committedcost", "plannedvalue", "earnedvalue"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # EVM calculations
    df["spi"] = (df["earnedvalue"] / df["plannedvalue"].replace(0, float("nan"))).round(4)
    df["cpi"] = (df["earnedvalue"] / df["actualcost"].replace(0, float("nan"))).round(4)
    df["eac"] = (df["approvedbudget"] / df["cpi"].replace(0, float("nan"))).round(2)

    # Budget utilisation %
    df["budget_utilisation_pct"] = (
        df["actualcost"] / df["approvedbudget"].replace(0, float("nan")) * 100
    ).round(2)

    # Risk classification
    def risk_level(row):
        util = row["budget_utilisation_pct"] or 0
        if util > 100:  return "CRITICAL"
        if util > 90:   return "WARNING"
        if util > 75:   return "WATCH"
        return "ON_TRACK"

    df["risk_level"] = df.apply(risk_level, axis=1)
    df["run_date"]   = run_date

    critical_count = (df["risk_level"] == "CRITICAL").sum()
    log.warning("CAPEX tracking: %d projects critical (budget overrun)", critical_count)

    context["ti"].xcom_push(key="capex_json", value=df.to_json(orient="records"))
    context["ti"].xcom_push(key="critical_count", value=int(critical_count))

    if critical_count > 0:
        critical_names = df[df["risk_level"] == "CRITICAL"]["projectname"].tolist()
        context["ti"].xcom_push(key="critical_projects", value=critical_names)


def merge_to_snowflake(**context) -> None:
    """MERGE CAPEX tracking rows into FINANCE.CAPEX_TRACKING."""
    df   = pd.read_json(context["ti"].xcom_pull(key="capex_json"), orient="records")
    hook = get_snowflake_hook()

    if df.empty:
        return

    hook.run("CREATE OR REPLACE TEMPORARY TABLE tmp_capex LIKE FINANCE.CAPEX_TRACKING")
    hook.insert_rows(
        table="tmp_capex",
        rows=df.values.tolist(),
        target_fields=list(df.columns),
    )
    hook.run("""
        MERGE INTO FINANCE.CAPEX_TRACKING tgt
        USING tmp_capex src ON (tgt.projectid = src.projectid AND tgt.run_date = src.run_date)
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    log.info("Merged %d CAPEX rows", len(df))


def branch_critical(**context) -> str:
    return "send_critical_alert" if (context["ti"].xcom_pull(key="critical_count") or 0) > 0 else "skip_alert"


with DAG(
    dag_id="nnn_capex_project_tracking_weekly",
    description="Weekly CAPEX project tracking from SAP PS with EVM metrics and budget alerts",
    schedule_interval="0 18 * * 0",    # Sunday 18:00 UTC = Monday 04:00 AEST
    start_date=datetime(2024, 1, 7),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "finance", "weekly", "capex", "evm"],
) as dag:

    t_extract  = PythonOperator(task_id="extract_sap_capex",      python_callable=extract_sap_capex,      sla=timedelta(hours=1))
    t_evm      = PythonOperator(task_id="calculate_evm_metrics",   python_callable=calculate_evm_metrics,  sla=timedelta(hours=2))
    t_merge    = PythonOperator(task_id="merge_to_snowflake",      python_callable=merge_to_snowflake,     sla=timedelta(hours=4))
    t_refresh  = SnowflakeOperator(
        task_id="refresh_capex_dashboard",
        snowflake_conn_id=CONN_SNOWFLAKE,
        sql="ALTER DYNAMIC TABLE FINANCE.CAPEX_BOARD_SUMMARY REFRESH",
        sla=timedelta(hours=5),
    )
    t_branch   = BranchPythonOperator(task_id="branch_critical",   python_callable=branch_critical)
    t_alert    = EmailOperator(
        task_id="send_critical_alert",
        to=["cfo@nnnco.com.au", "finance-projects@nnnco.com.au"],
        subject="[NNN CRITICAL] CAPEX Budget Overrun Detected — {{ ds }}",
        html_content="<p>The following projects have exceeded their approved budget: "
                     "{{ ti.xcom_pull(key='critical_projects') }}</p>"
                     "<p>Please review <b>FINANCE.CAPEX_TRACKING</b> immediately.</p>",
    )
    t_skip = DummyOperator(task_id="skip_alert")

    t_extract >> t_evm >> t_merge >> t_refresh >> t_branch >> [t_alert, t_skip]
