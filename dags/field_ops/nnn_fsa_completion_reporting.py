"""
nnn_fsa_completion_reporting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Owner:      nnn-data-engineering / Field Operations
Domain:     Field Operations
Schedule:   07:00 AEST daily (21:00 UTC prev day)
SLA:        2 hours (ops leadership needs reports by 09:00 AEST standup)

Calculates daily field service appointment (FSA) performance metrics
from ServiceNow FSA completion records and loads into Snowflake
FIELD_OPS.FSA_PERFORMANCE. Triggers a PowerBI dataset refresh at the end.

Key metrics:
  - First-time completion rate (FTC): resolved without follow-up visit
  - On-time arrival rate: technician arrived within ±30 min of appointment window
  - Average job duration by work order type
  - RNC (Right-first-time, No-fault-found, Cancelled) breakdown
  - Technician-level performance ranking

CSG (Customer Service Guarantee) eligibility:
  - Missed appointments (technician no-show) are flagged for CSG compensation
  - These feed into nnn_sla_compliance_daily for penalty calculation

Steps:
  1. Extract yesterday's completed FSAs from ServiceNow
  2. Calculate FTC, on-time, duration metrics
  3. Flag CSG-eligible missed appointments
  4. Load to Snowflake FIELD_OPS.FSA_PERFORMANCE
  5. Upsert CSG flags into COMPLIANCE.FSA_CSG_EVENTS
  6. Trigger PowerBI dataset refresh via REST API
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import CONN_SERVICENOW, CONN_POWERBI, get_run_date, get_snowflake_hook

log = logging.getLogger(__name__)

default_args = {
    "owner":                    "nnn-data-engineering",
    "depends_on_past":          False,
    "email":                    ["de-alerts@nnnco.com.au", "field-ops-analytics@nnnco.com.au"],
    "email_on_failure":         True,
    "email_on_retry":           False,
    "retries":                  3,
    "retry_delay":              timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay":          timedelta(minutes=15),
    "on_failure_callback":      nnn_failure_alert,
    "execution_timeout":        timedelta(hours=1),
}


def extract_fsa_records(**context) -> None:
    """Pull yesterday's completed FSA records from ServiceNow."""
    from airflow.providers.http.hooks.http import HttpHook

    run_date = get_run_date(context)
    hook     = HttpHook(method="GET", http_conn_id=CONN_SERVICENOW)

    resp = hook.run(
        endpoint="/api/now/table/sn_wo_task",
        data={
            "sysparm_query": (
                f"completed_date={run_date}^state=3"    # state=3 = Closed
                "^ORstate=4"                             # state=4 = Cancelled
            ),
            "sysparm_fields": (
                "sys_id,number,parent.number,type,technician_id,"
                "appointment_window_start,appointment_window_end,"
                "technician_arrived_at,completed_at,state,"
                "ftc_flag,rnc_code,job_outcome_code,premises_id"
            ),
            "sysparm_limit": 2000,
        },
        headers={"Accept": "application/json"},
    )
    records = resp.json().get("result", [])
    log.info("Extracted %d FSA records for %s", len(records), run_date)
    context["ti"].xcom_push(key="raw", value=records)


def calculate_metrics(**context) -> None:
    """Compute FTC rate, on-time arrival, duration, and CSG flags."""
    records  = context["ti"].xcom_pull(key="raw")
    run_date = get_run_date(context)

    if not records:
        context["ti"].xcom_push(key="metrics_json", value="[]")
        context["ti"].xcom_push(key="csg_events", value=[])
        return

    df = pd.DataFrame(records)
    df.columns = [c.split(".")[0].lower() if "." not in c else c.replace(".", "_").lower()
                  for c in df.columns]

    # Parse timestamps
    for col in ["appointment_window_start", "appointment_window_end",
                "technician_arrived_at", "completed_at"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # On-time arrival: arrived within 30 min of window start (before window end)
    df["arrived_within_window"] = (
        (df["technician_arrived_at"] <= df["appointment_window_end"]) &
        (df["technician_arrived_at"] >= df["appointment_window_start"] - timedelta(minutes=30))
    )

    # Job duration in minutes
    df["duration_mins"] = (
        (df["completed_at"] - df["technician_arrived_at"]).dt.total_seconds() / 60
    ).round(1)

    # CSG: technician no-show (state=4 Cancelled AND arrived_at is null AND window has passed)
    df["csg_eligible"] = (
        (df["state"] == "4") &
        df["technician_arrived_at"].isna() &
        (df["appointment_window_end"] < pd.Timestamp(run_date + " 23:59:59"))
    )

    df["run_date"] = run_date

    # CSG events for compliance table
    csg_df = df[df["csg_eligible"]][["sys_id", "premises_id", "appointment_window_start",
                                      "appointment_window_end", "run_date"]]

    context["ti"].xcom_push(key="metrics_json", value=df.to_json(orient="records"))
    context["ti"].xcom_push(key="csg_events", value=csg_df.to_dict(orient="records"))

    # Log summary
    total      = len(df)
    ftc_rate   = df["ftc_flag"].astype(str).eq("true").mean() if "ftc_flag" in df else 0
    ontime_rate = df["arrived_within_window"].mean()
    log.info("FSA metrics: total=%d FTC=%.1f%% on-time=%.1f%% CSG_eligible=%d",
             total, ftc_rate * 100, ontime_rate * 100, csg_df.shape[0])


def load_fsa_performance(**context) -> None:
    """Write FSA performance rows to FIELD_OPS.FSA_PERFORMANCE."""
    df   = pd.read_json(context["ti"].xcom_pull(key="metrics_json"), orient="records")
    hook = get_snowflake_hook()

    if df.empty:
        log.info("No FSA rows to load")
        return

    hook.insert_rows(
        table="FIELD_OPS.FSA_PERFORMANCE",
        rows=df.values.tolist(),
        target_fields=list(df.columns),
    )
    log.info("Loaded %d FSA performance rows", len(df))


def upsert_csg_events(**context) -> None:
    """Insert CSG-eligible missed appointments into COMPLIANCE.FSA_CSG_EVENTS."""
    events = context["ti"].xcom_pull(key="csg_events")
    if not events:
        log.info("No CSG events today")
        return

    hook = get_snowflake_hook()
    hook.insert_rows(
        table="COMPLIANCE.FSA_CSG_EVENTS",
        rows=[list(e.values()) for e in events],
        target_fields=list(events[0].keys()),
    )
    log.warning("Inserted %d CSG-eligible missed appointments", len(events))


with DAG(
    dag_id="nnn_fsa_completion_reporting",
    description="Daily FSA completion metrics with FTC rate, on-time arrival, and CSG flagging",
    schedule_interval="0 21 * * *",     # 07:00 AEST = 21:00 UTC prev day
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "field-ops", "daily", "compliance"],
) as dag:

    t_extract  = PythonOperator(task_id="extract_fsa_records",    python_callable=extract_fsa_records,    sla=timedelta(minutes=20))
    t_calc     = PythonOperator(task_id="calculate_metrics",      python_callable=calculate_metrics,      sla=timedelta(minutes=40))
    t_load     = PythonOperator(task_id="load_fsa_performance",   python_callable=load_fsa_performance,   sla=timedelta(hours=1))
    t_csg      = PythonOperator(task_id="upsert_csg_events",      python_callable=upsert_csg_events,      sla=timedelta(hours=1, minutes=15))

    # Trigger PowerBI premium dataset refresh via REST (fire-and-forget style)
    t_powerbi  = SimpleHttpOperator(
        task_id="trigger_powerbi_refresh",
        http_conn_id=CONN_POWERBI,
        endpoint="/v1.0/myorg/groups/{{ var.value.powerbi_fsa_workspace_id }}/datasets/{{ var.value.powerbi_fsa_dataset_id }}/refreshes",
        method="POST",
        headers={"Content-Type": "application/json"},
        data="{}",
        response_check=lambda resp: resp.status_code == 202,
        log_response=True,
    )

    t_extract >> t_calc >> t_load >> t_csg >> t_powerbi
