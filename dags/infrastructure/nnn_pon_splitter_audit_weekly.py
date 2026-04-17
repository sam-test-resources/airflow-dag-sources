"""
nnn_pon_splitter_audit_weekly
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Owner:      nnn-data-engineering / Network Engineering
Domain:     Infrastructure
Schedule:   Sunday 01:00 AEST (Saturday 15:00 UTC)
SLA:        6 hours

Weekly audit of the Passive Optical Network (PON) splitter plant and
FTTC Remote Integrated Multiplexer (RIM) cabinet utilisation.

Context:
  NNN's FTTP network uses PON splitters to connect multiple premises
  to a single optical fibre. Each splitter serves 1:32 or 1:64 premises.
  A "hot" splitter (>85% ports used) must be remediated before new activations
  can proceed in that distribution area.

  This DAG identifies:
    1. Hot splitters needing expansion (>85% utilisation)
    2. Cold splitters that could be consolidated (< 20% utilisation)
    3. RIM cabinets requiring copper/DSL capacity upgrades (>90% port use)
    4. Premises in rollout priority zones where splitter capacity is insufficient
       for planned activations over the next 90 days

Outputs:
  - Snowflake INFRASTRUCTURE.PON_AUDIT_WEEKLY   (primary record)
  - Snowflake INFRASTRUCTURE.SPLITTER_REMEDIATION_QUEUE (actionable work items)
  - S3: infrastructure/pon_audit/YYYY-WW/audit_report.xlsx (for Network Engineering)

Sources:
  - Postgres (network_inventory): splitter registry, RIM registry, port assignments
  - S3: field_survey/pon_capacity/ (weekly field survey data dumps)
  - Snowflake: INFRASTRUCTURE.PREMISES (planned activations)
"""

from __future__ import annotations

import logging
import tempfile
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_POSTGRES_NI, CONN_S3,
    NNN_S3_BUCKET, get_run_date, get_snowflake_hook, upload_to_s3,
)

log = logging.getLogger(__name__)

HOT_THRESHOLD  = 85.0   # % utilisation — flag for expansion
COLD_THRESHOLD = 20.0   # % utilisation — flag for consolidation
RIM_THRESHOLD  = 90.0   # % port utilisation for RIM cabinets
FORECAST_DAYS  = 90     # days ahead for planned activation demand

default_args = {
    "owner":                    "nnn-data-engineering",
    "depends_on_past":          False,
    "email":                    ["de-alerts@nnnco.com.au", "network-engineering@nnnco.com.au"],
    "email_on_failure":         True,
    "email_on_retry":           False,
    "retries":                  2,
    "retry_delay":              timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay":          timedelta(minutes=60),
    "on_failure_callback":      nnn_failure_alert,
    "execution_timeout":        timedelta(hours=3),
}


def extract_postgres_inventory(**context) -> None:
    """Pull splitter and RIM registry from Network Inventory Postgres."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=CONN_POSTGRES_NI)

    # PON splitters
    splitter_df = hook.get_pandas_df("""
        SELECT
            s.splitter_id, s.splitter_type, s.split_ratio,
            s.distribution_area_id, s.olt_id, s.olt_port,
            s.total_ports,
            COUNT(DISTINCT p.port_id) FILTER (WHERE p.status = 'ACTIVE') AS active_ports,
            COUNT(DISTINCT p.port_id) FILTER (WHERE p.status = 'RESERVED') AS reserved_ports,
            s.state_territory, s.latitude, s.longitude
        FROM network.pon_splitters s
        LEFT JOIN network.splitter_ports p ON p.splitter_id = s.splitter_id
        WHERE s.active = TRUE
        GROUP BY s.splitter_id, s.splitter_type, s.split_ratio,
                 s.distribution_area_id, s.olt_id, s.olt_port,
                 s.total_ports, s.state_territory, s.latitude, s.longitude
    """)

    # RIM cabinets
    rim_df = hook.get_pandas_df("""
        SELECT
            r.rim_id, r.rim_type, r.cabinet_id,
            r.total_dsl_ports,
            COUNT(DISTINCT c.port_id) FILTER (WHERE c.status = 'ACTIVE') AS active_dsl_ports,
            r.state_territory, r.distribution_area_id
        FROM network.rim_cabinets r
        LEFT JOIN network.rim_ports c ON c.rim_id = r.rim_id
        WHERE r.active = TRUE
        GROUP BY r.rim_id, r.rim_type, r.cabinet_id, r.total_dsl_ports,
                 r.state_territory, r.distribution_area_id
    """)

    context["ti"].xcom_push(key="splitter_json", value=splitter_df.to_json(orient="records"))
    context["ti"].xcom_push(key="rim_json",      value=rim_df.to_json(orient="records"))
    log.info("Extracted %d splitters and %d RIM cabinets", len(splitter_df), len(rim_df))


def load_field_survey_data(**context) -> None:
    """Read the latest field survey capacity dump from S3."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    run_date = get_run_date(context)
    hook     = S3Hook(aws_conn_id=CONN_S3)

    # Find most recent weekly survey file
    keys = sorted(hook.list_keys(
        bucket_name=NNN_S3_BUCKET,
        prefix="field_survey/pon_capacity/",
    ), reverse=True)

    if not keys:
        log.warning("No field survey files found — proceeding with inventory data only")
        context["ti"].xcom_push(key="survey_json", value="[]")
        return

    latest_key = keys[0]
    raw        = hook.read_key(latest_key, bucket_name=NNN_S3_BUCKET)
    survey_df  = pd.read_json(raw)

    log.info("Loaded field survey data from %s (%d records)", latest_key, len(survey_df))
    context["ti"].xcom_push(key="survey_json", value=survey_df.to_json(orient="records"))


def calculate_utilisation_and_flags(**context) -> None:
    """Compute utilisation %, flag hot/cold/RIM-critical items."""
    run_date    = get_run_date(context)
    splitter_df = pd.read_json(context["ti"].xcom_pull(key="splitter_json"), orient="records")
    rim_df      = pd.read_json(context["ti"].xcom_pull(key="rim_json"),      orient="records")

    # Splitter utilisation
    splitter_df["utilisation_pct"] = (
        (splitter_df["active_ports"] + splitter_df["reserved_ports"])
        / splitter_df["total_ports"] * 100
    ).round(2)
    splitter_df["flag"] = splitter_df["utilisation_pct"].apply(
        lambda u: "HOT" if u > HOT_THRESHOLD else ("COLD" if u < COLD_THRESHOLD else "OK")
    )

    # RIM cabinet utilisation
    rim_df["utilisation_pct"] = (
        rim_df["active_dsl_ports"] / rim_df["total_dsl_ports"] * 100
    ).round(2)
    rim_df["flag"] = rim_df["utilisation_pct"].apply(
        lambda u: "CRITICAL" if u > RIM_THRESHOLD else "OK"
    )

    splitter_df["run_date"] = run_date
    rim_df["run_date"]      = run_date

    hot_count  = (splitter_df["flag"] == "HOT").sum()
    cold_count = (splitter_df["flag"] == "COLD").sum()
    rim_crit   = (rim_df["flag"] == "CRITICAL").sum()

    log.info("Splitter audit: %d hot, %d cold, %d RIM critical",
             hot_count, cold_count, rim_crit)

    context["ti"].xcom_push(key="splitter_result", value=splitter_df.to_json(orient="records"))
    context["ti"].xcom_push(key="rim_result",      value=rim_df.to_json(orient="records"))


def load_audit_results(**context) -> None:
    """Load splitter and RIM results into Snowflake."""
    splitter_df = pd.read_json(context["ti"].xcom_pull(key="splitter_result"), orient="records")
    rim_df      = pd.read_json(context["ti"].xcom_pull(key="rim_result"),      orient="records")
    hook        = get_snowflake_hook()

    if not splitter_df.empty:
        hook.insert_rows(
            table="INFRASTRUCTURE.PON_AUDIT_WEEKLY",
            rows=splitter_df.values.tolist(),
            target_fields=list(splitter_df.columns),
        )

    # Upsert remediation queue for HOT splitters and CRITICAL RIMs
    hot_splitters = splitter_df[splitter_df["flag"] == "HOT"]
    rim_critical  = rim_df[rim_df["flag"] == "CRITICAL"]

    remediation = []
    for _, row in hot_splitters.iterrows():
        remediation.append({
            "asset_id":   row["splitter_id"], "asset_type": "PON_SPLITTER",
            "flag":       "HOT", "utilisation_pct": row["utilisation_pct"],
            "state":      row["state_territory"], "run_date": row["run_date"],
            "priority":   "HIGH" if row["utilisation_pct"] > 95 else "MEDIUM",
        })
    for _, row in rim_critical.iterrows():
        remediation.append({
            "asset_id": row["rim_id"], "asset_type": "RIM_CABINET",
            "flag": "CRITICAL", "utilisation_pct": row["utilisation_pct"],
            "state": row["state_territory"], "run_date": row["run_date"],
            "priority": "HIGH",
        })

    if remediation:
        hook.insert_rows(
            table="INFRASTRUCTURE.SPLITTER_REMEDIATION_QUEUE",
            rows=[list(r.values()) for r in remediation],
            target_fields=list(remediation[0].keys()),
        )
    log.info("Loaded %d remediation items", len(remediation))


def export_xlsx_report(**context) -> None:
    """Generate weekly PON audit Excel report for Network Engineering."""
    run_date    = get_run_date(context)
    week_num    = datetime.strptime(run_date, "%Y-%m-%d").strftime("%Y-W%V")
    splitter_df = pd.read_json(context["ti"].xcom_pull(key="splitter_result"), orient="records")
    rim_df      = pd.read_json(context["ti"].xcom_pull(key="rim_result"),      orient="records")

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp_path = tmp.name

    with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
        splitter_df.to_excel(writer, sheet_name="PON Splitters",   index=False)
        rim_df.to_excel(writer,      sheet_name="RIM Cabinets",    index=False)
        splitter_df[splitter_df["flag"] == "HOT"].to_excel(
            writer, sheet_name="HOT Splitters (Action Required)", index=False
        )

    s3_key = f"infrastructure/pon_audit/{week_num}/audit_report.xlsx"
    upload_to_s3(tmp_path, s3_key)
    log.info("PON audit XLSX uploaded: %s", s3_key)


with DAG(
    dag_id="nnn_pon_splitter_audit_weekly",
    description="Weekly PON splitter and RIM cabinet utilisation audit with remediation queue",
    schedule_interval="0 15 * * 6",    # Saturday 15:00 UTC = Sunday 01:00 AEST
    start_date=datetime(2024, 1, 7),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "infrastructure", "weekly", "pon", "fttp"],
) as dag:

    with TaskGroup("extract") as tg_extract:
        t_pg     = PythonOperator(task_id="extract_postgres_inventory",  python_callable=extract_postgres_inventory, sla=timedelta(hours=1))
        t_survey = PythonOperator(task_id="load_field_survey_data",      python_callable=load_field_survey_data,     sla=timedelta(hours=1))

    t_calc   = PythonOperator(task_id="calculate_utilisation_and_flags", python_callable=calculate_utilisation_and_flags, sla=timedelta(hours=2))
    t_load   = PythonOperator(task_id="load_audit_results",             python_callable=load_audit_results,         sla=timedelta(hours=4))
    t_export = PythonOperator(task_id="export_xlsx_report",             python_callable=export_xlsx_report,         sla=timedelta(hours=5))

    tg_extract >> t_calc >> t_load >> t_export
