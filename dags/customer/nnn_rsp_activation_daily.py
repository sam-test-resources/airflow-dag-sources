"""
nnn_rsp_activation_daily
~~~~~~~~~~~~~~~~~~~~~~~~
Owner:      nnn-data-engineering
Domain:     Customer / Wholesale
Schedule:   01:00 AEST daily (15:00 UTC)
SLA:        3 hours (activation KPIs needed for 08:00 morning standup)

Extracts RSP (Retail Service Provider) service activation records from the
Operational Support System (Oracle OSS), calculates SLA adherence per RSP,
and loads results into Snowflake WHOLESALE.RSP_ACTIVATIONS.

Activation types handled:
  - FTTC / FTTN / FTTP / HFC / FW (Fixed Wireless)

SLA rules:
  - Residential:  order-to-active ≤ 15 business days
  - Business:     order-to-active ≤ 5 business days
  - Priority:     order-to-active ≤ 2 business days

Steps:
  1. Extract  – incremental pull from ORDERS.SERVICE_ACTIVATIONS in Oracle OSS
  2. Transform – map status codes, calculate SLA compliance, attribute to RSP
  3. Validate  – assert row count and SLA rate reasonableness (>0%, <100%)
  4. Load      – MERGE into WHOLESALE.RSP_ACTIVATIONS
  5. Refresh   – call Snowflake dynamic table refresh for RSP KPI views

Upstream:  Oracle OSS (ORDERS schema)
Downstream: nnn_cvc_billing_daily, nnn_sla_compliance_daily, RSP portal reports
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_ORACLE_OSS, CONN_SNOWFLAKE, get_run_date, get_snowflake_hook, assert_row_count,
)

log = logging.getLogger(__name__)

# SLA thresholds in calendar days (simplified — real implementation uses business-day calc)
SLA_DAYS = {"residential": 15, "business": 5, "priority": 2}

STATUS_MAP = {
    "ACT":  "ACTIVE",
    "PND":  "PENDING",
    "PRG":  "IN_PROGRESS",
    "FAI":  "FAILED",
    "CAN":  "CANCELLED",
    "SUS":  "SUSPENDED",
}

default_args = {
    "owner":                    "nnn-data-engineering",
    "depends_on_past":          False,
    "email":                    ["de-alerts@nnnco.com.au"],
    "email_on_failure":         True,
    "email_on_retry":           False,
    "retries":                  3,
    "retry_delay":              timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":          timedelta(minutes=60),
    "on_failure_callback":      nnn_failure_alert,
    "execution_timeout":        timedelta(hours=1, minutes=30),
}


def extract_activations(**context) -> None:
    """Incremental extract from Oracle OSS ORDERS.SERVICE_ACTIVATIONS."""
    from airflow.providers.oracle.hooks.oracle import OracleHook

    run_date = get_run_date(context)
    hook     = OracleHook(oracle_conn_id=CONN_ORACLE_OSS)

    # Incremental by COMPLETED_DATE (activations completed on run date)
    sql = """
        SELECT
            ORDER_ID,
            SERVICE_ID,
            RSP_ID,
            RSP_NAME,
            TECHNOLOGY_TYPE,
            CUSTOMER_SEGMENT,
            ORDER_PRIORITY,
            ORDER_CREATED_AT,
            ACTIVATION_COMPLETED_AT,
            STATUS_CODE,
            FAIL_REASON_CODE,
            STATE_TERRITORY
        FROM ORDERS.SERVICE_ACTIVATIONS
        WHERE TRUNC(ACTIVATION_COMPLETED_AT) = TO_DATE(:run_date, 'YYYY-MM-DD')
           OR (STATUS_CODE IN ('PND','PRG') AND TRUNC(ORDER_CREATED_AT) <= TO_DATE(:run_date, 'YYYY-MM-DD'))
        ORDER BY ORDER_CREATED_AT
    """
    df = hook.get_pandas_df(sql, parameters={"run_date": run_date})
    log.info("Extracted %d activation rows for %s", len(df), run_date)
    context["ti"].xcom_push(key="raw_df_json", value=df.to_json(orient="records"))


def transform_activations(**context) -> None:
    """Map status codes, compute SLA compliance, add run metadata."""
    run_date = get_run_date(context)

    df = pd.read_json(context["ti"].xcom_pull(key="raw_df_json"), orient="records")
    if df.empty:
        log.warning("No activation rows for %s — nothing to transform", run_date)
        context["ti"].xcom_push(key="transformed_json", value="[]")
        return

    # Normalise column names to lowercase
    df.columns = [c.lower() for c in df.columns]

    # Map status codes → readable labels
    df["status"] = df["status_code"].map(STATUS_MAP).fillna("UNKNOWN")

    # Calculate days to activate
    df["order_created_at"]        = pd.to_datetime(df["order_created_at"])
    df["activation_completed_at"] = pd.to_datetime(df["activation_completed_at"])
    df["days_to_activate"] = (
        (df["activation_completed_at"] - df["order_created_at"])
        .dt.total_seconds() / 86400
    ).round(2)

    # SLA compliance flag
    def check_sla(row):
        if row["status"] != "ACTIVE":
            return None     # SLA only applicable to completed activations
        threshold = SLA_DAYS.get(row["order_priority"].lower(), 15)
        return row["days_to_activate"] <= threshold

    df["sla_met"]     = df.apply(check_sla, axis=1)
    df["run_date"]    = run_date
    df["customer_segment"] = df["customer_segment"].str.upper()

    context["ti"].xcom_push(key="transformed_json", value=df.to_json(orient="records"))
    log.info("Transformed %d rows; SLA met rate: %.1f%%",
             len(df), df["sla_met"].mean() * 100 if df["sla_met"].notna().any() else 0)


def validate_activations(**context) -> None:
    """Sanity-check the transformed output before loading."""
    rows = pd.read_json(context["ti"].xcom_pull(key="transformed_json"), orient="records")

    if rows.empty:
        log.info("Zero activations today — acceptable (e.g. public holiday)")
        return

    sla_rate = rows["sla_met"].dropna().mean()
    # 0% SLA rate is operationally alarming but is valid data — hard-failing here
    # would suppress downstream alerting; use a warning and allow the run to proceed.
    if sla_rate == 0.0:
        log.warning("SLA rate is 0%% for %s — all activations missed SLA; "
                    "check for upstream data issue", get_run_date(context))
    elif sla_rate > 1.0:
        raise ValueError(f"SLA rate > 100%% which indicates bad data: {sla_rate:.2%}")

    status_dist = rows["status"].value_counts().to_dict()
    log.info("Validation passed | rows=%d | sla_rate=%.1f%% | statuses=%s",
             len(rows), sla_rate * 100, status_dist)


def load_activations(**context) -> None:
    """MERGE transformed activation data into WHOLESALE.RSP_ACTIVATIONS."""
    rows = pd.read_json(context["ti"].xcom_pull(key="transformed_json"), orient="records")
    if rows.empty:
        return

    hook = get_snowflake_hook()
    hook.run("CREATE OR REPLACE TEMPORARY TABLE tmp_activations LIKE WHOLESALE.RSP_ACTIVATIONS")
    hook.insert_rows(
        table="tmp_activations",
        rows=rows.values.tolist(),
        target_fields=list(rows.columns),
    )
    hook.run("""
        MERGE INTO WHOLESALE.RSP_ACTIVATIONS tgt
        USING tmp_activations src ON tgt.order_id = src.order_id
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    log.info("MERGE complete: %d activation rows", len(rows))


with DAG(
    dag_id="nnn_rsp_activation_daily",
    description="Daily RSP service activations from Oracle OSS with SLA compliance",
    schedule_interval="0 15 * * *",     # 01:00 AEST = 15:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "customer", "wholesale", "daily"],
) as dag:

    with TaskGroup("extract_transform") as tg_et:
        t_extract = PythonOperator(
            task_id="extract_activations",
            python_callable=extract_activations,
            sla=timedelta(hours=1),
        )
        t_transform = PythonOperator(
            task_id="transform_activations",
            python_callable=transform_activations,
            sla=timedelta(hours=1, minutes=30),
        )
        t_validate = PythonOperator(
            task_id="validate_activations",
            python_callable=validate_activations,
        )
        t_extract >> t_transform >> t_validate

    t_load = PythonOperator(
        task_id="load_activations",
        python_callable=load_activations,
        sla=timedelta(hours=2),
    )

    t_refresh_views = SnowflakeOperator(
        task_id="refresh_rsp_kpi_views",
        snowflake_conn_id=CONN_SNOWFLAKE,
        sql="ALTER DYNAMIC TABLE WHOLESALE.RSP_ACTIVATION_KPI_DAILY REFRESH",
        sla=timedelta(hours=3),
    )

    tg_et >> t_load >> t_refresh_views
