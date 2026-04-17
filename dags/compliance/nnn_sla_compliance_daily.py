"""
nnn_sla_compliance_daily
~~~~~~~~~~~~~~~~~~~~~~~~~
Owner:      nnn-data-engineering / Regulatory Affairs
Domain:     Compliance
Schedule:   06:00 AEST daily (20:00 UTC)
SLA:        3 hours

Calculates daily SLA compliance across all NNN regulated service metrics
and writes breach records to COMPLIANCE.SLA_BREACHES. Breach records
feed into monthly invoice CSG credit calculations.

Regulated SLA categories (ACCC WBA Schedule 1B):
  - ACT-01: RSP service activation ≤ 15 business days (residential)
  - ACT-02: RSP service activation ≤ 5 business days (business)
  - FLT-01: Fault repair ≤ 2 business days (priority faults)
  - FLT-02: Fault repair ≤ 5 business days (standard faults)
  - APP-01: FSA appointment kept (technician arrives ≤ 30 min late)
  - APP-02: FSA appointment not cancelled < 24h notice

CSG credit amounts (per breach):
  - ACT-01/02: $25 per business day beyond SLA
  - FLT-01/02: $25 per business day beyond SLA
  - APP-01/02: $25 flat per missed/late appointment

Pipeline also detects trending: if a single RSP breaches >100 SLAs in a
rolling 7-day window, a Slack alert is sent to the account management team.

Steps:
  1. Calculate activation SLA breaches (from WHOLESALE.RSP_ACTIVATIONS)
  2. Calculate fault repair SLA breaches (from OPERATIONS.OUTAGE_INCIDENTS)
  3. Calculate FSA appointment SLA breaches (from COMPLIANCE.FSA_CSG_EVENTS)
  4. Compute CSG credit amounts
  5. Load to COMPLIANCE.SLA_BREACHES
  6. Check for RSP trending alerts
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert, nnn_post_slack_message
from nnn_common.utils import CONN_SNOWFLAKE, get_run_date, get_snowflake_hook

log = logging.getLogger(__name__)

CSG_DAILY_RATE     = 25.0   # AUD per breach day
CSG_FLAT_RATE      = 25.0   # AUD flat for appointment breaches

TRENDING_THRESHOLD = 100    # breaches per RSP per rolling 7 days

default_args = {
    "owner":                    "nnn-data-engineering",
    "depends_on_past":          False,
    "email":                    ["de-alerts@nnnco.com.au", "regulatory@nnnco.com.au"],
    "email_on_failure":         True,
    "email_on_retry":           False,
    "retries":                  3,
    "retry_delay":              timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":          timedelta(minutes=30),
    "on_failure_callback":      nnn_failure_alert,
    "execution_timeout":        timedelta(hours=1, minutes=30),
}

SQL_ACTIVATION_BREACHES = """
CREATE OR REPLACE TEMPORARY TABLE tmp_act_breaches AS
SELECT
    order_id,
    service_id,
    rsp_id,
    order_priority,
    days_to_activate,
    CASE order_priority
        WHEN 'priority'     THEN 2
        WHEN 'business'     THEN 5
        ELSE 15
    END                                           AS sla_threshold_days,
    GREATEST(days_to_activate - CASE order_priority
        WHEN 'priority' THEN 2
        WHEN 'business' THEN 5
        ELSE 15 END, 0)                           AS breach_days,
    CASE order_priority
        WHEN 'priority' THEN 'ACT-02'
        WHEN 'business' THEN 'ACT-02'
        ELSE 'ACT-01'
    END                                           AS sla_category,
    %(run_date)s::DATE                            AS breach_date
FROM WHOLESALE.RSP_ACTIVATIONS
WHERE run_date = %(run_date)s
  AND status   = 'ACTIVE'
  AND sla_met  = FALSE
"""

SQL_FAULT_BREACHES = """
CREATE OR REPLACE TEMPORARY TABLE tmp_flt_breaches AS
SELECT
    sys_id              AS incident_id,
    rsp_id,
    priority,
    DATEDIFF('day', opened_at, resolved_at)      AS actual_days,
    CASE priority
        WHEN '1'  THEN 2
        WHEN '2'  THEN 2
        ELSE 5
    END                                          AS sla_threshold_days,
    GREATEST(DATEDIFF('day', opened_at, resolved_at) - CASE priority WHEN '1' THEN 2 WHEN '2' THEN 2 ELSE 5 END, 0)
                                                 AS breach_days,
    CASE priority
        WHEN '1'  THEN 'FLT-01'
        WHEN '2'  THEN 'FLT-01'
        ELSE 'FLT-02'
    END                                          AS sla_category,
    %(run_date)s::DATE                           AS breach_date
FROM OPERATIONS.OUTAGE_INCIDENTS
WHERE DATE(resolved_at) = %(run_date)s
  AND DATEDIFF('day', opened_at, resolved_at) > CASE priority WHEN '1' THEN 2 WHEN '2' THEN 2 ELSE 5 END
"""


def calculate_csg_credits(**context) -> None:
    """Stage breach temp tables and compute CSG credit amounts in a single session.

    SQL_ACTIVATION_BREACHES and SQL_FAULT_BREACHES create TEMPORARY TABLEs.
    Snowflake TEMPORARY TABLEs are session-scoped; they cannot survive across
    Airflow tasks (each task gets its own Snowflake connection).  Both staging
    SQLs and the downstream reads must run in ONE session.
    """
    run_date = get_run_date(context)
    hook     = get_snowflake_hook()

    # ── Stage breach temp tables in the same session ───────────────────────────
    conn   = hook.get_conn()
    cursor = conn.cursor()
    try:
        # Render the %(run_date)s placeholder (SnowflakeOperator params= kwarg is
        # for Jinja context only, not for SQL %s substitution — use cursor directly)
        cursor.execute(SQL_ACTIVATION_BREACHES, {"run_date": run_date})
        cursor.execute(SQL_FAULT_BREACHES,      {"run_date": run_date})
        log.info("Breach staging temp tables created for %s", run_date)

        # ── Read back from temp tables (same session, same cursor) ─────────────
        cursor.execute(
            f"SELECT *, breach_days * {CSG_DAILY_RATE} AS credit_amount_aud FROM tmp_act_breaches"
        )
        act_rows = cursor.fetchall()
        act_cols = [d[0].lower() for d in cursor.description]

        cursor.execute(
            f"SELECT *, breach_days * {CSG_DAILY_RATE} AS credit_amount_aud FROM tmp_flt_breaches"
        )
        flt_rows = cursor.fetchall()
        flt_cols = [d[0].lower() for d in cursor.description]

        conn.commit()
    finally:
        cursor.close()
        conn.close()

    import pandas as pd
    act_df = pd.DataFrame(act_rows, columns=act_cols)
    flt_df = pd.DataFrame(flt_rows, columns=flt_cols)

    # Load FSA appointment breaches (already staged by nnn_fsa_completion_reporting)
    fsa_df = hook.get_pandas_df("""
        SELECT sys_id AS incident_id, NULL AS rsp_id, 'APP-01' AS sla_category,
               %(flat)s AS credit_amount_aud, %(d)s::DATE AS breach_date, 0 AS breach_days
        FROM COMPLIANCE.FSA_CSG_EVENTS
        WHERE run_date = %(d)s
    """, parameters={"flat": CSG_FLAT_RATE, "d": run_date})

    # Combine all breach types
    combined = pd.concat([act_df, flt_df, fsa_df], ignore_index=True)
    combined["run_date"] = run_date

    total_credit = combined["credit_amount_aud"].sum()
    log.info("CSG credits calculated: %d breaches, total $%.2f AUD", len(combined), total_credit)

    context["ti"].xcom_push(key="breaches_json", value=combined.to_json(orient="records"))
    context["ti"].xcom_push(key="total_credit", value=float(total_credit))


def load_sla_breaches(**context) -> None:
    """Insert breach records into COMPLIANCE.SLA_BREACHES."""
    breaches = pd.read_json(context["ti"].xcom_pull(key="breaches_json"), orient="records")
    if breaches.empty:
        log.info("No SLA breaches today")
        return

    hook = get_snowflake_hook()
    hook.insert_rows(
        table="COMPLIANCE.SLA_BREACHES",
        rows=breaches.values.tolist(),
        target_fields=list(breaches.columns),
    )
    log.info("Loaded %d SLA breach records", len(breaches))


def check_trending_alerts(**context) -> None:
    """Fire Slack alert if any RSP exceeds TRENDING_THRESHOLD in rolling 7 days."""

    run_date = get_run_date(context)
    hook     = get_snowflake_hook()

    trending = hook.get_pandas_df("""
        SELECT rsp_id, COUNT(*) AS breach_count_7d
        FROM COMPLIANCE.SLA_BREACHES
        WHERE breach_date BETWEEN DATEADD(day, -6, %(d)s::DATE) AND %(d)s::DATE
          AND rsp_id IS NOT NULL
        GROUP BY rsp_id
        HAVING COUNT(*) >= %(threshold)s
    """, parameters={"d": run_date, "threshold": TRENDING_THRESHOLD})

    if trending.empty:
        log.info("No RSPs trending on SLA breaches")
        return

    for _, row in trending.iterrows():
        msg = (
            f":rotating_light: *NNN SLA Trend Alert*\n"
            f"RSP `{row['rsp_id']}` has *{row['breach_count_7d']} SLA breaches* "
            f"in the last 7 days (threshold: {TRENDING_THRESHOLD})\n"
            f"Review: COMPLIANCE.SLA_BREACHES"
        )
        nnn_post_slack_message(msg)
        log.warning("Trend alert sent for RSP %s (%d breaches/7d)",
                    row["rsp_id"], row["breach_count_7d"])


with DAG(
    dag_id="nnn_sla_compliance_daily",
    description="Daily SLA breach detection and CSG credit calculation across all regulated SLA categories",
    schedule_interval="0 20 * * *",    # 06:00 AEST = 20:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "compliance", "daily", "csg", "sla"],
) as dag:

    # calculate_csg_credits now runs the staging SQLs AND the credit calculation
    # in a single Snowflake session — no separate stage_breaches TaskGroup needed.
    # (Previously the SnowflakeOperator staging tasks passed params= for Jinja
    # context rather than SQL parameterisation, so %(run_date)s was never rendered.)
    t_csg    = PythonOperator(task_id="calculate_csg_credits",  python_callable=calculate_csg_credits,  sla=timedelta(hours=1))
    t_load   = PythonOperator(task_id="load_sla_breaches",      python_callable=load_sla_breaches,      sla=timedelta(hours=2))
    t_trend  = PythonOperator(task_id="check_trending_alerts",  python_callable=check_trending_alerts,  sla=timedelta(hours=3))

    t_csg >> t_load >> t_trend
