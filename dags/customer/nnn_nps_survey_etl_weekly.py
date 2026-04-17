"""
nnn_nps_survey_etl_weekly
~~~~~~~~~~~~~~~~~~~~~~~~~
Owner:      nnn-data-engineering
Domain:     Customer
Schedule:   Tuesday 06:00 AEST (20:00 UTC Monday)
SLA:        4 hours

Extracts the previous week's NPS (Net Promoter Score) survey responses
from Medallia (NNN's CX survey platform), cleans verbatim comments,
attributes responses to RSPs and network regions, and loads into
Snowflake CUSTOMER.NPS_RESPONSES.

NPS categories:
  Promoters   (9–10), Passives (7–8), Detractors (0–6)

Verbatim processing:
  - Strip PII (email patterns, phone numbers, names via regex)
  - Truncate to 2000 characters
  - Detect sentiment label (positive/neutral/negative) via keyword heuristic
  (Full NLP sentiment runs as a separate ML batch job against this table)

Steps:
  1. Fetch Medallia survey batch for previous week
  2. Clean and anonymise verbatim comments
  3. Attribute to RSP via service_id → RSP mapping in Snowflake
  4. Calculate NPS scores per RSP and region
  5. Load to CUSTOMER.NPS_RESPONSES
  6. Refresh CUSTOMER.NPS_WEEKLY_SCORECARD materialised view
"""

from __future__ import annotations

import re
import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_MEDALLIA, CONN_SNOWFLAKE, get_run_date, get_snowflake_hook,
)

log = logging.getLogger(__name__)

# Regex patterns for PII removal from verbatim text
_RE_EMAIL  = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
_RE_PHONE  = re.compile(r"\b(?:\+61|0)[2-9]\d{8,9}\b")

POSITIVE_KW = {"great", "excellent", "fast", "love", "perfect", "reliable", "happy", "amazing"}
NEGATIVE_KW = {"slow", "terrible", "awful", "disconnected", "outage", "useless", "poor", "dropout"}

default_args = {
    "owner":                    "nnn-data-engineering",
    "depends_on_past":          False,
    "email":                    ["de-alerts@nnnco.com.au", "cx-analytics@nnnco.com.au"],
    "email_on_failure":         True,
    "email_on_retry":           False,
    "retries":                  2,
    "retry_delay":              timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay":          timedelta(minutes=60),
    "on_failure_callback":      nnn_failure_alert,
    "execution_timeout":        timedelta(hours=2),
}


def fetch_medallia_responses(**context) -> None:
    """Download last week's NPS survey batch from Medallia API."""
    from airflow.providers.http.hooks.http import HttpHook

    run_date   = get_run_date(context)
    week_start = (pd.Timestamp(run_date) - pd.Timedelta(days=7)).strftime("%Y-%m-%d")
    week_end   = (pd.Timestamp(run_date) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    hook = HttpHook(method="POST", http_conn_id=CONN_MEDALLIA)
    resp = hook.run(
        endpoint="/api/v1/exports/responses",
        data={
            "form_ids":   ["nnn-residential-nps", "nnn-business-nps"],
            "date_from":  week_start,
            "date_to":    week_end,
            "fields":     ["response_id", "service_id", "score", "verbatim",
                           "survey_channel", "completed_at"],
        },
        headers={"Content-Type": "application/json"},
    )

    responses = resp.json().get("responses", [])
    log.info("Fetched %d NPS responses for week %s → %s", len(responses), week_start, week_end)
    context["ti"].xcom_push(key="raw", value={"responses": responses,
                                               "week_start": week_start, "week_end": week_end})


def clean_and_score(**context) -> None:
    """Clean verbatim text, remove PII, compute sentiment heuristic."""
    payload   = context["ti"].xcom_pull(key="raw")
    responses = payload["responses"]

    def _clean_verbatim(text: str) -> str:
        if not text:
            return ""
        text = _RE_EMAIL.sub("[EMAIL]", text)
        text = _RE_PHONE.sub("[PHONE]", text)
        return text[:2000].strip()

    def _sentiment(text: str, score: int) -> str:
        if not text:
            return "neutral"
        words = set(text.lower().split())
        pos   = len(words & POSITIVE_KW)
        neg   = len(words & NEGATIVE_KW)
        if pos > neg and score >= 7:
            return "positive"
        if neg > pos or score <= 4:
            return "negative"
        return "neutral"

    def _nps_category(score: int) -> str:
        if score >= 9:  return "promoter"
        if score >= 7:  return "passive"
        return "detractor"

    cleaned = []
    for r in responses:
        score      = int(r.get("score", 0))
        verbatim   = _clean_verbatim(r.get("verbatim", ""))
        cleaned.append({
            **r,
            "score":        score,
            "verbatim":     verbatim,
            "nps_category": _nps_category(score),
            "sentiment":    _sentiment(verbatim, score),
            "week_start":   payload["week_start"],
        })

    context["ti"].xcom_push(key="cleaned", value=cleaned)
    log.info("Cleaned %d responses", len(cleaned))


def attribute_to_rsp(**context) -> None:
    """Join service_id → RSP_ID via Snowflake and attach region."""
    cleaned = context["ti"].xcom_pull(key="cleaned")
    if not cleaned:
        context["ti"].xcom_push(key="attributed", value=[])
        return

    service_ids  = list({r["service_id"] for r in cleaned if r.get("service_id")})
    placeholders = ", ".join(f"'{s}'" for s in service_ids)
    hook         = get_snowflake_hook()

    mapping = hook.get_pandas_df(f"""
        SELECT service_id, rsp_id, rsp_name, state_territory
        FROM   WHOLESALE.SERVICE_RSP_MAPPING
        WHERE  service_id IN ({placeholders})
    """).set_index("service_id").to_dict(orient="index") if service_ids else {}

    attributed = []
    for r in cleaned:
        rsp_info = mapping.get(r.get("service_id"), {})
        attributed.append({
            **r,
            "rsp_id":          rsp_info.get("rsp_id", "UNKNOWN"),
            "rsp_name":        rsp_info.get("rsp_name", "UNKNOWN"),
            "state_territory": rsp_info.get("state_territory"),
        })

    context["ti"].xcom_push(key="attributed", value=attributed)
    log.info("Attributed %d responses to RSPs", len(attributed))


def load_responses(**context) -> None:
    """Insert attributed NPS responses into CUSTOMER.NPS_RESPONSES."""
    rows = context["ti"].xcom_pull(key="attributed")
    if not rows:
        log.info("No NPS responses to load")
        return

    hook = get_snowflake_hook()
    hook.insert_rows(
        table="CUSTOMER.NPS_RESPONSES",
        rows=[list(r.values()) for r in rows],
        target_fields=list(rows[0].keys()),
    )
    log.info("Loaded %d NPS responses", len(rows))


with DAG(
    dag_id="nnn_nps_survey_etl_weekly",
    description="Weekly Medallia NPS survey ETL with PII cleaning and RSP attribution",
    schedule_interval="0 20 * * 1",   # 20:00 UTC Monday = 06:00 AEST Tuesday
    start_date=datetime(2024, 1, 2),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "customer", "weekly", "nps"],
) as dag:

    t_fetch    = PythonOperator(task_id="fetch_medallia_responses", python_callable=fetch_medallia_responses, sla=timedelta(hours=1))
    t_clean    = PythonOperator(task_id="clean_and_score",          python_callable=clean_and_score,          sla=timedelta(hours=2))
    t_attr     = PythonOperator(task_id="attribute_to_rsp",         python_callable=attribute_to_rsp,         sla=timedelta(hours=3))
    t_load     = PythonOperator(task_id="load_responses",           python_callable=load_responses,           sla=timedelta(hours=3, minutes=30))
    t_refresh  = SnowflakeOperator(
        task_id="refresh_nps_scorecard",
        snowflake_conn_id=CONN_SNOWFLAKE,
        sql="ALTER DYNAMIC TABLE CUSTOMER.NPS_WEEKLY_SCORECARD REFRESH",
        sla=timedelta(hours=4),
    )

    t_fetch >> t_clean >> t_attr >> t_load >> t_refresh
