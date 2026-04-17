"""
nnn_premises_activation_etl
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Owner:      nnn-data-engineering
Domain:     Field Operations / Infrastructure
Schedule:   01:30 AEST daily (15:30 UTC)
SLA:        4 hours

Incremental ETL of NNN premises records from Oracle OSS (Operational Support
System) into Snowflake INFRASTRUCTURE.PREMISES.

A "premise" is a unique addressable location to which NNN can deliver service.
Premises have lifecycle states: PASSED → CONNECTED → ACTIVE → CHURNED.

Processing steps:
  1. Extract new/updated premises from OSS since the last watermark
  2. Standardise addresses using GNAF (Geocoded National Address File) matching
  3. Enrich with technology type from the PON/node topology (Postgres NI)
  4. Classify premises type (Residential/SMB/Enterprise/MDU) using unit/lot heuristics
  5. MERGE into Snowflake INFRASTRUCTURE.PREMISES
  6. Update rollout progress counters in INFRASTRUCTURE.ROLLOUT_METRICS

Upstream: Oracle OSS, Postgres Network Inventory, GNAF reference data (S3)
Downstream: nnn_service_qualification_sync, RSP premises reporting
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_ORACLE_OSS, CONN_POSTGRES_NI, CONN_SNOWFLAKE,
    get_run_date, get_snowflake_hook, assert_row_count,
)

log = logging.getLogger(__name__)

# Unit/lot patterns for MDU (Multi Dwelling Unit) classification
_MDU_PATTERNS = re.compile(r"\b(unit|apt|apartment|flat|lvl|level|lot)\b", re.IGNORECASE)

PREMISES_TYPE_MAP = {
    "RES": "RESIDENTIAL",
    "SMB": "SMB",
    "ENT": "ENTERPRISE",
    "GOV": "GOVERNMENT",
    "MDU": "MDU",
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
    "execution_timeout":        timedelta(hours=2),
}


def extract_from_oss(**context) -> None:
    """Incremental extract of premises from Oracle OSS since last watermark."""
    from airflow.providers.oracle.hooks.oracle import OracleHook

    run_date = get_run_date(context)
    hook     = get_snowflake_hook()

    # Get watermark
    wm_row  = hook.get_first("""
        SELECT TO_CHAR(last_run_ts, 'YYYY-MM-DD HH24:MI:SS')
        FROM OPERATIONS.ETL_WATERMARKS WHERE dag_id = 'nnn_premises_activation_etl'
    """)
    watermark = wm_row[0] if wm_row else f"{run_date} 00:00:00"

    oss_hook = OracleHook(oracle_conn_id=CONN_ORACLE_OSS)
    df = oss_hook.get_pandas_df("""
        SELECT
            PREMISES_ID, ADDRESS_ID, GNAF_PID,
            STREET_NUMBER, STREET_NAME, SUBURB, STATE_CODE, POSTCODE,
            PREMISES_TYPE_CODE, LIFECYCLE_STATE,
            TECHNOLOGY_CODE, PON_ID, NODE_ID,
            PASS_DATE, CONNECT_DATE, ACTIVATE_DATE,
            LAST_UPDATED_AT
        FROM PREMISES.PREMISES_MASTER
        WHERE LAST_UPDATED_AT > TO_TIMESTAMP(:wm,'YYYY-MM-DD HH24:MI:SS')
        ORDER BY LAST_UPDATED_AT
    """, parameters={"wm": watermark})

    df.columns = [c.lower() for c in df.columns]
    log.info("Extracted %d premises updated since %s", len(df), watermark)
    context["ti"].xcom_push(key="raw_json", value=df.to_json(orient="records"))


def standardise_and_enrich(**context) -> None:
    """Clean addresses, classify premises type, and enrich with PON topology."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    df = pd.read_json(context["ti"].xcom_pull(key="raw_json"), orient="records")
    if df.empty:
        context["ti"].xcom_push(key="enriched_json", value="[]")
        return

    # Classify MDU vs residential from address string
    def classify(row):
        if row["premises_type_code"] in PREMISES_TYPE_MAP:
            return PREMISES_TYPE_MAP[row["premises_type_code"]]
        full_addr = f"{row['street_number']} {row['street_name']}"
        return "MDU" if _MDU_PATTERNS.search(full_addr) else "RESIDENTIAL"

    df["premises_type"] = df.apply(classify, axis=1)

    # Enrich with PON technology from Network Inventory
    pg_hook  = PostgresHook(postgres_conn_id=CONN_POSTGRES_NI)
    pon_ids  = df["pon_id"].dropna().unique().tolist()
    if pon_ids:
        placeholders = ",".join(f"'{p}'" for p in pon_ids)
        pon_df = pg_hook.get_pandas_df(
            f"SELECT pon_id, technology_type, distribution_area FROM network.pon_registry "
            f"WHERE pon_id IN ({placeholders})"
        )
        df = df.merge(pon_df, on="pon_id", how="left")
    else:
        df["technology_type"] = df["technology_code"]
        df["distribution_area"] = None

    # Standardise state codes
    state_map = {"NSW": "NSW", "VIC": "VIC", "QLD": "QLD", "SA": "SA",
                 "WA": "WA", "TAS": "TAS", "NT": "NT", "ACT": "ACT"}
    df["state_code"] = df["state_code"].str.upper().map(state_map).fillna("UNK")

    context["ti"].xcom_push(key="enriched_json", value=df.to_json(orient="records"))
    log.info("Enriched %d premises records", len(df))


def merge_to_snowflake(**context) -> None:
    """MERGE enriched premises into INFRASTRUCTURE.PREMISES."""
    df = pd.read_json(context["ti"].xcom_pull(key="enriched_json"), orient="records")
    if df.empty:
        log.info("No premises to merge")
        return

    hook = get_snowflake_hook()
    hook.run("CREATE OR REPLACE TEMPORARY TABLE tmp_premises LIKE INFRASTRUCTURE.PREMISES")
    hook.insert_rows(
        table="tmp_premises",
        rows=df.values.tolist(),
        target_fields=list(df.columns),
    )
    hook.run("""
        MERGE INTO INFRASTRUCTURE.PREMISES tgt
        USING tmp_premises src ON tgt.premises_id = src.premises_id
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    log.info("MERGE complete: %d premises rows", len(df))


def update_watermark(**context) -> None:
    hook = get_snowflake_hook()
    hook.run("""
        MERGE INTO OPERATIONS.ETL_WATERMARKS tgt
        USING (SELECT 'nnn_premises_activation_etl' AS dag_id, CURRENT_TIMESTAMP() AS ts) src
          ON tgt.dag_id = src.dag_id
        WHEN MATCHED     THEN UPDATE SET last_run_ts = src.ts
        WHEN NOT MATCHED THEN INSERT (dag_id, last_run_ts) VALUES (src.dag_id, src.ts)
    """)


with DAG(
    dag_id="nnn_premises_activation_etl",
    description="Daily incremental premises ETL from Oracle OSS with GNAF enrichment",
    schedule_interval="30 15 * * *",    # 01:30 AEST = 15:30 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "infrastructure", "field-ops", "daily"],
) as dag:

    t_extract  = PythonOperator(task_id="extract_from_oss",          python_callable=extract_from_oss,          sla=timedelta(hours=1))
    t_enrich   = PythonOperator(task_id="standardise_and_enrich",     python_callable=standardise_and_enrich,    sla=timedelta(hours=2))
    t_merge    = PythonOperator(task_id="merge_to_snowflake",         python_callable=merge_to_snowflake,        sla=timedelta(hours=3))
    t_rollout  = SnowflakeOperator(
        task_id="refresh_rollout_metrics",
        snowflake_conn_id=CONN_SNOWFLAKE,
        sql="""
            MERGE INTO INFRASTRUCTURE.ROLLOUT_METRICS tgt
            USING (
                SELECT state_code, technology_type, lifecycle_state, COUNT(*) AS premises_count
                FROM   INFRASTRUCTURE.PREMISES
                GROUP  BY 1, 2, 3
            ) src ON (tgt.state_code=src.state_code AND tgt.technology_type=src.technology_type
                      AND tgt.lifecycle_state=src.lifecycle_state)
            WHEN MATCHED THEN UPDATE SET premises_count=src.premises_count, refreshed_at=CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT *
        """,
        sla=timedelta(hours=4),
    )
    t_wm = PythonOperator(task_id="update_watermark", python_callable=update_watermark, trigger_rule="all_done")

    t_extract >> t_enrich >> t_merge >> t_rollout >> t_wm
