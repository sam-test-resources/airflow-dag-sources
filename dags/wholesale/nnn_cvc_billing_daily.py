"""
nnn_cvc_billing_daily
~~~~~~~~~~~~~~~~~~~~~
Owner:      nnn-data-engineering / Wholesale Finance
Domain:     Wholesale / Finance
Schedule:   05:00 AEST daily (19:00 UTC)
SLA:        4 hours  ← BILLING CRITICAL (P1 PagerDuty on SLA miss)

Calculates daily CVC (Connectivity Virtual Circuit) billing lines for each
RSP (Retail Service Provider) based on their peak throughput usage against
their contracted CVC tier.

NNN CVC pricing model:
  - RSPs purchase CVC in Gbps increments per POI
  - Billing is based on the 95th-percentile (P95) throughput in the month
  - Daily pipeline accumulates hourly samples; month-end invoice uses the
    rolling P95 across all daily peaks (see nnn_wholesale_invoice_monthly)

Steps:
  1. Calculate peak CVC per RSP per POI for the previous day (from hourly data)
  2. Join with RSP contracted tiers to determine overage
  3. Apply daily rate card (contracted + overage per Gbps)
  4. Write billing lines to Snowflake FINANCE.CVC_BILLING_LINES
  5. Write summary to Oracle EBS AR staging (for integration)
  6. Assert zero null RSP_IDs (any null means an unmapped POI — alert immediately)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_ORACLE_EBS, CONN_SNOWFLAKE, get_run_date, get_snowflake_hook,
)

log = logging.getLogger(__name__)

default_args = {
    "owner":                    "nnn-data-engineering",
    "depends_on_past":          True,          # daily billing is cumulative
    "email":                    ["de-alerts@nnnco.com.au", "wholesale-finance@nnnco.com.au"],
    "email_on_failure":         True,
    "email_on_retry":           False,
    "retries":                  3,
    "retry_delay":              timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":          timedelta(minutes=30),
    "on_failure_callback":      nnn_failure_alert,
    "execution_timeout":        timedelta(hours=2),
}

SQL_DAILY_PEAK = """
CREATE OR REPLACE TEMPORARY TABLE tmp_daily_peak AS
SELECT
    h.poi_id,
    m.rsp_id,
    m.rsp_name,
    %(run_date)s::DATE                                   AS billing_date,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY h.throughput_gbps)
                                                         AS p95_throughput_gbps,
    MAX(h.throughput_gbps)                               AS peak_throughput_gbps,
    COUNT(*)                                             AS sample_count
FROM NETWORK.CVC_UTILISATION_HOURLY h
JOIN WHOLESALE.POI_RSP_MAPPING      m ON m.poi_id = h.poi_id
WHERE DATE(h.hour_label) = %(run_date)s
GROUP BY h.poi_id, m.rsp_id, m.rsp_name
"""


def calculate_billing_lines(**context) -> None:
    """Join daily peaks with rate card and compute billing amounts."""
    run_date = get_run_date(context)
    hook     = get_snowflake_hook()

    # Load daily peak data
    hook.run(SQL_DAILY_PEAK, parameters={"run_date": run_date})

    # Load rate card for this billing period
    rate_df = hook.get_pandas_df("""
        SELECT rsp_id, contracted_gbps, contracted_rate_per_gbps, overage_rate_per_gbps
        FROM   WHOLESALE.CVC_RATE_CARDS
        WHERE  effective_from <= %(d)s
          AND  (effective_to IS NULL OR effective_to >= %(d)s)
    """, parameters={"d": run_date})

    peak_df = hook.get_pandas_df("SELECT * FROM tmp_daily_peak")

    # Integrity check: every POI must map to an RSP
    null_rsp = peak_df["rsp_id"].isna().sum()
    if null_rsp > 0:
        raise ValueError(f"BILLING INTEGRITY FAILURE: {null_rsp} POIs have no RSP mapping!")

    # Merge and calculate
    merged = peak_df.merge(rate_df, on="rsp_id", how="left")
    merged["contracted_gbps"]       = merged["contracted_gbps"].fillna(0)
    merged["overage_gbps"]          = (merged["p95_throughput_gbps"] - merged["contracted_gbps"]).clip(lower=0)
    merged["contracted_charge"]     = merged["contracted_gbps"] * merged["contracted_rate_per_gbps"]
    merged["overage_charge"]        = merged["overage_gbps"] * merged["overage_rate_per_gbps"]
    merged["total_daily_charge"]    = (merged["contracted_charge"] + merged["overage_charge"]).round(4)
    merged["billing_date"]          = run_date

    context["ti"].xcom_push(key="billing_lines", value=merged.to_json(orient="records"))
    log.info("Calculated %d CVC billing lines for %s; total charge: $%.2f AUD",
             len(merged), run_date, merged["total_daily_charge"].sum())


def load_billing_to_snowflake(**context) -> None:
    """Insert billing lines into FINANCE.CVC_BILLING_LINES."""
    lines = pd.read_json(context["ti"].xcom_pull(key="billing_lines"), orient="records")
    hook  = get_snowflake_hook()

    hook.insert_rows(
        table="FINANCE.CVC_BILLING_LINES",
        rows=lines.values.tolist(),
        target_fields=list(lines.columns),
    )
    log.info("Loaded %d billing lines to Snowflake", len(lines))


def validate_no_null_rsp(**context) -> None:
    """Raise immediately if any CVC billing line has a null RSP_ID.

    Using a SnowflakeOperator with SELECT ... HAVING is a no-op — the operator
    marks the task as succeeded even when the query returns rows.  A PythonOperator
    that inspects the result set is required to actually fail the task.
    """
    run_date = get_run_date(context)
    hook     = get_snowflake_hook()
    row = hook.get_first("""
        SELECT COUNT(*) FROM FINANCE.CVC_BILLING_LINES
        WHERE billing_date = %(d)s AND rsp_id IS NULL
    """, parameters={"d": run_date})
    null_count = row[0] if row else 0
    if null_count > 0:
        raise ValueError(
            f"BILLING INTEGRITY FAILURE: {null_count} CVC billing lines for "
            f"{run_date} have NULL rsp_id — every POI must map to an RSP."
        )
    log.info("Null RSP validation passed for billing_date=%s", run_date)


def push_to_oracle_ebs(**context) -> None:
    """Write daily billing summary to Oracle EBS AR staging for upstream processing."""
    from airflow.providers.oracle.hooks.oracle import OracleHook

    lines    = pd.read_json(context["ti"].xcom_pull(key="billing_lines"), orient="records")
    run_date = get_run_date(context)

    # Aggregate to RSP level for EBS staging (POI-level detail stays in Snowflake)
    summary = lines.groupby("rsp_id").agg(
        total_daily_charge=("total_daily_charge", "sum"),
        poi_count=("poi_id", "count"),
    ).reset_index()

    hook = OracleHook(oracle_conn_id=CONN_ORACLE_EBS)
    for _, row in summary.iterrows():
        hook.run(
            """
            INSERT INTO AR.CVC_BILLING_STAGING
                (rsp_id, billing_date, total_charge_aud, poi_count, loaded_at)
            VALUES (:1, TO_DATE(:2,'YYYY-MM-DD'), :3, :4, SYSDATE)
            """,
            parameters=(row["rsp_id"], run_date, float(row["total_daily_charge"]), int(row["poi_count"])),
        )

    hook.run("COMMIT")
    log.info("Pushed %d RSP billing summaries to Oracle EBS AR staging", len(summary))


with DAG(
    dag_id="nnn_cvc_billing_daily",
    description="Daily CVC peak throughput billing lines → Snowflake + Oracle EBS staging",
    schedule_interval="0 19 * * *",     # 05:00 AEST = 19:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "wholesale", "finance", "daily", "billing"],
) as dag:

    t_calc = PythonOperator(
        task_id="calculate_billing_lines",
        python_callable=calculate_billing_lines,
        sla=timedelta(hours=2),
    )

    t_load_sf = PythonOperator(
        task_id="load_billing_to_snowflake",
        python_callable=load_billing_to_snowflake,
        sla=timedelta(hours=3),
    )

    t_push_ebs = PythonOperator(
        task_id="push_to_oracle_ebs",
        python_callable=push_to_oracle_ebs,
        sla=timedelta(hours=3, minutes=30),
    )

    t_validate = PythonOperator(
        task_id="validate_no_null_rsp",
        python_callable=validate_no_null_rsp,
        sla=timedelta(hours=4),
    )

    t_calc >> t_load_sf >> t_push_ebs >> t_validate
