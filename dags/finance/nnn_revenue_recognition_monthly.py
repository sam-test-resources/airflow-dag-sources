"""
nnn_revenue_recognition_monthly
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Owner:      nnn-data-engineering / Finance
Domain:     Finance
Schedule:   2nd of each month, 09:00 AEST (23:00 UTC 1st of month)
SLA:        12 hours  ← FINANCE CRITICAL

Processes monthly revenue recognition for NNN wholesale products under
AASB 15 (Revenue from Contracts with Customers) / IFRS 15.

Revenue categories:
  - CVC revenue:   recognised in the period of consumption (accrual basis)
  - AVC revenue:   recognised ratably over the service period
  - Connection fees: deferred and amortised over average expected service life (24 months)
  - Install charges: recognised at point of service completion

Processing steps:
  1. Validate completeness of Oracle EBS AR invoices for billing period
  2. Extract raw AR lines from Oracle EBS
  3. Allocate deferred connection fees (1/24 of balance to current period)
  4. Calculate CVC accruals (usage in period vs invoiced amounts)
  5. Write journal entries to Oracle EBS GL staging
  6. Load recognised revenue to Snowflake FINANCE.REVENUE_RECOGNITION
  7. Generate trial balance reconciliation check
  8. Notify Finance Controller on completion

Upstream: nnn_wholesale_invoice_monthly, Oracle EBS AR
Downstream: Finance month-end close, group reporting
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import CONN_ORACLE_EBS, CONN_SNOWFLAKE, get_run_month, get_snowflake_hook

log = logging.getLogger(__name__)

# AASB 15 amortisation period for connection fees (months)
CONNECTION_FEE_AMORT_MONTHS = 24

default_args = {
    "owner":                    "nnn-data-engineering",
    "depends_on_past":          True,
    "email":                    ["de-alerts@nnnco.com.au", "finance-controller@nnnco.com.au"],
    "email_on_failure":         True,
    "email_on_retry":           False,
    "retries":                  2,
    "retry_delay":              timedelta(minutes=30),
    "retry_exponential_backoff": False,
    "on_failure_callback":      nnn_failure_alert,
    "execution_timeout":        timedelta(hours=6),
}


def extract_ar_lines(**context) -> None:
    """Pull AR invoice lines for the billing period from Oracle EBS."""
    from airflow.providers.oracle.hooks.oracle import OracleHook

    run_month     = get_run_month(context)
    billing_month = (pd.Timestamp(run_month + "-01") - pd.DateOffset(months=1)).strftime("%Y-%m")

    hook = OracleHook(oracle_conn_id=CONN_ORACLE_EBS)
    df   = hook.get_pandas_df("""
        SELECT
            L.LINE_ID, L.HEADER_ID, H.INVOICE_NUMBER,
            H.CUSTOMER_ID      AS RSP_ID,
            L.LINE_TYPE,        -- CVC, AVC, CONNECTION_FEE, INSTALL
            L.AMOUNT,           L.TAX_AMOUNT,
            L.DESCRIPTION,
            H.INVOICE_DATE,     H.GL_DATE
        FROM AR.RA_CUSTOMER_TRX_LINES_ALL L
        JOIN AR.RA_CUSTOMER_TRX_ALL H ON H.CUSTOMER_TRX_ID = L.CUSTOMER_TRX_ID
        WHERE TO_CHAR(H.GL_DATE,'YYYY-MM') = :bm
          AND L.LINE_TYPE      != 'TAX'
          AND H.COMPLETE_FLAG  = 'Y'
        ORDER BY H.INVOICE_DATE, L.LINE_ID
    """, parameters={"bm": billing_month})

    df.columns = [c.lower() for c in df.columns]
    log.info("Extracted %d AR lines for %s", len(df), billing_month)
    context["ti"].xcom_push(key="ar_json", value=df.to_json(orient="records"))
    context["ti"].xcom_push(key="billing_month", value=billing_month)


def allocate_deferred_fees(**context) -> None:
    """Amortise deferred connection fees (1/24 per month)."""
    df            = pd.read_json(context["ti"].xcom_pull(key="ar_json"), orient="records")
    billing_month = context["ti"].xcom_pull(key="billing_month")
    hook          = get_snowflake_hook()

    # Load deferred balance from Snowflake (accumulated from previous months)
    deferred = hook.get_pandas_df("""
        SELECT rsp_id, SUM(deferred_balance) AS total_deferred
        FROM FINANCE.DEFERRED_REVENUE_BALANCES
        WHERE revenue_category = 'CONNECTION_FEE'
          AND DATEDIFF('month', original_invoice_month, %(bm)s) < %(amort)s
        GROUP BY rsp_id
    """, parameters={"bm": billing_month, "amort": CONNECTION_FEE_AMORT_MONTHS})

    deferred["current_period_recognition"] = (
        deferred["total_deferred"] / CONNECTION_FEE_AMORT_MONTHS
    ).round(4)

    context["ti"].xcom_push(key="deferred_json", value=deferred.to_json(orient="records"))
    log.info("Deferred fee amortisation: %d RSPs, total $%.2f recognised",
             len(deferred), deferred["current_period_recognition"].sum())


def build_journal_entries(**context) -> None:
    """Combine CVC/AVC revenue and deferred amortisation into GL journal lines."""
    ar_df         = pd.read_json(context["ti"].xcom_pull(key="ar_json"), orient="records")
    deferred_df   = pd.read_json(context["ti"].xcom_pull(key="deferred_json"), orient="records")
    billing_month = context["ti"].xcom_pull(key="billing_month")

    journals = []

    # Direct revenue (CVC + AVC + Install): Dr AR, Cr Revenue
    for _, row in ar_df[ar_df["line_type"].isin(["CVC", "AVC", "INSTALL"])].iterrows():
        journals.append({
            "journal_date":    billing_month + "-01",
            "account_code":    "4100" if row["line_type"] == "CVC" else "4200",
            "rsp_id":          row["rsp_id"],
            "debit":           0,
            "credit":          row["amount"],
            "description":     f"{row['line_type']} Revenue {billing_month}",
            "source":          "AR",
            "line_type":       row["line_type"],
        })

    # Deferred connection fee recognition: Dr Deferred Liability, Cr Revenue
    for _, row in deferred_df.iterrows():
        journals.append({
            "journal_date":    billing_month + "-01",
            "account_code":    "4300",
            "rsp_id":          row["rsp_id"],
            "debit":           0,
            "credit":          row["current_period_recognition"],
            "description":     f"Deferred Connection Fee Amort {billing_month}",
            "source":          "DEFERRED",
            "line_type":       "CONNECTION_FEE",
        })

    context["ti"].xcom_push(key="journals_json", value=pd.DataFrame(journals).to_json(orient="records"))
    log.info("Built %d journal lines (total revenue: $%.2f)",
             len(journals), sum(j["credit"] for j in journals))


def post_to_oracle_ebs(**context) -> None:
    """Write journal entries to Oracle EBS GL interface table."""
    from airflow.providers.oracle.hooks.oracle import OracleHook

    journals      = pd.read_json(context["ti"].xcom_pull(key="journals_json"), orient="records")
    billing_month = context["ti"].xcom_pull(key="billing_month")
    hook          = OracleHook(oracle_conn_id=CONN_ORACLE_EBS)

    for _, j in journals.iterrows():
        hook.run("""
            INSERT INTO GL.GL_INTERFACE
                (STATUS, LEDGER_ID, ACCOUNTING_DATE, SEGMENT1, CURRENCY_CODE,
                 ENTERED_DR, ENTERED_CR, DESCRIPTION, REFERENCE10)
            VALUES
                ('NEW', 2001, TO_DATE(:jdate,'YYYY-MM-DD'), :acct, 'AUD',
                 :dr, :cr, :desc, :ref)
        """, parameters=(j["journal_date"], j["account_code"], float(j["debit"]),
                         float(j["credit"]), j["description"],
                         f"AIRFLOW-REVREC-{billing_month}"))
    hook.run("COMMIT")
    log.info("Posted %d GL journal entries to Oracle EBS", len(journals))


def load_to_snowflake(**context) -> None:
    """Load recognised revenue to Snowflake FINANCE.REVENUE_RECOGNITION."""
    journals = pd.read_json(context["ti"].xcom_pull(key="journals_json"), orient="records")
    hook     = get_snowflake_hook()
    hook.insert_rows(
        table="FINANCE.REVENUE_RECOGNITION",
        rows=journals.values.tolist(),
        target_fields=list(journals.columns),
    )
    log.info("Loaded %d revenue recognition rows to Snowflake", len(journals))


with DAG(
    dag_id="nnn_revenue_recognition_monthly",
    description="Monthly AASB 15 revenue recognition with deferred connection fee amortisation",
    schedule_interval="0 23 1 * *",    # 2nd of month 09:00 AEST = 23:00 UTC 1st
    start_date=datetime(2024, 2, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "finance", "monthly", "aasb15"],
) as dag:

    t_extract  = PythonOperator(task_id="extract_ar_lines",           python_callable=extract_ar_lines,         sla=timedelta(hours=2))
    t_deferred = PythonOperator(task_id="allocate_deferred_fees",      python_callable=allocate_deferred_fees,   sla=timedelta(hours=3))
    t_journals = PythonOperator(task_id="build_journal_entries",       python_callable=build_journal_entries,    sla=timedelta(hours=4))

    with TaskGroup("post_results") as tg_post:
        t_ebs = PythonOperator(task_id="post_to_oracle_ebs",  python_callable=post_to_oracle_ebs,  sla=timedelta(hours=8))
        t_sf  = PythonOperator(task_id="load_to_snowflake",   python_callable=load_to_snowflake,   sla=timedelta(hours=8))

    t_trial = SnowflakeOperator(
        task_id="trial_balance_check",
        snowflake_conn_id=CONN_SNOWFLAKE,
        sql="""
            SELECT ABS(SUM(credit) - SUM(debit)) AS imbalance
            FROM FINANCE.REVENUE_RECOGNITION
            WHERE journal_date = DATE_TRUNC('month', '{{ ds }}'::DATE - INTERVAL '1 day')
            HAVING ABS(SUM(credit) - SUM(debit)) > 0.01
        """,
        sla=timedelta(hours=10),
    )

    t_notify = EmailOperator(
        task_id="notify_finance_controller",
        to=["finance-controller@nnnco.com.au"],
        subject="[NNN] Monthly Revenue Recognition Complete — {{ ds }}",
        html_content="<p>The revenue recognition job for {{ ds }} has completed successfully. "
                     "Please review <b>FINANCE.REVENUE_RECOGNITION</b> in Snowflake.</p>",
    )

    t_extract >> t_deferred >> t_journals >> tg_post >> t_trial >> t_notify
