"""
nnn_rsp_reconciliation_weekly
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Owner:      nnn-data-engineering / Wholesale Finance
Domain:     Wholesale
Schedule:   Saturday 02:00 AEST (Friday 16:00 UTC)
SLA:        6 hours

Cross-system reconciliation of RSP service orders between Oracle EBS
(the billing system of record) and Salesforce (the RSP relationship CRM).

Discrepancies types detected:
  - GHOST:   service exists in EBS billing but not in Salesforce CRM
  - ORPHAN:  service in Salesforce but no billing record in EBS
  - MISMATCH: matching order_id but different RSP attribution, product, or status
  - RATE_ERR: CVC rate applied in billing differs from contracted rate in Salesforce

Discrepancies above threshold (>50 per RSP) trigger an email to the
wholesale account manager for that RSP.

Outputs:
  - Snowflake WHOLESALE.RSP_RECONCILIATION  (full reconciliation result)
  - Snowflake WHOLESALE.RECON_DISCREPANCIES (flagged rows only)
  - Email report to wholesale-ops@nnnco.com.au with summary table
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_ORACLE_EBS, CONN_SALESFORCE,
    get_run_date, get_snowflake_hook,
)

log = logging.getLogger(__name__)

DISCREPANCY_ALERT_THRESHOLD = 50

default_args = {
    "owner":                    "nnn-data-engineering",
    "depends_on_past":          False,
    "email":                    ["de-alerts@nnnco.com.au"],
    "email_on_failure":         True,
    "email_on_retry":           False,
    "retries":                  2,
    "retry_delay":              timedelta(minutes=15),
    "retry_exponential_backoff": True,
    "max_retry_delay":          timedelta(minutes=60),
    "on_failure_callback":      nnn_failure_alert,
    "execution_timeout":        timedelta(hours=3),
}


def extract_ebs_orders(**context) -> None:
    """Pull active service orders from Oracle EBS billing."""
    from airflow.providers.oracle.hooks.oracle import OracleHook
    run_date = get_run_date(context)

    hook = OracleHook(oracle_conn_id=CONN_ORACLE_EBS)
    df   = hook.get_pandas_df("""
        SELECT
            ORDER_ID,
            SERVICE_ID,
            RSP_ID,
            PRODUCT_CODE,
            BILLING_STATUS,
            CVC_GBPS_CONTRACTED,
            RATE_PER_GBPS,
            EFFECTIVE_FROM,
            EFFECTIVE_TO
        FROM AR.SERVICE_BILLING_LINES
        WHERE BILLING_STATUS IN ('ACTIVE','SUSPENDED')
          AND TRUNC(EFFECTIVE_FROM) <= TO_DATE(:run_date,'YYYY-MM-DD')
    """, parameters={"run_date": run_date})

    df.columns = [c.lower() for c in df.columns]
    log.info("EBS orders extracted: %d rows", len(df))
    context["ti"].xcom_push(key="ebs_df", value=df.to_json(orient="records"))


def extract_salesforce_orders(**context) -> None:
    """Pull CRM service records from Salesforce."""
    from simple_salesforce import Salesforce
    from airflow.hooks.base import BaseHook

    creds = BaseHook.get_connection(CONN_SALESFORCE)
    sf    = Salesforce(username=creds.login, password=creds.password,
                       security_token=creds.extra_dejson.get("security_token"),
                       domain="nnnco.my")

    result = sf.query_all("""
        SELECT Id, NNN_Order_ID__c, Service_ID__c, RSP_Account__r.RSP_Code__c,
               Product_Code__c, Status__c, CVC_Contracted_Gbps__c, Rate_Per_Gbps__c
        FROM NNN_Service_Order__c
        WHERE Status__c IN ('Active','Suspended')
    """)

    records = result.get("records", [])
    df      = pd.DataFrame(records).rename(columns={
        "NNN_Order_ID__c":    "order_id",
        "Service_ID__c":      "service_id",
        "Product_Code__c":    "product_code",
        "Status__c":          "status",
        "CVC_Contracted_Gbps__c": "cvc_gbps_contracted",
        "Rate_Per_Gbps__c":   "rate_per_gbps",
    })
    df["rsp_id"] = df["RSP_Account__r"].apply(lambda x: x.get("RSP_Code__c") if x else None)

    log.info("Salesforce orders extracted: %d rows", len(df))
    context["ti"].xcom_push(key="sf_df", value=df.to_json(orient="records"))


def reconcile(**context) -> None:
    """Cross-match EBS and Salesforce records and classify discrepancies."""
    ebs_df = pd.read_json(context["ti"].xcom_pull(key="ebs_df"), orient="records")
    sf_df  = pd.read_json(context["ti"].xcom_pull(key="sf_df"),  orient="records")

    ebs_ids = set(ebs_df["order_id"])
    sf_ids  = set(sf_df["order_id"])

    discrepancies = []

    # GHOST: in EBS but not Salesforce
    for oid in ebs_ids - sf_ids:
        row = ebs_df[ebs_df["order_id"] == oid].iloc[0]
        discrepancies.append({**row.to_dict(), "discrepancy_type": "GHOST", "source": "EBS_ONLY"})

    # ORPHAN: in Salesforce but not EBS
    for oid in sf_ids - ebs_ids:
        row = sf_df[sf_df["order_id"] == oid].iloc[0]
        discrepancies.append({**row.to_dict(), "discrepancy_type": "ORPHAN", "source": "SF_ONLY"})

    # MISMATCH / RATE_ERR: in both but different values
    common_ids = ebs_ids & sf_ids
    ebs_map = ebs_df.set_index("order_id").to_dict(orient="index")
    sf_map  = sf_df.set_index("order_id").to_dict(orient="index")

    for oid in common_ids:
        e = ebs_map[oid]; s = sf_map[oid]
        if e.get("rsp_id") != s.get("rsp_id") or e.get("product_code") != s.get("product_code"):
            discrepancies.append({**e, "discrepancy_type": "MISMATCH", "sf_rsp_id": s.get("rsp_id")})
        elif abs((e.get("rate_per_gbps") or 0) - (s.get("rate_per_gbps") or 0)) > 0.01:
            discrepancies.append({**e, "discrepancy_type": "RATE_ERR",
                                   "ebs_rate": e.get("rate_per_gbps"), "sf_rate": s.get("rate_per_gbps")})

    log.info("Reconciliation: %d discrepancies found "
             "(ghost=%d, orphan=%d, mismatch=%d, rate_err=%d)",
             len(discrepancies),
             sum(1 for d in discrepancies if d["discrepancy_type"] == "GHOST"),
             sum(1 for d in discrepancies if d["discrepancy_type"] == "ORPHAN"),
             sum(1 for d in discrepancies if d["discrepancy_type"] == "MISMATCH"),
             sum(1 for d in discrepancies if d["discrepancy_type"] == "RATE_ERR"))

    context["ti"].xcom_push(key="discrepancies", value=discrepancies)
    context["ti"].xcom_push(key="discrepancy_count", value=len(discrepancies))


def load_reconciliation(**context) -> None:
    run_date      = get_run_date(context)
    discrepancies = context["ti"].xcom_pull(key="discrepancies")
    hook          = get_snowflake_hook()

    if discrepancies:
        hook.insert_rows(
            table="WHOLESALE.RECON_DISCREPANCIES",
            rows=[[run_date] + list(d.values()) for d in discrepancies],
            target_fields=["recon_date"] + list(discrepancies[0].keys()),
        )
    log.info("Loaded %d discrepancy rows", len(discrepancies))


def branch_on_threshold(**context) -> str:
    count = context["ti"].xcom_pull(key="discrepancy_count")
    return "send_alert_email" if count >= DISCREPANCY_ALERT_THRESHOLD else "no_alert"


with DAG(
    dag_id="nnn_rsp_reconciliation_weekly",
    description="Weekly cross-system RSP order reconciliation (Oracle EBS vs Salesforce)",
    schedule_interval="0 16 * * 5",    # Friday 16:00 UTC = Saturday 02:00 AEST
    start_date=datetime(2024, 1, 6),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "wholesale", "weekly", "reconciliation"],
) as dag:

    t_ebs  = PythonOperator(task_id="extract_ebs_orders",        python_callable=extract_ebs_orders,        sla=timedelta(hours=1))
    t_sf   = PythonOperator(task_id="extract_salesforce_orders",  python_callable=extract_salesforce_orders, sla=timedelta(hours=1))
    t_rec  = PythonOperator(task_id="reconcile",                  python_callable=reconcile,                 sla=timedelta(hours=3))
    t_load = PythonOperator(task_id="load_reconciliation",        python_callable=load_reconciliation,       sla=timedelta(hours=4))
    t_branch = BranchPythonOperator(task_id="branch_on_threshold", python_callable=branch_on_threshold)

    t_alert = EmailOperator(
        task_id="send_alert_email",
        to=["wholesale-ops@nnnco.com.au"],
        subject="[NNN] Weekly RSP Reconciliation — Discrepancies Require Attention ({{ ds }})",
        html_content="""
            <p>The weekly RSP reconciliation run ({{ ds }}) found
            {{ ti.xcom_pull(key='discrepancy_count') }} discrepancies
            above the threshold of """ + str(DISCREPANCY_ALERT_THRESHOLD) + """.</p>
            <p>Please review <strong>WHOLESALE.RECON_DISCREPANCIES</strong> in Snowflake.</p>
        """,
    )
    t_no_alert = DummyOperator(task_id="no_alert")

    [t_ebs, t_sf] >> t_rec >> t_load >> t_branch >> [t_alert, t_no_alert]
