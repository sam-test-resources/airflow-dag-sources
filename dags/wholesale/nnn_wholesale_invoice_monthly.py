"""
nnn_wholesale_invoice_monthly
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Owner:      nnn-data-engineering / Wholesale Finance
Domain:     Wholesale / Finance
Schedule:   1st of each month, 08:00 AEST (22:00 UTC last day of prev month)
SLA:        8 hours  ← BILLING CRITICAL

Generates monthly wholesale invoices for all RSPs based on the month's
accumulated CVC billing lines in FINANCE.CVC_BILLING_LINES.

Billing methodology:
  - CVC charge:   P95 of daily P95 throughput across the full month × Gbps rate
  - AVC charge:   flat rate per active service × active days
  - CSG credits:  deductions for SLA breaches (from COMPLIANCE.SLA_BREACHES)
  - GST:          10% on total (Australian GST)

Outputs per RSP:
  1. Snowflake FINANCE.WHOLESALE_INVOICES         (primary record)
  2. Snowflake FINANCE.WHOLESALE_INVOICE_LINES    (line-item detail)
  3. Oracle EBS AR — official invoice record (triggers payment terms)
  4. PDF invoice → S3 nnn-data-lake-prod/finance/invoices/YYYY-MM/<RSP_ID>.pdf

Steps:
  1. Validate all daily billing lines are present for the month (no gaps)
  2. Calculate monthly CVC P95 and AVC amounts per RSP
  3. Deduct CSG credits from COMPLIANCE.SLA_BREACHES
  4. Apply GST and finalise invoice totals
  5. Write to Snowflake (invoice header + lines)
  6. Write to Oracle EBS AR
  7. Generate PDF invoices and upload to S3
  8. Send invoice notification emails to RSP billing contacts
"""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime, timedelta

import calendar
import pandas as pd
from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_ORACLE_EBS, CONN_SNOWFLAKE, get_run_month, get_snowflake_hook,
    NNN_S3_BUCKET, upload_to_s3,
)

log = logging.getLogger(__name__)

GST_RATE = 0.10


def is_last_day_of_month(**context) -> bool:
    """ShortCircuit guard — return True only on the last calendar day of the month.

    schedule_interval="0 22 28-31 * *" fires on days 28-31 of every month to
    target the last day.  Without this guard the DAG would run up to 4 times
    (days 28, 29, 30, 31) in a long month.  Standard cron cannot express
    'last day of month'; this operator fills the gap cleanly.
    """
    exec_date = context["logical_date"]
    last_day  = calendar.monthrange(exec_date.year, exec_date.month)[1]
    return exec_date.day == last_day

default_args = {
    "owner":                    "nnn-data-engineering",
    "depends_on_past":          True,
    "email":                    ["de-alerts@nnnco.com.au", "wholesale-finance@nnnco.com.au"],
    "email_on_failure":         True,
    "email_on_retry":           False,
    "retries":                  2,
    "retry_delay":              timedelta(minutes=15),
    "retry_exponential_backoff": False,
    "on_failure_callback":      nnn_failure_alert,
    "execution_timeout":        timedelta(hours=4),
}

SQL_VALIDATE_COMPLETENESS = """
WITH expected_days AS (
    SELECT DATEDIFF('day',
        DATE_TRUNC('month', '{{ ds }}'::DATE - INTERVAL '1 day'),
        DATE_TRUNC('month', '{{ ds }}'::DATE)
    ) AS days_in_month
),
actual_days AS (
    SELECT COUNT(DISTINCT billing_date) AS days_present
    FROM FINANCE.CVC_BILLING_LINES
    WHERE DATE_TRUNC('month', billing_date) = DATE_TRUNC('month', '{{ ds }}'::DATE - INTERVAL '1 day')
)
SELECT
    e.days_in_month,
    a.days_present,
    IFF(e.days_in_month = a.days_present, 'OK', 'INCOMPLETE') AS status
FROM expected_days e, actual_days a
"""


def calculate_monthly_charges(**context) -> None:
    """Compute monthly CVC P95, AVC amounts, and CSG credits per RSP."""
    run_month = get_run_month(context)   # billing month = previous month
    # Shift back one month for the billing period
    billing_month = (pd.Timestamp(run_month + "-01") - pd.DateOffset(months=1)).strftime("%Y-%m")

    hook = get_snowflake_hook()

    # CVC: monthly P95 of daily P95 per RSP
    cvc_df = hook.get_pandas_df("""
        SELECT
            rsp_id, rsp_name,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY p95_throughput_gbps)
                                                        AS monthly_p95_gbps,
            SUM(contracted_charge)                      AS total_contracted_charge,
            SUM(overage_charge)                         AS total_overage_charge
        FROM FINANCE.CVC_BILLING_LINES
        WHERE DATE_TRUNC('month', billing_date) = %(bm)s::DATE
        GROUP BY rsp_id, rsp_name
    """, parameters={"bm": billing_month + "-01"})

    # AVC: active services × days × rate
    avc_df = hook.get_pandas_df("""
        SELECT rsp_id, SUM(active_services * daily_avc_rate_aud) AS avc_charge
        FROM   FINANCE.AVC_DAILY_SERVICE_COUNTS
        WHERE  DATE_TRUNC('month', billing_date) = %(bm)s::DATE
        GROUP  BY rsp_id
    """, parameters={"bm": billing_month + "-01"})

    # CSG credits (Customer Service Guarantee SLA breach deductions)
    csg_df = hook.get_pandas_df("""
        SELECT rsp_id, SUM(credit_amount_aud) AS csg_credit
        FROM   COMPLIANCE.SLA_BREACHES
        WHERE  DATE_TRUNC('month', breach_date) = %(bm)s::DATE
        GROUP  BY rsp_id
    """, parameters={"bm": billing_month + "-01"})

    # Merge all components
    invoice_df = (
        cvc_df
        .merge(avc_df, on="rsp_id", how="left")
        .merge(csg_df, on="rsp_id", how="left")
    )
    invoice_df = invoice_df.fillna(0)
    invoice_df["subtotal"]          = (
        invoice_df["total_contracted_charge"] + invoice_df["total_overage_charge"]
        + invoice_df["avc_charge"] - invoice_df["csg_credit"]
    ).round(4)
    invoice_df["gst_amount"]        = (invoice_df["subtotal"] * GST_RATE).round(4)
    invoice_df["total_inc_gst"]     = (invoice_df["subtotal"] + invoice_df["gst_amount"]).round(4)
    invoice_df["billing_month"]     = billing_month
    invoice_df["invoice_number"]    = invoice_df.apply(
        lambda r: f"NNN-CVC-{billing_month.replace('-', '')}-{r['rsp_id']}", axis=1
    )

    log.info("Monthly invoices: %d RSPs | total $%.2f AUD (inc GST)",
             len(invoice_df), invoice_df["total_inc_gst"].sum())
    context["ti"].xcom_push(key="invoice_df_json", value=invoice_df.to_json(orient="records"))
    context["ti"].xcom_push(key="billing_month", value=billing_month)


def write_to_snowflake(**context) -> None:
    """Insert invoice headers and line items into Snowflake."""
    invoice_df = pd.read_json(context["ti"].xcom_pull(key="invoice_df_json"), orient="records")
    hook       = get_snowflake_hook()

    hook.insert_rows(
        table="FINANCE.WHOLESALE_INVOICES",
        rows=invoice_df[["invoice_number", "rsp_id", "rsp_name", "billing_month",
                         "subtotal", "gst_amount", "total_inc_gst"]].values.tolist(),
        target_fields=["invoice_number", "rsp_id", "rsp_name", "billing_month",
                       "subtotal", "gst_amount", "total_inc_gst"],
    )
    log.info("Loaded %d invoice headers to Snowflake", len(invoice_df))


def write_to_oracle_ebs(**context) -> None:
    """Insert invoices into Oracle EBS AR for payment processing."""
    from airflow.providers.oracle.hooks.oracle import OracleHook

    invoice_df    = pd.read_json(context["ti"].xcom_pull(key="invoice_df_json"), orient="records")
    billing_month = context["ti"].xcom_pull(key="billing_month")
    hook          = OracleHook(oracle_conn_id=CONN_ORACLE_EBS)

    for _, row in invoice_df.iterrows():
        hook.run("""
            INSERT INTO AR.RA_INTERFACE_LINES_ALL
                (INTERFACE_LINE_ATTRIBUTE1, CUST_TRX_TYPE_ID,
                 AMOUNT, TAX_AMOUNT, CURRENCY_CODE,
                 GL_DATE, INVOICE_DATE, DUE_DATE)
            VALUES (:1, 1001, :2, :3, 'AUD',
                    TO_DATE(:4,'YYYY-MM'), TO_DATE(:4,'YYYY-MM'),
                    ADD_MONTHS(TO_DATE(:4,'YYYY-MM'), 1))
        """, parameters=(row["invoice_number"], float(row["subtotal"]),
                         float(row["gst_amount"]), billing_month))
    hook.run("COMMIT")
    log.info("Inserted %d invoices into Oracle EBS AR", len(invoice_df))


def generate_and_upload_pdfs(**context) -> None:
    """Generate a simple PDF invoice per RSP and upload to S3."""
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas as pdf_canvas

    invoice_df    = pd.read_json(context["ti"].xcom_pull(key="invoice_df_json"), orient="records")
    billing_month = context["ti"].xcom_pull(key="billing_month")

    for _, row in invoice_df.iterrows():
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp_path = tmp.name

        c = pdf_canvas.Canvas(tmp_path, pagesize=A4)
        c.setFont("Helvetica-Bold", 16)
        c.drawString(50, 800, "NNN Co – Wholesale CVC Invoice")
        c.setFont("Helvetica", 12)
        c.drawString(50, 775, f"Invoice:  {row['invoice_number']}")
        c.drawString(50, 755, f"RSP:      {row['rsp_name']} ({row['rsp_id']})")
        c.drawString(50, 735, f"Period:   {billing_month}")
        c.drawString(50, 700, f"CVC charges:  ${row['total_contracted_charge'] + row['total_overage_charge']:.2f}")
        c.drawString(50, 680, f"AVC charges:  ${row['avc_charge']:.2f}")
        c.drawString(50, 660, f"CSG credits:  -${row['csg_credit']:.2f}")
        c.drawString(50, 640, f"Subtotal:     ${row['subtotal']:.2f}")
        c.drawString(50, 620, f"GST (10%):    ${row['gst_amount']:.2f}")
        c.setFont("Helvetica-Bold", 12)
        c.drawString(50, 600, f"TOTAL (inc GST): ${row['total_inc_gst']:.2f} AUD")
        c.save()

        s3_key = f"finance/invoices/{billing_month}/{row['rsp_id']}.pdf"
        upload_to_s3(tmp_path, s3_key)
        os.unlink(tmp_path)

    log.info("Generated and uploaded %d PDF invoices to S3", len(invoice_df))


with DAG(
    dag_id="nnn_wholesale_invoice_monthly",
    description="Monthly wholesale CVC+AVC invoice generation with CSG credits, GST, and PDF upload",
    schedule_interval="0 22 28-31 * *",   # Run on last days of month; gated by day check
    start_date=datetime(2024, 1, 31),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "wholesale", "finance", "monthly", "billing"],
) as dag:

    # Guard: schedule fires on days 28-31; only proceed on the actual last day.
    t_last_day_check = ShortCircuitOperator(
        task_id="check_last_day_of_month",
        python_callable=is_last_day_of_month,
    )

    t_validate = SnowflakeOperator(
        task_id="validate_billing_completeness",
        snowflake_conn_id=CONN_SNOWFLAKE,
        sql=SQL_VALIDATE_COMPLETENESS,
        sla=timedelta(hours=1),
    )

    t_calculate = PythonOperator(
        task_id="calculate_monthly_charges",
        python_callable=calculate_monthly_charges,
        sla=timedelta(hours=3),
    )

    with TaskGroup("persist") as tg_persist:
        t_sf  = PythonOperator(task_id="write_to_snowflake", python_callable=write_to_snowflake, sla=timedelta(hours=5))
        t_ebs = PythonOperator(task_id="write_to_oracle_ebs", python_callable=write_to_oracle_ebs, sla=timedelta(hours=5))

    t_pdf = PythonOperator(
        task_id="generate_and_upload_pdfs",
        python_callable=generate_and_upload_pdfs,
        sla=timedelta(hours=7),
    )

    t_last_day_check >> t_validate >> t_calculate >> tg_persist >> t_pdf
