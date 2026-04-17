"""
NNN Airflow DAG — SOAP Mediation CDR Ingest (Daily)
====================================================
Owner:      nnn-data-engineering
Domain:     Network / Mediation
Schedule:   0 3 * * *  (3 AM AEST daily)
SLA:        3 hours (must complete by 6 AM AEST)
Description:
    Fetches Call Data Records (CDRs) from the NNN network mediation system
    via a SOAP/XML HTTP interface. The NMS API connection (CONN_NMS_API) is
    used as the HTTP endpoint which supports SOAP over HTTPS.

    The SOAP `GetCDRsByDate` operation returns an XML response containing
    <Record> elements. These are parsed using xml.etree.ElementTree, flattened
    to dicts, and MERGEd into Snowflake NETWORK.MEDIATION_CDR using a single
    session (temp table → MERGE) to handle the upsert correctly.

Steps:
    1. fetch_soap_cdrs   — POST SOAP envelope, parse XML, push list to XCom
    2. load_to_snowflake — MERGE into NETWORK.MEDIATION_CDR on cdr_id
    3. validate          — assert row count >= 1 for run_date

Upstream:   NMS mediation system SOAP API (CONN_NMS_API)
Downstream: NETWORK.MEDIATION_CDR → usage analytics, billing reconciliation
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_NMS_API,
    assert_row_count,
    get_run_date,
    get_snowflake_hook,
)

# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner": "nnn-data-engineering",
    "depends_on_past": False,
    "email": ["de-alerts@nnnco.com.au"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": nnn_failure_alert,
    "execution_timeout": timedelta(hours=2),  # Large CDR batches can take time
}

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TARGET_TABLE = "NETWORK.MEDIATION_CDR"
SOAP_NAMESPACE = "http://nnnco.com.au/mediation/v2"
SOAP_ACTION = "GetCDRsByDate"

# SOAP envelope template — xs:date values are substituted at runtime
SOAP_ENVELOPE_TEMPLATE = """\
<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope
    xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:med="{namespace}">
  <soapenv:Header/>
  <soapenv:Body>
    <med:GetCDRsByDate>
      <med:StartDate>{start_date}</med:StartDate>
      <med:EndDate>{end_date}</med:EndDate>
      <med:RecordType>ALL</med:RecordType>
      <med:MaxRecords>50000</med:MaxRecords>
    </med:GetCDRsByDate>
  </soapenv:Body>
</soapenv:Envelope>"""


def _build_soap_envelope(run_date: str) -> str:
    """Render the SOAP envelope XML for a given run date (full day range)."""
    return SOAP_ENVELOPE_TEMPLATE.format(
        namespace=SOAP_NAMESPACE,
        start_date=f"{run_date}T00:00:00",
        end_date=f"{run_date}T23:59:59",
    )


def _parse_cdr_record(record_el: ET.Element, ns: str) -> dict:
    """Extract fields from a <Record> XML element into a flat dict."""

    def text(tag: str) -> str | None:
        el = record_el.find(f"{{{ns}}}{tag}")
        return el.text if el is not None else None

    return {
        "cdr_id": text("CdrId"),
        "record_type": text("RecordType"),
        "originating_number": text("OriginatingNumber"),
        "terminating_number": text("TerminatingNumber"),
        "start_time": text("StartTime"),
        "end_time": text("EndTime"),
        "duration_seconds": text("DurationSeconds"),
        "bytes_uplink": text("BytesUplink"),
        "bytes_downlink": text("BytesDownlink"),
        "network_element": text("NetworkElement"),
        "poi_code": text("PoiCode"),
        "region": text("Region"),
        "service_type": text("ServiceType"),
        "charge_amount": text("ChargeAmount"),
        "currency": text("Currency"),
        "rated": text("Rated"),
        "cdr_date": text("CdrDate"),
    }


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def fetch_soap_cdrs(**context) -> int:
    """POST SOAP GetCDRsByDate envelope, parse XML response, push records to XCom."""
    from airflow.providers.http.hooks.http import HttpHook

    run_date: str = get_run_date(context)
    soap_body = _build_soap_envelope(run_date)

    hook = HttpHook(method="POST", http_conn_id=CONN_NMS_API)

    response = hook.run(
        endpoint="/mediation/soap",
        data=soap_body,
        headers={
            "Content-Type": "text/xml; charset=UTF-8",
            "SOAPAction": f'"{SOAP_NAMESPACE}/{SOAP_ACTION}"',
        },
    )

    # Parse the XML SOAP response
    root = ET.fromstring(response.text)

    # Navigate past the SOAP envelope wrapper to the response body
    soap_ns = "http://schemas.xmlsoap.org/soap/envelope/"
    body_el = root.find(f"{{{soap_ns}}}Body")
    if body_el is None:
        raise ValueError("SOAP response missing <Body> element — unexpected format.")

    # Find the GetCDRsByDateResponse element in the mediation namespace
    response_el = body_el.find(f"{{{SOAP_NAMESPACE}}}GetCDRsByDateResponse")
    if response_el is None:
        raise ValueError(
            "SOAP response missing <GetCDRsByDateResponse> — check NMS API availability."
        )

    # Extract all <Record> elements
    record_elements = response_el.findall(f"{{{SOAP_NAMESPACE}}}Record")
    records = [_parse_cdr_record(el, SOAP_NAMESPACE) for el in record_elements]

    if not records:
        raise ValueError(
            f"SOAP GetCDRsByDate returned 0 records for {run_date}. "
            "This is unexpected — verify the mediation system processed data."
        )

    # PERF: CDR records can be up to 50,000 dicts — too large for XCom (metadata DB).
    # Write to a temp file and push only the file path.
    import json
    import tempfile

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", prefix=f"cdr_{run_date}_", delete=False
    ) as tmp:
        json.dump(records, tmp)
        tmp_path = tmp.name

    context["ti"].xcom_push(key="records_path", value=tmp_path)
    context["ti"].xcom_push(key="run_date", value=run_date)
    return len(records)


def load_to_snowflake(**context) -> None:
    """MERGE CDR records into NETWORK.MEDIATION_CDR using a single Snowflake session.

    Uses temp table → MERGE pattern so upserts are atomic. The temp table is
    session-scoped; everything must happen within a single connection.
    """
    import json
    import os as _os

    ti = context["ti"]
    records_path: str = ti.xcom_pull(task_ids="fetch_soap_cdrs", key="records_path")
    run_date: str = ti.xcom_pull(task_ids="fetch_soap_cdrs", key="run_date")
    with open(records_path) as fh:
        records: list[dict] = json.load(fh)

    hook = get_snowflake_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Create temp staging table for this session
        cursor.execute("""
            CREATE TEMPORARY TABLE tmp_mediation_cdr (
                cdr_id              VARCHAR,
                record_type         VARCHAR,
                originating_number  VARCHAR,
                terminating_number  VARCHAR,
                start_time          TIMESTAMP_NTZ,
                end_time            TIMESTAMP_NTZ,
                duration_seconds    NUMBER,
                bytes_uplink        NUMBER,
                bytes_downlink      NUMBER,
                network_element     VARCHAR,
                poi_code            VARCHAR,
                region              VARCHAR,
                service_type        VARCHAR,
                charge_amount       NUMBER(18,4),
                currency            VARCHAR,
                rated               BOOLEAN,
                cdr_date            DATE
            )
        """)

        # Batch INSERT all parsed CDR records into temp table
        # Snowflake connector uses named %(name)s param style, not positional %s.
        insert_tmp_sql = """
            INSERT INTO tmp_mediation_cdr VALUES (
                %(cdr_id)s, %(record_type)s, %(originating_number)s, %(terminating_number)s,
                TRY_TO_TIMESTAMP_NTZ(%(start_time)s),
                TRY_TO_TIMESTAMP_NTZ(%(end_time)s),
                TRY_TO_NUMBER(%(duration_seconds)s),
                TRY_TO_NUMBER(%(bytes_uplink)s),
                TRY_TO_NUMBER(%(bytes_downlink)s),
                %(network_element)s, %(poi_code)s, %(region)s, %(service_type)s,
                TRY_TO_NUMBER(%(charge_amount)s, 18, 4),
                %(currency)s,
                TRY_TO_BOOLEAN(%(rated)s),
                TRY_TO_DATE(%(cdr_date)s)
            )
        """
        rows = [
            {
                "cdr_id": r["cdr_id"],
                "record_type": r["record_type"],
                "originating_number": r["originating_number"],
                "terminating_number": r["terminating_number"],
                "start_time": r["start_time"],
                "end_time": r["end_time"],
                "duration_seconds": r["duration_seconds"],
                "bytes_uplink": r["bytes_uplink"],
                "bytes_downlink": r["bytes_downlink"],
                "network_element": r["network_element"],
                "poi_code": r["poi_code"],
                "region": r["region"],
                "service_type": r["service_type"],
                "charge_amount": r["charge_amount"],
                "currency": r["currency"],
                "rated": r["rated"],
                "cdr_date": r["cdr_date"],
            }
            for r in records
        ]
        cursor.executemany(insert_tmp_sql, rows)

        # MERGE temp table into permanent target on cdr_id
        cursor.execute(f"""
            MERGE INTO {TARGET_TABLE} AS tgt
            USING tmp_mediation_cdr AS src
            ON tgt.cdr_id = src.cdr_id
            WHEN MATCHED THEN UPDATE SET
                record_type        = src.record_type,
                originating_number = src.originating_number,
                terminating_number = src.terminating_number,
                start_time         = src.start_time,
                end_time           = src.end_time,
                duration_seconds   = src.duration_seconds,
                bytes_uplink       = src.bytes_uplink,
                bytes_downlink     = src.bytes_downlink,
                network_element    = src.network_element,
                poi_code           = src.poi_code,
                region             = src.region,
                service_type       = src.service_type,
                charge_amount      = src.charge_amount,
                currency           = src.currency,
                rated              = src.rated,
                cdr_date           = src.cdr_date
            WHEN NOT MATCHED THEN INSERT (
                cdr_id, record_type, originating_number, terminating_number,
                start_time, end_time, duration_seconds, bytes_uplink, bytes_downlink,
                network_element, poi_code, region, service_type,
                charge_amount, currency, rated, cdr_date
            ) VALUES (
                src.cdr_id, src.record_type, src.originating_number, src.terminating_number,
                src.start_time, src.end_time, src.duration_seconds, src.bytes_uplink,
                src.bytes_downlink, src.network_element, src.poi_code, src.region,
                src.service_type, src.charge_amount, src.currency, src.rated, src.cdr_date
            )
        """)

        conn.commit()
    finally:
        cursor.close()
        conn.close()
    _os.remove(records_path)


def validate_row_count(**context) -> None:
    """Assert at least one CDR row exists in Snowflake for run_date."""
    run_date = get_run_date(context)
    assert_row_count(TARGET_TABLE, run_date, min_rows=1)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_soap_mediation_cdr_daily",
    default_args=default_args,
    description="Daily SOAP CDR ingest from NMS mediation system → Snowflake NETWORK.MEDIATION_CDR",
    schedule_interval="0 3 * * *",  # 3 AM AEST
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "network", "mediation", "cdr", "soap", "snowflake", "daily"],
) as dag:

    # Step 1: POST SOAP envelope to NMS, parse XML response into record dicts
    t_fetch = PythonOperator(  # POST SOAP GetCDRsByDate envelope, parse XML, push records to XCom
        task_id="fetch_soap_cdrs",
        python_callable=fetch_soap_cdrs,
        sla=timedelta(hours=3),
    )

    # Step 2: MERGE CDR records into Snowflake via temp-table session pattern
    t_load = PythonOperator(  # INSERT into temp table then MERGE into NETWORK.MEDIATION_CDR on cdr_id
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
    )

    # Step 3: Confirm at least one row persisted for the run date
    t_validate = PythonOperator(  # Assert >= 1 CDR row in NETWORK.MEDIATION_CDR for run_date
        task_id="validate_row_count",
        python_callable=validate_row_count,
    )

    t_fetch >> t_load >> t_validate
