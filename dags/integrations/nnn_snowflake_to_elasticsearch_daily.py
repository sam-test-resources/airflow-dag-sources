"""
nnn_snowflake_to_elasticsearch_daily
--------------------------------------
Owner:      nnn-data-engineering
Domain:     Customer / Service Availability
Schedule:   Daily at 07:00 AEST (0 7 * * *)
SLA:        2 hours

Refreshes the Elasticsearch index used by the customer-facing service availability
checker.  The full CUSTOMER.SERVICE_AVAILABILITY_CURRENT table is exported from
Snowflake and bulk-indexed into Elasticsearch, replacing the previous index contents.

Steps:
  1. export_from_snowflake  — full-table export from Snowflake, write to temp file,
                              push path via XCom
  2. index_to_elasticsearch — create index with mapping if absent, bulk-index
                              all documents (_id = address_id)
  3. refresh_and_validate   — force index refresh, count docs, raise if below
                              minimum expected threshold

Upstream:   CUSTOMER.SERVICE_AVAILABILITY_CURRENT refresh
Downstream: Customer-facing availability checker API
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_ELASTICSEARCH,
    get_snowflake_hook,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner":                     "nnn-data-engineering",
    "depends_on_past":           False,
    "email":                     ["de-alerts@nnnco.com.au"],
    "email_on_failure":          True,
    "email_on_retry":            False,
    "retries":                   2,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":           timedelta(minutes=30),
    "on_failure_callback":       nnn_failure_alert,
    "execution_timeout":         timedelta(hours=2),
}

# Minimum number of documents expected in the index after a successful load
MIN_EXPECTED_DOCS  = 1_000
# Elasticsearch bulk helper chunk size per API call
ES_BULK_CHUNK_SIZE = 500   # documents per bulk request

# Elasticsearch index name
ES_INDEX = "nnn-service-availability"

# Index mapping for service-availability documents
ES_INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "address_id":          {"type": "keyword"},
            "address_line_1":      {"type": "text"},
            "address_line_2":      {"type": "text"},
            "suburb":              {"type": "keyword"},
            "state":               {"type": "keyword"},
            "postcode":            {"type": "keyword"},
            "technology_type":     {"type": "keyword"},
            "max_download_mbps":   {"type": "float"},
            "max_upload_mbps":     {"type": "float"},
            "is_available":        {"type": "boolean"},
            "availability_reason": {"type": "keyword"},
            "last_updated_at":     {"type": "date"},
        }
    },
    "settings": {
        "number_of_shards":   3,
        "number_of_replicas": 1,
    },
}


# ---------------------------------------------------------------------------
# Task functions (module-level so the scheduler does not re-define them on
# every DAG parse cycle)
# ---------------------------------------------------------------------------

def export_from_snowflake(**context) -> None:
    """Export all records from CUSTOMER.SERVICE_AVAILABILITY_CURRENT and write to temp file.

    INTENTIONAL FULL-TABLE SCAN: SERVICE_AVAILABILITY_CURRENT is a current-state snapshot
    (no date partition). The full table is required for a complete ES index refresh.
    PERF: records can be millions of rows — write to a temp file and push only the path
    via XCom to avoid bloating the Airflow metadata DB.
    """
    hook   = get_snowflake_hook()
    conn   = hook.get_conn()
    cursor = conn.cursor()
    try:
        sql = """
            SELECT
                ADDRESS_ID,
                ADDRESS_LINE_1,
                ADDRESS_LINE_2,
                SUBURB,
                STATE,
                POSTCODE,
                TECHNOLOGY_TYPE,
                MAX_DOWNLOAD_SPEED_MBPS  AS max_download_mbps,
                MAX_UPLOAD_SPEED_MBPS    AS max_upload_mbps,
                IS_AVAILABLE,
                AVAILABILITY_REASON,
                LAST_UPDATED_AT
            FROM CUSTOMER.SERVICE_AVAILABILITY_CURRENT
        """
        cursor.execute(sql)
        columns = [col[0].lower() for col in cursor.description]
        rows    = cursor.fetchall()
        records = [dict(zip(columns, row)) for row in rows]
    finally:
        cursor.close()
        conn.close()

    # Normalise non-serialisable types
    for rec in records:
        for k, v in rec.items():
            if hasattr(v, "isoformat"):
                rec[k] = v.isoformat()

    # PERF: write to temp file; push only the path via XCom
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", prefix="svc_availability_", delete=False
    ) as tmp:
        json.dump(records, tmp)
        tmp_path = tmp.name

    context["ti"].xcom_push(key="availability_records_path", value=tmp_path)
    log.info("Exported %d records from CUSTOMER.SERVICE_AVAILABILITY_CURRENT", len(records))


def index_to_elasticsearch(**context) -> None:
    """Create the ES index (if absent) and bulk-index all availability documents."""
    from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
    from elasticsearch.helpers import bulk

    records_path = context["ti"].xcom_pull(
        task_ids="export_from_snowflake", key="availability_records_path"
    )
    if not records_path:
        raise ValueError("No availability records path received from Snowflake export — aborting.")

    with open(records_path) as fh:
        records = json.load(fh)

    if not records:
        os.remove(records_path)
        raise ValueError("No availability records in export file — aborting.")

    hook      = ElasticsearchPythonHook(elasticsearch_conn_id=CONN_ELASTICSEARCH)
    es_client = hook.get_conn()  # returns the elasticsearch.Elasticsearch client

    # Create index with mapping if it does not already exist
    if not es_client.indices.exists(index=ES_INDEX):
        es_client.indices.create(index=ES_INDEX, body=ES_INDEX_MAPPING)
        log.info("Created Elasticsearch index '%s' with mapping.", ES_INDEX)
    else:
        log.info("Elasticsearch index '%s' already exists — reusing.", ES_INDEX)

    # Build bulk action iterator
    def _actions():
        for rec in records:
            yield {
                "_index":  ES_INDEX,
                "_id":     rec["address_id"],
                "_source": rec,
            }

    success_count, errors = bulk(
        es_client, _actions(), raise_on_error=True, chunk_size=ES_BULK_CHUNK_SIZE
    )
    log.info("Bulk index complete — %d documents indexed, %d errors.", success_count, len(errors))

    if errors:
        raise RuntimeError(
            f"Elasticsearch bulk indexing had {len(errors)} errors: {errors[:5]}"
        )

    context["ti"].xcom_push(key="docs_indexed", value=success_count)
    os.remove(records_path)


def refresh_and_validate(**context) -> None:
    """Refresh the ES index and confirm the doc count meets the minimum threshold."""
    from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook

    hook      = ElasticsearchPythonHook(elasticsearch_conn_id=CONN_ELASTICSEARCH)
    es_client = hook.get_conn()  # returns the elasticsearch.Elasticsearch client

    # Force a refresh so count reflects all indexed docs
    es_client.indices.refresh(index=ES_INDEX)
    log.info("Index '%s' refreshed.", ES_INDEX)

    # Count documents
    response  = es_client.count(index=ES_INDEX)
    doc_count = response["count"]
    log.info("Document count in '%s': %d", ES_INDEX, doc_count)

    if doc_count < MIN_EXPECTED_DOCS:
        raise ValueError(
            f"Index '{ES_INDEX}' has {doc_count} docs — below minimum of {MIN_EXPECTED_DOCS}. "
            "Possible data quality issue."
        )
    log.info("Validation passed: %d docs >= minimum %d.", doc_count, MIN_EXPECTED_DOCS)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_snowflake_to_elasticsearch_daily",
    description="Full refresh of Elasticsearch service-availability index from Snowflake",
    default_args=default_args,
    schedule_interval="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "customer", "elasticsearch", "snowflake", "daily"],
) as dag:

    task_export = PythonOperator(  # Full-table export from CUSTOMER.SERVICE_AVAILABILITY_CURRENT
        task_id="export_from_snowflake",
        python_callable=export_from_snowflake,
    )

    task_index = PythonOperator(  # Create ES index if absent then bulk-index all availability documents
        task_id="index_to_elasticsearch",
        python_callable=index_to_elasticsearch,
    )

    task_validate = PythonOperator(  # Force ES index refresh and assert doc count >= MIN_EXPECTED_DOCS
        task_id="refresh_and_validate",
        python_callable=refresh_and_validate,
    )

    # ------------------------------------------------------------------
    # Dependency chain
    # ------------------------------------------------------------------
    task_export >> task_index >> task_validate
