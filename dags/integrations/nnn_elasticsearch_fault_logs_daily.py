"""
NNN Airflow DAG — Elasticsearch Fault Log Summary (Daily)
=========================================================
Owner:      nnn-data-engineering
Domain:     Network Operations / Fault Management
Schedule:   0 1 * * *  (1 AM AEST daily)
SLA:        90 minutes (must complete by 2:30 AM AEST)
Description:
    Queries the Elasticsearch cluster for yesterday's network fault log
    entries from the `nnn-fault-logs-*` rolling index pattern. Aggregates
    by fault_type, severity, and affected_poi, then loads the summary rows
    into Snowflake OPERATIONS.FAULT_LOG_SUMMARY for NOC trend analysis.

Steps:
    1. extract_fault_logs    — ES date-range + bucket aggregation, push to XCom
    2. transform_and_load    — flatten buckets, truncate+INSERT into Snowflake
    3. validate              — assert >= 1 row loaded for run_date

Upstream:   Elasticsearch nnn-fault-logs-* (log pipeline)
Downstream: OPERATIONS.FAULT_LOG_SUMMARY → NOC dashboards, fault trend reports
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_ELASTICSEARCH,
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
    "execution_timeout": timedelta(minutes=45),  # ES aggregation can be slow on large indices
}

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TARGET_TABLE      = "OPERATIONS.FAULT_LOG_SUMMARY"
ES_INDEX_PATTERN  = "nnn-fault-logs-*"
ES_FAULT_TYPE_BUCKETS = 100   # max unique fault_type terms per aggregation
ES_SEVERITY_BUCKETS   = 20    # max unique severity terms per aggregation
ES_POI_BUCKETS        = 500   # max unique affected_poi terms per aggregation


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def extract_fault_logs(**context) -> int:
    """Aggregate fault log events from Elasticsearch for the previous day.

    Uses a composite aggregation over (fault_type, severity, affected_poi)
    to produce per-combination counts and other statistics. The aggregation
    results are pushed to XCom for the load step.
    """
    from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook

    run_date: str = get_run_date(context)

    # Build date range: the full calendar day in UTC
    date_from = f"{run_date}T00:00:00Z"
    date_to = f"{run_date}T23:59:59Z"

    hook = ElasticsearchPythonHook(elasticsearch_conn_ids=[CONN_ELASTICSEARCH])
    client = hook.get_conn  # ElasticsearchPythonHook exposes the client via get_conn

    # ES aggregation query: filter by date range, bucket by fault_type > severity > affected_poi
    query = {
        "size": 0,  # We only want aggregation buckets, not raw hits
        "query": {
            "bool": {
                "filter": [
                    {
                        "range": {
                            "@timestamp": {
                                "gte": date_from,
                                "lte": date_to,
                            }
                        }
                    }
                ]
            }
        },
        "aggs": {
            "by_fault_type": {
                "terms": {"field": "fault_type.keyword", "size": ES_FAULT_TYPE_BUCKETS},
                "aggs": {
                    "by_severity": {
                        "terms": {"field": "severity.keyword", "size": ES_SEVERITY_BUCKETS},
                        "aggs": {
                            "by_poi": {
                                "terms": {"field": "affected_poi.keyword", "size": ES_POI_BUCKETS},
                                "aggs": {
                                    "avg_duration_sec": {
                                        "avg": {"field": "duration_seconds"}
                                    },
                                    "max_duration_sec": {
                                        "max": {"field": "duration_seconds"}
                                    },
                                },
                            }
                        },
                    }
                },
            }
        },
    }

    response = client.search(index=ES_INDEX_PATTERN, body=query)

    # Extract flat list of bucket dicts from nested aggregation result
    buckets: list[dict] = []
    for ft_bucket in response["aggregations"]["by_fault_type"]["buckets"]:
        fault_type = ft_bucket["key"]
        for sev_bucket in ft_bucket["by_severity"]["buckets"]:
            severity = sev_bucket["key"]
            for poi_bucket in sev_bucket["by_poi"]["buckets"]:
                buckets.append(
                    {
                        "run_date": run_date,
                        "fault_type": fault_type,
                        "severity": severity,
                        "affected_poi": poi_bucket["key"],
                        "event_count": poi_bucket["doc_count"],
                        "avg_duration_sec": poi_bucket["avg_duration_sec"]["value"],
                        "max_duration_sec": poi_bucket["max_duration_sec"]["value"],
                    }
                )

    # PERF: buckets can be up to 100*20*500 = 1M dicts — too large for XCom (metadata DB).
    # Write to a temp file and push only the file path.
    import json
    import tempfile
    import os as _os

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", prefix=f"fault_log_buckets_{run_date}_", delete=False
    ) as tmp:
        json.dump(buckets, tmp)
        tmp_path = tmp.name

    context["ti"].xcom_push(key="buckets_path", value=tmp_path)
    return len(buckets)


def transform_and_load(**context) -> None:
    """Truncate existing summary rows for run_date then INSERT fresh aggregates."""
    import json
    import os as _os

    run_date: str = get_run_date(context)
    buckets_path: str = context["ti"].xcom_pull(
        task_ids="extract_fault_logs", key="buckets_path"
    )

    if not buckets_path:
        raise ValueError(
            f"No fault log aggregation buckets returned for {run_date}. "
            "Verify Elasticsearch index data exists."
        )

    with open(buckets_path) as fh:
        buckets: list[dict] = json.load(fh)

    if not buckets:
        _os.remove(buckets_path)
        raise ValueError(
            f"No fault log aggregation buckets returned for {run_date}. "
            "Verify Elasticsearch index data exists."
        )

    hook = get_snowflake_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Remove any existing summary rows for this run date (idempotent reload)
        cursor.execute(
            f"DELETE FROM {TARGET_TABLE} WHERE run_date = %(run_date)s",
            {"run_date": run_date},
        )

        # Batch INSERT the aggregated buckets
        insert_sql = f"""
            INSERT INTO {TARGET_TABLE} (
                run_date, fault_type, severity, affected_poi,
                event_count, avg_duration_sec, max_duration_sec, loaded_at
            ) VALUES (%(run_date)s, %(fault_type)s, %(severity)s, %(affected_poi)s,
                      %(event_count)s, %(avg_duration_sec)s, %(max_duration_sec)s, CURRENT_TIMESTAMP())
        """
        rows = [
            {
                "run_date": b["run_date"],
                "fault_type": b["fault_type"],
                "severity": b["severity"],
                "affected_poi": b["affected_poi"],
                "event_count": b["event_count"],
                "avg_duration_sec": b["avg_duration_sec"],
                "max_duration_sec": b["max_duration_sec"],
            }
            for b in buckets
        ]
        cursor.executemany(insert_sql, rows)
        conn.commit()
    finally:
        cursor.close()
        conn.close()
    _os.remove(buckets_path)


def validate_row_count(**context) -> None:
    """Assert that summary rows exist for the run date."""
    run_date = get_run_date(context)
    assert_row_count(TARGET_TABLE, run_date, min_rows=1)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_elasticsearch_fault_logs_daily",
    default_args=default_args,
    description="Daily ES fault-log aggregation → Snowflake OPERATIONS.FAULT_LOG_SUMMARY",
    schedule_interval="0 1 * * *",  # 1 AM AEST
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "operations", "faults", "elasticsearch", "snowflake", "daily"],
) as dag:

    # Step 1: Aggregate yesterday's fault logs from Elasticsearch
    t_extract = PythonOperator(  # ES date-range + bucket aggregation over fault_type/severity/poi
        task_id="extract_fault_logs",
        python_callable=extract_fault_logs,
        sla=timedelta(minutes=90),
    )

    # Step 2: Flatten aggregation buckets and load into Snowflake
    t_load = PythonOperator(  # DELETE existing summary rows for run_date, INSERT fresh aggregates
        task_id="transform_and_load",
        python_callable=transform_and_load,
    )

    # Step 3: Assert that at least one summary row was loaded
    t_validate = PythonOperator(  # Assert >= 1 row in OPERATIONS.FAULT_LOG_SUMMARY for run_date
        task_id="validate_row_count",
        python_callable=validate_row_count,
    )

    t_extract >> t_load >> t_validate
