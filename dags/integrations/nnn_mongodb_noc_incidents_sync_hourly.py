"""
NNN Airflow DAG — MongoDB NOC Incidents Sync (Hourly)
=====================================================
Owner:      nnn-data-engineering
Domain:     Network Operations / Incidents
Schedule:   5 * * * *  (5 minutes past each hour)
SLA:        30 minutes (near-real-time; row-freshness alerting via Snowflake)
Description:
    Extracts updated NOC incident documents from MongoDB collection
    `noc_tools.incidents` using a watermark-based approach, flattens nested
    structures, and MERGEs them into Snowflake OPERATIONS.NOC_INCIDENTS.
    The watermark is stored in OPERATIONS.ETL_WATERMARKS and advanced at the
    end of each successful run.

    Note: SLA miss callback is intentionally omitted — near-real-time
    monitoring is handled by a Snowflake row-freshness alert instead.

Steps:
    1. extract_mongodb_incidents — query MongoDB with watermark, push to XCom
    2. load_to_snowflake         — MERGE into OPERATIONS.NOC_INCIDENTS
    3. update_watermark          — advance watermark (trigger_rule=all_done)

Upstream:   MongoDB noc_tools.incidents (NOC tooling writes)
Downstream: OPERATIONS.NOC_INCIDENTS → NOC dashboards, SLA reports
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from nnn_common.alerts import nnn_failure_alert
from nnn_common.utils import (
    CONN_MONGODB,
    get_execution_date,
    get_snowflake_hook,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default arguments — higher retry cadence for near-real-time pipeline
# ---------------------------------------------------------------------------
default_args = {
    "owner": "nnn-data-engineering",
    "depends_on_past": False,
    "email": ["de-alerts@nnnco.com.au"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": False,  # fixed 1-min retry for near-real-time
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": nnn_failure_alert,
    "execution_timeout": timedelta(minutes=30),
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

WATERMARK_TABLE = "OPERATIONS.ETL_WATERMARKS"
TARGET_TABLE = "OPERATIONS.NOC_INCIDENTS"
DAG_ID = "nnn_mongodb_noc_incidents_sync_hourly"
MONGO_COLLECTION = "noc_tools.incidents"


def _flatten_incident(doc: dict) -> dict:
    """Flatten a nested MongoDB incident document to a Snowflake-ready dict."""
    return {
        "incident_id": str(doc.get("_id", "")),
        "incident_number": doc.get("incident_number"),
        "title": doc.get("title"),
        "severity": doc.get("severity"),
        "status": doc.get("status"),
        "category": doc.get("category"),
        "affected_region": (doc.get("location") or {}).get("region"),
        "affected_poi": (doc.get("location") or {}).get("poi_code"),
        "assigned_team": (doc.get("assignment") or {}).get("team"),
        "assigned_agent": (doc.get("assignment") or {}).get("agent"),
        "created_at": doc.get("created_at"),
        "updated_at": doc.get("updated_at"),
        "resolved_at": doc.get("resolved_at"),
        "root_cause": (doc.get("resolution") or {}).get("root_cause"),
        "notes": doc.get("notes"),
        "source_system": "MONGODB_NOC",
    }


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def extract_mongodb_incidents(**context) -> int:
    """Query MongoDB for incidents updated since the last watermark.

    The watermark is read from Snowflake OPERATIONS.ETL_WATERMARKS so that
    it survives Airflow restarts. Falls back to execution_date - 1 hour if
    no watermark exists (first run or reset).
    """
    from airflow.providers.mongo.hooks.mongo import MongoHook

    hook = get_snowflake_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Fetch the current watermark for this DAG
        # Snowflake connector uses named %(name)s param style, not positional %s.
        cursor.execute(
            f"""
            SELECT watermark_ts
            FROM {WATERMARK_TABLE}
            WHERE dag_id = %(dag_id)s
            """,
            {"dag_id": DAG_ID},
        )
        row = cursor.fetchone()
    finally:
        cursor.close()
        conn.close()

    execution_dt: datetime = get_execution_date(context)

    if row and row[0]:
        watermark_dt: datetime = row[0]
        if watermark_dt.tzinfo is None:
            watermark_dt = watermark_dt.replace(tzinfo=timezone.utc)
    else:
        # Default watermark: look back 1 hour from execution time
        watermark_dt = execution_dt - timedelta(hours=1)

    # Query MongoDB for incidents updated after the watermark
    mongo_hook = MongoHook(conn_id=CONN_MONGODB)
    client = mongo_hook.get_conn()

    db_name, coll_name = MONGO_COLLECTION.split(".", 1)
    collection = client[db_name][coll_name]

    cursor_mongo = collection.find(
        {"updated_at": {"$gte": watermark_dt}},
        sort=[("updated_at", 1)],
    )

    records = [_flatten_incident(doc) for doc in cursor_mongo]
    client.close()

    context["ti"].xcom_push(key="incidents", value=records)
    context["ti"].xcom_push(key="watermark_dt", value=watermark_dt.isoformat())
    context["ti"].xcom_push(key="new_watermark", value=execution_dt.isoformat())

    return len(records)


def load_to_snowflake(**context) -> None:
    """MERGE fetched incidents into OPERATIONS.NOC_INCIDENTS.

    Uses a single Snowflake session so that the temp table remains in scope
    for the duration of the MERGE operation.
    """
    ti = context["ti"]
    records: list[dict] = ti.xcom_pull(task_ids="extract_mongodb_incidents", key="incidents")

    if not records:
        return  # Nothing to load — watermark will still advance

    hook = get_snowflake_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Create a temporary staging table for this session
        cursor.execute("""
            CREATE TEMPORARY TABLE tmp_noc_incidents (
                incident_id      VARCHAR,
                incident_number  VARCHAR,
                title            VARCHAR,
                severity         VARCHAR,
                status           VARCHAR,
                category         VARCHAR,
                affected_region  VARCHAR,
                affected_poi     VARCHAR,
                assigned_team    VARCHAR,
                assigned_agent   VARCHAR,
                created_at       TIMESTAMP_NTZ,
                updated_at       TIMESTAMP_NTZ,
                resolved_at      TIMESTAMP_NTZ,
                root_cause       VARCHAR,
                notes            VARCHAR,
                source_system    VARCHAR
            )
        """)

        # Batch insert records into temp table
        # Snowflake connector uses named %(name)s param style, not positional %s.
        insert_sql = """
            INSERT INTO tmp_noc_incidents VALUES (
                %(incident_id)s, %(incident_number)s, %(title)s,
                %(severity)s, %(status)s, %(category)s,
                %(affected_region)s, %(affected_poi)s,
                %(assigned_team)s, %(assigned_agent)s,
                %(created_at)s, %(updated_at)s, %(resolved_at)s,
                %(root_cause)s, %(notes)s, %(source_system)s
            )
        """
        rows = [
            {
                "incident_id": r["incident_id"],
                "incident_number": r["incident_number"],
                "title": r["title"],
                "severity": r["severity"],
                "status": r["status"],
                "category": r["category"],
                "affected_region": r["affected_region"],
                "affected_poi": r["affected_poi"],
                "assigned_team": r["assigned_team"],
                "assigned_agent": r["assigned_agent"],
                "created_at": r["created_at"],
                "updated_at": r["updated_at"],
                "resolved_at": r["resolved_at"],
                "root_cause": r["root_cause"],
                "notes": r["notes"],
                "source_system": r["source_system"],
            }
            for r in records
        ]
        cursor.executemany(insert_sql, rows)

        # MERGE temp into target on incident_id
        cursor.execute(f"""
            MERGE INTO {TARGET_TABLE} AS tgt
            USING tmp_noc_incidents AS src
            ON tgt.incident_id = src.incident_id
            WHEN MATCHED THEN UPDATE SET
                incident_number = src.incident_number,
                title           = src.title,
                severity        = src.severity,
                status          = src.status,
                category        = src.category,
                affected_region = src.affected_region,
                affected_poi    = src.affected_poi,
                assigned_team   = src.assigned_team,
                assigned_agent  = src.assigned_agent,
                created_at      = src.created_at,
                updated_at      = src.updated_at,
                resolved_at     = src.resolved_at,
                root_cause      = src.root_cause,
                notes           = src.notes,
                source_system   = src.source_system
            WHEN NOT MATCHED THEN INSERT (
                incident_id, incident_number, title, severity, status,
                category, affected_region, affected_poi, assigned_team,
                assigned_agent, created_at, updated_at, resolved_at,
                root_cause, notes, source_system
            ) VALUES (
                src.incident_id, src.incident_number, src.title, src.severity,
                src.status, src.category, src.affected_region, src.affected_poi,
                src.assigned_team, src.assigned_agent, src.created_at,
                src.updated_at, src.resolved_at, src.root_cause,
                src.notes, src.source_system
            )
        """)

        conn.commit()
    finally:
        cursor.close()
        conn.close()


def update_watermark(**context) -> None:
    """Advance the ETL watermark regardless of upstream success/failure.

    Using trigger_rule=all_done ensures this runs even after partial failures,
    so the watermark never goes backwards on retry.
    """
    new_watermark: str = context["ti"].xcom_pull(
        task_ids="extract_mongodb_incidents", key="new_watermark"
    )
    if not new_watermark:
        return  # Extract failed entirely; nothing to advance

    hook = get_snowflake_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Snowflake connector uses named %(name)s param style, not positional %s.
        cursor.execute(
            f"""
            MERGE INTO {WATERMARK_TABLE} AS tgt
            USING (SELECT %(dag_id)s AS dag_id, %(watermark_ts)s::TIMESTAMP_NTZ AS watermark_ts) AS src
            ON tgt.dag_id = src.dag_id
            WHEN MATCHED THEN UPDATE SET watermark_ts = src.watermark_ts
            WHEN NOT MATCHED THEN INSERT (dag_id, watermark_ts) VALUES (src.dag_id, src.watermark_ts)
            """,
            {"dag_id": DAG_ID, "watermark_ts": new_watermark},
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Hourly watermark-based sync of MongoDB NOC incidents into Snowflake",
    schedule_interval="5 * * * *",  # 5 min past each hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    # No sla_miss_callback — freshness monitored via Snowflake row-freshness alert
    tags=["nnn", "operations", "noc", "mongodb", "snowflake", "hourly"],
) as dag:

    # Step 1: Extract incidents updated since last watermark
    t_extract = PythonOperator(  # Query MongoDB for incidents updated since last watermark timestamp
        task_id="extract_mongodb_incidents",
        python_callable=extract_mongodb_incidents,
    )

    # Step 2: MERGE extracted incidents into Snowflake target
    t_load = PythonOperator(  # MERGE extracted incidents into OPERATIONS.NOC_INCIDENTS on incident_id
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
    )

    # Step 3: Advance watermark — runs regardless of load success/failure
    t_watermark = PythonOperator(  # Advance ETL watermark in OPERATIONS.ETL_WATERMARKS (runs on all_done)
        task_id="update_watermark",
        python_callable=update_watermark,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_extract >> t_load >> t_watermark
