"""
NNN Airflow DAG — Kinesis RSP Portal Clickstream (Hourly)
=========================================================
Owner:      nnn-data-engineering
Domain:     ML / RSP Portal Analytics
Schedule:   45 * * * *  (45 min past each hour — reads the previous hour's stream)
SLA:        45 minutes
Description:
    Consumes records from the Kinesis Data Stream `nnn-rsp-portal-clickstream`
    for the previous complete hour (filtered by ApproximateArrivalTimestamp).
    Each record is base64-decoded and JSON-parsed, then batch-inserted into
    Snowflake ML.RSP_PORTAL_CLICKSTREAM for downstream ML feature pipelines.

    The previous-hour window is derived from the DAG execution date so the
    schedule offset (45 min past hour) naturally captures the preceding hour.

Steps:
    1. consume_kinesis_shard — get shard iterators, consume records for hour window
    2. load_to_snowflake     — batch INSERT decoded records into Snowflake

Upstream:   nnn-rsp-portal-clickstream Kinesis Data Stream (portal instrumentation)
Downstream: ML.RSP_PORTAL_CLICKSTREAM → feature engineering, recommender model
"""

from __future__ import annotations

import base64
import json
import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_KINESIS,
    get_execution_date,
    get_snowflake_hook,
)

log = logging.getLogger(__name__)

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
    "execution_timeout": timedelta(minutes=30),
}

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
KINESIS_STREAM_NAME = "nnn-rsp-portal-clickstream"
TARGET_TABLE = "ML.RSP_PORTAL_CLICKSTREAM"
# Maximum records to retrieve per GetRecords call (Kinesis limit = 10 000)
KINESIS_MAX_RECORDS_PER_CALL = 10_000
# Maximum total records we'll load per run to protect Snowflake ingest time
HARD_CAP_RECORDS = 500_000
# Rows per Snowflake executemany call to avoid parameter binding limits
SF_INSERT_CHUNK_SIZE = 1_000


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def consume_kinesis_shard(**context) -> int:
    """Consume all Kinesis records in the previous-hour window across all shards.

    Uses AT_TIMESTAMP shard iterators to start at the beginning of the previous
    hour, then reads forward until ApproximateArrivalTimestamp exceeds the
    window end or the shard is exhausted.
    """
    from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

    execution_dt: datetime = get_execution_date(context)

    # Compute the previous hour's [start, end) window
    window_end = execution_dt.replace(minute=0, second=0, microsecond=0)
    window_start = window_end - timedelta(hours=1)

    hook = AwsBaseHook(aws_conn_id=CONN_KINESIS, client_type="kinesis")
    client = hook.get_client_type("kinesis")

    # List all shards in the stream
    describe_resp = client.describe_stream_summary(StreamName=KINESIS_STREAM_NAME)
    shard_count = describe_resp["StreamDescriptionSummary"]["OpenShardCount"]

    shards_resp = client.list_shards(StreamName=KINESIS_STREAM_NAME)
    shards = shards_resp["Shards"]

    all_records: list[dict] = []

    for shard in shards:
        shard_id = shard["ShardId"]

        # Position iterator at start of the window
        iter_resp = client.get_shard_iterator(
            StreamName=KINESIS_STREAM_NAME,
            ShardId=shard_id,
            ShardIteratorType="AT_TIMESTAMP",
            Timestamp=window_start,
        )
        shard_iterator = iter_resp["ShardIterator"]

        shard_exhausted = False
        while not shard_exhausted and len(all_records) < HARD_CAP_RECORDS:
            if not shard_iterator:
                break  # Shard iterator expired or shard ended

            get_resp = client.get_records(
                ShardIterator=shard_iterator,
                Limit=KINESIS_MAX_RECORDS_PER_CALL,
            )
            raw_records = get_resp["Records"]
            shard_iterator = get_resp.get("NextShardIterator")

            if not raw_records:
                # No more records available in this shard at this time
                break

            for record in raw_records:
                arrival = record["ApproximateArrivalTimestamp"]
                # Ensure timezone-aware comparison
                if arrival.tzinfo is None:
                    arrival = arrival.replace(tzinfo=timezone.utc)

                if arrival >= window_end:
                    # Passed the window — stop reading this shard
                    shard_exhausted = True
                    break

                if arrival < window_start:
                    # Before window — skip (shouldn't happen with AT_TIMESTAMP but be safe)
                    continue

                # Decode base64 Kinesis record data and parse JSON payload
                try:
                    raw_data = base64.b64decode(record["Data"]).decode("utf-8")
                    payload = json.loads(raw_data)
                except (ValueError, json.JSONDecodeError):
                    # Log and skip malformed records rather than failing the run
                    continue

                all_records.append(
                    {
                        "sequence_number": record["SequenceNumber"],
                        "shard_id": shard_id,
                        "arrival_ts": arrival.isoformat(),
                        "session_id": payload.get("session_id"),
                        "rsp_id": payload.get("rsp_id"),
                        "user_id": payload.get("user_id"),
                        "event_type": payload.get("event_type"),
                        "page_path": payload.get("page_path"),
                        "referrer": payload.get("referrer"),
                        "device_type": payload.get("device_type"),
                        "duration_ms": payload.get("duration_ms"),
                        "event_ts": payload.get("event_ts"),
                    }
                )

    # PERF: all_records can reach up to HARD_CAP_RECORDS (500k) dicts — far too large
    # for XCom (stored in the Airflow metadata DB). Write to a temp file instead.
    import json
    import tempfile

    window_start_iso = window_start.isoformat()
    window_end_iso = window_end.isoformat()

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".json",
        prefix=f"kinesis_clickstream_{window_start_iso[:13]}_",
        delete=False,
    ) as tmp:
        json.dump(all_records, tmp)
        tmp_path = tmp.name

    context["ti"].xcom_push(key="records_path", value=tmp_path)
    context["ti"].xcom_push(key="window_start", value=window_start_iso)
    context["ti"].xcom_push(key="window_end", value=window_end_iso)
    return len(all_records)


def load_to_snowflake(**context) -> None:
    """Batch INSERT consumed clickstream records into ML.RSP_PORTAL_CLICKSTREAM."""
    import json
    import os as _os

    ti = context["ti"]
    records_path: str = ti.xcom_pull(task_ids="consume_kinesis_shard", key="records_path")
    window_start: str = ti.xcom_pull(task_ids="consume_kinesis_shard", key="window_start")
    with open(records_path) as fh:
        records: list[dict] = json.load(fh)

    if not records:
        # Possible during low-traffic periods; log but don't fail
        log.warning(
            "No clickstream records found for window %s. Skipping load.", window_start
        )
        _os.remove(records_path)
        return

    window_end: str = ti.xcom_pull(task_ids="consume_kinesis_shard", key="window_end")

    hook = get_snowflake_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # DELETE records for this hour window before inserting (idempotent on reruns)
        cursor.execute(
            f"""
            DELETE FROM {TARGET_TABLE}
            WHERE arrival_ts >= %(window_start)s::TIMESTAMP_TZ
              AND arrival_ts <  %(window_end)s::TIMESTAMP_TZ
            """,
            {"window_start": window_start, "window_end": window_end},
        )

        # Snowflake connector uses named %(name)s param style, not positional %s.
        insert_sql = f"""
            INSERT INTO {TARGET_TABLE} (
                sequence_number, shard_id, arrival_ts, session_id, rsp_id,
                user_id, event_type, page_path, referrer, device_type,
                duration_ms, event_ts, loaded_at
            ) VALUES (
                %(sequence_number)s, %(shard_id)s, %(arrival_ts)s,
                %(session_id)s, %(rsp_id)s, %(user_id)s,
                %(event_type)s, %(page_path)s, %(referrer)s,
                %(device_type)s, %(duration_ms)s, %(event_ts)s,
                CURRENT_TIMESTAMP()
            )
        """
        # Batch in chunks to avoid Snowflake parameter limits
        for i in range(0, len(records), SF_INSERT_CHUNK_SIZE):
            chunk = records[i : i + SF_INSERT_CHUNK_SIZE]
            rows = [
                {
                    "sequence_number": r["sequence_number"],
                    "shard_id":        r["shard_id"],
                    "arrival_ts":      r["arrival_ts"],
                    "session_id":      r["session_id"],
                    "rsp_id":          r["rsp_id"],
                    "user_id":         r["user_id"],
                    "event_type":      r["event_type"],
                    "page_path":       r["page_path"],
                    "referrer":        r["referrer"],
                    "device_type":     r["device_type"],
                    "duration_ms":     r["duration_ms"],
                    "event_ts":        r["event_ts"],
                }
                for r in chunk
            ]
            cursor.executemany(insert_sql, rows)

        conn.commit()
    finally:
        cursor.close()
        conn.close()
    _os.remove(records_path)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_kinesis_clickstream_hourly",
    default_args=default_args,
    description="Hourly Kinesis clickstream consumption → Snowflake ML.RSP_PORTAL_CLICKSTREAM",
    schedule_interval="45 * * * *",  # 45 min past hour; reads previous complete hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "ml", "clickstream", "kinesis", "snowflake", "hourly"],
) as dag:

    # Step 1: Consume Kinesis records for the previous complete hour window
    t_consume = PythonOperator(  # Get shard iterators, consume records within the hour window
        task_id="consume_kinesis_shard",
        python_callable=consume_kinesis_shard,
        sla=timedelta(minutes=45),
    )

    # Step 2: Batch INSERT decoded records into Snowflake ML schema
    t_load = PythonOperator(  # Batch INSERT base64-decoded clickstream records into ML.RSP_PORTAL_CLICKSTREAM
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
    )

    t_consume >> t_load
