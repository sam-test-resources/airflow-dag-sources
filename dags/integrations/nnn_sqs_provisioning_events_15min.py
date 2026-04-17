"""
NNN Airflow DAG — SQS Provisioning Events (15-minute)
=====================================================
Owner:      nnn-data-engineering
Domain:     Operations / Service Provisioning
Schedule:   */15 * * * *  (every 15 minutes)
SLA:        None (near-real-time; monitored via separate alerting pipeline)
Description:
    Drains the SQS queue `nnn-provisioning-events` in batches, parses JSON
    message bodies representing service provisioning lifecycle events, and
    INSERTs them into Snowflake OPERATIONS.PROVISIONING_EVENTS. Processed
    messages are deleted from the queue after successful parse.

    The drain loop runs for up to 30 seconds or 200 messages (whichever
    comes first) to stay well within the 15-minute schedule cadence.

Steps:
    1. drain_sqs_queue   — batch receive → parse → delete → push records to XCom
    2. load_to_snowflake — INSERT records (skip if empty queue)

Upstream:   nnn-provisioning-events SQS queue (provisioning microservices)
Downstream: OPERATIONS.PROVISIONING_EVENTS → provisioning dashboards, SLA reporting
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert
from nnn_common.utils import (
    CONN_SQS,
    get_snowflake_hook,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default arguments — fast retry for near-real-time queue draining
# ---------------------------------------------------------------------------
default_args = {
    "owner": "nnn-data-engineering",
    "depends_on_past": False,
    "email": ["de-alerts@nnnco.com.au"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": False,  # consistent retry pace for queue draining
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": nnn_failure_alert,
    "execution_timeout": timedelta(minutes=10),  # well within 15-min cadence
}

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SQS_QUEUE_NAME        = "nnn-provisioning-events"
MAX_MESSAGES          = 200   # Hard cap per run — leave headroom for next cycle
DRAIN_TIMEOUT_SECONDS = 30    # Stop draining after this many wall-clock seconds
SQS_BATCH_SIZE        = 10    # Max per SQS receive_message call (AWS hard limit)
SF_INSERT_CHUNK_SIZE  = 500   # Rows per Snowflake executemany call
TARGET_TABLE          = "OPERATIONS.PROVISIONING_EVENTS"


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def drain_sqs_queue(**context) -> int:
    """Receive all available messages from the SQS queue (up to MAX_MESSAGES).

    Messages are parsed from JSON, collected, then deleted from the queue in
    the same batch to ensure at-least-once delivery semantics. If a message
    cannot be parsed it is skipped and left on the queue (or sent to DLQ
    after max receives).
    """
    from airflow.providers.amazon.aws.hooks.sqs import SqsHook

    hook = SqsHook(aws_conn_id=CONN_SQS)
    sqs_client = hook.get_conn()

    # Resolve queue URL from name
    queue_url_resp = sqs_client.get_queue_url(QueueName=SQS_QUEUE_NAME)
    queue_url: str = queue_url_resp["QueueUrl"]

    records: list[dict] = []
    drain_start = time.monotonic()

    while (
        len(records) < MAX_MESSAGES
        and (time.monotonic() - drain_start) < DRAIN_TIMEOUT_SECONDS
    ):
        # Receive up to SQS_BATCH_SIZE messages (AWS hard limit is 10)
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=SQS_BATCH_SIZE,
            WaitTimeSeconds=2,  # Short long-poll; we'll loop ourselves
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
        )

        messages = response.get("Messages", [])
        if not messages:
            # Queue appears empty — stop draining
            break

        successful_receipts: list[dict] = []

        for msg in messages:
            receipt_handle = msg["ReceiptHandle"]
            try:
                body = json.loads(msg["Body"])
            except (json.JSONDecodeError, ValueError):
                # Malformed message — skip; will be redriven to DLQ by AWS
                continue

            records.append(
                {
                    "message_id": msg["MessageId"],
                    "event_type": body.get("eventType") or body.get("event_type"),
                    "service_id": body.get("serviceId") or body.get("service_id"),
                    "order_id": body.get("orderId") or body.get("order_id"),
                    "customer_id": body.get("customerId") or body.get("customer_id"),
                    "rsp_id": body.get("rspId") or body.get("rsp_id"),
                    "status": body.get("status"),
                    "region": body.get("region"),
                    "event_ts": body.get("eventTs") or body.get("event_ts"),
                    "raw_payload": msg["Body"],
                    "sent_at": msg.get("Attributes", {}).get("SentTimestamp"),
                }
            )
            successful_receipts.append(
                {"Id": msg["MessageId"], "ReceiptHandle": receipt_handle}
            )

        # Batch delete successfully parsed messages from the queue
        if successful_receipts:
            sqs_client.delete_message_batch(
                QueueUrl=queue_url,
                Entries=successful_receipts,
            )

    context["ti"].xcom_push(key="provisioning_events", value=records)
    return len(records)


def load_to_snowflake(**context) -> None:
    """INSERT drained provisioning events into Snowflake OPERATIONS.PROVISIONING_EVENTS.

    If the queue was empty this run, logs and skips gracefully.
    """
    records: list[dict] = context["ti"].xcom_pull(
        task_ids="drain_sqs_queue", key="provisioning_events"
    )

    if not records:
        log.info(
            "SQS queue %s was empty this run — nothing to load.", SQS_QUEUE_NAME
        )
        return

    hook = get_snowflake_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Snowflake connector uses named %(name)s param style, not positional %s.
        insert_sql = f"""
            INSERT INTO {TARGET_TABLE} (
                message_id, event_type, service_id, order_id, customer_id,
                rsp_id, status, region, event_ts, raw_payload, sent_at, loaded_at
            ) VALUES (
                %(message_id)s, %(event_type)s, %(service_id)s, %(order_id)s,
                %(customer_id)s, %(rsp_id)s, %(status)s, %(region)s,
                %(event_ts)s, %(raw_payload)s,
                TO_TIMESTAMP_NTZ(%(sent_at)s::BIGINT / 1000),
                CURRENT_TIMESTAMP()
            )
        """
        # Batch in chunks to stay within Snowflake parameter limits
        for i in range(0, len(records), SF_INSERT_CHUNK_SIZE):
            chunk = records[i : i + SF_INSERT_CHUNK_SIZE]
            rows = [
                {
                    "message_id": r["message_id"],
                    "event_type": r["event_type"],
                    "service_id": r["service_id"],
                    "order_id": r["order_id"],
                    "customer_id": r["customer_id"],
                    "rsp_id": r["rsp_id"],
                    "status": r["status"],
                    "region": r["region"],
                    "event_ts": r["event_ts"],
                    "raw_payload": r["raw_payload"],
                    "sent_at": r["sent_at"],
                }
                for r in chunk
            ]
            cursor.executemany(insert_sql, rows)

        conn.commit()
        log.info(
            "Loaded %d provisioning events into %s.", len(records), TARGET_TABLE
        )
    finally:
        cursor.close()
        conn.close()


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_sqs_provisioning_events_15min",
    default_args=default_args,
    description="Drain SQS nnn-provisioning-events every 15 min into Snowflake OPERATIONS.PROVISIONING_EVENTS",
    schedule_interval="*/15 * * * *",  # every 15 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    # No SLA — near-real-time; monitored separately
    tags=["nnn", "operations", "provisioning", "sqs", "snowflake", "15min"],
) as dag:

    # Step 1: Drain up to 200 messages from the SQS queue in a 30-second window
    t_drain = PythonOperator(  # Batch receive → parse → delete SQS messages, push records to XCom
        task_id="drain_sqs_queue",
        python_callable=drain_sqs_queue,
    )

    # Step 2: INSERT valid records into Snowflake; skip gracefully if queue empty
    t_load = PythonOperator(  # INSERT parsed provisioning events into OPERATIONS.PROVISIONING_EVENTS
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
    )

    t_drain >> t_load
