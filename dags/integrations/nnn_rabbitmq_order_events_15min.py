"""
nnn_rabbitmq_order_events_15min
---------------------------------
Owner:      nnn-data-engineering
Domain:     Operations / Order Management
Schedule:   Every 15 minutes (*/15 * * * *)
SLA:        None (near-real-time pipeline)

Drains the RabbitMQ queue 'nnn.orders.provisioned' of pending order management
events and inserts them into Snowflake OPERATIONS.ORDER_EVENTS.  Each run
processes up to 500 messages or drains within 30 seconds, whichever comes first.

Steps:
  1. consume_rabbitmq   — drain queue via basic_get loop, parse JSON message bodies,
                          push list of event dicts to XCom
  2. load_to_snowflake  — INSERT events into OPERATIONS.ORDER_EVENTS; skip if empty

Upstream:   Order management system (OMS) publishes to RabbitMQ
Downstream: OPERATIONS.ORDER_EVENTS → ops dashboards, SLA monitoring
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert
from nnn_common.utils import (
    CONN_RABBITMQ,
    get_snowflake_hook,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default arguments (overrides for near-real-time: retries=3, shorter delay)
# ---------------------------------------------------------------------------
default_args = {
    "owner":                     "nnn-data-engineering",
    "depends_on_past":           False,
    "email":                     ["de-alerts@nnnco.com.au"],
    "email_on_failure":          True,
    "email_on_retry":            False,
    "retries":                   3,
    "retry_delay":               timedelta(minutes=1),
    "retry_exponential_backoff": False,
    "max_retry_delay":           timedelta(minutes=30),
    "on_failure_callback":       nnn_failure_alert,
    "execution_timeout":         timedelta(minutes=30),
}

RABBITMQ_QUEUE  = "nnn.orders.provisioned"
MAX_MESSAGES    = 500   # Hard cap per run — leave headroom for next cycle
DRAIN_TIMEOUT_S = 30    # Stop draining after this many wall-clock seconds
SNOWFLAKE_TABLE = "OPERATIONS.ORDER_EVENTS"


# ---------------------------------------------------------------------------
# Task functions (module-level so the scheduler does not re-define them on
# every DAG parse cycle)
# ---------------------------------------------------------------------------

def consume_rabbitmq(**context) -> None:
    """Drain nnn.orders.provisioned queue (max 500 messages or 30 s) and push to XCom."""
    from airflow.providers.rabbitmq.hooks.rabbitmq import RabbitMQHook

    hook      = RabbitMQHook(rabbitmq_conn_id=CONN_RABBITMQ)
    # get_conn() returns a pika BlockingConnection
    pika_conn = hook.get_conn()
    channel   = pika_conn.channel()

    events   = []
    start_ts = time.monotonic()

    try:
        while len(events) < MAX_MESSAGES:
            # Enforce wall-clock drain timeout
            if time.monotonic() - start_ts > DRAIN_TIMEOUT_S:
                log.info("Drain timeout (%ds) reached — stopping consumption.", DRAIN_TIMEOUT_S)
                break

            method_frame, _header_frame, body = channel.basic_get(
                queue=RABBITMQ_QUEUE, auto_ack=False
            )

            if method_frame is None:
                # Queue is empty
                log.info("Queue empty — no more messages.")
                break

            try:
                event = json.loads(body.decode("utf-8"))
                event["_rabbitmq_delivery_tag"] = method_frame.delivery_tag
                event["_ingested_at"] = datetime.now(tz=timezone.utc).isoformat()
                events.append(event)
                # Acknowledge message only after successful parse
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            except json.JSONDecodeError as exc:
                # NACK malformed messages so they route to the dead-letter queue
                channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)
                log.warning("Could not parse message body (NACKed): %s", exc)

    finally:
        channel.close()
        pika_conn.close()

    elapsed = time.monotonic() - start_ts
    log.info("Consumed %d messages from '%s' in %.1fs", len(events), RABBITMQ_QUEUE, elapsed)
    context["ti"].xcom_push(key="order_events", value=events)
    context["ti"].xcom_push(key="event_count",  value=len(events))


def load_to_snowflake(**context) -> None:
    """INSERT consumed order events into OPERATIONS.ORDER_EVENTS; skip if none."""
    events = context["ti"].xcom_pull(task_ids="consume_rabbitmq", key="order_events") or []

    if not events:
        log.info("No order events to load — skipping Snowflake insert.")
        return

    hook   = get_snowflake_hook()
    conn   = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Snowflake connector uses named %(name)s param style, not positional %s.
        insert_sql = f"""
            INSERT INTO {SNOWFLAKE_TABLE} (
                ORDER_ID,
                EVENT_TYPE,
                SERVICE_ID,
                CUSTOMER_ID,
                RSP_ID,
                STATUS,
                EVENT_TIMESTAMP,
                PAYLOAD,
                INGESTED_AT
            )
            VALUES (
                %(order_id)s, %(event_type)s, %(service_id)s,
                %(customer_id)s, %(rsp_id)s, %(status)s,
                %(event_timestamp)s, %(payload)s, %(ingested_at)s
            )
        """
        rows = [
            {
                "order_id":        event.get("order_id"),
                "event_type":      event.get("event_type"),
                "service_id":      event.get("service_id"),
                "customer_id":     event.get("customer_id"),
                "rsp_id":          event.get("rsp_id"),
                "status":          event.get("status"),
                "event_timestamp": event.get("event_timestamp"),
                "payload":         json.dumps(event),
                "ingested_at":     event.get("_ingested_at"),
            }
            for event in events
        ]
        cursor.executemany(insert_sql, rows)
        conn.commit()
        log.info("Inserted %d order events into %s", len(events), SNOWFLAKE_TABLE)
    finally:
        cursor.close()
        conn.close()


# ---------------------------------------------------------------------------
# DAG definition (no SLA for near-real-time pipeline)
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_rabbitmq_order_events_15min",
    description="Drain RabbitMQ order events queue every 15 min → Snowflake",
    default_args=default_args,
    schedule_interval="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["nnn", "operations", "rabbitmq", "snowflake", "near-realtime"],
) as dag:

    task_consume = PythonOperator(  # Drain nnn.orders.provisioned via basic_get loop, push events to XCom
        task_id="consume_rabbitmq",
        python_callable=consume_rabbitmq,
    )

    task_load = PythonOperator(  # INSERT parsed order events into OPERATIONS.ORDER_EVENTS; skip if empty
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
    )

    # ------------------------------------------------------------------
    # Dependency chain
    # ------------------------------------------------------------------
    task_consume >> task_load
