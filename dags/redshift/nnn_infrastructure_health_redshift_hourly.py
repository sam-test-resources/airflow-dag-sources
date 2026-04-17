"""
nnn_infrastructure_health_redshift_hourly
------------------------------------------
Owner:      nnn-data-engineering
Domain:     Network / Infrastructure Health
Schedule:   Hourly at :20 past the hour (20 * * * *)
SLA:        30 minutes

Appends the latest hour's node-health records from Snowflake
NETWORK.NODE_HEALTH_HOURLY to the Redshift analytics layer
(analytics.node_health_hourly) and flags any CRITICAL nodes into the NOC
alert queue.  Runs in append mode so intra-day history is fully preserved.

Steps:
  1. unload_latest_hour  — UNLOAD the last hour's node-health rows from Snowflake to S3
  2. append_to_redshift  — COPY (append) from S3 into analytics.node_health_hourly
  3. flag_critical_nodes — query CRITICAL nodes; upsert into analytics.noc_alert_queue

Upstream:   NETWORK.NODE_HEALTH_HOURLY (Snowflake node-health load, completes at :05)
Downstream: analytics.noc_alert_queue → NOC dashboards
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    get_redshift_hook,
    get_execution_date,
    redshift_copy_from_s3,
    snowflake_unload_to_s3,
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
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "on_failure_callback": nnn_failure_alert,
    "execution_timeout": timedelta(hours=2),
}


def _hourly_s3_prefix(execution_dt: str) -> str:
    """Return the S3 staging prefix for the given execution datetime string.

    The prefix embeds the full ISO hour so that each hourly run lands in a
    unique S3 prefix, preventing cross-hour file collisions.
    """
    # execution_dt format: "2024-01-01T05:00:00+00:00" — safe for S3 paths
    safe_dt = execution_dt.replace(":", "-").replace("+", "plus")
    return f"redshift-staging/node_health_hourly/execution_dt={safe_dt}/"


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def unload_latest_hour(**context: object) -> None:
    """Unload the most recent hour of node-health records from Snowflake to S3.

    Uses the Airflow execution date to derive the hour window so that each
    DAG run selects exactly the records produced in the preceding hour.
    """
    execution_dt = get_execution_date(context)
    s3_prefix = _hourly_s3_prefix(str(execution_dt))

    # Select records from the hour window ending at this execution timestamp
    select_sql = f"""
        SELECT *
        FROM NETWORK.NODE_HEALTH_HOURLY
        WHERE collected_at >= DATEADD('hour', -1, '{execution_dt}')
          AND collected_at <  '{execution_dt}'
    """
    snowflake_unload_to_s3(select_sql=select_sql, s3_prefix=s3_prefix)


def append_to_redshift(**context: object) -> None:
    """COPY (append) staged node-health Parquet files into ``analytics.node_health_hourly``.

    A DELETE for the specific hour window is performed first so that reruns
    for the same execution_dt are idempotent and do not create duplicate rows.
    """
    execution_dt = get_execution_date(context)
    s3_prefix = _hourly_s3_prefix(str(execution_dt))

    # Delete any existing rows for this exact hour window before re-loading
    hook = get_redshift_hook()
    hook.run(
        f"""
        DELETE FROM analytics.node_health_hourly
        WHERE collected_at >= DATEADD('hour', -1, '{execution_dt}')
          AND collected_at <  '{execution_dt}'
        """
    )

    redshift_copy_from_s3(
        table="analytics.node_health_hourly",
        s3_prefix=s3_prefix,
        file_format="PARQUET",
        truncate=False,  # append mode — do not truncate other hours' rows
    )


def flag_critical_nodes(**context: object) -> None:
    """Insert CRITICAL-status nodes from the latest hour into the NOC alert queue.

    Queries ``analytics.node_health_hourly`` for nodes with
    ``health_status = 'CRITICAL'`` recorded in the last hour, then upserts
    them into ``analytics.noc_alert_queue`` using the same hook connection
    to avoid opening a second Redshift connection.

    Existing alerts for the same node within the same hour are updated rather
    than duplicated (DELETE+INSERT via temp table).
    """
    execution_dt = get_execution_date(context)
    hook = get_redshift_hook()

    # PERF: use get_records() (returns a list of tuples) instead of get_pandas_df()
    # which loads all rows into a DataFrame. CRITICAL nodes per hour is typically
    # small, but get_records() avoids pandas overhead on the write path.
    records = hook.get_records(
        f"""
        SELECT
            node_id,
            node_name,
            region,
            health_status,
            health_score,
            collected_at
        FROM analytics.node_health_hourly
        WHERE health_status = 'CRITICAL'
          AND collected_at >= DATEADD('hour', -1, '{execution_dt}')
          AND collected_at <  '{execution_dt}'
        ORDER BY health_score ASC
        """
    )

    if not records:
        return  # no critical nodes this hour — nothing to queue

    # Build VALUES string for batch insert
    # row indices: 0=node_id, 1=node_name, 2=region, 3=health_status, 4=health_score, 5=collected_at
    value_rows = ", ".join(
        f"('{row[0]}', '{row[1]}', '{row[2]}', "
        f"'{row[3]}', {row[4]}, '{row[5]}', GETDATE())"
        for row in records
    )

    # Upsert into NOC alert queue using temp table DELETE+INSERT pattern
    hook.run(
        f"""
        CREATE TEMP TABLE tmp_noc_alerts (LIKE analytics.noc_alert_queue);

        INSERT INTO tmp_noc_alerts
            (node_id, node_name, region, health_status, health_score, collected_at, queued_at)
        VALUES {value_rows};

        DELETE FROM analytics.noc_alert_queue
        USING tmp_noc_alerts
        WHERE analytics.noc_alert_queue.node_id    = tmp_noc_alerts.node_id
          AND analytics.noc_alert_queue.collected_at = tmp_noc_alerts.collected_at;

        INSERT INTO analytics.noc_alert_queue
        SELECT * FROM tmp_noc_alerts;
        """
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_infrastructure_health_redshift_hourly",
    description="Append hourly node-health data from Snowflake to Redshift; flag CRITICAL nodes",
    default_args=default_args,
    schedule_interval="20 * * * *",  # :20 every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "network", "hourly", "redshift"],
) as dag:

    # Step 1 – export the latest hour from Snowflake to S3
    t_unload = PythonOperator(  # Unload NETWORK.NODE_HEALTH_HOURLY for the last hour to S3 Parquet
        task_id="unload_latest_hour",
        python_callable=unload_latest_hour,
        sla=timedelta(minutes=15),
        doc_md="Unload the most recent hour of node-health records from Snowflake to S3.",
    )

    # Step 2 – append to Redshift (no truncate — preserve intra-day history)
    t_append = PythonOperator(  # COPY (append) staged node-health Parquet files into analytics.node_health_hourly
        task_id="append_to_redshift",
        python_callable=append_to_redshift,
        sla=timedelta(minutes=25),
        doc_md="Append staged node-health Parquet files into analytics.node_health_hourly.",
    )

    # Step 3 – populate NOC alert queue for CRITICAL nodes
    t_flag = PythonOperator(  # Upsert CRITICAL-status nodes from this hour into analytics.noc_alert_queue
        task_id="flag_critical_nodes",
        python_callable=flag_critical_nodes,
        sla=timedelta(minutes=30),
        doc_md="Insert CRITICAL-status nodes from this hour into analytics.noc_alert_queue.",
    )

    # Linear dependency chain
    t_unload >> t_append >> t_flag
