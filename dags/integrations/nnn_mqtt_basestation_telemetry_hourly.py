"""
NNN Airflow DAG — MQTT Fixed Wireless Base Station Telemetry (Hourly)
=====================================================================
Owner:      nnn-data-engineering
Domain:     Network / Fixed Wireless
Schedule:   15 * * * *  (15 min past each hour)
SLA:        30 minutes (must complete by 45 min past the hour)
Description:
    Processes Fixed Wireless base station IoT telemetry that is pre-buffered
    from the MQTT broker into S3 by a Kinesis Firehose pipeline (the MQTT →
    Firehose → S3 path is pre-existing and managed separately). This DAG:

    1. Waits for the hourly S3 telemetry file to land (S3KeySensor).
    2. Downloads the JSON-lines file and batch-INSERTs into Snowflake
       NETWORK.FW_BASESTATION_TELEMETRY.
    3. Validates signal quality — logs a WARNING for stations reporting
       signal_dbm < -100 (poor signal threshold).

    CONN_MQTT is declared as a dependency for lineage purposes; the actual
    MQTT broker connection is used by the Firehose pipeline, not this DAG.

Steps:
    1. check_s3_telemetry_file  — S3KeySensor (mode=reschedule)
    2. load_telemetry           — download JSON-lines from S3, INSERT to Snowflake
    3. validate_signal_quality  — query for poor-signal stations, log warning

Upstream:   MQTT → Kinesis Firehose → S3 (pre-existing pipeline)
Downstream: NETWORK.FW_BASESTATION_TELEMETRY → NOC signal heatmaps, capacity planning
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
    CONN_MQTT,      # Declared for data-lineage documentation
    CONN_S3,
    NNN_S3_BUCKET,
    download_from_s3,
    get_execution_date,
    get_run_date,
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
TARGET_TABLE              = "NETWORK.FW_BASESTATION_TELEMETRY"
S3_ENV                    = "prod"  # Substituted at deploy time via env var or Airflow Variable
POOR_SIGNAL_THRESHOLD_DBM = -100
SF_INSERT_CHUNK_SIZE      = 1_000   # Rows per Snowflake executemany call


def _build_s3_telemetry_key(run_date: str, execution_hour: int) -> str:
    """Build the S3 object key for the Firehose-delivered telemetry file."""
    run_hour = f"{run_date}T{execution_hour:02d}"
    return f"mqtt/fw-telemetry/{run_hour}/data.json"


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def load_telemetry(**context) -> int:
    """Download the JSON-lines telemetry file from S3 and batch-INSERT into Snowflake."""
    execution_dt: datetime = get_execution_date(context)
    run_date: str = get_run_date(context)
    execution_hour: int = execution_dt.hour

    s3_object_key = _build_s3_telemetry_key(run_date, execution_hour)

    # Download the file from S3 to a local temp path
    with tempfile.NamedTemporaryFile(
        suffix=".json",
        prefix=f"fw_telemetry_{run_date}_{execution_hour:02d}_",
        delete=False,
    ) as tmp_file:
        local_path = tmp_file.name

    try:
        download_from_s3(s3_object_key, local_path)

        records: list[dict] = []
        with open(local_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    records.append(record)
                except json.JSONDecodeError:
                    log.warning("Skipping malformed JSON line in telemetry file.")
    finally:
        os.remove(local_path)

    if not records:
        raise ValueError(
            f"Telemetry file {s3_object_key} is empty or unreadable. "
            "Check Firehose delivery for this hour."
        )

    hook = get_snowflake_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # DELETE existing rows for this run_date + hour before inserting (idempotent on reruns)
        cursor.execute(
            f"""
            DELETE FROM {TARGET_TABLE}
            WHERE run_date = %(run_date)s
              AND HOUR(telemetry_ts) = %(execution_hour)s
            """,
            {"run_date": run_date, "execution_hour": execution_hour},
        )

        # Snowflake connector uses named %(name)s param style, not positional %s.
        insert_sql = f"""
            INSERT INTO {TARGET_TABLE} (
                station_id, station_name, region, poi_code,
                signal_dbm, snr_db, connected_devices,
                bandwidth_mbps, uptime_pct, firmware_version,
                telemetry_ts, run_date, loaded_at
            ) VALUES (
                %(station_id)s, %(station_name)s, %(region)s, %(poi_code)s,
                %(signal_dbm)s, %(snr_db)s, %(connected_devices)s,
                %(bandwidth_mbps)s, %(uptime_pct)s, %(firmware_version)s,
                %(telemetry_ts)s, %(run_date)s, CURRENT_TIMESTAMP()
            )
        """
        # Batch in chunks to avoid Snowflake parameter binding limits
        for i in range(0, len(records), SF_INSERT_CHUNK_SIZE):
            chunk = records[i : i + SF_INSERT_CHUNK_SIZE]
            rows = [
                {
                    "station_id":       r.get("station_id"),
                    "station_name":     r.get("station_name"),
                    "region":           r.get("region"),
                    "poi_code":         r.get("poi_code"),
                    "signal_dbm":       r.get("signal_dbm"),
                    "snr_db":           r.get("snr_db"),
                    "connected_devices": r.get("connected_devices"),
                    "bandwidth_mbps":   r.get("bandwidth_mbps"),
                    "uptime_pct":       r.get("uptime_pct"),
                    "firmware_version": r.get("firmware_version"),
                    "telemetry_ts":     r.get("telemetry_ts"),
                    "run_date":         run_date,
                }
                for r in chunk
            ]
            cursor.executemany(insert_sql, rows)

        conn.commit()
    finally:
        cursor.close()
        conn.close()

    context["ti"].xcom_push(key="record_count", value=len(records))
    return len(records)


def validate_signal_quality(**context) -> None:
    """Query Snowflake for base stations with critically poor signal and log a warning.

    This is a soft validation — it does not fail the DAG but surfaces signal
    quality issues for NOC follow-up via the Airflow task logs.
    """
    run_date: str = get_run_date(context)
    execution_dt: datetime = get_execution_date(context)
    execution_hour: int = execution_dt.hour

    hook = get_snowflake_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Snowflake connector uses named %(name)s param style, not positional %s.
        cursor.execute(
            f"""
            SELECT
                COUNT(DISTINCT station_id)     AS poor_signal_count,
                MIN(signal_dbm)                AS worst_signal_dbm,
                ARRAY_AGG(DISTINCT station_id) AS station_ids
            FROM {TARGET_TABLE}
            WHERE run_date         = %(run_date)s
              AND HOUR(telemetry_ts) = %(execution_hour)s
              AND signal_dbm         < %(threshold)s
            """,
            {"run_date": run_date, "execution_hour": execution_hour, "threshold": POOR_SIGNAL_THRESHOLD_DBM},
        )
        row = cursor.fetchone()
    finally:
        cursor.close()
        conn.close()

    if row and row[0]:
        poor_count, worst_dbm, station_ids = row
        log.warning(
            "SIGNAL QUALITY ALERT: %d base station(s) reported signal_dbm < %d "
            "for run_date=%s hour=%d. Worst signal: %s dBm. Affected stations: %s",
            poor_count,
            POOR_SIGNAL_THRESHOLD_DBM,
            run_date,
            execution_hour,
            worst_dbm,
            station_ids,
        )
    else:
        log.info(
            "Signal quality check passed — no stations below %d dBm for %s hour %d.",
            POOR_SIGNAL_THRESHOLD_DBM,
            run_date,
            execution_hour,
        )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_mqtt_basestation_telemetry_hourly",
    default_args=default_args,
    description="Hourly FW base station telemetry from S3 (MQTT→Firehose) into Snowflake NETWORK schema",
    schedule_interval="15 * * * *",  # 15 min past each hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "network", "fixed-wireless", "mqtt", "telemetry", "snowflake", "hourly"],
) as dag:
    from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

    # Step 1: Sensor — wait for Firehose to deliver the telemetry file to S3
    # Uses reschedule mode so the worker slot is released while waiting
    t_sense = S3KeySensor(
        task_id="check_s3_telemetry_file",
        bucket_name=NNN_S3_BUCKET,
        # Template the S3 key using Jinja — resolves run_date and execution_hour at runtime
        bucket_key=(
            "mqtt/fw-telemetry/{{ ds }}"
            "T{{ execution_date.strftime('%H') }}/data.json"
        ),
        aws_conn_id=CONN_S3,
        poke_interval=60,       # check every 60 seconds
        timeout=600,            # give Firehose up to 10 minutes to deliver
        mode="reschedule",      # release worker slot between pokes
        sla=timedelta(minutes=30),
    )

    # Step 2: Download telemetry JSON-lines from S3 and INSERT into Snowflake
    t_load = PythonOperator(
        task_id="load_telemetry",
        python_callable=load_telemetry,
    )

    # Step 3: Check signal quality — logs warning for poor-signal stations
    t_validate = PythonOperator(
        task_id="validate_signal_quality",
        python_callable=validate_signal_quality,
    )

    t_sense >> t_load >> t_validate
