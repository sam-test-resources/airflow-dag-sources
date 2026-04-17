"""
nnn_service_qualification_sync
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Owner:      nnn-data-engineering
Domain:     Customer
Schedule:   Every 6 hours (0 */6 * * *)
SLA:        1 hour

Synchronises the NNN Service Qualification (SQ) API's address-level
technology availability data into the Network Inventory Postgres database.

The SQ API exposes which technology (FTTP/FTTC/FTTN/HFC/FW/Satellite)
is available at each address_id, along with predicted speeds and activation
readiness status. This data is used by RSPs and NNN's own service teams
to determine what product a customer can order.

Steps:
  1. Fetch  – paginate through SQ API /addresses/availability endpoint
  2. Diff   – compare against last-known state in Postgres
  3. Apply  – INSERT new records, UPDATE changed records in Postgres
  4. Audit  – write a change summary to Snowflake CUSTOMER.SQ_SYNC_AUDIT

Upstream: NNN SQ API (internal)
Downstream: RSP portal, customer-facing availability checker
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_SQ_API, CONN_POSTGRES_NI,
    get_execution_date, get_snowflake_hook, AEST,
)

log = logging.getLogger(__name__)

default_args = {
    "owner":                    "nnn-data-engineering",
    "depends_on_past":          False,
    "email":                    ["de-alerts@nnnco.com.au"],
    "email_on_failure":         True,
    "email_on_retry":           False,
    "retries":                  3,
    "retry_delay":              timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":          timedelta(minutes=30),
    "on_failure_callback":      nnn_failure_alert,
    "execution_timeout":        timedelta(minutes=50),
}


def fetch_sq_availability(**context) -> None:
    """Paginate through SQ API and collect all address availability records."""
    from airflow.providers.http.hooks.http import HttpHook

    hook    = HttpHook(method="GET", http_conn_id=CONN_SQ_API)
    records, cursor = [], None

    while True:
        params = {"page_size": 1000}
        if cursor:
            params["cursor"] = cursor

        resp   = hook.run("/api/v3/addresses/availability", data=params,
                          headers={"Accept": "application/json"})
        body   = resp.json()
        batch  = body.get("data", [])
        records.extend(batch)

        cursor = body.get("next_cursor")
        if not cursor:
            break

        log.debug("Fetched %d records (running total: %d)", len(batch), len(records))

    log.info("SQ API fetch complete: %d address records", len(records))
    context["ti"].xcom_push(key="sq_records", value=records)


def diff_and_apply(**context) -> None:
    """Compare fetched data against Postgres and apply inserts/updates."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    records = context["ti"].xcom_pull(key="sq_records")
    if not records:
        log.warning("No records from SQ API — skipping diff")
        context["ti"].xcom_push(key="audit", value={"inserted": 0, "updated": 0})
        return

    pg_hook   = PostgresHook(postgres_conn_id=CONN_POSTGRES_NI)
    api_df    = pd.DataFrame(records)
    api_df.columns = [c.lower() for c in api_df.columns]

    # Load current state from Postgres
    existing_df = pg_hook.get_pandas_df(
        "SELECT address_id, technology_type, max_dl_mbps, activation_status, updated_at "
        "FROM public.nnn_service_areas"
    )

    existing = existing_df.set_index("address_id").to_dict(orient="index") if not existing_df.empty else {}

    inserts, updates = [], []
    for _, row in api_df.iterrows():
        addr_id = row["address_id"]
        if addr_id not in existing:
            inserts.append(row)
        else:
            cur = existing[addr_id]
            # Update if technology type, speed, or activation status changed
            if (cur["technology_type"] != row.get("technology_type")
                    or cur["max_dl_mbps"] != row.get("max_dl_mbps")
                    or cur["activation_status"] != row.get("activation_status")):
                updates.append(row)

    # Batch insert new records
    # Use datetime.now() for updated_at — passing "NOW()" as a string would insert
    # the literal text 'NOW()' rather than the current timestamp.
    from datetime import datetime
    now_ts = datetime.now(AEST).isoformat()
    if inserts:
        pg_hook.insert_rows(
            table="public.nnn_service_areas",
            rows=[[r["address_id"], r["technology_type"], r.get("max_dl_mbps"),
                   r.get("max_ul_mbps"), r.get("activation_status"), now_ts]
                  for r in inserts],
            target_fields=["address_id", "technology_type", "max_dl_mbps",
                            "max_ul_mbps", "activation_status", "updated_at"],
        )

    # Update changed records one-by-one (low volume expected)
    for row in updates:
        pg_hook.run(
            """
            UPDATE public.nnn_service_areas
            SET technology_type   = %s,
                max_dl_mbps       = %s,
                max_ul_mbps       = %s,
                activation_status = %s,
                updated_at        = NOW()
            WHERE address_id = %s
            """,
            parameters=(
                row.get("technology_type"), row.get("max_dl_mbps"),
                row.get("max_ul_mbps"), row.get("activation_status"),
                row["address_id"],
            ),
        )

    audit = {"inserted": len(inserts), "updated": len(updates)}
    log.info("SQ sync applied: %d inserts, %d updates", len(inserts), len(updates))
    context["ti"].xcom_push(key="audit", value=audit)


def write_audit(**context) -> None:
    """Record sync summary into Snowflake for trend analysis."""
    audit   = context["ti"].xcom_pull(key="audit")
    exec_ts = get_execution_date(context).isoformat()
    hook    = get_snowflake_hook()

    hook.run(
        """
        INSERT INTO CUSTOMER.SQ_SYNC_AUDIT
            (sync_ts, addresses_inserted, addresses_updated)
        VALUES (%(ts)s, %(ins)s, %(upd)s)
        """,
        parameters={"ts": exec_ts, "ins": audit["inserted"], "upd": audit["updated"]},
    )
    log.info("Audit record written: %s", audit)


with DAG(
    dag_id="nnn_service_qualification_sync",
    description="6-hourly SQ API → Postgres address technology availability sync",
    schedule_interval="0 */6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "customer", "6hourly"],
) as dag:

    t_fetch = PythonOperator(
        task_id="fetch_sq_availability",
        python_callable=fetch_sq_availability,
        sla=timedelta(minutes=30),
    )

    t_diff = PythonOperator(
        task_id="diff_and_apply",
        python_callable=diff_and_apply,
        sla=timedelta(minutes=50),
    )

    t_audit = PythonOperator(
        task_id="write_audit",
        python_callable=write_audit,
    )

    t_fetch >> t_diff >> t_audit
