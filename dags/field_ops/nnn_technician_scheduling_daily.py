"""
nnn_technician_scheduling_daily
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Owner:      nnn-data-engineering / Field Operations
Domain:     Field Operations
Schedule:   03:00 AEST daily (17:00 UTC)
SLA:        3 hours (scheduling data must be in Snowflake before 09:00 AEST ops briefing)

Extracts field technician dispatch work orders from ServiceNow, enriches
with technician skill profiles and travel-time estimates, and loads into
Snowflake FIELD_OPS.TECHNICIAN_SCHEDULE for operational dashboards.

Work order types processed:
  - NEW_DEVELOPMENT   : new premises connection (FTTP/HFC node connection)
  - FAULT_REPAIR      : customer-reported fault requiring on-site visit
  - PLANNED_MAINTENANCE: scheduled network maintenance
  - PREMISES_UPGRADE  : technology upgrade (e.g. FTTN→FTTP)

Technician allocation rules (applied in Python, not Airflow branches):
  1. Match technology type to technician certification (FTTP/FTTN/HFC)
  2. Proximity scoring: prefer technicians within 30km of premises
  3. Workload cap: max 8 work orders per technician per day

Steps:
  1. Extract work orders for tomorrow from ServiceNow
  2. Load technician availability from FIELD_OPS.TECHNICIAN_ROSTER
  3. Run allocation algorithm
  4. Load schedule to FIELD_OPS.TECHNICIAN_SCHEDULE
  5. Update ServiceNow work orders with assigned technician (write-back)
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import CONN_SERVICENOW, CONN_SNOWFLAKE, get_run_date, get_snowflake_hook

log = logging.getLogger(__name__)

MAX_ORDERS_PER_TECH = 8
PROXIMITY_RADIUS_KM = 30

default_args = {
    "owner":                    "nnn-data-engineering",
    "depends_on_past":          False,
    "email":                    ["de-alerts@nnnco.com.au", "field-ops@nnnco.com.au"],
    "email_on_failure":         True,
    "email_on_retry":           False,
    "retries":                  3,
    "retry_delay":              timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":          timedelta(minutes=30),
    "on_failure_callback":      nnn_failure_alert,
    "execution_timeout":        timedelta(hours=1, minutes=30),
}


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    """Calculate great-circle distance between two coordinates in km."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def extract_work_orders(**context) -> None:
    """Fetch tomorrow's scheduled work orders from ServiceNow."""
    from airflow.providers.http.hooks.http import HttpHook

    run_date   = get_run_date(context)
    target_day = (pd.Timestamp(run_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    hook = HttpHook(method="GET", http_conn_id=CONN_SERVICENOW)
    resp = hook.run(
        endpoint="/api/now/table/sn_wo_workorder",
        data={
            "sysparm_query":  f"scheduled_date={target_day}^state=1",   # state=1 = Open
            "sysparm_fields": (
                "sys_id,number,type,technology_type,priority,"
                "premises_id,premises_lat,premises_lon,premises_address"
            ),
            "sysparm_limit": 1000,
        },
        headers={"Accept": "application/json"},
    )
    orders = resp.json().get("result", [])
    log.info("Fetched %d work orders for %s", len(orders), target_day)
    context["ti"].xcom_push(key="work_orders", value=orders)
    context["ti"].xcom_push(key="target_day", value=target_day)


def load_technician_availability(**context) -> None:
    """Read tomorrow's available technician roster from Snowflake."""
    target_day = context["ti"].xcom_pull(key="target_day")
    hook       = get_snowflake_hook()

    # NOTE: Snowflake does not support the PostgreSQL @> array containment operator
    # or ARRAY[...]::DATE[] syntax.  Use ARRAY_CONTAINS with PARSE_JSON or a
    # semi-structured VARIANT column, or an ARRAY_CONTAINS with TO_VARIANT cast.
    # Here we use a simple ARRAY_CONTAINS with a string cast, assuming
    # available_dates is stored as a Snowflake ARRAY of date strings.
    df = hook.get_pandas_df("""
        SELECT
            t.technician_id, t.name, t.certifications,
            t.base_lat, t.base_lon, t.max_orders_day,
            COALESCE(r.assigned_orders, 0) AS already_assigned
        FROM FIELD_OPS.TECHNICIAN_PROFILES t
        LEFT JOIN (
            SELECT technician_id, COUNT(*) AS assigned_orders
            FROM   FIELD_OPS.TECHNICIAN_SCHEDULE
            WHERE  schedule_date = %(day)s
            GROUP  BY technician_id
        ) r ON r.technician_id = t.technician_id
        WHERE ARRAY_CONTAINS(%(day)s::VARIANT, t.available_dates)
          AND t.active = TRUE
    """, parameters={"day": target_day})

    df["remaining_capacity"] = df["max_orders_day"].clip(upper=MAX_ORDERS_PER_TECH) - df["already_assigned"]
    df = df[df["remaining_capacity"] > 0]

    log.info("Available technicians for %s: %d", target_day, len(df))
    context["ti"].xcom_push(key="technicians", value=df.to_json(orient="records"))


def allocate_technicians(**context) -> None:
    """Greedy proximity-based allocation of technicians to work orders."""
    orders     = context["ti"].xcom_pull(key="work_orders")
    target_day = context["ti"].xcom_pull(key="target_day")
    techs_df   = pd.read_json(context["ti"].xcom_pull(key="technicians"), orient="records")

    if not orders:
        context["ti"].xcom_push(key="schedule", value=[])
        return

    # Build a mutable capacity tracker
    capacity = techs_df.set_index("technician_id")["remaining_capacity"].to_dict()
    schedule  = []

    for order in sorted(orders, key=lambda o: int(o.get("priority", 3))):   # priority 1 first
        tech_type = order.get("technology_type", "")
        o_lat     = float(order.get("premises_lat") or -33.87)
        o_lon     = float(order.get("premises_lon") or 151.21)

        # Filter eligible technicians: certified + remaining capacity
        eligible = techs_df[
            (techs_df["certifications"].str.contains(tech_type, na=False)) &
            (techs_df["technician_id"].map(capacity) > 0)
        ].copy()

        if eligible.empty:
            log.warning("No eligible technician for work order %s (tech=%s)", order["number"], tech_type)
            continue

        # Proximity scoring
        eligible["distance_km"] = eligible.apply(
            lambda r: haversine_km(r["base_lat"], r["base_lon"], o_lat, o_lon), axis=1
        )
        best = eligible.sort_values("distance_km").iloc[0]

        # Assign
        capacity[best["technician_id"]] -= 1
        schedule.append({
            "schedule_date":    target_day,
            "work_order_id":    order["sys_id"],
            "work_order_number": order["number"],
            "work_order_type":  order.get("type"),
            "technology_type":  tech_type,
            "premises_id":      order.get("premises_id"),
            "technician_id":    best["technician_id"],
            "technician_name":  best["name"],
            "distance_km":      round(best["distance_km"], 1),
            "priority":         order.get("priority"),
        })

    log.info("Allocated %d / %d work orders", len(schedule), len(orders))
    context["ti"].xcom_push(key="schedule", value=schedule)


def load_schedule(**context) -> None:
    """Write the daily schedule to Snowflake FIELD_OPS.TECHNICIAN_SCHEDULE."""
    schedule = context["ti"].xcom_pull(key="schedule")
    if not schedule:
        log.info("Empty schedule — nothing to load")
        return

    hook = get_snowflake_hook()
    hook.insert_rows(
        table="FIELD_OPS.TECHNICIAN_SCHEDULE",
        rows=[list(r.values()) for r in schedule],
        target_fields=list(schedule[0].keys()),
    )
    log.info("Loaded %d schedule rows", len(schedule))


def write_back_to_servicenow(**context) -> None:
    """Update ServiceNow work orders with the assigned technician."""
    from airflow.providers.http.hooks.http import HttpHook

    schedule = context["ti"].xcom_pull(key="schedule")
    if not schedule:
        return

    hook = HttpHook(method="PATCH", http_conn_id=CONN_SERVICENOW)
    for s in schedule:
        hook.run(
            endpoint=f"/api/now/table/sn_wo_workorder/{s['work_order_id']}",
            data={"assigned_to": s["technician_id"], "state": "2"},   # state 2 = In Progress
            headers={"Content-Type": "application/json"},
        )

    log.info("Updated %d ServiceNow work orders with technician assignments", len(schedule))


with DAG(
    dag_id="nnn_technician_scheduling_daily",
    description="Daily field technician work-order allocation with proximity scoring",
    schedule_interval="0 17 * * *",     # 03:00 AEST = 17:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "field-ops", "daily"],
) as dag:

    t_orders     = PythonOperator(task_id="extract_work_orders",          python_callable=extract_work_orders,          sla=timedelta(minutes=30))
    t_roster     = PythonOperator(task_id="load_technician_availability",  python_callable=load_technician_availability, sla=timedelta(minutes=45))
    t_allocate   = PythonOperator(task_id="allocate_technicians",          python_callable=allocate_technicians,         sla=timedelta(hours=1))
    t_load       = PythonOperator(task_id="load_schedule",                 python_callable=load_schedule,                sla=timedelta(hours=2))
    t_writeback  = PythonOperator(task_id="write_back_to_servicenow",      python_callable=write_back_to_servicenow,     sla=timedelta(hours=3))

    t_orders >> t_roster >> t_allocate >> t_load >> t_writeback
