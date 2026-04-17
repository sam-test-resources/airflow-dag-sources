"""
nnn_outage_incident_etl
~~~~~~~~~~~~~~~~~~~~~~~
Owner:      nnn-data-engineering
Domain:     Network / Operations
Schedule:   Every 15 minutes (*/15 * * * *)
SLA:        None (near-real-time; monitored via Snowflake row-freshness alert)

Polls ServiceNow for network outage incidents updated in the last 15 minutes,
enriches each incident with affected-premises and POI data from Snowflake,
and upserts into OPERATIONS.OUTAGE_INCIDENTS.

Incident states tracked:
  New → In Progress → Pending Vendor → Resolved → Closed

The resolved/closed records feed the ACCC Major Service Failure (MSF)
weekly report (nnn_accc_reporting_weekly).

Steps:
  1. Extract – GET ServiceNow incidents updated since last watermark
  2. Enrich  – join affected_poi with NETWORK.POI_REGISTRY
  3. Classify – tag MSF-eligible records (>2500 premises, duration >1h predicted)
  4. Upsert  – MERGE into OPERATIONS.OUTAGE_INCIDENTS on sys_id
  5. Watermark – update last_run_ts in OPERATIONS.ETL_WATERMARKS
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert
from nnn_common.utils import CONN_SERVICENOW, get_snowflake_hook

log = logging.getLogger(__name__)

default_args = {
    "owner":                    "nnn-data-engineering",
    "depends_on_past":          False,
    "email":                    ["de-alerts@nnnco.com.au", "network-ops@nnnco.com.au"],
    "email_on_failure":         True,
    "email_on_retry":           False,
    "retries":                  3,
    "retry_delay":              timedelta(minutes=1),
    "retry_exponential_backoff": False,      # fixed delay for near-real-time
    "max_retry_delay":          timedelta(minutes=10),
    "on_failure_callback":      nnn_failure_alert,
    "execution_timeout":        timedelta(minutes=10),
}


def get_watermark() -> str:
    """Read last successful run timestamp from ETL watermarks table."""
    hook = get_snowflake_hook()
    row  = hook.get_first("""
        SELECT TO_CHAR(last_run_ts, 'YYYY-MM-DD HH24:MI:SS')
        FROM   OPERATIONS.ETL_WATERMARKS
        WHERE  dag_id = 'nnn_outage_incident_etl'
    """)
    if row:
        return row[0]
    # Bootstrap: look back 15 minutes if no watermark exists
    ts = datetime.now(timezone.utc) - timedelta(minutes=15)
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def extract_servicenow_incidents(**context) -> None:
    """Fetch all outage incidents updated since the watermark."""
    from airflow.providers.http.hooks.http import HttpHook

    watermark = get_watermark()
    hook      = HttpHook(method="GET", http_conn_id=CONN_SERVICENOW)

    # ServiceNow encoded query: category=network AND sys_updated_on > watermark
    sysparm_query = (
        f"category=network^sys_updated_on>{watermark}"
        "^ORDERBYsys_updated_on"
    )
    resp = hook.run(
        endpoint="/api/now/table/incident",
        data={
            "sysparm_query":  sysparm_query,
            "sysparm_fields": (
                "sys_id,number,state,priority,short_description,"
                "affected_poi_id,premises_count,opened_at,resolved_at,closed_at"
            ),
            "sysparm_limit":  500,
        },
        headers={"Accept": "application/json"},
    )

    incidents = resp.json().get("result", [])
    log.info("Fetched %d incidents since watermark %s", len(incidents), watermark)
    context["ti"].xcom_push(key="incidents", value=incidents)


def enrich_incidents(**context) -> None:
    """Join each incident with POI registry and classify MSF eligibility."""
    incidents = context["ti"].xcom_pull(key="incidents")
    if not incidents:
        context["ti"].xcom_push(key="enriched", value=[])
        return

    hook = get_snowflake_hook()
    # Fetch POI metadata for all affected POIs in one query
    poi_ids      = list({i["affected_poi_id"] for i in incidents if i.get("affected_poi_id")})
    placeholders = ", ".join(f"'{p}'" for p in poi_ids)
    poi_df       = hook.get_pandas_df(f"""
        SELECT poi_id, poi_name, state_territory, technology_type
        FROM   NETWORK.POI_REGISTRY
        WHERE  poi_id IN ({placeholders})
    """) if poi_ids else None

    poi_map = {} if poi_df is None else poi_df.set_index("poi_id").to_dict(orient="index")

    enriched = []
    for inc in incidents:
        poi_meta = poi_map.get(inc.get("affected_poi_id"), {})
        # MSF flag: >2500 premises AND not yet resolved within 1 hour
        premises = int(inc.get("premises_count") or 0)
        msf_flag = premises >= 2500 and inc.get("resolved_at") is None

        enriched.append({
            **inc,
            "poi_name":        poi_meta.get("poi_name"),
            "state_territory": poi_meta.get("state_territory"),
            "technology_type": poi_meta.get("technology_type"),
            "msf_eligible":    msf_flag,
        })

    context["ti"].xcom_push(key="enriched", value=enriched)
    log.info("Enriched %d incidents; %d MSF-eligible", len(enriched),
             sum(1 for e in enriched if e["msf_eligible"]))


def upsert_incidents(**context) -> None:
    """MERGE enriched incidents into OPERATIONS.OUTAGE_INCIDENTS."""
    enriched = context["ti"].xcom_pull(key="enriched")
    if not enriched:
        log.info("No incidents to upsert")
        return

    hook = get_snowflake_hook()
    hook.run("CREATE OR REPLACE TEMPORARY TABLE tmp_incidents LIKE OPERATIONS.OUTAGE_INCIDENTS")
    hook.insert_rows(
        table="tmp_incidents",
        rows=[list(r.values()) for r in enriched],
        target_fields=list(enriched[0].keys()),
    )
    hook.run("""
        MERGE INTO OPERATIONS.OUTAGE_INCIDENTS tgt
        USING tmp_incidents src ON tgt.sys_id = src.sys_id
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    log.info("Upserted %d outage incident rows", len(enriched))


def update_watermark(**context) -> None:
    """Stamp current UTC time as the new watermark."""
    hook = get_snowflake_hook()
    hook.run("""
        MERGE INTO OPERATIONS.ETL_WATERMARKS tgt
        USING (SELECT 'nnn_outage_incident_etl' AS dag_id, CURRENT_TIMESTAMP() AS ts) src
          ON tgt.dag_id = src.dag_id
        WHEN MATCHED     THEN UPDATE SET last_run_ts = src.ts
        WHEN NOT MATCHED THEN INSERT (dag_id, last_run_ts) VALUES (src.dag_id, src.ts)
    """)
    log.info("Watermark updated to CURRENT_TIMESTAMP()")


with DAG(
    dag_id="nnn_outage_incident_etl",
    description="Near-real-time ServiceNow network outage incidents → OPERATIONS.OUTAGE_INCIDENTS",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["nnn", "network", "operations", "15min"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract_servicenow_incidents",
        python_callable=extract_servicenow_incidents,
    )

    t_enrich = PythonOperator(
        task_id="enrich_incidents",
        python_callable=enrich_incidents,
    )

    t_upsert = PythonOperator(
        task_id="upsert_incidents",
        python_callable=upsert_incidents,
    )

    t_watermark = PythonOperator(
        task_id="update_watermark",
        python_callable=update_watermark,
        trigger_rule="all_done",   # always advance watermark even on partial failure
    )

    t_extract >> t_enrich >> t_upsert >> t_watermark
