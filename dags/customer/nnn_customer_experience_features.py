"""
nnn_customer_experience_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Owner:      nnn-data-engineering / ML Platform
Domain:     Customer / ML
Schedule:   04:00 AEST daily (18:00 UTC)
SLA:        5 hours (feature store must be ready before ML training jobs at 09:00 AEST)

Builds the daily feature store for the NNN Customer Experience (CX) ML model,
which predicts the probability of an RSP's customer lodging a complaint or
churning within the next 30 days based on service quality signals.

Feature groups produced:
  - connectivity     : speed test pass rate, dropouts/week, latency trend
  - support_history  : tickets opened/closed, escalation rate, resolution time
  - activation_exp   : time-to-activate, re-attempt count, fail-reason distribution
  - network_quality  : POI congestion exposure, outage minutes experienced
  - tenure_billing   : months on-nnn, product tier, CVC entitlement

All features are indexed by (service_id, feature_date) and written to
Snowflake ML.CX_FEATURE_STORE as TYPE 1 (overwrite on re-run).

Steps:
  1. Extract connectivity features from NETWORK.SPEED_TEST_RESULTS
  2. Extract support features from OPERATIONS.OUTAGE_INCIDENTS + CUSTOMER.TICKETS
  3. Extract activation experience from WHOLESALE.RSP_ACTIVATIONS
  4. Extract network quality from NETWORK.CVC_UTILISATION_HOURLY
  5. Join all feature groups on service_id
  6. Validate feature completeness (no nulls on critical columns)
  7. Overwrite ML.CX_FEATURE_STORE partition for feature_date
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import CONN_SNOWFLAKE, get_run_date, get_snowflake_hook

log = logging.getLogger(__name__)

default_args = {
    "owner":                    "nnn-data-engineering",
    "depends_on_past":          False,
    "email":                    ["de-alerts@nnnco.com.au", "ml-platform@nnnco.com.au"],
    "email_on_failure":         True,
    "email_on_retry":           False,
    "retries":                  2,
    "retry_delay":              timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay":          timedelta(minutes=30),
    "on_failure_callback":      nnn_failure_alert,
    "execution_timeout":        timedelta(hours=2),
}

# ── Feature extraction SQLs ────────────────────────────────────────────────────

SQL_CONNECTIVITY = """
CREATE OR REPLACE TEMPORARY TABLE tmp_feat_connectivity AS
SELECT
    s.service_id,
    AVG(CASE WHEN s.result = 'PASS' THEN 1.0 ELSE 0.0 END)  AS speed_test_pass_rate_28d,
    COUNT(DISTINCT CASE WHEN s.result = 'FAIL' THEN s.test_date END) AS speed_fail_days_28d,
    AVG(s.latency_ms)                                        AS avg_latency_ms_28d,
    STDDEV(s.latency_ms)                                     AS stddev_latency_ms_28d,
    SUM(s.dropout_count)                                     AS total_dropouts_28d
FROM NETWORK.SPEED_TEST_RESULTS s
WHERE s.test_date BETWEEN DATEADD(day, -28, '{{ ds }}'::DATE) AND '{{ ds }}'::DATE
GROUP BY s.service_id
"""

SQL_SUPPORT = """
CREATE OR REPLACE TEMPORARY TABLE tmp_feat_support AS
SELECT
    t.service_id,
    COUNT(*)                                                           AS tickets_28d,
    SUM(CASE WHEN t.escalated THEN 1 ELSE 0 END)                      AS escalations_28d,
    AVG(DATEDIFF('hour', t.created_at, t.resolved_at))                AS avg_resolution_hrs_28d,
    SUM(CASE WHEN t.category = 'OUTAGE' THEN t.outage_duration_mins ELSE 0 END)
                                                                       AS outage_mins_experienced_28d
FROM CUSTOMER.SUPPORT_TICKETS t
WHERE t.created_at >= DATEADD(day, -28, '{{ ds }}'::DATE)
GROUP BY t.service_id
"""

SQL_ACTIVATION_EXP = """
CREATE OR REPLACE TEMPORARY TABLE tmp_feat_activation AS
SELECT
    service_id,
    days_to_activate,
    sla_met,
    COALESCE(fail_reason_code, 'NONE')  AS last_fail_reason
FROM WHOLESALE.RSP_ACTIVATIONS
WHERE run_date = (
    SELECT MAX(run_date) FROM WHOLESALE.RSP_ACTIVATIONS WHERE run_date <= '{{ ds }}'
)
"""

SQL_NETWORK_QUALITY = """
CREATE OR REPLACE TEMPORARY TABLE tmp_feat_network AS
SELECT
    svc.service_id,
    AVG(cvc.utilisation_pct)        AS avg_poi_utilisation_28d,
    MAX(cvc.utilisation_pct)        AS max_poi_utilisation_28d,
    SUM(cvc.congestion_flag::INT)   AS congested_hours_28d
FROM NETWORK.CVC_UTILISATION_HOURLY cvc
JOIN CUSTOMER.SERVICE_POI_MAPPING   svc ON svc.poi_id = cvc.poi_id
WHERE cvc.hour_label >= DATEADD(day, -28, '{{ ds }}'::DATE)
GROUP BY svc.service_id
"""

SQL_JOIN_AND_LOAD = """
INSERT OVERWRITE INTO ML.CX_FEATURE_STORE
SELECT
    COALESCE(c.service_id, s.service_id, n.service_id)  AS service_id,
    '{{ ds }}'::DATE                                      AS feature_date,
    c.speed_test_pass_rate_28d,
    c.speed_fail_days_28d,
    c.avg_latency_ms_28d,
    c.stddev_latency_ms_28d,
    c.total_dropouts_28d,
    s.tickets_28d,
    s.escalations_28d,
    s.avg_resolution_hrs_28d,
    s.outage_mins_experienced_28d,
    a.days_to_activate,
    a.sla_met             AS activation_sla_met,
    a.last_fail_reason,
    n.avg_poi_utilisation_28d,
    n.max_poi_utilisation_28d,
    n.congested_hours_28d,
    CURRENT_TIMESTAMP()   AS feature_created_at
FROM       tmp_feat_connectivity c
FULL JOIN  tmp_feat_support      s ON s.service_id = c.service_id
FULL JOIN  tmp_feat_activation   a ON a.service_id = COALESCE(c.service_id, s.service_id)
FULL JOIN  tmp_feat_network      n ON n.service_id = COALESCE(c.service_id, s.service_id, a.service_id)
WHERE feature_date = '{{ ds }}'::DATE
"""


def build_feature_store(**context) -> None:
    """Run all 4 feature extraction SQLs + join/load in a SINGLE Snowflake session.

    Snowflake TEMPORARY TABLEs are session-scoped.  Each Airflow task runs in its
    own process (separate connection), so temp tables created in one task are
    invisible to any downstream task.  All SQL must execute on the same connection.
    """
    run_date = get_run_date(context)
    hook     = get_snowflake_hook()

    # Render the {{ ds }} Jinja placeholder (only available in Operator templates,
    # not in PythonOperator callables).
    def render(sql: str) -> str:
        return sql.replace("{{ ds }}", run_date)

    conn   = hook.get_conn()
    cursor = conn.cursor()
    try:
        # Steps 1–4 run in parallel conceptually; here they are sequential within
        # one session so each temp table is visible when SQL_JOIN_AND_LOAD runs.
        for sql in [SQL_CONNECTIVITY, SQL_SUPPORT, SQL_ACTIVATION_EXP, SQL_NETWORK_QUALITY]:
            cursor.execute(render(sql))
            log.info("Executed feature extraction SQL (session shared)")

        # Step 5: join all temp tables and INSERT OVERWRITE into the feature store
        cursor.execute(render(SQL_JOIN_AND_LOAD))
        conn.commit()
        log.info("Feature store INSERT OVERWRITE complete for %s", run_date)
    finally:
        cursor.close()
        conn.close()


def validate_features(**context) -> None:
    """Ensure no nulls on critical feature columns and minimum row count."""
    run_date = get_run_date(context)
    hook     = get_snowflake_hook()

    row = hook.get_first("""
        SELECT COUNT(*),
               SUM(CASE WHEN service_id IS NULL THEN 1 ELSE 0 END) AS null_ids
        FROM ML.CX_FEATURE_STORE
        WHERE feature_date = %(d)s
    """, parameters={"d": run_date})

    total, null_ids = row
    log.info("Feature store: %d rows for %s (null service_ids: %d)", total, run_date, null_ids)

    if total < 10_000:
        raise ValueError(f"Feature row count too low: {total} (expected ≥ 10,000)")
    if null_ids > 0:
        raise ValueError(f"Null service_ids found in feature store: {null_ids}")


with DAG(
    dag_id="nnn_customer_experience_features",
    description="Daily CX ML feature store build (connectivity, support, activation, network quality)",
    schedule_interval="0 18 * * *",     # 04:00 AEST = 18:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "customer", "ml", "daily"],
) as dag:

    # All 4 feature extracts + join run in ONE task sharing a single Snowflake
    # session.  Snowflake TEMPORARY TABLEs are session-scoped; separating extracts
    # into parallel SnowflakeOperator tasks (each with its own connection) would
    # make the temp tables invisible when SQL_JOIN_AND_LOAD runs.
    t_build = PythonOperator(
        task_id="build_feature_store",
        python_callable=build_feature_store,
        sla=timedelta(hours=3),
    )

    t_validate = PythonOperator(
        task_id="validate_features",
        python_callable=validate_features,
        sla=timedelta(hours=4),
    )

    t_build >> t_validate
