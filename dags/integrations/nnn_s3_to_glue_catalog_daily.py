"""
nnn_s3_to_glue_catalog_daily
-------------------------------
Owner:      nnn-data-engineering
Domain:     Data Lake / Governance
Schedule:   Daily at 09:00 AEST (0 9 * * *)
SLA:        1 hour

Keeps the AWS Glue Data Catalog in sync with new daily partitions that land in
the NNN S3 data lake.  After partition registration the Glue crawler is run so
Athena and Redshift Spectrum can immediately query the new data.

Steps:
  1. list_new_partitions      — scan S3 under each domain prefix for today's
                                run_date and build partition specs; push to XCom
  2. register_glue_partitions — call batch_create_partition for each domain/table
  3. run_glue_crawler         — start nnn-datalake-crawler, poll until READY
                                (max 10 min)

Upstream:   Daily ETL DAGs that write Parquet to S3
Downstream: Athena / Redshift Spectrum consumers
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from nnn_common.alerts import nnn_failure_alert, nnn_sla_miss_alert
from nnn_common.utils import (
    CONN_GLUE,
    CONN_S3,
    NNN_S3_BUCKET,
    get_run_date,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner":                     "nnn-data-engineering",
    "depends_on_past":           False,
    "email":                     ["de-alerts@nnnco.com.au"],
    "email_on_failure":          True,
    "email_on_retry":            False,
    "retries":                   2,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":           timedelta(minutes=30),
    "on_failure_callback":       nnn_failure_alert,
    "execution_timeout":         timedelta(minutes=30),
}

GLUE_CRAWLER_NAME      = "nnn-datalake-crawler"
GLUE_DATABASE          = "nnn_datalake"
CRAWLER_POLL_INTERVAL_SEC = 30
CRAWLER_MAX_WAIT_SEC   = 600  # 10 minutes

# Domain → list of table names registered in the Glue catalog
DOMAIN_TABLES: dict[str, list[str]] = {
    "network":    ["link_utilisation", "fault_events", "node_inventory"],
    "customer":   ["service_eligibility", "service_availability", "customer_services"],
    "wholesale":  ["partner_data_ingest", "rsp_manual_reports", "wholesale_orders"],
    "finance":    ["invoice_summary", "payment_events", "revenue_daily"],
    "operations": ["order_events", "daily_network_health_summary", "ticket_summary"],
}


# ---------------------------------------------------------------------------
# Task functions (module-level so the scheduler does not re-define them on
# every DAG parse cycle)
# ---------------------------------------------------------------------------

def list_new_partitions(**context) -> None:
    """Scan S3 for today's partitions across all domains and push partition specs to XCom."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    run_date = get_run_date(context)
    hook     = S3Hook(aws_conn_id=CONN_S3)

    partition_specs = []

    for domain, tables in DOMAIN_TABLES.items():
        for table in tables:
            prefix = f"{domain}/{table}/run_date={run_date}/"
            keys   = hook.list_keys(bucket_name=NNN_S3_BUCKET, prefix=prefix)

            if keys:
                partition_specs.append(
                    {
                        "domain":    domain,
                        "table":     table,
                        "run_date":  run_date,
                        "s3_prefix": prefix,
                        "s3_uri":    f"s3://{NNN_S3_BUCKET}/{prefix}",
                    }
                )
                log.info("Found partition: %s (%d objects)", prefix, len(keys))
            else:
                log.info("No objects found for %s — skipping.", prefix)

    context["ti"].xcom_push(key="partition_specs", value=partition_specs)
    log.info("Total new partitions identified: %d", len(partition_specs))


def register_glue_partitions(**context) -> None:
    """Call Glue batch_create_partition for each domain/table partition spec."""
    from collections import defaultdict

    from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

    partition_specs = context["ti"].xcom_pull(
        task_ids="list_new_partitions", key="partition_specs"
    )
    if not partition_specs:
        log.info("No new partitions to register — skipping.")
        return

    hook  = AwsBaseHook(aws_conn_id=CONN_GLUE, client_type="glue")
    glue  = hook.get_client_type("glue")

    # Group specs by (domain, table) for batch calls
    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for spec in partition_specs:
        grouped[(spec["domain"], spec["table"])].append(spec)

    registered = 0
    for (domain, table), specs in grouped.items():
        partition_input_list = [
            {
                "Values": [spec["run_date"]],
                "StorageDescriptor": {
                    "Location":     spec["s3_uri"],
                    "InputFormat":  "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    },
                },
            }
            for spec in specs
        ]

        try:
            response = glue.batch_create_partition(
                DatabaseName=GLUE_DATABASE,
                TableName=table,
                PartitionInputList=partition_input_list,
            )
            errors = response.get("Errors", [])
            if errors:
                # AlreadyExistsException is acceptable (idempotent run)
                non_exists = [
                    e for e in errors
                    if e.get("ErrorDetail", {}).get("ErrorCode") != "AlreadyExistsException"
                ]
                if non_exists:
                    log.warning(
                        "Glue partition registration errors for %s.%s: %s",
                        domain, table, non_exists,
                    )
            registered += len(partition_input_list)
            log.info(
                "Registered %d partition(s) for %s.%s",
                len(partition_input_list), GLUE_DATABASE, table,
            )
        except glue.exceptions.EntityNotFoundException:
            log.warning("Glue table %s.%s not found — skipping.", GLUE_DATABASE, table)

    log.info("Total partitions registered in Glue catalog: %d", registered)


def run_glue_crawler(**context) -> None:
    """Start the nnn-datalake-crawler and poll until READY state (max 10 min)."""
    from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

    hook = AwsBaseHook(aws_conn_id=CONN_GLUE, client_type="glue")
    glue = hook.get_client_type("glue")

    # Start the crawler
    glue.start_crawler(Name=GLUE_CRAWLER_NAME)
    log.info("Started Glue crawler '%s'", GLUE_CRAWLER_NAME)

    # Poll until READY or timeout
    elapsed = 0
    while elapsed < CRAWLER_MAX_WAIT_SEC:
        time.sleep(CRAWLER_POLL_INTERVAL_SEC)
        elapsed += CRAWLER_POLL_INTERVAL_SEC

        response = glue.get_crawler(Name=GLUE_CRAWLER_NAME)
        state    = response["Crawler"]["State"]
        log.info("Crawler state: %s (elapsed %ds)", state, elapsed)

        if state == "READY":
            last_crawl = response["Crawler"].get("LastCrawl", {})
            status     = last_crawl.get("Status", "UNKNOWN")
            log.info("Crawler finished with last crawl status: %s", status)
            if status == "FAILED":
                raise RuntimeError(f"Glue crawler '{GLUE_CRAWLER_NAME}' last crawl FAILED.")
            return

    raise TimeoutError(
        f"Glue crawler '{GLUE_CRAWLER_NAME}' did not reach READY within "
        f"{CRAWLER_MAX_WAIT_SEC}s."
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nnn_s3_to_glue_catalog_daily",
    description="Register new S3 partitions in AWS Glue Data Catalog and run Glue crawler",
    default_args=default_args,
    schedule_interval="0 9 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=nnn_sla_miss_alert,
    tags=["nnn", "datalake", "glue", "s3", "catalog", "daily"],
) as dag:

    task_list = PythonOperator(  # Scan S3 domain prefixes for today's run_date partitions
        task_id="list_new_partitions",
        python_callable=list_new_partitions,
    )

    task_register = PythonOperator(  # Call Glue batch_create_partition for each domain/table
        task_id="register_glue_partitions",
        python_callable=register_glue_partitions,
    )

    task_crawl = PythonOperator(  # Start nnn-datalake-crawler and poll until READY (max 10 min)
        task_id="run_glue_crawler",
        python_callable=run_glue_crawler,
    )

    # ------------------------------------------------------------------
    # Dependency chain
    # ------------------------------------------------------------------
    task_list >> task_register >> task_crawl
