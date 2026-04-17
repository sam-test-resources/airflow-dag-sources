"""
nnn_common.alerts
~~~~~~~~~~~~~~~~~
Centralised failure and SLA-miss callbacks used across all NNN DAGs.

Channels:
  - PagerDuty (P2 by default; P1 for SLA breaches on billing/compliance DAGs)
  - Slack  #de-alerts channel
  - Email  de-alerts@nnnco.com.au

Environment variables expected:
  NNN_PAGERDUTY_ROUTING_KEY   – PagerDuty Events v2 integration key
  NNN_SLACK_WEBHOOK_URL       – Slack incoming webhook URL
  NNN_ENV                     – "prod" | "staging" | "dev"
"""

import os
import json
import logging
import urllib.request
from datetime import datetime

log = logging.getLogger(__name__)

_SLACK_WEBHOOK  = os.getenv("NNN_SLACK_WEBHOOK_URL", "")
_PD_ROUTING_KEY = os.getenv("NNN_PAGERDUTY_ROUTING_KEY", "")
_ENV            = os.getenv("NNN_ENV", "prod")


def _post_slack(message: str) -> None:
    if not _SLACK_WEBHOOK:
        log.warning("NNN_SLACK_WEBHOOK_URL not set – skipping Slack alert")
        return
    payload = json.dumps({"text": message}).encode()
    req = urllib.request.Request(_SLACK_WEBHOOK, data=payload,
                                 headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as exc:
        log.error("Slack alert failed: %s", exc)


def _post_pagerduty(summary: str, severity: str = "error", dedup_key: str = "") -> None:
    if not _PD_ROUTING_KEY:
        log.warning("NNN_PAGERDUTY_ROUTING_KEY not set – skipping PagerDuty alert")
        return
    payload = json.dumps({
        "routing_key": _PD_ROUTING_KEY,
        "event_action": "trigger",
        "dedup_key": dedup_key or summary[:64],
        "payload": {
            "summary": summary,
            "severity": severity,
            "source": f"airflow-{_ENV}",
            "timestamp": datetime.utcnow().isoformat() + "Z",
        },
    }).encode()
    req = urllib.request.Request(
        "https://events.pagerduty.com/v2/enqueue",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as exc:
        log.error("PagerDuty alert failed: %s", exc)


def nnn_failure_alert(context: dict) -> None:
    """Standard on_failure_callback for all NNN DAGs."""
    dag_id   = context["dag"].dag_id
    task_id  = context["task_instance"].task_id
    run_id   = context["run_id"]
    log_url  = context["task_instance"].log_url

    summary = (
        f"[{_ENV.upper()}] DAG FAILED: {dag_id} | task: {task_id} | run: {run_id}"
    )
    slack_msg = (
        f":red_circle: *NNN Airflow Failure* ({_ENV})\n"
        f"*DAG:* `{dag_id}`   *Task:* `{task_id}`\n"
        f"*Run:* `{run_id}`\n"
        f"<{log_url}|View logs>"
    )

    log.error(summary)
    _post_slack(slack_msg)
    _post_pagerduty(summary, severity="error", dedup_key=f"{dag_id}/{task_id}")


def nnn_post_slack_message(message: str) -> None:
    """Public helper — post a plain-text message to the NNN #de-alerts Slack channel.

    Use this instead of importing the private ``_post_slack`` function directly.
    Designed for DAG-level trend alerts and custom notifications that fall outside
    the standard failure / SLA-miss callback patterns.
    """
    _post_slack(message)


def nnn_sla_miss_alert(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """SLA miss callback — fires PagerDuty critical for billing/compliance DAGs."""
    dag_id = dag.dag_id
    task_ids = [t.task_id for t in (task_list or [])]

    summary = (
        f"[{_ENV.upper()}] SLA MISSED: {dag_id} | tasks: {task_ids}"
    )
    slack_msg = (
        f":warning: *NNN Airflow SLA Miss* ({_ENV})\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Tasks past SLA:* {task_ids}"
    )

    # Billing and compliance DAGs get a critical (P1) page
    severity = "critical" if any(kw in dag_id for kw in ("billing", "revenue", "accc", "compliance")) else "error"

    log.error(summary)
    _post_slack(slack_msg)
    _post_pagerduty(summary, severity=severity, dedup_key=f"sla/{dag_id}")
