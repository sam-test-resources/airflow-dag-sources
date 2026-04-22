# Integration Patterns Catalog

**Audience:** Developers building new source/target integration DAGs.  
**How to use:** Find your source and target system in the table of contents. Each pattern includes the rationale, required provider packages, a production-grade code skeleton, and critical gotchas drawn from the existing integration DAGs.

---

## Table of Contents

**Ingest patterns (→ Snowflake / Redshift)**
1. [SFTP / SSH File Drop](#1-sftp--ssh-file-drop)
2. [REST API (HTTP/S)](#2-rest-api-https)
3. [GraphQL API](#3-graphql-api)
4. [SOAP / XML API](#4-soap--xml-api)
5. [MongoDB](#5-mongodb)
6. [Elasticsearch (read)](#6-elasticsearch-read)
7. [AWS DynamoDB (read)](#7-aws-dynamodb-read)
8. [AWS Kinesis Data Stream](#8-aws-kinesis-data-stream)
9. [AWS SQS](#9-aws-sqs)
10. [MQTT / IoT Telemetry (via S3 Firehose)](#10-mqtt--iot-telemetry-via-s3-firehose)
11. [RabbitMQ](#11-rabbitmq)
12. [Redis Cache Scan](#12-redis-cache-scan)
13. [IMAP Email + Attachment](#13-imap-email--attachment)
14. [Azure Blob Storage](#14-azure-blob-storage)
15. [MS SQL Server](#15-ms-sql-server)

**Export patterns (Snowflake →)**
16. [Snowflake → S3 Parquet](#16-snowflake--s3-parquet)
17. [Snowflake → Redshift (via S3)](#17-snowflake--redshift-via-s3)
18. [Snowflake → AWS DynamoDB](#18-snowflake--aws-dynamodb)
19. [Snowflake → Elasticsearch](#19-snowflake--elasticsearch)
20. [Snowflake → RDS PostgreSQL](#20-snowflake--rds-postgresql)
21. [Snowflake → Google Cloud Storage](#21-snowflake--google-cloud-storage)
22. [S3 → AWS Glue Data Catalog](#22-s3--aws-glue-data-catalog)

---

## General Rules That Apply to All Patterns

Before reading the individual patterns, understand these cross-cutting rules:

1. **Idempotency is mandatory.** Every load task must be safe to re-run for the same `run_date`. Use DELETE + INSERT or MERGE — never append-only INSERT.
2. **Large payloads go to temp files.** If a task might produce >100 rows or >1 MB, write to `tempfile.NamedTemporaryFile`, push the path via XCom, and delete the file after consumption.
3. **Snowflake params are named `%(name)s` with dict.** Redshift/Postgres/MSSQL params are positional `%s` with tuple.
4. **Provider hooks import inside the function.** Provider operators import at module level.
5. **Always close Snowflake connections in `finally`.** `cursor.close(); conn.close()`.

---

## 1. SFTP / SSH File Drop

**Reference DAG:** `dags/integrations/nnn_sftp_rsp_billing_ingest_daily.py`  
**Pattern:** Sensor → Download → Transform → Load → Validate  
**Provider package:** `apache-airflow-providers-ssh`

### When to use
A third party drops files to an SFTP server on a schedule. You poll for the file, download it, process it, and load to Snowflake.

### Key hook
```python
from airflow.providers.ssh.hooks.ssh import SSHHook

hook       = SSHHook(ssh_conn_id=CONN_SFTP_RSP)
ssh_client = hook.get_conn()
sftp       = ssh_client.open_sftp()
sftp.get(remote_path, local_path)
sftp.close()
ssh_client.close()
```

### S3 staging (for Snowflake COPY)
Download → local `/tmp/` → `upload_to_s3()` → Snowflake `COPY INTO ... FROM '@stage/<key>'`

### Gotchas
- **Check file existence** before downloading — use `sftp.stat(remote_path)` and raise `FileNotFoundError` if absent; Airflow retries will re-check.
- **Archive processed files** on the remote after successful load: `sftp.rename(remote_path, archive_path)`.
- **Skip header row** in Snowflake COPY: `FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)`.

---

## 2. REST API (HTTP/S)

**Reference DAGs:** `nnn_network_performance_daily.py`, `nnn_graphql_partner_portal_daily.py`  
**Provider package:** `apache-airflow-providers-http`

### Key hook
```python
from airflow.providers.http.hooks.http import HttpHook

hook     = HttpHook(http_conn_id=CONN_NMS_API, method="GET")
response = hook.run(
    endpoint="/api/v2/metrics",
    data={"from": window_start, "to": window_end},
    headers={"Authorization": f"Bearer {api_key}"},
)
response.raise_for_status()
data = response.json()
```

### Pagination pattern
```python
page, all_records = 1, []
while True:
    resp  = hook.run(endpoint=f"/api/v2/items?page={page}&size=200")
    items = resp.json().get("items", [])
    if not items:
        break
    all_records.extend(items)
    page += 1
```

### Connection setup (Airflow UI)
| Field | Value |
|---|---|
| Connection Type | HTTP |
| Host | `api.example.com` |
| Schema | `https` |
| Extra | `{"Authorization": "Bearer <token>"}` |

The `HttpHook` reads `host`, `schema`, and the `extra` JSON as default headers.

### Gotchas
- Always call `response.raise_for_status()` — `HttpHook.run()` does not raise on 4xx/5xx by default.
- Use `extra_options={"verify": False}` only as a last resort for self-signed certs in dev.
- Rate-limit: add `time.sleep(0.1)` between paginated calls if the API enforces rate limits.

---

## 3. GraphQL API

**Reference DAG:** `dags/integrations/nnn_graphql_partner_portal_daily.py`  
**Provider package:** `apache-airflow-providers-http`

### Key pattern
GraphQL is just HTTP POST with a JSON body containing `query` and `variables`:

```python
def fetch_from_graphql(**context) -> None:
    from airflow.providers.http.hooks.http import HttpHook

    run_date = get_run_date(context)
    hook     = HttpHook(http_conn_id=CONN_NMS_API, method="POST")

    query = """
        query PartnerActivity($date: String!, $limit: Int!, $offset: Int!) {
            partnerActivities(date: $date, limit: $limit, offset: $offset) {
                partnerId
                eventType
                quantity
                recordedAt
            }
        }
    """

    all_records, offset, page_size = [], 0, 200
    while True:
        response = hook.run(
            endpoint="/graphql",
            data=json.dumps({
                "query": query,
                "variables": {"date": run_date, "limit": page_size, "offset": offset},
            }),
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        payload = response.json()

        # GraphQL errors are returned as HTTP 200 with an "errors" key
        if "errors" in payload:
            raise RuntimeError(f"GraphQL errors: {payload['errors']}")

        items = payload.get("data", {}).get("partnerActivities", [])
        if not items:
            break
        all_records.extend(items)
        offset += page_size
```

### Gotchas
- GraphQL always returns HTTP 200, even for errors. **Always check `payload.get("errors")`**.
- Use `FIELD_NAMING` conventions: GraphQL camelCase → convert to snake_case for Snowflake columns.
- Some GraphQL APIs use cursor-based pagination (`pageInfo.endCursor`) instead of offset — adapt the loop.

---

## 4. SOAP / XML API

**Reference DAG:** `dags/integrations/nnn_soap_mediation_cdr_daily.py`  
**Provider package:** `apache-airflow-providers-http` (send raw XML via HttpHook)

### Key pattern
```python
def fetch_cdr_records(**context) -> None:
    from airflow.providers.http.hooks.http import HttpHook

    run_date = get_run_date(context)
    hook     = HttpHook(http_conn_id=CONN_NMS_API, method="POST")

    soap_envelope = f"""<?xml version="1.0" encoding="UTF-8"?>
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                      xmlns:cdr="http://nnn.com.au/mediation/cdr">
      <soapenv:Header/>
      <soapenv:Body>
        <cdr:GetCDRRequest>
          <cdr:StartDate>{run_date}T00:00:00</cdr:StartDate>
          <cdr:EndDate>{run_date}T23:59:59</cdr:EndDate>
          <cdr:MaxRecords>10000</cdr:MaxRecords>
        </cdr:GetCDRRequest>
      </soapenv:Body>
    </soapenv:Envelope>"""

    response = hook.run(
        endpoint="/MediationService",
        data=soap_envelope.encode("utf-8"),
        headers={
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction":   "http://nnn.com.au/mediation/cdr/GetCDR",
        },
    )
    response.raise_for_status()

    # Parse SOAP response with ElementTree
    import xml.etree.ElementTree as ET
    root = ET.fromstring(response.text)
    ns   = {"cdr": "http://nnn.com.au/mediation/cdr"}

    records = []
    for item in root.findall(".//cdr:CDRRecord", ns):
        records.append({
            "session_id":   item.findtext("cdr:SessionId",  namespaces=ns),
            "calling_party": item.findtext("cdr:CallingParty", namespaces=ns),
            "duration_sec": int(item.findtext("cdr:DurationSeconds", default="0", namespaces=ns)),
        })
```

### Gotchas
- SOAP responses often have deeply nested namespaces — always define the `ns` dict and use it in `findall`/`findtext`.
- Some SOAP APIs return HTTP 500 for business-logic errors with a `<Fault>` body — parse `<faultcode>` before assuming a transport error.
- Never use `eval()` on SOAP payloads; always use `ElementTree` or `lxml`.

---

## 5. MongoDB

**Reference DAG:** `dags/integrations/nnn_mongodb_noc_incidents_sync_hourly.py`  
**Provider package:** `apache-airflow-providers-mongo`

### Key hook
```python
from airflow.providers.mongo.hooks.mongo import MongoHook

hook       = MongoHook(conn_id=CONN_MONGODB)
client     = hook.get_conn()
collection = client["noc_tools"]["incidents"]
```

### Query pattern (time-windowed)
```python
from datetime import datetime, timezone

window_start = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
window_end   = datetime(2024, 6, 1, 23, 59, 59, tzinfo=timezone.utc)

cursor = collection.find(
    {"created_at": {"$gte": window_start, "$lt": window_end}},
    {"_id": 0, "incident_id": 1, "severity": 1, "region": 1, "created_at": 1},
)
records = list(cursor)
```

### BSON → JSON serialisation
MongoDB returns BSON `ObjectId` and `datetime` objects that are not JSON-serialisable. Before pushing to XCom or writing to JSON:

```python
import json
from bson import json_util

# Safe serialisation
json_str = json.dumps(records, default=json_util.default)

# Or manually convert known types
for r in records:
    for k, v in r.items():
        if hasattr(v, "isoformat"):
            r[k] = v.isoformat()
        elif hasattr(v, "__str__") and "ObjectId" in type(v).__name__:
            r[k] = str(v)
```

### Gotchas
- Always project only needed fields — do not `find({})` without a projection on large collections.
- `list(cursor)` loads all results into memory. For large result sets, write to a temp file iterating the cursor.
- Use `client.close()` in a `finally` block to release the connection.

---

## 6. Elasticsearch (read)

**Reference DAG:** `dags/integrations/nnn_elasticsearch_fault_logs_daily.py`  
**Provider package:** `apache-airflow-providers-elasticsearch`  
**Additional package:** `elasticsearch>=8.0`

### Key hook
```python
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook

hook      = ElasticsearchPythonHook(elasticsearch_conn_id=CONN_ELASTICSEARCH)
es_client = hook.get_conn()   # returns elasticsearch.Elasticsearch client — note the ()
```

### Scroll / search pattern (large result sets)
```python
query = {
    "query": {
        "range": {
            "@timestamp": {
                "gte": f"{run_date}T00:00:00",
                "lt":  f"{run_date}T24:00:00",
                "format": "strict_date_optional_time",
            }
        }
    }
}

# For <10k hits: use search with size
response = es_client.search(index="nnn-fault-logs-*", body=query, size=10_000)
hits     = response["hits"]["hits"]

# For >10k hits: use scroll API
resp = es_client.search(index="nnn-fault-logs-*", body=query, scroll="2m", size=1_000)
scroll_id = resp["_scroll_id"]
all_hits  = list(resp["hits"]["hits"])

while True:
    resp = es_client.scroll(scroll_id=scroll_id, scroll="2m")
    batch = resp["hits"]["hits"]
    if not batch:
        break
    all_hits.extend(batch)

es_client.clear_scroll(scroll_id=scroll_id)
```

### Gotchas
- `hook.get_conn` **without `()`** returns the bound method, not a client. Always call `hook.get_conn()`.
- The `_source` of each hit is in `hit["_source"]` — flatten it before loading to Snowflake.
- Index patterns with wildcards (`nnn-fault-logs-*`) span multiple physical indices — always set an explicit time range to avoid scanning all historical data.

---

## 7. AWS DynamoDB (read)

**Reference DAG:** `dags/integrations/nnn_dynamodb_portal_sessions_daily.py`  
**Provider package:** `apache-airflow-providers-amazon`

### Key pattern — paginated Scan with FilterExpression
```python
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook

hook = DynamoDBHook(
    aws_conn_id=CONN_DYNAMODB,
    table_name="nnn-rsp-portal-sessions",
    table_keys=["session_id"],
)
client    = hook.conn.meta.client     # boto3 DynamoDB client from the resource
paginator = client.get_paginator("scan")

page_iterator = paginator.paginate(
    TableName="nnn-rsp-portal-sessions",
    FilterExpression="session_date = :rd",
    ExpressionAttributeValues={":rd": {"S": run_date}},
)

all_items = []
for page in page_iterator:
    for raw_item in page.get("Items", []):
        all_items.append(_deserialize_dynamo_item(raw_item))
```

### DynamoDB item deserialiser
DynamoDB client returns typed attribute maps (`{"S": "value"}`, `{"N": "123"}`). Deserialise:

```python
def _deserialize_dynamo_item(item: dict) -> dict:
    flat = {}
    for key, value in item.items():
        if isinstance(value, dict):
            for dtype, val in value.items():
                if dtype == "N":
                    flat[key] = float(val) if "." in str(val) else int(val)
                elif dtype == "BOOL":
                    flat[key] = bool(val)
                elif dtype == "NULL":
                    flat[key] = None
                else:
                    flat[key] = val
                break
        else:
            flat[key] = value
    return flat
```

### Gotchas
- DynamoDB `Scan` reads every partition — it's expensive for large tables. Prefer `Query` when you have a partition key.
- The paginator handles the 1 MB per-call limit automatically. Never try to manually paginate without it.
- `FilterExpression` is applied **after** reading from disk, not before — you pay for reads of all items even if most are filtered out.

---

## 8. AWS Kinesis Data Stream

**Reference DAG:** `dags/integrations/nnn_kinesis_clickstream_hourly.py`  
**Provider package:** `apache-airflow-providers-amazon`

### Key pattern — per-shard AT_TIMESTAMP iteration
```python
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

hook   = AwsBaseHook(aws_conn_id=CONN_KINESIS, client_type="kinesis")
client = hook.get_client_type("kinesis")

# List all shards
shards_resp = client.list_shards(StreamName="nnn-rsp-portal-clickstream")
shards      = shards_resp["Shards"]

for shard in shards:
    iter_resp = client.get_shard_iterator(
        StreamName=STREAM_NAME,
        ShardId=shard["ShardId"],
        ShardIteratorType="AT_TIMESTAMP",
        Timestamp=window_start,          # datetime object
    )
    shard_iterator = iter_resp["ShardIterator"]

    while shard_iterator:
        get_resp       = client.get_records(ShardIterator=shard_iterator, Limit=10_000)
        raw_records    = get_resp["Records"]
        shard_iterator = get_resp.get("NextShardIterator")

        for record in raw_records:
            arrival = record["ApproximateArrivalTimestamp"]
            if arrival >= window_end:
                shard_iterator = None   # stop reading this shard
                break
            
            # Decode base64 Kinesis payload
            payload = json.loads(base64.b64decode(record["Data"]).decode("utf-8"))
```

### Gotchas
- **Write to a temp file**, not XCom — Kinesis runs can produce 100k+ records.
- `AT_TIMESTAMP` starts at the first record **at or after** the timestamp — you may still see records slightly before the window. Always filter by `ApproximateArrivalTimestamp`.
- `NextShardIterator` can be `None` if the shard ends — always guard with `if not shard_iterator: break`.
- Kinesis shard iterators expire after 5 minutes of inactivity. Do not `time.sleep()` between `get_records` calls.

---

## 9. AWS SQS

**Reference DAG:** `dags/integrations/nnn_sqs_provisioning_events_15min.py`  
**Provider package:** `apache-airflow-providers-amazon`

### Key pattern — receive + delete loop
```python
from airflow.providers.amazon.aws.hooks.sqs import SqsHook

hook   = SqsHook(aws_conn_id=CONN_SQS)
client = hook.get_conn()    # returns boto3 SQS client

QUEUE_URL   = "https://sqs.ap-southeast-2.amazonaws.com/123456789/nnn-provisioning-events"
MAX_BATCH   = 10    # SQS ReceiveMessage hard limit per call

all_records, receipt_handles = [], []

while len(all_records) < MAX_MESSAGES:
    resp     = client.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=MAX_BATCH,
        WaitTimeSeconds=5,            # long-poll for up to 5 s
        AttributeNames=["All"],
    )
    messages = resp.get("Messages", [])
    if not messages:
        break                         # queue is empty

    for msg in messages:
        try:
            body = json.loads(msg["Body"])
            all_records.append(body)
            receipt_handles.append(msg["ReceiptHandle"])
        except json.JSONDecodeError:
            log.warning("Skipping malformed SQS message: %s", msg["MessageId"])

# Delete processed messages in batches of 10 (SQS hard limit)
for i in range(0, len(receipt_handles), 10):
    batch = receipt_handles[i : i + 10]
    client.delete_message_batch(
        QueueUrl=QUEUE_URL,
        Entries=[{"Id": str(j), "ReceiptHandle": h} for j, h in enumerate(batch)],
    )
```

### Gotchas
- **Always delete messages** after processing — undeleted messages become visible again after the visibility timeout (default 30 s), causing duplicate processing.
- SQS does not guarantee ordering — design the Snowflake upsert to be order-independent.
- Use `WaitTimeSeconds=5` (long poll) to reduce empty receives — it's cheaper and faster than short polling.

---

## 10. MQTT / IoT Telemetry (via S3 Firehose)

**Reference DAG:** `dags/integrations/nnn_mqtt_basestation_telemetry_hourly.py`  
**Provider packages:** `apache-airflow-providers-amazon` (S3 sensor)

### Architecture
The MQTT broker → Kinesis Firehose → S3 path is managed separately. This DAG only processes what Firehose has already delivered to S3.

### Key pattern — S3KeySensor + download + INSERT
```python
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

t_sense = S3KeySensor(
    task_id="check_s3_telemetry_file",
    bucket_name=NNN_S3_BUCKET,
    bucket_key="mqtt/fw-telemetry/{{ ds }}T{{ execution_date.strftime('%H') }}/data.json",
    aws_conn_id=CONN_S3,
    poke_interval=60,
    timeout=600,
    mode="reschedule",          # releases worker slot while waiting
    sla=timedelta(minutes=30),
)
```

### Gotchas
- Always use `mode="reschedule"` for sensors — `mode="poke"` holds the worker slot while waiting, which starves other tasks.
- The Jinja template `execution_date.strftime('%H')` gives zero-padded hour. `| string.zfill(2)` is **not valid Jinja2** — do not use it.
- If Firehose delivery is delayed, the sensor retries. Set `timeout` to allow enough buffer time for Firehose to deliver.

---

## 11. RabbitMQ

**Reference DAG:** `dags/integrations/nnn_rabbitmq_order_events_15min.py`  
**Provider package:** `apache-airflow-providers-rabbitmq`

### Key pattern — basic_get drain loop
```python
from airflow.providers.rabbitmq.hooks.rabbitmq import RabbitMQHook

hook      = RabbitMQHook(rabbitmq_conn_id=CONN_RABBITMQ)
pika_conn = hook.get_conn()       # returns pika BlockingConnection
channel   = pika_conn.channel()

events, start_ts = [], time.monotonic()

try:
    while len(events) < MAX_MESSAGES:
        if time.monotonic() - start_ts > DRAIN_TIMEOUT_S:
            break

        method_frame, _, body = channel.basic_get(queue=QUEUE_NAME, auto_ack=False)

        if method_frame is None:
            break    # queue empty

        try:
            event = json.loads(body.decode("utf-8"))
            events.append(event)
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)   # ack AFTER parse
        except json.JSONDecodeError as exc:
            # NACK → dead-letter queue (requeue=False)
            channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)
            log.warning("Malformed message NACKed: %s", exc)
finally:
    channel.close()
    pika_conn.close()
```

### Gotchas
- **Acknowledge AFTER** successful processing, not before. If processing fails between receive and ack, the message is re-queued on the next consumer reconnect.
- **NACK with `requeue=False`** for unparseable messages — this routes them to the dead-letter exchange rather than re-delivering them infinitely.
- `basic_get` is synchronous pull; it is appropriate for batch draining. Do not use `basic_consume` (push model) inside a PythonOperator.

---

## 12. Redis Cache Scan

**Reference DAG:** `dags/integrations/nnn_redis_session_analytics_daily.py`  
**Provider package:** `apache-airflow-providers-redis`

### Key pattern — scan_iter + HGETALL
```python
from airflow.providers.redis.hooks.redis import RedisHook

hook   = RedisHook(redis_conn_id=CONN_REDIS)
client = hook.get_conn()    # returns redis.Redis client

sessions = []
try:
    # scan_iter: cursor-based lazy iteration — NEVER use client.keys()
    for key in client.scan_iter(match="session:*", count=100):
        raw = client.hgetall(key)    # returns {bytes: bytes}
        if not raw:
            continue
        session = {k.decode("utf-8"): v.decode("utf-8") for k, v in raw.items()}
        sessions.append(session)
finally:
    client.close()
```

### Gotchas
- **Never use `client.keys("pattern")`** — it loads all matching keys into memory and blocks Redis for seconds on a large keyspace.
- `scan_iter(count=100)` is a *hint* to Redis about how many keys to return per internal cursor step, not a hard batch size.
- `hgetall` returns raw `bytes` keys and values — always decode.
- Write sessions to a temp file before pushing to XCom when scanning thousands of keys.

---

## 13. IMAP Email + Attachment

**Reference DAG:** `dags/integrations/nnn_imap_rsp_report_weekly.py`  
**Provider package:** `apache-airflow-providers-imap`

### Key pattern — search + fetch + download attachment
```python
from airflow.providers.imap.hooks.imap import ImapHook
import email

hook = ImapHook(imap_conn_id=CONN_IMAP)
hook.get_conn()
mail = hook.mail     # imaplib.IMAP4_SSL object

mail.select("INBOX")
_, ids_raw = mail.search(None, '(UNSEEN SUBJECT "Weekly Report")')
message_ids = ids_raw[0].split() if ids_raw[0] else []

for msg_id in message_ids:
    _, msg_data  = mail.fetch(msg_id, "(RFC822)")
    msg          = email.message_from_bytes(msg_data[0][1])

    for part in msg.walk():
        fname = part.get_filename()
        if fname and fname.lower().endswith(".xlsx"):
            with open(f"/tmp/{fname}", "wb") as fh:
                fh.write(part.get_payload(decode=True))

    # Mark as read after successful download
    mail.store(msg_id, "+FLAGS", "\\Seen")

hook.mail.logout()
```

### Gotchas
- **Mark emails as read (`\\Seen`)** after processing to prevent re-ingestion on reruns — the IMAP search uses `UNSEEN`.
- **Clean up temp files** with `os.remove()` after uploading to S3 or loading to Snowflake.
- `part.get_payload(decode=True)` returns `bytes`. Write in binary mode (`"wb"`).
- If multiple emails arrived since the last run, process all of them — the search returns a list.

---

## 14. Azure Blob Storage

**Reference DAG:** `dags/integrations/nnn_azure_blob_partner_data_daily.py`  
**Provider package:** `apache-airflow-providers-microsoft-azure`

### Key hooks
```python
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

hook  = WasbHook(wasb_conn_id=CONN_AZURE_BLOB)

# List blobs under a prefix
blobs = hook.get_blobs_list(container_name="partner-data-drops", prefix=f"{run_date}/")

# Download blob content
data = hook.read_file(container_name="partner-data-drops", blob_name=blob_name)
# data is bytes or a str depending on WasbHook version — handle both:
content = data if isinstance(data, bytes) else data.encode()

# Upload blob
hook.load_file(
    file_path="/tmp/output.csv",
    container_name="partner-data-drops",
    blob_name=f"processed/{run_date}/output.csv",
    overwrite=True,
)
```

### S3 staging pipeline
Azure → Download to `/tmp/` → `upload_to_s3()` → Snowflake COPY INTO

```python
safe_name   = blob_name.replace("/", "_")
local_path  = f"/tmp/partner_{safe_name}"
with open(local_path, "wb") as fh:
    fh.write(content)

s3_key_path = f"wholesale/partner_data_ingest/run_date={run_date}/{safe_name}"
upload_to_s3(local_path=local_path, s3_key_path=s3_key_path)
os.remove(local_path)
```

### Gotchas
- Snowflake cannot directly read from Azure Blob — always stage through S3 using the Snowflake S3 external stage.
- If `get_blobs_list` returns an empty list, log and push an empty list to XCom — do not raise. Partners may miss a day; downstream tasks should handle no-data gracefully.

---

## 15. MS SQL Server

**Reference DAG:** `dags/integrations/nnn_mssql_legacy_billing_daily.py`  
**Provider package:** `apache-airflow-providers-microsoft-mssql`

### Key hook
```python
# get_mssql_hook() is a convenience wrapper from nnn_common.utils
hook    = get_mssql_hook()
records = hook.get_records(
    "SELECT TOP 10000 * FROM dbo.BillingRecords WHERE BillDate = %s",
    parameters=(run_date,),
)
# get_records returns list of tuples; pair with column names from description
columns = [desc[0].lower() for desc in hook.get_cursor().description]
```

Or for bulk reads:
```python
conn   = hook.get_conn()
cursor = conn.cursor()
cursor.execute("SELECT * FROM dbo.BillingRecords WHERE BillDate = %s", (run_date,))
# Fetch in chunks to avoid memory issues
while True:
    batch = cursor.fetchmany(1_000)
    if not batch:
        break
    # process batch
```

### Gotchas
- MS SQL uses `%s` positional params with tuple (same as psycopg2, NOT Snowflake's named params).
- Use `TOP N` or date filters on every query — legacy billing tables can be hundreds of GB.
- `MsSqlHook` uses `pymssql` by default. If you see charset errors, add `charset=utf8` to the connection extra JSON.

---

## 16. Snowflake → S3 Parquet

**Reference DAG:** `dags/integrations/nnn_snowflake_to_s3_ml_export_daily.py`  
**Pattern:** UNLOAD via Snowflake external stage

### Key helper
```python
from nnn_common.utils import snowflake_unload_to_s3

s3_path = snowflake_unload_to_s3(
    select_sql=f"SELECT * FROM ML.CUSTOMER_FEATURES WHERE feature_date = '{run_date}'",
    s3_prefix=f"ml-exports/customer_features/run_date={run_date}/",
    file_format="PARQUET",
    overwrite=True,
)
```

### Parallel fan-out (multiple tables)
Define one PythonOperator per table; use `[t_a, t_b, t_c] >> t_trigger` dependency:

```python
[task_export_customer, task_export_network, task_export_churn] >> task_trigger_training
```

### Gotchas
- `overwrite=True` removes old files at the S3 prefix before writing. Use for idempotent daily exports.
- Do **not** pass the returned full S3 URI to `redshift_copy_from_s3()`. Keep the relative `s3_prefix` in a separate variable.
- For exports > 1 GB, Snowflake automatically splits output into multiple files. `redshift_copy_from_s3()` handles multi-file prefixes automatically.

---

## 17. Snowflake → Redshift (via S3)

**Reference DAGs:** All 10 DAGs in `dags/redshift/`  
**Pattern:** Snowflake UNLOAD → S3 → Redshift COPY

### Standard 3-task pattern

```python
def export_from_snowflake(**context) -> None:
    run_date  = get_run_date(context)
    s3_prefix = f"analytics/network_performance/run_date={run_date}/"
    snowflake_unload_to_s3(
        select_sql=f"SELECT * FROM NETWORK.LINK_PERFORMANCE_DAILY WHERE REPORT_DATE = '{run_date}'",
        s3_prefix=s3_prefix,
        file_format="PARQUET",
        overwrite=True,
    )
    context["ti"].xcom_push(key="s3_prefix", value=s3_prefix)   # push RELATIVE key

def load_to_redshift(**context) -> None:
    run_date  = get_run_date(context)
    s3_prefix = context["ti"].xcom_pull(task_ids="export_from_snowflake", key="s3_prefix")

    hook = get_redshift_hook()
    hook.run(
        "DELETE FROM analytics.network_performance_daily WHERE report_date = %s",
        parameters=(run_date,),
    )
    redshift_copy_from_s3(
        table="analytics.network_performance_daily",
        s3_prefix=s3_prefix,    # relative key only
        file_format="PARQUET",
        truncate=False,
    )

def validate(**context) -> None:
    run_date = get_run_date(context)
    assert_redshift_row_count(
        table="analytics.network_performance_daily",
        run_date=run_date,
        date_column="report_date",    # override if not 'run_date'
    )
```

### Full-refresh vs. date-partitioned

| Table type | `truncate` | DELETE before COPY |
|---|---|---|
| Date-partitioned (most tables) | `False` | Yes — `DELETE WHERE date_col = run_date` |
| Full-refresh (CAPEX, lookup tables) | `True` | No — TRUNCATE replaces all rows |

---

## 18. Snowflake → AWS DynamoDB

**Reference DAG:** `dags/integrations/nnn_snowflake_to_dynamodb_cache_daily.py`  
**Provider package:** `apache-airflow-providers-amazon`

### Key pattern — batch_writer
```python
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

hook     = AwsBaseHook(aws_conn_id=CONN_DYNAMODB, resource_type="dynamodb")
dynamodb = hook.get_resource_type("dynamodb")
table    = dynamodb.Table("nnn-rsp-portal-cache")

BATCH_SIZE = 25    # DynamoDB BatchWriteItem hard limit

for i in range(0, len(records), BATCH_SIZE):
    chunk = records[i : i + BATCH_SIZE]
    with table.batch_writer() as batch:
        for record in chunk:
            item = {k: str(v) if v is not None else "" for k, v in record.items()}
            batch.put_item(Item=item)
```

### Gotchas
- `table.batch_writer()` internally batches and retries unprocessed items. Always use it instead of calling `put_item` in a loop.
- DynamoDB attributes are strongly typed — use `Decimal` for numbers or convert to `str` to avoid `TypeError: Float types are not supported`.
- For large exports (millions of items), write Snowflake output to S3 first and use DynamoDB's **Import from S3** feature rather than putting items one by one.

---

## 19. Snowflake → Elasticsearch

**Reference DAG:** `dags/integrations/nnn_snowflake_to_elasticsearch_daily.py`  
**Provider package:** `apache-airflow-providers-elasticsearch`  
**Additional:** `elasticsearch>=8.0`

### Key pattern — bulk index
```python
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
from elasticsearch.helpers import bulk

hook      = ElasticsearchPythonHook(elasticsearch_conn_id=CONN_ELASTICSEARCH)
es_client = hook.get_conn()    # MUST call with ()

def _actions(records):
    for rec in records:
        yield {"_index": "nnn-service-availability", "_id": rec["address_id"], "_source": rec}

success_count, errors = bulk(es_client, _actions(records), chunk_size=500, raise_on_error=True)

if errors:
    raise RuntimeError(f"ES bulk errors: {errors[:5]}")

es_client.indices.refresh(index="nnn-service-availability")
```

### Gotchas
- `hook.get_conn` without `()` is the unbound method — always `hook.get_conn()`.
- `raise_on_error=True` in `bulk()` raises on the first batch error. Set to `False` and inspect `errors` for partial-success behaviour.
- Full-refresh (delete all + reindex) requires explicit `delete_by_query` first. A simpler alternative is to write to a new index name and swap aliases atomically.

---

## 20. Snowflake → RDS PostgreSQL

**Reference DAG:** `dags/integrations/nnn_snowflake_to_rds_postgres_daily.py`  
**Provider package:** `apache-airflow-providers-postgres`

### Key pattern — export to temp file → DELETE + insert_rows
```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

hook = PostgresHook(postgres_conn_id=CONN_RDS_POSTGRES)
hook.run("DELETE FROM operational_reporting.network_health_daily WHERE report_date = %s", parameters=(run_date,))

columns = list(records[0].keys())
for i in range(0, len(records), 1_000):
    batch = records[i : i + 1_000]
    rows  = [tuple(rec[col] for col in columns) for rec in batch]
    hook.insert_rows(
        table="operational_reporting.network_health_daily",
        rows=rows,
        target_fields=columns,
        commit_every=1_000,
    )
```

### Gotchas
- `PostgresHook` uses **positional `%s`** with tuple (psycopg2 style), not Snowflake named params.
- `commit_every=0` commits after **every individual row** — always use `commit_every=1000` for bulk loads.
- Large Snowflake exports (millions of rows) should go through S3 → `COPY FROM` (using `hook.run("COPY ...")`) rather than Python-level `insert_rows`.

---

## 21. Snowflake → Google Cloud Storage

**Reference DAG:** `dags/integrations/nnn_snowflake_to_gcs_regulatory_weekly.py`  
**Provider package:** `apache-airflow-providers-google`

### Key pattern
```python
from airflow.providers.google.cloud.hooks.gcs import GCSHook

hook = GCSHook(gcp_conn_id=CONN_GCS)
hook.upload(
    bucket_name="nnn-accc-regulatory-prod",
    object_name=f"{run_year}/{run_month}/weekly_report_{run_date}.csv",
    filename="/tmp/weekly_report.csv",
    mime_type="text/csv",
)
```

### Connection setup
The `CONN_GCS` connection uses **Google Cloud** type. Set the Keyfile JSON field to the service account key, or use Workload Identity in GKE.

### Gotchas
- Clean up the local temp file with `os.remove()` after upload.
- GCS object names are case-sensitive and must not start with `/`.
- For large files, GCSHook handles multipart upload automatically.

---

## 22. S3 → AWS Glue Data Catalog

**Reference DAG:** `dags/integrations/nnn_s3_to_glue_catalog_daily.py`  
**Provider package:** `apache-airflow-providers-amazon`

### Key pattern — batch_create_partition
```python
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

hook = AwsBaseHook(aws_conn_id=CONN_GLUE, client_type="glue")
glue = hook.get_client_type("glue")

response = glue.batch_create_partition(
    DatabaseName="nnn_datalake",
    TableName="link_utilisation",
    PartitionInputList=[
        {
            "Values": ["2024-06-01"],
            "StorageDescriptor": {
                "Location":     "s3://nnn-data-lake-prod/network/link_utilisation/run_date=2024-06-01/",
                "InputFormat":  "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                },
            },
        }
    ],
)
# AlreadyExistsException in Errors is acceptable (idempotent)
errors = [e for e in response.get("Errors", []) if e["ErrorDetail"]["ErrorCode"] != "AlreadyExistsException"]
if errors:
    raise RuntimeError(f"Glue partition errors: {errors}")
```

### Crawler polling
```python
glue.start_crawler(Name="nnn-datalake-crawler")

elapsed = 0
while elapsed < 600:
    time.sleep(30)
    elapsed += 30
    state = glue.get_crawler(Name="nnn-datalake-crawler")["Crawler"]["State"]
    if state == "READY":
        break
else:
    raise TimeoutError("Crawler did not reach READY within 10 minutes")
```

### Gotchas
- `AlreadyExistsException` in `batch_create_partition` errors is **expected and safe** — partitions already registered on reruns. Filter it out before raising.
- `EntityNotFoundException` means the Glue table does not exist in the catalog. Log a warning and skip — do not raise (new tables may not be catalogued yet).
- Running the crawler after partition registration ensures Athena and Redshift Spectrum immediately see the new data.
