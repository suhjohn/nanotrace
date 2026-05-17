# Nanotrace Loader

`nanotrace-loader` owns the AWS S3-to-ClickHouse path:

```text
S3 raw NDJSON -> SQS notification -> nanotrace-loader -> ClickHouse
```

The loader polls `LOADER_SQS_QUEUE_URL`, batches notified S3 objects, and
inserts each batch into ClickHouse with `FORMAT JSONEachRow`. Raw and derived
rows are submitted with ClickHouse async insert; SQS messages are deleted after
ClickHouse accepts the async insert requests.

Required settings:

```text
LOADER_SQS_QUEUE_URL
CLICKHOUSE_URL
CLICKHOUSE_USER
CLICKHOUSE_PASSWORD
```

Optional settings:

```text
CLICKHOUSE_DATABASE=observatory
CLICKHOUSE_TABLE=events
LOADER_POLL_WAIT_SECS=20
LOADER_MAX_MESSAGES=10
LOADER_VISIBILITY_TIMEOUT_SECS=300
LOADER_REQUEST_TIMEOUT_SECS=60
LOADER_CONCURRENCY=4
LOADER_DEFINITIONS_REFRESH_SECS=60
CLICKHOUSE_INSERT_CONCURRENCY=4
```

Delivery is at-least-once: if the loader crashes after ClickHouse accepts a
batch but before SQS delete succeeds, SQS redelivery can reinsert the same
objects. Target ClickHouse tables should use deterministic row identity or
deduplication for retry-safe counts.
