# Nanotrace Loader

`nanotrace-loader` owns the AWS S3-to-ClickHouse path:

```text
S3 raw NDJSON -> SQS notification -> nanotrace-loader -> ClickHouse
```

The loader polls `LOADER_SQS_QUEUE_URL`, fetches each notified S3 object, and
inserts the object body into ClickHouse with `FORMAT JSONEachRow`. It deletes the
SQS message only after ClickHouse accepts the insert.

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
```

Current behavior is identity loading: raw S3 rows are inserted as-is. The next
layer is applying the current loader processor before the ClickHouse insert.
