# Nanotrace Loader

`nanotrace-loader` owns the AWS S3-to-lakehouse-to-ClickHouse path:

```text
S3 raw NDJSON -> SQS notification -> nanotrace-loader -> Iceberg table -> ClickHouse
```

The loader polls `LOADER_SQS_QUEUE_URL`, batches notified S3 objects, and
commits each processed batch to the Iceberg-backed lakehouse when
`NANOTRACE_LAKEHOUSE_ENABLED=true`. It then inserts the same committed batch
into ClickHouse with `FORMAT JSONEachRow` for hot serving. By default the
serving path is raw-only: it writes `events` and does not build secondary
analytics tables. Optional derivation can materialize promoted schema rows. SQS
messages are deleted after the lakehouse commit and ClickHouse inserts succeed.

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
LOADER_DERIVATION_MODE=raw
CLICKHOUSE_INSERT_CONCURRENCY=4
NANOTRACE_LAKEHOUSE_ENABLED=false
NANOTRACE_LAKEHOUSE_WAREHOUSE_DIR=/data/lakehouse
NANOTRACE_ICEBERG_REST_URI=
NANOTRACE_ICEBERG_WAREHOUSE=s3://nanotrace-lakehouse
NANOTRACE_ICEBERG_CATALOG_NAME=nanotrace
```

If `NANOTRACE_ICEBERG_REST_URI` is set, the loader uses an Iceberg REST
catalog and writes data files to `NANOTRACE_ICEBERG_WAREHOUSE`. Without it,
local development uses a filesystem-backed Iceberg table under
`NANOTRACE_LAKEHOUSE_WAREHOUSE_DIR`.

`LOADER_DERIVATION_MODE` values:

- `raw`: insert only `events`.
- `promoted`: insert `events` plus schema-defined `field_index`,
  `event_measures`, and `entity_state_updates`.

Delivery is at-least-once: if the loader crashes after ClickHouse accepts a
batch but before SQS delete succeeds, SQS redelivery can reinsert the same
objects. Target ClickHouse tables should use deterministic row identity or
deduplication for retry-safe counts.
