# Sentry Snuba And Unstructured Data In ClickHouse

- Source: https://sentry.engineering/blog/introducing-snuba-sentrys-new-search-infrastructure and https://sentry.engineering/blog/how-sentry-queries-unstructured-data-in-clickhouse-62x-faster
- Type: posts
- Year: 2019, 2025
- Authors/Org: Sentry; Colin Chartier / Sentry

## Problem

Sentry's original search, tag, and time-series abstractions used relational SQL and Redis-backed implementations. As event volume and product needs grew, denormalized tag-count tables became hard to evolve. Adding a new query dimension, such as environment, required months of layout changes and backfills. Retention deletes and mutable relational heaps were also expensive for mostly immutable event data.

The later unstructured-data post describes a related problem for spans: customers send billions of spans with arbitrary attributes. A strict column per attribute would create thousands of columns and fail on insert memory. A single ClickHouse `Map` avoids column explosion but forces reads of all attributes when only one key is queried.

## System / Architecture

Snuba is Sentry's service layer over ClickHouse. It abstracts ClickHouse from application developers through a higher-level JSON query interface and client. Snuba query servers are Flask services that translate structured Snuba requests into ClickHouse SQL. This lets the storage team change physical data models centrally while preserving a stable product-facing query API.

The write path reads normalized JSON events from Kafka, batches them, turns each event into one ClickHouse row, and inserts into ClickHouse. Snuba also uses Redis to cache individual query results and coalesce repeated bursty queries into fewer ClickHouse queries.

## Storage Model

The 2019 Snuba post chose ClickHouse because immutable events fit columnar OLAP. Rows are sorted by primary key, columns are stored and compressed separately, and Tagstore-like data shrank from terabytes to gigabytes.

For unstructured spans, Sentry compared three schemas:

- `spans_v1`: thousands of typed columns, one per possible attribute, which failed due to insert-time memory and schema-evolution problems.
- `spans_v2`: a small number of `Map` columns such as `attributes_string` and `attributes_float`, which handled arbitrary keys but made selective reads scan massive attribute maps.
- `spans_v3`: many bounded map buckets, for example `attributes_string_0` through `attributes_string_49`, using a hash of the attribute key to choose the bucket.

The bucketed design preserves a bounded column count while reducing the data scanned for a single attribute to approximately one bucket.

## Ingest / Write Path

Snuba writes from Kafka in batches because each ClickHouse insert creates a physical directory with per-column files and a replication metadata record. The 2019 post says roughly one write per second is recommended to avoid overwhelming ZooKeeper and the filesystem with too many small inserts.

Events pass through Sentry normalization and processing before Snuba sees them. Each processed event becomes a tuple corresponding to one ClickHouse row. Data is partitioned by time and retention window so expired data can be removed efficiently.

For bucketed span attributes, the bucket assignment is deterministic from the attribute key hash, so the query translator and ingest path can agree on where an attribute lives.

## Query / Index Model

Snuba exposes a high-level query JSON model with projects, aggregations, conditions, group-bys, granularity, time ranges, ordering, and limits. It translates requests into ClickHouse SQL using features such as `PREWHERE` for highly selective filters. The service hides ClickHouse-specific query shape from Sentry application developers.

For unstructured attributes, Snuba rewrites an expression such as `attributes_string['hello']` into an access against the specific bucket column computed by a fast hash function. This avoids scanning one huge map column. The 2025 benchmark reported the OLAP pattern `sum(attrs['x']) WHERE attrs['y']` becoming 62x faster with bucketing: 2.643 seconds on the single-map schema versus 0.042 seconds on the bucketed schema.

## Compaction / Materialization / Evolution

The 2019 architecture relied on ClickHouse background merges to combine the many inserted parts. Time and retention partitioning was the main evolution mechanism for removing old data. Snuba itself was the abstraction boundary for schema evolution: application developers query Snuba, while the storage team can change ClickHouse schemas and translations.

The 2025 bucketed-map design is a schema-level materialization of a hash table idea. It does not materialize each attribute as a full column, but it materializes enough buckets to make selective reads efficient while bounding file count.

## Relevance To Event-Native Analytics And Observability

Snuba is a strong event-native observability case study. Sentry needed raw immutable event data queryable ad hoc without backfilling every new dimension. ClickHouse enabled real-time reads after writes, compressed event storage, and a flat OLAP model better suited to search, graphs, issue details, rule-processing queries, tracing, custom dashboards, logs, and span analytics.

The 2025 post is especially relevant for arbitrary observability attributes. It shows that the right physical layout for event attributes may be neither strict columns nor one giant map, but a bounded bucketing scheme that keeps flexible keys while restoring selective columnar reads.

## Tradeoffs And Limitations

Snuba adds an application-owned query service and translator, which is extra infrastructure but creates a useful boundary around ClickHouse. The bucketed-map approach requires deterministic query rewriting and a fixed bucket count; skewed keys or too few buckets could still leave hot buckets large, while too many buckets could increase file and metadata overhead.

The single-map design is flexible but undermines ClickHouse's columnar advantage for selective reads. The strict-column design is fastest in principle for known fields but unacceptable for arbitrary user attributes and high schema churn.

## Notable Details

- Snuba initially powered search, graphs, issue detail pages, rule-processing queries, and visibility features.
- Alert-rule queries accounted for roughly 40% of Sentry's queries per second when moved to Snuba.
- The 2019 post credits Brett Hoerner, Ted Kaemming, Alex Hofsteede, James Cunningham, and Jason Shaw for Snuba work.
- The 2025 bucketing design used a hash-table analogy: fixed buckets, key hash, and bucket-specific lookup.
- Sentry's bucketed schema powers later features including custom dashboards, log storage, and tracing.
