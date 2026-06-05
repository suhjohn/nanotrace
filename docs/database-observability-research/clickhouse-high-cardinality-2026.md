# ClickHouse vs Prometheus For High Cardinality, Part 2

- Source: https://clickhouse.com/blog/clickhouse-vs-promethous-high-cardinality-part-2-cardinality-in-clickhouse
- Type: post
- Year: 2026
- Authors/Org: Rory Crispin, Dale McDiarmid / ClickHouse

## Problem

The post contrasts Prometheus-style series storage with ClickHouse-style event storage for high-cardinality observability. In Prometheus, every unique label set creates an independent time series, so high cardinality creates series metadata, memory, write amplification, and query-planning pressure. ClickHouse avoids creating a structural object for each label combination; it stores rows and pays mainly at read time.

The post is careful that cardinality is not free in ClickHouse. The claim is narrower: high-cardinality dimensions fit better when telemetry is modeled as wide, timestamped events with attributes and measurements, rather than as pre-decomposed metric series.

## System / Architecture

The recommended architecture is an event-oriented observability table in ClickHouse, with dynamic labels in a `Map` and common dimensions or metrics materialized into real columns. This resembles ClickStack/OpenTelemetry log schemas, where resource and scope attributes are dynamic maps and frequent filters become typed columns.

ClickHouse is positioned as the analytical backend for logs, traces, and metrics-derived-from-events. It is not presented as a drop-in Prometheus replacement because Prometheus has first-class semantics for counters, resets, histograms, summaries, range vectors, alerting, and PromQL.

## Storage Model

The post discourages a rigid column-per-label schema for dynamic observability data. It recommends storing dynamic labels as `Map(LowCardinality(String), String)` or similar, while materializing selected labels and metrics such as `host`, `status`, `application`, and `response_time`.

Modern sharded map serialization distributes map keys across buckets based on the label-name hash. Small level-0 parts may start with a flat map layout; background merges rewrite maps into bucketed storage. Querying a single label can then read only the bucket containing that key rather than the entire map.

The table still benefits from normal MergeTree storage: rows are sorted by an ordering key, columns are compressed separately, common values cluster together on disk, and numeric measurements can use codecs such as Delta or Gorilla before general compression.

## Ingest / Write Path

Ingest writes rows, not series objects. A new label combination simply means another event row with different map values. There is no per-series lifecycle, in-memory series registry, or index object created for each unique combination.

Materialized columns are evaluated at insert time for new data. The post notes that materialized columns can also be added after data already exists: new rows store the column physically, while older rows can still resolve the value from the source map when queried.

## Query / Index Model

Query speed depends on how much data must be read and aggregated. Common filters should align with materialized columns and the sorting key so ClickHouse can prune granules. Secondary indexes over `mapKeys(labels)` and `mapValues(labels)`, including text or Bloom-filter indexes, help prune data for non-materialized labels.

The post gives examples over roughly 5.34 billion rows. A time-bucketed average over response time grouped by status processed about 49.21 million rows in 0.122 seconds on a 32-core machine. A narrower filtered query processed about 53.40 million rows in 0.078 seconds. A dynamic map access for a less common `size` metric remained possible but processed 13.60 GB and took 1.138 seconds, showing the cost of leaving a metric inside the map.

## Compaction / Materialization / Evolution

Background merges are part of the storage evolution story because they rewrite map storage into bucketed form and compact small parts. Materialized columns provide a gradual schema-evolution mechanism: promote common labels or metrics when query patterns justify it, without forcing all attributes into rigid columns upfront.

The practical pattern is to start flexible, observe query patterns, and materialize hot dimensions or measures. Cold or rarely queried attributes can stay in the map and be cast at query time.

## Relevance To Event-Native Analytics And Observability

The post is directly about event-native observability. It argues that logs, traces, and metric values attached to events preserve drilldown: an aggregate spike can be traced back to the raw requests or spans that produced it. This avoids the irreversible loss that occurs when signals are emitted only as pre-aggregated time series.

ClickHouse is strongest for full-fidelity, high-cardinality analysis where questions change over time: long-term trends, SLIs, KPIs, trace/log metrics, and aggregations across dimensions that would be too expensive or too ephemeral to model as Prometheus labels.

## Tradeoffs And Limitations

High-cardinality `GROUP BY` operations still cost memory and time because aggregation states must be built at read time. The post shows grouping by a high-cardinality `request_path` over 153.23 million rows taking 9.433 seconds and 22.82 GiB peak memory. Enabling external aggregation reduced memory to 9.57 GiB but increased elapsed time to 13.930 seconds.

Prometheus remains better for mature PromQL workflows, moderate and predictable cardinality, alerting, targeted single-series lookups, and metric types whose semantics matter. ClickHouse query builders for Prometheus-style metrics are still higher-level abstractions rather than full native PromQL parity.

## Notable Details

- The post uses the phrase "wide events" for logs-with-metrics-style telemetry.
- High-cardinality filters can be beneficial when they reduce scanned rows.
- Rendering millions of separate dashboard lines is a product problem even if the database can compute them.
- Query-time casting from maps is flexible but can be much slower than materialized typed columns.
- The source URL contains `promethous`, but the article title uses Prometheus.
