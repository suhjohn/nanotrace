# Research Synthesis: Event-Native Analytics And Observability Storage

Date: 2026-06-05

Status: synthesis of the per-source research notes in
[docs/database-observability-research](database-observability-research/README.md).
This document is research-only. It does not assess or recommend changes to any
specific codebase.

## Corpus

The detailed notes live under
[docs/database-observability-research](database-observability-research/README.md).
They cover papers, engineering posts, and system documentation across six
groups:

| Group | Representative notes |
| --- | --- |
| ClickHouse and ClickHouse-based systems | [ClickHouse PVLDB 2024](database-observability-research/clickhouse-pvldb-2024.md), [Sentry Snuba](database-observability-research/sentry-snuba-clickhouse.md), [Cloudflare ClickHouse analytics](database-observability-research/cloudflare-clickhouse-log-analytics.md), [ClickHouse JSON](database-observability-research/clickhouse-json-2024-2025.md) |
| Observability storage and metrics systems | [Datadog Husky event store](database-observability-research/datadog-husky-event-store.md), [Husky compaction](database-observability-research/datadog-husky-compaction.md), [LogStore](database-observability-research/logstore-sigmod-2021.md), [Mach](database-observability-research/mach-cidr-2022.md), [Monarch](database-observability-research/monarch-pvldb-2020.md), [Gorilla](database-observability-research/gorilla-pvldb-2015.md) |
| Real-time OLAP and serving warehouses | [Druid](database-observability-research/druid-sigmod-2014.md), [Pinot](database-observability-research/pinot-sigmod-2018.md), [Procella](database-observability-research/procella-pvldb-2019.md), [Dremel](database-observability-research/dremel-vldb-2010-and-decade.md), [Napa](database-observability-research/napa-pvldb-2023.md), [Snowflake](database-observability-research/snowflake-sigmod-2016.md) |
| Lakehouse and object-storage analytics | [Lakehouse comparison](database-observability-research/lakehouse-comparison-cidr-2023.md), [Iceberg row-level operations](database-observability-research/iceberg-row-level-ops-pvldb-2024.md), [Delta Lake](database-observability-research/delta-lake-pvldb-2020.md), [cloud object storage analytics](database-observability-research/cloud-object-storage-analytics-pvldb-2023.md), [Kafka-to-Iceberg streaming](database-observability-research/kafka-to-iceberg-streaming.md) |
| Semi-structured analytics | [JSON Tiles](database-observability-research/json-tiles-sigmod-2021.md), [schemaless LSM columnar formats](database-observability-research/schemaless-lsm-columnar-pvldb-2022.md), [analytical DBMS formats](database-observability-research/analytical-dbms-formats-vldb-journal-2025.md) |
| Operational and business signals | [Samsara operational signals](database-observability-research/samsara-operational-signals.md), [Uber Pinot operational analytics](database-observability-research/uber-pinot-operational-analytics.md), [GitLab and Lago ClickHouse analytics](database-observability-research/gitlab-lago-clickhouse-business-analytics.md), [Observation Lakehouses](database-observability-research/observation-lakehouses-2025.md) |

## Core Synthesis

The strongest cross-system conclusion is that modern observability and business
analytics are converging around event-native data systems. The durable record is
usually a timestamped fact with dimensions, optional measures, identity, source
metadata, and sometimes causal or state-transition context. Metrics, reports,
dashboards, traces, cohorts, and alerts are then derived or serving-optimized
views over those facts.

This pattern appears in:

- ClickHouse's append-heavy OLAP model and its observability case studies;
- Sentry's Snuba event query service over ClickHouse;
- Cloudflare's ClickHouse-backed log and HTTP analytics;
- Datadog Husky's schemaless event store over object storage;
- Druid and Pinot's event-segment serving model;
- Procella's unified serving and analytical workloads at YouTube;
- Iceberg/Delta/Hudi lakehouse table formats over immutable files;
- Honeycomb's wide-event observability model;
- Samsara-style physical operations telemetry and Uber catalog operations.

## Repeated Architecture Pattern

The mature systems separate five concerns.

| Concern | Common shape | Example sources |
| --- | --- | --- |
| Durable facts | Append-heavy event/log/span/metric/state records with source metadata | [Husky](database-observability-research/datadog-husky-event-store.md), [LogStore](database-observability-research/logstore-sigmod-2021.md), [Iceberg](database-observability-research/iceberg-row-level-ops-pvldb-2024.md) |
| Fast serving | Columnar storage, sorted segments/parts, sparse indexes, skipping indexes, rollups | [ClickHouse](database-observability-research/clickhouse-pvldb-2024.md), [Druid](database-observability-research/druid-sigmod-2014.md), [Pinot](database-observability-research/pinot-sigmod-2018.md) |
| Query service | Context resolution, validation, routing, throttling, SQL generation, result aggregation | [Snuba](database-observability-research/sentry-snuba-clickhouse.md), [Husky query engine](database-observability-research/datadog-husky-query-engine.md), [Procella](database-observability-research/procella-pvldb-2019.md) |
| Maintenance | Compaction, merging, clustering, metadata cleanup, file sizing, retention | [Husky compaction](database-observability-research/datadog-husky-compaction.md), [Delta Lake](database-observability-research/delta-lake-pvldb-2020.md), [cloud object storage analytics](database-observability-research/cloud-object-storage-analytics-pvldb-2023.md) |
| Semantic outputs | Metrics, rollups, reports, materialized views, sampled traces, entity/state views | [Mach](database-observability-research/mach-cidr-2022.md), [Monarch](database-observability-research/monarch-pvldb-2020.md), [Honeycomb](database-observability-research/honeycomb-wide-events-and-sampling.md) |

The separation is not accidental. Writers optimize for cheap, correct ingestion;
readers optimize for latency and workload isolation; compactors optimize storage
shape; query services hide physical complexity; materializers make repeated
questions cheaper.

## High Cardinality

High cardinality is not solved by a single database choice. Systems move the
cost to different places.

| System family | Where cardinality tends to cost |
| --- | --- |
| Series-oriented metrics systems | Write-time active series, index memory, label churn, retention pressure |
| Columnar event systems | Query-time scans, grouping, sorting, high-cardinality aggregation |
| Search systems | Inverted-index size, merge cost, tokenization/analyzer tradeoffs |
| Lakehouse tables | File/manifest metadata, partition/sort quality, query engine scan cost |
| Precomputed reports/rollups | Write amplification, storage duplication, freshness/version management |

The ClickHouse high-cardinality discussion, Honeycomb's wide-event model, Sentry
attribute bucketing, and Mach/Monarch metric systems all agree on the same
underlying point: high-cardinality context is essential for debugging and
analysis, but it must be placed in the right physical model for the workload.

## Semi-Structured Data

The semi-structured sources converge on a middle path:

- keep raw JSON or document structure for fidelity and schema evolution;
- extract stable or heavily queried paths into columnar form;
- avoid promoting every possible path;
- avoid one giant opaque map for all hot analytical queries;
- use statistics, path limits, bucketing, or query-driven extraction to preserve
  performance.

Sentry's bucketed attribute maps, ClickHouse's JSON/Dynamic types, JSON Tiles,
and schemaless LSM columnar formats are different implementations of the same
idea: flexible events need selective physical structure.

## Lakehouse And Serving Engines

The lakehouse papers show why open table formats matter:

- ACID table metadata over immutable object files;
- schema and partition evolution;
- time travel and rollback;
- file skipping through manifest statistics;
- row-level operations through rewrite/delete metadata;
- cross-engine access to the same durable data.

The ClickHouse, Pinot, Druid, and Husky sources show why serving engines still
matter:

- low-latency recent queries;
- sorted parts or segments;
- sparse/skipping/inverted/bloom indexes;
- local caches;
- pre-aggregated rollups;
- workload-aware query execution.

The synthesis is coexistence, not replacement. Durable open tables are strong
for replay, history, and interoperability. Native serving layouts are strong for
interactive products and operational workflows.

## Workload Taxonomy

The sources repeatedly distinguish these workload classes:

| Workload | Typical physical need |
| --- | --- |
| Recent event browsing | Time-ordered columnar serving, tenant/source pruning |
| Point lookup | Sort keys, sparse indexes, bloom filters, key/value indexes |
| Trace reconstruction | Causal ids, bounded time windows, trace-aware sampling |
| Dashboards | Rollups, pre-aggregation, adaptive resolution, cached query plans |
| Alerts | Recent metric/state paths, predictable latency, control-plane semantics |
| Full-text search | Inverted/text indexes and analyzers, often separate from OLAP |
| Business reports | Materialized results, cubes, publication/freshness semantics |
| Long historical scans | Object storage, lakehouse metadata, compaction, distributed scan |
| Operational state | Entity-state updates, latest/as-of views, CDC/upsert semantics |

No source argues convincingly that all of these should use one physical table
layout. The common industry move is a unified query/product surface over
multiple physical routes.

## Notable Tensions

### Raw Fidelity vs. Write Amplification

Wide events preserve debugging context, but every index, materialized view, or
rollup increases write cost. Honeycomb emphasizes preserving wide context;
ClickHouse, Druid, Pinot, and Husky show the serving structures needed to make
common reads fast.

### Freshness vs. Compaction

Small batches make data visible quickly but create many files or parts. Large
batches and compaction improve read efficiency but add latency and background
work. This tension is explicit in ClickHouse parts, Husky fragments, lakehouse
small files, and streaming-to-Iceberg systems.

### Unified Query Surface vs. Specialized Storage

Procella and Google's observability query work push toward one language or
surface, but the underlying systems still need specialized physical paths for
metrics, logs, search, reports, and historical scans.

### Schema Flexibility vs. Query Speed

JSON and schemaless data are attractive because event producers evolve quickly.
But JSON Tiles, Sentry's buckets, and ClickHouse JSON all show that hot paths
eventually need columnar extraction, statistics, or layout constraints.

### Open Storage vs. Native Performance

Iceberg, Delta Lake, and Hudi make object storage transactional and portable.
ClickHouse, Pinot, Druid, and Snowflake demonstrate the continuing value of
native storage formats, local caches, query workers, and custom indexes.

## Timeline Of Ideas

| Period | Representative systems | Architectural emphasis |
| --- | --- | --- |
| 2010-2016 | Dremel, Gorilla, Druid, Snowflake | Columnar analytics, hot metrics, real-time exploratory OLAP, cloud warehouse separation |
| 2018-2021 | Pinot, Procella, Delta Lake, LogStore, JSON Tiles | User-facing real-time OLAP, unified serving/analytics, lakehouse transactions, multi-tenant log storage, semi-structured acceleration |
| 2022-2024 | Mach, schemaless LSM columnar formats, Husky, ClickHouse PVLDB, Iceberg row-level ops | Observability-specific storage, schemaless event stores, ClickHouse formalization, lakehouse evolution |
| 2025-2026 | Husky query/compaction posts, ClickHouse JSON/shared-data work, Redpanda/Confluent/Flink Iceberg paths, Pinot resilience | Operational maturity: compaction, query routing, workload isolation, streaming-lakehouse convergence |

## Open Questions

1. How much physical design can be automated from query telemetry without
   creating uncontrolled write amplification?
2. What is the right boundary between event-columnar storage and metric-specific
   hot paths for alerting?
3. How should systems expose freshness, lineage, and versioning for derived
   reports and materialized views?
4. When should full-text search stay in an OLAP engine, and when should it move
   to a dedicated search subsystem?
5. Can streaming logs/topics and lakehouse tables converge enough to eliminate
   duplicate ingestion pipelines?
6. What is the best practical strategy for high-cardinality semi-structured
   attributes: dynamic subcolumns, bucketed maps, separate key/value indexes,
   query-driven promotion, or combinations of these?
7. How should trace-aware sampling preserve debugging value under extreme data
   volume?
8. What abstractions best represent operational state, cohorts, sequences,
   reports, and business metrics over a shared event substrate?

## Final Synthesis

The strongest research-backed architecture is not a single database pattern. It
is a layered event analytics pattern:

- preserve rich timestamped facts;
- use columnar engines for interactive aggregation and high-cardinality
  exploration;
- use lakehouse table formats for durable, evolvable, replayable history;
- build workload-specific serving layouts for common queries;
- separate full-text search, hot alerting, and broad historical scans when their
  physical needs diverge;
- treat compaction, metadata, freshness, workload isolation, and query routing
  as core systems problems.

The boundary between "observability platform" and "business analytics
warehouse" is becoming less fundamental. The more important distinction is
between durable facts and the many serving views needed to answer different
questions at different latencies and costs.
