# Database Observability Research Notes

Date: 2026-06-05

This directory contains per-source research notes for papers, engineering posts,
and system documentation related to event-native analytics, ClickHouse,
lakehouse storage, real-time OLAP, engineering observability, semi-structured
analytics, and operational signal systems.

The top-level synthesis is:

- [Research Review: Event-Native Analytics And Observability Storage](../database-observability-research-review.md)

## Note Template

Each note uses this structure:

- source metadata;
- problem;
- system / architecture;
- storage model;
- ingest / write path;
- query / index model;
- compaction / materialization / evolution;
- relevance to event-native analytics and observability;
- tradeoffs and limitations;
- notable details.

## Corpus

The note files in this directory are generated from primary papers and
engineering posts. When a paper PDF was available, the note is based on reading
the paper content, not only metadata or search snippets.

- [Analyzing And Comparing Lakehouse Storage Systems](lakehouse-comparison-cidr-2023.md)
- [ClickHouse - Lightning Fast Analytics For Everyone](clickhouse-pvldb-2024.md)
- [ClickHouse JSON Type Design And Shared Data Improvements](clickhouse-json-2024-2025.md)
- [ClickHouse vs Prometheus For High Cardinality, Part 2](clickhouse-high-cardinality-2026.md)
- [Cloudflare ClickHouse Log And HTTP Analytics](cloudflare-clickhouse-log-analytics.md)
- [Columnar Formats for Schemaless LSM-based Document Stores](schemaless-lsm-columnar-pvldb-2022.md)
- [Data Formats In Analytical DBMSs: Performance Trade-Offs And Future Directions](analytical-dbms-formats-vldb-journal-2025.md)
- [Datadog Husky Event Store Introduction](datadog-husky-event-store.md)
- [Datadog Husky Exactly-Once Ingestion And Multi-Tenancy](datadog-husky-ingestion-multitenancy.md)
- [Datadog Husky Query Engine](datadog-husky-query-engine.md)
- [Datadog Husky Storage Compaction](datadog-husky-compaction.md)
- [Delta Lake: High-Performance ACID Table Storage Over Cloud Object Stores](delta-lake-pvldb-2020.md)
- [Dremel: Interactive Analysis of Web-Scale Datasets and a Decade of Interactive SQL Analysis](dremel-vldb-2010-and-decade.md)
- [Druid: A Real-time Analytical Data Store](druid-sigmod-2014.md)
- [Exploiting Cloud Object Storage For High-Performance Analytics](cloud-object-storage-analytics-pvldb-2023.md)
- [GitLab And Lago ClickHouse Business Analytics](gitlab-lago-clickhouse-business-analytics.md)
- [Gorilla: A Fast, Scalable, In-Memory Time Series Database](gorilla-pvldb-2015.md)
- [Honeycomb Wide Events, High Cardinality, Distributed Column Store, And Sampling](honeycomb-wide-events-and-sampling.md)
- [JSON Tiles: Fast Analytics on Semi-Structured Data](json-tiles-sigmod-2021.md)
- [Kafka-To-Iceberg Streaming Convergence](kafka-to-iceberg-streaming.md)
- [LogStore: A Cloud-Native And Multi-Tenant Log Database](logstore-sigmod-2021.md)
- [Mach: A Pluggable Metrics Storage Engine For The Age Of Observability](mach-cidr-2022.md)
- [Monarch: Google's Planet-Scale In-Memory Time Series Database](monarch-pvldb-2020.md)
- [Observability Query Language At Google](google-observability-query-language-2024.md)
- [OpenTelemetry Semantic Conventions And Specification](opentelemetry-semantic-conventions.md)
- [Petabyte-Scale Row-Level Operations In Data Lakehouses](iceberg-row-level-ops-pvldb-2024.md)
- [Pinot: Realtime OLAP for 530 Million Users](pinot-sigmod-2018.md)
- [Procella: Unifying Serving and Analytical Data at YouTube](procella-pvldb-2019.md)
- [Progressive Partitioning for Parallelized Query Execution in Google's Napa](napa-pvldb-2023.md)
- [Samsara Operational Signals](samsara-operational-signals.md)
- [Sentry Snuba And Unstructured Data In ClickHouse](sentry-snuba-clickhouse.md)
- [The Snowflake Elastic Data Warehouse](snowflake-sigmod-2016.md)
- [Towards Observation Lakehouses: Living, Interactive Archives of Software Behavior](observation-lakehouses-2025.md)
- [Uber Pinot Operational Analytics](uber-pinot-operational-analytics.md)
