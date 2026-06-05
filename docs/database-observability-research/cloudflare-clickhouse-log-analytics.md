# Cloudflare ClickHouse Log And HTTP Analytics

- Source: https://blog.cloudflare.com/log-analytics-using-clickhouse/ and https://blog.cloudflare.com/http-analytics-for-6m-requests-per-second-using-clickhouse/
- Type: posts
- Year: 2018, 2022
- Authors/Org: Alex Bocharov; Monika Singh, Pradeep Chhetri / Cloudflare

## Problem

Cloudflare had two related analytics pressures: customer-facing HTTP analytics at millions of requests per second, and internal error-log analytics at hundreds of thousands of failed requests per second. The older HTTP pipeline relied on Kafka, Go consumers, PostgreSQL rollups, Citus, cron aggregation, PHP/Go APIs, and many operational dependencies. The log analytics pipeline relied on Elasticsearch and hit slow queries, high resource use, mapping explosion, weak multi-tenancy controls, operational fragility, and JVM garbage-collection costs.

For logs, the goal was to remove sampling, store every error log for the retention period, support fast queries over huge volumes, and avoid increasing cost. For HTTP analytics, the goal was to replace a complex aggregate pipeline with a simpler, fault-tolerant system that could process around 6M requests per second.

## System / Architecture

The 2018 HTTP analytics pipeline moved aggregation into ClickHouse. Kafka still carried Cap'n Proto request logs, and 106 Go consumers extracted more than 100 ClickHouse fields from raw logs. ClickHouse ingested non-aggregated request rows and produced aggregates with materialized views. A rewritten Go Zone Analytics API queried the new ClickHouse-backed data.

The resulting HTTP cluster had 36 ClickHouse nodes with 3x replication. It served analytics for millions of domains, more than 2.5 billion monthly unique visitors, and more than 1.5 trillion monthly page views. Average processing was 6M HTTP requests per second, with peaks up to 8M.

The 2022 logging architecture kept the familiar producer, shipper, queue, consumer, datastore model. Applications wrote local Cap'n Proto logs; an in-house shipper pushed streams to Kafka; inserters consumed Kafka and wrote batches into ClickHouse; Grafana and similar tools queried the resulting tables.

## Storage Model

Cloudflare emphasized columnar storage, large schemas, sparse indexes, compression, and linear scaling. For log analytics, they considered three schemas:

- A strict typed schema with every field as a column, fastest when the field set is known.
- Dynamic JSON/object-style ingestion where ClickHouse adds columns, useful only when total field count is controlled.
- Arrays grouped by data type plus array functions, with frequently accessed elements promoted to materialized columns.

They recommended the third log schema for safety when field count can exceed 1000. For HTTP analytics, they used non-aggregated request tables plus aggregate tables/materialized views. `SummingMergeTree`, nested `Map`-style structures, and `ReplicatedAggregatingMergeTree` handled many dashboard rollups and unique-count aggregate states.

## Ingest / Write Path

Cloudflare treated the inserter as part of the database system. The log analytics post stresses batch size because small ClickHouse batches create too many small parts and too much merge work. Inserters scale by increasing Kafka partitions and adding pods.

For HTTP analytics, Kafka consumers stopped doing aggregation logic. They extracted and prepared columns, wrote raw rows into ClickHouse, and ClickHouse materialized aggregates. The cluster processed all pipelines at about 11M rows per second and 47 Gbps insertion bandwidth after later upgrades.

## Query / Index Model

For logs, Cloudflare partitioned by hour with `toStartOfHour(dateTime)` because the pipeline generated TBs per day and queries normally include time predicates. Primary key choice controls on-disk sort order, compression, and query pruning. They noted that ClickHouse primary keys are not unique row constraints and cannot be updated after table creation.

Data-skipping indexes fill the gap for columns not in the primary key, especially Bloom-filter indexes over many optional fields. For dashboards that only need approximate anomaly detection, Cloudflare used an internal Adaptive Bit Rate approach: write multiple sample-resolution tables and choose the cheapest resolution appropriate for the query.

For HTTP analytics, index granularity tuning mattered. The raw request table used a larger granularity of 16384 because queries scanned millions to billions of rows. Aggregated tables used granularity 32, which cut query latency by about 50% and increased throughput by roughly 3x for small-row API queries.

## Compaction / Materialization / Evolution

The HTTP pipeline used materialized views to produce minutely aggregates from raw request data. The first schema design with eight separate `ReplicatedAggregatingMergeTree` views was too awkward: joins became huge, and parallel independent queries produced only moderate gains. The second design used `SummingMergeTree` with nested map-like structures to reduce table count and resemble the old Citus/hstore model.

Cloudflare added or relied on ClickHouse functionality such as `sumMap` to aggregate map values across shards, and kept uniques in a separate `ReplicatedAggregatingMergeTree` view because aggregate states needed merge semantics. For logs, frequently accessed array elements could be promoted into materialized columns.

Retention and purging were built around partitions. Hourly partitions in logging made retention deletes practical and gave the optimizer coarse time pruning.

## Relevance To Event-Native Analytics And Observability

Cloudflare's posts are production evidence that ClickHouse can support event-native observability at very high write rates. The logging article explicitly describes logs as unpredictable, semi-structured, contextual, and write-heavy, with less than 1% commonly read but that 1% being critical.

The design pattern is full-fidelity or near-full-fidelity event capture into ClickHouse, then query-time or materialized analytical views for dashboards and support workflows. The error-log migration replaced sampling with 100% retention for the target period because ClickHouse row compression and analytical scans made the economics feasible.

## Tradeoffs And Limitations

ClickHouse is not a full-text search replacement for every Elasticsearch workload. Cloudflare's conclusion is that Elasticsearch is excellent for full-text search while ClickHouse is excellent for analytics. Dynamic schema ingestion can be dangerous when one application emits too many fields. Primary-key mistakes are hard to fix because the key cannot be changed in place.

Operationally, ClickHouse still required careful batch sizing, partition choice, index design, hardware planning, and recovery behavior management. The 2018 post noted uncertainty about query performance at hundreds of nodes, and that ClickHouse did not throttle recovery well in heterogeneous hardware replacement.

## Notable Details

- The 2018 old HTTP Kafka topic carried about 6M logs per second across 106 brokers and 106 partitions.
- ClickHouse compressed average HTTP request records to about 36.74 B in the planning comparison, far smaller than raw Cap'n Proto.
- Moving error logs from Elasticsearch to ClickHouse reduced one cited document/row footprint from 600 bytes to 60 bytes.
- The error-log migration reduced inserter CPU and memory by about 8x and improved p99 query latency.
- Cloudflare contributed upstream ClickHouse functions and optimizations, including `sumMap` and SummingMergeTree map merge speedups.
