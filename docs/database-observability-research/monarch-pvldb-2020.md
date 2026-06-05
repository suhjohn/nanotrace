# Monarch: Google's Planet-Scale In-Memory Time Series Database

- Source: https://www.vldb.org/pvldb/vol13/p3181-adams.pdf
- Type: paper
- Year: 2020
- Authors/Org: Colin Adams, Luis Alonso, Benjamin Atkin, John Banning, Sumeer Bhola, Rick Buskens, Ming Chen, Xi Chen, Yoo Chung, Qin Jia, Nick Sakharov, George Talbot, Adam Tart, Nick Taylor; Google LLC

## Problem

Monarch addresses Google's internal monitoring at global scale: billions of monitored entities, terabytes per second of ingested time-series data, and millions of queries per second. The earlier Borgmon model left teams operating many isolated monitoring instances, which created operational toil and made cross-service correlation hard. Borgmon also lacked schematized dimensions and value types, creating query ambiguity and limiting richer statistics such as latency distributions.

The monitoring use case drives the strongest design constraints. Alerts need fresh data even during failures, so Monarch trades consistency for high availability and partition tolerance. It avoids depending on Google's persistent storage systems on the alerting path because those systems themselves rely on Monarch for monitoring.

## System / Architecture

Monarch is a multi-tenant, globally distributed, in-memory time-series database. Its organizing principle is regional autonomy plus global management and querying. Data is ingested and stored in regional zones near its source. Global configuration, root mixers, root index servers, and root evaluators provide a unified view across zones.

Major components:

- Leaves store time-series data in memory and also participate in ingestion and query execution.
- Recovery logs store the same monitoring data on disk and feed a long-term repository.
- Ingestion routers route data to zones based on location fields.
- Leaf routers route zonal data to leaves.
- Range assigners split, merge, move, and replicate target ranges for load balancing.
- Mixers fan out queries, merge subquery results, and run at root and zone levels.
- Index servers hold field hints indexes used to reduce query fanout.
- Evaluators execute standing queries and write results back into Monarch.

## Storage Model

Monarch stores monitoring data as schematized time-series tables. A time series key is built from target fields and metric fields. A target identifies the monitored entity, such as a Borg task, and follows a target schema. One target field is annotated as the location field and determines the zone where data is stored. A metric describes a measured aspect and follows a metric schema.

Metric value types include boolean, int64, double, string, tuple, and distribution. Distribution is a compact histogram-like type with configurable bucket boundaries, statistics, and optional exemplars. Exemplars can retain a representative value and related context such as an RPC trace, which makes aggregated tail-latency views actionable.

Monarch distinguishes gauge and cumulative metrics. Cumulative time series include a start timestamp and are robust to missing points because each point reflects accumulation since a start time. This is important in distributed systems where processes restart regularly.

## Ingest / Write Path

Clients usually use Google's instrumentation library, which sends time-series points at frequencies needed by configured retention policies. Ingestion routers choose a destination zone from the target's location field. Leaf routers use a live range map to forward writes to leaves responsible for the target range. Leaves write to in-memory stores and best-effort recovery logs.

Within a zone, data is lexicographically sharded by target strings, not by every metric key. Keeping all metrics for a target together lets a single write message include many metrics and enables common intra-target joins at the leaf. Target ranges can have heterogeneous replication policies so users can trade storage cost against availability.

Range movement is designed for continuous availability. A destination leaf starts collecting new data for the moving range, recovers recent older data from recovery logs in reverse chronological order, then the source leaf is unassigned. During the transition, both leaves collect and log overlapping data.

## Query / Index Model

The query language is a pipeline of relational-algebra-like table operations over time-series tables. Example operations include `fetch`, `filter`, `align`, `join`, `group_by`, top-n selection, schema remapping, union, cross-time aggregation, and expressions such as extracting percentiles from distributions. Alignment normalizes timestamps before combining series.

Queries are either ad hoc or standing. Standing queries are periodic materialized-view queries used for faster later queries, cost reduction, and alerting. Most standing queries are evaluated at zone level because static analysis can prove they do not need cross-zone data.

Query execution forms a three-level tree: root mixer, zone mixers, and leaves. Monarch uses static analysis of schemas and query operations to push work as close to source data as possible. Location-field invariants allow zone-level completion. Target-range sharding allows leaf-level completion for operations that stay within a target. Partial aggregation at leaves and zones reduces data transfer upward.

The key index structure is the field hints index. It is a concise in-memory index over excerpts of field values, often trigrams, mapping hints to children that may contain matching data. It supports equality and regular-expression pruning without storing all exact field values. False positives are allowed, but false negatives are not. The paper reports very high fanout suppression: around 99.5% at zone level and 80% at root level.

## Compaction / Materialization / Evolution

Recovery logs are compacted, rewritten into a fast-read format, and merged into a long-term repository by background processes. Collection aggregation materializes lower-cardinality streams during ingest. For workloads such as disk I/O, clients send deltas into time buckets; leaves aggregate deltas with a configured reducer, finalize buckets after an admission window, and write the resulting points.

Standing queries are the primary query-side materialization mechanism. They are periodically evaluated and stored back into Monarch. Users can configure standing queries and alerts, including sharded execution for large inputs.

The paper's lessons emphasize continuous evolution: index servers, collection aggregation, and sharded standing queries were added after initial design as scaling pressure changed.

## Relevance To Event-Native Analytics And Observability

Monarch is directly relevant for multi-tenant observability infrastructure because it combines schema, locality, high ingest, hierarchical query execution, and materialization. It shows that structure can improve both robustness and performance without eliminating user flexibility.

For event-native analytics, the most useful ideas are:

- Store rich typed values such as distributions with exemplars so aggregates preserve drill-down context.
- Use source/entity schemas for routing, storage locality, and query planning.
- Push joins and aggregations to the lowest level where layout invariants make them complete.
- Use approximate in-memory indexes to reduce fanout while preserving correctness.
- Treat standing queries as managed materializations that support both alerts and cost reduction.

## Tradeoffs And Limitations

Monarch's design is specialized for monitoring and alerting. It accepts partial query results and drops delayed writes when necessary. That is correct for alerting freshness, but less appropriate for workloads requiring strong completeness or exact historical replay.

The system stores close to a petabyte compressed in memory in the reported deployment, which is expensive but chosen to avoid circular dependencies and improve availability. It also relies on strong internal infrastructure assumptions: global configuration in Spanner, TrueTime, Colossus-backed logs, and Google's internal instrumentation practices.

The field hints index intentionally trades precision for compactness. False positives still consume query work. The query language is powerful for time-series tables but is not a general wide-event analytics model.

## Notable Details

- Monarch has been in continuous operation since 2010.
- As of the paper's July 2019 measurement, the internal deployment stored nearly 950 billion time series using around 750 TB of memory.
- The internal deployment ingested around 2.2 TB/s in July 2019 and served more than 6 million QPS.
- Around 95% of all queries were standing queries, including alerting queries.
- Collection aggregation averaged 36 input time series per output time series, with extreme cases over one million to one.
