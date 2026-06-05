# Datadog Husky Query Engine

- Source: https://www.datadoghq.com/blog/engineering/husky-query-architecture/
- Type: post
- Year: 2025
- Authors/Org: Sami Tabet / Datadog

## Problem

Husky must provide interactive queries over more than 100 trillion events and billions of daily queries across logs, traces, network data, and other event types.

The query engine has to work despite:

- no fixed schema or stable column types;
- large differences in tenant data shape and event size;
- queries that may span millions of object-store fragments and petabytes of data;
- a mix of selective needle-in-haystack searches and broad analytical aggregations;
- object storage latency and cost;
- multi-tenant noisy-neighbor risk.

## System / Architecture

The query path is split into four services:

- Query planner: entry point for event-store queries; resolves context such as facets, index configurations, and Flex Logs settings; validates and throttles; plans; aggregates subquery results; connects to other Datadog stores.
- Query orchestrator: fetches fragment metadata, prunes work, dispatches fragment queries to readers, and coordinates aggregation.
- Metadata service: frontend over FoundationDB clusters; abstracts metadata internals while preserving atomic file-set visibility during compaction.
- Reader service: executes fragment-level scans, uses caches and pruning, and fetches blob ranges only when necessary.

The planner splits queries into time-based steps and schedules them to orchestrators. Orchestrators use fragment metadata such as file paths, versions, row counts, timestamp boundaries, and zone maps before sending work to readers.

## Storage Model

Husky stores events as fragments in blob storage. Each event has a timestamp and arbitrary attributes. Different products and tenants create highly variable schemas.

The query post highlights these physical structures:

- fragments can hold millions of rows;
- fragments are divided into row groups;
- row groups include metadata such as per-column min/max values;
- fragment metadata can include column existence and small explicit value lists;
- text-search segments attach posting-list and n-gram indexes to fragments;
- immutable fragment references make cache invalidation tractable.

Because fragment metadata points to static data, unchanged metadata implies unchanged data behind it.

## Ingest / Write Path

This post is query-focused, but it depends on the ingest and compaction paths:

- Writers create immutable fragments.
- Compactors rewrite fragments and atomically update metadata.
- Locality compaction produces pruning regexes used by metadata-level filtering.
- Text-search segment data and row group metadata are available to readers for query reduction.

The query path relies on the invariant that any fragment version referenced by metadata is immutable for the life of the query.

## Query / Index Model

Husky supports two dominant query families:

- highly selective event searches, such as IPs, request IDs, errors, or trace identifiers;
- analytical searches, such as time series, breakdowns, and distributions.

Reader execution uses an iterator model inspired by Volcano, with operators supporting open, next, and close. Operators can be chained, reordered when semantics allow, and run in parallel pipelines.

Key optimizations:

- Lazy row group decoding: scan returns references, and data is decoded only when accessed.
- Row group metadata pruning: min/max and other metadata can skip whole row groups.
- Cost-based predicate ordering: cheap predicates run first so expensive columns may never be decoded.
- Vectorized operations: small row groups and simple operators enable CPU-friendly execution.
- Text search segments: posting lists answer term searches; hashed n-grams narrow wildcard searches; bitsets rewrite predicates into row sets.
- Fragment-level result cache: caches previous query results per fragment in memory and on disk.
- Blob range cache: RocksDB-backed cache for object-store chunks, with cache bypass under disk pressure.
- Predicate cache: caches expensive predicate bitsets per fragment and reader node.

The pruning example in the post shows that out of 1,000 fragment queries, 300 can be pruned at metadata level, 560 by result cache, 78 by column metadata, 28 by other caches, leaving only 34 that read actual fragment data and 4 that hit blob storage.

## Compaction / Materialization / Evolution

Compaction contributes to query efficiency by:

- reducing small fragments;
- generating locality layouts;
- creating pruning metadata used by the metadata service;
- preserving immutable fragment snapshots for result caching and query shadowing.

The post also describes future evolution toward a more deconstructed database design using Apache Arrow, Parquet, Substrait, and DataFusion. Datadog is exploring decoupling cache storage from query compute, after already separating ingest from query compute.

## Relevance To Event-Native Analytics And Observability

This is one of the clearest production descriptions of interactive observability querying over an event-native store:

- The query layer is a full service architecture, not a thin database client.
- Query context resolution includes product-specific settings, access controls, and cross-store connectors.
- Fragment pruning, lazy evaluation, and cache locality allow schema-less event data to behave interactively.
- Text search and analytical aggregation coexist in one event store, but use specialized per-fragment indexes and bitsets.
- Tenant isolation is handled by routing, shuffle sharding, and reader-pool controls rather than by separate clusters for every tenant.

For event-native analytics, the post shows that broad retention and rich events are viable only if most queries avoid reading most data.

## Tradeoffs And Limitations

- Many caches are reader-local, so routing consistency is essential for hit rate.
- Round-robin routing would balance load but lose affinity and isolation.
- Consistent hashing gives affinity but not enough tenant isolation because a tenant could reach all workers.
- Shuffle sharding improves isolation but requires sizing shards by tenant usage and handling shard overlap.
- Streaming partial results improves interactivity but makes retries harder; checkpointing is needed to avoid duplicate results after mid-query failures.
- Predicate cache has a low hit ratio, around 3 percent, so its value depends on high saved-work efficiency when it does hit.

## Notable Details

- Zone-map pruning can reduce downstream work by up to 60 percent for structured events and around 30 percent on average.
- Result cache hit ratio is around 80 percent for fragment-level queries.
- Blob range cache hit ratio is around 70 percent.
- Predicate cache average efficiency is around 11, meaning saved CPU can be about an order of magnitude above build cost.
- Only 3.4 percent of sample fragment queries scan real data, and 0.4 percent hit blob storage.
- Reader routing uses shuffle sharding with configurable per-tenant shard size, fragment-based affinity, weighted virtual nodes for cold starts and heterogeneous machines, and load-aware secondary routing.
- Progressive result streaming lets dashboards show partial results before long-tail fragments finish.
