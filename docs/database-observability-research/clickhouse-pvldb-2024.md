# ClickHouse - Lightning Fast Analytics For Everyone

- Source: https://www.vldb.org/pvldb/vol17/p3731-schulze.pdf
- Type: paper
- Year: 2024
- Authors/Org: Robert Schulze, Tom Schreiber, Ilya Yatsishin, Ryadh Dahimene, Alexey Milovidov / ClickHouse Inc.

## Problem

The paper frames ClickHouse as an OLAP system for internet-scale analytical workloads: petabyte-scale data, high ingestion rates, many concurrent low-latency queries, and a need to keep recent data queryable while aging or aggregating historical data. Observability is explicitly named as a workload where thousands of agents continuously send small events or metrics, so the database must support real-time inserts without making queries wait for batch windows.

The target is not transactional generality. ClickHouse optimizes for append-heavy analytical data, denormalized fact tables, fast scans, compression, pruning, and practical deployment across single-node and multi-node clusters.

## System / Architecture

ClickHouse is split into query processing, storage, and integration layers, with access protocols, threading, caching, RBAC, backups, and monitoring as cross-cutting components. It is implemented in C++ as a statically linked native binary.

The main persistent storage family is `MergeTree*`. Other engines provide in-memory dictionaries, distributed table views over shards, virtual access to systems such as Kafka or PostgreSQL, and table engines for files, object stores, and lake formats. Sharding distributes independent table shards; replication is available through `ReplicatedMergeTree*` engines coordinated by ClickHouse Keeper, a Raft-based ZooKeeper replacement.

The query engine uses a SQL dialect, PRQL, or KQL, then produces optimized logical and physical plans. Execution is vectorized, parallelized across SIMD lanes, CPU cores, and shards, and can opportunistically compile expressions with LLVM.

## Storage Model

`MergeTree*` tables are collections of immutable sorted parts. Each insert creates or eventually contributes to a new part. Parts contain per-column files, and small parts can co-locate columns in a single file for locality. Rows inside a part are logically divided into granules of 8192 records, the smallest unit processed by scan and index lookup operators. Compressed blocks span one or more granules, typically targeting around 1 MB by default.

Primary key columns define local sort order within each part rather than uniqueness. ClickHouse stores a sparse primary-key index mapping the first row of each granule to the granule id, which keeps the index small enough to stay memory resident. Columns can use general-purpose and specialized codecs such as LZ4, delta coding, Gorilla, FPC, ZSTD, and chained codecs. `LowCardinality(T)` adds dictionary encoding for repetitive values; `Nullable(T)` adds a null bitmap.

## Ingest / Write Path

Synchronous inserts create a new part per `INSERT`, so clients are encouraged to insert in batches. Asynchronous inserts address real-time telemetry-style small writes by buffering rows across incoming inserts into the same table until a size threshold or timeout is reached, then writing a part.

ClickHouse writes parts directly to disk instead of relying on a traditional WAL in the common path. It maintains hashes of recent inserted parts, locally or in Keeper for replicated tables, to make retrying an insert batch idempotent after a timeout. Replication records local inserts, merges, mutations, and DDL transitions in a global replication log; other replicas replay those transitions asynchronously and converge on the same table state.

## Query / Index Model

ClickHouse prunes data through the primary key index, projections, and skipping indexes. The primary key supports binary search on sorted granule marks for equality/range predicates that align with key prefixes. Projections are alternate physical layouts of the same table sorted by another primary key; the optimizer can choose a projection based on estimated I/O. Skipping indexes include min-max, set, and Bloom-filter variants over configurable groups of granules.

The execution engine pushes filters, reorders work, evaluates the most selective filters first where useful, and can remove sort operators when storage order already satisfies the plan. It parallelizes scans and aggregations across plan lanes and shards, uses exchange operators to rebalance work, supports sort aggregation when grouping columns align with the primary key, and includes many specialized hash-table implementations for joins and aggregations.

## Compaction / Materialization / Evolution

Background merges combine small immutable parts into larger sorted parts, with a default target around 150 GB. Unlike level-based LSM designs, ClickHouse treats parts as peers, which gives merge selection more freedom but means updates and deletes need separate mechanisms.

Merge-time transformations are central. Replacing merges retain the newest row for equal primary-key values, optionally guided by a version column. Aggregating merges combine partial aggregate states, commonly in materialized views. Materialized views are incrementally updated as new source parts arrive rather than periodically refreshing the whole source table. TTL merges move, recompress, delete, or roll up old parts.

Mutations rewrite parts in place and are expensive for deletes because all columns may be rewritten. Lightweight deletes mark rows in a bitmap and rely on later merges for physical removal, trading cheaper deletes for an extra read-time filter.

## Relevance To Event-Native Analytics And Observability

The paper describes a database model well aligned with event-native observability: append-heavy writes, immutable event rows, columnar compression, sparse pruning, vectorized aggregations, and background rollups. Observability agents can send small batches through asynchronous inserts, and recent data becomes queryable quickly.

A practical observability design would put common dimensions and time into the sorting key, keep raw events for drilldown, and use materialized views or TTL rollups for cheaper historical dashboards. The built-in OpenTelemetry span generation and system tables also make ClickHouse itself observable with the same style of analytical tooling.

## Tradeoffs And Limitations

The design favors append-only analytical data. Updates and deletes exist, but they are deliberately not the primary path. Replicated tables are eventually consistent by default, and statements generally do not provide full ACID semantics across concurrent writes. By default, inserts are not forced with `fsync`, accepting a small durability risk for throughput.

Choosing primary keys, projections, skipping indexes, and partitioning remains workload-dependent. Bad layouts can turn queries into broad scans. Merge-time transformations are eventual; queries may need `FINAL` when the latest deduplication or aggregation state must be applied immediately.

## Notable Details

- The sparse primary-key index can index millions of rows with only thousands of entries because it indexes granules, not rows.
- Parts are immutable directories with self-contained metadata, which simplifies snapshot-style reads and background merges.
- `Distributed` tables provide a logical view over shards but do not own storage.
- ClickHouse includes integration table functions and engines for Kafka, object stores, relational databases, lake formats, and many file formats.
- Performance tooling includes system tables, query metrics, `EXPLAIN`, sampling profiler output, and OpenTelemetry spans.
