# Procella: Unifying Serving and Analytical Data at YouTube

- Source: https://www.vldb.org/pvldb/vol12/p2022-chattopadhyay.pdf and https://research.google/pubs/procella-unifying-serving-and-analytical-data-at-youtube/
- Type: paper
- Year: 2019
- Authors/Org: Biswapesh Chattopadhyay, Priyam Dutta, Weiran Liu, Ott Tinn, Andrew McCormick, Aniket Mokashi, Paul Harvey, Hector Gonzalez, David Lomax, Sagar Mittal, Roee Ebenstein, Nikita Mikhaylin, Hung-ching Lee, Xiaoyan Zhao, Tony Xu, Luis Perez, Farhad Shahmohammadi, Tran Bui, Neil McKay, Selcuk Aya, Vera Lychagina, Brett Elliott; Google / YouTube

## Problem

Procella was built to collapse several YouTube data-serving silos into one SQL system. YouTube had reporting dashboards, embedded page statistics, monitoring/time-series workloads, and ad-hoc analysis, each historically served by different systems such as Dremel, Mesa, Monarch, Bigtable, and Vitess-like infrastructure. That fragmentation created duplicated ETL, inconsistent data, extra cost, slower loading, incompatible APIs, and limited feature portability.

The target is unusually broad: tens of thousands of low-latency reporting queries per second, millions of real-time updates and lookups for embedded stats, monitoring functions such as expiry and downsampling, and complex ad-hoc SQL over trillions of rows.

## System / Architecture

Procella separates storage and compute on Google infrastructure. Durable data sits in Colossus. Compute runs on Borg. Metadata is stored in Bigtable and Spanner and served through metadata servers. The main components are Root Servers for query planning and orchestration, Data Servers for scan/evaluation, Metadata Servers, Registration Servers for DDL and batch registration, Ingestion Servers for real-time writes, and Compaction Servers.

The design accepts Google production realities: no local durable storage, frequent Borg eviction, heterogeneous cheap machines, high tail latency, and remote file metadata overhead. Procella therefore relies on caching, affinity scheduling, backup RPCs, request priority, and small recoverable tasks.

## Storage Model

Tables are logical SQL tables stored as many files/tablets/partitions in Colossus. Procella uses its own Artus columnar format for most data while also supporting other formats such as Capacitor.

Artus is optimized for both scans and lookups. It uses adaptive encodings rather than relying only on generic compression; supports O(1)-like row seeks for many encodings; stores metadata such as min/max, sort order, bloom filters, and encoding summaries; supports inverted indexes stored alongside columns; and exposes encoding details to the execution engine so filters can be pushed into the data format.

For nested and repeated data, Artus differs from Dremel-style repetition/definition encoding. It stores a separate column for each schema tree field and stores child occurrence counts for present parent fields, which reduces overhead on sparse nested data and still supports fast seeks.

## Ingest / Write Path

Batch ingestion is common: users generate files through offline pipelines and register them with Procella via DDL/RPC. Registration extracts metadata and secondary structures from file headers without scanning full data. If required indexes are missing, data servers can lazily generate expensive secondary structures.

Real-time ingestion goes through the Ingestion Server. It receives streamed rows through RPC or PubSub, optionally transforms them, appends them to a write-ahead log in Colossus, and sends them to Data Server memory buffers according to table partitioning. Buffers are queryable immediately and checkpointed best-effort to Colossus. Queries combine durable tablets and in-memory buffers while avoiding duplicate coverage.

Serving from buffers gives dirty-read freshness in seconds or less; it can be disabled when consistency matters more than latency.

## Query / Index Model

Procella supports near-complete standard SQL with joins, set operations, analytic functions, approximate aggregations, nested/repeated schemas, and UDFs. The Root Server parses, rewrites, optimizes, builds a query-block DAG, consults metadata for pruning, and orchestrates execution. Data Servers execute fragments over local memory buffers, cached Colossus data, remote memory, or other data servers.

The Superluminal evaluation engine uses C++ template metaprogramming, block/vectorized processing, native operation on encoded data, fully columnar structured processing, and aggressive filter pushdown. Procella supports multiple distributed join strategies: broadcast, co-partitioned, shuffle, pipelined, and remote lookup. Adaptive optimization collects statistics during execution for joins, aggregation, sorting, and shuffles.

Indexing is deliberately lightweight: zone maps/min-max, bloom filters, sort keys, partition keys, and inverted indexes. MDS-level pruning can eliminate enormous numbers of tablets before Data Servers run.

## Compaction / Materialization / Evolution

Compaction turns real-time write-ahead logs into larger partitioned columnar bundles. It can repartition data, apply SQL-defined filtering/aggregation, age out data, keep latest values, and update metadata through the registration path. That makes compaction both a storage maintenance step and a user-visible data lifecycle tool.

Procella also uses virtual tables for materialized-view-like optimization. The virtual table layer can choose among aggregate tables using both size and index/layout awareness, stitch tables with `UNION ALL`, handle lambda-style batch-plus-real-time ranges, and insert star-join logic when a dimension is not denormalized into the fact table.

## Relevance To Event-Native Analytics And Observability

Procella is a strong observability reference because it unifies four workloads that map directly to modern telemetry platforms: external reporting, embedded counters, monitoring/time-series, and ad-hoc incident analysis. It shows that an observability warehouse can preserve one SQL surface while specializing paths internally for high-QPS counters, fresh buffers, dashboard aggregates, and complex joins.

The real-time path is especially relevant: use a write-ahead log for durability, memory buffers for freshness, and background compaction for optimized durable serving. The system also demonstrates that downsampling, expiry, latest-value retention, and SQL-based lifecycle policies belong in the storage evolution layer, not only in offline jobs.

## Tradeoffs And Limitations

Procella depends heavily on Google infrastructure: Borg, Colossus, Bigtable, Spanner, RDMA, PubSub, and internal file/cache layers. Some design choices may not transfer directly outside that environment.

The breadth of workload support creates complexity. Embedded stats are optimized by disabling expensive features, compiling metadata into the root path, preloading data, caching plans, and batching RPCs. Ad-hoc workloads use adaptive execution with overhead that can double very small queries. One product surface therefore hides several specialized execution regimes.

Dirty-read real-time serving gives excellent freshness but exposes a consistency/freshness tradeoff. Users can disable buffer reads, but then accept higher data latency.

## Notable Details

- The Google publication page states Procella serves hundreds of billions of queries per day.
- Artus inverted indexes reduced some experiment-analysis query latencies by roughly 500x in production use cases described by the paper.
- Reporting instances in the paper can get high cache hit rates even when only a small fraction of data fits in memory because metadata, file handles, and affinity matter.
- Adaptive optimization is useful for large queries but too expensive for very small low-latency queries, so hints and specialized paths remain necessary.
