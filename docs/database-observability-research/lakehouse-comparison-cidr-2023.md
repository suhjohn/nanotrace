# Analyzing And Comparing Lakehouse Storage Systems

- Source: https://www.cidrdb.org/cidr2023/papers/p92-jain.pdf
- Type: paper
- Year: 2023
- Authors/Org: Paras Jain, Peter Kraft, Conor Power, Tathagata Das, Ion Stoica, Matei Zaharia; UC Berkeley, Stanford University, Databricks

## Problem

The paper studies the design tradeoffs behind open lakehouse table formats: Delta Lake, Apache Hudi, and Apache Iceberg. These systems are expected to provide database-like management features over cheap object storage while staying open to multiple engines and non-SQL workloads.

The core problem is that object stores are high-latency key-value systems with weak multi-object transactional primitives. Raw Parquet/ORC data lakes make it expensive to discover files, coordinate multi-file mutations, enforce isolation, and provide low-latency query planning. Lakehouse formats solve this by adding transaction metadata, file-level statistics, snapshots, and write protocols, but the paper shows that different metadata and ingest designs produce large behavioral and performance differences.

## System / Architecture

All three systems use multi-version concurrency control. A transaction reads a table snapshot, writes new data and metadata, and commits by atomically updating the table's metadata state. The architectural split is between:

- A storage layer made of open data files, usually Parquet or ORC, in object storage.
- A metadata layer that defines the current snapshot and tracks file statistics.
- Client libraries embedded in compute engines such as Spark or Presto.

Delta Lake coordinates through object-store atomic append or put-if-absent mechanisms when available. Hudi and Iceberg use table-level locks, often backed by Hive Metastore, ZooKeeper, or DynamoDB. The paper notes that common Hive Metastore lock integrations can have edge-case correctness risks if a lock heartbeat times out around a metadata write.

## Storage Model

The formats differ mainly in metadata representation. Delta Lake and Hudi use a tabular/log-like metadata model: commits add records to a metadata table or transaction log, and checkpoints compact that state. Iceberg uses a hierarchical metadata tree of root metadata files, manifest lists, and manifests. That hierarchy allows clients to skip large groups of files using manifest-level summaries before reading per-file metadata.

Data files remain open columnar objects. The metadata stores file names, partition values, file sizes, and zone-map-like min/max statistics used by query planners. This makes object-store `LIST` less central than in raw Hive-style layouts.

## Ingest / Write Path

The paper compares two broad mutation strategies:

- Copy-on-write rewrites full data files that contain modified rows.
- Merge-on-read writes incremental changes separately and reconciles them during reads.

All three systems support copy-on-write. Iceberg and Hudi support merge-on-read in the versions studied; Delta Lake was copy-on-write in the paper, with deletion-vector work noted as a future direction.

The ingest tradeoff is direct: copy-on-write keeps reads simple and fast but causes write amplification for sparse updates. Merge-on-read lowers write latency for frequent row-level changes but increases read amplification and usually needs compaction.

## Query / Index Model

The paper treats metadata access as a first-order query planning cost. Delta Lake and Hudi can process metadata in distributed Spark jobs, which is useful for very large tables with many files. Iceberg's client-side planning was faster for small tables but could become a bottleneck at very high file counts because planning was performed on a single node in the tested implementation.

The index model is mainly file and partition pruning through statistics: partition summaries, file min/max, and metadata structures that help avoid scanning irrelevant data files. The paper's benchmark isolates selective queries to show that metadata strategy, not scan throughput, can dominate latency.

## Compaction / Materialization / Evolution

Compaction is the bridge between ingest and query performance. Merge-on-read systems need periodic compaction or clustering to keep read amplification bounded. Copy-on-write systems perform materialization eagerly, so they shift more work onto write paths.

The paper frames an open problem around adaptive compaction: a lakehouse should pick a strategy based on workload, update density, file count, and read/write latency goals rather than exposing a fixed choice to users.

## Relevance To Event-Native Analytics And Observability

Observability data has the same shape that stresses lakehouse systems: high ingest volume, many small appends, late-arriving corrections, sparse deletes for retention/privacy, and selective queries over time, service, host, trace, or tenant dimensions. This paper is useful because it separates three concerns often conflated in lakehouse discussions:

- Transaction coordination affects concurrent ingestion safety.
- Metadata planning affects p95 query startup on large event tables.
- Mutation strategy affects freshness and compaction debt.

For event-native analytics, the strongest takeaway is that "open table format" is not enough. The metadata planner and materialization policy determine whether a trace or log table stays queryable as file counts and update rates grow.

## Tradeoffs And Limitations

The benchmark uses Spark on EMR to focus on table-format differences, but this also means results reflect the tested engine integrations and versions. Vendor-optimized runtimes may behave differently.

The analysis is table-local. Cross-table transactions, global indexes, high-QPS OLTP-like updates, and sub-second streaming ingest are outside the evaluated scope. The paper also emphasizes that all three systems still face open problems around high write QPS because every write must update object-store-resident metadata.

## Notable Details

- LHBench compares read-only TPC-DS queries, load/merge workloads, metadata planning, and update strategies.
- The paper reports up to 10x differences for individual read-only queries and over 5x differences for load and merge workloads.
- For selective queries over a 200K-file table, Delta's distributed metadata planning scaled better than Iceberg's single-node planner in the tested version.
- The conclusion highlights three future directions: workload-aware compaction, cost-based metadata planning, and higher concurrent write throughput.
