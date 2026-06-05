# Petabyte-Scale Row-Level Operations In Data Lakehouses

- Source: https://www.vldb.org/pvldb/vol17/p4159-okolnychyi.pdf
- Type: paper
- Year: 2024
- Authors/Org: Anton Okolnychyi, Chao Sun, Kazuyuki Tanimura, Russell Spitzer, Ryan Blue, Szehon Ho, Yufei Gu, Vishwanath Lakkundi, DB Tsai; Apple and Tabular

## Problem

The paper explains how Apache Iceberg and Apache Spark were extended to support efficient row-level `DELETE`, `UPDATE`, and `MERGE` operations at petabyte scale. Before these capabilities, data lake users often handled changes by replacing whole partitions, which caused large rewrites, unsafe concurrent behavior, and brittle custom pipelines.

The central challenge is to support both sparse and high-density row changes without giving up lakehouse properties: open files, ACID snapshots, object storage, multiple engines, and fast analytical reads.

## System / Architecture

Iceberg stores table state as a persistent metadata tree. A catalog maps a table identifier to a root metadata file. The root points to snapshots; snapshots point to manifest lists; manifest lists point to manifests that enumerate data files and file statistics. A commit builds a new metadata tree and atomically swaps the catalog pointer.

Spark provides the execution substrate for determining affected rows and writing replacement or delete files. The paper's contribution includes Spark planning improvements that make row-level operations practical, especially storage-partitioned joins, runtime filtering, and adaptive writes.

## Storage Model

Iceberg data files are typically Parquet, ORC, or Avro objects. Row-level changes are represented using either new data files or delete files:

- Equality deletes identify rows by column values.
- Position deletes identify rows by file path and row position.
- Eager materialization rewrites affected data files.

The metadata tree records the valid data and delete files for a snapshot. Readers use snapshot metadata, partition summaries, column statistics, and delete-file associations to plan scans.

## Ingest / Write Path

For `MERGE`, `UPDATE`, and `DELETE`, Spark first determines which target rows are affected. The implementation then chooses a materialization strategy:

- Eager materialization rewrites files during the write operation.
- Lazy equality deletes write key-based delete files and merge them during reads.
- Lazy position deletes find exact row positions and write compact delete files that are applied during reads.

Storage-partitioned joins avoid unnecessary shuffles when source and target data are compatibly partitioned. Runtime filtering minimizes write amplification by pushing dynamic filters from matching source rows into target scans. Adaptive writes tune output layout to avoid producing poorly sized or badly clustered files.

## Query / Index Model

Reads use Iceberg scan planning to select relevant data files and associated delete files. Eager materialization keeps query execution close to normal table scans because deleted or updated rows have already been rewritten into base data files. Lazy strategies add read-time work because the reader must merge delete files with data files.

The paper emphasizes that delete representations are not just write-path artifacts. Equality deletes are easier to produce but may apply broadly and increase read work. Position deletes are more precise and can tolerate more sparse changes before compaction, but require finding and storing row positions.

## Compaction / Materialization / Evolution

Compaction is required mainly for lazy strategies. Equality and position delete files accumulate and eventually degrade query performance. Position deletes can sustain more modifications before compaction, while equality deletes may require more aggressive maintenance depending on key distribution and query patterns.

Eager materialization is preferred for large daily bulk operations that touch a high fraction of a table because it avoids read-time delete application and usually needs no special maintenance. Lazy strategies are better for sparse or streaming changes where rewriting whole files would waste compute.

## Relevance To Event-Native Analytics And Observability

Observability systems need row-level changes for retention, privacy deletion, deduplication, late correction, incident annotation, and CDC-derived materialized views. This paper is highly relevant because it provides a vocabulary for choosing how event mutations should be materialized:

- Bulk retention rollups can use eager rewrites.
- Sparse deletes or corrections can use position deletes.
- CDC-style ingest can benefit from lazy delete/update representations if compaction is predictable.

The runtime filtering and adaptive write lessons also map directly to high-cardinality telemetry, where only a tiny fraction of files may contain a trace ID, tenant, or time bucket affected by a correction.

## Tradeoffs And Limitations

The design targets OLAP workloads with limited concurrent writers and many readers, not OLTP workloads with millions of tiny transactions per minute. It is also centered on Iceberg plus Spark; other engines must implement compatible delete-file handling and planning behavior to get the same semantics and performance.

Lazy strategies move cost from writes to reads and maintenance. Eager strategies keep reads fast but can be very expensive for sparse changes. The paper identifies future work around secondary indexes, better runtime filtering, and better on-disk encodings for position deletes.

## Notable Details

- Iceberg supports configurable isolation by operation type, including serializable and snapshot isolation modes in its libraries.
- The paper reports order-of-magnitude performance improvement after the Spark and Iceberg optimizations.
- Position deletes are represented in memory with Roaring bitmaps, while the paper discusses custom on-disk serialization as future work.
- The related work section contrasts Iceberg with Hudi merge-on-read, Delta Lake delete vectors, and Hive ACID delta files.
