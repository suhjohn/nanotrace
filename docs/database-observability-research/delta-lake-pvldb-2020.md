# Delta Lake: High-Performance ACID Table Storage Over Cloud Object Stores

- Source: https://www.vldb.org/pvldb/vol13/p3411-armbrust.pdf
- Type: paper
- Year: 2020
- Authors/Org: Michael Armbrust, Tathagata Das, Liwen Sun, Burak Yavuz, Shixiong Zhu, Mukul Murthy, Joseph Torres, Herman van Hovell, Adrian Ionescu, Alicja Luszczak, Michal Switakowski, Michal Szafranski, Xiao Li, Takuya Ueshin, Mostafa Mokhtar, Peter Boncz, Ali Ghodsi, Sameer Paranjpye, Pieter Senster, Reynold Xin, Matei Zaharia; Databricks, CWI, UC Berkeley, Stanford University

## Problem

Delta Lake addresses the gap between cheap cloud object storage and database-like table management. Object stores are durable and scalable, but they expose a key-value object API with expensive listing, high per-request latency, no cheap renames, and no native atomic multi-object updates.

Raw Parquet data lakes therefore suffer from partial updates, hard rollback, slow metadata listing, and poor support for continuously updated enterprise datasets. Delta Lake adds ACID table semantics while preserving direct access to open Parquet files.

## System / Architecture

Delta Lake is an ACID table storage layer over object stores. A Delta table is a directory containing Parquet data files plus a `_delta_log` directory. The transaction log is the source of truth for which files belong to each table version.

Clients read a snapshot from the log, write new Parquet files, then commit by appending an atomic log record. Delta uses optimistic concurrency control and object-store-specific log stores. On S3, where atomic put-if-absent behavior was historically insufficient, Delta can use a lightweight coordinator for log record IDs; on stronger stores, coordination can be done directly through storage operations.

## Storage Model

Data is stored in immutable Parquet objects, usually partitioned with Hive-style directory names when useful. The log contains actions such as add file, remove file, metadata change, protocol version, and commit information. Add actions include file-level statistics such as min/max values and null counts.

The log is periodically checkpointed into Parquet so clients do not need to replay a long JSON log from the beginning. This checkpointed metadata enables faster search over file statistics and table state than listing and reading many individual Parquet footers from object storage.

## Ingest / Write Path

Writers produce new Parquet files and append log records describing file additions and removals. Updates, deletes, and merges are implemented by rewriting relevant data files and recording a transactional replacement: old files are removed from the snapshot and new files are added.

Delta also supports streaming I/O. Streaming writers can commit small files at low latency, while later compaction coalesces them into larger files. The transaction log lets streaming applications record committed offsets and achieve exactly-once behavior.

## Query / Index Model

Readers reconstruct a table snapshot from the transaction log and checkpoints, then scan the Parquet files referenced by that snapshot. Query planning uses partition pruning and file-level statistics from the log. Delta also supports time travel by reading an earlier version of the log.

The paper discusses Z-order data layout as a multi-dimensional clustering strategy. For high-dimensional predicates, Z-ordering improves data skipping beyond single-column sort or partition order because file-level min/max ranges become useful across several dimensions.

## Compaction / Materialization / Evolution

Delta uses immutable files plus transactional file replacement. Compaction rewrites many small files into larger Parquet files and commits the change as an atomic log update. Readers that have already planned against the older files can ignore compaction because table versions are immutable.

Schema evolution is handled through metadata in the log so older Parquet files can remain readable after schema changes. Vacuum-like cleanup is constrained by retention windows because old snapshots and concurrent readers may still reference removed files.

## Relevance To Event-Native Analytics And Observability

The paper is directly relevant to event analytics because it treats a table as both a batch table and a streaming substrate. Delta's transaction log can support tailing newly added files, exactly-once streaming writes, audit history, and rollback after bad ingest jobs.

For observability workloads, the most important lessons are:

- File and log metadata are part of the query path, not just catalog bookkeeping.
- Small streaming files must be compacted to preserve read performance.
- Layout strategies such as Z-ordering can matter for high-cardinality dimensions like source IP, destination IP, trace ID, service, region, or tenant.

## Tradeoffs And Limitations

The paper's Delta design provides serializable transactions within one table, not across multiple tables. Very high write transaction rates are limited by the latency of appending to object-store metadata. The authors also note that object stores make millisecond-scale streaming latency difficult; seconds-level latency was acceptable for their target enterprise workloads.

At the time of the paper, Delta had no general secondary indexes beyond file statistics, though Bloom filter indexes were being prototyped. Updates used file replacement, so sparse row changes could still create write amplification.

## Notable Details

- The paper reports that Delta Lake was deployed at thousands of Databricks customers processing exabytes per day.
- Log checkpoints are written in Parquet, allowing distributed processing of table metadata.
- Manifest files expose Delta snapshots to engines that understand directory listings but not the Delta transaction log.
- The paper includes use cases in ETL, BI, network security analytics, bioinformatics, GDPR deletion, and machine learning data management.
