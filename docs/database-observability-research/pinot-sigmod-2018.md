# Pinot: Realtime OLAP for 530 Million Users

- Source: https://cwiki.apache.org/confluence/download/attachments/103092375/Pinot.pdf
- Type: paper
- Year: 2018
- Authors/Org: Jean-Francois Im, Kishore Gopalakrishna, Subbu Subramaniam, Mayank Shrivastava, Adwait Tumbde, Xiaotian Jiang, Jennifer Dai, Seunghyun Lee, Neha Pawar, Jialiang Li, Ravi Aringunram; LinkedIn

## Problem

Pinot addresses near-real-time OLAP for internet-scale user-facing applications. LinkedIn needed one system that could ingest fresh events from streams, serve tens of thousands of analytical queries per second, and support both simple high-QPS product features and lower-QPS exploratory dashboards.

The paper contrasts Pinot with relational databases, key-value stores, batch OLAP systems, and offline engines. The specific hard combination is high ingest rate, high query rate, low latency, flexible drill-down, uninterrupted operation, and cloud-friendly deployment. Pinot explicitly trades full transactional semantics and general SQL for serving-focused analytical performance.

## System / Architecture

Pinot has controllers, brokers, servers, and minions, with Zookeeper, Apache Helix, and a persistent object store as supporting infrastructure. Servers host segments and process queries. Brokers parse and route queries, merge results, and hide replication/partitioning choices. Controllers manage cluster metadata, segment assignment, and real-time segment completion. Minions run maintenance tasks such as purging and segment rewrites.

The architecture supports both offline and real-time data. At LinkedIn, business events are published to Kafka and ETL'd to HDFS; Pinot can consume Kafka directly while also loading optimized offline segments. This is a lambda-style design, with brokers presenting a merged query surface across streaming and batch-built data.

## Storage Model

Pinot data is organized into fixed-schema tables made of immutable segments. A segment usually contains tens of millions of records and is stored as a directory with metadata and index files. Segment metadata records schema, column type, cardinality, encoding, statistics, and available indexes.

Segments are columnar. Pinot uses dictionary encoding, bit packing, inverted indexes, and physical ordering. The paper emphasizes that physical record ordering by high-value filter columns can be more effective than bitmap-only execution for dominant query patterns. Segments can be replaced with newer versions to support corrections or updates at segment granularity.

## Ingest / Write Path

Offline ingestion pushes segments generated from Hadoop or other batch systems. Real-time ingestion consumes Kafka partitions. Independent replicas consume from the same Kafka start offset and build local real-time segments. Because replicas may complete at different offsets when using time- or size-based thresholds, Pinot uses a segment completion protocol coordinated by the controller.

The controller state machine can instruct consumers to hold, catch up, keep, commit, discard, or redirect to the current leader. The goal is to ensure all replicas converge on identical committed segment contents with minimal network transfer. Kafka retention bounds how long Pinot can recover directly from the stream, so committed segments are persisted to durable object storage.

Pinot also supports on-the-fly schema changes. Adding a new column applies defaults to existing segments and makes the new field available without downtime.

## Query / Index Model

Pinot exposes PQL, a SQL-like subset supporting selection, projection, aggregation, and top-N queries. It does not support joins, nested queries, DDL, or row-level mutations in the paper.

Query planning is designed around specialized physical operators for different encodings and indexes. Pinot supports bitmap inverted indexes like Druid, but also uses sorted-column range scans, vectorized execution when predicates align with physical ordering, and star-tree indexes for iceberg-style aggregations. Star-trees store preaggregated nodes and can transparently answer eligible queries; other queries fall back to raw segment data.

Brokers use routing tables to assign segments to servers. For large clusters, Pinot avoids contacting every server for every query because stragglers dominate tail latency. It generates approximately minimal, balanced routing tables and supports partitioned routing when query filters match the partition function.

## Compaction / Materialization / Evolution

Pinot's main materialization is the immutable segment, but it evolves segments aggressively. Servers can append index files, create inverted indexes on demand, and reindex data. Minions rewrite segments for expensive maintenance tasks, including legal purges, by downloading, expunging records, rebuilding indexes, and replacing prior segments.

Star-tree indexes are a more explicit materialization layer: they preaggregate common high-value paths while retaining raw data for non-covered queries. Offline data generated from Hadoop can replace or complement real-time data because offline generation can globally optimize an hour or day of data, including aggregation and layout.

## Relevance To Event-Native Analytics And Observability

Pinot is highly relevant for user-facing observability-style analytics where latency and concurrency are product requirements. The system is optimized for fresh immutable append-only data, segment-level replacement, multi-tenant query serving, and workloads split between simple high-QPS queries and richer operator dashboards.

For observability, Pinot's lesson is that the physical layout should track query shapes. If most trace, metric, or event queries filter by tenant, service, entity, or viewer id, sorted columns and partition-aware routing can beat generic inverted indexes alone. The star-tree path is also relevant for top-k dashboards where exact raw flexibility is less important than predictable latency.

## Tradeoffs And Limitations

Pinot narrows the query language. No joins or nested queries means users must denormalize or precompute relationships. Segment immutability simplifies serving, but corrections require segment replacement. Real-time ingestion is asynchronous and eventually timeline-consistent rather than strongly transactional.

Several optimizations are workload-specific. Physical sorting by `vieweeId` is excellent for "Who viewed my profile" but less useful when predicates are unpredictable. Star-trees improve iceberg queries, but add storage and build complexity and only help matching query forms.

The architecture also has operational complexity: Helix/Zookeeper metadata, controllers, brokers, servers, minions, Kafka, and object storage all participate in correctness and availability.

## Notable Details

- Production scale in the paper: over 3,000 geographically distributed hosts, 1,500+ tables, more than one million segments, and 50,000+ QPS.
- Pinot treats local storage as cache; persistent state lives in object storage and Zookeeper metadata.
- Multitenancy uses token buckets where query cost is proportional to execution time.
- Query routing is explicitly tail-latency-aware; larger clusters need fewer contacted hosts per query.
- Pinot and Druid share many architectural ideas, but Pinot emphasizes high-throughput simple serving queries more heavily.
