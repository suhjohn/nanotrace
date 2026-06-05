# Kafka-To-Iceberg Streaming Convergence

- Source: https://www.confluent.io/blog/introducing-tableflow/; https://www.redpanda.com/blog/redpanda-25-1-iceberg-topics-ga; https://flink.apache.org/2025/10/14/from-stream-to-lakehouse-kafka-ingestion-with-the-flink-dynamic-iceberg-sink/; https://www.vldb.org/pvldb/vol18/p5184-guo.pdf
- Type: posts/paper
- Year: 2024-2025
- Authors/Org: Marc Selwan / Confluent; Matt Schumpert and Mike Broberg / Redpanda; Swapna Marru / Apache Flink; Matteo Merli, Sijie Guo, Penghui Li, Hang Chen, Neng Lu / StreamNative

## Problem

The sources describe a converging pattern: Kafka-compatible streams are being exposed directly as lakehouse tables, especially Apache Iceberg tables. The traditional approach copies data from Kafka into object storage through connectors, schema converters, compaction jobs, and catalog registration. That creates duplicate data, operational failure points, lag between event capture and queryability, and repeated schema-mapping work.

The shared problem is making operational event streams immediately useful to SQL engines and lakehouse tools without fragile ETL.

## System / Architecture

The sources represent four architecture points:

- Confluent Tableflow materializes Kafka topics and schemas into Iceberg or Delta Lake tables using Confluent's Kora storage layer and a metadata materializer.
- Redpanda Iceberg Topics write topic data into Iceberg tables and register those tables with Iceberg-compatible catalogs.
- Flink Dynamic Iceberg Sink is an ingestion pattern and Iceberg feature that routes records dynamically from Kafka to many Iceberg tables with schema evolution.
- Ursa is a Kafka-compatible streaming engine that writes directly to open table formats in object storage through a leaderless architecture.

The pattern is stream-table duality: the same event data should be accessible as a log for producers/consumers and as a table for analytics.

## Storage Model

Tableflow converts Kafka segments to Parquet and generates Iceberg metadata or Delta transaction logs. Redpanda Iceberg Topics store data in cloud storage as Iceberg-managed Parquet files and integrate with REST-compatible catalogs such as Snowflake Open Catalog and Databricks Unity Catalog. Flink Dynamic Iceberg Sink relies on Iceberg tables, schemas, partition specs, and catalog loaders selected per record. Ursa combines a write-ahead log for hot stream reads with long-term Parquet data in Iceberg or Delta Lake.

Across sources, object storage is the durable analytical store, Parquet is the common data-file representation, and Iceberg catalogs are the interoperability point for SQL engines.

## Ingest / Write Path

Tableflow uses Schema Registry to map stream schemas into table schemas, handle schema evolution and type conversion, and continuously compact small Parquet files. Redpanda writes transactional Iceberg table updates and keeps catalog registrations current as data arrives. Flink's dynamic sink preserves Kafka metadata, extracts schema IDs, fetches writer schemas, deserializes payloads to `RowData`, and emits `DynamicRecord` objects containing a table identifier, Iceberg schema, partition spec, and payload. Ursa writes new messages to a WAL first, commits offset metadata, and later compacts WAL objects into Parquet table files.

The main difference is whether the streaming platform itself owns lakehouse writes or whether Flink/connectors perform the bridge.

## Query / Index Model

The query model is catalog-first lakehouse access. Redpanda emphasizes automatic table discovery through REST catalog sync so engines such as Snowflake, Databricks SQL, BigQuery, ClickHouse, Trino, Spark SQL, and Flink can query topic-backed tables. Tableflow exposes Iceberg tables via Iceberg REST catalog endpoints. Flink dynamic sink relies on ordinary Iceberg readers after writing. Ursa exposes committed stream data as Iceberg or Delta tables and maintains offset indexes so Kafka consumers can find records in either WAL or lakehouse storage.

Indexes are primarily Iceberg metadata, partition specs, snapshots, and file statistics. Ursa adds streaming-specific offset indexing.

## Compaction / Materialization / Evolution

Compaction is central. Streaming writes naturally create small objects; Tableflow explicitly compacts small Parquet files into larger files. Ursa compacts row-oriented WAL objects into columnar Parquet, removing WAL objects after compaction when possible. Redpanda includes snapshot expiry to manage Iceberg metadata size and supports custom partitioning for query layout. Flink Dynamic Iceberg Sink automates table creation, schema evolution, and partition-spec adaptation without restarting the Flink job.

The materialization model ranges from topic-to-table conversion (Tableflow, Redpanda), to job-driven dynamic table routing (Flink), to a streaming engine designed around open table storage (Ursa).

## Relevance To Event-Native Analytics And Observability

This convergence is highly relevant to observability because telemetry already starts as event streams. If topics can become lakehouse tables without ETL, then logs, traces, metrics events, audit events, and CDC records can be queryable through standard SQL engines while still serving streaming consumers.

The most important observability implications are:

- Freshness becomes a storage and compaction question rather than a connector scheduling question.
- Schema evolution can move closer to the event contract.
- Consumer lag, table lag, compaction lag, and catalog commit lag become key operational metrics.
- Trace/log systems can avoid separate hot stream and cold table copies if stream-table duality is implemented carefully.

## Tradeoffs And Limitations

The sources are partly product announcements, so implementation details and failure semantics vary in depth. Tableflow's 2024 post described product direction and early access, with a later note that it became generally available in March 2025. Redpanda's post is also release-oriented and emphasizes GA capabilities more than internal algorithms.

Flink Dynamic Iceberg Sink gives flexibility but adds per-record dynamic routing and schema handling overhead compared with static sinks. Its conclusion explicitly frames the choice as operational agility versus static-binding performance. Ursa reduces Kafka-style infrastructure costs by relaxing the sub-100 ms latency assumption; it targets workloads that can accept roughly hundreds of milliseconds to sub-second latency. Direct-to-table systems also need robust dead-letter handling, schema governance, compaction, snapshot expiry, and catalog failure recovery.

## Notable Details

- Tableflow uses Confluent's Kora storage layer to write Kafka segments into Parquet and a metadata materializer to produce Iceberg metadata or Delta logs.
- Redpanda Iceberg Topics GA adds custom hierarchical bucketed partitioning, dead-letter queues, schema evolution, snapshot expiry, REST catalog sync, transactional writes, and native consumer group lag metrics.
- Flink Dynamic Iceberg Sink is available with Apache Iceberg 1.10.0 or newer and Flink 1.20, 2.0, and 2.1.
- Ursa reports up to 10x infrastructure cost reduction and uses stateless brokers, a metadata service, WAL storage, compaction, and open table formats.
