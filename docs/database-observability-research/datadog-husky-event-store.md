# Datadog Husky Event Store Introduction

- Source: https://www.datadoghq.com/blog/engineering/introducing-husky/
- Type: post
- Year: 2022
- Authors/Org: Richard Artoul, Cecilia Watt / Datadog

## Problem

Datadog's earlier log storage systems were built for a narrower log-management product. As the platform expanded into logs, RUM, network performance, profiling, and other event-like products, the old designs exposed several scaling limits:

- Metrics-style preaggregation was not appropriate for logs because logs require retaining high-cardinality, high-context events and supporting arbitrary aggregation at query time.
- The first log system used multi-tenant clustered search infrastructure where one bad node or tenant could disrupt a large cluster.
- The second system isolated storage nodes and introduced shard routing, but tenant bursts could still degrade colocated tenants, and new products required longer retention, arbitrary-field querying, array/window functions, and sketch reaggregation.
- Datadog needed independent scaling of ingest, storage, compaction, and query compute, plus stronger control over tenant isolation and quality of service.

## System / Architecture

Husky is Datadog's third-generation event store. The post describes it as an unbundled, distributed, schemaless, vectorized column store over commodity object storage, with hybrid search and analytics behavior.

The architecture splits the old storage-node role into:

- Writers: consume from Kafka, briefly buffer events, encode custom storage files, upload to blob storage, then commit file visibility to metadata.
- Compactors: discover small files through metadata, rewrite them into larger files, upload compacted outputs, and atomically swap old files for new ones in metadata.
- Readers: run leaf-level scans over files in blob storage and return partial results to the distributed query layer.
- Metadata store: a thin layer over FoundationDB, used as the strongly consistent source of truth for visible files.
- Blob storage: durable raw file storage, with S3-like systems handling byte durability and replication.

The important architectural shift is that query-time systems never call Writer nodes. Ingest and query can therefore be scaled, isolated, and degraded independently.

## Storage Model

Husky stores events in custom files on blob storage and tracks those files in FoundationDB-backed metadata. The blog does not reveal the file format internals in this introductory post, but it emphasizes these storage principles:

- Data is stored as events rather than preaggregated metric series.
- The system is columnar and vectorized because Datadog's event platform increasingly resembles a real-time data lake.
- Stateful storage is reduced to two abstractions: metadata and blob storage.
- The design does not distinguish "fresh" and "historical" data at the architecture level.
- Files become visible to queries only after metadata commit.

This removes the old concepts of search-cluster replicas and per-node shard ownership from the durable data model.

## Ingest / Write Path

The write path is:

1. Writers read events from Kafka.
2. Writers buffer briefly in memory.
3. Writers encode and upload custom files to blob storage.
4. Writers commit the new files into the metadata store.

Because Writers are stateless, they can autoscale with incoming volume. Since query traffic does not touch Writers, large queries should not starve ingestion and ingestion bursts should not directly impair query workers.

## Query / Index Model

The Reader nodes scan individual files in blob storage and return partial aggregates. A distributed query engine reaggregates those partial results into final query output.

The post focuses less on indexing details and more on query isolation:

- Query compute is independent of tenant ingest volume.
- A small tenant can temporarily burst a long historical query across many Reader nodes if capacity is available.
- A large ingest tenant can be limited on query compute if it does not need low-latency reads.
- Reader pools can be partitioned by product, workload, or tenant.
- Human-generated queries can be isolated from automated monitor queries.

This is directly tied to Datadog's Flex Logs tier, where query performance and retention/cost can be tuned separately.

## Compaction / Materialization / Evolution

Compactors are distributed services analogous to the compaction subsystem in an LSM-tree, except they operate on object-store files rather than local disk files. They:

- scan metadata for small files from Writers or prior compactions;
- merge files into larger ones;
- upload compacted outputs;
- perform an atomic metadata transaction that deletes old inputs and creates new outputs.

Queries therefore see either the pre-compaction file set or the post-compaction file set, never a partial transition.

The post frames compaction as a first-class role rather than incidental maintenance. That is a central event-native storage pattern: ingestion writes quickly, and background processes evolve the layout for efficient query serving.

## Relevance To Event-Native Analytics And Observability

Husky is a direct production example of event-native observability storage:

- Logs, RUM, network events, profiler data, and other product events share a common event-store substrate.
- The durable record is the granular event, not only a metric point.
- Arbitrary dimensions and high cardinality are design requirements.
- Query cost, retention cost, and ingest cost are controlled by separating compute and storage roles.
- Monitoring queries and human exploration can be isolated even when they read the same durable event corpus.

The migration story also shows why a storage engine for observability tends to become a platform: once the event model supports multiple products, new products push for richer query functions and retention tiers.

## Tradeoffs And Limitations

- Remote object storage raises median query latency compared with local SSD storage. Datadog accepted a slightly higher latency floor to reduce p95, p99, and max latencies.
- Building and migrating to Husky took more than a year and a half before a full product migration.
- Owning the storage engine gives flexibility but also creates long-term maintenance burden.
- The introductory post does not explain file layout, indexing, deduplication, or query optimization mechanics; those are covered in later posts.
- FoundationDB becomes a critical control-plane dependency, though its strict serializability is also what simplifies correctness.

## Notable Details

- Datadog dual-wrote to the old system and Husky, then shadowed all query load for months before switching.
- The system's main stateful dependencies are FoundationDB for metadata and blob storage for files.
- Migration improved tail latency while slightly worsening median latency because object storage has a higher latency floor.
- Query pools can be isolated by automated monitoring versus human exploration.
- The old second-generation system used Shard Router, Kafka shards, two storage replicas per shard, and a custom query engine. Husky keeps the lesson of controlled routing but removes durable shard ownership from storage nodes.
