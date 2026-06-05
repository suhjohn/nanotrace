# Datadog Husky Exactly-Once Ingestion And Multi-Tenancy

- Source: https://www.datadoghq.com/blog/engineering/husky-deep-dive/
- Type: post
- Year: 2023
- Authors/Org: Daniel Intskirveli, Cecilia Watt / Datadog

## Problem

Husky is optimized for large scans and aggregations, not high-rate point lookups. That creates a hard ingestion problem: Datadog must prevent duplicate events without querying the event store for every incoming event ID.

The system also has to preserve multi-tenant isolation under bursty workloads:

- tenants can increase traffic by one or two orders of magnitude quickly;
- routing decisions must be deterministic across distributed Shard Router nodes;
- each downstream Writer should process only a bounded tenant set;
- file count must stay low because object creation is a cost and compaction input;
- Writer nodes should remain stateless and autoscalable.

Exactly-once ingestion matters because duplicate observability events can affect monitors, usage reporting, billing, and cost attribution.

## System / Architecture

The ingestion architecture combines upstream deterministic routing with Husky Writers:

- Shard Router introduces locality into Kafka streams by routing events to shard placements.
- A shard is a group of Kafka partitions; multiple partitions, topics, or clusters can map to a shard.
- Sharding Allocator creates immutable, time-bounded shard placements using FoundationDB transactions.
- Autosharder adjusts per-tenant shard counts using recent traffic metrics, especially bytes ingested.
- Sharding Balancer adjusts placement salt values to reduce per-shard traffic imbalance.
- Writers consume assigned shards, create Husky files in blob storage, and commit those files to the metadata store.

This keeps deduplication local to a shard while allowing tenant shard counts and shard assignments to evolve over time.

## Storage Model

Husky isolates tenant event data into dedicated tables and does not mix different tables in the same file. That isolation improves tenant management and correctness, but it creates a near-linear relationship between the number of tenants a Writer processes and the number of output files it uploads.

The post introduces a second storage class for deduplication:

- regular event data tables store the events;
- ID tables store event IDs;
- ID tables are unique per shard ID and partitioned by time;
- event data and ID data are committed together in the same metadata transaction.

ID tables are ordinary Husky tables, so compaction, retention, and object-store layout optimization apply to them too.

## Ingest / Write Path

The write path has two connected layers.

At the routing layer:

1. Shard Router receives events.
2. It looks up the tenant's immutable shard placement for the event timestamp.
3. It hashes the event ID against that placement to pick a shard.
4. It publishes to Kafka partitions corresponding to that shard.

At the Writer layer:

1. Writers consume one or more shards.
2. They maintain in-memory event ID sets for assigned shard/time intervals.
3. If needed, they lazily page event ID intervals from Husky ID tables.
4. They write both event data and ID table data.
5. They commit both atomically to the metadata service.

During shard reassignment or worker restart, two Writers can temporarily process the same shard. Husky handles this with optimistic concurrency over ID table versions in FoundationDB-backed metadata. Out-of-order updates are rejected, local ID cache is discarded, and consumer progress resets to the latest committed fragment.

## Query / Index Model

The post is ingestion-focused, but its query implications are important:

- Deduplication avoids the need for query-time duplicate filtering.
- Locality means each Writer and downstream file set sees fewer tenants, which reduces file fanout and later query/compaction overhead.
- ID tables are read by Writers rather than user queries, but they use Husky's storage format and compaction path.
- The decision not to query Husky for event IDs keeps the scan-oriented query engine away from high-rate point lookup traffic.

## Compaction / Materialization / Evolution

Shard placements are immutable but time-bounded, usually covering only a few minutes. This makes routing deterministic for a given event timestamp while still letting future placements change as tenant traffic changes.

Autosharding and balancing evolve the routing topology:

- Autosharder increases or decreases a tenant's shard count based on observed byte volume.
- Sharding Allocator chooses shards with consistent hashing plus a persisted salt.
- Excluded shards can be skipped to help lagging consumers recover.
- Sharding Balancer periodically simulates tenant placement moves and applies moves that reduce traffic variance.

Compaction is indirectly affected because fewer tenants per Writer means fewer output files and less compaction work.

## Relevance To Event-Native Analytics And Observability

This post is highly relevant for event-native systems that want correctness without giving up append-heavy ingestion:

- Exactly-once is treated as an ingestion and metadata problem, not a query cleanup problem.
- Event IDs become part of the durable event corpus, with the same retention and compaction machinery as events.
- Tenant routing is dynamic and time-aware rather than a static hash partition.
- Byte volume, not event count, is the primary cost predictor because observability events vary heavily in size.
- The system accepts that burst handling, deduplication, and file-count control are the same design space.

For observability platforms, this is a strong pattern: use deterministic stream locality to make deduplication tractable, then use a strongly consistent metadata store to make file visibility and ID visibility atomic.

## Tradeoffs And Limitations

- Sharding Allocator is critical to ingestion availability and latency, so Datadog shards it into isolated deployments with separate FoundationDB clusters.
- Deterministic routing requires every Shard Router to agree on placements; stale or inconsistent placement state would risk duplicates.
- ID tables increase stored data and background compaction work, though they avoid a separate deduplication database.
- Writer restarts may cause event ID page-in work, though the post says this is far lower than the ingest rate in practice.
- Assignment changes are only reflected at new placement boundaries, so sharp bursts inside one placement interval can overload the current shard set until autoscaling, exclusion, or future placement changes help.

## Notable Details

- Shard placements are fixed-interval, immutable, non-overlapping, and consistent for a tenant/timestamp tuple.
- FoundationDB is used both to create placements atomically and to detect conflicting Writer commits.
- The Autosharder down-shards more slowly than it up-shards.
- Placement selection uses a consistent hash over tenant ID plus a balancing salt, then adjacent shards on a ring.
- A Shard Exclusion removes an overloaded shard from future placements while old traffic drains.
- Writer ID page-in can run around two orders of magnitude faster than event ingestion processing.
