# Druid: A Real-time Analytical Data Store

- Source: https://static.druid.io/docs/druid.pdf and https://people.csail.mit.edu/matei/courses/2015/6.S897/readings/druid.pdf
- Type: paper
- Year: 2014
- Authors/Org: Fangjin Yang, Eric Tschetter, Xavier Leaute, Nelson Ray, Gian Merlino, Deep Ganguli; Metamarkets

## Problem

Druid targets append-heavy machine event streams where users need interactive drill-downs over arbitrary dimension combinations. The paper frames the gap as a product problem: Hadoop could store and batch-process large logs, but could not provide freshness, low latency, or high-concurrency availability for customer-facing dashboards and alerting. Relational systems and key-value stores also failed the combination of low-latency ingestion, flexible filtering, and sub-second aggregate exploration over billion-row tables.

The motivating data model is timestamped events with dimensions and metrics. Druid assumes that the core analytic questions are time-bounded filters and aggregates, not transactional updates or arbitrary joins.

## System / Architecture

Druid separates roles into real-time nodes, historical nodes, broker nodes, coordinator nodes, and external dependencies: Zookeeper, MySQL metadata, deep storage, and a stream bus such as Kafka.

Real-time nodes ingest events and make them immediately queryable. Historical nodes serve immutable segments from deep storage. Brokers route queries to the right real-time and historical nodes, merge partial results, and cache per-segment answers. Coordinators own historical segment placement, replication, drops, and load balancing. The architecture is deliberately shared-nothing at the serving layer; node types interact mostly through metadata announcements rather than tight coupling.

Availability is an explicit design theme. If Zookeeper or MySQL is unavailable, Druid largely keeps serving the last known state. Historical nodes can keep answering HTTP queries for loaded segments, brokers can use their last cluster view, and coordinators stop changing assignment rather than making data unavailable.

## Storage Model

Druid tables, called data sources, are partitioned into immutable segments. A segment is typically 5-10 million rows over a time interval, with a data source id, interval, and version. The version lets Druid expose the latest segment set for an interval and drop older superseded segments.

Segments are column-oriented. String dimensions are dictionary encoded into integer ids; numeric metrics are stored as compressed raw arrays. Druid uses column compression, LZF in the paper, and stores dimension indexes for filtering. The storage model is specialized for time-series event analytics: a timestamp is mandatory and is used for distribution, retention, and first-level pruning.

## Ingest / Write Path

Real-time nodes read from a message bus, maintain an in-memory row-oriented index, and make that data queryable immediately. They periodically persist the in-memory index to disk, converting it into the columnar segment format. Persisted indexes are loaded off-heap and remain queryable.

At interval boundaries, a real-time node waits a configurable window for late events, merges persisted indexes for the interval, writes an immutable segment to deep storage, records metadata, and hands the segment off to historical nodes. Once a historical node has loaded and announced the segment, the real-time node unannounces its copy for that interval.

Kafka offsets are committed when in-memory buffers are persisted. If a node keeps its local disk, it can recover by loading persisted indexes and continuing from the last committed offset. Multiple real-time nodes can consume the same stream for replication, or consume partitions for scale.

## Query / Index Model

Druid exposes a JSON-over-HTTP query API rather than SQL in this paper. Query types include time-series aggregation, group-by-like drill-downs, top-N, and filters over Boolean expressions of dimensions. Brokers map a query to segments, use cached results for historical immutable segments, forward misses to the right serving nodes, and merge results.

The key index is a compressed bitmap inverted index from dimension value to row positions. Boolean filters are evaluated through bitmap operations, then only matching metric rows are scanned. This borrows from search infrastructure and is a major reason Druid can combine flexible filters with fast aggregation.

The paper explicitly notes that joins were not implemented. The authors argue that large distributed joins would introduce expensive materialization and memory management under high concurrency, and that Druid's target workloads got more value from fast filtered aggregates.

## Compaction / Materialization / Evolution

The core materialization unit is the immutable segment. Real-time ingestion produces many persisted indexes, then merges them into interval segments for handoff. Coordinators use rules to retain, drop, tier, and replicate segments. Hot and cold historical tiers can store recent or frequently queried data on stronger hardware and older data on cheaper hardware.

Segments can be superseded by higher-version segments for the same interval, which gives Druid a simple multi-version concurrency story over immutable files. Broker caches exploit immutability: historical segment results are cacheable, while real-time data is not.

## Relevance To Event-Native Analytics And Observability

Druid is directly relevant to observability because the paper's workload is almost exactly event-native analytics: timestamped facts, dimensions, numeric metrics, high ingestion rate, freshness, drill-downs, alerting, and high dashboard concurrency. Its major lesson is that freshness and interactivity come from treating ingestion, serving, and retention as one continuous pipeline rather than a batch load followed by warehouse queries.

For observability systems, Druid's segment handoff model is a useful pattern: fresh mutable data can be served from an ingest path while immutable optimized segments serve older data. The broker can hide that split, giving users one query surface.

## Tradeoffs And Limitations

Druid optimizes for append-only event streams and aggregate exploration, not general SQL. Joins are absent in the paper, and schema/data modeling must anticipate denormalized dimensions. The mandatory timestamp and segment interval model are strengths for time-series workloads but constraints for non-temporal data.

The system depends on several external systems: Zookeeper, MySQL metadata, deep storage, and a durable stream bus. It is resilient to outages of some of them for serving existing data, but those components still shape operational complexity.

Memory-mapped historical serving can page cold segments in and out; if a query needs more segment data than memory can hold, latency suffers. Bitmap indexes also work best when dimension encodings and cardinalities remain manageable.

## Notable Details

- Real-time ingestion latency from event creation to consumption is described as ordinarily hundreds of milliseconds.
- The paper reports a production ingest example around 500 MB/s, 150,000 events/s, or 2 TB/hour.
- Druid uses segment versions for stable reads: queries use the latest version for a time range.
- Broker cache entries are per segment, which makes invalidation simple because historical segments are immutable.
- The original source URL reset repeatedly during collection; the same paper content was read from an academic mirror.
