# Mach: A Pluggable Metrics Storage Engine For The Age Of Observability

- Source: https://vldb.org/cidrdb/2022/mach-a-pluggable-metrics-storage-engine-for-the-age-of-observability.html ; https://vldb.org/cidrdb/papers/2022/p12-solleza.pdf
- Type: paper
- Year: 2022
- Authors/Org: Franco Solleza, Andrew Crotty, Suman Karumuri, Nesime Tatbul, Stan Zdonik; Brown University, CMU, Slack Technologies, Intel Labs, MIT

## Problem

Mach targets the part of observability storage where metrics arrive as very high-volume, mostly append-only small writes and must remain queryable in real time. The paper uses Slack as the motivating scale point: billions of unique sources per day, millions of samples per second, and terabytes of compressed data per day. The core problem is not only time growth, but also the "space dimension": millions or billions of active sources, source churn, and irregular write behavior.

The paper argues that common storage models fit poorly. Relational schemas struggle with variable labels and multiple values without sparse columns, complex opaque attributes, or per-source tables. Traditional time-series assumptions also miss observability realities such as irregular event-driven samples. General embedded engines pay for flexibility that the workload does not need, while full TSDBs are often optimized for analytical reads or bulk loads rather than unbatched real-time ingestion.

## System / Architecture

Mach is positioned as a pluggable embedded storage engine, analogous in role to RocksDB or WiredTiger, rather than a whole distributed monitoring system. It intentionally leaves routing, scraping, Kafka, dashboards, alerts, and higher-level query language concerns to surrounding systems.

Its main architectural idea is loose coordination. A data source is assigned to one writer thread; each writer behaves like an independent mini storage engine with private state. This avoids the mutex-heavy path of systems that allow multiple writers to converge on a shared per-series object. Mach also adds thread-level elasticity: more writer threads can be added under bursty workloads without increasing write contention in the same way a tightly coordinated design would.

## Storage Model

The logical sample model is:

- Source identity: labels or tags identify the emitting source.
- Timestamp: a 64-bit time value, expected to increase per source for the common fast path.
- Values: one or more floating-point values, so Mach directly supports multivariate metrics.

Internally, application registration maps a source to a 64-bit id and configuration describing sample shape and compression parameters. The write buffer has two main layers: an active segment and an active block. Active segments are in-memory, columnar, fixed-size buffers, with the default size described as 256 samples. When a segment fills, it becomes immutable and is compressed. Active blocks are page-sized buffers that collect compressed segments before flushing to files.

Persistent data is organized as blocks in writer-private files. The block identifier combines file name and offset. A per-source block index stored through global state points readers to persisted blocks. The current prototype uses a linked-list index, chosen to make snapshots cheap and recent reads fast.

## Ingest / Write Path

The public API has `register`, `push`, and `get_range`. `register(config)` creates a source id and binds shape/compression metadata. `push(id, ts, values)` appends a sample from that source. The fast path looks up source metadata in thread-local state and appends to the active segment. Rare out-of-order inserts go into a separate buffer that is periodically merged.

Mach delays heavier work until a segment fills. It compresses a full active segment at once rather than each sample on arrival, amortizing compression and enabling column-specific choices. It mentions converting floating-point values to fixed-point integers using user-chosen significant digits as a practical lossy compression option that users may find easier to reason about than abstract error bounds.

Blocks are flushed when full. Each writer maintains a private file, calls `fsync` periodically, and rolls to a new file after a configurable size. The defaults described include flushing after ten blocks or five seconds, whichever comes first.

## Query / Index Model

Mach exposes a low-level range read: `get_range(min, max)` over a snapshot for one source. The paper also says higher-level queries can use label and time indexes built during ingestion to locate relevant sources and ranges, but the API discussed in detail is source/time range retrieval rather than a full query language.

Snapshotting is designed to avoid long read/write blocking. The reader briefly takes a source snapshot lock, copies pointers to the active segment, active block, and the head of the persistent block list, and reads atomic counters. After that short critical section, it can identify relevant blocks and scan them. Since blocks for a source are written sequentially by time, reads become sequential I/O plus decompression. The linked-list block index favors freshness-biased queries because scanning from the recent head is cheap.

## Compaction / Materialization / Evolution

Mach's main materialization path is segment-to-block conversion: in-memory uncompressed columns become compressed segments, then page-sized blocks, then persistent file blocks. It does not present an LSM-style compaction pipeline. Old or infrequently queried segments may be migrated to OLAP systems such as ClickHouse for analysis or to remote archival storage such as S3 Glacier.

The conclusion frames Mach as a possible unifying storage engine for metrics, logs, events, and traces, but acknowledges that logs may require search over compressed data and trace support may require new models such as graph-oriented structures.

## Relevance To Event-Native Analytics And Observability

Mach is useful as a design reference for an append-heavy hot store with high-cardinality source churn. The strongest transferable ideas are:

- Optimize around freshness-biased reads rather than treating old and recent time ranges symmetrically.
- Partition write ownership so ingestion threads avoid coordination.
- Make multivariate values first-class so related measurements from one event/source can be stored together.
- Use cheap snapshots to let dashboard/alert reads coexist with continuous ingestion.

For event-native analytics, Mach is narrower than a wide-event store, but its storage path resembles a hot ingest layer that could later feed columnar analytical storage.

## Tradeoffs And Limitations

Mach is a prototype storage engine, not a complete observability platform. It does not provide the distributed query plane, global routing, retention policy management, alert evaluation, or user-facing query language found in systems like Monarch or Honeycomb.

The fast path assumes mostly in-order writes per source. Out-of-order data is supported through a side buffer, but it is not the primary optimization case. The linked-list block index is intentionally simple and snapshot-friendly, but the authors note that alternative structures may be worth investigating. Column chunks grouped together help multivariate reads, but can be less optimal when queries repeatedly touch only a narrow subset of columns.

## Notable Details

- Slack workload cited by the paper: 4 billion unique sources per day, 12 million samples per second, and up to 12 TB compressed data per day.
- 95% of Slack monitoring queries were for the past hour, and almost 98% were for data less than four hours old.
- The paper reports nearly 10x higher write throughput and up to 3x higher read throughput versus selected alternatives in preliminary experiments.
- Mach deliberately compares itself to lower-level storage engines, not only to complete TSDB products.
