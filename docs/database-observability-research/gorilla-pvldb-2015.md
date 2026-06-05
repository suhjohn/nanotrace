# Gorilla: A Fast, Scalable, In-Memory Time Series Database

- Source: https://www.vldb.org/pvldb/vol8/p1816-teller.pdf
- Type: paper
- Year: 2015
- Authors/Org: Tuomas Pelkonen, Scott Franklin, Justin Teller, Paul Cavallaro, Qi Huang, Justin Meza, Kaushik Veeraraghavan; Facebook, Inc.

## Problem

Gorilla was built because Facebook's HBase-backed Operational Data Store could not support the read latency and query volume needed for real-time monitoring tools. Writes dominated, but read latency mattered for automation, dashboards, and incident diagnosis. The workload also strongly favored recent data: analysis of ODS showed that at least 85% of queries targeted data from the prior 26 hours.

The key design insight is that monitoring users care more about aggregate behavior and recent state transitions than individual point durability. Gorilla therefore optimizes for available writes and fast recent reads, even if small amounts of data can be dropped.

## System / Architecture

Gorilla is an in-memory TSDB that acts as a write-through cache in front of long-term HBase storage. It stores the most recent 26 hours of time-series data at full resolution. HBase remains the durable historical backend.

Gorilla uses a shared-nothing sharded architecture. A time-series string key maps to a shard, and each shard maps to one Gorilla host. Scaling is done by adding hosts and adjusting the shard assignment. Each time-series value is written to two independent Gorilla instances in different regions. Read clients prefer the nearest healthy region and fail over when needed.

Within a region, a Paxos-based ShardManager assigns shards to nodes. When a node fails, its shards are reassigned. Write clients buffer approximately one minute of data during movement, prioritizing newer data when buffers overflow.

## Storage Model

The stored tuple is simple: string key, 64-bit timestamp, and double-precision floating-point value. Unlike systems with typed dimensions or tag maps, Gorilla treats the key as opaque and relies on higher-level tools to parse metadata.

Compression is the core storage technique. Gorilla compresses each time series independently in two-hour blocks. Timestamps use delta-of-delta encoding: if intervals are regular, a zero delta-of-delta is encoded as one bit. Values use XOR compression against the previous floating-point value, exploiting repeated values and stable sign/exponent/mantissa regions. The first value in a block is stored less compactly, then later values encode the XOR payload.

The reported production compression ratio is about 1.37 bytes per point, roughly a 12x reduction compared with 16 raw bytes for timestamp plus value.

## Ingest / Write Path

Incoming points are sharded by string key and appended to the relevant in-memory time-series structure. The active in-memory block is an append-only string. Once a two-hour block is complete, it is closed, copied into slab-allocated memory to reduce fragmentation, and never mutated until it expires.

Gorilla also writes per-shard append-only logs to disk on GlusterFS with 3x replication. These logs are not strict write-ahead logs. Data is buffered up to 64 KB, usually one or two seconds of data, before flushing. This can lose a small amount of data on crash, but improves throughput and write availability.

Every two hours, compressed block data is copied to disk in a more compact complete-block file. A checkpoint file marks the block file as trustworthy. On restart, missing checkpoints cause recovery from logs instead.

## Query / Index Model

The in-memory data structure is optimized for both exact time-series lookup and full scans. A `TSmap` holds a vector of pointers to time series for efficient paged scans and a case-insensitive map from time-series name to pointer for constant-time lookup. A `ShardMap` maps shard ids to `TSmap` objects.

Queries copy compressed blocks that intersect the requested range and return them to clients; decompression happens outside Gorilla. This keeps server-side query work low. Efficient full scans enabled tools such as a correlation engine that computes Pearson correlation between one time series and up to one million candidates.

Gorilla does not provide a rich query language, tag filtering, joins, or schema-aware optimization. It is a very fast recent-data store and scan substrate.

## Compaction / Materialization / Evolution

Gorilla's main compaction is time-series compression into two-hour blocks, plus periodic movement from append-only logs into complete block files. It keeps full-resolution recent data in memory and relies on HBase for older data.

The system also changed downstream materialization. Before Gorilla, time rollups over older ODS data were MapReduce jobs over HBase. After Gorilla, background processes could scan completed Gorilla buckets directly and produce lower-granularity rollup tables, reducing HBase load.

Future work in the paper included an intermediate flash-backed store to hold about two weeks of full-resolution Gorilla-compressed data between the 26-hour memory layer and HBase.

## Relevance To Event-Native Analytics And Observability

Gorilla is a strong reference for hot-window observability storage. It demonstrates that a recent in-memory cache can dramatically change the product surface: lower latency enabled dense visualizations, correlation search, anomaly checking, alerts, and automated remediation.

The relevance to event-native analytics is mostly about hot-path tradeoffs, not data shape. Gorilla sacrifices rich dimensions for tiny points and opaque keys. But its emphasis on recent data, compression tuned to telemetry regularity, and failover that returns partial fresh data are all useful for incident-oriented analytics.

## Tradeoffs And Limitations

Gorilla is not a general-purpose analytical database. It stores a single double value per timestamp and key, with metadata outside the storage model. It cannot natively query arbitrary tags or wide event fields. It also chooses availability over strict durability: buffered writes can be lost, cross-region replicas can diverge, and partial results are accepted.

Its compression is excellent for regular time series with stable values, but less directly applicable to wide events, logs, or traces. Querying a short range may require decoding larger two-hour blocks, a deliberate tradeoff for better compression.

## Notable Details

- Production requirements included 2 billion unique series, 700 million points per minute, 26 hours of data, and more than 40,000 peak queries per second.
- Around 96% of timestamps compressed to a single bit in the sampled workload.
- Roughly 51% of values compressed to a single bit because they matched the previous value.
- Gorilla reduced production query latency by more than 70x compared with the previous on-disk TSDB.
- The paper reports zero alerting-impacting Gorilla events in the prior six months and only one real-time monitoring disruption since launch.
