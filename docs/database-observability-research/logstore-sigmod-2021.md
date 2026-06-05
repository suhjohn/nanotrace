# LogStore: A Cloud-Native And Multi-Tenant Log Database

- Source: https://users.cs.utah.edu/~lifeifei/papers/logstore-sigmod21.pdf
- Type: paper
- Year: 2021
- Authors/Org: Wei Cao, Xiaojie Feng, Boyuan Liang, Tianyu Zhang, Yusong Gao, Yunyang Zhang, Feifei Li / Zhejiang University and Alibaba Group

## Problem

LogStore addresses cloud-scale log storage for Alibaba Cloud. The workload combines:

- tens of millions of log records per second;
- roughly 100 GB/s ingest in the DBaaS audit-log example;
- petabyte-scale historical logs and a production capacity around 10 PB;
- more than 100,000 tenants;
- highly skewed tenant sizes, with some tenants reaching 100 TB;
- interactive retrieval over massive data, often in hundreds of milliseconds or seconds;
- full-text search plus lightweight analytical queries.

The paper argues that existing systems each miss part of the requirement set. Elasticsearch has strong search but builds indexes during ingest and cannot meet the write-throughput target. Shared-nothing systems have expensive data migration and weaker elasticity. Cloud warehouses separate storage and compute but are bulk-load oriented, reserve compute per tenant, and lack efficient full-text log retrieval.

## System / Architecture

LogStore combines shared-nothing and shared-data ideas:

- local workers handle low-latency writes, replication, WAL, and real-time storage;
- background builders convert local row-store data into read-optimized columnar LogBlocks;
- LogBlocks are uploaded to Alibaba Cloud OSS object storage;
- a distributed query layer accepts SQL requests, plans DAGs, routes subqueries to shards, and merges results;
- a controller manages cluster monitoring, metadata, schemas, task scheduling, checkpointing, and expired-data cleanup;
- global traffic control adjusts routing rules to balance tenant traffic across shards and workers.

The deployed architecture includes controllers, brokers, workers, Raft groups, local SSD, CacheFS, in-memory cache, and OSS.

## Storage Model

LogStore uses a row-column hybrid storage architecture:

- Fresh writes go into a local write-optimized row store, organized mainly by timestamp to reduce random I/O and improve space efficiency.
- Archived data is converted into tenant-isolated columnar LogBlocks on OSS.
- Each tenant has an OSS directory containing chronological LogBlocks.
- Large tenants can be split into multiple LogBlocks.
- Metadata records each tenant's LogBlock path, size, and timestamp range.
- Expiration can delete tenant-specific LogBlocks without affecting other tenants.

LogBlock is the basic object-store storage unit. It is:

- self-contained, with schema and column metadata;
- compressed, with ZSTD as default for higher compression ratio;
- column-oriented, so queries avoid irrelevant columns;
- full-column indexed and skippable.

A LogBlock contains header, column metadata, indexes, column block headers, and compressed column blocks with bitsets. String columns use inverted indexes; numerical columns use BKD-tree indexes. Each column and column block also stores small materialized aggregates such as min/max values.

## Ingest / Write Path

The write path has two phases.

Local write phase:

1. Brokers route writes according to controller-pushed tenant routing rules.
2. Workers receive shard traffic.
3. Workers generate WAL, synchronize replicas with Raft, and write the local row store.
4. Production uses three replicas: two full row-store replicas and one WAL-only replica, trading storage cost against availability.

Remote archive phase:

1. Data builder asynchronously converts row-store data into column-oriented LogBlocks.
2. It uploads immutable LogBlocks to OSS.
3. Metadata is updated with path, size, timestamp range, and tenant ownership.

The object-store write is off the foreground critical path, so clients can receive low-latency write acknowledgments without waiting for OSS upload.

## Query / Index Model

The query layer supports SQL over tenant/time ranges and field predicates. Brokers parse, optimize, create DAG plans, dispatch subqueries to shards, and merge responses.

LogStore uses multi-level data skipping:

- LogBlock map pruning by tenant ID and timestamp range.
- Column-level and column-block-level pruning using min/max statistics.
- Full-column indexes to collect row IDs for matching predicates.
- Sequential scan only for remaining blocks that cannot be pruned or indexed.
- Final log data load by merged matching row IDs.

It also uses object-store latency hiding:

- LogBlocks are packaged as tar files to avoid object explosion, while the manifest allows seeking to internal metadata, indexes, and data files.
- Memory block cache stores loaded OSS file blocks.
- SSD block cache receives spillover from memory cache.
- Object memory cache reduces JVM allocation and garbage-collection pressure.
- Parallel prefetch divides files into data blocks, merges duplicate read requests, and loads blocks concurrently.

## Compaction / Materialization / Evolution

The paper does not describe LSM-style compaction in the same depth as Husky. The main storage evolution is the two-phase conversion from local row-store data to immutable, read-optimized LogBlocks on OSS.

Background maintenance includes:

- checkpointing;
- remote archive building;
- expired-data cleanup;
- metadata updates for new LogBlocks;
- data deletion by tenant/time retention.

Packaging LogBlock internal files into one large tar object is an object-store materialization choice: it avoids listing and managing many small files while preserving seekable subfiles for query reads.

## Relevance To Event-Native Analytics And Observability

LogStore is directly relevant to event-native observability storage:

- It treats logs as high-volume, tenant-scoped, timestamped event records.
- It separates fresh write optimization from historical read optimization.
- It physically isolates tenants in archived object-store layout without giving each tenant a dedicated cluster.
- It acknowledges that cloud object storage is attractive for cost but requires indexes, skipping, cache, prefetch, and file packaging to meet interactive latency.
- It combines full-text search and analytical retrieval in one log database.
- It treats traffic skew and burst control as first-order database design problems, not just deployment concerns.

For observability platforms, the row-to-column pipeline is a useful pattern: accept events quickly in a durable local form, then build a compressed indexed serving layout asynchronously.

## Tradeoffs And Limitations

- Query latency on OSS remains slower than local storage; the paper reports parallel prefetch reducing but not eliminating the gap.
- Indexing every column adds storage overhead, though the authors consider this acceptable on low-cost object storage.
- ZSTD improves storage and network efficiency but costs more CPU.
- The row-store mixes tenants for write efficiency, so full tenant physical isolation only appears after archive conversion.
- The max-flow traffic controller adds control-plane complexity and requires accurate runtime metrics.
- Backpressure protects availability by rejecting or slowing writes, which means overload can surface to clients.
- The paper's future work calls out vectorized execution and JIT compilation, implying query execution itself was not yet as optimized as possible.

## Notable Details

- OSS durability is cited as twelve nines and availability as 99.995 percent.
- The traffic controller models tenants, shards, and workers as a single-source/single-sink flow network and uses Dinic's max-flow algorithm.
- Routing rules assign weighted tenant traffic shares to shards, for example splitting one tenant across several shards by percentage.
- The monitor collects tenant traffic, shard load, and worker load every 300 seconds by default.
- Backpressure monitors queue count and byte size, including Raft sync and apply queues.
- In experiments, max-flow control reduced shard access standard deviation by 2.8x and worker access standard deviation by 5x under skew.
- Data skipping improved average query latency by 1.7x and by 2.6x for the largest tenant.
- Without parallel prefetch, local storage was 18.5x faster than OSS; with parallel prefetch, the gap dropped to 6x.
- With all optimizations enabled, 99 percent of queries returned within 2 seconds, 90 percent within 1 second, and over 75 percent within 100 ms.
