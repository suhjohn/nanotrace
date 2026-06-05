# Progressive Partitioning for Parallelized Query Execution in Google's Napa

- Source: https://www.vldb.org/pvldb/vol16/p3475-sankaranarayanan.pdf and https://research.google/pubs/progressive-partitioning-for-parallelized-query-execution-in-googles-napa/
- Type: paper
- Year: 2023
- Authors/Org: Junichi Tatemura, Tao Zou, Jagan Sankaranarayanan, Yanlai Huang, Jim Chen, Yupu Zhang, Kevin Lai, Hao Zhang, Gokul Nath Babu Manoharan, Goetz Graefe, Divyakant Agrawal, Brad Adelberg, Shilpa Kolhar, Indrajit Roy; Google

## Problem

Napa is Google's critical data warehouse for continuously updated petabyte-scale tables and materialized views. It ingests massive streams of updates while serving billions of queries per day with sub-second latency. The 2023 paper focuses on a specific production bottleneck: many-key lookup queries over highly skewed LSM-backed tables and indexes.

Write-time partitioning cannot meet the workload because query selectivity, latency budgets, and available parallelism vary per query. Fixed fine partitions create high metadata and planning overhead; coarse partitions underutilize workers; hash partitioning handles some skew poorly. Napa needs query-specific partitioning that is even enough, fast enough, and progressive enough to stop when further refinement is not worth the latency.

## System / Architecture

Napa has an ingestion framework, compaction and materialized-view maintenance, external querying, controllers, and query serving over LSM-organized tables. It uses F1 Query for general distributed SQL execution, while Napa's query server decides scan parallelism and partition ranges for Napa table scans.

The relevant execution unit is the scan worker. A scan worker processes a partition of table data locally, applying selection, projection, and partial aggregation before downstream distributed operations such as aggregation, sort, or join. Query planning time includes partitioning time, so better partitioning is not free: the algorithm must improve parallel scan balance without spending too long reading index metadata.

## Storage Model

Napa tables and materialized views are stored as LSM trees. Each immutable file/run is called a delta and contains updates over a timestamp range. Each delta is sorted and indexed by a hierarchical B-tree. Deltas overlap in key space even when they cover different ingestion-time windows.

For snapshot reads at time T, Napa selects the smallest set of deltas that contains all updates up to T without duplicates. If a table has a primary key, updates for the same key across deltas must be reconciled at read time. This makes partitioning harder: one logical key range may have work spread unevenly across many deltas, and a worker must process all relevant delta contributions for its assigned key range.

The B-trees are enhanced with size statistics: nodes store row or byte counts for key ranges. These statistics are the basis for query-specific partitioning.

## Ingest / Write Path

Napa uses high-throughput streaming ingestion into an LSM structure. New data is committed as base-level deltas. Compaction uses a tiered policy, merging multiple deltas into larger deltas at higher levels. Deltas are immutable files in a distributed file system, and metadata lives in Spanner.

The paper is not primarily an ingest paper, but ingestion is central to the partitioning problem. Continuous writes create many deltas with overlapping key ranges, and compaction changes the delta set over time. Any query partitioning method must therefore work over multiple B-trees of different sizes and levels rather than one static sorted file.

## Query / Index Model

Napa queries include large scans, range scans, and many-key lookups. The paper's representative query filters on prefix keys such as `(K1, K2)` and groups by those keys. Scan workers seek sorted deltas using prefix key ranges.

Progressive partitioning starts from virtual root entries for each delta, combines matching B-tree entries across all deltas into an approximate histogram, chooses split points near target cumulative sizes, estimates error bounds, and drills down only entries that contribute to unacceptable error. It stops when partitions satisfy the error margin ratio or no further refinement is possible.

This is "good enough" partitioning. It avoids leaf-level index reads unless needed, but can descend toward leaf precision for skewed areas. The same B-tree supports both lookup execution and partition planning.

## Compaction / Materialization / Evolution

Napa's storage evolution is LSM compaction and view maintenance. Deltas represent updates for time windows; compaction merges lower-level deltas into higher-level deltas to maintain read efficiency and storage cost. Materialized views are part of Napa's maintained data model.

The partitioning algorithm must evolve with that structure. Since each delta has its own B-tree, partitioning combines information from many B-trees. The algorithm is robust to deltas of uneven sizes because it drills down entries selectively and uses per-entry levels and size bounds.

## Relevance To Event-Native Analytics And Observability

Napa is relevant for observability systems that combine real-time ingest, high-cardinality keys, materialized views, and strict dashboard SLOs. Trace or event stores often face the same problem: one tenant, service, route, customer, or time bucket can dominate a query, making static partitions uneven.

The paper's key observability lesson is that parallelism should be query-specific. A query over a small incident window may need many tiny partitions in one skewed key range and no work elsewhere; a broad report may need coarse partitions to avoid planning overhead. Embedding size statistics in lookup indexes lets the serving layer make that decision without building separate planning metadata.

## Tradeoffs And Limitations

The algorithm is approximate by design. It balances partition planning time against scan evenness, so it may return uneven partitions when further refinement is too expensive or impossible. The right error margin is workload-dependent; the paper notes production uses different theta values for latency-sensitive lookups and large batch processing.

It depends on sorted key structures and prefix-key access. Workloads without strong key ordering, or queries dominated by non-prefix predicates, would need different metadata. It also assumes index metadata is accessible through a distributed cache; cold cache misses can make deep drill-down expensive.

The paper focuses on scan partitioning, not full SQL optimization. Joins, downstream aggregation, and broader Napa design are referenced but not deeply developed here.

## Notable Details

- The Google page summarizes Napa as using LSM for real-time ingestion and serving billions of queries per day with sub-second latency.
- Production tables can be multi-petabyte, and key skew can be extreme enough that a single key range spans terabytes.
- Experiments show fixed-size partitioning loses effective worker parallelism; progressive partitioning better matches requested parallelism.
- The production replay section reports progressive partitioning with low partitioning-time ratios while fixed-level or fixed-size baselines either over-read index metadata or under-parallelize.
- The same B-tree metadata serves two purposes: indexed lookup and query partition planning.
