# Datadog Husky Storage Compaction

- Source: https://www.datadoghq.com/blog/engineering/husky-storage-compaction/
- Type: post
- Year: 2025
- Authors/Org: Damien Profeta, George Talbot / Datadog

## Problem

Husky Writers must make recent data queryable quickly, so they flush small fragments to object storage. Small fragments are bad for queries because they increase object-store GETs, metadata scans, scheduling overhead, and per-fragment processing cost.

The compaction problem is a balance:

- fragments that are too small cause high object-store and query overhead;
- fragments that are too large reduce parallelism for broad queries;
- compaction work itself costs CPU and object-store requests;
- layout should improve pruning and compression without over-spending compaction resources.

Datadog's workload is append-heavy observability data: trillions of events per day, very few updates, fresh-biased queries, and some large analytical scans over old data.

## System / Architecture

Husky stores ingested events as object-store fragments plus metadata. A query first uses metadata to discover relevant fragments, then dispatches fragment scans to workers and merges their results.

The storage architecture therefore tries to minimize:

- the number of fragments fetched from object storage;
- the number of events scanned inside those fragments.

Compactors are distributed workers that merge fragments inside a table/time bucket. FoundationDB-backed metadata provides the atomic swap: old fragments become invisible and compacted fragments become visible in one transaction.

## Storage Model

A fragment is a custom columnar file, conceptually similar to Parquet with one row group structure and many pages, but designed for observability data and streaming compaction.

Important layout features:

- A fragment contains many columns, each preceded by a column header.
- Compaction can discover columns as it streams the file from one object-store GET.
- A fragment can contain hundreds of thousands or millions of columns because tenant event shapes are highly heterogeneous.
- Column data is split into fixed-row-count row groups that are uniform across columns in a fragment.
- Only one row group per input fragment needs to be in memory during compaction.
- Fragments include a skip list of column offsets so queries can find relevant columns.

The format is optimized for bounded-memory k-way merge over many input fragments.

## Ingest / Write Path

Writers buffer events per tenant, sort them according to the configured sort schema, and flush small fragments to object storage. The buffer delay is capped to keep ingest-to-query latency low, so writer output fragments contain at most a few thousand events.

Each fragment belongs to a logically contiguous table and time window. Fresh data and late-arriving historical data are both just modified buckets that need compaction.

## Query / Index Model

The query cost model has two major terms: fragment count and rows scanned. Compaction helps both.

Husky uses:

- time bucketing, so metadata can restrict a query to overlapping time windows;
- table-level tenant/product organization;
- sort schemas over common tags plus timestamp;
- min/max row keys in fragment headers;
- finite-state automata converted to regular expressions for sort-schema column value pruning.

The regex pruning approach is designed to avoid false negatives. It can produce false positives, but a non-match means the fragment definitely cannot contain that value. This improves on pure min/max lexical pruning for common low-cardinality tags such as service, status, and environment.

## Compaction / Materialization / Evolution

The post describes three compaction layers.

Size-tiered compaction:

- waits until merging is efficient enough, usually when a few dozen fragments can be merged;
- groups fragments into exponentially increasing size classes;
- reduces writer-output thousand-row fragments into roughly million-row fragments;
- reduces object-store GETs and metadata entries.

Streaming k-way merge:

- reads each input fragment with one GET;
- merges rows according to a lexicographic sort schema;
- streams one column and row group at a time;
- outputs one or more sorted, disjoint fragments.

Locality compaction:

- takes size-tiered outputs and organizes them into levels;
- uses fragment min/max row keys to detect overlap;
- compacts overlapping fragments at a level into non-overlapping fragments;
- promotes data through exponentially larger levels;
- makes higher-level fragments lexically narrower, increasing the chance of pruning.

This is a hybrid LSM approach: size-tiered compaction for operational file count, followed by best-effort locality compaction for query layout.

## Relevance To Event-Native Analytics And Observability

The post is a strong example of storage layout as an observability product feature:

- Small files are not a background nuisance; they directly affect user query latency and object-store costs.
- Sorting by common tags makes repeated observability predicates cheaper.
- Compaction is where an append-only event corpus evolves from write-optimized fragments into read-optimized fragments.
- Query telemetry and product knowledge inform sort schemas.
- File layout, row group sizing, and pruning metadata decide whether massive historical event queries are feasible.

For event-native analytics, the key lesson is that durable events can remain granular, but the serving layout must be continuously reorganized around time, tenant/table, and high-value dimensions.

## Tradeoffs And Limitations

- Delaying compaction saves work but means fresh data can temporarily remain in many small fragments.
- More aggressive locality compaction improves pruning but spends more CPU and object-store I/O.
- Sort schemas are product-level approximations; one schema cannot optimize every query shape.
- The regex/automaton pruning deliberately allows false positives, so it reduces but does not eliminate scans.
- Very large or variable-width columns force careful row group sizing and can require restarting output generation with smaller row groups.
- Custom file format improves Datadog's workload but reduces interoperability compared with standard formats.

## Notable Details

- Writers cap buffer time to keep data queryable quickly.
- Final compaction is triggered when the probability of more data arriving in a bucket becomes low.
- The main per-compaction goals are one GET per input fragment, CPU saturation, and fixed memory below what one CPU can support.
- Logs can allow message fields up to 75 KiB, making row group memory control essential.
- Locality compaction initially reduced the query worker pool by 30 percent while using no more CPU than size-tiering alone.
- Compactors process thousands of fragments and dozens of GB of data per second at Datadog scale.
