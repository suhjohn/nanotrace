# Uber Pinot Operational Analytics

- Source: https://www.uber.com/blog/rebuilding-ubers-apache-pinot-query-architecture/ ; https://www.uber.com/us/en/blog/blazing-fast-olap-on-ubers-inventory-and-catalog-data-with-apache-pinot/
- Type: post
- Year: 2025
- Authors/Org: Uber Engineering; Christina Li, Ankit Sultana, Shreyaa Sharma, Shaurya Chaturvedi, Tarun Mavani, Suraj Modi

## Problem

Uber uses Apache Pinot for real-time analytics at massive scale. One post focuses on the query architecture: Uber historically served nearly all Pinot queries through Neutrino, an internal Presto fork, but that layered architecture produced confusing semantics, partial-result risks, and weak tenant isolation.

The second post focuses on inventory and catalog analytics for Uber Eats. Uber's INCA catalog stores products, items, and catalogs with high-volume cascading changes. The source of truth, Docstore, is good for persistence and point lookup but not interactive search, filtering, or aggregation. Hive is too slow for operational catalog management.

## System / Architecture

For the query layer, Uber is moving from Neutrino to Cellar, a lightweight passthrough proxy over Pinot brokers. Users can query with PinotSQL or M3QL on Pinot. Cellar does not rewrite queries, so users deal with Pinot semantics directly. It supports client libraries, monitoring, timeout propagation, warnings for partial results, and direct connection mode where clients bypass the proxy and hit Pinot brokers for stronger resource isolation.

For catalog analytics, Uber built INCA indexing on Pinot. An `inca-indexer` service reacts to catalog changes, enriches entities with current attributes, flattens them for indexing, emits Kafka events, and feeds one Pinot table per entity type such as items, products, and customizations.

## Storage Model

Pinot stores real-time OLAP tables with indexes and upsert support. INCA uses Pinot tables for entity-specific flattened views of catalog objects. Product is the "what" being sold, Item is how the product is offered in a specific store, and Catalog groups merchant products and items under rules and configuration.

For large INCA tables, Uber enables inverted indexes on most columns. Free-form text columns use text indexes rather than dictionary encoding and inverted indexes. Bloom filters on identifiers such as `item_id` and `catalog_id` help prune segments for exact lookups.

## Ingest / Write Path

Whenever a product or item changes in Docstore, an event is generated. The indexing service enriches it with latest attributes, simplifies it into a flat structure, and emits it to Kafka. Pinot ingests from Kafka in real time and applies upserts so queries return the freshest known view.

Uber also runs scheduled backfills every few days to reload full datasets. Backfills fill gaps from dropped events, retention limits, or newly added fields, and they are rate-limited to protect source systems. The system targets freshness within 5-10 minutes.

## Query / Index Model

Uber's older Neutrino architecture pushed down a maximal sub-plan to Pinot and executed the rest in Presto. It added default limits to prevent dangerous scans, but that changed query semantics and could return partial results. Neutrino also became a shared query proxy that weakened Pinot tenant isolation.

The newer architecture chooses between Pinot's single-stage engine, Multi-Stage Engine Lite Mode, and full Multi-Stage Engine. Lite Mode adds a configurable max leaf-stage record limit visible in the explain plan and runs leaf stages on Pinot servers while other operators run in a single broker thread. Full MSE access is gated through Uber's dynamic config store because distributed joins can easily overwhelm OLAP clusters with hundreds of billions of records.

For INCA, query speed relies on Pinot indexes, text search, bloom-filter pruning, upserts, and segment compaction. Queries typically return in 1-3 seconds across billions of rows.

## Compaction / Materialization / Evolution

INCA's upsert workload created many small segments under compaction-only maintenance. That forced short retention and frequent full reingestion, with backfill traffic exceeding 100,000 messages per second.

Uber built and contributed the Small Segment Merger minion task for Pinot. SSM combines compaction, which removes invalidated upsert records, with merge, which coalesces small segments into larger segments. In the reported results, SSM reduced segment count by up to 70%, table size by 40%, p99 query latency by 75%, and p50 query latency by 55%.

Evolution happens through adding fields to flattened index events, scheduled backfills, and Pinot table/index tuning. Uber is also migrating INCA queries from Neutrino to Cellar using Multi-Stage Engine Lite Mode.

## Relevance To Event-Native Analytics And Observability

Uber's Pinot work is directly relevant to operational analytics and observability. The query architecture post says Pinot powers user-facing analytics, log search, tracing, segmentation, and other internal platforms. Cellar's M3QL-on-Pinot path is explicitly an observability query surface.

The INCA post shows how event-native indexing works when the source of truth is not analytically queryable: emit entity changes, enrich and flatten them, ingest to a real-time OLAP store, use upserts for current state, and run backfills for correctness.

## Tradeoffs And Limitations

Pinot MSE can express richer SQL, but unrestricted distributed joins are dangerous at Uber's scale. Lite Mode intentionally limits leaf-stage records, which improves safety but can truncate data; Uber is working on better warnings.

Upserts require careful compaction and retention management. Without SSM, small segments and invalidated records created high storage and latency costs. Backfills are necessary for consistency but can pressure both source systems and Pinot.

Text search has language-specific pitfalls. Uber found Lucene Standard Analyzer tokenized much non-Latin text at each character, hurting wildcard queries, so query-time logic had to treat Latin and non-Latin text differently.

## Notable Details

- Uber reports Pinot tables as large as hundreds of terabytes with hundreds of billions of records, with more than 90% of use cases expecting sub-second latency and 10-50 QPS or more.
- Cellar traffic was nearly 20% of Neutrino QPS at the time of the query-architecture post and powered major use cases including segmentation and tracing.
- INCA targets over 10 billion entities and hundreds of thousands of updates per second.
- UUID primary-key bin packing converts UUID strings to 16-byte representations, reducing old-gen memory usage by about 35% for high-volume upserts.
- Moving from JRE 11 to JRE 17 produced nearly 10x improvement in peak latencies for each reported quantile in a high-primary-key upsert workload.
