# GitLab And Lago ClickHouse Business Analytics

- Source: https://clickhouse.com/blog/how-gitlab-uses-clickhouse-to-scale-analytical-workloads ; https://clickhouse.com/blog/lago-using-clickhouse-to-scale-an-events-engine
- Type: post
- Year: 2024, 2025
- Authors/Org: ClickHouse; GitLab; Lago; Mathew Pregasen

## Problem

Both GitLab and Lago outgrew Postgres for user-facing analytical workloads while keeping Postgres for transactional state.

GitLab needed sub-second product analytics for more than 50 million registered users across GitLab.com, GitLab Dedicated, and self-managed deployments. Existing Postgres-backed analytics often approached GitLab's 15-second internal performance threshold, and some queries over 100 million rows took 30-40 seconds.

Lago, an open-source usage-based billing platform, needed to ingest millions of billable events per minute and run aggregations without locking up the rest of the application.

## System / Architecture

GitLab adopted ClickHouse as the default OLAP engine for analytics, with Postgres remaining the OLTP database. The architecture uses a hybrid data access layer that routes queries to either Postgres or ClickHouse depending on the workload or data range. ClickHouse Cloud supports SaaS workloads, while OSS ClickHouse remains an option for self-managed and air-gapped environments.

Lago uses a hybrid stack: Postgres as the primary transactional database for Rails models, invoices, fees, coupons, subscriptions, and other business objects; ClickHouse only for streamed billable events and analytical aggregations.

## Storage Model

GitLab uses ClickHouse for product analytics, Contribution Analytics, GitLab Duo and SDLC trends, DORA-style metrics, AI adoption metrics, logs, events, and other analytical data. The post emphasizes ClickHouse as a columnar, distributed, single-binary analytical engine that can run locally and scale in production.

Lago's ClickHouse instance has a narrow model: `raw_events_queue`, `raw_events`, and one materialized view, `events_raw_mv`. `raw_events_queue` uses the Kafka table engine; the materialized view maps event metadata from JSON into a string array and writes into `raw_events`, a MergeTree table. The primary key is a tuple of organization, external subscription, code, and timestamp.

## Ingest / Write Path

GitLab uses TSV over HTTP for ingestion and has an in-house CDC framework called Siphon, intended to stream more than 100 GB per hour of operational analytics into ClickHouse. The HTTP endpoint makes onboarding straightforward for engineers.

Lago ingests raw billable events through Kafka into ClickHouse. The Kafka-engine queue table receives rows; a materialized view triggers on insert and writes transformed rows into the MergeTree analytical table. This shifts work to ingest time and gives application code a compact table for metric aggregation.

## Query / Index Model

GitLab uses ClickHouse for low-latency customer-facing analytics. Queries over 100 million rows that took 30-40 seconds in Postgres now return in under a second. A deeply hierarchical query over organizations, groups, subgroups, projects, and shared business objects reportedly dropped to 0.24 seconds.

Materialized views are an important GitLab optimization tool, especially for aggregating large volumes. The post says dictionaries and projections are not heavily used yet but are on the roadmap.

Lago's query path centers on billable metric aggregation over `raw_events`. ClickHouse's columnar storage, materialized views, specialized engines, and vectorized execution are the key performance reasons. Lago reports 6.6 seconds to 48 ms for weighted sum aggregation and 6.5 seconds to 350 ms for count-and-sum aggregation versus Postgres.

## Compaction / Materialization / Evolution

GitLab originally built its own ClickHouse operator with object storage and horizontal scaling, then learned the operational burden was high. ClickHouse Cloud's separation of storage and compute through SharedMergeTree, automatic scaling, multi-AZ support, backups, and stable pod rotation reduced operational load.

GitLab still notes operational maturity issues around mutations. Deletes and updates run in the background across parts and blocks; checking system tables and logs works, but the post calls for a clearer active-mutations dashboard.

Lago uses ClickHouse materialized views as insert-time triggers. The materialized view transforms incoming Kafka events and writes the denormalized event table used by application aggregation. This makes materialization part of the event ingestion path rather than a scheduled refresh.

## Relevance To Event-Native Analytics And Observability

GitLab and Lago are examples of business analytics becoming event-native. Product usage, AI adoption, SDLC metrics, and billing are all event streams where transactional databases remain necessary but are no longer sufficient for analytical latency and scale.

For observability systems, the lesson is the OLTP/OLAP split: keep authoritative state and transactions in Postgres, stream high-volume immutable or mostly immutable events to ClickHouse, and expose a routing layer so features can migrate gradually.

## Tradeoffs And Limitations

ClickHouse speed does not remove modeling and operations work. GitLab warns that self-managing ClickHouse was a large operational burden unless strict environment control is required. Mutations require careful monitoring, and materialized views need internal guidelines.

Lago's design intentionally keeps business-critical transactional data out of ClickHouse. That reduces correctness risk, but it means queries requiring rich business-object joins may still need Postgres or pre-enrichment.

Both cases depend on accepting OLAP semantics for analytical workloads. ClickHouse is excellent for append-heavy event aggregation, but not a replacement for transactional consistency.

## Notable Details

- GitLab benchmarked alternatives and found ClickHouse loaded metrics orders of magnitude faster, used almost 10x less disk space, and had much better p95 latency.
- GitLab chose ClickHouse partly because the single binary aligned with the requirement to run in SaaS, self-managed, and air-gapped deployments.
- GitLab's internal standard is that analytics slower than 15 seconds does not ship.
- Lago reports query speedups of at least 18x and up to 137x after moving event aggregations from Postgres to ClickHouse.
- Both stories converge on the same pattern: Postgres for OLTP, ClickHouse for event-scale OLAP.
