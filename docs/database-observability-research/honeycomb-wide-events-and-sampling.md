# Honeycomb Wide Events, High Cardinality, Distributed Column Store, And Sampling

- Source: https://docs.honeycomb.io/get-started/basics/observability/concepts/high-cardinality/ ; https://www.honeycomb.io/blog/why-observability-requires-distributed-column-store ; https://docs.honeycomb.io/manage-data-volume/sample/honeycomb-refinery
- Type: docs / post
- Year: 2024 blog update; current docs accessed 2026
- Authors/Org: Alex Vondrak; Honeycomb / Hound Technology, Inc.

## Problem

Honeycomb's docs frame observability as the ability to query high-cardinality and high-dimensionality data freely. High cardinality means a field has many possible values, such as user ids, cart ids, order ids, request ids, or URLs with many query-parameter combinations. High dimensionality means events contain many fields. Traditional metrics systems often store a time series for every attribute combination, so high-cardinality dimensions can cause cost explosion.

Honeycomb's thesis is that debugging unknown problems requires rich context first and aggregation later. Users cannot know in advance which dimension will matter during an incident, so the backend should not require predefined schemas, preselected indexes, or preaggregated metrics as the only fast path.

## System / Architecture

Honeycomb describes its backend as a purpose-built distributed column store. Each dataset is stored as a table. Columns correspond to event fields, and each span or event is written as a row. The columnar layout is paired with distributed computation so aggregates over many rows and a few fields can be split across workers and combined.

The Refinery component is separate from the storage engine. Refinery is a trace-aware tail-based sampling proxy. It inspects whole traces, decides whether to keep or discard the trace, and sends sampled data to Honeycomb. It supports dynamic, rules-based, throughput-based, and deterministic probability sampling.

## Storage Model

The logical data model is event-native. An event is a collection of key-value pairs describing a unit of work. A collection of events is a dataset. Each attribute is a field or dimension. A single event does not need values for every field in the dataset.

The column-store post maps this directly onto storage: dataset equals table, event/span equals row, event field equals column. New fields dynamically expand the table. Because columns are stored independently, adding a new field does not require rewriting all old rows in the same way a row-oriented fixed-schema layout would.

The docs emphasize storing raw, unaggregated data and aggregating at query time. Honeycomb says users can group or filter on any attribute regardless of cardinality.

## Ingest / Write Path

On ingest, each event adds a row to the dataset table. In a column-oriented layout, writing an event appends values to the relevant column files. If an event introduces a new field, Honeycomb can create a new column and treat historical rows as null or absent via sparse metadata rather than rewriting the table.

For sampling, Refinery sits before Honeycomb and decides at trace granularity. Tail-based sampling waits until enough of a trace is available to decide based on its contents. Dynamic sampling chooses rates from key values and their frequencies. Rules can keep known-important traces, such as errors, while applying dynamic sampling to other traffic. Throughput-based sampling targets a span-per-second ceiling. Deterministic probability sampling makes consistent decisions from trace id alone.

Refinery handles OpenTelemetry traces and logs. Logs associated with traces are sampled with the trace; unassociated log events are forwarded directly.

## Query / Index Model

Honeycomb's query model is scan-and-aggregate over columnar segments rather than predeclared indexes. The blog states that there should be no indexes users must choose ahead of time and no reliance on preaggregated data to understand behavior. Queries commonly compute aggregates such as count, average, percentiles, and heatmaps over a handful of fields across many rows.

Time range is a core pruning dimension. The column-store post describes splitting column files into time-ordered segments. Each segment has metadata such as minimum and maximum timestamps. Because every query has a time range, workers can skip segments outside the range.

BubbleUp is described in the high-cardinality docs as a tool that looks across dimensions to identify attributes that stand out. This is a product-level example of why high-dimensional, high-cardinality query support matters: it can isolate a single user's latency even when the cardinality is in the millions.

## Compaction / Materialization / Evolution

The sources do not describe low-level compaction algorithms. They do describe logical evolution: schemas are dynamic because new fields can be added without a migration. The storage model tolerates sparse fields, and datasets can contain many hundreds of dimensions or even thousands of columns.

Sampling is the main volume-management mechanism discussed. Refinery reduces stored volume before ingest while attempting to preserve the most useful traces. This is not compaction after the fact; it is admission control based on trace content and traffic patterns.

## Relevance To Event-Native Analytics And Observability

This is one of the clearest event-native references in the source set. Honeycomb's argument maps directly to storing rich raw events and delaying aggregation until query time. Important design lessons:

- Wide events avoid premature decisions about what will be useful during debugging.
- Columnar storage makes high-dimensional sparse data practical.
- Avoiding user-managed indexes keeps arbitrary dimensions queryable.
- Time-segment metadata can provide coarse pruning without undermining arbitrary field queries.
- Sampling must be trace-aware when decisions require the full request path.

For event analytics systems, this source supports a design that favors raw columnar events with flexible attributes over metrics-first rollups.

## Tradeoffs And Limitations

The blog is an architectural explanation, not a full storage-engine paper. It does not disclose exact file formats, compression methods, distribution protocols, consistency behavior, or query optimizer details.

Column stores are excellent for aggregates over many rows and few fields, but row reconstruction across many columns can be less natural than in row stores. Honeycomb's model also depends on time ranges for efficient pruning. Sampling reduces cost but can discard data; Refinery mitigates that by making decisions over whole traces and supporting rules, but it cannot preserve everything under aggressive volume limits.

Honeycomb docs also note that datasets approaching very high column counts may indicate instrumentation mistakes. Flexibility does not remove the need for hygiene and governance.

## Notable Details

- Honeycomb defines each event as key-value attributes for a unit of work.
- Honeycomb says datasets with many hundreds of dimensions are not unusual.
- The high-cardinality docs explicitly contrast event storage with metrics systems that store time series for every attribute combination.
- The column-store post says each span is written as one row in the dataset table.
- Refinery 3.0 is described as the latest major release in the accessed docs.
