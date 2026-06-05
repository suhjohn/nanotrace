# OpenTelemetry Semantic Conventions And Specification

- Source: https://opentelemetry.io/docs/specs/semconv/ ; https://opentelemetry.io/docs/specs/otel/ ; https://opentelemetry.io/docs/specs/otel/metrics/data-model/ ; https://opentelemetry.io/docs/specs/otel/logs/data-model/
- Type: docs / spec
- Year: 2026 current docs snapshot; SemConv 1.41.1 and OTel Specification 1.57.0
- Authors/Org: OpenTelemetry Authors / Cloud Native Computing Foundation ecosystem

## Problem

OpenTelemetry addresses interoperability across telemetry producers, processors, and backends. Without common names, types, resources, signal models, and semantic conventions, telemetry from different languages and libraries is hard to correlate and expensive for backend vendors to interpret consistently.

Semantic conventions define common attributes and signal-specific meanings. The specification defines API, SDK, data model, resource, trace, metrics, logs, protocol, and compatibility behavior. The docs explicitly frame semantic conventions as a way to standardize naming across codebases, libraries, and platforms so data is easier to correlate and consume.

## System / Architecture

OpenTelemetry is not a database. It is a specification and implementation ecosystem for producing, processing, and exporting telemetry. Its architecture separates:

- APIs used by application code and instrumentation.
- SDKs that implement collection, processors, sampling, aggregation, and exporting.
- Resources that describe the observed entity.
- Instrumentation scopes that identify the library or component producing telemetry.
- OTLP as a transport data model.
- Semantic conventions layered on top of traces, metrics, logs, events, profiles, and resources.

Resources are immutable attribute sets associated with telemetry from a provider, such as `TracerProvider`, `MeterProvider`, or `LoggerProvider`. That makes entity identity common across signals.

## Storage Model

The spec defines portable data models, not backend physical storage. Common values use `AnyValue`: primitives, homogeneous arrays, byte arrays, nested arrays, maps, and empty values. Attributes are key-value pairs over this common value model.

Metrics have a detailed model. OTel frames metrics as a path from instrumentation events to metric streams to timeseries. A timeseries is identified by metric name, attributes, point value type, and unit. OTLP metric streams are grouped by resource attributes, instrumentation scope, name, data point type, unit, description, and intrinsic properties such as aggregation temporality and monotonicity. Data point kinds include Sum, Gauge, Histogram, ExponentialHistogram, and legacy Summary.

Logs use a stable log record data model designed to map existing log formats into and out of OTel. Log records include timestamps, observed timestamp, trace context fields, severity fields, body, resource, instrumentation scope, attributes, and event name.

Events in semantic conventions are modeled as a specialized kind of `LogRecord`: a named occurrence at an instant in time with attributes and optional severity. This is important because events are not a separate physical signal model in the same way metrics and traces are.

## Ingest / Write Path

The spec describes producer and SDK behavior rather than a server ingest path. Instrumentation records spans, metric observations, log records, and events. SDKs process and export them. The metrics model explicitly allows transformations inside an SDK or collector before export.

Metrics support collection-path transformations:

- Temporal reaggregation for longer intervals.
- Spatial reaggregation to remove or combine attributes.
- Delta-to-cumulative conversion, which lets clients avoid holding high-cardinality cumulative state.

The logs data model is designed for efficient serialization/deserialization and space requirements, but does not dictate a storage layout.

## Query / Index Model

OpenTelemetry does not define a query language or backend index model. Its query relevance is semantic: it standardizes fields such as span names, span kind, metric instruments, units, attribute names, attribute types, meanings, and valid values. That gives downstream query engines a stable vocabulary for filtering, grouping, joining, and correlating telemetry.

Trace semantic conventions focus on annotating spans for operations and protocols such as HTTP, database calls, messaging, RPC, and exceptions. Metrics semantic conventions define general naming/unit guidance and domain-specific metric conventions. Logs and events conventions define generic log identification attributes and event naming guidance.

## Compaction / Materialization / Evolution

OpenTelemetry's materialization concepts are mostly logical. Metric views can transform one instrument's event stream into one or more metric streams by selecting aggregation interval and attributes. Reaggregation can reduce temporal resolution or dimensionality. Delta and cumulative temporality let systems choose where state is maintained.

Evolution is handled through versioned specs, stability statuses, schema URLs, semantic convention stability, and migration guidance. The current pages mix stable, development, and mixed statuses, so implementers must treat each signal area separately.

## Relevance To Event-Native Analytics And Observability

OpenTelemetry is highly relevant because it defines the incoming shape and semantic contract for modern observability data. For event-native analytics, the most useful lessons are:

- Treat resources, instrumentation scope, trace context, and attributes as first-class dimensions.
- Preserve signal-specific semantics while allowing common attribute-based correlation.
- Model events as named records with structured attributes, not as opaque strings.
- Keep metric transformations explicit so cost reductions do not silently change semantics.
- Expect semantic convention churn and carry schema/version metadata.

OTel also makes clear that a backend needs to support both low-cardinality standardized fields and arbitrary high-cardinality user/application attributes.

## Tradeoffs And Limitations

OTel standardizes production and interchange, not storage or query. A backend still must decide how to index, sample, aggregate, retain, compact, and query the data. Some conventions are stable while others are development or mixed, so blindly treating the entire SemConv surface as stable is risky.

The attribute model is flexible, including maps and arrays, but the common-concepts page warns that arrays and maps may carry higher performance overhead than primitives. High-cardinality attributes are not prohibited by the spec, but cost control is left to SDK configuration, collectors, and backends.

## Notable Details

- The SemConv page read as OpenTelemetry semantic conventions 1.41.1.
- The OTel spec page read as OpenTelemetry Specification 1.57.0.
- General events conventions describe an event as a named instant and require event definitions to document event name and attributes.
- The metrics model explicitly supports spatial and temporal reaggregation and delta-to-cumulative conversion.
- Resource association is immutable at provider creation time for traces, and analogous provider association exists for metrics and logs.
