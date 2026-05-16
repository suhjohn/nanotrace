/*
 * Architecture: all semantic fields live inside the `data` JSON column as
 * type-hinted paths. Since ClickHouse v25.3 (March 2025), typed JSON paths
 * are stored as physical columnar subcolumns. We only promote a path to a
 * top-level MATERIALIZED column when it must participate in ORDER BY, carry
 * a skip index, or use a non-default codec. Everything else stays inside
 * `data` and is queried as `data.<path>` or `getSubcolumn(data, 'path')`.
 */
CREATE DATABASE IF NOT EXISTS observatory;

CREATE TABLE
    IF NOT EXISTS observatory.events (
        /* Client-provided event identity. */
        event_id String CODEC (ZSTD (1)),

        /* Event time. Drives partitioning and ORDER BY. Delta+ZSTD is the
           canonical timestamp codec; adjacent rows differ by small deltas. */
        timestamp DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),

        /* Optional producer/collector observation time; defaults to event time. */
        observed_timestamp DateTime64 (3, 'UTC') DEFAULT timestamp CODEC (Delta (8), ZSTD (1)),

        /* ClickHouse insert/index time. */
        ingested_timestamp DateTime64 (3, 'UTC') DEFAULT now64 (3) CODEC (Delta (8), ZSTD (1)),

        /* Raw object byte range for fetching the accepted event line from object storage. */
        source_file String DEFAULT '' CODEC (ZSTD (1)),
        source_offset UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        source_length UInt32 DEFAULT 0 CODEC (Delta, ZSTD (1)),

        /*
         * Canonical payload. Type hints below pin hot paths to typed
         * subcolumns at insert time. Hints are Nullable because this table
         * stores mixed signal families: a log row should not read duration_ms
         * as 0 just because the path is absent.
         *
         * Dotted names match common OTEL attributes and current fixtures.
         * Use getSubcolumn(data, 'http.method') when a query builder needs to
         * address a dotted path without relying on identifier parsing.
         */
        data JSON (
            /* Routing dimensions. */
            tenant_id LowCardinality (Nullable (String)),
            service LowCardinality (Nullable (String)),
            service.namespace LowCardinality (Nullable (String)),
            service.instance.id Nullable (String),
            service_version LowCardinality (Nullable (String)),
            event_type Nullable (String),
            signal LowCardinality (Nullable (String)),
            environment LowCardinality (Nullable (String)),
            host.name LowCardinality (Nullable (String)),
            host.id Nullable (String),
            /* Span name / metric name / event name. Bounded per tenant
               (routes like `GET /users/:id` repeat heavily). */
            name LowCardinality (Nullable (String)),
            /* Instrumentation scope (OTEL). */
            scope_name LowCardinality (Nullable (String)),
            scope_version LowCardinality (Nullable (String)),

            /* Tracing -- OTEL trace semconv. */
            trace_id Nullable (String),
            span_id Nullable (String),
            parent_span_id Nullable (String),
            trace_state Nullable (String),
            span_kind LowCardinality (Nullable (String)),
            span_status_code LowCardinality (Nullable (String)),
            span_status_message Nullable (String),
            /* Existing producers emit start_time/end_time. The span_* aliases
               are reserved for normalized producers that want clearer names. */
            start_time Nullable (DateTime64 (3, 'UTC')),
            end_time Nullable (DateTime64 (3, 'UTC')),
            span_start_time Nullable (DateTime64 (3, 'UTC')),
            span_end_time Nullable (DateTime64 (3, 'UTC')),
            duration_ms Nullable (Float64),
            is_error Nullable (UInt8),

            /* HTTP semconv -- the most-queried span attributes in practice. */
            http.method LowCardinality (Nullable (String)),
            http.route LowCardinality (Nullable (String)),
            http.status_code Nullable (UInt16),
            http.request.method LowCardinality (Nullable (String)),
            http.response.status_code Nullable (UInt16),
            url.path Nullable (String),
            url.full Nullable (String),
            user_agent.original Nullable (String),

            /* Network semconv. */
            client.ip Nullable (String),
            client.port Nullable (UInt16),
            server.address LowCardinality (Nullable (String)),
            server.port Nullable (UInt16),

            /* Exception semconv -- status_code='ERROR' tells you it failed,
               these tell you why. */
            exception.type LowCardinality (Nullable (String)),
            exception.message Nullable (String),
            exception.stacktrace Nullable (String),

            /* Logs -- OTEL log semconv. severity_number is the int form
               (1-24) used for range filters like `> 17` (= ERROR or worse). */
            severity_text LowCardinality (Nullable (String)),
            severity_number Nullable (UInt8),
            body Nullable (String),
            logger.name LowCardinality (Nullable (String)),
            thread.name LowCardinality (Nullable (String)),

            /* DB/RPC/messaging semconv. Free-form statements stay nullable and
               should be redacted/truncated by producers before ingest. */
            db.system LowCardinality (Nullable (String)),
            db.operation LowCardinality (Nullable (String)),
            db.statement Nullable (String),
            rpc.system LowCardinality (Nullable (String)),
            rpc.service LowCardinality (Nullable (String)),
            rpc.method LowCardinality (Nullable (String)),
            messaging.system LowCardinality (Nullable (String)),
            messaging.destination.name LowCardinality (Nullable (String)),
            messaging.operation.name LowCardinality (Nullable (String)),

            /* Metrics. metric_min/max are standard on histograms and summaries
               and frequently queried directly. */
            metric_name LowCardinality (Nullable (String)),
            metric_type LowCardinality (Nullable (String)),
            metric_value Nullable (Float64),
            metric_unit LowCardinality (Nullable (String)),
            metric.temporality LowCardinality (Nullable (String)),
            metric.is_monotonic Nullable (Bool),
            /* Existing histogram fixtures use count/sum. metric_* aliases are
               accepted for normalized producers. */
            count Nullable (UInt64),
            sum Nullable (Float64),
            metric_count Nullable (UInt64),
            metric_sum Nullable (Float64),
            metric_min Nullable (Float64),
            metric_max Nullable (Float64),

            /* Product analytics: identity. */
            user_id Nullable (String),
            anonymous_id Nullable (String),
            device_id Nullable (String),
            session_id Nullable (String),
            account_id Nullable (String),

            /* Product analytics: page/screen context. page_path is bounded
               (routes); page_url is unique per query string. */
            page_url Nullable (String),
            page_path LowCardinality (Nullable (String)),
            page_title Nullable (String),
            referrer Nullable (String),
            screen_name LowCardinality (Nullable (String)),

            /* Product analytics: attribution. */
            utm_source LowCardinality (Nullable (String)),
            utm_medium LowCardinality (Nullable (String)),
            utm_campaign LowCardinality (Nullable (String)),
            utm_term LowCardinality (Nullable (String)),
            utm_content LowCardinality (Nullable (String)),

            /* Product analytics: device/geo/app context. */
            country LowCardinality (Nullable (String)),
            region LowCardinality (Nullable (String)),
            city LowCardinality (Nullable (String)),
            continent LowCardinality (Nullable (String)),
            location_lat Nullable (Float64),
            location_lng Nullable (Float64),
            device_type LowCardinality (Nullable (String)),
            device_brand LowCardinality (Nullable (String)),
            device_manufacturer LowCardinality (Nullable (String)),
            device_model LowCardinality (Nullable (String)),
            browser LowCardinality (Nullable (String)),
            browser_version LowCardinality (Nullable (String)),
            os LowCardinality (Nullable (String)),
            os_version LowCardinality (Nullable (String)),
            app_version LowCardinality (Nullable (String)),
            locale LowCardinality (Nullable (String)),
            timezone LowCardinality (Nullable (String)),
            user_agent Nullable (String),
            screen_height Nullable (UInt32),
            screen_width Nullable (UInt32),
            viewport_height Nullable (UInt32),
            viewport_width Nullable (UInt32),
            screen_dpi Nullable (UInt32),
            /* Raw IP kept for backfilling geo or fraud review. */
            ip Nullable (String),

            /* Product analytics: revenue events (Amplitude-style). */
            revenue Nullable (Float64),
            currency LowCardinality (Nullable (String)),
            price Nullable (Float64),
            quantity Nullable (Float64),
            product_id Nullable (String),
            revenue_type LowCardinality (Nullable (String)),

            /* Experimentation. */
            experiment_id LowCardinality (Nullable (String)),
            variant LowCardinality (Nullable (String)),
            feature_flag LowCardinality (Nullable (String)),

            max_dynamic_paths = 8192,
            max_dynamic_types = 8
        ),

        /*
         * Promoted to top-level columns *only* because they participate in
         * ORDER BY (or carry a skip index below). All other dimensions are
         * read directly from `data.<path>` -- no MATERIALIZED needed.
         *
         * `ifNull(..., '')` is required here: sort-key columns cannot be
         * Nullable. Inside `data`, hot paths are Nullable so missing values
         * remain distinguishable from empty strings and zeroes.
         */
        tenant_id LowCardinality (String) MATERIALIZED ifNull (data.tenant_id, ''),
        event_type String MATERIALIZED ifNull (data.event_type, '') CODEC (ZSTD (1)),
        trace_id String MATERIALIZED ifNull (data.trace_id, '') CODEC (ZSTD (1)),
        span_id String MATERIALIZED ifNull (data.span_id, '') CODEC (ZSTD (1)),

        /*
         * Closed-set signal classifier. The 'other' fallback bucket prevents
         * arbitrary `event_type` values from being absorbed into the
         * LowCardinality dictionary, which would degrade merge performance.
         */
        signal LowCardinality (String) MATERIALIZED multiIf (
            ifNull (data.signal, '') != '', ifNull (data.signal, ''),
            ifNull (data.event_type, '') IN ('span', 'span_start', 'span_end'), 'trace',
            ifNull (data.event_type, '') = 'metric', 'metric',
            ifNull (data.event_type, '') = 'log', 'log',
            ifNull (data.event_type, '') IN ('analytics', 'track', 'page', 'screen', 'identify', 'group', 'alias'), 'analytics',
            'other'
        ),

        /* The sort key is tenant/time first because product reads, facet
           backfills, retention, and event pages are tenant-scoped time scans.
           Event kind stays as metadata, not a storage-order dimension. */
        INDEX idx_trace_id trace_id TYPE bloom_filter (0.01) GRANULARITY 4,
        INDEX idx_span_id span_id TYPE bloom_filter (0.01) GRANULARITY 4,
        INDEX idx_event_id event_id TYPE bloom_filter (0.01) GRANULARITY 4
    ) ENGINE = MergeTree
PARTITION BY
    toYYYYMMDD (timestamp)
ORDER BY
    (tenant_id, timestamp, trace_id, span_id);

/*
 * V2 analytics model.
 *
 * The UI is event-centric: a group list, an event/time graph, and an event
 * table all come from the same read surface. `events` remains the canonical
 * raw log. These tables are derived serving paths for fast analytics; they are
 * intentionally not trace-specific.
 *
 * Scale assumption used in the table notes below:
 *   1M requests/sec, at least 10 emitted events/request
 *   = 10M events/sec
 *   = 864B events/day
 *   = 25.92T events/30 days
 *
 * The exact production mix will vary, but these numbers keep the design honest:
 * a query shape that scans "only 1% of events" still touches 8.64B rows/day.
 */
/*
 * event_index: narrow serving copy of raw events.
 *
 * Concrete case:
 *   The Logs screen is open on service=canvas-agent for the last 15 minutes.
 *   At 10M events/sec, that 15-minute time window contains 9B raw events. If
 *   the service is 0.5% of traffic, the visible candidate set is still 45M
 *   events. The UI needs to render a timeline, a flamegraph-like event graph,
 *   and a paginated event table. It does not need to parse every arbitrary JSON
 *   field to do that; it mostly needs timestamp, title/name, service, ids,
 *   duration, error, and source pointers.
 *
 * Without this table:
 *   Each refresh reads the canonical `events` table and repeatedly extracts
 *   common JSON paths such as data.service, data.name, data.trace_id,
 *   data.duration_ms, and source offsets. If each event has a 1KB compressed-ish
 *   JSON payload, the full 15-minute window is about 9TB of raw event payload
 *   before pruning. Even with good compression and indexes, reparsing the same
 *   fields for every UI refresh is the wrong hot path.
 *
 * With this table:
 *   Ingestion writes one compact row per event ordered by tenant/time/event_id.
 *   For the 45M service-specific candidates, the query can read narrow columns
 *   and page to the first 100-500 visible rows instead of repeatedly touching
 *   the JSON-heavy canonical log. The source_file/source_offset/source_length
 *   columns still point back to the raw object store bytes when full fidelity is
 *   required.
 */
CREATE TABLE
    IF NOT EXISTS observatory.event_index (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),

        timestamp DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        bucket_time DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        event_id String CODEC (ZSTD (1)),

        event_type String CODEC (ZSTD (1)),
        signal LowCardinality (String),
        service LowCardinality (String),
        environment LowCardinality (String),
        name String CODEC (ZSTD (1)),
        title String CODEC (ZSTD (1)),

        is_error UInt8 DEFAULT 0,

        correlation_id String DEFAULT '' CODEC (ZSTD (1)),
        parent_id String DEFAULT '' CODEC (ZSTD (1)),
        trace_id String DEFAULT '' CODEC (ZSTD (1)),
        span_id String DEFAULT '' CODEC (ZSTD (1)),
        parent_span_id String DEFAULT '' CODEC (ZSTD (1)),

        start_time Nullable (DateTime64 (3, 'UTC')) CODEC (Delta (8), ZSTD (1)),
        end_time Nullable (DateTime64 (3, 'UTC')) CODEC (Delta (8), ZSTD (1)),
        duration_ms Nullable (Float64) CODEC (ZSTD (1)),

        source_file String DEFAULT '' CODEC (ZSTD (1)),
        source_offset UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        source_length UInt32 DEFAULT 0 CODEC (Delta, ZSTD (1)),

        data JSON (max_dynamic_paths = 8192, max_dynamic_types = 8),

        INDEX idx_event_index_event_id event_id TYPE bloom_filter (0.01) GRANULARITY 4,
        INDEX idx_event_index_correlation_id correlation_id TYPE bloom_filter (0.01) GRANULARITY 4
    ) ENGINE = ReplacingMergeTree
PARTITION BY
    toYYYYMMDD (timestamp)
ORDER BY
    (tenant_id, timestamp, event_id);

DROP VIEW IF EXISTS observatory.mv_event_index;

CREATE MATERIALIZED VIEW
    IF NOT EXISTS observatory.mv_event_index TO observatory.event_index AS
SELECT
    tenant_id,
    timestamp,
    toStartOfInterval (timestamp, INTERVAL 1 MINUTE) AS bucket_time,
    event_id,
    event_type,
    signal,
    ifNull (data.service, '') AS service,
    ifNull (data.environment, '') AS environment,
    ifNull (data.name, '') AS name,
    multiIf (
        ifNull (data.name, '') != '', ifNull (data.name, ''),
        ifNull (data.metric_name, '') != '', ifNull (data.metric_name, ''),
        ifNull (data.body, '') != '', ifNull (data.body, ''),
        event_type
    ) AS title,
    toUInt8 (
        ifNull (data.is_error, 0) != 0
        OR lower (ifNull (data.span_status_code, '')) = 'error'
        OR endsWith (lower (event_type), '_error')
    ) AS is_error,
    multiIf (
        trace_id != '', trace_id,
        ifNull (data.request_id, '') != '', ifNull (data.request_id, ''),
        span_id != '', span_id,
        ifNull (data.session_id, '') != '', ifNull (data.session_id, ''),
        ifNull (data.account_id, '') != '', ifNull (data.account_id, ''),
        ifNull (data.user_id, '') != '', ifNull (data.user_id, ''),
        ''
    ) AS correlation_id,
    ifNull (data.parent_span_id, '') AS parent_id,
    trace_id,
    span_id,
    ifNull (data.parent_span_id, '') AS parent_span_id,
    coalesce (data.start_time, data.span_start_time) AS start_time,
    coalesce (data.end_time, data.span_end_time) AS end_time,
    data.duration_ms AS duration_ms,
    source_file,
    source_offset,
    source_length,
    CAST(toJSONString(data), 'JSON(max_dynamic_paths=8192, max_dynamic_types=8)') AS data
FROM
    observatory.events;

/*
 * span_fragments: normalized append-only span lifecycle fragments.
 *
 * Raw `events` preserves exactly what was ingested: producers may send
 * `span_start`, `span_end`, a complete `span`, or operation-shaped events such
 * as `llm.call`/`tool.call` with trace/span ids and duration. This table turns
 * those inputs into one common shape so trace UI never has to understand
 * producer lifecycle details.
 *
 * Concrete case:
 *   A trace contains 24 rows: agent.request, llm.call, retrieval.step,
 *   tool.call, plus HTTP spans. The event table should still show those raw
 *   events, but the flamegraph needs bounded operations with start/end times.
 *   Reading raw JSON and merging start/end in every browser refresh works for
 *   a demo and becomes expensive/noisy once a tenant has millions of traces.
 *
 * With this table:
 *   The loader writes one compact fragment row for every span-shaped event.
 *   `spans` and `trace_summaries` are derived from this append-only stream.
 */
CREATE TABLE
    IF NOT EXISTS observatory.span_fragments (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),

        trace_id String CODEC (ZSTD (1)),
        span_id String CODEC (ZSTD (1)),
        parent_span_id String DEFAULT '' CODEC (ZSTD (1)),

        event_id String CODEC (ZSTD (1)),
        event_type String CODEC (ZSTD (1)),
        signal LowCardinality (String) DEFAULT 'trace',
        service LowCardinality (String) DEFAULT '',
        environment LowCardinality (String) DEFAULT '',
        name String DEFAULT '' CODEC (ZSTD (1)),

        start_time Nullable (DateTime64 (3, 'UTC')) CODEC (Delta (8), ZSTD (1)),
        end_time Nullable (DateTime64 (3, 'UTC')) CODEC (Delta (8), ZSTD (1)),
        duration_ms Nullable (Float64) CODEC (ZSTD (1)),
        is_error UInt8 DEFAULT 0,

        timestamp DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        updated_at DateTime64 (3, 'UTC') DEFAULT now64 (3) CODEC (Delta (8), ZSTD (1)),

        source_file String DEFAULT '' CODEC (ZSTD (1)),
        source_offset UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        source_length UInt32 DEFAULT 0 CODEC (Delta, ZSTD (1)),

        data JSON (max_dynamic_paths = 8192, max_dynamic_types = 8),

        INDEX idx_span_fragments_trace_id trace_id TYPE bloom_filter (0.01) GRANULARITY 4,
        INDEX idx_span_fragments_span_id span_id TYPE bloom_filter (0.01) GRANULARITY 4
    ) ENGINE = ReplacingMergeTree (updated_at)
PARTITION BY
    toYYYYMMDD (timestamp)
ORDER BY
    (tenant_id, trace_id, span_id, timestamp, event_id);

/*
 * spans: queryable logical spans.
 *
 * This is the user-facing trace operation table. It intentionally hides
 * `span_start`/`span_end`; UI and dashboards ask for spans directly.
 *
 * ReplacingMergeTree keeps the most complete fragment per span. Complete
 * spans/end fragments use span_version=2; start-only fragments use
 * span_version=1. This means a late end fragment replaces the earlier open
 * span without requiring mutations.
 */
CREATE TABLE
    IF NOT EXISTS observatory.spans (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),

        trace_id String CODEC (ZSTD (1)),
        span_id String CODEC (ZSTD (1)),
        parent_span_id String DEFAULT '' CODEC (ZSTD (1)),

        event_ids Array (String) CODEC (ZSTD (1)),
        event_type String CODEC (ZSTD (1)),
        signal LowCardinality (String) DEFAULT 'trace',
        service LowCardinality (String) DEFAULT '',
        environment LowCardinality (String) DEFAULT '',
        name String DEFAULT '' CODEC (ZSTD (1)),

        start_time DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        end_time Nullable (DateTime64 (3, 'UTC')) CODEC (Delta (8), ZSTD (1)),
        duration_ms Nullable (Float64) CODEC (ZSTD (1)),
        is_error UInt8 DEFAULT 0,

        span_version UInt64 DEFAULT 1,
        updated_at DateTime64 (3, 'UTC') DEFAULT now64 (3) CODEC (Delta (8), ZSTD (1)),

        source_file String DEFAULT '' CODEC (ZSTD (1)),
        source_offset UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        source_length UInt32 DEFAULT 0 CODEC (Delta, ZSTD (1)),

        data JSON (max_dynamic_paths = 8192, max_dynamic_types = 8),

        INDEX idx_spans_trace_id trace_id TYPE bloom_filter (0.01) GRANULARITY 4,
        INDEX idx_spans_span_id span_id TYPE bloom_filter (0.01) GRANULARITY 4
    ) ENGINE = ReplacingMergeTree (span_version)
PARTITION BY
    toYYYYMMDD (start_time)
ORDER BY
    (tenant_id, trace_id, start_time, span_id);

CREATE MATERIALIZED VIEW
    IF NOT EXISTS observatory.mv_spans TO observatory.spans AS
SELECT
    tenant_id,
    trace_id,
    span_id,
    parent_span_id,
    [event_id] AS event_ids,
    multiIf (event_type IN ('span_start', 'span_end'), 'span', event_type) AS event_type,
    signal,
    service,
    environment,
    name,
    ifNull (
        start_time,
        if (
            isNotNull (end_time)
            AND isNotNull (duration_ms),
            end_time - toIntervalMillisecond (toInt64 (greatest (duration_ms, 0))),
            timestamp
        )
    ) AS start_time,
    if (
        isNull (end_time)
        AND isNotNull (duration_ms),
        ifNull (
            start_time,
            if (
                isNotNull (end_time)
                AND isNotNull (duration_ms),
                end_time - toIntervalMillisecond (toInt64 (greatest (duration_ms, 0))),
                timestamp
            )
        ) + toIntervalMillisecond (toInt64 (greatest (duration_ms, 0))),
        end_time
    ) AS end_time,
    if (
        isNull (duration_ms)
        AND isNotNull (start_time)
        AND isNotNull (end_time),
        toFloat64 (dateDiff ('millisecond', start_time, end_time)),
        duration_ms
    ) AS duration_ms,
    is_error,
    if (isNotNull (end_time) OR isNotNull (duration_ms), 2, 1) AS span_version,
    updated_at,
    source_file,
    source_offset,
    source_length,
    CAST(toJSONString(data), 'JSON(max_dynamic_paths=8192, max_dynamic_types=8)') AS data
FROM
    observatory.span_fragments
WHERE
    trace_id != ''
    AND span_id != '';

/*
 * trace_summaries: fast trace-list rows.
 *
 * The Logs sidebar should not group billions of span rows by trace_id just to
 * render 120 trace choices. This table stores aggregate states by 5-minute
 * bucket and trace; queries merge the states across the selected time range.
 */
CREATE TABLE
    IF NOT EXISTS observatory.trace_summaries (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        bucket_time DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        trace_id String CODEC (ZSTD (1)),

        trace_start_state AggregateFunction (min, DateTime64 (3, 'UTC')),
        trace_end_state AggregateFunction (max, DateTime64 (3, 'UTC')),
        span_count_state AggregateFunction (uniqCombined64, String),
        event_count_state AggregateFunction (sum, UInt64),
        error_count_state AggregateFunction (sum, UInt64),
        root_service_state AggregateFunction (argMin, String, DateTime64 (3, 'UTC')),
        root_name_state AggregateFunction (argMin, String, DateTime64 (3, 'UTC')),
        services_state AggregateFunction (groupUniqArray (64), String)
    ) ENGINE = AggregatingMergeTree
PARTITION BY
    toYYYYMM (bucket_time)
ORDER BY
    (tenant_id, bucket_time, trace_id);

CREATE MATERIALIZED VIEW
    IF NOT EXISTS observatory.mv_trace_summaries TO observatory.trace_summaries AS
SELECT
    tenant_id,
    toStartOfInterval (timestamp, INTERVAL 5 MINUTE) AS bucket_time,
    trace_id,
    minState (
        assumeNotNull (
        ifNull (
            start_time,
            if (
                isNotNull (end_time)
                AND isNotNull (duration_ms),
                end_time - toIntervalMillisecond (toInt64 (greatest (duration_ms, 0))),
                timestamp
            )
        )
        )
    ) AS trace_start_state,
    maxState (
        assumeNotNull (
        ifNull (
            end_time,
            if (
                isNotNull (start_time)
                AND isNotNull (duration_ms),
                start_time + toIntervalMillisecond (toInt64 (greatest (duration_ms, 0))),
                timestamp
            )
        )
        )
    ) AS trace_end_state,
    uniqCombined64State (span_id) AS span_count_state,
    sumState (toUInt64 (1)) AS event_count_state,
    sumState (toUInt64 (is_error)) AS error_count_state,
    argMinState (service, assumeNotNull (ifNull (start_time, timestamp))) AS root_service_state,
    argMinState (name, assumeNotNull (ifNull (start_time, timestamp))) AS root_name_state,
    groupUniqArrayState (64) (service) AS services_state
FROM
    observatory.span_fragments
WHERE
    trace_id != ''
    AND span_id != ''
GROUP BY
    tenant_id,
    bucket_time,
    trace_id;

/*
 * field_index: per-event extracted string fields.
 *
 * Concrete case:
 *   A user filters `service = api`, then switches the group-by to
 *   `customer.tier`, then searches for `request_id = req_123`. These are not
 *   numeric analytics; they are labels and lookup keys.
 *
 * Without this table:
 *   Every filter/group-by/search walks raw JSON in `events`. For a 24h window,
 *   that means up to 864B JSON documents. A field that appears on only 5% of
 *   events still requires checking 864B documents unless we have already
 *   extracted it. If a user runs this interaction repeatedly while narrowing a
 *   timeline, each click can become another huge JSON scan.
 *
 * With this table:
 *   A field definition promotes a JSON path into a row shaped like
 *   `(field_name, value, timestamp, event_id)`. Low-cardinality fields use
 *   mode='facet' and can drive group lists and counts. High-cardinality fields
 *   use mode='lookup' and are only used for exact searches. If we extract three
 *   fields on every event, that is 30M field rows/sec or 2.592T field rows/day,
 *   so this table must be budgeted and scoped. Values are stored as strings
 *   because fields are dimensions, not numeric measures; numeric math belongs
 *   in event_measures.
 */
CREATE TABLE
    IF NOT EXISTS observatory.field_index (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),

        mode LowCardinality (String),
        field_name LowCardinality (String),
        value String CODEC (ZSTD (1)),
        value_type LowCardinality (String),
        value_hash UInt64 MATERIALIZED cityHash64 (value),

        timestamp DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        bucket_time DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        event_id String CODEC (ZSTD (1)),

        event_type String CODEC (ZSTD (1)),
        signal LowCardinality (String),
        is_error UInt8 DEFAULT 0,
        trace_id String DEFAULT '' CODEC (ZSTD (1)),
        span_id String DEFAULT '' CODEC (ZSTD (1)),
        parent_span_id String DEFAULT '' CODEC (ZSTD (1)),
        name String DEFAULT '' CODEC (ZSTD (1)),
        start_time Nullable (DateTime64 (3, 'UTC')) CODEC (Delta (8), ZSTD (1)),
        end_time Nullable (DateTime64 (3, 'UTC')) CODEC (Delta (8), ZSTD (1)),
        duration_ms Nullable (Float64) CODEC (ZSTD (1)),

        definition_id String DEFAULT '' CODEC (ZSTD (1)),
        definition_version UInt64 DEFAULT 0
    ) ENGINE = ReplacingMergeTree
PARTITION BY
    toYYYYMMDD (timestamp)
ORDER BY
    (
        tenant_id,
        mode,
        field_name,
        value_hash,
        value,
        timestamp,
        event_id,
        definition_id
    );

/*
 * field_counts_5m: precomputed counts for facet values.
 *
 * Concrete case:
 *   The left panel needs to show top service values for the last 24h:
 *     api: 300B events
 *     canvas-agent: 120B events
 *     payments: 80B events
 *   These numbers are plausible when the full tenant emits 864B events/day and
 *   a few services dominate traffic.
 *
 * Without this table:
 *   The query groups raw field_index rows:
 *     SELECT value, count() FROM field_index
 *     WHERE field_name='service' AND timestamp >= now()-24h
 *     GROUP BY value
 *   If service is extracted on every event, this scans 864B service rows just
 *   to draw a small list of values. If the UI refreshes every 10 seconds, this
 *   becomes a repeated fleet-scale aggregate.
 *
 * With this table:
 *   The MV rolls facet fields into 5-minute buckets. The same UI request reads
 *   at most 288 buckets per value for a 24h window. With 500 services, that is
 *   around 144K rows/day for the service facet instead of 864B rows/day. This
 *   is the difference between "interactive value list" and "large ad hoc
 *   aggregate." Lookup-only ids such as trace_id/request_id should not feed this
 *   table because their value counts approach input row count.
 */
CREATE TABLE
    IF NOT EXISTS observatory.field_counts_5m (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        bucket_time DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        field_name LowCardinality (String),
        value String CODEC (ZSTD (1)),
        value_type LowCardinality (String),
        count UInt64 CODEC (Delta, ZSTD (1)),
        error_count UInt64 CODEC (Delta, ZSTD (1))
    ) ENGINE = SummingMergeTree
PARTITION BY
    toYYYYMM (bucket_time)
ORDER BY
    (tenant_id, field_name, bucket_time, value, value_type);

CREATE MATERIALIZED VIEW
    IF NOT EXISTS observatory.mv_field_counts_5m TO observatory.field_counts_5m AS
SELECT
    tenant_id,
    toStartOfInterval (timestamp, INTERVAL 5 MINUTE) AS bucket_time,
    field_name,
    value,
    value_type,
    count () AS count,
    sum (toUInt64 (is_error)) AS error_count
FROM
    observatory.field_index
WHERE
    mode = 'facet'
GROUP BY
    tenant_id,
    bucket_time,
    field_name,
    value,
    value_type;

/*
 * definitions: registry for promoted schema.
 *
 * Concrete case:
 *   A user adds:
 *     field  path=customer.tier mode=facet
 *     measure path=duration_ms
 *     rollup path=duration_ms group_by=service
 *   If customer.tier appears on 40% of events, future extraction writes about
 *   4M field rows/sec or 345.6B rows/day. A 30-day backfill scans 25.92T raw
 *   events and may write 10.37T customer.tier rows. That operation needs one
 *   durable definition record that every worker agrees on.
 *
 * Without this table:
 *   The loader, backfill worker, and UI would each need separate config for
 *   what to extract. A backfill could write rows that future ingestion does not
 *   write, or the UI could show a field that the loader does not understand. At
 *   the scale above, a mismatch is not a small correctness bug; it can create
 *   trillions of unexpected derived rows.
 *
 * With this table:
 *   Definitions are the source of truth. The loader reads enabled definitions
 *   and extracts future events. The backfill endpoint reads the same record and
 *   fills historical rows. The UI reads capabilities and latest stats to show
 *   whether a field is facet/lookup/numeric/precomputed and whether it has been
 *   backfilled.
 */
CREATE TABLE
    IF NOT EXISTS observatory.definitions (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        definition_id String CODEC (ZSTD (1)),
        name String CODEC (ZSTD (1)),
        kind LowCardinality (String),
        mode LowCardinality (String) DEFAULT '',
        enabled UInt8 DEFAULT 1,
        config JSON (max_dynamic_paths = 1024, max_dynamic_types = 8),
        capabilities JSON (max_dynamic_paths = 128, max_dynamic_types = 4),
        created_at DateTime64 (3, 'UTC') DEFAULT now64 (3),
        updated_at DateTime64 (3, 'UTC') DEFAULT now64 (3),
        deleted_at Nullable (DateTime64 (3, 'UTC')),
        version UInt64 DEFAULT toUnixTimestamp64Milli (updated_at)
    ) ENGINE = ReplacingMergeTree (updated_at)
ORDER BY
    (tenant_id, definition_id);

/*
 * event_rollups_5m: default summaries for the group list.
 *
 * Concrete case:
 *   The main Logs screen groups by service over 24h and shows:
 *     api          300B events   900M errors   duration summaries
 *     canvas-agent 120B events   120M errors
 *   The visible result might be 10-500 groups, but the underlying day contains
 *   864B events.
 *
 * Without this table:
 *   The UI must group field_index by service, count all events, count errors,
 *   and sum durations every refresh. For a 24h service grouping, this can scan
 *   864B service facet rows to produce a few hundred service rows.
 *
 * With this table:
 *   The MV keeps a small 5-minute summary for every facet value. The UI can
 *   read `(group_name, group_value, count, error_count, duration_sum)` and draw
 *   the group list quickly. With 500 services and 288 five-minute buckets/day,
 *   the service group list reads about 144K summary rows/day instead of 864B
 *   raw field rows/day. Richer numeric analytics use event_measures and
 *   measure_rollups.
 */
CREATE TABLE
    IF NOT EXISTS observatory.event_rollups_5m (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        bucket_time DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        group_name LowCardinality (String),
        group_value String CODEC (ZSTD (1)),
        count UInt64 CODEC (Delta, ZSTD (1)),
        error_count UInt64 CODEC (Delta, ZSTD (1)),
        duration_sum Float64 DEFAULT 0 CODEC (ZSTD (1)),
        duration_count UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1))
    ) ENGINE = SummingMergeTree
PARTITION BY
    toYYYYMM (bucket_time)
ORDER BY
    (tenant_id, group_name, bucket_time, group_value);

CREATE MATERIALIZED VIEW
    IF NOT EXISTS observatory.mv_event_rollups_5m TO observatory.event_rollups_5m AS
SELECT
    tenant_id,
    toStartOfInterval (timestamp, INTERVAL 5 MINUTE) AS bucket_time,
    field_name AS group_name,
    value AS group_value,
    count () AS count,
    sum (toUInt64 (is_error)) AS error_count,
    sum (ifNull (duration_ms, 0)) AS duration_sum,
    countIf (isNotNull (duration_ms)) AS duration_count
FROM
    observatory.field_index
WHERE
    mode = 'facet'
GROUP BY
    tenant_id,
    bucket_time,
    group_name,
    group_value;

/*
 * event_measures: per-event numeric observations.
 *
 * Concrete case:
 *   A user defines duration_ms, revenue, gpu_ms, or tokens as a measure. They
 *   then ask for average, p95, p99, top services by p95, or a range filter like
 *   duration_ms > 500. If duration_ms exists on 20% of events, that is 2M
 *   numeric values/sec, 172.8B values/day, and 5.184T values/30 days.
 *
 * Without this table:
 *   Each query has to parse raw JSON and cast strings dynamically:
 *     toFloat64OrNull(toString(data.duration_ms))
 *   over every candidate event. Over 30 days, the raw scan is up to 25.92T
 *   events. Even a 1% scoped query is 259.2B JSON documents to inspect and cast.
 *   It also mixes type errors and analytics execution in the same query.
 *
 * With this table:
 *   Numeric extraction happens once at ingestion/backfill time. The value is
 *   stored as Float64 with a measure_name and optional unit/dimension metadata.
 *   Short-window ad hoc numeric queries can scan the 172.8B/day measure stream
 *   for that measure directly, and long-window aggregate queries can read
 *   measure_rollups instead.
 */
CREATE TABLE
    IF NOT EXISTS observatory.event_measures (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        definition_id String DEFAULT '' CODEC (ZSTD (1)),
        definition_version UInt64 DEFAULT 0,
        measure_name LowCardinality (String),
        value Float64 CODEC (ZSTD (1)),
        unit LowCardinality (String) DEFAULT '',

        timestamp DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        bucket_time DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        bucket_seconds UInt32 DEFAULT 300,
        event_id String CODEC (ZSTD (1)),
        event_type String CODEC (ZSTD (1)),
        signal LowCardinality (String),

        dimension_name LowCardinality (String) DEFAULT '',
        dimension_value String DEFAULT '' CODEC (ZSTD (1))
    ) ENGINE = ReplacingMergeTree
PARTITION BY
    toYYYYMMDD (timestamp)
ORDER BY
    (
        tenant_id,
        definition_id,
        measure_name,
        dimension_name,
        dimension_value,
        timestamp,
        event_id,
        definition_id
    );

ALTER TABLE observatory.event_measures
    ADD COLUMN IF NOT EXISTS bucket_seconds UInt32 DEFAULT 300 AFTER bucket_time;

/*
 * measure_rollups: aggregate states for numeric measures.
 *
 * Concrete case:
 *   A dashboard asks for p95 duration_ms by service over the last 30 days with
 *   5-minute buckets. Suppose duration_ms exists on 20% of the 10M events/sec
 *   stream. That is 2M numeric observations/sec, 172.8B observations/day, and
 *   5.184T observations/30 days.
 *
 * Without this table:
 *   Every dashboard refresh scans raw event_measures for the full window and
 *   computes quantiles/grouping from scratch. For 30 days, that can mean 5.184T
 *   numeric rows just to render a line chart.
 *
 * With this table:
 *   The MV stores aggregate states per bucket/dimension:
 *     count, sum, min, max, avg, p50/p90/p95/p99 state
 *   A query reads one row per `(bucket, service)` instead of one row per event.
 *   With 500 services and 5-minute buckets, 30 days is:
 *     500 services * 288 buckets/day * 30 days = 4.32M rollup rows
 *   That is roughly 1.2M times fewer rows than 5.184T observations.
 */
CREATE TABLE
    IF NOT EXISTS observatory.measure_rollups (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        definition_id String DEFAULT '' CODEC (ZSTD (1)),
        definition_version UInt64 DEFAULT 0,
        bucket_time DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        bucket_seconds UInt32 DEFAULT 300,
        measure_name LowCardinality (String),
        unit LowCardinality (String),
        dimension_name LowCardinality (String),
        dimension_value String CODEC (ZSTD (1)),

        count_state AggregateFunction (sum, UInt64),
        sum_state AggregateFunction (sum, Float64),
        min_state AggregateFunction (min, Float64),
        max_state AggregateFunction (max, Float64),
        avg_state AggregateFunction (avg, Float64),
        quantiles_state AggregateFunction (quantilesTDigest (0.5, 0.9, 0.95, 0.99), Float64)
    ) ENGINE = AggregatingMergeTree
PARTITION BY
    toYYYYMM (bucket_time)
ORDER BY
    (
        tenant_id,
        definition_id,
        measure_name,
        bucket_seconds,
        bucket_time,
        dimension_name,
        dimension_value
    );

ALTER TABLE observatory.measure_rollups
    ADD COLUMN IF NOT EXISTS definition_id String DEFAULT '' CODEC (ZSTD (1)) AFTER tenant_id;

ALTER TABLE observatory.measure_rollups
    ADD COLUMN IF NOT EXISTS definition_version UInt64 DEFAULT 0 AFTER definition_id;

ALTER TABLE observatory.measure_rollups
    ADD COLUMN IF NOT EXISTS bucket_seconds UInt32 DEFAULT 300 AFTER bucket_time;

DROP VIEW IF EXISTS observatory.mv_measure_rollups;

CREATE MATERIALIZED VIEW
    IF NOT EXISTS observatory.mv_measure_rollups TO observatory.measure_rollups AS
SELECT
    tenant_id,
    definition_id,
    anyLast (definition_version) AS definition_version,
    bucket_time,
    bucket_seconds,
    measure_name,
    unit,
    dimension_name,
    dimension_value,
    sumState (toUInt64 (1)) AS count_state,
    sumState (value) AS sum_state,
    minState (value) AS min_state,
    maxState (value) AS max_state,
    avgState (value) AS avg_state,
    quantilesTDigestState (0.5, 0.9, 0.95, 0.99) (value) AS quantiles_state
FROM
    observatory.event_measures
GROUP BY
    tenant_id,
    definition_id,
    bucket_time,
    bucket_seconds,
    measure_name,
    unit,
    dimension_name,
    dimension_value;

/*
 * entity_state_updates: longitudinal state transitions.
 *
 * Concrete case:
 *   A customer changes account.plan from free -> pro at T. Later we ask:
 *     - how many accounts converted to pro by week?
 *     - what was the account plan before churn?
 *     - did risk_tier change before a failed withdrawal?
 *   If account/user state changes are only 0.01% of the stream, that is still
 *   1K state-changing events/sec or 86.4M state updates/day. The full raw event
 *   stream is 864B events/day.
 *
 * Without this table:
 *   Each query has to find every relevant raw event for every entity, order
 *   those events by time, and reconstruct state transitions. For one account,
 *   filtering raw events by account_id is fine. For millions of accounts over
 *   months, it becomes a fleet-wide temporal reconstruction query. A 30-day
 *   "plan before churn" report can otherwise inspect up to 25.92T raw events to
 *   rediscover a much smaller state-change history.
 *
 * With this table:
 *   State-changing events are extracted once as `(entity_type, entity_id,
 *   state_name, value, timestamp)`. Longitudinal queries scan about 86.4M/day
 *   state updates under the 0.01% assumption instead of 864B/day raw events.
 *   This table stores the history, not only current state, because analytics
 *   often needs the state at a past point in time.
 */
CREATE TABLE
    IF NOT EXISTS observatory.entity_state_updates (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        definition_id String DEFAULT '' CODEC (ZSTD (1)),
        definition_version UInt64 DEFAULT 0,

        entity_type LowCardinality (String),
        entity_id String CODEC (ZSTD (1)),
        entity_hash UInt64 MATERIALIZED cityHash64 (entity_id),
        state_name LowCardinality (String),
        value String CODEC (ZSTD (1)),
        value_type LowCardinality (String),

        timestamp DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        event_id String CODEC (ZSTD (1)),
        event_type String CODEC (ZSTD (1)),
        signal LowCardinality (String)
    ) ENGINE = ReplacingMergeTree
PARTITION BY
    toYYYYMMDD (timestamp)
ORDER BY
    (
        tenant_id,
        entity_type,
        entity_hash,
        state_name,
        timestamp,
        event_id,
        definition_id
    );

/*
 * report_results: generic scheduled report output.
 *
 * Concrete case:
 *   A dashboard shows daily revenue by plan, p95 latency by service, and SLO
 *   burn by route for the last 90 days. These are stable business/dashboard
 *   numbers that many users may view repeatedly. If checkout/revenue events are
 *   0.1% of traffic, that is 10K events/sec, 864M events/day, and 77.76B events
 *   over 90 days before any joins to plan/customer/service dimensions.
 *
 * Without this table:
 *   Every page load recomputes the same grouped aggregates from raw/indexed
 *   events or measures. Even with field_index and measure_rollups, complex
 *   reports can require multiple joins, filters, and post-processing steps. Ten
 *   users refreshing the same report can trigger the same 77.76B-row logical
 *   computation repeatedly.
 *
 * With this table:
 *   A worker computes the report on a schedule and writes result rows containing
 *   JSON dimensions and JSON metrics. Daily revenue by 5 plans over 90 days is
 *   about 450 rows; adding 500 services is about 225K rows. The UI reads compact
 *   report output instead of recomputing billions of source events on every
 *   dashboard refresh.
 */
CREATE TABLE
    IF NOT EXISTS observatory.report_results (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        report_id String CODEC (ZSTD (1)),
        report_version UInt64 DEFAULT 0,
        bucket_time DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        dimensions JSON (max_dynamic_paths = 128, max_dynamic_types = 8),
        metrics JSON (max_dynamic_paths = 128, max_dynamic_types = 8),
        refreshed_at DateTime64 (3, 'UTC') DEFAULT now64 (3)
    ) ENGINE = ReplacingMergeTree (refreshed_at)
PARTITION BY
    toYYYYMM (bucket_time)
ORDER BY
    (tenant_id, report_id, bucket_time);

/*
 * sequence_report_results: ordered-step funnel output.
 *
 * Concrete case:
 *   Activation funnel:
 *     signup -> create_workspace -> send_first_event -> view_dashboard
 *   Entity: user_id. Each user has 7 days to complete the sequence. Over a
 *   30-day report window, even if only 1% of 25.92T events are relevant funnel
 *   events, the computation may still inspect 259.2B events and correlate them
 *   by user_id in timestamp order. If there are 50M signing users in the window,
 *   the query also has to dedupe/order tens of millions of entity timelines.
 *
 * Without this table:
 *   Every dashboard refresh repeats the expensive ordered correlation:
 *     find signups, join later create_workspace, join later send_first_event,
 *     join later view_dashboard, dedupe users, bucket by signup week.
 *   ClickHouse can execute this style of query, but it is not a cheap
 *   interactive refresh over months of data.
 *
 * With this table:
 *   A worker materializes the step counts:
 *     bucket_time, step_index, step_name, entity_count, conversion_count
 *   For 30 daily buckets and 4 steps, the UI reads about 120 rows instead of
 *   reprocessing 259.2B relevant events and 50M user timelines.
 */
CREATE TABLE
    IF NOT EXISTS observatory.sequence_report_results (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        report_id String CODEC (ZSTD (1)),
        report_version UInt64 DEFAULT 0,
        bucket_time DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        step_index UInt16,
        step_name String CODEC (ZSTD (1)),
        entity_count UInt64 CODEC (Delta, ZSTD (1)),
        conversion_count UInt64 CODEC (Delta, ZSTD (1)),
        refreshed_at DateTime64 (3, 'UTC') DEFAULT now64 (3)
    ) ENGINE = ReplacingMergeTree (refreshed_at)
PARTITION BY
    toYYYYMM (bucket_time)
ORDER BY
    (tenant_id, report_id, bucket_time, step_index);

/*
 * cohort_memberships: reusable materialized entity sets.
 *
 * Concrete case:
 *   Define a cohort: "accounts that activated within 7 days of signup" or
 *   "users with three failed payments in 30 days". That cohort is then reused
 *   by retention, revenue, support, and churn reports. If payment events are
 *   0.1% of traffic, failed-payment cohort construction may inspect 864M
 *   payment events/day or 25.92B payment events/30 days before it produces,
 *   say, a 20M-user cohort.
 *
 * Without this table:
 *   Every downstream report recomputes the cohort membership first, then runs
 *   its own analysis. If the cohort itself requires ordered events, absence
 *   checks, or multi-step conditions, five downstream reports can repeat the
 *   same 25.92B-row cohort build five times.
 *
 * With this table:
 *   The worker computes membership once and stores `(cohort_id, entity_type,
 *   entity_id, first_seen, last_seen)`. Downstream queries can join or filter
 *   against the 20M materialized members instead of rebuilding the cohort from
 *   25.92B source events.
 */
CREATE TABLE
    IF NOT EXISTS observatory.cohort_memberships (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        cohort_id String CODEC (ZSTD (1)),
        cohort_version UInt64 DEFAULT 0,
        entity_type LowCardinality (String),
        entity_id String CODEC (ZSTD (1)),
        entity_hash UInt64 MATERIALIZED cityHash64 (entity_id),
        first_seen DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        last_seen DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        refreshed_at DateTime64 (3, 'UTC') DEFAULT now64 (3)
    ) ENGINE = ReplacingMergeTree (refreshed_at)
PARTITION BY
    cityHash64 (cohort_id) % 16
ORDER BY
    (tenant_id, cohort_id, entity_type, entity_hash);

/*
 * definition_stats: backfill/estimate audit trail.
 *
 * Concrete case:
 *   A user adds customer.tier with a full-history backfill. The system needs to
 *   show whether the backfill ran, what time range it covered, how many rows it
 *   matched, how many distinct values it saw, and whether cardinality looks
 *   safe for facets/rollups. With the scale assumption, a 30-day backfill scans
 *   25.92T raw events. If customer.tier exists on 35% of events, it writes
 *   about 9.07T field rows. If request_id exists on 70%, it writes 18.14T
 *   lookup rows and should not enable facet counts.
 *
 * Without this table:
 *   The UI can only show "not run" or rely on transient worker logs. On refresh,
 *   the system loses the operational record of what happened and cannot explain
 *   why a definition is cheap, expensive, rejected, or lookup-only. After a
 *   25.92T-row scan, losing the result metadata is operationally unacceptable.
 *
 * With this table:
 *   Each estimate/backfill writes durable stats. The Schema page can show latest
 *   status, rows scanned/matched, distinct values, and decision. Future
 *   admission control can use these rows to prevent a user from accidentally
 *   enabling a multi-trillion-row facet or rollup.
 */
CREATE TABLE
    IF NOT EXISTS observatory.definition_stats (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        definition_id String CODEC (ZSTD (1)),
        definition_version UInt64 DEFAULT 0,
        measured_at DateTime64 (3, 'UTC') DEFAULT now64 (3),
        window_start DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        window_end DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        rows_scanned UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        rows_matched UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        distinct_values UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        estimated_rows_per_sec Float64 DEFAULT 0 CODEC (ZSTD (1)),
        estimated_storage_bytes_per_day UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        cardinality_class LowCardinality (String) DEFAULT '',
        decision LowCardinality (String) DEFAULT '',
        warnings Array (String) DEFAULT []
    ) ENGINE = ReplacingMergeTree (measured_at)
ORDER BY
    (tenant_id, definition_id, definition_version);

/*
 * pipeline_metrics: internal operational metrics.
 *
 * Concrete case:
 *   The loader is behind by 12 minutes, a processor is failing builds, or
 *   backfills are scanning too many rows. At 10M events/sec, 12 minutes of
 *   loader lag means 7.2B events are not yet reflected in field_index,
 *   event_measures, and rollups. Operators need to debug this using the same
 *   ClickHouse query surface as product analytics.
 *
 * Without this table:
 *   Pipeline health is scattered across process logs and container metrics. It
 *   is hard to correlate "UI missing facet values" with "loader extraction lag"
 *   or "processor failed after deploy". A 7.2B-event lag would look like a data
 *   quality problem in the UI rather than an ingestion health problem.
 *
 * With this table:
 *   Components write metric_name/value/unit plus JSON attributes. Dashboards and
 *   ad hoc queries can inspect ingestion lag, processor failures, backfill
 *   throughput, and other system health signals per tenant/component. For
 *   example, one row per component/metric/minute is only thousands of rows/day,
 *   but it explains whether billions of event-derived rows are stale.
 */
CREATE TABLE
    IF NOT EXISTS observatory.pipeline_metrics (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        component LowCardinality (String),
        metric_name LowCardinality (String),
        value Float64 CODEC (ZSTD (1)),
        unit LowCardinality (String) DEFAULT '',
        timestamp DateTime64 (3, 'UTC') DEFAULT now64 (3) CODEC (Delta (8), ZSTD (1)),
        attributes JSON (max_dynamic_paths = 128, max_dynamic_types = 8)
    ) ENGINE = MergeTree
PARTITION BY
    toYYYYMMDD (timestamp)
ORDER BY
    (tenant_id, component, metric_name, timestamp);
