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
            event_type LowCardinality (Nullable (String)),
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
        event_type LowCardinality (String) MATERIALIZED ifNull (data.event_type, ''),
        trace_id String MATERIALIZED ifNull (data.trace_id, '') CODEC (ZSTD (1)),
        span_id String MATERIALIZED ifNull (data.span_id, '') CODEC (ZSTD (1)),

        /*
         * Closed-set signal classifier. The 'other' fallback bucket prevents
         * arbitrary `event_type` values from being absorbed into the
         * LowCardinality dictionary, which would degrade merge performance.
         */
        signal LowCardinality (String) MATERIALIZED multiIf (
            ifNull (data.event_type, '') IN ('span', 'span_start', 'span_end'), 'trace',
            ifNull (data.event_type, '') = 'metric', 'metric',
            ifNull (data.event_type, '') = 'log', 'log',
            ifNull (data.event_type, '') IN ('analytics', 'track', 'page', 'screen', 'identify', 'group', 'alias'), 'analytics',
            'other'
        ),

        /* The sort key starts with tenant_id, so trace/span lookups by id
           alone scan a wide range. Bloom filters cut that to matching parts. */
        INDEX idx_trace_id trace_id TYPE bloom_filter (0.01) GRANULARITY 1,
        INDEX idx_span_id span_id TYPE bloom_filter (0.01) GRANULARITY 1
    ) ENGINE = MergeTree
PARTITION BY
    toYYYYMMDD (timestamp)
ORDER BY
    (tenant_id, event_type, timestamp, trace_id, span_id);
