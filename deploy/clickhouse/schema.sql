/*
 * Nanotrace raw-first analytics schema.
 *
 * Default ingestion writes one fact table: observatory.events. Business
 * analytics acceleration is explicit: users promote fields, measures, states,
 * cohorts, and reports, then backfill/materialize only those read models.
 */
CREATE DATABASE IF NOT EXISTS observatory;

DROP VIEW IF EXISTS observatory.mv_event_index;
DROP VIEW IF EXISTS observatory.mv_spans;
DROP VIEW IF EXISTS observatory.mv_trace_summaries;
DROP VIEW IF EXISTS observatory.mv_field_counts_5m;
DROP VIEW IF EXISTS observatory.mv_event_rollups_5m;
DROP VIEW IF EXISTS observatory.mv_field_density_1s;
DROP VIEW IF EXISTS observatory.mv_field_topk_1m;
DROP VIEW IF EXISTS observatory.mv_field_rollups;
DROP VIEW IF EXISTS observatory.mv_field_values;
DROP VIEW IF EXISTS observatory.mv_flamegraph_rollups_1m;

DROP TABLE IF EXISTS observatory.event_index;
DROP TABLE IF EXISTS observatory.span_fragments;
DROP TABLE IF EXISTS observatory.spans;
DROP TABLE IF EXISTS observatory.trace_summaries;
DROP TABLE IF EXISTS observatory.field_counts_5m;
DROP TABLE IF EXISTS observatory.event_rollups_5m;
DROP TABLE IF EXISTS observatory.field_density_1s;
DROP TABLE IF EXISTS observatory.field_topk_1m;

CREATE TABLE
    IF NOT EXISTS observatory.events (
        event_id String CODEC (ZSTD (1)),
        timestamp DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        observed_timestamp DateTime64 (3, 'UTC') DEFAULT timestamp CODEC (Delta (8), ZSTD (1)),
        ingested_timestamp DateTime64 (3, 'UTC') DEFAULT now64 (3) CODEC (Delta (8), ZSTD (1)),

        source_file String DEFAULT '' CODEC (ZSTD (1)),
        source_offset UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        source_length UInt32 DEFAULT 0 CODEC (Delta, ZSTD (1)),

        data JSON (
            tenant_id LowCardinality (Nullable (String)),
            event_type Nullable (String),
            signal LowCardinality (Nullable (String)),
            name LowCardinality (Nullable (String)),

            user_id Nullable (String),
            anonymous_id Nullable (String),
            account_id Nullable (String),
            session_id Nullable (String),
            group_id Nullable (String),

            service LowCardinality (Nullable (String)),
            environment LowCardinality (Nullable (String)),
            http.method LowCardinality (Nullable (String)),
            http.route Nullable (String),
            http.status_code Nullable (String),
            http.response.status_code Nullable (String),
            severity_text LowCardinality (Nullable (String)),
            severity_number Nullable (Int32),
            llm.model Nullable (String),
            llm.provider LowCardinality (Nullable (String)),
            tool_name Nullable (String),
            processor_name Nullable (String),

            trace_id Nullable (String),
            span_id Nullable (String),
            parent_span_id Nullable (String),
            request_id Nullable (String),
            organization_id Nullable (String),
            thread_id Nullable (String),
            conversation_id Nullable (String),
            start_time Nullable (DateTime64 (3, 'UTC')),
            end_time Nullable (DateTime64 (3, 'UTC')),
            duration_ms Nullable (Float64),
            is_error Nullable (UInt8),
            metric_name LowCardinality (Nullable (String)),
            metric_type LowCardinality (Nullable (String)),
            metric_unit LowCardinality (Nullable (String)),
            metric_value Nullable (Float64),

            revenue Nullable (Float64),
            currency LowCardinality (Nullable (String)),

            joined_at Nullable (DateTime64 (3, 'UTC')),
            user_group LowCardinality (Nullable (String)),
            plan LowCardinality (Nullable (String)),
            country LowCardinality (Nullable (String)),
            region LowCardinality (Nullable (String)),

            max_dynamic_paths = 8192,
            max_dynamic_types = 8
        ),

        tenant_id LowCardinality (String) MATERIALIZED ifNull (data.tenant_id, ''),
        event_type String MATERIALIZED ifNull (data.event_type, '') CODEC (ZSTD (1)),
        trace_id String MATERIALIZED ifNull (data.trace_id, '') CODEC (ZSTD (1)),
        span_id String MATERIALIZED ifNull (data.span_id, '') CODEC (ZSTD (1)),
        signal LowCardinality (String) MATERIALIZED multiIf (
            ifNull (data.signal, '') != '', ifNull (data.signal, ''),
            ifNull (data.event_type, '') IN ('span', 'span_start', 'span_end'), 'trace',
            ifNull (data.event_type, '') = 'metric', 'metric',
            ifNull (data.event_type, '') = 'log', 'log',
            ifNull (data.event_type, '') IN ('analytics', 'track', 'page', 'screen', 'identify', 'group', 'alias'), 'analytics',
            'other'
        ),

        INDEX idx_event_id event_id TYPE bloom_filter (0.01) GRANULARITY 4,
        INDEX idx_trace_id trace_id TYPE bloom_filter (0.01) GRANULARITY 4,
        INDEX idx_span_id span_id TYPE bloom_filter (0.01) GRANULARITY 4
    ) ENGINE = MergeTree
PARTITION BY
    toYYYYMMDD (timestamp)
ORDER BY
    (tenant_id, timestamp, event_type, event_id);

CREATE TABLE
    IF NOT EXISTS observatory.event_density_1s (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        bucket_time DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        count UInt64 CODEC (Delta, ZSTD (1)),
        error_count UInt64 CODEC (Delta, ZSTD (1))
    ) ENGINE = SummingMergeTree
PARTITION BY
    toYYYYMM (bucket_time)
ORDER BY
    (tenant_id, bucket_time);

CREATE MATERIALIZED VIEW
    IF NOT EXISTS observatory.mv_event_density_1s TO observatory.event_density_1s AS
SELECT
    tenant_id,
    toStartOfInterval (timestamp, INTERVAL 1 SECOND) AS bucket_time,
    count () AS count,
    sum (toUInt64 (ifNull (data.is_error, 0) != 0 OR endsWith (lower (event_type), '_error'))) AS error_count
FROM
    observatory.events
GROUP BY
    tenant_id,
    bucket_time;

CREATE TABLE
    IF NOT EXISTS observatory.field_rollups (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        field_name LowCardinality (String),
        value String CODEC (ZSTD (1)),
        value_hash UInt64 MATERIALIZED cityHash64 (value),
        bucket_time DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        bucket_seconds UInt32 DEFAULT 60,
        count UInt64 CODEC (Delta, ZSTD (1)),
        error_count UInt64 CODEC (Delta, ZSTD (1)),
        trace_count UInt64 CODEC (Delta, ZSTD (1)),
        log_count UInt64 CODEC (Delta, ZSTD (1)),
        metric_count UInt64 CODEC (Delta, ZSTD (1)),
        analytics_count UInt64 CODEC (Delta, ZSTD (1)),
        duration_count UInt64 CODEC (Delta, ZSTD (1)),
        duration_sum Float64 CODEC (ZSTD (1))
    ) ENGINE = SummingMergeTree
PARTITION BY
    toYYYYMM (bucket_time)
ORDER BY
    (tenant_id, field_name, bucket_seconds, bucket_time, value_hash, value);

CREATE MATERIALIZED VIEW
    IF NOT EXISTS observatory.mv_field_rollups TO observatory.field_rollups AS
WITH
    toString (ifNull (data.service, '')) AS service_value,
    toString (ifNull (data.environment, '')) AS environment_value,
    toString (ifNull (data.name, '')) AS name_value,
    toUInt64 (ifNull (data.is_error, 0) != 0 OR endsWith (lower (event_type), '_error')) AS error_value,
    toFloat64 (ifNull (data.duration_ms, 0)) AS duration_value,
    arrayFilter (
        dimension -> tupleElement (dimension, 2) != '',
        [
            ('signal', signal),
            ('event_type', event_type),
            ('name', name_value),
            ('service', service_value),
            ('environment', environment_value),
            ('is_error', toString (error_value))
        ]
    ) AS dimensions
SELECT
    tenant_id,
    tupleElement (rollup, 1) AS field_name,
    tupleElement (rollup, 2) AS value,
    tupleElement (rollup, 3) AS bucket_time,
    tupleElement (rollup, 4) AS bucket_seconds,
    count () AS count,
    sum (error_value) AS error_count,
    countIf (signal = 'trace') AS trace_count,
    countIf (signal = 'log') AS log_count,
    countIf (signal = 'metric') AS metric_count,
    countIf (signal = 'analytics') AS analytics_count,
    countIf (ifNull (data.duration_ms, 0) > 0) AS duration_count,
    sum (duration_value) AS duration_sum
FROM
    observatory.events
ARRAY JOIN
    arrayConcat (
        arrayMap (
            dimension -> (
                tupleElement (dimension, 1),
                tupleElement (dimension, 2),
                toStartOfInterval (timestamp, INTERVAL 1 SECOND),
                toUInt32 (1)
            ),
            dimensions
        ),
        arrayMap (
            dimension -> (
                tupleElement (dimension, 1),
                tupleElement (dimension, 2),
                toStartOfInterval (timestamp, INTERVAL 1 MINUTE),
                toUInt32 (60)
            ),
            dimensions
        )
    ) AS rollup
GROUP BY
    tenant_id,
    field_name,
    value,
    bucket_time,
    bucket_seconds;

CREATE TABLE
    IF NOT EXISTS observatory.field_values (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        field_name LowCardinality (String),
        value String CODEC (ZSTD (1)),
        value_hash UInt64 MATERIALIZED cityHash64 (value),
        timestamp DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        bucket_time DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        event_id String CODEC (ZSTD (1)),
        event_type String CODEC (ZSTD (1)),
        signal LowCardinality (String),
        is_error UInt8 DEFAULT 0,
        trace_id String DEFAULT '' CODEC (ZSTD (1)),
        span_id String DEFAULT '' CODEC (ZSTD (1)),
        name String DEFAULT '' CODEC (ZSTD (1))
    ) ENGINE = MergeTree
PARTITION BY
    toYYYYMMDD (timestamp)
ORDER BY
    (tenant_id, field_name, value_hash, value, timestamp, event_id);

CREATE MATERIALIZED VIEW
    IF NOT EXISTS observatory.mv_field_values TO observatory.field_values AS
WITH
    toString (getSubcolumn (data, 'request_id')) AS request_id_value,
    toString (ifNull (data.user_id, '')) AS user_id_value,
    toString (ifNull (data.anonymous_id, '')) AS anonymous_id_value,
    toString (ifNull (data.account_id, '')) AS account_id_value,
    toString (ifNull (data.session_id, '')) AS session_id_value,
    toString (ifNull (data.group_id, '')) AS group_id_value,
    toString (getSubcolumn (data, 'organization_id')) AS organization_id_value,
    toString (getSubcolumn (data, 'thread_id')) AS thread_id_value,
    toString (getSubcolumn (data, 'conversation_id')) AS conversation_id_value,
    toUInt8 (ifNull (data.is_error, 0) != 0 OR endsWith (lower (event_type), '_error')) AS error_value
SELECT
    tenant_id,
    tupleElement (lookup, 1) AS field_name,
    tupleElement (lookup, 2) AS value,
    timestamp,
    toStartOfInterval (timestamp, INTERVAL 1 MINUTE) AS bucket_time,
    event_id,
    event_type,
    signal,
    error_value AS is_error,
    trace_id,
    span_id,
    toString (ifNull (data.name, '')) AS name
FROM
    observatory.events
ARRAY JOIN
    arrayFilter (
        lookup -> tupleElement (lookup, 2) != '',
        [
            ('trace_id', trace_id),
            ('span_id', span_id),
            ('request_id', request_id_value),
            ('user_id', user_id_value),
            ('anonymous_id', anonymous_id_value),
            ('account_id', account_id_value),
            ('session_id', session_id_value),
            ('group_id', group_id_value),
            ('organization_id', organization_id_value),
            ('thread_id', thread_id_value),
            ('conversation_id', conversation_id_value)
        ]
    ) AS lookup;

CREATE TABLE
    IF NOT EXISTS observatory.flamegraph_rollups_1m (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        flamegraph_id LowCardinality (String),
        bucket_time DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        level UInt8,
        path String CODEC (ZSTD (1)),
        path_hash UInt64 MATERIALIZED cityHash64 (path),
        parent_path String DEFAULT '' CODEC (ZSTD (1)),
        parent_path_hash UInt64 MATERIALIZED cityHash64 (parent_path),
        count UInt64 CODEC (Delta, ZSTD (1)),
        error_count UInt64 CODEC (Delta, ZSTD (1)),
        duration_count UInt64 CODEC (Delta, ZSTD (1)),
        duration_sum Float64 CODEC (ZSTD (1))
    ) ENGINE = SummingMergeTree
PARTITION BY
    toYYYYMM (bucket_time)
ORDER BY
    (tenant_id, flamegraph_id, bucket_time, level, parent_path_hash, path_hash, path);

/*
 * flamegraph_rollups_1m is retained as an explicit report/materialization
 * target. It is intentionally not populated by an always-on events MV because
 * hierarchy rollups add high insert-path fanout and do not back the trace
 * waterfall UI directly.
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
    (tenant_id, mode, field_name, value_hash, value, timestamp, event_id, definition_id);

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
    (tenant_id, definition_id, measure_name, dimension_name, dimension_value, timestamp, event_id, definition_version);

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
    (tenant_id, definition_id, measure_name, bucket_seconds, bucket_time, dimension_name, dimension_value);

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

CREATE TABLE
    IF NOT EXISTS observatory.counter_rollups (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        definition_id String DEFAULT '' CODEC (ZSTD (1)),
        definition_version UInt64 DEFAULT 0,
        metric_name LowCardinality (String),
        unit LowCardinality (String) DEFAULT '',
        bucket_time DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        bucket_seconds UInt32 DEFAULT 60,
        dimensions JSON (max_dynamic_paths = 128, max_dynamic_types = 8),
        dimensions_hash UInt64 MATERIALIZED cityHash64 (toJSONString (dimensions)),
        count SimpleAggregateFunction (sum, UInt64),
        sum SimpleAggregateFunction (sum, Float64)
    ) ENGINE = AggregatingMergeTree
PARTITION BY
    toYYYYMM (bucket_time)
ORDER BY
    (tenant_id, definition_id, metric_name, bucket_seconds, bucket_time, dimensions_hash);

CREATE TABLE
    IF NOT EXISTS observatory.gauge_rollups (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        definition_id String DEFAULT '' CODEC (ZSTD (1)),
        definition_version UInt64 DEFAULT 0,
        metric_name LowCardinality (String),
        unit LowCardinality (String) DEFAULT '',
        bucket_time DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        bucket_seconds UInt32 DEFAULT 60,
        dimensions JSON (max_dynamic_paths = 128, max_dynamic_types = 8),
        dimensions_hash UInt64 MATERIALIZED cityHash64 (toJSONString (dimensions)),
        count SimpleAggregateFunction (sum, UInt64),
        sum SimpleAggregateFunction (sum, Float64),
        min SimpleAggregateFunction (min, Float64),
        max SimpleAggregateFunction (max, Float64),
        last SimpleAggregateFunction (anyLast, Float64)
    ) ENGINE = AggregatingMergeTree
PARTITION BY
    toYYYYMM (bucket_time)
ORDER BY
    (tenant_id, definition_id, metric_name, bucket_seconds, bucket_time, dimensions_hash);

CREATE TABLE
    IF NOT EXISTS observatory.histogram_rollups (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        definition_id String DEFAULT '' CODEC (ZSTD (1)),
        definition_version UInt64 DEFAULT 0,
        metric_name LowCardinality (String),
        unit LowCardinality (String) DEFAULT '',
        bucket_time DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        bucket_seconds UInt32 DEFAULT 60,
        dimensions JSON (max_dynamic_paths = 128, max_dynamic_types = 8),
        dimensions_hash UInt64 MATERIALIZED cityHash64 (toJSONString (dimensions)),
        count SimpleAggregateFunction (sum, UInt64),
        sum SimpleAggregateFunction (sum, Float64),
        min SimpleAggregateFunction (min, Float64),
        max SimpleAggregateFunction (max, Float64)
    ) ENGINE = AggregatingMergeTree
PARTITION BY
    toYYYYMM (bucket_time)
ORDER BY
    (tenant_id, definition_id, metric_name, bucket_seconds, bucket_time, dimensions_hash);

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
    (tenant_id, entity_type, entity_hash, state_name, timestamp, event_id, definition_id);

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
    (tenant_id, cohort_id, cohort_version, entity_type, entity_hash);

CREATE TABLE
    IF NOT EXISTS observatory.report_results (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        report_id String CODEC (ZSTD (1)),
        report_version UInt64 DEFAULT 0,
        bucket_time DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        dimensions JSON (max_dynamic_paths = 128, max_dynamic_types = 8),
        dimensions_hash UInt64 MATERIALIZED cityHash64 (toJSONString (dimensions)),
        metrics JSON (max_dynamic_paths = 128, max_dynamic_types = 8),
        refreshed_at DateTime64 (3, 'UTC') DEFAULT now64 (3)
    ) ENGINE = ReplacingMergeTree (refreshed_at)
PARTITION BY
    toYYYYMM (bucket_time)
ORDER BY
    (tenant_id, report_id, report_version, bucket_time, dimensions_hash);

CREATE TABLE
    IF NOT EXISTS observatory.sequence_report_results (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        report_id String CODEC (ZSTD (1)),
        report_version UInt64 DEFAULT 0,
        bucket_time DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        segment JSON (max_dynamic_paths = 128, max_dynamic_types = 8),
        segment_hash UInt64 MATERIALIZED cityHash64 (toJSONString (segment)),
        step_index UInt16,
        step_name String CODEC (ZSTD (1)),
        entity_count UInt64 CODEC (Delta, ZSTD (1)),
        conversion_count UInt64 CODEC (Delta, ZSTD (1)),
        refreshed_at DateTime64 (3, 'UTC') DEFAULT now64 (3)
    ) ENGINE = ReplacingMergeTree (refreshed_at)
PARTITION BY
    toYYYYMM (bucket_time)
ORDER BY
    (tenant_id, report_id, report_version, bucket_time, segment_hash, step_index);

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
        decision LowCardinality (String) DEFAULT ''
    ) ENGINE = ReplacingMergeTree (measured_at)
ORDER BY
    (tenant_id, definition_id, definition_version);

CREATE TABLE
    IF NOT EXISTS observatory.query_usage (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        query_id String CODEC (ZSTD (1)),
        query_hash UInt64,
        query_shape String CODEC (ZSTD (3)),
        surface LowCardinality (String) DEFAULT '',
        plan_kind LowCardinality (String) DEFAULT '',
        is_raw_fallback UInt8 DEFAULT 0,
        source_tables Array (String) DEFAULT [],
        json_paths Array (String) DEFAULT [],
        filter_paths Array (String) DEFAULT [],
        group_by_paths Array (String) DEFAULT [],
        time_range_start Nullable (DateTime64 (3, 'UTC')) CODEC (Delta (8), ZSTD (1)),
        time_range_end Nullable (DateTime64 (3, 'UTC')) CODEC (Delta (8), ZSTD (1)),
        result_rows UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        read_rows UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        read_bytes UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        elapsed_ms UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        status LowCardinality (String) DEFAULT 'ok',
        error String DEFAULT '' CODEC (ZSTD (3)),
        observed_at DateTime64 (3, 'UTC') DEFAULT now64 (3) CODEC (Delta (8), ZSTD (1)),
        attributes JSON (max_dynamic_paths = 256, max_dynamic_types = 8)
    ) ENGINE = MergeTree
PARTITION BY
    toYYYYMMDD (observed_at)
ORDER BY
    (tenant_id, query_hash, observed_at, query_id);

CREATE TABLE
    IF NOT EXISTS observatory.materialization_jobs (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        job_id String CODEC (ZSTD (1)),
        job_kind LowCardinality (String),
        status LowCardinality (String) DEFAULT 'pending',
        priority UInt8 DEFAULT 50,
        target_type LowCardinality (String),
        target_table LowCardinality (String),
        target_id String CODEC (ZSTD (1)),
        target_version UInt64 DEFAULT 0,
        source_table LowCardinality (String) DEFAULT 'events',
        source_start DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        source_end DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        chunk_seconds UInt32 DEFAULT 3600,
        total_chunks UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        completed_chunks UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        failed_chunks UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        rows_scanned UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        rows_written UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        bytes_scanned UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        bytes_written UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        lease_owner String DEFAULT '' CODEC (ZSTD (1)),
        leased_until Nullable (DateTime64 (3, 'UTC')) CODEC (Delta (8), ZSTD (1)),
        attempt UInt32 DEFAULT 0,
        max_attempts UInt32 DEFAULT 5,
        error String DEFAULT '' CODEC (ZSTD (3)),
        config JSON (max_dynamic_paths = 1024, max_dynamic_types = 8),
        created_at DateTime64 (3, 'UTC') DEFAULT now64 (3),
        updated_at DateTime64 (3, 'UTC') DEFAULT now64 (3),
        completed_at Nullable (DateTime64 (3, 'UTC')) CODEC (Delta (8), ZSTD (1))
    ) ENGINE = ReplacingMergeTree (updated_at)
ORDER BY
    (tenant_id, status, priority, job_kind, job_id);

CREATE TABLE
    IF NOT EXISTS observatory.materialization_chunks (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        job_id String CODEC (ZSTD (1)),
        chunk_id String CODEC (ZSTD (1)),
        chunk_index UInt64 CODEC (Delta, ZSTD (1)),
        status LowCardinality (String) DEFAULT 'pending',
        target_type LowCardinality (String),
        target_table LowCardinality (String),
        target_id String CODEC (ZSTD (1)),
        target_version UInt64 DEFAULT 0,
        source_table LowCardinality (String) DEFAULT 'events',
        source_start DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        source_end DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        rows_scanned UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        rows_written UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        bytes_scanned UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        bytes_written UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        lease_owner String DEFAULT '' CODEC (ZSTD (1)),
        leased_until Nullable (DateTime64 (3, 'UTC')) CODEC (Delta (8), ZSTD (1)),
        attempt UInt32 DEFAULT 0,
        max_attempts UInt32 DEFAULT 5,
        error String DEFAULT '' CODEC (ZSTD (3)),
        started_at Nullable (DateTime64 (3, 'UTC')) CODEC (Delta (8), ZSTD (1)),
        updated_at DateTime64 (3, 'UTC') DEFAULT now64 (3),
        completed_at Nullable (DateTime64 (3, 'UTC')) CODEC (Delta (8), ZSTD (1)),
        attributes JSON (max_dynamic_paths = 256, max_dynamic_types = 8)
    ) ENGINE = ReplacingMergeTree (updated_at)
PARTITION BY
    toYYYYMM (source_start)
ORDER BY
    (tenant_id, status, job_id, chunk_index, chunk_id);

CREATE TABLE
    IF NOT EXISTS observatory.materialization_versions (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        target_type LowCardinality (String),
        target_id String CODEC (ZSTD (1)),
        target_version UInt64 DEFAULT 0,
        status LowCardinality (String) DEFAULT 'building',
        active UInt8 DEFAULT 0,
        source_start DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        source_end DateTime64 (3, 'UTC') CODEC (Delta (8), ZSTD (1)),
        row_count UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        chunk_count UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        config_hash UInt64 DEFAULT 0,
        config JSON (max_dynamic_paths = 1024, max_dynamic_types = 8),
        stats JSON (max_dynamic_paths = 512, max_dynamic_types = 8),
        created_at DateTime64 (3, 'UTC') DEFAULT now64 (3),
        updated_at DateTime64 (3, 'UTC') DEFAULT now64 (3),
        completed_at Nullable (DateTime64 (3, 'UTC')) CODEC (Delta (8), ZSTD (1))
    ) ENGINE = ReplacingMergeTree (updated_at)
ORDER BY
    (tenant_id, target_type, target_id, target_version, status);

CREATE TABLE
    IF NOT EXISTS observatory.materialization_watermarks (
        tenant_id LowCardinality (String) CODEC (ZSTD (1)),
        target_type LowCardinality (String),
        target_id String CODEC (ZSTD (1)),
        target_version UInt64 DEFAULT 0,
        source_table LowCardinality (String) DEFAULT 'events',
        low_watermark Nullable (DateTime64 (3, 'UTC')) CODEC (Delta (8), ZSTD (1)),
        high_watermark Nullable (DateTime64 (3, 'UTC')) CODEC (Delta (8), ZSTD (1)),
        status LowCardinality (String) DEFAULT 'active',
        lag_ms UInt64 DEFAULT 0 CODEC (Delta, ZSTD (1)),
        updated_at DateTime64 (3, 'UTC') DEFAULT now64 (3),
        attributes JSON (max_dynamic_paths = 256, max_dynamic_types = 8)
    ) ENGINE = ReplacingMergeTree (updated_at)
ORDER BY
    (tenant_id, target_type, target_id, target_version);

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

CREATE TABLE
    IF NOT EXISTS observatory.lakehouse_commits (
        namespace LowCardinality (String),
        table_name LowCardinality (String),
        snapshot_id String CODEC (ZSTD (1)),
        sequence_number UInt64 CODEC (Delta, ZSTD (1)),
        committed_at_ms UInt64 CODEC (Delta, ZSTD (1)),
        data_file String CODEC (ZSTD (1)),
        data_files Array (String) DEFAULT [] CODEC (ZSTD (1)),
        record_count UInt64 CODEC (Delta, ZSTD (1)),
        content_sha256 FixedString (64),
        metadata_location String DEFAULT '' CODEC (ZSTD (1)),
        source_batch_id String DEFAULT '' CODEC (ZSTD (1)),
        deduplicated UInt8 DEFAULT 0 CODEC (T64, ZSTD (1)),
        loaded_at DateTime64 (3, 'UTC') DEFAULT now64 (3)
    ) ENGINE = ReplacingMergeTree (loaded_at)
ORDER BY
    (namespace, table_name, sequence_number, snapshot_id);

CREATE TABLE
    IF NOT EXISTS observatory.serving_watermarks (
        serving_table LowCardinality (String),
        source_namespace LowCardinality (String),
        source_table LowCardinality (String),
        source_snapshot_id String CODEC (ZSTD (1)),
        source_sequence_number UInt64 CODEC (Delta, ZSTD (1)),
        source_record_count UInt64 CODEC (Delta, ZSTD (1)),
        status LowCardinality (String) DEFAULT 'loaded',
        updated_at DateTime64 (3, 'UTC') DEFAULT now64 (3),
        attributes JSON (max_dynamic_paths = 128, max_dynamic_types = 8)
    ) ENGINE = ReplacingMergeTree (source_sequence_number)
ORDER BY
    (serving_table, source_namespace, source_table);

ALTER TABLE observatory.report_results
    ADD COLUMN IF NOT EXISTS dimensions_hash UInt64 MATERIALIZED cityHash64 (toJSONString (dimensions)) AFTER dimensions;

ALTER TABLE observatory.sequence_report_results
    ADD COLUMN IF NOT EXISTS segment JSON (max_dynamic_paths = 128, max_dynamic_types = 8) AFTER bucket_time;

ALTER TABLE observatory.sequence_report_results
    ADD COLUMN IF NOT EXISTS segment_hash UInt64 MATERIALIZED cityHash64 (toJSONString (segment)) AFTER segment;

ALTER TABLE observatory.definition_stats
    DROP COLUMN IF EXISTS estimated_rows_per_sec;

ALTER TABLE observatory.definition_stats
    DROP COLUMN IF EXISTS estimated_storage_bytes_per_day;

ALTER TABLE observatory.definition_stats
    DROP COLUMN IF EXISTS cardinality_class;

ALTER TABLE observatory.definition_stats
    DROP COLUMN IF EXISTS warnings;

DROP TABLE IF EXISTS observatory.optimization_recommendations;
