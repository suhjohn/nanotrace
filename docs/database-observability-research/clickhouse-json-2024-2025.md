# ClickHouse JSON Type Design And Shared Data Improvements

- Source: https://clickhouse.com/blog/a-new-powerful-json-data-type-for-clickhouse/ and https://clickhouse.com/blog/json-data-type-gets-even-better
- Type: posts
- Year: 2024, 2026
- Authors/Org: Pavel Kruglov / ClickHouse

## Problem

The JSON posts address semi-structured and unstructured analytical data at scale. JSON is common in logs, observability, streaming, mobile, and ML pipelines, but naive storage as strings forces repeated parsing and prevents columnar pruning. Naively turning each JSON path into a physical column creates too many files for high-path-cardinality payloads. Sparse paths also risk storing many null/default values.

The design goal is to keep JSON flexible while preserving ClickHouse's core advantages: dense columnar storage, compression, vectorized filters and aggregations, bounded file counts, and efficient selective reads.

## System / Architecture

The 2024 JSON type is built from two lower-level types:

- `Variant`, which stores values of multiple known concrete types in one logical column without forcing them into a least-common type.
- `Dynamic`, which extends `Variant` by allowing unknown types and limiting how many concrete types become separate subcolumns.

The JSON type uses these primitives to expose JSON paths as subcolumns. Type hints, skip hints, maximum dynamic paths, and maximum dynamic types allow users to steer storage and performance.

The later shared-data improvements target cases where a JSON object has more unique paths than the dynamic-path limit. Instead of requiring ClickHouse to read a single large shared map for every overflow path lookup, the new serializations make shared data more selectively readable.

## Storage Model

`Variant` stores each concrete type in a dense subcolumn and stores a discriminator file that records which type appears in each table row. Discriminator value 255 is reserved for `NULL`, so a variant can have up to 255 concrete types. Dense subcolumns avoid storing nulls for absent values.

`Dynamic` stores values similarly but adds metadata describing the types present in a part. It has `max_types`, defaulting to 32; once the limit is exceeded, additional types go into a shared variant representation.

The JSON type stores typed hinted paths as regular typed subcolumns and dynamic paths as `Dynamic` subcolumns. It uses metadata such as `object_structure` to track dynamic paths and non-null statistics. `max_dynamic_paths` defaults to 1024; paths beyond the limit are stored as shared data.

In v25.8-era improvements, shared data can be bucketed or advanced. Bucketed shared data splits the path/value map into deterministic path buckets. Advanced shared data adds per-granule structure, path marks, substream marks, and metadata so ClickHouse can skip granules or substreams that do not contain the requested path.

## Ingest / Write Path

During JSON parsing and insert, ClickHouse discovers JSON paths, applies type and skip hints, assigns paths to typed, dynamic, or shared storage, and writes dense columnar files inside MergeTree parts. The system avoids requiring users to predeclare every path, but it provides limits to prevent file-count explosions.

New small parts may use simpler layouts initially. As with other MergeTree storage, later merges reconcile part metadata and storage layouts. For shared-data improvements, compatibility copies can be stored so whole-object reads and merges remain efficient even when the advanced selective-read layout is present.

## Query / Index Model

Users can read JSON paths as subcolumns, such as `json_col.a.b`. Untyped paths return `Dynamic`; users can read a concrete dynamic subtype with syntax like `json_col.path.:Int64`. Nested objects can be read as JSON with the `.^` syntax, which is distinct because reconstructing objects can require reading more data than scalar path reads.

The advanced shared-data serialization improves selective reads by checking whether a granule contains the requested path before reading path data. For nested arrays and objects, substream metadata lets ClickHouse read only the substreams needed to reconstruct the requested subcolumn.

The 2026 post reports a single-key read test over 200k rows with 10,000 paths per document. Advanced shared data took 0.063 seconds and 3.89 MiB, compared with 3.63 seconds and 12.53 GiB for the older shared map representation. Full JSON reads remained close to the older representation because a compatibility copy preserves efficient whole-object access.

## Compaction / Materialization / Evolution

The main evolution mechanism is bounded promotion of paths into subcolumns. Users can tune `max_dynamic_paths` and `max_dynamic_types`, add explicit type hints for important paths, and skip expensive or irrelevant paths. Merges use JSON structure metadata to combine parts with different discovered paths and dynamic types.

The advanced shared-data layout is also an evolution in materialization strategy: it gives overflow paths a quasi-columnar, selectively readable layout without materializing every unique path as a separate file.

## Relevance To Event-Native Analytics And Observability

Observability data often arrives as JSON with changing schemas, nested attributes, and high path cardinality. The JSON type directly addresses the tension between raw flexible ingestion and fast analytical reads. It can keep raw payload shape available while making common scalar paths behave more like regular columns.

For event-native analytics, this suggests a tiered strategy: typed columns for stable high-value dimensions, JSON subcolumns for semi-structured fields, and advanced shared data for the long tail of attributes. This preserves drilldown without turning every possible attribute into a top-level schema decision.

## Tradeoffs And Limitations

The JSON type is more complex than storing strings or maps. Limits such as `max_dynamic_paths` and `max_dynamic_types` are necessary and workload-sensitive. Raising them too high, especially with remote storage such as S3, can reintroduce file and merge overhead.

Advanced shared data optimizes selective reads but has costs for merges and full-column reads. The compatibility-copy approach avoids much of that runtime penalty but can roughly double storage for the shared-data portion. Reading nested objects can still require more data than reading scalar paths.

## Notable Details

- The 2024 post describes JSON as a first-class columnar type rather than a parsed string convention.
- `SKIP` and `SKIP REGEXP` hints let users avoid storing noisy paths.
- `Dynamic(max_types=N)` defaults to 32 and must remain below 255.
- Advanced shared data tracks paths per granule, not just per part.
- The improvement material extends the 2024 JSON redesign with later shared-data optimizations in ClickHouse v25.8.
