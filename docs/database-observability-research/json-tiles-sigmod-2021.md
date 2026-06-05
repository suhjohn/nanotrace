# JSON Tiles: Fast Analytics on Semi-Structured Data

- Source: https://portal.fis.tum.de/en/publications/json-tiles-fast-analytics-on-semi-structured-data/ ; https://doi.org/10.1145/3448016.3452809 ; https://db.cs.tum.edu/~durner/papers/json-tiles-sigmod21.pdf
- Type: paper
- Year: 2021
- Authors/Org: Dominik Durner, Viktor Leis, Thomas Neumann

## Problem

JSON is convenient for logs, APIs, and evolving application data, but analytics over JSON inside relational systems is slow when each row is stored as opaque text or per-document binary JSON. Existing approaches either parse too much at query time, require a mostly static global schema, or shred records in a way that remains CPU-heavy for optional and heterogeneous fields.

The paper targets the middle ground: preserve JSON flexibility while making common analytical access paths behave like typed relational columns.

## System / Architecture

JSON Tiles is integrated into the Umbra relational DBMS. It changes loading, storage, scan operators, expression pushdown, cast handling, optimizer statistics, and update behavior.

The main design is local schema discovery. Instead of finding one table-wide schema, the system breaks incoming JSON rows into disjoint "tiles" of hundreds or thousands of documents, detects frequent key paths inside each tile, materializes those paths as column chunks, and leaves the remaining data in an optimized binary JSON fallback.

## Storage Model

Each tile stores:

- Materialized typed column chunks for locally frequent key paths.
- A tile header describing extracted paths, value types, nullability, and path availability.
- A binary JSON representation for infrequent keys, outlier values, or values whose type does not match the extracted column type.
- Tile-level statistics that are later aggregated into relation-level optimizer inputs.

Key paths encode nested object and array structure. Type is part of the extraction identity, so the same logical key with different primitive JSON types can be handled without losing JSON semantics.

## Ingest / Write Path

During bulk load, JSON Tiles collects key paths per tuple and runs frequent itemset mining to find common structures. It extracts the union of maximum frequent itemsets above a threshold. The authors recommend a tile size of 2^10 and partition size 8 after evaluating performance and load overhead.

For workloads whose insertion order mixes document types, the system reorders tuples between neighboring tiles. It mines itemsets at a reduced threshold across a partition of tiles, groups records by the best matching itemset, and swaps tuples so each final tile has stronger local structure. This improves extraction when the data lacks natural locality.

## Query / Index Model

Query speed depends on pushing JSON access expressions into table scans. The optimizer replaces higher-level JSON access expressions with placeholders so the scan operator can decide per tile whether a requested path has a materialized column or must use binary JSON fallback.

Cast rewriting is important. A common expression such as `data->>'key'::Int` is rewritten so the scan can return a typed value directly instead of materializing text and then casting. Tile headers are cached for a scan so path availability is computed once per tile rather than once per tuple.

JSON Tiles also feeds the optimizer with path-aware statistics. It uses frequency counters for key existence and HyperLogLog sketches for value-domain estimates, allowing better filter selectivity and join ordering than opaque JSON columns.

## Compaction / Materialization / Evolution

Materialization is per tile, so schema evolution is natural: new fields can become extracted in future tiles, and removed fields stop being extracted in later tiles. This makes the system robust to APIs and logs whose fields change over time.

Updates can modify extracted column values directly when the changed key already exists in the tile. If a new key path is introduced, the tile header's availability metadata must be updated so tile skipping remains correct. Full tile recomputation is reserved for cases where many outlier documents no longer match the tile's extracted structure.

## Relevance To Event-Native Analytics And Observability

The paper is highly relevant to observability data because logs and traces often have recurring structure without a stable declared schema. JSON Tiles suggests a storage design where common event attributes become cheap column scans, while rare payload fields remain queryable through fallback storage.

The local tile idea is especially applicable to event streams with temporal schema locality: a service version, deployment, or instrumentation change can create a period where fields are consistent, even if the whole corpus is heterogeneous.

## Tradeoffs And Limitations

JSON Tiles is deeply integrated into the DBMS; it is not just a file format. Systems without control over scan planning, expression pushdown, and optimizer statistics would get less benefit.

Frequent itemset mining can be expensive, so the implementation bounds mining work and accepts approximate materialization quality. High-cardinality arrays are only partly handled by tile extraction; the paper suggests detecting such arrays and extracting them into separate tables.

The system optimizes common keys, not arbitrary ad hoc access. Rare fields still require binary JSON lookup, although the fallback format is designed for efficient object and array access.

## Notable Details

- The binary JSON fallback stores sorted object keys, giving logarithmic key lookup and contiguous traversal for nested values.
- Tile skipping is only valid when query semantics allow absent paths to be treated as skipped nulls; the system tracks whether nulls are skipped or evaluated as false.
- Date and time values can be inferred from strings and stored as timestamps when query casts make exact original string reconstruction unnecessary.
- Experiments compare against PostgreSQL JSONB, Spark/Parquet, Spark/MongoDB, Hyper, Sinew, raw JSON, and the authors' binary JSON format.
- The evaluation reports order-of-magnitude speedups on imperfect and combined workloads while adding minimal overhead on well-structured data.
