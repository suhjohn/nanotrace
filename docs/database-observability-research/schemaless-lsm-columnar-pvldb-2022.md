# Columnar Formats for Schemaless LSM-based Document Stores

- Source: https://www.vldb.org/pvldb/vol15/p2085-alkowaileet.pdf
- Type: paper
- Year: 2022
- Authors/Org: Wail Y. Alkowaileet, Michael J. Carey

## Problem

Document stores give users flexible, schemaless ingestion, but analytical workloads suffer because records are stored in row-major formats and each record carries structural metadata. Parquet and Dremel are effective for nested data when schema is known, but they assume a declared schema and homogeneous field types, which does not fit document stores where fields can appear, disappear, or change type at runtime.

The paper asks how to bring columnar analytics to schemaless, LSM-based document stores without forcing users to predeclare schema.

## System / Architecture

The implementation is in Apache AsterixDB, an LSM-based, shared-nothing semi-structured DBMS. The design builds on an existing tuple compaction framework that infers schema during LSM flushes.

The paper contributes two major pieces:

- An extended Dremel representation that supports schema changes, heterogeneous values, union types, array delimiters, and LSM anti-matter tuples.
- AMAX, a columnar page layout for storing inferred columns inside LSM B+ tree leaf nodes.

It also explores query compilation using Oracle Truffle to reduce CPU overhead when executing dynamically typed document queries.

## Storage Model

AMAX stretches B+ tree leaf nodes into multi-page "mega leaf nodes." Page 0 stores metadata, min/max-style prefixes, and primary keys; each following "megapage" corresponds to a column, while small columns may share physical pages.

Columns store definition levels and values. The extended Dremel format replaces repetition levels with array delimiter levels, using selected definition-level values to mark repeated-value boundaries. Heterogeneous field types are represented with union nodes, physically storing each possible atomic type in separate columns.

Primary keys use definition levels to represent LSM anti-matter entries, so deletes can participate in the same columnar representation.

## Ingest / Write Path

Records first enter the in-memory LSM component in a row-major vector-based format. When the component flushes, the system infers the component schema, splits records into columns, writes AMAX pages, and persists the inferred schema in component metadata.

The ingest path piggybacks on LSM lifecycle events rather than requiring a separate batch conversion step. This is important because it lets the document store continue accepting schemaless writes while producing columnar on-disk components.

Secondary indexes complicate ingestion. Upserts and deletes require point lookups in the primary index to maintain secondary index correctness. Point lookups are more expensive against AMAX than row-major storage, so the authors add a primary-key index to avoid reading the columnar primary index when the key does not exist.

## Query / Index Model

Before execution, each partition consults the inferred schema to identify only the columns required by the query. AMAX reads those megapages rather than whole records. Prefix metadata can skip mega leaf nodes that cannot satisfy predicates.

The system presents an abstract tuple view over row-major in-memory components and column-major on-disk components, allowing the LSM reconciliation process to handle updates and deletes. During reconciliation, it decodes only primary keys and lazily advances other column iterators in batches, avoiding wasted decoding for records later shadowed by newer versions.

For secondary-index range queries, the secondary index yields sorted primary keys, and point lookups can retrieve matching records. The experiments show indexes help AMAX when queries access multiple columns or have selective predicates.

## Compaction / Materialization / Evolution

LSM merges must merge both keys and column values. A naive merge would touch many memory regions and decode many columns. The paper introduces a vertical merge: first merge primary keys and record the source component sequence, then merge one column at a time according to that sequence. This keeps memory access bounded by the number of components instead of number of components times number of columns.

The authors also limit concurrent AMAX merges because decoding and encoding columns can consume significant CPU during compaction. This is a direct tradeoff between ingest stability, query capacity, and compaction throughput.

Schema evolution is component-local. Newly observed fields and type variants are added to inferred schemas during flushes; union columns allow heterogeneous values without rewriting older immutable LSM components.

## Relevance To Event-Native Analytics And Observability

The paper is relevant to event-native observability because it treats schema as something learned at write-boundary time, not declared up front. That fits telemetry, logs, traces, and operational events where fields evolve with deployments and instrumentation changes.

AMAX also shows how an LSM write path can produce analytical storage without abandoning high-ingest document-store behavior. The vertical merge idea is directly relevant to observability stores that need background compaction without destroying query latency.

## Tradeoffs And Limitations

AMAX can reduce I/O substantially, but CPU becomes the next bottleneck. Query execution that eagerly reassembles row-shaped records can erase columnar gains, which is why the paper adds code generation. The code generation prototype only covers pipelining operators and not full pipeline breakers such as group-by internals.

Update-heavy workloads with secondary indexes are harder for columnar layout. In the paper's stress test with 50% random updates, AMAX ingestion was about 35% slower than the row-major Open format because maintaining secondary indexes required expensive point lookups and decoding.

Very wide datasets increase flush and merge costs because many columns must be transformed and merged. Dictionary encoding is left as future work.

## Notable Details

- The artifact is available at https://github.com/wailyk/column.
- The evaluation uses real, scaled, and synthetic datasets including telecom cell records, sensors, tweets, and Web of Science metadata.
- Numeric sensor data benefits strongly from column encoding; large textual datasets see smaller storage savings but still benefit from column pruning.
- For one Twitter query, AMAX ran in 3.1 seconds versus 48.5 seconds for AsterixDB's Open format and 39.9 seconds for the vector-based row format.
- The paper explicitly positions itself against making users give up schemaless flexibility in exchange for analytical performance.
