# Dremel: Interactive Analysis of Web-Scale Datasets and a Decade of Interactive SQL Analysis

- Source: https://research.google.com/pubs/archive/36632.pdf and https://research.google/pubs/dremel-a-decade-of-interactive-sql-analysis-at-web-scale/
- Type: paper
- Year: 2010 and 2020
- Authors/Org: 2010: Sergey Melnik, Andrey Gubarev, Jing Jing Long, Geoffrey Romer, Shiva Shivakumar, Matt Tolton, Theo Vassilakis; Google. 2020: original authors plus Hossein Ahmadi, Dan Delorey, Slava Min, Mosha Pasumansky, Jeff Shute; Google.

## Problem

The 2010 Dremel paper targets interactive ad-hoc analysis over web-scale, read-only nested data. Google had enormous semi-structured datasets in distributed storage and needed analysts and engineers to query them in seconds rather than author and wait for MapReduce jobs. The system was designed to complement MapReduce, not replace it: MapReduce could produce datasets, and Dremel could inspect them interactively.

The 2020 decade paper reframes Dremel as an early foundation for cloud-native analytics: SQL over disaggregated storage, in situ analysis, nested columnar storage, and serverless multi-tenant execution.

## System / Architecture

The original Dremel architecture uses a multi-level serving tree. A root server receives a SQL-like query, reads table metadata, rewrites the query, and pushes fragments down a tree of intermediate and leaf servers. Leaves scan tablets from local disk or GFS. Intermediate nodes combine partial aggregates as results flow upward.

The serving tree borrows from search infrastructure and is optimized for one-pass aggregation over large fanout. A query dispatcher schedules work, handles priorities, balances load, retries slow tablets, and can return approximate answers after scanning a configured percentage of tablets.

The decade paper explains major evolution: Dremel moved from shared-nothing local disks to disaggregated storage in GFS/Colossus, added disaggregated memory shuffle, centralized scheduling, flexible execution DAGs, dynamic execution choices, and a slot abstraction that became central to BigQuery. The fixed aggregation tree worked for early queries, but joins and complex SQL required shuffle-backed stages.

## Storage Model

Dremel's core storage contribution is columnar encoding for nested records. It stores values for each leaf path contiguously while preserving enough structure to reconstruct records. The 2010 paper introduces repetition and definition levels: repetition records where a repeated field repeats, and definition records how much optional/repeated structure is present for null or missing values.

This allows Dremel to scan only selected nested fields, avoid expensive normalization, and operate on protocol-buffer-like records directly. The query engine can often avoid record assembly entirely by scanning columns in lockstep and emitting aggregates with the right repetition/definition levels.

The decade paper describes later evolution into Capacitor, BigQuery's columnar format. Capacitor retained the nested-columnar foundation but added embedded predicate evaluation, richer encodings, row reordering for compression, columnar schema representation, and a shift toward length/presence encodings that can be smaller than repetition/definition for deep and wide messages.

## Ingest / Write Path

The 2010 Dremel paper is mostly read-only and in situ. Users define a table over paths in distributed storage and query existing data without a database load step. Data is commonly produced by MapReduce, Sawzall, or other pipelines, then analyzed by Dremel.

The decade paper notes that Dremel originally required explicit loading into a proprietary format, but migration to GFS and a shared self-describing columnar format opened Dremel data to other tools. Later BigQuery introduced managed storage because pure in situ analysis leaves users responsible for data governance, layout, statistics, updates, and schema changes.

## Query / Index Model

Dremel's SQL-like language supports projection, selection, nested subqueries, intra-record and inter-record aggregation, top-k, joins, and UDFs, though the 2010 paper focuses on one-pass aggregations. Nested path expressions and `WITHIN` aggregation let users compute within repeated subrecords without flattening all data.

The execution model is scan-heavy and metadata-driven rather than secondary-index-heavy. It depends on parallel scans, column pruning, local prefetch, scheduling, and aggregate combination. Some queries return approximate results using one-pass algorithms, and users can trade completeness for latency by scanning less than 100% of tablets.

In the BigQuery-era system, shuffle persistence enables joins, analytic functions, dynamic query execution, and stage DAGs. Dremel can begin with one join strategy and change when runtime statistics show a better path.

## Compaction / Materialization / Evolution

The original paper does not present compaction as a write-path concept; its concern is file/tablet storage and efficient scan execution. Evolution happens through the broader ecosystem: MapReduce produces data, Dremel reads it, and later managed storage optimizes layout and statistics.

The decade paper's most important evolution is architectural, not LSM-like compaction: disaggregated storage, self-describing formats, disaggregated shuffle, serverless slots, common GoogleSQL, and Capacitor. Materialization also appears through shuffle checkpoints, final result storage in the shuffle layer, and managed BigQuery storage that can maintain statistics and support DML/DDL where standalone files cannot.

## Relevance To Event-Native Analytics And Observability

Dremel is relevant to observability as the archetype of interactive scan analytics over huge nested event records. Logs, traces, spans, and structured telemetry naturally resemble nested records, and Dremel shows how to keep that structure without normalizing into many join-heavy tables.

For observability design, the key lessons are columnar nested storage, column pruning, in situ analysis over lake data, and fanout execution with straggler mitigation. The decade paper adds the modern lesson: usable telemetry analytics requires both raw lake access and managed storage. Raw files are flexible, but managed storage supplies governance, statistics, updates, schema evolution, and predictable latency.

## Tradeoffs And Limitations

The 2010 system is read-only and initially optimized for aggregations returning small or medium results. Large joins, updates, and complex warehouse features were either future work or later additions. In situ analysis reduces load friction but loses opportunities for layout tuning and statistics when data is first seen at query time.

The nested encoding also has tradeoffs. Repetition/definition levels make each leaf self-describing, but duplicate ancestor-structure information across children. Later formats found smaller representations for some deep/wide schemas.

Disaggregated storage improves elasticity and data sharing but makes latency harder: file opens, metadata calls, and network reads become part of every query. The decade paper emphasizes that Dremel needed storage-format tuning, metadata reuse, prefetching, affinity, and scheduling work to make disaggregation fast enough.

## Notable Details

- The 2010 paper reports aggregation queries over trillion-row tables in seconds and production use at thousands of Google users.
- Dremel's 2010 experiments show near-linear scaling from 1,000 to 4,000 nodes on a trillion-row table.
- The decade paper says Dremel's in-memory shuffle reduced shuffle latency by an order of magnitude and enabled an order of magnitude larger shuffles.
- BigQuery's slot abstraction descends from Dremel's virtual scheduling units.
- Dremel influenced Parquet and other nested columnar formats.
