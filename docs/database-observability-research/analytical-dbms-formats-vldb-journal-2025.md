# Data Formats In Analytical DBMSs: Performance Trade-Offs And Future Directions

- Source: https://link.springer.com/article/10.1007/s00778-025-00911-1; https://link.springer.com/content/pdf/10.1007/s00778-025-00911-1.pdf
- Type: paper
- Year: 2025
- Authors/Org: Chunwei Liu, Anna Pavlenko, Matteo Interlandi, Brandon Haynes; MIT CSAIL and Microsoft

## Problem

The paper evaluates whether Apache Arrow, Parquet, and ORC are suitable as native analytical DBMS formats rather than just interchange or storage formats. Open formats improve interoperability, but their design choices can conflict with classic column-store execution techniques such as lightweight encoding, compressed-domain evaluation, vectorization, and query compilation.

The authors focus on three tensions: compression ratio versus decompression speed, in-memory representation versus on-disk layout, and general interoperability versus workload-specific DBMS optimization.

## System / Architecture

The paper is an empirical format study, not a new system. It defines a common columnar architecture: a table is split into row batches, then each batch is split into column chunks. Metadata describes row batch locations, lengths, encodings, compression algorithms, and statistics.

It then compares:

- Arrow and Feather for in-memory and serialized Arrow-style representation.
- Parquet as an on-disk storage format with pages, dictionaries, encodings, and footer statistics.
- ORC as an analytical storage format with indexes, row data streams, and an associated in-memory representation.

## Storage Model

All three are columnar, but their storage assumptions differ.

Arrow is optimized for in-memory access and cheap deserialization, with contiguous arrays and validity bitmaps. Feather serializes Arrow IPC data to disk and adds compression options. Parquet stores column chunks as dictionary and data pages, with page and row-group metadata. ORC stores index streams and row data streams, with min/max values, bloom filters, and present bitmaps.

Parquet and ORC provide richer compression and encoding support than Arrow/Feather. Arrow is simpler to access in memory but lacks the encoded representation that many high-performance analytical engines depend on.

## Ingest / Write Path

The write path is framed as serialization, encoding, compression, and transcoding. A DBMS must decide whether to write a format that is cheap to ingest, cheap to scan, compact on disk, or cheap to convert into an execution representation.

The paper highlights that transcoding is not incidental. Common pipelines read Parquet from storage and convert to Arrow in memory, but that can throw away Parquet's encoded structure and impose CPU cost before query execution.

## Query / Index Model

The paper studies select-project operations near the leaves of query plans, combined query fragments, vectorized execution, direct querying, and data skipping. Parquet and ORC expose statistics and encodings that support skipping and compressed representations. Arrow provides efficient random access and iteration but, by default, little encoded-domain query support.

The authors emphasize that none of the formats fully supports direct querying in the strongest DBMS sense. They are uneven in their ability to support SIMD, query compilation, skipping granularity, and compressed-domain execution.

## Compaction / Materialization / Evolution

This paper does not discuss lakehouse compaction directly. Its closest analog is physical materialization: when and how data is converted among serialized, compressed, encoded, and in-memory forms.

The future direction is a unified representation that co-designs in-memory and on-disk layouts. Such a representation would avoid repeated conversion between Parquet and Arrow, preserve useful encodings into execution, and still support open interoperability.

## Relevance To Event-Native Analytics And Observability

Telemetry analytics is dominated by scans, selective filters, high-cardinality strings, nested attributes, and increasingly vector-like payloads for logs, embeddings, or AI-assisted search. This paper is relevant because it shows that "Parquet versus Arrow" is not a simple storage choice.

An event-native system needs:

- Encodings and statistics that support fast filters over time, service, tenant, trace, and attribute columns.
- A memory format that does not discard useful compressed or dictionary-encoded structure.
- Efficient handling of nested vectors and embeddings if observability data feeds RAG or similarity search workflows.

The paper's main warning is that interoperability formats are not automatically ideal internal DBMS formats.

## Tradeoffs And Limitations

The evaluation focuses on Arrow, Parquet, and ORC because they are widely adopted. It explicitly does not exhaustively evaluate newer or domain-specific formats such as Lance, Vortex, Nimble, HDF5, or ML-focused stores.

The results are format- and implementation-dependent, and workload-specific layout methods such as learned partitioning or workload-driven clustering are outside the main scope. The paper also notes that popular machine-learning vector tasks are poorly served by all three studied formats.

## Notable Details

- Parquet row groups are recommended at hundreds of MB, while Arrow row batches default much smaller.
- Parquet's footer includes zone maps such as min/max and null counts; ORC includes index structures and can include bloom filters.
- Arrow deserialization can be effectively zero-copy, but Arrow's default lack of encoding is a mismatch with many DBMS internals.
- The conclusion calls for holistic co-design of a unified in-memory and on-disk representation for modern OLAP systems.
