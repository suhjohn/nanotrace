# Towards Observation Lakehouses: Living, Interactive Archives of Software Behavior

- Source: https://arxiv.org/abs/2512.02795 ; https://arxiv.org/pdf/2512.02795 ; https://github.com/SoftwareObservatorium/observation-lakehouse
- Type: paper
- Year: 2025 (arXiv v2 dated 2026-01-16)
- Authors/Org: Marcus Kessel, University of Mannheim

## Problem

The paper argues that code-generating LLMs learn mostly from static artifacts rather than trustworthy runtime behavior. Unit tests usually preserve only pass/fail outcomes, while tracing captures too much low-level execution state and hides functional responses. Prior structures such as Sequence Sheets, Stimulus-Response Matrices, and Stimulus-Response Cubes made behavior analyzable offline, but lacked persistence, schema evolution, and interactive querying at scale.

The proposed problem is to make runtime software behavior a first-class analytical dataset.

## System / Architecture

The Observation Lakehouse is a Python application built on Apache Parquet, Apache Iceberg, DuckDB, PyIceberg, PyArrow, and Arrow. It uses a data lakehouse stack rather than a conventional data lake or traditional RDBMS.

The architecture unifies controlled experiment data from LASSO and practitioner data from CI pipelines. It stores observations in append-only Iceberg tables and reconstructs logical SRM/SRC views with SQL on demand.

## Storage Model

The logical SRC is represented as three Iceberg tables:

- `code_implementations`, storing source code and static metrics once per implementation.
- `tests`, storing the stimulus definition once per test.
- `observations`, the fact table storing each invocation step with operation, inputs, outputs, and execution context.

Two global identifiers, `data_set_id` and `problem_id`, unify experimental and practitioner sources. The physical partitioning key is `(data_set_id, problem_id)`, colocating all rows needed to reconstruct one logical SRM.

The core atomic record is an invocation step: an operation, inputs, outputs, step order, implementation, test, and context. Per-run metrics can be denormalized onto each step record because Parquet compression makes repeated values cheap.

## Ingest / Write Path

New observations are appended as execution records, preserving continual SRC behavior. New tests add rows to a logical SRM; new implementations add columns; new contexts and metrics become dimensions.

The paper describes two ingestion streams: controlled software experiments and CI/unit-test executions. It transforms sequence sheets or mined unit-test methods into flat invocation step records. In the evaluation, the system bulk-imports code implementations and tests, then streams observation records in batches using a Python worker with DuckDB and PyArrow.

## Query / Index Model

The main query interface is SQL through DuckDB. Analysts reconstruct SRM output views, join observations to tests and implementations, and compute behavioral clusters.

The partitioning strategy is the central "index" mechanism: queries for one problem prune unrelated Parquet files and read only the relevant partition. DuckDB's vectorized execution and Arrow integration provide interactive local performance.

The paper reports three representative workloads:

- SRM output view reconstruction with about 52 ms average latency.
- Column-wise behavioral clustering with about 29 ms average latency.
- Full three-table SRM reconstruction join with about 90 ms average latency.

## Compaction / Materialization / Evolution

The design avoids pre-materializing every SRM/SRC view. It stores a tall append-only observation table and materializes SRC slices on demand with SQL.

Iceberg supplies ACID commits, snapshot isolation, time travel, and schema evolution without rewriting existing files. That matters because behavioral dimensions evolve: new tests, implementations, environments, code hashes, measurements, and metadata can appear later.

Future evolution includes physical partitioning changes, semantic search, federation across lakehouses, and an MCP server for augmented LLM/agent use.

## Relevance To Event-Native Analytics And Observability

This is directly event-native: every invocation step is an observed event with stimulus, response, and context. The lakehouse turns those events into behavioral analytics, clustering, consensus oracles, n-version assessment, and semantic drift analysis.

For observability, the paper's key move is to preserve functional behavior rather than only traces or pass/fail outcomes. It also demonstrates that a lakehouse table model can retain raw observations while letting analysts build matrix/cube views on demand.

## Tradeoffs And Limitations

The current evaluation is a preliminary laptop-scale benchmark, not a production multi-tenant observability platform. The workload is derived from 509 coding problems and LASSO-generated observations; the paper lists real-world CI ingestion as future work.

Stimulus and response serialization is difficult. The current JSON-based approach is useful for polyglot interoperability but does not fully solve robust equivalence for complex values, exceptions, streams, or binary data. The author suggests content-based hashing, specialized handlers, and SQL user-defined equivalence functions.

The model is optimized around reconstructing behavior by problem/function. Different partitioning may be needed for cross-repository, time-range, organization-wide, or incident-style observability queries.

## Notable Details

- The evaluation ingests about 8.56 million observation rows from 95,154 call sequences, 13,384 implementations, and 509 problems.
- The observations table is reported as 50.9 MiB on disk, with total table size 69.4 MiB, roughly 8 MiB per million observations.
- Continuous observation ingestion reached about 155,000 records per second in the reported experiment.
- The paper explicitly positions testing as capturing too little and tracing as capturing too much.
- The open-source repository and dataset are linked from the paper.
