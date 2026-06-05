# Exploiting Cloud Object Storage For High-Performance Analytics

- Source: https://www.vldb.org/pvldb/vol16/p2769-durner.pdf
- Type: paper
- Year: 2023
- Authors/Org: Dominik Durner, Viktor Leis, Thomas Neumann; Technische Universitat Munchen

## Problem

The paper asks whether analytical DBMSs can query data directly from cloud object storage without relying on local SSD caches. Historically, object storage was considered too slow for large scans because public-cloud network bandwidth lagged local NVMe bandwidth and object requests had high latency.

The authors argue that high-bandwidth cloud instances changed the equation. The remaining challenges are saturating instance bandwidth despite per-request latency, reducing CPU overhead from HTTP/TLS/network retrieval, and supporting multiple object-store vendors without wiring each vendor SDK deeply into the DBMS.

## System / Architecture

The paper studies object-store behavior and presents AnyBlob, a multi-cloud download manager integrated into the Umbra analytical DBMS. The architecture treats object retrieval as part of the scan operator rather than an external file-system layer.

AnyBlob schedules many concurrent range/object requests, uses asynchronous I/O, and minimizes per-request thread overhead. Umbra's scan operator interleaves downloading with query processing so remote object reads can feed execution pipelines without waiting for whole-object staging.

## Storage Model

The object stores are conventional cloud storage services such as AWS S3, IBM COS, Google Cloud Storage, Azure Blob Storage, and OCI Object Storage. Objects are immutable blobs addressed by keys and served over HTTP/TCP.

The paper is not a table-format paper. It focuses on retrieval characteristics beneath formats such as Parquet or lakehouse tables. Storage cost is split into capacity, API request cost, and cross-region transfer; intra-region reads from compute to object storage usually avoid transfer charges but still pay request costs.

## Ingest / Write Path

The paper is mostly read-path focused, but the object-store model shapes writes. Objects are written as full objects, not updated in place, and PUT costs are size-independent at the request level. Analytical formats and lakehouse systems therefore need to write reasonably large objects and avoid excessive small-file creation.

For observability ingestion, this reinforces a core design constraint: the write path must balance freshness against object count, request overhead, and later scan efficiency.

## Query / Index Model

The query model is high-throughput object retrieval feeding analytical scans. To saturate 100 Gbit/s-class instances, the engine must issue many concurrent requests because individual object requests have nontrivial latency. The paper derives request-size and concurrency guidance from measurements, then integrates the scheduler into Umbra.

Indexes are not the focus. The implicit index boundary is that higher-level systems should use table metadata, partition pruning, and columnar file statistics to decide which ranges or objects to fetch; AnyBlob then fetches them efficiently.

## Compaction / Materialization / Evolution

The paper's materialization stance is anti-staging: do not require a local SSD cache before query execution if the network path can be made fast enough. That improves elasticity because compute instances can be replaced or scaled without cache warmup cliffs.

However, the results do not remove the need for table-level compaction. Object stores still prefer fewer, larger reads; lakehouse and event pipelines still need to avoid pathological small-file layouts.

## Relevance To Event-Native Analytics And Observability

Telemetry systems often choose between keeping hot data on local disks and querying colder data from object storage. This paper makes object storage a more credible primary analytical backing store, provided the DBMS owns retrieval scheduling deeply enough.

For observability, the useful insight is that object storage latency is not a reason by itself to abandon direct lakehouse querying. The harder requirement is a query engine that can:

- Plan enough useful object/range requests.
- Keep network retrieval from stealing CPU needed for scans and decompression.
- Avoid cache-dependent performance cliffs during autoscaling or failover.

## Tradeoffs And Limitations

The approach is most compelling for bandwidth-dominated analytical workloads. Very selective queries still depend heavily on metadata pruning and file layout, and very small reads can suffer from latency and request overhead.

The work assumes a DBMS integration that can manage asynchronous retrieval directly. Systems that access object storage through generic file-system adapters or poorly tuned vendor SDKs may not see the same performance. The paper also focuses on reads; transactional commit protocols and row-level lakehouse updates are outside its scope.

## Notable Details

- The paper reports that remote network bandwidth on modern instances is approaching local NVMe bandwidth for scan workloads.
- AnyBlob aims to match vendor SDK throughput while using substantially less CPU.
- The Umbra integration achieved performance similar to cloud warehouses that cache data locally, while keeping cache-free elasticity.
- The artifact is available at https://github.com/durner/AnyBlob.
