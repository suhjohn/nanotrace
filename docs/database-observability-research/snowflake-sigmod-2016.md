# The Snowflake Elastic Data Warehouse

- Source: https://www.snowflake.com/wp-content/uploads/2019/06/Snowflake_SIGMOD.pdf
- Type: paper
- Year: 2016
- Authors/Org: Benoit Dageville, Thierry Cruanes, Marcin Zukowski, Vadim Antonov, Artin Avanes, Jon Bock, Jonathan Claybaugh, Daniel Engovatov, Martin Hentschel, Jiansheng Huang, Allison W. Lee, Ashish Motivala, Abdul Q. Munir, Steven Pelley, Peter Povinec, Greg Rahn, Spyridon Triantafyllis, Philipp Unterbrunner; Snowflake Computing

## Problem

Snowflake was designed because traditional data warehouses fit poorly into cloud environments. Shared-nothing warehouses tightly coupled compute and storage, making elasticity, workload isolation, online upgrade, and rapid scaling hard. Hadoop/Spark-style systems offered scale but lacked the efficiency, SQL features, governance, and service experience expected from enterprise data warehouses.

The paper also highlights changing data: cloud-era warehouses ingest logs, mobile events, web data, social data, sensor data, and semi-structured formats. Long ETL pipelines and manual physical tuning conflict with rapidly evolving schemas and freshness expectations.

## System / Architecture

Snowflake uses a multi-cluster shared-data architecture with three layers: data storage in Amazon S3, virtual warehouses for compute, and cloud services for metadata, transactions, optimization, access control, and warehouse management.

Virtual warehouses are isolated EC2 clusters that can be created, resized, suspended, or dropped independently of data. Each query runs on exactly one virtual warehouse, which gives strong performance isolation. Multiple warehouses can query the same shared tables without copying data.

Cloud Services are long-lived multi-tenant services. They parse and optimize queries, manage transactions, store metadata in a transactional key-value store, track query state and statistics, and coordinate access control and security. This layer is the "brain"; virtual warehouses are the query execution "muscle."

## Storage Model

Table data is stored in S3 as large immutable files. Within each file, values for each attribute are grouped and compressed using a PAX/hybrid-columnar layout. File headers include column offsets and metadata, so queries can fetch only needed file ranges and columns.

Local worker disks are caches, not durable table storage. Caches store file headers and selected columns and are shared across worker processes on the node. The optimizer assigns files to workers using consistent hashing over file names to improve cache reuse. Cache maintenance is lazy; when a warehouse resizes or a node fails, data is not eagerly shuffled.

Snowflake uses pruning metadata per table file, including min/max and other distribution information. It also extracts metadata for selected paths inside semi-structured data.

## Ingest / Write Path

Data is loaded into immutable files in S3. S3's object semantics strongly shape the design: files are written as whole objects, not appended or updated in place. Inserts, updates, deletes, and merges produce new table versions by adding and removing whole files in metadata.

For semi-structured data, users can load JSON, Avro, or XML into `VARIANT` columns without specifying a full schema. Snowflake performs automatic type inference and path extraction at write time. Frequently common typed paths are stored separately in compressed columnar form, while the original document representation remains available.

Temporary query data and large query results can also spill or be stored in S3, which lets the system handle queries larger than local memory or disk.

## Query / Index Model

Snowflake implements a full SQL engine with columnar, vectorized, push-based execution. Operators process batches of rows in columnar form, avoid unnecessary intermediate materialization, and spill recursively if memory is exhausted.

The optimizer is Cascades-style and cost-based, with automatically maintained statistics. Snowflake deliberately avoids traditional user-managed indexes. Instead it relies on file-level pruning metadata, min/max ranges, expression-aware pruning, and dynamic pruning during execution. For example, hash join build-side statistics can be pushed to the probe side to skip entire files.

For semi-structured values, projection and cast expressions can be pushed into scans so only relevant extracted columns are loaded. Bloom filters over document paths help skip files that do not contain a needed path.

## Compaction / Materialization / Evolution

Snowflake's immutable-file and metadata model enables snapshot isolation, time travel, undrop, and cloning. A write creates a new version by changing the file set. Old files are retained for a configurable period, allowing queries against previous table states through `AT` or `BEFORE` syntax.

Cloning copies metadata rather than table files. A clone initially points to the same files as the source and diverges through later writes. This gives cheap snapshots of tables, schemas, and databases.

The paper does not center on background compaction in the same way as LSM systems. Evolution is mostly metadata-driven: file replacement, pruning metadata, schema-later semi-structured extraction, online upgrades, key rotation/rekeying, and warehouse scaling.

## Relevance To Event-Native Analytics And Observability

Snowflake is relevant to observability as the cloud warehouse endpoint for event data: it emphasizes separation of storage and compute, immutable files, pruning over columnar data, semi-structured ingestion, and service-managed operations. The system is less focused on sub-second real-time dashboards than Druid/Pinot/Napa, but its elastic warehouse isolation is important for mixed observability workloads such as bulk backfills, compliance queries, ad-hoc debugging, and customer reporting.

For event-native systems, Snowflake's semi-structured storage is especially instructive. It lets producers load flexible JSON-like events first and have consumers extract structure later, while still recovering much of columnar performance through automatic path extraction and pruning metadata.

## Tradeoffs And Limitations

S3 provides durability and elasticity but adds higher latency and request overhead than local disk. Snowflake mitigates this with local caching, consistent hashing, pruning, and file stealing, but long-running queries and stragglers remain areas of concern in the paper.

The architecture separates storage and compute but each query runs on one virtual warehouse. Sharing workers across warehouses is noted as future work for better utilization when strict isolation is less important.

Pruning is not a full index replacement. It works best when files are clustered or metadata can prove ranges/paths are irrelevant. If predicates do not align with file-level statistics, queries still scan large amounts of data.

## Notable Details

- Snowflake was generally available in June 2015 and, at paper time, ran several million queries per day over multiple petabytes.
- Virtual warehouses can be stopped when no queries run; compute cost is decoupled from data volume.
- File stealing mitigates stragglers by letting faster workers take remaining file work directly from S3 rather than from slow peers.
- The paper reports schema-less TPC-H-style queries within about 10% of relational performance for most queries after automatic semi-structured extraction.
- Security is built into storage and execution: hierarchical keys, key rotation/rekeying, encrypted local and S3 data, and role-based access control.
