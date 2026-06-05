# Samsara Operational Signals

- Source: https://www.samsara.com/blog/unlocking-vehicle-telematics-with-samsara-engineering ; https://www.samsara.com/blog/data-pipelines-at-samsara/ ; https://www.samsara.com/blog/built-to-connect-how-samsaras-open-architecture-helps-customers-unify-operations ; https://www.samsara.com/blog/samsaras-extensibile-platform
- Type: post
- Year: 2021, 2024, 2025, 2026
- Authors/Org: Samsara; Jack Roof; Rajeev Pathak

## Problem

Samsara operates in physical operations: fleets, equipment, safety, compliance, routing, maintenance, and field workflows. The data problem is not just volume; it is a mixture of real-time telemetry, intermittent connectivity, customer-facing reporting, enterprise integrations, and operational workflows that must trigger actions in other systems.

Their posts describe a platform that processes trillions of hardware-generated data points per year, billions of daily device data points, and tens of billions of monthly API calls. Customers need both live operational state and derived analytics such as fuel efficiency, driver behavior, compliance, predictive alerts, and workflow automation.

## System / Architecture

The architecture has two broad layers:

- A real-time Connected Operations Cloud fed by vehicle gateways, IoT sensors, cameras, mobile apps, and external systems.
- A data platform that moves production backend data into a Spark-based warehouse on AWS with Delta Lake for durable analytical storage.

For derived data products, Samsara built a transformation-pipeline framework. Developers define SQL transformations and dependencies using a DSL; a Go engine validates and deploys the DAG; AWS Step Functions orchestrate execution; Lambda tasks run transformations on Spark clusters in Databricks; outputs are persisted to the data lake.

The more recent platform posts emphasize an open integration surface: REST APIs, SDKs, webhooks, Kafka streams, connectors, marketplace integrations, and serverless Functions.

## Storage Model

The 2021 data pipeline post identifies Delta Lake as the persistence layer for warehouse data, giving Spark ACID-backed storage for analytical workloads. Transformation outputs are stored back into the data lake with declared schema and partitioning metadata supplied by the developer's JSON configuration.

Operational telemetry itself is modeled as a wide set of signals: GPS location, engine diagnostics, mileage, fuel level, driver behavior, EV/fuel/energy data, maintenance, safety, compliance, workflows, and customer-specific business context.

## Ingest / Write Path

Vehicle gateways and sensors send real-time data into Samsara Cloud. The telematics post emphasizes cellular transmission from vehicles and operation under harsh environments and intermittent connectivity, especially for compliance workflows where drivers, administrators, and devices must reconcile state eventually.

For analytics, production backend data is moved into the Spark/Delta warehouse. Transformation pipelines are DAGs of SQL nodes. Each node has an SQL file and a JSON config that declares schema, partitioning, and dependencies. Step Functions orchestrate nodes; Lambda invokes Spark/Databricks execution; DynamoDB acts as a metastore and lock manager so shared upstream nodes are not executed multiple times during a pipeline run.

For external integration, Samsara exposes event-driven webhooks, high-volume Kafka streams, SDKs, and direct cloud delivery to AWS, Azure, and Google Cloud environments.

## Query / Index Model

The posts do not describe a low-level query index, but they show two query surfaces:

- Customer-facing reports and dashboards built from transformed warehouse tables, such as fuel usage joined with driver assignments.
- Operational and integration APIs that let developers access organized telemetry, safety, compliance, maintenance, and workflow data.

The transformation framework exists because large analyses had become hard to maintain as copy-pasted notebooks and SQL files. The DAG DSL makes dependencies explicit and version-controlled, which improves repeatability and reduces subtle inconsistencies across customer-facing reports.

## Compaction / Materialization / Evolution

Materialization happens as pipeline nodes persist transformed data into the data lake. The framework treats intermediate transformations as reusable nodes, so downstream data products can depend on shared derived tables without duplicate execution.

Evolution is handled at the platform/API layer through versioned SDKs, documented APIs, Kafka schemas, and extensible Functions. The 2026 post describes SDKs in TypeScript, Java, C#, and Python that abstract pagination, retries, async operations, and concurrency, lowering integration maintenance cost.

The Step Functions/DynamoDB design also handles operational evolution of DAGs: shared dependencies can appear in multiple partitions, while the global metastore prevents duplicate execution and race conditions.

## Relevance To Event-Native Analytics And Observability

Samsara is a strong operational-signal example because the platform starts from physical-world events and turns them into live operations, reports, alerts, and integrations. It shows that event-native analytics needs both streaming/event delivery and curated transformation pipelines.

For observability research, Samsara's shape is useful: raw signals are not enough. Customers need context joins, derived metrics, reliable workflow triggers, and developer-accessible APIs. The platform also shows why event data must leave the source system through standard APIs, webhooks, Kafka, and serverless functions when customers need unified operations across vendors.

## Tradeoffs And Limitations

The posts are product and engineering narratives, not full architecture papers. They do not specify the production serving stores, schema evolution rules, query planners, retention model, or exact streaming storage internals.

The 2021 transformation design optimizes for developer productivity and managed orchestration, but Step Functions, Lambda, Databricks, Delta, and DynamoDB create a multi-service control plane. That reduces custom orchestration burden but pushes reliability and debugging across AWS services.

The operational signal model must handle intermittent connectivity and eventual consistency, especially in compliance workflows, but the posts do not give detailed conflict-resolution algorithms.

## Notable Details

- Samsara says its cloud platform processes over 9 trillion data points per year in the 2024 telematics post and over 20 trillion annually in the 2025 open architecture post.
- The 2021 pipeline example joins low-level fuel usage with driver assignments to power customer-facing reports.
- DynamoDB is used as both global metastore and lock manager for transformation state.
- The 2026 extensibility post reports SDK general availability, Kafka Connector general availability, marketplace app health metrics, and open beta Functions.
- Functions run custom Python logic inside Samsara's platform and can be scheduled or event-triggered without customer-managed infrastructure.
