# Platform Architecture

This document describes the system architecture of the **Unified Streaming Lakehouse Platform**.

The platform simulates the architecture used by modern data-intensive companies to process large-scale event streams and expose analytical data to multiple consumers.

The architecture combines streaming systems, lakehouse storage, distributed query engines, and containerized infrastructure into a unified data platform.


---

# System Goals

The platform is designed to demonstrate the architecture patterns required to build modern event-driven data systems.

Primary goals:

• real-time event ingestion  
• scalable stream processing  
• durable event storage  
• unified analytical storage (lakehouse)  
• interactive distributed SQL analytics  
• batch analytics capability  
• containerized infrastructure deployment  
• observability of platform health and throughput  

The system must support both real-time analytics and historical analysis from the same underlying data.


---

# High-Level Architecture

The platform consists of the following primary layers:

```

Event Producers
│
▼
Apache Kafka
(Event ingestion layer)
│
▼
Apache Flink
(Stream processing layer)
│
▼
Iceberg Lakehouse Tables
(Storage layer on object storage)
│
├────────────► Trino
│              (interactive analytics)
│
└────────────► Spark / Databricks
(batch analytics and ML)

```

Supporting infrastructure:

```

MinIO
(Object storage)

Kubernetes
(Container orchestration)

Prometheus
(Metrics collection)

Grafana
(Platform monitoring dashboards)

```

The architecture separates ingestion, processing, storage, and query responsibilities to allow each layer to scale independently.


---

# Platform Components

## Event Producers

Event producers simulate operational systems generating real-time events.

Examples of simulated sources:

• user activity events  
• orders and transactions  
• payment processing  
• device telemetry  

Producers publish events to Kafka topics using domain-specific schemas.

Each event contains metadata including:

- event_id
- event_type
- event_time
- ingestion_time
- producer_version

Domain payload fields vary by event type.


---

## Apache Kafka

Kafka acts as the **event ingestion backbone** of the platform.

Responsibilities:

• durable event log  
• partitioned event streams  
• decoupling producers and consumers  
• event replay capability  
• horizontal scalability  

Kafka topics are partitioned by domain-specific keys such as:

- user_id
- order_id
- device_id
- merchant_id

Partitioning ensures ordering guarantees within partitions and allows parallel consumption by the stream processing layer.

Kafka runs in **KRaft mode** (ZooKeeper-free architecture).


---

## Apache Flink

Apache Flink performs **stateful stream processing** on the incoming event streams.

Primary responsibilities:

• consume Kafka event streams  
• parse and validate event schemas  
• convert events into lakehouse-compatible records  
• perform windowed aggregations  
• maintain keyed state for metrics  
• handle out-of-order events using event-time processing  

Processing semantics include:

• event-time processing  
• watermarking  
• tumbling time windows  
• keyed aggregations  
• late event handling


---

# Lakehouse Storage Layer

The platform stores analytical datasets using **Apache Iceberg** tables.

Iceberg provides:

• schema evolution  
• partition management  
• ACID table operations  
• compatibility with multiple compute engines  
• efficient large-scale analytical queries


---

# Object Storage

Iceberg tables are stored on **MinIO**, which provides S3-compatible object storage.

Object storage advantages:

• highly scalable  
• compute-storage separation  
• compatible with modern lakehouse engines  
• cost-efficient storage model  

Iceberg metadata and data files are stored in MinIO buckets representing the lakehouse storage layer.


---

# Lakehouse Data Layers

The platform follows a medallion-style data architecture.


## Bronze Layer

Raw normalized events.

Characteristics:

• append-only  
• immutable event history  
• minimal transformation  
• source-of-truth event archive  

Example tables:

- bronze_user_activity_events
- bronze_orders_events
- bronze_payments_events
- bronze_device_telemetry_events


---

## Silver Layer

Structured domain datasets derived from bronze events.

Characteristics:

• validated schemas  
• normalized fields  
• typed columns  
• cleaned event data  

Example tables:

- silver_orders
- silver_payments
- silver_user_sessions
- silver_device_metrics


---

## Gold Layer

Business-oriented aggregated datasets.

Characteristics:

• time-windowed aggregates  
• operational KPIs  
• analytics-ready datasets  

Example tables:

- gold_order_metrics_5m
- gold_payment_success_rate
- gold_user_activity_metrics
- gold_device_telemetry_metrics


---

# Query and Analytics Layer

## Trino

Trino provides distributed SQL queries over Iceberg tables.

Typical use cases:

• operational dashboards  
• analyst queries  
• product analytics  
• near-real-time metrics  

Trino reads Iceberg metadata and data files directly from object storage.


---

## Spark / Databricks

Spark-based systems interact with the same Iceberg lakehouse tables.

Typical use cases:

• historical analytics  
• machine learning pipelines  
• feature engineering  
• batch transformations  

Because Iceberg separates compute from storage, both Trino and Spark can operate on the same datasets.


---

# Data Flow Lifecycle

1. Event producers generate domain events.

2. Events are published to Kafka topics.

3. Kafka stores events in partitioned logs.

4. Flink consumes Kafka topics using consumer groups.

5. Flink processes events with event-time semantics.

6. Processed records are written into Iceberg tables on MinIO.

7. Iceberg tables become available to query engines.

8. Trino provides distributed SQL analytics.

9. Spark / Databricks perform batch analytics and machine learning workloads.


---

# Observability

Distributed data platforms require visibility into system health.

The platform includes a full observability stack.

## Prometheus

Prometheus collects metrics from:

• Kafka brokers  
• Flink jobs  
• Trino services  
• container infrastructure  

Metrics include:

• event ingestion throughput  
• processing latency  
• consumer lag  
• query performance  
• infrastructure health


## Grafana

Grafana dashboards visualize platform metrics such as:

• Kafka ingestion rate  
• Flink processing throughput  
• streaming job latency  
• platform resource usage  
• system health indicators


---

# Deployment Model

The platform supports two deployment modes.

## Local Development

Local execution uses **Docker Compose**.

Services started locally include:

- Kafka
- Flink
- MinIO
- Trino
- Prometheus
- Grafana


---

## Kubernetes Deployment

Production-style deployment uses **Kubernetes**.

Kubernetes provides:

• container orchestration  
• service discovery  
• scaling capabilities  
• resource isolation  

Core services are deployed as Kubernetes workloads including:

- Kafka brokers
- Flink job manager and task managers
- Trino coordinator and workers
- MinIO object storage
- observability stack


---

# Design Tradeoffs

Several architectural choices were made to balance realism and practicality.

Kafka vs alternatives  
Kafka remains the most recognizable streaming backbone and a strong signal of real-world data platform experience.

Iceberg vs Delta  
Iceberg provides strong interoperability with Trino, Spark, and Flink without requiring vendor-specific infrastructure.

Object storage vs HDFS  
Modern lakehouse systems prefer object storage due to scalability and compute-storage separation.

Docker Compose + Kubernetes  
Compose provides a runnable local environment while Kubernetes demonstrates production deployment maturity.


---

# Architectural Outcomes

This architecture demonstrates several key platform engineering capabilities:

• event-driven system design  
• streaming data pipelines  
• lakehouse data architecture  
• distributed SQL analytics  
• containerized infrastructure  
• observability of distributed systems  

The system intentionally mirrors the architectural patterns used in real production data platforms.
