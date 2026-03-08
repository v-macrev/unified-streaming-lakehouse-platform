# Unified Streaming Lakehouse Platform

A production-style distributed data platform that ingests real-time events, processes them with Apache Flink, stores them in a lakehouse on object storage, and exposes analytics through Trino and Spark.

This project simulates the architecture patterns used by modern technology companies to process large-scale event streams.

The goal is to demonstrate real platform engineering capability across streaming systems, lakehouse storage, distributed SQL engines, and container orchestration.

---

# Project Overview

Modern digital platforms generate continuous streams of events such as:

- user interactions
- orders and transactions
- payments
- device telemetry
- application metrics

These events must be:

- ingested in real time
- processed with streaming engines
- stored in scalable analytical storage
- queried interactively by analysts
- used for batch analytics and machine learning

This repository implements a **Unified Streaming Lakehouse Platform** that combines these capabilities in a coherent distributed system.

The architecture mirrors real-world data platforms used by companies such as Uber, Netflix, Airbnb, and large e-commerce platforms.

---

# Core Architecture

```text
Event Producers
      │
      ▼
Apache Kafka
(Event ingestion and durable log)
      │
      ▼
Apache Flink
(Stateful stream processing)
      │
      ▼
Iceberg Lakehouse Tables
(on MinIO object storage)
      │
      ├──────────────► Trino
      │                (interactive SQL analytics)
      │
      └──────────────► Spark / Databricks
                       (batch analytics and feature engineering)
````

Supporting infrastructure:

```text
Kubernetes
(Container orchestration)

Prometheus
(Metrics collection)

Grafana
(Platform monitoring dashboards)
```

---

# Key Technologies

### Streaming and Event Processing

* **Apache Kafka**
  Distributed event streaming platform used for ingestion and durable event logs.

* **Apache Flink**
  Stateful stream processing engine used for event-time processing, windowed aggregations, and real-time analytics.

### Lakehouse Storage

* **Apache Iceberg**
  Open table format for large-scale analytical datasets.

* **MinIO**
  S3-compatible object storage used as the lakehouse storage layer.

### Analytics Engines

* **Trino**
  Distributed SQL engine for interactive analytics over Iceberg tables.

* **Apache Spark / Databricks**
  Batch processing and advanced analytics over the same lakehouse data.

### Platform Infrastructure

* **Docker / Docker Compose**
  Local development environment.

* **Kubernetes**
  Container orchestration and production-style deployment.

### Observability

* **Prometheus**
  Metrics collection.

* **Grafana**
  Monitoring dashboards for system health and pipeline metrics.

---

# Data Platform Design

The platform follows a **lakehouse architecture** with layered datasets.

### Bronze Layer

Raw normalized events ingested from Kafka.

Examples:

* bronze_user_activity_events
* bronze_orders_events
* bronze_payments_events
* bronze_device_telemetry_events

Characteristics:

* append-only
* immutable event history
* minimal transformation
* replayable

### Silver Layer

Cleaned and structured domain datasets.

Examples:

* silver_orders
* silver_payments
* silver_user_sessions
* silver_device_metrics

Characteristics:

* validated schema
* typed columns
* normalized business entities

### Gold Layer

Business-ready analytical datasets and streaming aggregates.

Examples:

* gold_order_metrics_5m
* gold_payment_success_rate
* gold_user_activity_metrics
* gold_device_telemetry_metrics

Characteristics:

* windowed aggregations
* business KPIs
* optimized for analytics

---

# Event Streaming Model

Events are produced into Kafka topics representing different domains.

Example topics:

```text
events.user_activity.v1
events.orders.v1
events.payments.v1
events.device_telemetry.v1
```

Events include:

* event_id
* event_type
* event_time
* ingestion_time
* producer_version
* partition_key

Each topic is partitioned by a domain key such as:

* user_id
* order_id
* device_id
* merchant_id

This allows scalable parallel processing and deterministic ordering within partitions.

---

# Stream Processing

Apache Flink performs the following tasks:

* consumes events from Kafka
* parses and validates event schemas
* applies event-time semantics
* handles late events using watermarks
* performs keyed stateful aggregations
* computes windowed metrics
* writes results to Iceberg lakehouse tables

Examples of streaming outputs:

* orders per 5-minute window
* payment success rate
* active users by region
* telemetry metrics by device and app version

---

# Interactive Analytics

Trino provides distributed SQL queries over Iceberg tables stored in object storage.

Example use cases:

* operational dashboards
* product analytics
* ad-hoc analyst queries
* real-time business metrics

---

# Batch Analytics and Machine Learning

Spark / Databricks consume the same Iceberg tables to perform:

* historical KPI computation
* feature engineering
* anomaly detection
* machine learning pipelines
* large-scale data transformations

---

# Observability

The platform includes full observability for infrastructure and pipelines.

Prometheus collects metrics from:

* Kafka
* Flink
* Trino
* container infrastructure

Grafana provides dashboards for:

* ingestion throughput
* processing latency
* consumer lag
* streaming job metrics
* system health

---

# Repository Structure

```text
architecture/
    system architecture and design documentation

schemas/
    event schema definitions

infra/
    docker compose and infrastructure bootstrap scripts

event-producer/
    synthetic event generator that feeds Kafka

stream-processing/
    Apache Flink streaming jobs

lakehouse/
    Iceberg table definitions and storage layout

trino/
    distributed SQL engine configuration

observability/
    Prometheus and Grafana configuration

kubernetes/
    Kubernetes manifests for platform deployment

databricks/
    notebooks demonstrating batch analytics on lakehouse data

docs/
    runbooks and operational documentation
```

---

# Running the Platform (Local)

The platform can be started locally using Docker Compose.

This will launch:

* Kafka
* MinIO
* Flink
* Trino
* Prometheus
* Grafana

and supporting services required for the lakehouse platform.

Detailed instructions are available in:

```text
docs/local-runbook.md
```

---

# Project Goals

This project demonstrates engineering capability in:

* distributed streaming systems
* event-driven architecture
* real-time stream processing
* lakehouse storage design
* distributed SQL analytics
* containerized data infrastructure
* Kubernetes platform deployment
* observability for data systems

The repository is designed to resemble a **real platform engineering project**, not just an isolated data pipeline.

---

# Future Extensions

Possible platform extensions include:

* schema registry integration
* CDC ingestion pipelines
* data quality monitoring
* feature store integration
* streaming ML inference
* automated scaling in Kubernetes

---

# License

This project is licensed under the **GNU Affero General Public License v3.0**.

See the `LICENSE` file for the full licence text.

SPDX identifier:

```text
AGPL-3.0-only
```