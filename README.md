<div align="center">

<img src="docs/logo.svg" alt="Kinetic Logo" width="84" height="84" style="border-radius: 18px;" />

# Kinetic

**Sub-second Change Data Capture (CDC) into Apache Iceberg in a single native Rust binary.**

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-stable-brightgreen.svg)](https://www.rust-lang.org/)
[![Iceberg](https://img.shields.io/badge/Apache%20Iceberg-0.3.0-orange.svg)](https://iceberg.apache.org/)

*Stream changes from PostgreSQL, MySQL, and MongoDB directly to Apache Iceberg REST Catalogs (Unity Catalog, Snowflake Horizon, Tabulario) with a lightweight footprint and zero external message brokers.*

[Features](#-key-features) • [Why Kinetic?](#-why-kinetic) • [Quickstart](#-quickstart-in-2-minutes) • [Architecture](#-architecture) • [Documentation](https://orbitmint.github.io/kinetic)

</div>

---

## ⚡ The Problem: Modern CDC is Too Heavy

To get transactional changes from your production database into Apache Iceberg today, data engineering teams are forced into one of two heavy architectures:

```text
The Old Way (Debezium + Kafka):
Postgres WAL ──► Debezium ──► Kafka Connect ──► Kafka Brokers ──► Iceberg Sink ──► S3 / Iceberg
(High JVM memory, 5-30s latency, $1,500+/mo baseline idle infrastructure)
```

```text
The Kinetic Way:
Postgres WAL / MySQL Binlog / Mongo Stream ──► [ Kinetic (Single Rust Binary) ] ──► S3 / Iceberg
(~35–80MB typical RAM, sub-second latency, zero intermediate queues)
```

---

## 🚀 Key Features

* **Single Native Binary:** Zero JVM dependencies, zero Kafka clusters, zero ZooKeeper/KRaft. Starts in milliseconds.
* **Multi-Source Support:**
  * **PostgreSQL:** Native `pgoutput` logical replication decoding (Relation, Insert, Update, Delete).
  * **MySQL:** Live binlog streaming (TableMap, WriteRows, UpdateRows, DeleteRows).
  * **MongoDB:** Change stream tailing via replica set oplogs (`watch()`).
* **In-Memory Arrow Columnar Buffering:** Converts raw changes directly into Apache Arrow `RecordBatch`es without intermediate string or JSON heap overhead.
* **Direct Snappy Parquet Generation:** Generates byte-aligned Parquet chunks using `parquet::arrow::ArrowWriter` and uploads directly to S3/MinIO.
* **Sub-Second Commits:** Dual flush triggers (`max_batch_size: 5000` or `max_batch_timeout: 500ms`) commit atomic transactions directly to your Iceberg REST Catalog.
* **Native Flow Control & Pruning:** Bounded Tokio MPSC channels propagate backpressure all the way to database replication streams. Periodic LSN/resume-token acknowledgments enable database WAL pruning without disk exhaustion.
* **Built-in OpenTelemetry:** Live Prometheus metrics exported on `/metrics` (event rates, byte throughput, replication lag, and commit latency).

---

## 🏎️ Quickstart in 2 Minutes

### 1. Launch the Test Cluster
Start PostgreSQL 16 (with logical replication enabled), an Apache Iceberg REST Catalog, and MinIO:

```bash
docker compose up -d
```

### 2. Seed the Database
Initialize a sample table and replication publication:

```bash
docker compose exec -T postgres psql -U admin -d kinetic_db < scripts/setup_test_db.sql
```

### 3. Run Kinetic
Start the streaming engine:

```bash
cargo run -- \
  --cdc-source postgres \
  --pg-url "postgres://admin:password@localhost:5432/kinetic_db" \
  --iceberg-rest-url "http://localhost:8181" \
  --s3-bucket "warehouse"
```

In a second terminal, execute inserts or updates in PostgreSQL and watch Kinetic flush Snappy Parquet files and commit snapshots directly into Iceberg!

---

## 📊 Live Observability

Kinetic includes an embedded Prometheus metrics exporter listening on `0.0.0.0:9090/metrics`:

```bash
curl -s http://localhost:9090/metrics
```

Key exported metrics:
* `kinetic_cdc_events_total{op="INSERT|UPDATE|DELETE", table="..."}`
* `kinetic_replication_lag_bytes`
* `kinetic_parquet_bytes_uploaded_total{table="..."}`
* `kinetic_iceberg_commit_duration_seconds`

---

## 🏗️ Architecture

```text
 ┌──────────────────────┐
 │ PostgreSQL / MySQL / │
 │   MongoDB Cluster    │
 └──────────┬───────────┘
            │  (WAL / Binlog / Change Stream)
            ▼
 ┌────────────────────────────────────────────────────────┐
 │                   Kinetic Engine                       │
 │                                                        │
 │  ┌─────────────────┐       ┌────────────────────────┐  │
 │  │ Stream Decoders │ ────► │  Arrow MultiTablePool  │  │
 │  └─────────────────┘       └───────────┬────────────┘  │
 │                                        │ (500ms Flush) │
 │                                        ▼               │
 │                            ┌────────────────────────┐  │
 │                            │ In-Memory Parquet / S3 │  │
 │                            └───────────┬────────────┘  │
 └────────────────────────────────────────┼───────────────┘
                                          │
                        Atomic Snapshot   │  Parquet DataFiles
                        REST Commit       ▼
                           ┌─────────────────────────────┐
                           │ Apache Iceberg REST Catalog │
                           │   (Unity / Horizon / S3)    │
                           └─────────────────────────────┘
```

---

## 🛠️ CLI Configuration

| Flag | Env Variable | Default | Description |
|---|---|---|---|
| `--cdc-source` | `CDC_SOURCE` | `postgres` | CDC source engine (`postgres`, `mysql`, `mongo`) |
| `--pg-url` | `PG_URL` | `postgres://admin:password@localhost:5432/kinetic_db` | PostgreSQL connection URL |
| `--mysql-url` | `MYSQL_URL` | `mysql://admin:password@localhost:3306/` | MySQL connection URL |
| `--mongo-uri` | `MONGO_URI` | `mongodb://localhost:27017/?replicaSet=rs0` | MongoDB connection URI |
| `--max-batch-size` | `MAX_BATCH_SIZE` | `5000` | Flush batch size threshold |
| `--max-batch-timeout-ms` | `MAX_BATCH_TIMEOUT_MS` | `500` | Flush timeout threshold (ms) |
| `--s3-bucket` | `S3_BUCKET` | `warehouse` | Target object storage bucket |
| `--iceberg-rest-url` | `ICEBERG_REST_URL` | `http://localhost:8181` | Iceberg REST Catalog endpoint |
| `--metrics-addr` | `METRICS_ADDR` | `0.0.0.0:9090` | OpenTelemetry Prometheus metrics listener |

---

## 📄 License

Kinetic is released under the [Apache 2.0 License](LICENSE).
