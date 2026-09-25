use anyhow::Result;
use axum::response::IntoResponse;
use opentelemetry::metrics::{Counter, Gauge, Histogram, MeterProvider};
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use prometheus::{Encoder, Registry, TextEncoder};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

/// OpenTelemetry metrics container for Kinetic CDC engine
#[derive(Clone)]
pub struct KineticMetrics {
    pub cdc_events_total: Counter<u64>,
    pub replication_lag_bytes: Gauge<u64>,
    pub parquet_bytes_uploaded_total: Counter<u64>,
    pub iceberg_commit_duration_seconds: Histogram<f64>,
    registry: Arc<Registry>,
    _provider: Arc<SdkMeterProvider>,
}

impl KineticMetrics {
    /// Records a CDC change event
    pub fn record_event(&self, op: &str, table: &str) {
        self.cdc_events_total.add(
            1,
            &[
                KeyValue::new("op", op.to_string()),
                KeyValue::new("table", table.to_string()),
            ],
        );
    }

    /// Records replication lag in bytes between WAL end and confirmed LSN
    pub fn record_replication_lag(&self, lag_bytes: u64) {
        self.replication_lag_bytes.record(lag_bytes, &[]);
    }

    /// Records uploaded Parquet file bytes
    pub fn record_parquet_upload(&self, table: &str, bytes: u64) {
        self.parquet_bytes_uploaded_total.add(
            bytes,
            &[KeyValue::new("table", table.to_string())],
        );
    }

    /// Records Iceberg table commit duration
    pub fn record_commit_duration(&self, table: &str, duration_secs: f64) {
        self.iceberg_commit_duration_seconds.record(
            duration_secs,
            &[KeyValue::new("table", table.to_string())],
        );
    }

    /// Exports all registered metrics formatted in standard Prometheus exposition format
    pub fn encode_metrics(&self) -> Result<String> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer)?;
        Ok(String::from_utf8(buffer)?)
    }
}

/// Initializes OpenTelemetry MeterProvider wired to Prometheus registry
pub fn init_telemetry() -> Result<KineticMetrics> {
    let registry = Arc::new(Registry::new());

    let exporter = opentelemetry_prometheus::exporter()
        .with_registry((*registry).clone())
        .build()?;

    let provider = Arc::new(
        SdkMeterProvider::builder()
            .with_reader(exporter)
            .build(),
    );

    let meter = provider.meter("kinetic");

    let cdc_events_total = meter
        .u64_counter("kinetic_cdc_events")
        .with_description("Total number of CDC events processed by Kinetic")
        .build();

    let replication_lag_bytes = meter
        .u64_gauge("kinetic_replication_lag_bytes")
        .with_description("Current PostgreSQL logical replication lag in bytes")
        .build();

    let parquet_bytes_uploaded_total = meter
        .u64_counter("kinetic_parquet_bytes_uploaded")
        .with_description("Total bytes of Parquet files uploaded to object storage")
        .build();

    let iceberg_commit_duration_seconds = meter
        .f64_histogram("kinetic_iceberg_commit_duration_seconds")
        .with_description("Latency distribution of Iceberg REST Catalog commits in seconds")
        .build();

    Ok(KineticMetrics {
        cdc_events_total,
        replication_lag_bytes,
        parquet_bytes_uploaded_total,
        iceberg_commit_duration_seconds,
        registry,
        _provider: provider,
    })
}

/// Spawns an HTTP server exposing the `/metrics` endpoint for Prometheus scraping
pub async fn start_metrics_server(metrics: KineticMetrics, addr: SocketAddr) -> Result<()> {
    let app = axum::Router::new().route(
        "/metrics",
        axum::routing::get(move || {
            let m = metrics.clone();
            async move {
                match m.encode_metrics() {
                    Ok(body) => (
                        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
                        body,
                    )
                        .into_response(),
                    Err(e) => (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        e.to_string(),
                    )
                        .into_response(),
                }
            }
        }),
    );

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("OpenTelemetry Prometheus metrics exporter listening on http://{}/metrics", addr);
    axum::serve(listener, app).await?;
    Ok(())
}

