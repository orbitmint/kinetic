mod catalog;
mod cdc;
mod config;
mod engine;
mod telemetry;

use crate::catalog::iceberg_rest::IcebergCommitter;
use crate::cdc::WalStream;
use crate::config::Config;
use crate::engine::multi_table_buffer::MultiTableBuffer;
use crate::engine::parquet_writer::ParquetUploader;
use clap::Parser;
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = Config::parse();
    info!("Starting Kinetic CDC Engine");
    info!("CDC Source: {}", config.cdc_source);
    info!(
        "Iceberg Catalog: {} (namespace: {})",
        config.iceberg_rest_url, config.iceberg_namespace
    );

    let metrics = telemetry::init_telemetry()?;
    let metrics_for_server = metrics.clone();
    let metrics_addr = config.metrics_addr;
    let metrics_task = tokio::spawn(async move {
        if let Err(e) = telemetry::start_metrics_server(metrics_for_server, metrics_addr).await {
            tracing::error!("Metrics server error: {:?}", e);
        }
    });

    let s3_store = AmazonS3Builder::new()
        .with_bucket_name(&config.s3_bucket)
        .with_endpoint("http://localhost:9000")
        .with_access_key_id("admin")
        .with_secret_access_key("password")
        .with_allow_http(true)
        .build()?;

    let parquet_uploader = Arc::new(ParquetUploader::new(
        Arc::new(s3_store),
        config.s3_prefix,
    ));

    let iceberg_committer = Arc::new(
        IcebergCommitter::new(
            &config.iceberg_rest_url,
            &config.iceberg_namespace,
            &config.iceberg_table,
        )
        .await?,
    );

    let (event_tx, event_rx) = mpsc::channel(10000);
    let (ack_tx, ack_rx) = mpsc::channel(1000);

    let cdc_task = {
        let source_type = config.cdc_source.to_lowercase();
        let wal_metrics = metrics.clone();

        tokio::spawn(async move {
                    info!("Connecting to PostgreSQL logical replication...");
                    match WalStream::new(&config.pg_url, &config.pg_slot, &config.pg_publication).await {
                        Ok(mut wal_stream) => {
                            if let Err(e) = wal_stream.ensure_slot_and_publication().await {
                                tracing::warn!("Postgres slot/pub warning: {}", e);
                            }
                            if let Err(e) = wal_stream.start_stream(event_tx, ack_rx, Some(wal_metrics)).await {
                                tracing::error!("PostgreSQL replication stream stopped: {:?}", e);
                            }
                        }
                        Err(e) => tracing::error!("Failed to connect to PostgreSQL: {:?}", e),
                    }
                    tracing::error!("Unsupported CDC source: '{}' (supported: postgres, mysql, mongo)", unknown);
                }
            }
        })
    };

    let multi_buffer = MultiTableBuffer::new(
        config.max_batch_size,
        config.max_batch_timeout_ms,
        parquet_uploader,
        iceberg_committer,
        metrics,
        ack_tx,
    );

    let buffer_task = tokio::spawn(async move {
        multi_buffer.run(event_rx).await;
    });

    let _ = tokio::join!(cdc_task, buffer_task, metrics_task);

    Ok(())
}
