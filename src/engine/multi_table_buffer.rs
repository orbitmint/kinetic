use crate::catalog::iceberg_rest::IcebergCommitter;
use crate::cdc::pgoutput::RelationMetadata;
use crate::cdc::events::ChangeEvent;
use crate::engine::parquet_writer::ParquetUploader;
use crate::engine::schema_registry::SchemaRegistry;
use crate::engine::table_buffer::TableBuffer;
use crate::telemetry::KineticMetrics;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use tracing::{error, info};

/// Coordinates dynamic in-memory Arrow buffering across multiple tables,
/// handling schema evolution, flush thresholds, Parquet upload, Iceberg commits,
/// and OpenTelemetry metrics instrumentation.
pub struct MultiTableBuffer {
    registry: SchemaRegistry,
    buffers: HashMap<String, TableBuffer>,
    max_batch_size: usize,
    max_batch_timeout: Duration,
    parquet_uploader: Arc<ParquetUploader>,
    iceberg_committer: Arc<IcebergCommitter>,
    metrics: KineticMetrics,
    ack_tx: mpsc::Sender<u64>,
}

impl MultiTableBuffer {
    pub fn new(
        max_batch_size: usize,
        max_batch_timeout_ms: u64,
        parquet_uploader: Arc<ParquetUploader>,
        iceberg_committer: Arc<IcebergCommitter>,
        metrics: KineticMetrics,
        ack_tx: mpsc::Sender<u64>,
    ) -> Self {
        Self {
            registry: SchemaRegistry::new(),
            buffers: HashMap::new(),
            max_batch_size,
            max_batch_timeout: Duration::from_millis(max_batch_timeout_ms),
            parquet_uploader,
            iceberg_committer,
            metrics,
            ack_tx,
        }
    }

    /// Registers a PostgreSQL relation schema dynamically
    pub fn register_relation(&mut self, relation: &RelationMetadata) {
        let schema = self.registry.register_relation(relation);
        // If buffer already exists for this table, we update its schema for future flushes
        if let Some(buf) = self.buffers.get_mut(&relation.name) {
            buf.schema = schema;
        }
    }

    /// Primary execution loop: routes events to table-keyed buffers and enforces flush policies
    pub async fn run(mut self, mut rx: mpsc::Receiver<ChangeEvent>) {
        loop {
            match timeout(self.max_batch_timeout, rx.recv()).await {
                Ok(Some(event)) => {
                    self.process_event(event).await;
                }
                Ok(None) => {
                    info!("CDC stream channel closed, flushing all active table buffers...");
                    self.flush_all().await;
                    break;
                }
                Err(_) => {
                    // Timeout elapsed: flush all active tables with pending records
                    self.flush_all().await;
                }
            }
        }
    }

    /// Processes an individual change event and flushes the table buffer if max_batch_size is hit
    async fn process_event(&mut self, event: ChangeEvent) {
        match event {
            ChangeEvent::Relation(rel) => {
                info!("Updating dynamic schema for table '{}'", rel.name);
                self.register_relation(&rel);
            }
            ChangeEvent::Insert { table, values, position } => {
                let lsn = position.as_u64().unwrap_or(0);
                self.record_and_append("INSERT", &table, lsn, values).await;
            }
            ChangeEvent::Update { table, values, position, .. } => {
                let lsn = position.as_u64().unwrap_or(0);
                self.record_and_append("UPDATE", &table, lsn, values).await;
            }
            ChangeEvent::Delete { table, values, position } => {
                let lsn = position.as_u64().unwrap_or(0);
                self.record_and_append("DELETE", &table, lsn, values).await;
            }
        }
    }

    async fn record_and_append(&mut self, op: &str, table_name: &str, lsn: u64, values: Vec<(String, Option<String>)>) {
        self.metrics.record_event(op, table_name);

        let schema_reg = &mut self.registry;
        let buffer = self.buffers.entry(table_name.to_string()).or_insert_with(|| {
            let schema = schema_reg.get_or_create_default_schema(table_name);
            TableBuffer::new(table_name, schema)
        });

        buffer.append(op, lsn, &values);

        if buffer.len() >= self.max_batch_size {
            if let Err(e) = self.flush_table(table_name).await {
                error!("Error flushing table '{}': {}", table_name, e);
            }
        }
    }

    async fn flush_all(&mut self) {
        let tables_to_flush: Vec<String> = self
            .buffers
            .iter()
            .filter(|(_, buf)| !buf.is_empty())
            .map(|(name, _)| name.clone())
            .collect();

        for table_name in tables_to_flush {
            if let Err(e) = self.flush_table(&table_name).await {
                error!("Error flushing table '{}': {}", table_name, e);
            }
        }
    }

    async fn flush_table(&mut self, table_name: &str) -> Result<()> {
        let buffer = match self.buffers.get_mut(table_name) {
            Some(b) if !b.is_empty() => b,
            _ => return Ok(()),
        };

        let (batch, flushed_lsn) = buffer.flush()?;
        let num_rows = batch.num_rows();
        info!(
            "Flushing {} rows for table '{}' (LSN: {})",
            num_rows, table_name, flushed_lsn
        );

        let uploader = self.parquet_uploader.clone();
        let committer = self.iceberg_committer.clone();
        let metrics = self.metrics.clone();
        let ack_tx = self.ack_tx.clone();
        let table = table_name.to_string();

        tokio::spawn(async move {
            match uploader.write_and_upload_for_table(batch, &table).await {
                Ok(upload_res) => {
                    info!(
                        "Uploaded Parquet for '{}': {} ({} bytes, {} rows)",
                        table, upload_res.file_uri, upload_res.file_size_bytes, upload_res.record_count
                    );

                    metrics.record_parquet_upload(&table, upload_res.file_size_bytes as u64);

                    let commit_start = Instant::now();
                    match committer
                        .commit_file_for_table(
                            &table,
                            &upload_res.file_uri,
                            upload_res.file_size_bytes,
                            upload_res.record_count as i64,
                        )
                        .await
                    {
                        Ok(_) => {
                            let duration = commit_start.elapsed().as_secs_f64();
                            metrics.record_commit_duration(&table, duration);
                            info!("Committed DataFile to '{}' in {:.3}s", table, duration);

                            if flushed_lsn > 0 {
                                let _ = ack_tx.send(flushed_lsn).await;
                            }
                        }
                        Err(e) => {
                            error!("Failed to commit DataFile to '{}': {}", table, e);
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to upload Parquet for '{}': {}", table, e);
                }
            }
        });

        Ok(())
    }
}

