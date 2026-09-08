use anyhow::Result;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::{path::Path, ObjectStore};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use uuid::Uuid;

pub struct ParquetUploadResult {
    pub file_uri: String,
    pub file_size_bytes: usize,
    pub record_count: usize,
}

pub struct ParquetUploader {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl ParquetUploader {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        Self { store, prefix }
    }

    /// Writes a RecordBatch to an in-memory byte array using Parquet Encoding (SNAPPY),
    /// and uploads the byte buffer directly to S3 / Mock S3 storage.
    #[allow(dead_code)]
    pub async fn write_and_upload(&self, batch: RecordBatch) -> Result<ParquetUploadResult> {
        self.write_and_upload_for_table(batch, "default").await
    }

    /// Writes a RecordBatch for a specific table to an in-memory byte array using
    /// Parquet Encoding (SNAPPY), partitioned under {prefix}/{table_name}/{uuid}.parquet.
    pub async fn write_and_upload_for_table(
        &self,
        batch: RecordBatch,
        table_name: &str,
    ) -> Result<ParquetUploadResult> {
        let record_count = batch.num_rows();
        let mut buffer = Vec::new();

        // Configure Parquet writer with SNAPPY compression and dictionary encoding
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_dictionary_enabled(true)
            .build();

        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        let file_size_bytes = buffer.len();
        let file_name = format!("{}/{}/{}.parquet", self.prefix, table_name, Uuid::new_v4());
        let path = Path::from(file_name.as_str());

        // Upload to object store
        self.store.put(&path, Bytes::from(buffer).into()).await?;

        Ok(ParquetUploadResult {
            file_uri: format!("s3://{}", file_name),
            file_size_bytes,
            record_count,
        })
    }
}

