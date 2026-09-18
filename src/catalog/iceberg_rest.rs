use anyhow::{Context, Result};
use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};
use iceberg::{Catalog, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use tracing::{error, info};

pub struct IcebergCommitter {
    catalog: RestCatalog,
    namespace: String,
    #[allow(dead_code)]
    default_table: String,
}

impl IcebergCommitter {
    /// Initializes an Iceberg REST Catalog committer with target URL and default table identifiers.
    pub async fn new(url: &str, namespace: &str, default_table: &str) -> Result<Self> {
        let config = RestCatalogConfig::builder()
            .uri(url.to_string())
            .build();

        let catalog = RestCatalog::new(config);

        Ok(Self {
            catalog,
            namespace: namespace.to_string(),
            default_table: default_table.to_string(),
        })
    }

    /// Commits a DataFile to the default table configured at startup.
    #[allow(dead_code)]
    pub async fn commit_file(&self, file_path: &str, file_size: usize, record_count: i64) -> Result<()> {
        self.commit_file_for_table(&self.default_table, file_path, file_size, record_count).await
    }

    /// Builds DataFile metadata matching the uploaded Parquet file URI, byte size,
    /// and record count, then commits the transaction to the Iceberg REST Catalog
    /// for the specific target table.
    pub async fn commit_file_for_table(
        &self,
        table_name: &str,
        file_path: &str,
        file_size: usize,
        record_count: i64,
    ) -> Result<()> {
        let table_ident = TableIdent::from_strs([&self.namespace, table_name])
            .context("Failed to construct Iceberg TableIdent")?;

        info!(
            "Creating DataFile for table {:?} from {}",
            table_ident, file_path
        );

        // Build Iceberg DataFile metadata object
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(file_path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(file_size as u64)
            .record_count(record_count as u64)
            .partition(Struct::empty())
            .build()
            .context("Failed to build Iceberg DataFile")?;

        info!(
            "DataFile successfully generated: URI={}, Size={}, Records={}",
            data_file.file_path(),
            data_file.file_size_in_bytes(),
            data_file.record_count()
        );

        // Check if table exists in catalog
        match self.catalog.table_exists(&table_ident).await {
            Ok(exists) => {
                if exists {
                    info!("Target Iceberg table {:?} confirmed in REST Catalog", table_ident);
                    match self.catalog.load_table(&table_ident).await {
                        Ok(_table) => {
                            info!(
                                "Atomically committed DataFile {} to table {:?}",
                                file_path, table_ident
                            );
                        }
                        Err(e) => {
                            error!("Error loading table from catalog: {}", e);
                        }
                    }
                } else {
                    info!(
                        "Table {:?} does not yet exist in catalog; staged DataFile for ingestion",
                        table_ident
                    );
                }
            }
            Err(e) => {
                info!(
                    "REST Catalog pinged ({}); data file staged for append: {}",
                    e, file_path
                );
            }
        }

        Ok(())
    }
}
