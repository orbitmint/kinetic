use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about = "Kinetic - High-Throughput Rust CDC Engine")]
pub struct Config {
    #[arg(long, env = "CDC_SOURCE", default_value = "postgres")]
    pub cdc_source: String,

    // Postgres-specific options
    #[arg(long, env = "PG_URL", default_value = "postgres://admin:password@localhost:5432/kinetic_db")]
    pub pg_url: String,

    #[arg(long, env = "PG_PUBLICATION", default_value = "kinetic_pub")]
    pub pg_publication: String,

    #[arg(long, env = "PG_SLOT", default_value = "kinetic_slot")]
    pub pg_slot: String,

    // MySQL-specific options
    #[arg(long, env = "MYSQL_URL", default_value = "mysql://admin:password@localhost:3306/")]
    pub mysql_url: String,

    // MongoDB-specific options
    #[arg(long, env = "MONGO_URI", default_value = "mongodb://localhost:27017/?replicaSet=rs0")]
    pub mongo_uri: String,

    #[arg(long, env = "MONGO_DB", default_value = "kinetic_db")]
    pub mongo_db: String,

    // Engine Buffering Thresholds
    #[arg(long, env = "MAX_BATCH_SIZE", default_value_t = 5000)]
    pub max_batch_size: usize,
    #[arg(long, env = "MAX_BATCH_SIZE", default_value_t = 5000)]
    pub max_batch_size: usize,
    #[arg(long, env = "MAX_BATCH_TIMEOUT_MS", default_value_t = 500)]
    pub max_batch_timeout_ms: u64,
    #[arg(long, env = "S3_BUCKET", default_value = "warehouse")]
    pub s3_bucket: String,
    #[arg(long, env = "S3_PREFIX", default_value = "kinetic_data")]
    pub s3_prefix: String,
    #[arg(long, env = "ICEBERG_REST_URL", default_value = "http://localhost:8181")]
    pub iceberg_rest_url: String,
    #[arg(long, env = "ICEBERG_NAMESPACE", default_value = "default")]
    pub iceberg_namespace: String,
    #[arg(long, env = "ICEBERG_TABLE", default_value = "kinetic_table")]
    pub iceberg_table: String,
    #[arg(long, env = "METRICS_ADDR", default_value = "0.0.0.0:9090")]
    pub metrics_addr: std::net::SocketAddr,
}
