use crate::cdc::events::{ChangeEvent, StreamPosition};
use crate::cdc::pgoutput::{PgOutputDecoder, ParsedMessage};
use crate::telemetry::KineticMetrics;
use anyhow::{Context, Result};
use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, ReplicationEvent};
use tokio::sync::mpsc;
use tokio_postgres::NoTls;
use tracing::{error, info, warn};
use url::Url;

pub struct WalStream {
    pg_url: String,
    slot_name: String,
    publication_name: String,
}

impl WalStream {
    pub async fn new(pg_url: &str, slot_name: &str, publication_name: &str) -> Result<Self> {
        Ok(Self {
            pg_url: pg_url.to_string(),
            slot_name: slot_name.to_string(),
            publication_name: publication_name.to_string(),
        })
    }

    /// Connects via tokio-postgres to ensure replication slot and publication exist.
    pub async fn ensure_slot_and_publication(&self) -> Result<()> {
        let (client, connection) = tokio_postgres::connect(&self.pg_url, NoTls).await
            .context("Failed to connect to PostgreSQL for schema setup")?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Control plane PostgreSQL connection error: {}", e);
            }
        });

        // 1. Create publication if it doesn't exist
        let pub_check = client
            .query("SELECT 1 FROM pg_publication WHERE pubname = $1", &[&self.publication_name])
            .await?;

        if pub_check.is_empty() {
            let create_pub = format!("CREATE PUBLICATION {} FOR ALL TABLES", self.publication_name);
            match client.simple_query(&create_pub).await {
                Ok(_) => info!("Created publication '{}'", self.publication_name),
                Err(e) => warn!("Note on publication creation: {}", e),
            }
        } else {
            info!("Publication '{}' already exists", self.publication_name);
        }

        // 2. Create logical replication slot if it doesn't exist
        let slot_check = client
            .query("SELECT 1 FROM pg_replication_slots WHERE slot_name = $1", &[&self.slot_name])
            .await?;

        if slot_check.is_empty() {
            let create_slot = format!(
                "SELECT pg_create_logical_replication_slot('{}', 'pgoutput')",
                self.slot_name
            );
            client.simple_query(&create_slot).await
                .context("Failed to create logical replication slot")?;
            info!("Created logical replication slot '{}' with plugin 'pgoutput'", self.slot_name);
        } else {
            info!("Logical replication slot '{}' already exists", self.slot_name);
        }

        Ok(())
    }

    pub async fn start_stream(
        &mut self,
        event_tx: mpsc::Sender<ChangeEvent>,
        mut ack_rx: mpsc::Receiver<u64>,
        metrics: Option<KineticMetrics>,
    ) -> Result<()> {
        let parsed_url = Url::parse(&self.pg_url).context("Failed to parse PG_URL")?;
        let host = parsed_url.host_str().unwrap_or("localhost");
        let port = parsed_url.port().unwrap_or(5432);
        let user = parsed_url.username();
        let password = parsed_url.password().unwrap_or("");
        let database = parsed_url.path().trim_start_matches('/');

        let config = ReplicationConfig::new(
            host,
            user,
            password,
            database,
            &self.slot_name,
            self.publication_name.as_str(),
        )
        .with_port(port)
        .with_start_lsn(Lsn::ZERO);

        let mut client = ReplicationClient::connect(config).await
            .context("Failed to start PostgreSQL logical replication stream")?;

        info!("Streaming logical replication from slot '{}'", self.slot_name);

        let mut decoder = PgOutputDecoder::new();

        loop {
            tokio::select! {
                Some(ack_lsn) = ack_rx.recv() => {
                    info!("Acknowledged LSN: {}", ack_lsn);
                    client.update_applied_lsn(Lsn::from(ack_lsn));
                }

                event_res = client.recv() => {
                    match event_res {
                        Ok(Some(event)) => {
                            match event {
                                ReplicationEvent::XLogData { wal_start, wal_end, data, .. } => {
                                    let lsn_val = if wal_end.as_u64() > 0 { wal_end.as_u64() } else { wal_start.as_u64() };

                                    if let Some(ref m) = metrics {
                                        if wal_end.as_u64() >= wal_start.as_u64() {
                                            m.record_replication_lag(wal_end.as_u64() - wal_start.as_u64());
                                        }
                                    }

                                    if let Some(parsed) = decoder.decode(data) {
                                        match parsed {
                                            ParsedMessage::Insert { relation_id: _, new_row } => {
                                                let change = ChangeEvent::Insert {
                                                    table: new_row.table_name,
                                                    values: new_row.columns,
                                                    position: StreamPosition::Lsn(lsn_val),
                                                };
                                                if let Err(e) = event_tx.send(change).await {
                                                    error!("Failed to forward Insert event: {}", e);
                                                    break;
                                                }
                                            }
                                            ParsedMessage::Update { relation_id: _, old_row, new_row } => {
                                                let change = ChangeEvent::Update {
                                                    table: new_row.table_name,
                                                    old_values: old_row.map(|r| r.columns),
                                                    values: new_row.columns,
                                                    position: StreamPosition::Lsn(lsn_val),
                                                };
                                                if let Err(e) = event_tx.send(change).await {
                                                    error!("Failed to send Update ChangeEvent: {}", e);
                                                    break;
                                                }
                                            }
                                            ParsedMessage::Delete { relation_id: _, old_row } => {
                                                let change = ChangeEvent::Delete {
                                                    table: old_row.table_name,
                                                    values: old_row.columns,
                                                    position: StreamPosition::Lsn(lsn_val),
                                                };
                                                if let Err(e) = event_tx.send(change).await {
                                                    error!("Failed to send Delete ChangeEvent: {}", e);
                                                    break;
                                                }
                                            }
                                            ParsedMessage::Relation(rel) => {
                                                info!("Registered relation metadata for table '{}' ({} columns)", rel.name, rel.columns.len());
                                                // Propagate relation schema to downstream buffers
                                                let _ = event_tx.send(ChangeEvent::Relation(rel)).await;
                                            }
                                            ParsedMessage::Other(_) => {}
                                        }
                                    }
                                }
                                ReplicationEvent::KeepAlive { wal_end, .. } => {
                                    // Acknowledge keepalive to satisfy wal_sender_timeout
                                    client.update_applied_lsn(wal_end);
                                }
                                ReplicationEvent::Begin { final_lsn, xid, .. } => {
                                    tracing::debug!("Transaction Begin xid={} final_lsn={}", xid, final_lsn);
                                }
                                ReplicationEvent::Commit { lsn, end_lsn, .. } => {
                                    tracing::debug!("Transaction Commit lsn={} end_lsn={}", lsn, end_lsn);
                                }
                                ReplicationEvent::StoppedAt { reached } => {
                                    info!("Reached stop LSN: {}", reached);
                                    break;
                                }
                                _ => {}
                            }
                        }
                        Ok(None) => {
                            info!("Replication stream closed by server");
                            break;
                        }
                        Err(e) => {
                            error!("Error in replication stream: {}", e);
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
