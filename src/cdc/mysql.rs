use crate::cdc::events::{ChangeEvent, StreamPosition};
use crate::cdc::pgoutput::RelationMetadata;
use anyhow::{Context, Result};
use mysql_cdc::binlog_client::BinlogClient;
use mysql_cdc::binlog_options::BinlogOptions;
use mysql_cdc::events::binlog_event::BinlogEvent;
use mysql_cdc::replica_options::ReplicaOptions;
use mysql_cdc::ssl_mode::SslMode;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info};
use url::Url;

pub struct MysqlBinlogStream {
    url: String,
}

impl MysqlBinlogStream {
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
        }
    }

    /// Connects to MySQL using the replication client, tracks TableMap schemas,
    /// and emits unified ChangeEvent instances into event_tx.
    pub async fn start_stream(
        &mut self,
        event_tx: mpsc::Sender<ChangeEvent>,
        mut ack_rx: mpsc::Receiver<u64>,
    ) -> Result<()> {
        let parsed_url = Url::parse(&self.url).context("Failed to parse MYSQL_URL")?;
        let host = parsed_url.host_str().unwrap_or("localhost").to_string();
        let port = parsed_url.port().unwrap_or(3306);
        let username = parsed_url.username().to_string();
        let password = parsed_url.password().unwrap_or("").to_string();

        let options = ReplicaOptions {
            hostname: host,
            port,
            username,
            password,
            blocking: true,
            ssl_mode: SslMode::Disabled,
            binlog: BinlogOptions::from_start(),
            ..Default::default()
        };

        // We run the blocking mysql_cdc replication client in a dedicated blocking thread
        tokio::task::spawn_blocking(move || {
            let mut client = BinlogClient::new(options);

            let mut table_map: HashMap<u64, (String, String, Vec<String>)> = HashMap::new();

            let events = match client.replicate() {
                Ok(evs) => evs,
                Err(e) => {
                    error!("Failed to start MySQL replication: {:?}", e);
                    return;
                }
            };

            for result in events {
                match result {
                    Ok((header, event)) => {
                        let position = StreamPosition::Lsn(header.next_event_position as u64);

                        match event {
                            BinlogEvent::TableMapEvent(ev) => {
                                let mut col_names = Vec::new();
                                for i in 0..ev.column_types.len() {
                                    col_names.push(format!("col_{}", i));
                                }

                                table_map.insert(
                                    ev.table_id,
                                    (ev.database_name.clone(), ev.table_name.clone(), col_names),
                                );

                                // Register relation schema dynamically
                                let rel = Arc::new(RelationMetadata {
                                    relation_id: ev.table_id as u32,
                                    namespace: ev.database_name,
                                    name: ev.table_name,
                                    replica_identity: b'd',
                                    columns: vec![],
                                });

                                let _ = event_tx.blocking_send(ChangeEvent::Relation(rel));
                            }
                            BinlogEvent::WriteRowsEvent(ev) => {
                                if let Some((_, table_name, col_names)) = table_map.get(&ev.table_id) {
                                    for row in ev.rows {
                                        let mut values = Vec::new();
                                        for (i, cell) in row.cells.iter().enumerate() {
                                            let col = col_names.get(i).cloned().unwrap_or_else(|| format!("col_{}", i));
                                            let val_str = cell.as_ref().map(|c| format!("{:?}", c));
                                            values.push((col, val_str));
                                        }

                                        let change = ChangeEvent::Insert {
                                            table: table_name.clone(),
                                            values,
                                            position: position.clone(),
                                        };

                                        if let Err(e) = event_tx.blocking_send(change) {
                                            error!("Failed to send MySQL Insert event: {}", e);
                                            return;
                                        }
                                    }
                                }
                            }
                            BinlogEvent::UpdateRowsEvent(ev) => {
                                if let Some((_, table_name, col_names)) = table_map.get(&ev.table_id) {
                                    for row_data in ev.rows {
                                        let mut old_values = Vec::new();
                                        for (i, cell) in row_data.before_update.cells.iter().enumerate() {
                                            let col = col_names.get(i).cloned().unwrap_or_else(|| format!("col_{}", i));
                                            let val_str = cell.as_ref().map(|c| format!("{:?}", c));
                                            old_values.push((col, val_str));
                                        }

                                        let mut new_values = Vec::new();
                                        for (i, cell) in row_data.after_update.cells.iter().enumerate() {
                                            let col = col_names.get(i).cloned().unwrap_or_else(|| format!("col_{}", i));
                                            let val_str = cell.as_ref().map(|c| format!("{:?}", c));
                                            new_values.push((col, val_str));
                                        }

                                        let change = ChangeEvent::Update {
                                            table: table_name.clone(),
                                            old_values: Some(old_values),
                                            values: new_values,
                                            position: position.clone(),
                                        };

                                        if let Err(e) = event_tx.blocking_send(change) {
                                            error!("Failed to send MySQL Update event: {}", e);
                                            return;
                                        }
                                    }
                                }
                            }
                            BinlogEvent::DeleteRowsEvent(ev) => {
                                if let Some((_, table_name, col_names)) = table_map.get(&ev.table_id) {
                                    for row in ev.rows {
                                        let mut values = Vec::new();
                                        for (i, cell) in row.cells.iter().enumerate() {
                                            let col = col_names.get(i).cloned().unwrap_or_else(|| format!("col_{}", i));
                                            let val_str = cell.as_ref().map(|c| format!("{:?}", c));
                                            values.push((col, val_str));
                                        }

                                        let change = ChangeEvent::Delete {
                                            table: table_name.clone(),
                                            values,
                                            position: position.clone(),
                                        };

                                        if let Err(e) = event_tx.blocking_send(change) {
                                            error!("Failed to send MySQL Delete event: {}", e);
                                            return;
                                        }
                                    }
                                }
                            }
                            BinlogEvent::XidEvent(_) => {
                                // Transaction commit boundary
                                client.commit(&header, &event);
                            }
                            _ => {}
                        }
                    }
                    Err(e) => {
                        error!("MySQL binlog replication error: {:?}", e);
                        break;
                    }
                }
            }
        });

        // Loop to consume ack_rx feedback messages
        while let Some(ack) = ack_rx.recv().await {
            info!("Acknowledged MySQL binlog commit pos: {}", ack);
        }

        Ok(())
    }
}
