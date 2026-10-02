use crate::cdc::events::{ChangeEvent, StreamPosition};
use anyhow::{Context, Result};
use futures::StreamExt;
use mongodb::bson::Document;
use mongodb::change_stream::event::OperationType;
use mongodb::options::{ChangeStreamOptions, FullDocumentType};
use mongodb::Client;
use tokio::sync::mpsc;
use tracing::{error, info};

pub struct MongoChangeStream {
    uri: String,
    database: String,
}

impl MongoChangeStream {
    pub fn new(uri: &str, database: &str) -> Self {
        Self {
            uri: uri.to_string(),
            database: database.to_string(),
        }
    }

    /// Connects to a MongoDB replica set and watches the database for change stream events.
    pub async fn start_stream(
        &mut self,
        event_tx: mpsc::Sender<ChangeEvent>,
        mut ack_rx: mpsc::Receiver<u64>,
    ) -> Result<()> {
        let client = Client::with_uri_str(&self.uri)
            .await
            .context("Failed to connect to MongoDB")?;

        let db = client.database(&self.database);

        let opts = ChangeStreamOptions::builder()
            .full_document(Some(FullDocumentType::UpdateLookup))
            .build();

        let mut stream = db
            .watch()
            .with_options(opts)
            .await
            .context("Failed to open MongoDB change stream")?;

        info!("Watching MongoDB database '{}' for change streams...", self.database);

        loop {
            tokio::select! {
                Some(ack) = ack_rx.recv() => {
                    info!("Acknowledged MongoDB commit token/offset: {}", ack);
                }

                Some(change_res) = stream.next() => {
                    match change_res {
                        Ok(event) => {
                            let table = event.ns.and_then(|ns| ns.coll).unwrap_or_else(|| "default_collection".to_string());
                            let token_str = format!("{:?}", event.id);
                            let position = StreamPosition::Token(token_str);

                            match event.operation_type {
                                OperationType::Insert => {
                                    if let Some(doc) = event.full_document {
                                        let values = doc_to_values(&doc);
                                        let change = ChangeEvent::Insert {
                                            table,
                                            values,
                                            position,
                                        };
                                        if let Err(e) = event_tx.send(change).await {
                                            error!("Failed to send MongoDB Insert event: {}", e);
                                            break;
                                        }
                                    }
                                }
                                OperationType::Update | OperationType::Replace => {
                                    if let Some(doc) = event.full_document {
                                        let values = doc_to_values(&doc);
                                        let change = ChangeEvent::Update {
                                            table,
                                            old_values: None,
                                            values,
                                            position,
                                        };
                                        if let Err(e) = event_tx.send(change).await {
                                            error!("Failed to send MongoDB Update event: {}", e);
                                            break;
                                        }
                                    }
                                }
                                OperationType::Delete => {
                                    let mut values = Vec::new();
                                    if let Some(doc_key) = event.document_key {
                                        values = doc_to_values(&doc_key);
                                    }
                                    let change = ChangeEvent::Delete {
                                        table,
                                        values,
                                        position,
                                    };
                                    if let Err(e) = event_tx.send(change).await {
                                        error!("Failed to send MongoDB Delete event: {}", e);
                                        break;
                                    }
                                }
                                _ => {}
                            }
                        }
                        Err(e) => {
                            error!("Error in MongoDB change stream: {:?}", e);
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

/// Converts a BSON Document into a list of column name and optional string value pairs
fn doc_to_values(doc: &Document) -> Vec<(String, Option<String>)> {
    let mut values = Vec::with_capacity(doc.len());
    for (key, val) in doc {
        let val_str = match val {
            mongodb::bson::Bson::String(s) => Some(s.clone()),
            mongodb::bson::Bson::Int32(i) => Some(i.to_string()),
            mongodb::bson::Bson::Int64(i) => Some(i.to_string()),
            mongodb::bson::Bson::Double(d) => Some(d.to_string()),
            mongodb::bson::Bson::Boolean(b) => Some(b.to_string()),
            mongodb::bson::Bson::Null => None,
            other => Some(other.to_string()),
        };
        values.push((key.clone(), val_str));
    }
    values
}
