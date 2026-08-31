use std::sync::Arc;
use crate::cdc::pgoutput::RelationMetadata;

/// A generic cursor/token representing a position in the stream.
/// For Postgres/MySQL this is usually a serialized LSN or binlog coordinate.
/// For MongoDB, it is the BSON ResumeToken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamPosition {
    Lsn(u64),
    Token(String),
}

impl StreamPosition {
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            StreamPosition::Lsn(val) => Some(*val),
            StreamPosition::Token(s) => s.parse().ok(),
        }
    }
}

/// Unified Change Data Capture Event for all supported database sources.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum ChangeEvent {
    /// Schema mapping/relation update (e.g. Postgres Relation message or MySQL TableMap)
    Relation(Arc<RelationMetadata>),
    Insert {
        table: String,
        values: Vec<(String, Option<String>)>,
        position: StreamPosition,
    },
    Update {
        table: String,
        old_values: Option<Vec<(String, Option<String>)>>,
        values: Vec<(String, Option<String>)>,
        position: StreamPosition,
    },
    Delete {
        table: String,
        values: Vec<(String, Option<String>)>,
        position: StreamPosition,
    },
}

#[allow(dead_code)]
impl ChangeEvent {
    pub fn lsn(&self) -> u64 {
        self.position().as_u64().unwrap_or(0)
    }

    pub fn position(&self) -> StreamPosition {
        match self {
            ChangeEvent::Relation(_) => StreamPosition::Lsn(0),
            ChangeEvent::Insert { position, .. } => position.clone(),
            ChangeEvent::Update { position, .. } => position.clone(),
            ChangeEvent::Delete { position, .. } => position.clone(),
        }
    }

    pub fn table(&self) -> &str {
        match self {
            ChangeEvent::Relation(r) => &r.name,
            ChangeEvent::Insert { table, .. } => table,
            ChangeEvent::Update { table, .. } => table,
            ChangeEvent::Delete { table, .. } => table,
        }
    }
}
