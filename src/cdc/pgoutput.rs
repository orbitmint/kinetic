#![allow(dead_code)]

use bytes::{Buf, Bytes};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, warn};

/// Postgres type OID constants
pub const OID_BOOL: u32 = 16;
pub const OID_INT8: u32 = 20;
pub const OID_INT2: u32 = 21;
pub const OID_INT4: u32 = 23;
pub const OID_TEXT: u32 = 25;
pub const OID_VARCHAR: u32 = 1043;
pub const OID_TIMESTAMP: u32 = 1114;
pub const OID_TIMESTAMPTZ: u32 = 1184;

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub flags: u8,
    pub name: String,
    pub type_oid: u32,
    pub atttypmod: i32,
}

#[derive(Debug, Clone)]
pub struct RelationMetadata {
    pub relation_id: u32,
    pub namespace: String,
    pub name: String,
    pub replica_identity: u8,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone)]
pub struct RowData {
    pub table_name: String,
    pub columns: Vec<(String, Option<String>)>,
}

#[derive(Debug, Clone)]
pub enum ParsedMessage {
    Relation(Arc<RelationMetadata>),
    Insert {
        relation_id: u32,
        new_row: RowData,
    },
    Update {
        relation_id: u32,
        old_row: Option<RowData>,
        new_row: RowData,
    },
    Delete {
        relation_id: u32,
        old_row: RowData,
    },
    Other(u8),
}

/// Stateful pgoutput parser that maintains relation schema mappings
#[derive(Default)]
pub struct PgOutputDecoder {
    relations: HashMap<u32, Arc<RelationMetadata>>,
}

impl PgOutputDecoder {
    pub fn new() -> Self {
        Self {
            relations: HashMap::new(),
        }
    }

    /// Parses a raw pgoutput message payload
    pub fn decode(&mut self, mut data: Bytes) -> Option<ParsedMessage> {
        if !data.has_remaining() {
            return None;
        }

        let tag = data.get_u8();
        match tag {
            b'R' => {
                // Relation message
                if let Some(rel) = self.parse_relation(&mut data) {
                    let rel_arc = Arc::new(rel);
                    self.relations.insert(rel_arc.relation_id, rel_arc.clone());
                    Some(ParsedMessage::Relation(rel_arc))
                } else {
                    None
                }
            }
            b'I' => {
                // Insert message
                if data.remaining() < 5 {
                    return None;
                }
                let relation_id = data.get_u32();
                let tuple_type = data.get_u8();
                if tuple_type != b'N' {
                    warn!("Unexpected tuple type {} in Insert message", tuple_type as char);
                    return None;
                }

                let rel = self.relations.get(&relation_id)?.clone();
                let columns = parse_tuple_data(&mut data, &rel.columns);

                Some(ParsedMessage::Insert {
                    relation_id,
                    new_row: RowData {
                        table_name: rel.name.clone(),
                        columns,
                    },
                })
            }
            b'U' => {
                // Update message
                if data.remaining() < 5 {
                    return None;
                }
                let relation_id = data.get_u32();
                let rel = self.relations.get(&relation_id)?.clone();

                let mut old_row = None;
                let mut tuple_marker = data.get_u8();

                if tuple_marker == b'K' || tuple_marker == b'O' {
                    let old_cols = parse_tuple_data(&mut data, &rel.columns);
                    old_row = Some(RowData {
                        table_name: rel.name.clone(),
                        columns: old_cols,
                    });
                    if data.has_remaining() {
                        tuple_marker = data.get_u8();
                    }
                }

                if tuple_marker == b'N' {
                    let new_cols = parse_tuple_data(&mut data, &rel.columns);
                    Some(ParsedMessage::Update {
                        relation_id,
                        old_row,
                        new_row: RowData {
                            table_name: rel.name.clone(),
                            columns: new_cols,
                        },
                    })
                } else {
                    None
                }
            }
            b'D' => {
                // Delete message
                if data.remaining() < 5 {
                    return None;
                }
                let relation_id = data.get_u32();
                let rel = self.relations.get(&relation_id)?.clone();
                let tuple_marker = data.get_u8();

                if tuple_marker == b'K' || tuple_marker == b'O' {
                    let old_cols = parse_tuple_data(&mut data, &rel.columns);
                    Some(ParsedMessage::Delete {
                        relation_id,
                        old_row: RowData {
                            table_name: rel.name.clone(),
                            columns: old_cols,
                        },
                    })
                } else {
                    None
                }
            }
            other => {
                debug!("Received other pgoutput message tag: {} (0x{:02X})", other as char, other);
                Some(ParsedMessage::Other(other))
            }
        }
    }

    fn parse_relation(&self, data: &mut Bytes) -> Option<RelationMetadata> {
        if data.remaining() < 4 {
            return None;
        }
        let relation_id = data.get_u32();
        let namespace = read_cstring(data)?;
        let name = read_cstring(data)?;

        if data.remaining() < 3 {
            return None;
        }
        let replica_identity = data.get_u8();
        let num_columns = data.get_u16() as usize;

        let mut columns = Vec::with_capacity(num_columns);
        for _ in 0..num_columns {
            if !data.has_remaining() {
                break;
            }
            let flags = data.get_u8();
            let col_name = read_cstring(data)?;
            if data.remaining() < 8 {
                break;
            }
            let type_oid = data.get_u32();
            let atttypmod = data.get_i32();

            columns.push(ColumnInfo {
                flags,
                name: col_name,
                type_oid,
                atttypmod,
            });
        }

        Some(RelationMetadata {
            relation_id,
            namespace,
            name,
            replica_identity,
            columns,
        })
    }
}

fn read_cstring(buf: &mut Bytes) -> Option<String> {
    let mut bytes = Vec::new();
    while buf.has_remaining() {
        let b = buf.get_u8();
        if b == 0 {
            return String::from_utf8(bytes).ok();
        }
        bytes.push(b);
    }
    None
}

fn parse_tuple_data(buf: &mut Bytes, cols: &[ColumnInfo]) -> Vec<(String, Option<String>)> {
    if buf.remaining() < 2 {
        return Vec::new();
    }
    let num_cols = buf.get_u16() as usize;
    let mut values = Vec::with_capacity(num_cols);

    for i in 0..num_cols {
        let col_name = cols
            .get(i)
            .map(|c| c.name.clone())
            .unwrap_or_else(|| format!("col_{}", i));

        if !buf.has_remaining() {
            values.push((col_name, None));
            continue;
        }

        let kind = buf.get_u8();
        match kind {
            b'n' => {
                // NULL
                values.push((col_name, None));
            }
            b'u' => {
                // Unchanged TOAST column
                values.push((col_name, None));
            }
            b't' => {
                // Text value: 4-byte length followed by UTF-8 bytes
                if buf.remaining() >= 4 {
                    let len = buf.get_u32() as usize;
                    if buf.remaining() >= len {
                        let mut str_bytes = vec![0u8; len];
                        buf.copy_to_slice(&mut str_bytes);
                        let s = String::from_utf8(str_bytes).ok();
                        values.push((col_name, s));
                    } else {
                        values.push((col_name, None));
                    }
                } else {
                    values.push((col_name, None));
                }
            }
            b'b' => {
                // Binary format
                if buf.remaining() >= 4 {
                    let len = buf.get_u32() as usize;
                    if buf.remaining() >= len {
                        let mut bin_bytes = vec![0u8; len];
                        buf.copy_to_slice(&mut bin_bytes);
                        let s = hex::encode(bin_bytes);
                        values.push((col_name, Some(s)));
                    } else {
                        values.push((col_name, None));
                    }
                } else {
                    values.push((col_name, None));
                }
            }
            _ => {
                values.push((col_name, None));
            }
        }
    }

    values
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    #[test]
    fn test_decode_relation_and_insert() {
        let mut decoder = PgOutputDecoder::new();

        // 1. Construct Relation 'R' message
        let mut r_buf = BytesMut::new();
        r_buf.put_u8(b'R');
        r_buf.put_u32(16384); // relation_id
        r_buf.put_slice(b"public\0"); // namespace
        r_buf.put_slice(b"users\0"); // name
        r_buf.put_u8(b'd'); // replica_identity
        r_buf.put_u16(3); // 3 columns

        // col 0: id (INT8 = 20)
        r_buf.put_u8(1); // key flag
        r_buf.put_slice(b"id\0");
        r_buf.put_u32(OID_INT8);
        r_buf.put_i32(-1);

        // col 1: name (VARCHAR = 1043)
        r_buf.put_u8(0);
        r_buf.put_slice(b"name\0");
        r_buf.put_u32(OID_VARCHAR);
        r_buf.put_i32(255);

        // col 2: active (BOOL = 16)
        r_buf.put_u8(0);
        r_buf.put_slice(b"active\0");
        r_buf.put_u32(OID_BOOL);
        r_buf.put_i32(-1);

        let parsed_r = decoder.decode(r_buf.freeze());
        assert!(matches!(parsed_r, Some(ParsedMessage::Relation(_))));

        // 2. Construct Insert 'I' message
        let mut i_buf = BytesMut::new();
        i_buf.put_u8(b'I');
        i_buf.put_u32(16384);
        i_buf.put_u8(b'N'); // New tuple marker
        i_buf.put_u16(3); // 3 columns

        // col 0: id = "1001"
        i_buf.put_u8(b't');
        i_buf.put_u32(4);
        i_buf.put_slice(b"1001");

        // col 1: name = "Alice"
        i_buf.put_u8(b't');
        i_buf.put_u32(5);
        i_buf.put_slice(b"Alice");

        // col 2: active = "t"
        i_buf.put_u8(b't');
        i_buf.put_u32(1);
        i_buf.put_slice(b"t");

        let parsed_i = decoder.decode(i_buf.freeze());
        match parsed_i {
            Some(ParsedMessage::Insert { relation_id, new_row }) => {
                assert_eq!(relation_id, 16384);
                assert_eq!(new_row.table_name, "users");
                assert_eq!(new_row.columns.len(), 3);
                assert_eq!(new_row.columns[0], ("id".to_string(), Some("1001".to_string())));
                assert_eq!(new_row.columns[1], ("name".to_string(), Some("Alice".to_string())));
                assert_eq!(new_row.columns[2], ("active".to_string(), Some("t".to_string())));
            }
            _ => panic!("Expected ParsedMessage::Insert"),
        }
    }
}
