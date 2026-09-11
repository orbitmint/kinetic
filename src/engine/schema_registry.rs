use crate::cdc::pgoutput::RelationMetadata;
use arrow_schema::{DataType, Field, Schema};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

/// Manages dynamic PostgreSQL to Apache Arrow schema mappings for multi-table replication
#[derive(Default, Clone)]
pub struct SchemaRegistry {
    schemas: HashMap<String, Arc<Schema>>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }

    /// Registers or updates the Arrow Schema for a table from PostgreSQL RelationMetadata
    pub fn register_relation(&mut self, relation: &RelationMetadata) -> Arc<Schema> {
        let mut fields = vec![
            Field::new("_cdc_op", DataType::Utf8, false),
            Field::new("_cdc_lsn", DataType::Int64, false),
            Field::new("_cdc_ts", DataType::Int64, false),
        ];

        for col in &relation.columns {
            let arrow_type = pg_oid_to_arrow_type(col.type_oid);
            fields.push(Field::new(&col.name, arrow_type, true));
        }

        let schema = Arc::new(Schema::new(fields));
        info!(
            "Registered Arrow schema for table '{}' with {} columns",
            relation.name,
            schema.fields().len()
        );

        self.schemas.insert(relation.name.clone(), schema.clone());
        schema
    }

    /// Gets the Arrow Schema for a table, or generates a default schema if not yet observed
    pub fn get_or_create_default_schema(&mut self, table_name: &str) -> Arc<Schema> {
        if let Some(schema) = self.schemas.get(table_name) {
            return schema.clone();
        }

        let default_schema = Arc::new(Schema::new(vec![
            Field::new("_cdc_op", DataType::Utf8, false),
            Field::new("_cdc_lsn", DataType::Int64, false),
            Field::new("_cdc_ts", DataType::Int64, false),
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("data", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
        ]));

        self.schemas.insert(table_name.to_string(), default_schema.clone());
        default_schema
    }

    #[allow(dead_code)]
    pub fn get_schema(&self, table_name: &str) -> Option<Arc<Schema>> {
        self.schemas.get(table_name).cloned()
    }
}

/// Converts PostgreSQL Type OID to Apache Arrow DataType
pub fn pg_oid_to_arrow_type(oid: u32) -> DataType {
    match oid {
        // Boolean
        16 => DataType::Boolean,
        // Integers
        20 => DataType::Int64, // INT8 / BIGINT
        21 => DataType::Int32, // INT2 / SMALLINT
        23 => DataType::Int32, // INT4 / INTEGER
        // Floats
        700 => DataType::Float32, // FLOAT4 / REAL
        701 => DataType::Float64, // FLOAT8 / DOUBLE PRECISION
        // Text & Strings
        25 | 1042 | 1043 => DataType::Utf8, // TEXT, BPCHAR, VARCHAR
        // JSON
        114 | 3802 => DataType::Utf8, // JSON, JSONB
        // Date / Timestamp / UUID
        1082 | 1114 | 1184 | 2950 => DataType::Utf8,
        // Binary
        17 => DataType::Utf8, // BYTEA (hex string)
        _ => DataType::Utf8,  // Default fallback to Utf8 string
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cdc::pgoutput::ColumnInfo;

    #[test]
    fn test_register_relation() {
        let mut registry = SchemaRegistry::new();
        let relation = RelationMetadata {
            relation_id: 100,
            namespace: "public".to_string(),
            name: "customers".to_string(),
            replica_identity: b'd',
            columns: vec![
                ColumnInfo {
                    flags: 1,
                    name: "customer_id".to_string(),
                    type_oid: 20, // INT8
                    atttypmod: -1,
                },
                ColumnInfo {
                    flags: 0,
                    name: "full_name".to_string(),
                    type_oid: 1043, // VARCHAR
                    atttypmod: 255,
                },
                ColumnInfo {
                    flags: 0,
                    name: "is_active".to_string(),
                    type_oid: 16, // BOOL
                    atttypmod: -1,
                },
            ],
        };

        let schema = registry.register_relation(&relation);
        assert_eq!(schema.fields().len(), 6); // 3 metadata fields + 3 table columns
        assert_eq!(schema.field(0).name(), "_cdc_op");
        assert_eq!(schema.field(1).name(), "_cdc_lsn");
        assert_eq!(schema.field(2).name(), "_cdc_ts");
        assert_eq!(schema.field(3).name(), "customer_id");
        assert_eq!(schema.field(3).data_type(), &DataType::Int64);
        assert_eq!(schema.field(4).name(), "full_name");
        assert_eq!(schema.field(4).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(5).name(), "is_active");
        assert_eq!(schema.field(5).data_type(), &DataType::Boolean);
    }
}
