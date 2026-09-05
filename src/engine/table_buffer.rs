use anyhow::{Context, Result};
use arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder,
    Int64Builder, StringBuilder,
};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Schema};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

pub enum ColumnBuilder {
    Int64(Int64Builder),
    Int32(Int32Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    Utf8(StringBuilder),
}

impl ColumnBuilder {
    pub fn from_datatype(dt: &DataType) -> Self {
        match dt {
            DataType::Int64 => ColumnBuilder::Int64(Int64Builder::new()),
            DataType::Int32 => ColumnBuilder::Int32(Int32Builder::new()),
            DataType::Float32 => ColumnBuilder::Float32(Float32Builder::new()),
            DataType::Float64 => ColumnBuilder::Float64(Float64Builder::new()),
            DataType::Boolean => ColumnBuilder::Boolean(BooleanBuilder::new()),
            _ => ColumnBuilder::Utf8(StringBuilder::new()),
        }
    }

    pub fn append_text(&mut self, text: Option<&str>) {
        match self {
            ColumnBuilder::Int64(b) => {
                let val = text.and_then(|s| s.parse::<i64>().ok());
                b.append_option(val);
            }
            ColumnBuilder::Int32(b) => {
                let val = text.and_then(|s| s.parse::<i32>().ok());
                b.append_option(val);
            }
            ColumnBuilder::Float32(b) => {
                let val = text.and_then(|s| s.parse::<f32>().ok());
                b.append_option(val);
            }
            ColumnBuilder::Float64(b) => {
                let val = text.and_then(|s| s.parse::<f64>().ok());
                b.append_option(val);
            }
            ColumnBuilder::Boolean(b) => {
                let val = text.and_then(|s| match s.to_lowercase().as_str() {
                    "t" | "true" | "1" => Some(true),
                    "f" | "false" | "0" => Some(false),
                    _ => None,
                });
                b.append_option(val);
            }
            ColumnBuilder::Utf8(b) => {
                b.append_option(text);
            }
        }
    }

    pub fn finish(&mut self) -> ArrayRef {
        match self {
            ColumnBuilder::Int64(b) => Arc::new(b.finish()),
            ColumnBuilder::Int32(b) => Arc::new(b.finish()),
            ColumnBuilder::Float32(b) => Arc::new(b.finish()),
            ColumnBuilder::Float64(b) => Arc::new(b.finish()),
            ColumnBuilder::Boolean(b) => Arc::new(b.finish()),
            ColumnBuilder::Utf8(b) => Arc::new(b.finish()),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ColumnBuilder::Int64(b) => b.len(),
            ColumnBuilder::Int32(b) => b.len(),
            ColumnBuilder::Float32(b) => b.len(),
            ColumnBuilder::Float64(b) => b.len(),
            ColumnBuilder::Boolean(b) => b.len(),
            ColumnBuilder::Utf8(b) => b.len(),
        }
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Dynamic table buffer holding typed ColumnBuilders corresponding to an Arrow Schema
pub struct TableBuffer {
    pub table_name: String,
    pub schema: Arc<Schema>,
    builders: Vec<ColumnBuilder>,
    field_index_map: HashMap<String, usize>,
    highest_lsn: u64,
}

impl TableBuffer {
    pub fn new(table_name: &str, schema: Arc<Schema>) -> Self {
        let mut builders = Vec::with_capacity(schema.fields().len());
        let mut field_index_map = HashMap::with_capacity(schema.fields().len());

        for (idx, field) in schema.fields().iter().enumerate() {
            builders.push(ColumnBuilder::from_datatype(field.data_type()));
            field_index_map.insert(field.name().to_lowercase(), idx);
        }

        Self {
            table_name: table_name.to_string(),
            schema,
            builders,
            field_index_map,
            highest_lsn: 0,
        }
    }

    /// Appends a change event to this table's dynamic Arrow builders
    pub fn append(&mut self, op: &str, lsn: u64, values: &[(String, Option<String>)]) {
        self.highest_lsn = self.highest_lsn.max(lsn);

        let now_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);

        let mut incoming_map: HashMap<String, Option<&str>> = HashMap::with_capacity(values.len());
        for (col, val) in values {
            incoming_map.insert(col.to_lowercase(), val.as_deref());
        }

        for (col, _) in values {
            let lower_col = col.to_lowercase();
            if !self.field_index_map.contains_key(&lower_col) {
                warn!(
                    "table {}: column '{}' not present in schema, ignoring",
                    self.table_name, col
                );
            }
        }

        for (idx, field) in self.schema.fields().iter().enumerate() {
            let field_name_lower = field.name().to_lowercase();
            let builder = &mut self.builders[idx];

            match field_name_lower.as_str() {
                "_cdc_op" => {
                    builder.append_text(Some(op));
                }
                "_cdc_lsn" => {
                    let lsn_str = lsn.to_string();
                    builder.append_text(Some(&lsn_str));
                }
                "_cdc_ts" => {
                    let ts_str = now_micros.to_string();
                    builder.append_text(Some(&ts_str));
                }
                _ => {
                    if let Some(val_opt) = incoming_map.get(&field_name_lower) {
                        builder.append_text(*val_opt);
                    } else {
                        // Missing column: append NULL to preserve row alignment
                        builder.append_text(None);
                    }
                }
            }
        }
    }

    /// Flushes builders into an Arrow RecordBatch and resets builders
    pub fn flush(&mut self) -> Result<(RecordBatch, u64)> {
        let record_count = self.len();
        if record_count == 0 {
            anyhow::bail!("Cannot flush empty TableBuffer");
        }

        let mut arrays = Vec::with_capacity(self.builders.len());
        for builder in &mut self.builders {
            arrays.push(builder.finish());
        }

        let batch = RecordBatch::try_new(self.schema.clone(), arrays)
            .context("Failed to build RecordBatch from TableBuffer")?;

        let flushed_lsn = self.highest_lsn;
        self.highest_lsn = 0;

        Ok((batch, flushed_lsn))
    }

    pub fn len(&self) -> usize {
        self.builders.first().map(|b| b.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[allow(dead_code)]
    pub fn highest_lsn(&self) -> u64 {
        self.highest_lsn
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray};
    use arrow_schema::Field;

    #[test]
    fn test_table_buffer_append_and_flush() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_cdc_op", DataType::Utf8, false),
            Field::new("_cdc_lsn", DataType::Int64, false),
            Field::new("_cdc_ts", DataType::Int64, false),
            Field::new("id", DataType::Int64, true),
            Field::new("username", DataType::Utf8, true),
            Field::new("enabled", DataType::Boolean, true),
        ]));

        let mut buf = TableBuffer::new("accounts", schema);

        // Append 1: Insert
        buf.append(
            "INSERT",
            50001,
            &[
                ("id".to_string(), Some("101".to_string())),
                ("username".to_string(), Some("orbitmint".to_string())),
                ("enabled".to_string(), Some("true".to_string())),
            ],
        );

        // Append 2: Update with missing enabled field (schema evolution / partial column)
        buf.append(
            "UPDATE",
            50002,
            &[
                ("id".to_string(), Some("101".to_string())),
                ("username".to_string(), Some("orbitmint_updated".to_string())),
            ],
        );

        assert_eq!(buf.len(), 2);

        let (batch, flushed_lsn) = buf.flush().unwrap();
        assert_eq!(flushed_lsn, 50002);
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 6);

        // Check values in column 0 (_cdc_op)
        let op_col = batch.column(0).as_string::<i32>();
        assert_eq!(op_col.value(0), "INSERT");
        assert_eq!(op_col.value(1), "UPDATE");

        // Check values in column 3 (id)
        let id_col = batch.column(3).as_primitive::<arrow::datatypes::Int64Type>();
        assert_eq!(id_col.value(0), 101);
        assert_eq!(id_col.value(1), 101);

        // Check values in column 5 (enabled)
        let enabled_col = batch.column(5).as_boolean();
        assert_eq!(enabled_col.value(0), true);
        assert!(enabled_col.is_null(1)); // Partial column was correctly mapped to null!
    }
}
