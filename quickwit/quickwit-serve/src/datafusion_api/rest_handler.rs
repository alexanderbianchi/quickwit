// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! REST handler for DataFusion SQL queries.
//!
//! `POST /api/v1/_sql` returns pretty-printed ASCII table output by default,
//! or JSON when `Accept: application/json` is set.

use arrow::array::{Array, RecordBatch};
use arrow::util::pretty::pretty_format_batches;
use serde::Serialize;

#[derive(Serialize)]
pub struct SqlJsonResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub num_rows: usize,
}

/// Format batches as a pretty-printed ASCII table string.
pub fn batches_to_pretty_string(batches: &[RecordBatch]) -> String {
    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        return "(empty result set)\n".to_string();
    }
    match pretty_format_batches(batches) {
        Ok(table) => format!("{table}\n({} rows)\n", batches.iter().map(|b| b.num_rows()).sum::<usize>()),
        Err(err) => format!("(error formatting results: {err})\n"),
    }
}

/// Convert batches to JSON-serializable response.
pub fn batches_to_json(batches: &[RecordBatch]) -> SqlJsonResponse {
    let columns: Vec<String> = if let Some(batch) = batches.first() {
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    } else {
        Vec::new()
    };

    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let row: Vec<serde_json::Value> = (0..batch.num_columns())
                .map(|col_idx| arrow_value_to_json(batch.column(col_idx).as_ref(), row_idx))
                .collect();
            rows.push(row);
        }
    }

    let num_rows = rows.len();
    SqlJsonResponse {
        columns,
        rows,
        num_rows,
    }
}

fn arrow_value_to_json(col: &dyn Array, row: usize) -> serde_json::Value {
    if col.is_null(row) {
        return serde_json::Value::Null;
    }
    use arrow::datatypes::DataType;
    match col.data_type() {
        DataType::Boolean => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .unwrap();
            serde_json::Value::Bool(arr.value(row))
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let arr = arrow::compute::cast(col, &DataType::Int64).unwrap();
            let val = arr
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(row);
            serde_json::Value::Number(val.into())
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            let arr = arrow::compute::cast(col, &DataType::UInt64).unwrap();
            let val = arr
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .unwrap()
                .value(row);
            serde_json::Value::Number(val.into())
        }
        DataType::Float32 | DataType::Float64 => {
            let arr = arrow::compute::cast(col, &DataType::Float64).unwrap();
            let val = arr
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap()
                .value(row);
            serde_json::json!(val)
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let arr = arrow::compute::cast(col, &DataType::Utf8).unwrap();
            let val = arr
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .value(row);
            serde_json::Value::String(val.to_string())
        }
        DataType::Timestamp(_, _) => {
            let arr = arrow::compute::cast(col, &DataType::Int64).unwrap();
            let val = arr
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(row);
            serde_json::Value::Number(val.into())
        }
        DataType::Dictionary(_, _) => {
            let arr = arrow::compute::cast(col, &DataType::Utf8).unwrap();
            let val = arr
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .value(row);
            serde_json::Value::String(val.to_string())
        }
        _ => serde_json::Value::String(format!("<unsupported: {:?}>", col.data_type())),
    }
}
