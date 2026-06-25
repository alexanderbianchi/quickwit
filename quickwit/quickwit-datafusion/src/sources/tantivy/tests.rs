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

//! Unit tests for the tantivy DataFusion data source.
//!
//! These tests create in-memory tantivy indexes, register them as
//! DataFusion tables via `TantivyTableProvider`, and execute SQL queries.

use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, RecordBatch};
use datafusion::prelude::SessionContext;
use tantivy::schema::{FAST, INDEXED, STORED, STRING, SchemaBuilder, TEXT};
use tantivy_datafusion::{TantivyTableProvider, full_text_udf};

/// Create a tantivy index in a temporary directory with the given documents.
fn build_test_index(docs: &[serde_json::Value]) -> tantivy::Index {
    let mut schema_builder = SchemaBuilder::new();
    schema_builder.add_i64_field("timestamp", FAST | INDEXED | STORED);
    schema_builder.add_text_field("severity", STRING | FAST | STORED);
    schema_builder.add_f64_field("response_time", FAST | STORED);
    schema_builder.add_text_field("message", TEXT | STORED);
    schema_builder.add_text_field("service", STRING | FAST | STORED);
    let schema = schema_builder.build();

    let index = tantivy::Index::create_in_ram(schema.clone());
    let mut writer = index.writer(50_000_000).unwrap();

    let timestamp_field = schema.get_field("timestamp").unwrap();
    let severity_field = schema.get_field("severity").unwrap();
    let response_time_field = schema.get_field("response_time").unwrap();
    let message_field = schema.get_field("message").unwrap();
    let service_field = schema.get_field("service").unwrap();

    for doc_value in docs {
        let doc_obj = doc_value.as_object().unwrap();
        let mut tantivy_doc = tantivy::TantivyDocument::new();
        tantivy_doc.add_i64(timestamp_field, doc_obj["timestamp"].as_i64().unwrap());
        tantivy_doc.add_text(severity_field, doc_obj["severity"].as_str().unwrap());
        tantivy_doc.add_f64(
            response_time_field,
            doc_obj["response_time"].as_f64().unwrap(),
        );
        tantivy_doc.add_text(message_field, doc_obj["message"].as_str().unwrap());
        tantivy_doc.add_text(service_field, doc_obj["service"].as_str().unwrap());
        writer.add_document(tantivy_doc).unwrap();
    }

    writer.commit().unwrap();
    index
}

fn sample_log_docs() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({
            "timestamp": 1000, "severity": "INFO", "response_time": 0.5,
            "message": "request completed successfully", "service": "web"
        }),
        serde_json::json!({
            "timestamp": 2000, "severity": "ERROR", "response_time": 5.2,
            "message": "database connection timeout", "service": "api"
        }),
        serde_json::json!({
            "timestamp": 3000, "severity": "WARN", "response_time": 1.8,
            "message": "slow query detected in database", "service": "web"
        }),
        serde_json::json!({
            "timestamp": 4000, "severity": "INFO", "response_time": 0.3,
            "message": "health check passed", "service": "api"
        }),
        serde_json::json!({
            "timestamp": 5000, "severity": "ERROR", "response_time": 10.0,
            "message": "connection refused by upstream", "service": "web"
        }),
    ]
}

/// Register a tantivy index as a DataFusion table and execute SQL.
async fn run_tantivy_sql(index: tantivy::Index, sql: &str) -> Vec<RecordBatch> {
    let provider = TantivyTableProvider::new(index);
    let ctx = SessionContext::new();
    ctx.register_udf(full_text_udf());
    ctx.register_table("logs", Arc::new(provider)).unwrap();
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_select_all_fast_fields() {
    let index = build_test_index(&sample_log_docs());
    let batches = run_tantivy_sql(
        index,
        "SELECT timestamp, severity, response_time, service FROM logs",
    )
    .await;

    assert_eq!(total_rows(&batches), 5);
    // Schema should have the fast fields
    let schema = batches[0].schema();
    assert!(schema.field_with_name("timestamp").is_ok());
    assert!(schema.field_with_name("severity").is_ok());
    assert!(schema.field_with_name("response_time").is_ok());
    assert!(schema.field_with_name("service").is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fast_field_filter() {
    let index = build_test_index(&sample_log_docs());
    let batches = run_tantivy_sql(
        index,
        "SELECT timestamp, response_time FROM logs WHERE timestamp >= 3000",
    )
    .await;

    assert_eq!(total_rows(&batches), 3);
    // All returned timestamps should be >= 3000
    for batch in &batches {
        let ts_col = batch
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for idx in 0..ts_col.len() {
            assert!(ts_col.value(idx) >= 3000);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_aggregation_sum() {
    let index = build_test_index(&sample_log_docs());
    let batches = run_tantivy_sql(index, "SELECT SUM(response_time) as total_rt FROM logs").await;

    assert_eq!(total_rows(&batches), 1);
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    let expected = 0.5 + 5.2 + 1.8 + 0.3 + 10.0;
    assert!(
        (total - expected).abs() < 0.01,
        "expected {expected}, got {total}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_aggregation_group_by() {
    let index = build_test_index(&sample_log_docs());
    let batches = run_tantivy_sql(
        index,
        "SELECT service, COUNT(*) as cnt FROM logs GROUP BY service ORDER BY service",
    )
    .await;

    assert_eq!(total_rows(&batches), 2);
    // Should have "api" (2 rows) and "web" (3 rows)
    let batch = &batches[0];
    let service_col = batch.column_by_name("service").unwrap();
    let cnt_col = batch.column_by_name("cnt").unwrap();

    // Cast service column to plain Utf8 for easy comparison
    let service_strings =
        arrow::compute::cast(service_col, &arrow::datatypes::DataType::Utf8).unwrap();
    let service_arr = service_strings
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let services: Vec<&str> = (0..service_arr.len())
        .map(|i| service_arr.value(i))
        .collect();

    assert!(services.contains(&"api"));
    assert!(services.contains(&"web"));

    let counts: Vec<i64> = cnt_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    // api=2, web=3 (sorted by service)
    assert_eq!(counts, vec![2, 3]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_full_text_search() {
    let index = build_test_index(&sample_log_docs());
    let batches = run_tantivy_sql(
        index,
        "SELECT timestamp, response_time FROM logs WHERE full_text('message', 'database')",
    )
    .await;

    // Two documents mention "database": timestamp 2000 and 3000
    assert_eq!(total_rows(&batches), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_combined_filter_and_full_text() {
    let index = build_test_index(&sample_log_docs());
    let batches = run_tantivy_sql(
        index,
        "SELECT timestamp FROM logs WHERE full_text('message', 'database') AND timestamp >= 2500",
    )
    .await;

    // Only timestamp 3000 matches both conditions
    assert_eq!(total_rows(&batches), 1);
    let ts = batches[0]
        .column_by_name("timestamp")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(ts, 3000);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_limit() {
    let index = build_test_index(&sample_log_docs());
    let batches = run_tantivy_sql(
        index,
        "SELECT timestamp FROM logs ORDER BY timestamp LIMIT 2",
    )
    .await;

    assert_eq!(total_rows(&batches), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_split_query() {
    // Create two separate indexes (simulating two splits)
    let docs1 = vec![
        serde_json::json!({
            "timestamp": 1000, "severity": "INFO", "response_time": 1.0,
            "message": "first split doc 1", "service": "web"
        }),
        serde_json::json!({
            "timestamp": 2000, "severity": "ERROR", "response_time": 2.0,
            "message": "first split doc 2", "service": "api"
        }),
    ];
    let docs2 = vec![
        serde_json::json!({
            "timestamp": 3000, "severity": "WARN", "response_time": 3.0,
            "message": "second split doc 1", "service": "web"
        }),
        serde_json::json!({
            "timestamp": 4000, "severity": "INFO", "response_time": 4.0,
            "message": "second split doc 2", "service": "api"
        }),
    ];

    let index1 = build_test_index(&docs1);
    let index2 = build_test_index(&docs2);

    let provider = TantivyTableProvider::from_local_splits(vec![index1, index2]).unwrap();
    let ctx = SessionContext::new();
    ctx.register_udf(full_text_udf());
    ctx.register_table("logs", Arc::new(provider)).unwrap();

    let batches = ctx
        .sql("SELECT COUNT(*) as cnt FROM logs")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 4);

    // Filter across splits
    let batches = ctx
        .sql("SELECT SUM(response_time) as total FROM logs WHERE timestamp >= 3000")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    assert!((total - 7.0).abs() < 0.01, "expected 7.0, got {total}");
}
