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

//! Integration tests for tantivy DataFusion queries — full cluster sandbox.
//!
//! Creates a quickwit index, ingests log documents via the REST API, waits for
//! splits to be published, then queries them via DataFusion SQL using the
//! `TantivyDataSource`.

use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, RecordBatch};
use quickwit_config::service::QuickwitService;
use quickwit_datafusion::DataFusionSessionBuilder;
use quickwit_datafusion::sources::tantivy::TantivyDataSource;
use quickwit_metastore::SplitState;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_rest_client::rest_client::CommitType;
use quickwit_search::SearcherContext;

use crate::test_utils::{ClusterSandbox, ClusterSandboxBuilder, ingest};

// ── Setup ──────────────────────────────────────────────────────────

async fn start_sandbox() -> ClusterSandbox {
    unsafe {
        std::env::set_var("QW_DISABLE_TELEMETRY", "1");
        std::env::set_var("QW_ENABLE_DATAFUSION_ENDPOINT", "true");
    }
    quickwit_common::setup_logging_for_tests();
    ClusterSandboxBuilder::build_and_start_standalone().await
}

fn metastore_client(sandbox: &ClusterSandbox) -> MetastoreServiceClient {
    let (config, _) = sandbox
        .node_configs
        .iter()
        .find(|(_, svc)| svc.contains(&QuickwitService::Metastore))
        .unwrap();
    let addr = config.grpc_listen_addr;
    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect_lazy();
    MetastoreServiceClient::from_channel(addr, channel, bytesize::ByteSize::mib(20), None)
}

fn session_builder(
    sandbox: &ClusterSandbox,
    metastore: MetastoreServiceClient,
) -> DataFusionSessionBuilder {
    let searcher_config = quickwit_config::SearcherConfig::default();
    let searcher_context = Arc::new(SearcherContext::new_without_invoker(
        searcher_config,
        None,
    ));
    let source = Arc::new(TantivyDataSource::new(
        metastore,
        sandbox.storage_resolver().clone(),
        searcher_context,
    ));
    DataFusionSessionBuilder::new().with_source(source)
}

async fn run_sql(builder: &DataFusionSessionBuilder, sql: &str) -> Vec<RecordBatch> {
    let ctx = builder.build_session().unwrap();
    let fragments: Vec<&str> = sql
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    for fragment in &fragments[..fragments.len().saturating_sub(1)] {
        ctx.sql(fragment).await.unwrap().collect().await.unwrap();
    }
    ctx.sql(fragments.last().unwrap())
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

const INDEX_CONFIG: &str = r#"
version: 0.8
index_id: tantivy-df-test
doc_mapping:
  field_mappings:
  - name: timestamp
    type: datetime
    input_formats:
    - unix_timestamp
    output_format: unix_timestamp_secs
    fast_precision: seconds
    fast: true
  - name: severity
    type: text
    tokenizer: raw
    fast: true
  - name: response_time
    type: f64
    fast: true
  - name: message
    type: text
  - name: service
    type: text
    tokenizer: raw
    fast: true
  timestamp_field: timestamp
indexing_settings:
  commit_timeout_secs: 1
"#;

async fn create_index_and_ingest(sandbox: &ClusterSandbox) {
    let client = sandbox.rest_client(QuickwitService::Indexer);
    client
        .indexes()
        .create(INDEX_CONFIG, quickwit_config::ConfigFormat::Yaml, false)
        .await
        .unwrap();

    sandbox.wait_for_indexing_pipelines(1).await.unwrap();

    // Timestamps must be valid unix timestamps (post-1972). Using 2024 timestamps.
    let docs = r#"
{"timestamp": 1704067200, "severity": "INFO",  "response_time": 0.5,  "message": "request completed successfully", "service": "web"}
{"timestamp": 1704070800, "severity": "ERROR", "response_time": 5.2,  "message": "database connection timeout",     "service": "api"}
{"timestamp": 1704074400, "severity": "WARN",  "response_time": 1.8,  "message": "slow query detected in database", "service": "web"}
{"timestamp": 1704078000, "severity": "INFO",  "response_time": 0.3,  "message": "health check passed",             "service": "api"}
{"timestamp": 1704081600, "severity": "ERROR", "response_time": 10.0, "message": "connection refused by upstream",   "service": "web"}
"#;
    ingest(
        &client,
        "tantivy-df-test",
        quickwit_rest_client::models::IngestSource::Str(docs.to_string()),
        CommitType::WaitFor,
    )
    .await
    .unwrap();

    sandbox
        .wait_for_splits(
            "tantivy-df-test",
            Some(vec![SplitState::Published]),
            1,
        )
        .await
        .unwrap();
}

// ═══════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tantivy_select_all() {
    let sandbox = start_sandbox().await;
    create_index_and_ingest(&sandbox).await;

    let metastore = metastore_client(&sandbox);
    let builder = session_builder(&sandbox, metastore);

    let batches = run_sql(
        &builder,
        r#"SELECT timestamp, severity, response_time, service FROM "tantivy-df-test""#,
    )
    .await;

    assert_eq!(total_rows(&batches), 5, "expected 5 rows, got {}", total_rows(&batches));
    sandbox.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tantivy_fast_field_filter() {
    let sandbox = start_sandbox().await;
    create_index_and_ingest(&sandbox).await;

    let metastore = metastore_client(&sandbox);
    let builder = session_builder(&sandbox, metastore);

    // 1704074400 is the 3rd document's timestamp (2024-01-01T02:00:00Z).
    // The tantivy date field maps to Timestamp(Microsecond, None) in Arrow,
    // so we must cast the literal to match.
    let batches = run_sql(
        &builder,
        r#"SELECT response_time FROM "tantivy-df-test"
           WHERE timestamp >= arrow_cast(1704074400000000, 'Timestamp(Microsecond, None)')"#,
    )
    .await;

    assert_eq!(total_rows(&batches), 3, "expected 3 rows with timestamp >= 1704074400");
    sandbox.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tantivy_aggregation() {
    let sandbox = start_sandbox().await;
    create_index_and_ingest(&sandbox).await;

    let metastore = metastore_client(&sandbox);
    let builder = session_builder(&sandbox, metastore);

    let batches = run_sql(
        &builder,
        r#"SELECT SUM(response_time) as total FROM "tantivy-df-test""#,
    )
    .await;

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
    sandbox.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tantivy_full_text_search() {
    let sandbox = start_sandbox().await;
    create_index_and_ingest(&sandbox).await;

    let metastore = metastore_client(&sandbox);
    let builder = session_builder(&sandbox, metastore);

    let batches = run_sql(
        &builder,
        r#"SELECT timestamp FROM "tantivy-df-test" WHERE full_text('message', 'database')"#,
    )
    .await;

    // Two documents mention "database": timestamp 2000 and 3000
    assert_eq!(total_rows(&batches), 2);
    sandbox.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tantivy_count_with_filter() {
    let sandbox = start_sandbox().await;
    create_index_and_ingest(&sandbox).await;

    let metastore = metastore_client(&sandbox);
    let builder = session_builder(&sandbox, metastore);

    let batches = run_sql(
        &builder,
        r#"SELECT COUNT(*) as cnt FROM "tantivy-df-test" WHERE response_time > 1.0"#,
    )
    .await;

    assert_eq!(total_rows(&batches), 1);
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    // response_time > 1.0: 5.2, 1.8, 10.0 = 3 docs
    assert_eq!(count, 3);
    sandbox.shutdown().await.unwrap();
}
