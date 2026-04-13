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
use arrow::datatypes::DataType;
use datafusion::physical_plan::common::collect as collect_stream;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use prost::Message;
use quickwit_config::service::QuickwitService;
use quickwit_datafusion::sources::tantivy::TantivyDataSource;
use quickwit_datafusion::DataFusionSessionBuilder;
use quickwit_metastore::SplitState;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_rest_client::rest_client::CommitType;
use quickwit_search::{create_search_client_from_grpc_addr, SearcherContext, SearcherPool};

use crate::test_utils::{ingest, ClusterSandbox, ClusterSandboxBuilder};

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
    let searcher_context = Arc::new(SearcherContext::new_without_invoker(searcher_config, None));
    let source = Arc::new(TantivyDataSource::new(
        metastore,
        sandbox.storage_resolver().clone(),
        searcher_context,
    ));
    DataFusionSessionBuilder::new().with_source(source)
}

fn distributed_session_builder(
    sandbox: &ClusterSandbox,
    metastore: MetastoreServiceClient,
) -> DataFusionSessionBuilder {
    let searcher_config = quickwit_config::SearcherConfig::default();
    let searcher_context = Arc::new(SearcherContext::new_without_invoker(searcher_config, None));
    let source = Arc::new(TantivyDataSource::new(
        metastore,
        sandbox.storage_resolver().clone(),
        searcher_context,
    ));

    let pool = SearcherPool::default();
    for (config, services) in &sandbox.node_configs {
        if services.contains(&QuickwitService::Searcher) {
            let addr = config.grpc_listen_addr;
            pool.insert(
                addr,
                create_search_client_from_grpc_addr(addr, bytesize::ByteSize::mib(20)),
            );
        }
    }

    DataFusionSessionBuilder::new()
        .with_source(source)
        .with_searcher_pool(pool)
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

fn utf8_values(column: &Arc<dyn Array>) -> arrow::array::StringArray {
    let casted = arrow::compute::cast(column, &DataType::Utf8).unwrap();
    casted
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .clone()
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
        .wait_for_splits("tantivy-df-test", Some(vec![SplitState::Published]), 1)
        .await
        .unwrap();
}

async fn create_index_and_ingest_two_splits(sandbox: &ClusterSandbox) {
    let client = sandbox.rest_client(QuickwitService::Indexer);
    client
        .indexes()
        .create(INDEX_CONFIG, quickwit_config::ConfigFormat::Yaml, false)
        .await
        .unwrap();

    sandbox.wait_for_indexing_pipelines(1).await.unwrap();

    let split_one = r#"
{"timestamp": 1704067200, "severity": "INFO",  "response_time": 0.5,  "message": "request completed successfully", "service": "web"}
{"timestamp": 1704070800, "severity": "ERROR", "response_time": 5.2,  "message": "database connection timeout",     "service": "api"}
{"timestamp": 1704074400, "severity": "WARN",  "response_time": 1.8,  "message": "slow query detected in database", "service": "web"}
"#;
    ingest(
        &client,
        "tantivy-df-test",
        quickwit_rest_client::models::IngestSource::Str(split_one.to_string()),
        CommitType::WaitFor,
    )
    .await
    .unwrap();

    let split_two = r#"
{"timestamp": 1704078000, "severity": "INFO",  "response_time": 0.3,  "message": "health check passed",             "service": "api"}
{"timestamp": 1704081600, "severity": "ERROR", "response_time": 10.0, "message": "connection refused by upstream",   "service": "web"}
"#;
    ingest(
        &client,
        "tantivy-df-test",
        quickwit_rest_client::models::IngestSource::Str(split_two.to_string()),
        CommitType::WaitFor,
    )
    .await
    .unwrap();

    sandbox
        .wait_for_splits("tantivy-df-test", Some(vec![SplitState::Published]), 2)
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

    assert_eq!(
        total_rows(&batches),
        5,
        "expected 5 rows, got {}",
        total_rows(&batches)
    );
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

    assert_eq!(
        total_rows(&batches),
        3,
        "expected 3 rows with timestamp >= 1704074400"
    );
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tantivy_forced_schema_via_ddl() {
    let sandbox = start_sandbox().await;
    create_index_and_ingest(&sandbox).await;

    let metastore = metastore_client(&sandbox);
    let builder = session_builder(&sandbox, metastore);

    let batches = run_sql(
        &builder,
        r#"CREATE OR REPLACE EXTERNAL TABLE "tantivy-df-test" (
             response_time VARCHAR,
             service VARCHAR,
             _document VARCHAR,
             missing_col VARCHAR
           ) STORED AS tantivy LOCATION 'tantivy-df-test';
           SELECT response_time, missing_col, _document
           FROM "tantivy-df-test"
           WHERE service = 'api'
           ORDER BY response_time"#,
    )
    .await;

    assert_eq!(total_rows(&batches), 2);
    let batch = &batches[0];
    assert!(matches!(
        batch.column_by_name("response_time").unwrap().data_type(),
        DataType::Utf8 | DataType::Utf8View
    ));
    assert!(matches!(
        batch.column_by_name("missing_col").unwrap().data_type(),
        DataType::Utf8 | DataType::Utf8View
    ));
    assert!(matches!(
        batch.column_by_name("_document").unwrap().data_type(),
        DataType::Utf8 | DataType::Utf8View
    ));

    let response_time = utf8_values(batch.column_by_name("response_time").unwrap());
    assert_eq!(response_time.value(0), "0.3");
    assert_eq!(response_time.value(1), "5.2");

    let missing_col = utf8_values(batch.column_by_name("missing_col").unwrap());
    assert_eq!(missing_col.null_count(), 2);

    let document_col = utf8_values(batch.column_by_name("_document").unwrap());
    assert!(document_col.value(0).contains("api"));
    assert!(document_col.value(1).contains("api"));

    sandbox.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tantivy_substrait_uses_base_schema_for_forced_schema() {
    let sandbox = start_sandbox().await;
    create_index_and_ingest(&sandbox).await;

    let metastore = metastore_client(&sandbox);
    let builder = session_builder(&sandbox, metastore);
    let ctx = builder.build_session().unwrap();

    ctx.sql(
        r#"CREATE OR REPLACE EXTERNAL TABLE "tantivy-df-test" (
             response_time VARCHAR,
             service VARCHAR,
             missing_col VARCHAR
           ) STORED AS tantivy LOCATION 'tantivy-df-test'"#,
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    let df = ctx
        .sql(
            r#"SELECT response_time, missing_col
               FROM "tantivy-df-test"
               WHERE service = 'api'
               ORDER BY response_time"#,
        )
        .await
        .unwrap();
    let plan = df.into_optimized_plan().unwrap();
    let substrait_plan = to_substrait_plan(&plan, &ctx.state()).unwrap();
    let plan_bytes = substrait_plan.encode_to_vec();

    let stream = builder.execute_substrait(&plan_bytes).await.unwrap();
    let batches = collect_stream(stream).await.unwrap();

    assert_eq!(total_rows(&batches), 2);
    let batch = &batches[0];
    assert!(matches!(
        batch.column_by_name("response_time").unwrap().data_type(),
        DataType::Utf8 | DataType::Utf8View
    ));
    assert!(matches!(
        batch.column_by_name("missing_col").unwrap().data_type(),
        DataType::Utf8 | DataType::Utf8View
    ));

    let response_time = utf8_values(batch.column_by_name("response_time").unwrap());
    assert_eq!(response_time.value(0), "0.3");
    assert_eq!(response_time.value(1), "5.2");

    let missing_col = utf8_values(batch.column_by_name("missing_col").unwrap());
    assert_eq!(missing_col.null_count(), 2);

    sandbox.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tantivy_distributed_aggregation_uses_split_tasks() {
    unsafe {
        std::env::set_var("QW_DISABLE_TELEMETRY", "1");
        std::env::set_var("QW_ENABLE_DATAFUSION_ENDPOINT", "true");
    }
    quickwit_common::setup_logging_for_tests();

    let sandbox = ClusterSandboxBuilder::default()
        .add_node(QuickwitService::supported_services())
        .add_node([QuickwitService::Searcher])
        .build_and_start()
        .await;
    create_index_and_ingest_two_splits(&sandbox).await;

    let metastore = metastore_client(&sandbox);
    let builder = distributed_session_builder(&sandbox, metastore);
    let ddl = r#"CREATE OR REPLACE EXTERNAL TABLE "tantivy-df-test" (
          service VARCHAR NOT NULL
        ) STORED AS tantivy LOCATION 'tantivy-df-test'"#;
    let agg_sql = format!(
        "{ddl}; SELECT service, COUNT(*) as cnt \
         FROM \"tantivy-df-test\" GROUP BY service ORDER BY service"
    );

    let ctx = builder.build_session().unwrap();
    let fragments: Vec<&str> = agg_sql
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    ctx.sql(fragments[0])
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let df = ctx.sql(fragments[1]).await.unwrap();
    let plan = df.clone().create_physical_plan().await.unwrap();
    let plan_str = format!(
        "{}",
        datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
    );
    println!("=== Tantivy distributed physical plan ===\n{plan_str}");

    assert!(
        plan_str.contains("DistributedExec") && plan_str.contains("PartitionIsolatorExec"),
        "expected distributed split tasks in plan:\n{plan_str}"
    );
    assert!(
        plan_str.contains("AggDataSource"),
        "expected native tantivy aggregation datasource in plan:\n{plan_str}"
    );
    assert!(
        !plan_str.contains("SingleTableDataSource"),
        "expected aggregate pushdown to avoid row scan datasource:\n{plan_str}"
    );
    assert!(
        plan_str.contains("NetworkShuffleExec"),
        "expected grouped final merge to repartition partial states by group key:\n{plan_str}"
    );

    let batches = df.collect().await.unwrap();
    assert_eq!(total_rows(&batches), 2);
    let services = arrow::compute::cast(
        batches[0].column_by_name("service").unwrap(),
        &arrow::datatypes::DataType::Utf8,
    )
    .unwrap();
    let services = services
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let counts = batches[0]
        .column_by_name("cnt")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(services.value(0), "api");
    assert_eq!(counts.value(0), 2);
    assert_eq!(services.value(1), "web");
    assert_eq!(counts.value(1), 3);
    sandbox.shutdown().await.unwrap();
}
