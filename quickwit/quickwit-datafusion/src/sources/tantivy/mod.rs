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

//! Tantivy/logs data source for DataFusion.
//!
//! Supports two table registration paths:
//!
//! 1. **DDL** (recommended): `CREATE EXTERNAL TABLE "logs" (...) STORED AS tantivy LOCATION 'logs'`
//!    — schema is declared, no splits opened until scan.
//!
//! 2. **Auto-resolve**: `SELECT ... FROM "logs"` — resolves index metadata and
//!    doc-mapper schema from the metastore, without opening splits during planning.

pub(crate) mod factory;
pub(crate) mod index_resolver;
pub(crate) mod prepared_split_factory;
pub(crate) mod sync_pool;
pub(crate) mod table_provider;

#[cfg(test)]
mod tests;

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow;
use datafusion::catalog::{MemorySchemaProvider, SchemaProvider, TableProviderFactory};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion_substrait::substrait::proto::read_rel::ReadType;
use quickwit_common::is_metrics_index;
use quickwit_df_core::{
    QuickwitRuntimePlugin, QuickwitRuntimeRegistration, QuickwitSubstraitConsumerExt,
};
use quickwit_proto::metastore::{MetastoreError, MetastoreServiceClient};
use quickwit_search::SearcherContext;
use quickwit_storage::StorageResolver;
use tantivy_datafusion::{
    SplitRuntimeFactory, SplitRuntimeFactoryExt, SyncExecutionPoolExt, SyncExecutionPoolRef,
    TantivyCodec,
};

use self::factory::{TANTIVY_FILE_TYPE, TantivyTableProviderFactory};
use self::index_resolver::{MetastoreTantivyResolver, TantivyIndexResolver};
use self::prepared_split_factory::QuickwitPreparedSplitFactory;
use self::table_provider::TantivyTableProvider;

fn is_index_not_found(err: &datafusion::error::DataFusionError) -> bool {
    match err {
        datafusion::error::DataFusionError::External(boxed) => boxed
            .downcast_ref::<MetastoreError>()
            .map(|me| matches!(me, MetastoreError::NotFound(_)))
            .unwrap_or(false),
        _ => false,
    }
}

async fn resolve_tantivy_index(
    index_resolver: &dyn TantivyIndexResolver,
    index_name: &str,
) -> DFResult<Option<self::index_resolver::ResolvedIndex>> {
    if is_metrics_index(index_name) {
        tracing::debug!(
            index_name,
            "metrics index belongs to parquet source, skipping tantivy"
        );
        return Ok(None);
    }

    match index_resolver.resolve(index_name).await {
        Ok(resolved) => Ok(Some(resolved)),
        Err(err) => {
            if is_index_not_found(&err) {
                Ok(None)
            } else {
                Err(err)
            }
        }
    }
}

/// Runtime/Substrait integration for tantivy/logs indexes.
#[derive(Debug)]
pub struct TantivyDataSource {
    index_resolver: Arc<dyn TantivyIndexResolver>,
    metastore: MetastoreServiceClient,
    searcher_context: Arc<SearcherContext>,
    storage_resolver: StorageResolver,
    sync_pool: SyncExecutionPoolRef,
    rayon_pool: Arc<rayon::ThreadPool>,
}

impl TantivyDataSource {
    pub fn new(
        metastore: MetastoreServiceClient,
        storage_resolver: StorageResolver,
        searcher_context: Arc<SearcherContext>,
    ) -> Self {
        let resolver = MetastoreTantivyResolver::new(metastore.clone());
        let pool = sync_pool::RayonSyncExecutionPool::new(
            quickwit_common::thread_pool::ThreadPool::new("df-tantivy-search", None),
        );
        let rayon_pool = pool.rayon_pool();
        Self {
            index_resolver: Arc::new(resolver),
            metastore,
            searcher_context,
            storage_resolver,
            sync_pool: Arc::new(pool),
            rayon_pool,
        }
    }

    pub fn schema_provider(&self) -> Arc<dyn SchemaProvider> {
        Arc::new(TantivySchemaProvider::new(
            Arc::clone(&self.index_resolver),
            self.metastore.clone(),
        ))
    }
}

#[async_trait]
impl QuickwitRuntimePlugin for TantivyDataSource {
    fn registration(&self) -> QuickwitRuntimeRegistration {
        let split_runtime_factory: Arc<dyn SplitRuntimeFactory> =
            Arc::new(QuickwitPreparedSplitFactory::new(
                Arc::clone(&self.searcher_context),
                self.storage_resolver.clone(),
                Arc::clone(&self.rayon_pool),
            ));
        let sync_pool = Arc::clone(&self.sync_pool);
        let factory: Arc<dyn TableProviderFactory> = Arc::new(TantivyTableProviderFactory::new(
            Arc::clone(&self.index_resolver),
            self.metastore.clone(),
        ));

        QuickwitRuntimeRegistration::default()
            .with_session_config_setter(move |config| {
                config.set_split_runtime_factory(Arc::clone(&split_runtime_factory));
                config.set_sync_execution_pool(Arc::clone(&sync_pool));
            })
            .with_udf(Arc::new(tantivy_datafusion::full_text_udf()))
            .with_physical_optimizer_rule(Arc::new(tantivy_datafusion::AggPushdown::new()))
            .with_distributed_user_codec(Arc::new(TantivyCodec))
            .with_table_factory(TANTIVY_FILE_TYPE, factory)
    }

    async fn register_for_worker(
        &self,
        _state: &datafusion::execution::SessionState,
    ) -> DFResult<()> {
        Ok(())
    }
}

#[async_trait]
impl QuickwitSubstraitConsumerExt for TantivyDataSource {
    async fn try_consume_read_rel(
        &self,
        rel: &datafusion_substrait::substrait::proto::ReadRel,
        schema_hint: Option<arrow::datatypes::SchemaRef>,
    ) -> DFResult<Option<(String, Arc<dyn TableProvider>)>> {
        let Some(ReadType::NamedTable(nt)) = &rel.read_type else {
            return Ok(None);
        };
        let Some(index_name) = nt.names.last() else {
            return Ok(None);
        };

        let Some(resolved) =
            resolve_tantivy_index(self.index_resolver.as_ref(), index_name).await?
        else {
            return Ok(None);
        };

        let schema = schema_hint.unwrap_or_else(|| Arc::clone(&resolved.schema));
        let provider = TantivyTableProvider::with_schema(
            schema,
            self.metastore.clone(),
            resolved.index_uid,
            resolved.index_uri,
            resolved.tantivy_schema,
        );
        Ok(Some((index_name.to_string(), Arc::new(provider))))
    }
}

/// Native OSS `SchemaProvider` for tantivy/log indexes.
pub struct TantivySchemaProvider {
    index_resolver: Arc<dyn TantivyIndexResolver>,
    metastore: MetastoreServiceClient,
    ddl_tables: MemorySchemaProvider,
}

impl TantivySchemaProvider {
    pub fn new(
        index_resolver: Arc<dyn TantivyIndexResolver>,
        metastore: MetastoreServiceClient,
    ) -> Self {
        Self {
            index_resolver,
            metastore,
            ddl_tables: MemorySchemaProvider::new(),
        }
    }
}

impl std::fmt::Debug for TantivySchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TantivySchemaProvider")
            .field("num_ddl_tables", &self.ddl_tables.table_names().len())
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for TantivySchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let resolver = Arc::clone(&self.index_resolver);
        let mut names = self.ddl_tables.table_names();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                if let Ok(mut resolved_names) = resolver.list_index_names().await {
                    resolved_names.retain(|index_name| !is_metrics_index(index_name));
                    names.append(&mut resolved_names);
                }
            })
        });
        names.sort();
        names.dedup();
        names
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        if let Some(provider) = self.ddl_tables.table(name).await? {
            return Ok(Some(provider));
        }

        let Some(resolved) = resolve_tantivy_index(self.index_resolver.as_ref(), name).await?
        else {
            return Ok(None);
        };
        let provider = TantivyTableProvider::try_from_index(self.metastore.clone(), resolved);
        Ok(Some(Arc::new(provider)))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.ddl_tables.table_exist(name)
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.ddl_tables.register_table(name, table)
    }

    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.ddl_tables.deregister_table(name)
    }
}
