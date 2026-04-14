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
pub(crate) mod table_provider;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::TableProviderFactory;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::execution::SessionState;
use datafusion::prelude::SessionConfig;
use datafusion_substrait::substrait::proto::read_rel::ReadType;
use quickwit_common::is_metrics_index;
use quickwit_proto::metastore::{MetastoreError, MetastoreServiceClient};
use quickwit_search::SearcherContext;
use quickwit_storage::StorageResolver;
use tantivy_datafusion::{SplitRuntimeFactoryExt, TantivyCodec};

use self::factory::{TANTIVY_FILE_TYPE, TantivyTableProviderFactory};
use self::index_resolver::{MetastoreTantivyResolver, TantivyIndexResolver};
use self::prepared_split_factory::QuickwitPreparedSplitFactory;
use self::table_provider::TantivyTableProvider;
use crate::data_source::{DataSourceContributions, QuickwitDataSource};

fn is_index_not_found(err: &datafusion::error::DataFusionError) -> bool {
    match err {
        datafusion::error::DataFusionError::External(boxed) => boxed
            .downcast_ref::<MetastoreError>()
            .map(|me| matches!(me, MetastoreError::NotFound(_)))
            .unwrap_or(false),
        _ => false,
    }
}

/// `QuickwitDataSource` implementation for tantivy/logs indexes.
#[derive(Debug)]
pub struct TantivyDataSource {
    index_resolver: Arc<dyn TantivyIndexResolver>,
    metastore: MetastoreServiceClient,
    searcher_context: Arc<SearcherContext>,
    storage_resolver: StorageResolver,
}

impl TantivyDataSource {
    pub fn new(
        metastore: MetastoreServiceClient,
        storage_resolver: StorageResolver,
        searcher_context: Arc<SearcherContext>,
    ) -> Self {
        let resolver = MetastoreTantivyResolver::new(metastore.clone());
        Self {
            index_resolver: Arc::new(resolver),
            metastore,
            searcher_context,
            storage_resolver,
        }
    }

    async fn resolve_index(
        &self,
        index_name: &str,
    ) -> DFResult<Option<self::index_resolver::ResolvedIndex>> {
        if is_metrics_index(index_name) {
            tracing::debug!(
                index_name,
                "metrics index belongs to parquet source, skipping tantivy"
            );
            return Ok(None);
        }

        match self.index_resolver.resolve(index_name).await {
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
}

#[async_trait]
impl QuickwitDataSource for TantivyDataSource {
    fn configure_session(&self, config: &mut SessionConfig) {
        config.set_split_runtime_factory(Arc::new(QuickwitPreparedSplitFactory::new(
            Arc::clone(&self.searcher_context),
            self.storage_resolver.clone(),
        )));
    }

    fn contributions(&self) -> DataSourceContributions {
        DataSourceContributions::default()
            .with_udf(Arc::new(tantivy_datafusion::full_text_udf()))
            .with_physical_optimizer_rule(Arc::new(tantivy_datafusion::AggPushdown::new()))
            .with_codec_applier(|builder| {
                use datafusion_distributed::DistributedExt;
                builder.with_distributed_user_codec(TantivyCodec)
            })
    }

    fn ddl_registration(&self) -> Option<(String, Arc<dyn TableProviderFactory>)> {
        let factory: Arc<dyn TableProviderFactory> = Arc::new(TantivyTableProviderFactory::new(
            Arc::clone(&self.index_resolver),
            self.metastore.clone(),
        ));
        Some((TANTIVY_FILE_TYPE.to_string(), factory))
    }

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

        let Some(resolved) = self.resolve_index(index_name).await? else {
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

    /// Auto-resolve path: derives schema from index metadata and doc-mapper state.
    /// Returns Ok(None) only when the index is absent or belongs to the metrics source.
    async fn create_default_table_provider(
        &self,
        index_name: &str,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let Some(resolved) = self.resolve_index(index_name).await? else {
            return Ok(None);
        };
        let provider = TantivyTableProvider::try_from_index(self.metastore.clone(), resolved);

        Ok(Some(Arc::new(provider)))
    }

    async fn register_for_worker(&self, _state: &SessionState) -> DFResult<()> {
        Ok(())
    }

    async fn list_index_names(&self) -> DFResult<Vec<String>> {
        self.index_resolver.list_index_names().await
    }
}
