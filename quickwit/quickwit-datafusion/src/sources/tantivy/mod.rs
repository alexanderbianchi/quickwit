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
//! 2. **Auto-resolve**: `SELECT ... FROM "logs"` — opens the newest split to
//!    derive the schema, then defers all split I/O to scan time.

pub(crate) mod factory;
pub(crate) mod index_resolver;
pub(crate) mod split_opener;
pub(crate) mod table_provider;

#[cfg(test)]
mod tests;

use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::TableProviderFactory;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::execution::SessionState;
use datafusion::prelude::SessionConfig;
use quickwit_proto::metastore::{MetastoreError, MetastoreServiceClient};
use quickwit_proto::search::SplitIdAndFooterOffsets;
use quickwit_search::SearcherContext;
use quickwit_storage::StorageResolver;
use tantivy_datafusion::codec::{OpenerFactoryExt, TantivyCodec};
use tantivy_datafusion::IndexOpener;

use crate::data_source::{DataSourceContributions, QuickwitDataSource};
use self::factory::{TANTIVY_FILE_TYPE, TantivyTableProviderFactory};
use self::index_resolver::{MetastoreTantivyResolver, TantivyIndexResolver};
use self::split_opener::QuickwitSplitOpener;
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
        let resolver = MetastoreTantivyResolver::new(metastore.clone(), storage_resolver.clone());
        Self {
            index_resolver: Arc::new(resolver),
            metastore,
            searcher_context,
            storage_resolver,
        }
    }
}

#[async_trait]
impl QuickwitDataSource for TantivyDataSource {
    fn configure_session(&self, config: &mut SessionConfig) {
        let searcher_context = Arc::clone(&self.searcher_context);
        let storage_resolver = self.storage_resolver.clone();

        let factory: tantivy_datafusion::OpenerFactory = Arc::new(move |metadata| {
            // The identifier encodes "{index_uri}\0{split_id}".
            let (index_uri_str, split_id) =
                QuickwitSplitOpener::parse_identifier(&metadata.identifier)
                    .unwrap_or(("", &metadata.identifier));

            let footer = SplitIdAndFooterOffsets {
                split_id: split_id.to_string(),
                split_footer_start: metadata.footer_start,
                split_footer_end: metadata.footer_end,
                timestamp_start: None,
                timestamp_end: None,
                num_docs: 0,
            };

            // Resolve storage from the index URI encoded in the identifier.
            let index_storage = tokio::task::block_in_place(|| {
                let storage_resolver = storage_resolver.clone();
                let uri_str = index_uri_str.to_string();
                tokio::runtime::Handle::current().block_on(async move {
                    let uri = quickwit_common::uri::Uri::from_str(&uri_str)
                        .expect("invalid index URI in opener identifier");
                    storage_resolver.resolve(&uri).await
                        .expect("failed to resolve storage from index URI")
                })
            });

            let opener = QuickwitSplitOpener::new(
                Arc::clone(&searcher_context),
                index_storage,
                footer,
                metadata.tantivy_schema.clone(),
                metadata.segment_sizes.clone(),
                metadata.multi_valued_fields.clone(),
            );
            Arc::new(opener) as Arc<dyn IndexOpener>
        });
        config.set_opener_factory(factory);
    }

    fn contributions(&self) -> DataSourceContributions {
        use datafusion::physical_optimizer::PhysicalOptimizerRule;

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
            Arc::clone(&self.searcher_context),
        ));
        Some((TANTIVY_FILE_TYPE.to_string(), factory))
    }

    /// Auto-resolve path: opens the newest split to derive schema.
    /// Returns Ok(None) if the index doesn't exist or can't be opened as tantivy.
    async fn create_default_table_provider(
        &self,
        index_name: &str,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let resolved = match self.index_resolver.resolve(index_name).await {
            Ok(resolved) => resolved,
            Err(err) => {
                if is_index_not_found(&err) {
                    return Ok(None);
                }
                return Err(err);
            }
        };

        match TantivyTableProvider::try_from_index(
            self.metastore.clone(),
            Arc::clone(&self.searcher_context),
            resolved.index_uid,
            resolved.storage,
        )
        .await
        {
            Ok(provider) => Ok(Some(Arc::new(provider))),
            Err(err) => {
                tracing::debug!(
                    index_name,
                    error = %err,
                    "index is not a tantivy index, skipping"
                );
                Ok(None)
            }
        }
    }

    async fn register_for_worker(&self, _state: &SessionState) -> DFResult<()> {
        Ok(())
    }

    async fn list_index_names(&self) -> DFResult<Vec<String>> {
        self.index_resolver.list_index_names().await
    }
}
