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

//! `TableProviderFactory` for tantivy indexes.
//!
//! ```sql
//! CREATE EXTERNAL TABLE "logs" (
//!     timestamp    BIGINT,
//!     severity     VARCHAR,
//!     message      VARCHAR,
//!     service      VARCHAR,
//!     response_time DOUBLE
//! ) STORED AS tantivy LOCATION 'logs';
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProviderFactory};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::CreateExternalTable;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_search::SearcherContext;

use super::index_resolver::TantivyIndexResolver;
use super::table_provider::TantivyTableProvider;

pub const TANTIVY_FILE_TYPE: &str = "tantivy";

#[derive(Debug)]
pub struct TantivyTableProviderFactory {
    index_resolver: Arc<dyn TantivyIndexResolver>,
    metastore: MetastoreServiceClient,
    searcher_context: Arc<SearcherContext>,
}

impl TantivyTableProviderFactory {
    pub fn new(
        index_resolver: Arc<dyn TantivyIndexResolver>,
        metastore: MetastoreServiceClient,
        searcher_context: Arc<SearcherContext>,
    ) -> Self {
        Self {
            index_resolver,
            metastore,
            searcher_context,
        }
    }
}

#[async_trait]
impl TableProviderFactory for TantivyTableProviderFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> DFResult<Arc<dyn datafusion::datasource::TableProvider>> {
        let index_name = if cmd.location.is_empty() {
            cmd.name.table().to_string()
        } else {
            cmd.location.clone()
        };

        let arrow_schema = Arc::new(cmd.schema.as_arrow().clone());

        if arrow_schema.fields().is_empty() {
            return Err(DataFusionError::Plan(format!(
                "CREATE EXTERNAL TABLE '{index_name}' must declare at least one column"
            )));
        }

        let resolved = self.index_resolver.resolve(&index_name).await?;

        let provider = TantivyTableProvider::with_schema(
            arrow_schema,
            self.metastore.clone(),
            Arc::clone(&self.searcher_context),
            resolved.index_uid,
            resolved.storage,
        );

        Ok(Arc::new(provider))
    }
}
