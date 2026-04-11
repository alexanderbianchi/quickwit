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

//! Index resolution for the tantivy/logs data source.
//!
//! `MetastoreTantivyResolver` resolves an index name to a storage handle and
//! split metadata needed to construct `QuickwitSplitOpener` instances.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::Result as DFResult;
use quickwit_metastore::{IndexMetadataResponseExt, ListIndexesMetadataResponseExt};
use quickwit_proto::metastore::{
    IndexMetadataRequest, ListIndexesMetadataRequest, MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::types::IndexUid;
use quickwit_storage::{Storage, StorageResolver};
use tracing::debug;

/// Resources needed to scan a tantivy index: the index UID and a storage handle.
pub struct ResolvedIndex {
    pub index_uid: IndexUid,
    pub storage: Arc<dyn Storage>,
}

/// Resolves an index name to the resources needed for scanning.
#[async_trait]
pub trait TantivyIndexResolver: Send + Sync + std::fmt::Debug {
    /// Resolve an index name to its UID and storage handle.
    async fn resolve(&self, index_name: &str) -> DFResult<ResolvedIndex>;

    /// List all index names available from this resolver.
    async fn list_index_names(&self) -> DFResult<Vec<String>>;
}

/// Production resolver backed by the quickwit metastore.
#[derive(Clone)]
pub struct MetastoreTantivyResolver {
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
}

impl MetastoreTantivyResolver {
    pub fn new(metastore: MetastoreServiceClient, storage_resolver: StorageResolver) -> Self {
        Self {
            metastore,
            storage_resolver,
        }
    }
}

impl std::fmt::Debug for MetastoreTantivyResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetastoreTantivyResolver").finish()
    }
}

#[async_trait]
impl TantivyIndexResolver for MetastoreTantivyResolver {
    async fn resolve(&self, index_name: &str) -> DFResult<ResolvedIndex> {
        debug!(index_name, "resolving tantivy index");

        let response = self
            .metastore
            .clone()
            .index_metadata(IndexMetadataRequest::for_index_id(index_name.to_string()))
            .await
            .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?;

        let index_metadata = response
            .deserialize_index_metadata()
            .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?;

        let index_uid = index_metadata.index_uid.clone();
        let index_uri = &index_metadata.index_config.index_uri;

        debug!(%index_uid, %index_uri, "resolved tantivy index metadata");

        let storage = self
            .storage_resolver
            .resolve(index_uri)
            .await
            .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?;

        Ok(ResolvedIndex {
            index_uid,
            storage,
        })
    }

    async fn list_index_names(&self) -> DFResult<Vec<String>> {
        let response = self
            .metastore
            .clone()
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await
            .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?;

        let indexes = response
            .deserialize_indexes_metadata()
            .await
            .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?;

        Ok(indexes
            .into_iter()
            .map(|idx| idx.index_config.index_id)
            .collect())
    }
}
