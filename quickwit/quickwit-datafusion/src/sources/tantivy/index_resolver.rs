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
//! Planning resolves index metadata and the canonical schema from the metastore
//! and doc mapper only. It does not open any splits.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result as DFResult};
use quickwit_common::uri::Uri;
use quickwit_config::build_doc_mapper;
use quickwit_metastore::{IndexMetadataResponseExt, ListIndexesMetadataResponseExt};
use quickwit_proto::metastore::{
    IndexMetadataRequest, ListIndexesMetadataRequest, MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::types::IndexUid;
use tracing::debug;

/// Planner-visible information about a Quickwit index.
pub struct ResolvedIndex {
    pub index_uid: IndexUid,
    pub index_uri: Uri,
    pub schema: Arc<arrow::datatypes::Schema>,
    pub tantivy_schema: tantivy::schema::Schema,
}

fn build_catalog_schema(tantivy_schema: &tantivy::schema::Schema) -> Arc<Schema> {
    let fast_field_schema = tantivy_datafusion::tantivy_schema_to_arrow(tantivy_schema);
    let mut fields: Vec<Field> = fast_field_schema
        .fields()
        .iter()
        .filter(|field| field.name() != "_doc_id" && field.name() != "_segment_ord")
        .map(|field| field.as_ref().clone())
        .collect();
    fields.push(Field::new("_score", DataType::Float32, true));
    fields.push(Field::new("_document", DataType::Utf8, true));
    Arc::new(Schema::new(fields))
}

#[async_trait]
pub trait TantivyIndexResolver: Send + Sync + std::fmt::Debug {
    async fn resolve(&self, index_name: &str) -> DFResult<ResolvedIndex>;
    async fn list_index_names(&self) -> DFResult<Vec<String>>;
}

#[derive(Clone)]
pub struct MetastoreTantivyResolver {
    metastore: MetastoreServiceClient,
}

impl MetastoreTantivyResolver {
    pub fn new(metastore: MetastoreServiceClient) -> Self {
        Self { metastore }
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
            .map_err(|err| DataFusionError::External(Box::new(err)))?;

        let index_metadata = response
            .deserialize_index_metadata()
            .map_err(|err| DataFusionError::External(Box::new(err)))?;

        let doc_mapper = build_doc_mapper(
            &index_metadata.index_config.doc_mapping,
            &index_metadata.index_config.search_settings,
        )
        .map_err(|err| {
            DataFusionError::Internal(format!(
                "failed to build doc mapper for '{index_name}': {err}"
            ))
        })?;

        let index_uid = index_metadata.index_uid.clone();
        let index_uri = index_metadata.index_config.index_uri.clone();
        let tantivy_schema = doc_mapper.schema().clone();
        let schema = build_catalog_schema(&tantivy_schema);

        debug!(%index_uid, %index_uri, "resolved tantivy index metadata");

        Ok(ResolvedIndex {
            index_uid,
            index_uri,
            schema,
            tantivy_schema,
        })
    }

    async fn list_index_names(&self) -> DFResult<Vec<String>> {
        let response = self
            .metastore
            .clone()
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;

        let indexes = response
            .deserialize_indexes_metadata()
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;

        Ok(indexes
            .into_iter()
            .map(|idx| idx.index_config.index_id)
            .collect())
    }
}
