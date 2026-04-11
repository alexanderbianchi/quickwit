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

//! `TantivyTableProvider` — DataFusion TableProvider for a quickwit tantivy index.
//!
//! Schema is provided at construction time (via DDL or auto-resolve from the
//! newest split). Splits are listed and opened only at `scan()` time.

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use quickwit_metastore::{
    ListSplitsQuery, ListSplitsRequestExt, MetastoreServiceStreamSplitsExt, SplitMetadata,
    SplitState,
};
use quickwit_proto::metastore::{
    ListSplitsRequest, MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::search::SplitIdAndFooterOffsets;
use quickwit_proto::types::IndexUid;
use quickwit_search::SearcherContext;
use quickwit_storage::Storage;
use tantivy_datafusion::{DirectIndexOpener, IndexOpener, SingleTableProvider};
use tracing::debug;

use super::split_opener::QuickwitSplitOpener;

/// TableProvider for a quickwit tantivy index.
///
/// Construction is cheap — only stores the schema and index coordinates.
/// All split I/O is deferred to `scan()`.
#[derive(Debug)]
pub struct TantivyTableProvider {
    schema: SchemaRef,
    metastore: MetastoreServiceClient,
    searcher_context: Arc<SearcherContext>,
    index_uid: IndexUid,
    index_storage: Arc<dyn Storage>,
}

impl TantivyTableProvider {
    /// Create with an explicit schema (from DDL or auto-resolve).
    pub fn with_schema(
        schema: SchemaRef,
        metastore: MetastoreServiceClient,
        searcher_context: Arc<SearcherContext>,
        index_uid: IndexUid,
        index_storage: Arc<dyn Storage>,
    ) -> Self {
        Self {
            schema,
            metastore,
            searcher_context,
            index_uid,
            index_storage,
        }
    }

    /// Create by opening the most recent split to derive the schema.
    ///
    /// Used by the auto-resolve path when no DDL is provided. Opens exactly
    /// one split — cheap enough for table resolution.
    pub async fn try_from_index(
        metastore: MetastoreServiceClient,
        searcher_context: Arc<SearcherContext>,
        index_uid: IndexUid,
        index_storage: Arc<dyn Storage>,
    ) -> DFResult<Self> {
        let splits = list_published_splits(&metastore, &index_uid).await?;
        if splits.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "no published splits found for index '{}'",
                index_uid.index_id
            )));
        }

        let newest = splits
            .iter()
            .max_by_key(|s| s.create_timestamp)
            .unwrap();

        let (index, _) = open_split(&searcher_context, &index_storage, newest).await?;
        // Derive the schema directly from the tantivy index without creating
        // a full SingleTableProvider (which would compute partition stats and
        // trigger synchronous fast field reads).
        let ff_schema = tantivy_datafusion::tantivy_schema_to_arrow_from_index(&index);
        let schema = build_unified_schema(&ff_schema);

        Ok(Self {
            schema,
            metastore,
            searcher_context,
            index_uid,
            index_storage,
        })
    }
}

/// Build the unified schema: fast fields + _score + _document.
/// Mirrors `SingleTableProvider`'s schema without computing partition stats.
fn build_unified_schema(ff_schema: &SchemaRef) -> SchemaRef {
    use arrow::datatypes::{DataType, Field, Schema};

    let mut fields: Vec<Arc<Field>> = ff_schema.fields().to_vec();
    fields.push(Arc::new(Field::new("_score", DataType::Float32, true)));
    fields.push(Arc::new(Field::new("_document", DataType::Utf8, true)));
    Arc::new(Schema::new(fields))
}

fn split_to_footer(split: &SplitMetadata) -> SplitIdAndFooterOffsets {
    SplitIdAndFooterOffsets {
        split_id: split.split_id.clone(),
        split_footer_start: split.footer_offsets.start,
        split_footer_end: split.footer_offsets.end,
        timestamp_start: split.time_range.as_ref().map(|tr| *tr.start()),
        timestamp_end: split.time_range.as_ref().map(|tr| *tr.end()),
        num_docs: split.num_docs as u64,
    }
}

async fn list_published_splits(
    metastore: &MetastoreServiceClient,
    index_uid: &IndexUid,
) -> DFResult<Vec<SplitMetadata>> {
    let query = ListSplitsQuery::for_index(index_uid.clone())
        .with_split_state(SplitState::Published);
    let request = ListSplitsRequest::try_from_list_splits_query(&query)
        .map_err(|err| DataFusionError::Internal(format!("failed to build split query: {err}")))?;
    metastore
        .clone()
        .list_splits(request)
        .await
        .map_err(|err| DataFusionError::Internal(format!("failed to list splits: {err}")))?
        .collect_splits_metadata()
        .await
        .map_err(|err| DataFusionError::Internal(format!("failed to collect splits: {err}")))
}

async fn open_split(
    searcher_context: &Arc<SearcherContext>,
    index_storage: &Arc<dyn Storage>,
    split: &SplitMetadata,
) -> DFResult<(tantivy::Index, quickwit_directories::HotDirectory)> {
    let footer = split_to_footer(split);
    let ephemeral_cache = quickwit_storage::ByteRangeCache::with_infinite_capacity(
        &quickwit_storage::STORAGE_METRICS.shortlived_cache,
    );
    quickwit_search::leaf::open_index_with_caches(
        searcher_context,
        Arc::clone(index_storage),
        &footer,
        None,
        Some(ephemeral_cache),
    )
    .await
    .map_err(|err| {
        DataFusionError::Internal(format!(
            "failed to open split '{}': {err}",
            split.split_id
        ))
    })
}

async fn build_openers(
    searcher_context: &Arc<SearcherContext>,
    index_storage: &Arc<dyn Storage>,
    splits: &[SplitMetadata],
) -> DFResult<Vec<Arc<dyn IndexOpener>>> {
    let mut openers: Vec<Arc<dyn IndexOpener>> = Vec::with_capacity(splits.len());
    for split in splits {
        let footer = split_to_footer(split);
        let (index, _) = open_split(searcher_context, index_storage, split).await?;
        let direct = DirectIndexOpener::new(index);
        let opener = QuickwitSplitOpener::new(
            Arc::clone(searcher_context),
            Arc::clone(index_storage),
            footer,
            direct.schema(),
            direct.segment_sizes(),
            direct.multi_valued_fields(),
        );
        openers.push(Arc::new(opener));
    }
    Ok(openers)
}

#[async_trait]
impl TableProvider for TantivyTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if tantivy_datafusion::extract_full_text_call(f).is_some() {
                    // full_text() is fully handled by tantivy's inverted index —
                    // tell DataFusion not to keep it as a post-filter.
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Inexact
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let splits = list_published_splits(&self.metastore, &self.index_uid).await?;
        debug!(
            index_uid = %self.index_uid,
            num_splits = splits.len(),
            "opening splits for tantivy scan"
        );
        let openers = build_openers(
            &self.searcher_context,
            &self.index_storage,
            &splits,
        )
        .await?;
        let inner = SingleTableProvider::from_splits(openers)?;
        inner.scan(state, projection, filters, limit).await
    }
}
