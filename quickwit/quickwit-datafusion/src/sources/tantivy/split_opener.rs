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

//! `QuickwitSplitOpener` — bridges quickwit split storage to tantivy-datafusion's
//! [`IndexOpener`] trait.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use quickwit_proto::search::SplitIdAndFooterOffsets;
use quickwit_search::SearcherContext;
use quickwit_storage::{ByteRangeCache, Storage};
use tantivy::Index;
use tantivy_datafusion::IndexOpener;

/// Separator between index_uri and split_id in the serialized identifier.
const ID_SEPARATOR: char = '\0';

/// Opens a single quickwit split as a tantivy `Index`.
#[derive(Clone)]
pub struct QuickwitSplitOpener {
    searcher_context: Arc<SearcherContext>,
    index_storage: Arc<dyn Storage>,
    split_footer: SplitIdAndFooterOffsets,
    tantivy_schema: tantivy::schema::Schema,
    segment_sizes: Vec<u32>,
    multi_valued_fields: Vec<String>,
    /// `{index_uri}\0{split_id}` — encodes both pieces so the distributed
    /// codec can pass the index URI to the worker's opener factory.
    identifier: String,
}

impl QuickwitSplitOpener {
    pub fn new(
        searcher_context: Arc<SearcherContext>,
        index_storage: Arc<dyn Storage>,
        split_footer: SplitIdAndFooterOffsets,
        tantivy_schema: tantivy::schema::Schema,
        segment_sizes: Vec<u32>,
        multi_valued_fields: Vec<String>,
    ) -> Self {
        let index_uri = index_storage.uri().to_string();
        let identifier = format!("{index_uri}{ID_SEPARATOR}{}", split_footer.split_id);
        Self {
            searcher_context,
            index_storage,
            split_footer,
            tantivy_schema,
            segment_sizes,
            multi_valued_fields,
            identifier,
        }
    }

    /// Parse an identifier back into `(index_uri, split_id)`.
    pub fn parse_identifier(identifier: &str) -> Option<(&str, &str)> {
        identifier.split_once(ID_SEPARATOR)
    }
}

impl std::fmt::Debug for QuickwitSplitOpener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuickwitSplitOpener")
            .field("split_id", &self.split_footer.split_id)
            .finish()
    }
}

#[async_trait]
impl IndexOpener for QuickwitSplitOpener {
    async fn open(&self) -> Result<Index> {
        let ephemeral_cache = ByteRangeCache::with_infinite_capacity(
            &quickwit_storage::STORAGE_METRICS.shortlived_cache,
        );
        let (index, _hot_directory) = quickwit_search::leaf::open_index_with_caches(
            &self.searcher_context,
            Arc::clone(&self.index_storage),
            &self.split_footer,
            None,
            Some(ephemeral_cache),
        )
        .await
        .map_err(|err| DataFusionError::Internal(format!("failed to open split: {err}")))?;
        Ok(index)
    }

    fn schema(&self) -> tantivy::schema::Schema {
        self.tantivy_schema.clone()
    }

    fn segment_sizes(&self) -> Vec<u32> {
        self.segment_sizes.clone()
    }

    fn identifier(&self) -> &str {
        &self.identifier
    }

    fn footer_range(&self) -> (u64, u64) {
        (
            self.split_footer.split_footer_start,
            self.split_footer.split_footer_end,
        )
    }

    fn needs_warmup(&self) -> bool {
        true
    }

    fn multi_valued_fields(&self) -> Vec<String> {
        self.multi_valued_fields.clone()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
