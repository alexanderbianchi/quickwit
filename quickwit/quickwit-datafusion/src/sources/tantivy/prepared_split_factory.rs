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

//! Worker-side split runtime factory for Quickwit-backed tantivy scans.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use quickwit_common::uri::Uri;
use quickwit_proto::search::SplitIdAndFooterOffsets;
use quickwit_search::SearcherContext;
use quickwit_storage::{ByteRangeCache, StorageResolver};
use serde::{Deserialize, Serialize};
use tantivy_datafusion::{PreparedSplit, SplitDescriptor, SplitRuntimeFactory};
use tokio::sync::{Mutex, OnceCell};

type PreparedSplitCell = Arc<OnceCell<Arc<PreparedSplit>>>;
type PreparedSplitCache = Arc<Mutex<HashMap<String, PreparedSplitCell>>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuickwitSplitPayload {
    pub index_uri: String,
    pub split_id: String,
    pub split_footer_start: u64,
    pub split_footer_end: u64,
}

impl QuickwitSplitPayload {
    pub fn cache_key(&self) -> String {
        format!(
            "{}\0{}\0{}\0{}",
            self.index_uri, self.split_id, self.split_footer_start, self.split_footer_end
        )
    }

    pub fn split_footer(&self) -> SplitIdAndFooterOffsets {
        SplitIdAndFooterOffsets {
            split_id: self.split_id.clone(),
            split_footer_start: self.split_footer_start,
            split_footer_end: self.split_footer_end,
            timestamp_start: None,
            timestamp_end: None,
            num_docs: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct QuickwitPreparedSplitFactory {
    searcher_context: Arc<SearcherContext>,
    storage_resolver: StorageResolver,
    prepared_splits: PreparedSplitCache,
}

impl QuickwitPreparedSplitFactory {
    pub fn new(searcher_context: Arc<SearcherContext>, storage_resolver: StorageResolver) -> Self {
        Self {
            searcher_context,
            storage_resolver,
            prepared_splits: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl SplitRuntimeFactory for QuickwitPreparedSplitFactory {
    async fn prepare_split(&self, descriptor: &SplitDescriptor) -> DFResult<Arc<PreparedSplit>> {
        let payload: QuickwitSplitPayload =
            serde_json::from_slice(&descriptor.payload).map_err(|e| {
                DataFusionError::Internal(format!("decode quickwit split payload: {e}"))
            })?;
        let cache_key = payload.cache_key();
        let cell = {
            let mut prepared_splits = self.prepared_splits.lock().await;
            Arc::clone(
                prepared_splits
                    .entry(cache_key)
                    .or_insert_with(|| Arc::new(OnceCell::new())),
            )
        };

        cell.get_or_try_init(|| async {
            let index_uri = Uri::from_str(&payload.index_uri).map_err(|e| {
                DataFusionError::Internal(format!("parse index URI '{}': {e}", payload.index_uri))
            })?;
            let storage = self
                .storage_resolver
                .resolve(&index_uri)
                .await
                .map_err(|e| DataFusionError::Internal(format!("resolve split storage: {e}")))?;
            let ephemeral_cache = ByteRangeCache::with_infinite_capacity(
                &quickwit_storage::STORAGE_METRICS.shortlived_cache,
            );
            let (index, hot_directory) = quickwit_search::leaf::open_index_with_caches(
                &self.searcher_context,
                storage,
                &payload.split_footer(),
                None,
                Some(ephemeral_cache),
            )
            .await
            .map_err(|e| DataFusionError::Internal(format!("open split with caches: {e}")))?;
            let prepared = PreparedSplit::new(index, Arc::new(hot_directory))?;
            Ok::<Arc<PreparedSplit>, DataFusionError>(Arc::new(prepared))
        })
        .await
        .map(Arc::clone)
    }
}
