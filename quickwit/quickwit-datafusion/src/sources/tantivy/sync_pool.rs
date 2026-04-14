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

//! Quickwit's bounded Rayon-backed [`SyncExecutionPool`] for tantivy work.
//!
//! Uses a **dedicated** [`ThreadPool`] so that DataFusion query execution does
//! not share admission with the legacy search-service path.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use quickwit_common::thread_pool::ThreadPool;
use tantivy_datafusion::SyncExecutionPool;

/// Bounded Rayon-backed sync execution pool for tantivy CPU work.
///
/// Wraps [`ThreadPool`] so the pool size is bounded to `num_cpus` (or a
/// configured value) and scheduled-but-unstarted tasks are cancellable
/// when the awaiting future is dropped.
pub struct RayonSyncExecutionPool {
    pool: ThreadPool,
}

impl std::fmt::Debug for RayonSyncExecutionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RayonSyncExecutionPool").finish_non_exhaustive()
    }
}

impl RayonSyncExecutionPool {
    pub fn new(pool: ThreadPool) -> Self {
        Self { pool }
    }

    /// Access the underlying Rayon thread pool, e.g. for setting
    /// `tantivy::Index::set_executor`.
    pub fn rayon_pool(&self) -> Arc<rayon::ThreadPool> {
        self.pool.get_underlying_rayon_thread_pool()
    }
}

#[async_trait]
impl SyncExecutionPool for RayonSyncExecutionPool {
    async fn run_boxed(
        &self,
        task: Box<dyn FnOnce() -> Result<Box<dyn Any + Send>> + Send>,
    ) -> Result<Box<dyn Any + Send>> {
        self.pool
            .run_cpu_intensive(task)
            .await
            .map_err(|_| {
                DataFusionError::Internal("sync execution task panicked".to_string())
            })?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy_datafusion::sync_exec::run_sync;

    fn test_pool() -> RayonSyncExecutionPool {
        RayonSyncExecutionPool::new(ThreadPool::new("test-df-tantivy", Some(2)))
    }

    #[tokio::test]
    async fn rayon_pool_runs_closure() {
        let pool = test_pool();
        let result = run_sync(&pool, || Ok(42u64)).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn rayon_pool_propagates_err() {
        let pool = test_pool();
        let result = run_sync::<()>(&pool, || {
            Err(DataFusionError::Internal("boom".into()))
        })
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("boom"));
    }

    #[tokio::test]
    async fn rayon_pool_panic_returns_error() {
        let pool = test_pool();
        let result = pool
            .run_boxed(Box::new(|| panic!("intentional")))
            .await;
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("panicked"),
            "expected panic error"
        );
    }
}
