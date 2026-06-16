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

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{MemorySchemaProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;

/// Schema provider that routes table lookup across several Quickwit sources.
///
/// DataFusion gives a catalog/schema pair a single [`SchemaProvider`]. Quickwit
/// exposes metrics and Tantivy indexes under the same `quickwit.public` schema,
/// so this provider keeps DDL tables locally and delegates auto-discovery to
/// source-specific providers in priority order.
pub struct QuickwitSchemaProvider {
    providers: Vec<Arc<dyn SchemaProvider>>,
    ddl_tables: MemorySchemaProvider,
}

impl QuickwitSchemaProvider {
    pub fn new(providers: Vec<Arc<dyn SchemaProvider>>) -> Self {
        Self {
            providers,
            ddl_tables: MemorySchemaProvider::new(),
        }
    }
}

impl std::fmt::Debug for QuickwitSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuickwitSchemaProvider")
            .field("num_providers", &self.providers.len())
            .field("num_ddl_tables", &self.ddl_tables.table_names().len())
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for QuickwitSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let mut names = self.ddl_tables.table_names();
        for provider in &self.providers {
            names.extend(provider.table_names());
        }
        names.sort();
        names.dedup();
        names
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        if let Some(provider) = self.ddl_tables.table(name).await? {
            return Ok(Some(provider));
        }

        for provider in &self.providers {
            if let Some(table) = provider.table(name).await? {
                return Ok(Some(table));
            }
        }
        Ok(None)
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
