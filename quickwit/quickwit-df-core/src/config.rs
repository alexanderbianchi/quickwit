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

//! DataFusion config options owned by Quickwit.

use datafusion::common::extensions_options;
use datafusion::config::ConfigExtension;

extensions_options! {
    /// Quickwit-specific request/session options.
    pub struct QuickwitConfig {
        /// Default Quickwit index used when a Substrait plan omits a table name.
        pub event_substrait_index: String, default = String::new()

        /// Execute the physical plan while collecting EXPLAIN ANALYZE metrics.
        pub explain_analyze: bool, default = false
    }
}

impl ConfigExtension for QuickwitConfig {
    const PREFIX: &'static str = "quickwit";
}
