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

//! `TantivyTableProvider` — DataFusion TableProvider for a Quickwit tantivy index.
//!
//! Schema is fixed at construction time (via DDL or index metadata). Splits are
//! listed at `scan()` time, but not opened until execution on the worker.

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use quickwit_common::uri::Uri;
use quickwit_metastore::{
    ListSplitsQuery, ListSplitsRequestExt, MetastoreServiceStreamSplitsExt, SplitMetadata,
    SplitState,
};
use quickwit_proto::metastore::{ListSplitsRequest, MetastoreService, MetastoreServiceClient};
use quickwit_proto::types::IndexUid;
use tantivy_datafusion::{SingleTableProvider, SplitDescriptor};
use tracing::debug;

use super::prepared_split_factory::QuickwitSplitPayload;

#[derive(Debug)]
pub struct TantivyTableProvider {
    schema: SchemaRef,
    metastore: MetastoreServiceClient,
    index_uid: IndexUid,
    index_uri: Uri,
    tantivy_schema: tantivy::schema::Schema,
    timestamp_field: Option<String>,
    multi_valued_fields: Vec<String>,
}

impl TantivyTableProvider {
    pub fn with_schema(
        schema: SchemaRef,
        metastore: MetastoreServiceClient,
        index_uid: IndexUid,
        index_uri: Uri,
        tantivy_schema: tantivy::schema::Schema,
        timestamp_field: Option<String>,
    ) -> Self {
        let multi_valued_fields = collect_declared_multi_valued_fields(&schema);
        Self {
            schema,
            metastore,
            index_uid,
            index_uri,
            tantivy_schema,
            timestamp_field,
            multi_valued_fields,
        }
    }

    pub fn try_from_index(
        metastore: MetastoreServiceClient,
        resolved: super::index_resolver::ResolvedIndex,
    ) -> Self {
        Self::with_schema(
            resolved.schema,
            metastore,
            resolved.index_uid,
            resolved.index_uri,
            resolved.tantivy_schema,
            resolved.timestamp_field,
        )
    }
}

fn collect_declared_multi_valued_fields(schema: &SchemaRef) -> Vec<String> {
    schema
        .fields()
        .iter()
        .filter(|field| matches!(field.data_type(), DataType::List(_)))
        .map(|field| field.name().to_string())
        .collect()
}

fn build_inner_fast_field_schema(requested_schema: &SchemaRef) -> SchemaRef {
    let mut fields = Vec::new();

    if requested_schema
        .fields()
        .iter()
        .all(|field| field.name() != "_doc_id")
    {
        fields.push(Field::new("_doc_id", DataType::UInt32, false));
    }
    if requested_schema
        .fields()
        .iter()
        .all(|field| field.name() != "_segment_ord")
    {
        fields.push(Field::new("_segment_ord", DataType::UInt32, false));
    }

    fields.extend(
        requested_schema
            .fields()
            .iter()
            .filter(|field| field.name() != "_score" && field.name() != "_document")
            .map(|field| field.as_ref().clone()),
    );

    Arc::new(Schema::new(fields))
}

fn translate_projection(
    requested_schema: &SchemaRef,
    inner_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> DFResult<Vec<usize>> {
    let projected_indices: Vec<usize> = match projection {
        Some(indices) => indices.clone(),
        None => (0..requested_schema.fields().len()).collect(),
    };

    projected_indices
        .into_iter()
        .map(|projected_idx| {
            let field = requested_schema
                .fields()
                .get(projected_idx)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "projection index {projected_idx} is out of bounds for declared tantivy schema"
                    ))
                })?;

            inner_schema.index_of(field.name()).map_err(|_| {
                DataFusionError::Plan(format!(
                    "declared tantivy field '{}' is not available in the index scan schema",
                    field.name()
                ))
            })
        })
        .collect()
}

fn build_unified_schema(ff_schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Arc<Field>> = ff_schema.fields().to_vec();
    fields.push(Arc::new(Field::new("_score", DataType::Float32, true)));
    fields.push(Arc::new(Field::new("_document", DataType::Utf8, true)));
    Arc::new(Schema::new(fields))
}

fn split_descriptor(
    index_uri: &Uri,
    tantivy_schema: &tantivy::schema::Schema,
    multi_valued_fields: &[String],
    split: &SplitMetadata,
) -> DFResult<SplitDescriptor> {
    let payload = QuickwitSplitPayload {
        index_uri: index_uri.to_string(),
        split_id: split.split_id.clone(),
        split_footer_start: split.footer_offsets.start,
        split_footer_end: split.footer_offsets.end,
    };
    let payload_bytes = serde_json::to_vec(&payload)
        .map_err(|e| DataFusionError::Internal(format!("encode split payload: {e}")))?;
    Ok(SplitDescriptor::new(
        split.split_id.clone(),
        payload_bytes,
        tantivy_schema.clone(),
        multi_valued_fields.to_vec(),
    ))
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct SplitTimeRange {
    start_secs: Option<i64>,
    end_secs_exclusive: Option<i64>,
}

impl SplitTimeRange {
    fn tighten_start(&mut self, start_secs: i64) {
        self.start_secs = Some(match self.start_secs {
            Some(previous) => previous.max(start_secs),
            None => start_secs,
        });
    }

    fn tighten_end(&mut self, end_secs_exclusive: i64) {
        self.end_secs_exclusive = Some(match self.end_secs_exclusive {
            Some(previous) => previous.min(end_secs_exclusive),
            None => end_secs_exclusive,
        });
    }

    fn is_empty(self) -> bool {
        matches!(
            (self.start_secs, self.end_secs_exclusive),
            (Some(start), Some(end)) if start >= end
        )
    }
}

fn extract_split_time_range(filters: &[Expr], timestamp_field: Option<&str>) -> SplitTimeRange {
    let Some(timestamp_field) = timestamp_field else {
        return SplitTimeRange::default();
    };
    let mut time_range = SplitTimeRange::default();
    for filter in filters {
        collect_timestamp_filter(filter, timestamp_field, &mut time_range);
    }
    time_range
}

fn collect_timestamp_filter(expr: &Expr, timestamp_field: &str, time_range: &mut SplitTimeRange) {
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
        return;
    };

    match op {
        Operator::And => {
            collect_timestamp_filter(left, timestamp_field, time_range);
            collect_timestamp_filter(right, timestamp_field, time_range);
        }
        Operator::GtEq => {
            if let (Some(column), Some(value)) = (column_name(left), timestamp_floor_secs(right))
                && column == timestamp_field
            {
                time_range.tighten_start(value);
            } else if let (Some(value), Some(column)) =
                (timestamp_floor_secs(left), column_name(right))
                && column == timestamp_field
            {
                time_range.tighten_end(value.saturating_add(1));
            }
        }
        Operator::Gt => {
            if let (Some(column), Some(value)) = (column_name(left), timestamp_floor_secs(right))
                && column == timestamp_field
            {
                time_range.tighten_start(value.saturating_add(1));
            } else if let (Some(value), Some(column)) =
                (timestamp_ceil_secs(left), column_name(right))
                && column == timestamp_field
            {
                time_range.tighten_end(value);
            }
        }
        Operator::Lt => {
            if let (Some(column), Some(value)) = (column_name(left), timestamp_ceil_secs(right))
                && column == timestamp_field
            {
                time_range.tighten_end(value);
            } else if let (Some(value), Some(column)) =
                (timestamp_floor_secs(left), column_name(right))
                && column == timestamp_field
            {
                time_range.tighten_start(value.saturating_add(1));
            }
        }
        Operator::LtEq => {
            if let (Some(column), Some(value)) = (column_name(left), timestamp_floor_secs(right))
                && column == timestamp_field
            {
                time_range.tighten_end(value.saturating_add(1));
            } else if let (Some(value), Some(column)) =
                (timestamp_floor_secs(left), column_name(right))
                && column == timestamp_field
            {
                time_range.tighten_start(value);
            }
        }
        Operator::Eq => {
            if let (Some(column), Some(value)) = (column_name(left), timestamp_floor_secs(right))
                && column == timestamp_field
            {
                time_range.tighten_start(value);
                time_range.tighten_end(value.saturating_add(1));
            } else if let (Some(value), Some(column)) =
                (timestamp_floor_secs(left), column_name(right))
                && column == timestamp_field
            {
                time_range.tighten_start(value);
                time_range.tighten_end(value.saturating_add(1));
            }
        }
        _ => {}
    }
}

fn column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(column) => Some(column.name()),
        Expr::Cast(datafusion::logical_expr::Cast { expr, .. })
        | Expr::TryCast(datafusion::logical_expr::TryCast { expr, .. }) => column_name(expr),
        _ => None,
    }
}

fn timestamp_floor_secs(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(value, _) => scalar_timestamp_floor_secs(value),
        Expr::Cast(datafusion::logical_expr::Cast { expr, .. })
        | Expr::TryCast(datafusion::logical_expr::TryCast { expr, .. }) => {
            timestamp_floor_secs(expr)
        }
        _ => None,
    }
}

fn timestamp_ceil_secs(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(value, _) => scalar_timestamp_ceil_secs(value),
        Expr::Cast(datafusion::logical_expr::Cast { expr, .. })
        | Expr::TryCast(datafusion::logical_expr::TryCast { expr, .. }) => {
            timestamp_ceil_secs(expr)
        }
        _ => None,
    }
}

fn scalar_timestamp_floor_secs(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::TimestampSecond(Some(value), _) | ScalarValue::Int64(Some(value)) => {
            Some(*value)
        }
        ScalarValue::TimestampMillisecond(Some(value), _) => Some(floor_div(*value, 1_000)),
        ScalarValue::TimestampMicrosecond(Some(value), _) => Some(floor_div(*value, 1_000_000)),
        ScalarValue::TimestampNanosecond(Some(value), _) => Some(floor_div(*value, 1_000_000_000)),
        ScalarValue::Int32(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt32(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt64(Some(value)) => i64::try_from(*value).ok(),
        _ => None,
    }
}

fn scalar_timestamp_ceil_secs(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::TimestampSecond(Some(value), _) | ScalarValue::Int64(Some(value)) => {
            Some(*value)
        }
        ScalarValue::TimestampMillisecond(Some(value), _) => Some(ceil_div(*value, 1_000)),
        ScalarValue::TimestampMicrosecond(Some(value), _) => Some(ceil_div(*value, 1_000_000)),
        ScalarValue::TimestampNanosecond(Some(value), _) => Some(ceil_div(*value, 1_000_000_000)),
        ScalarValue::Int32(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt32(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt64(Some(value)) => i64::try_from(*value).ok(),
        _ => None,
    }
}

fn floor_div(value: i64, divisor: i64) -> i64 {
    value.div_euclid(divisor)
}

fn ceil_div(value: i64, divisor: i64) -> i64 {
    value
        .div_euclid(divisor)
        .saturating_add(i64::from(value.rem_euclid(divisor) != 0))
}

async fn list_published_splits(
    metastore: &MetastoreServiceClient,
    index_uid: &IndexUid,
    time_range: SplitTimeRange,
) -> DFResult<Vec<SplitMetadata>> {
    if time_range.is_empty() {
        return Ok(Vec::new());
    }

    let mut query =
        ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Published);
    if let Some(start_secs) = time_range.start_secs {
        query = query.with_time_range_start_gte(start_secs);
    }
    if let Some(end_secs) = time_range.end_secs_exclusive {
        query = query.with_time_range_end_lt(end_secs);
    }
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
            .map(|filter| {
                if tantivy_datafusion::extract_full_text_call(filter).is_some() {
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
        let time_range = extract_split_time_range(filters, self.timestamp_field.as_deref());
        let splits = list_published_splits(&self.metastore, &self.index_uid, time_range).await?;
        debug!(
            index_uid = %self.index_uid,
            num_splits = splits.len(),
            ?time_range,
            "planning tantivy split descriptors"
        );

        let split_descriptors = splits
            .iter()
            .map(|split| {
                split_descriptor(
                    &self.index_uri,
                    &self.tantivy_schema,
                    &self.multi_valued_fields,
                    split,
                )
            })
            .collect::<DFResult<Vec<_>>>()?;

        let inner = SingleTableProvider::from_split_descriptors_with_fast_field_schema(
            split_descriptors,
            build_inner_fast_field_schema(&self.schema),
        )?;
        let translated_projection = translate_projection(
            &self.schema,
            &build_unified_schema(&build_inner_fast_field_schema(&self.schema)),
            projection,
        )?;
        inner
            .scan(state, Some(&translated_projection), filters, limit)
            .await
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::{col, lit};

    use super::*;

    fn schema(fields: Vec<Field>) -> SchemaRef {
        Arc::new(Schema::new(fields))
    }

    #[test]
    fn test_build_inner_fast_field_schema_prepends_hidden_columns() {
        let requested_schema = schema(vec![
            Field::new("severity", DataType::Utf8, true),
            Field::new("timestamp", DataType::Int64, true),
        ]);

        let inner_fast_field_schema = build_inner_fast_field_schema(&requested_schema);
        let field_names: Vec<_> = inner_fast_field_schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();

        assert_eq!(
            field_names,
            vec!["_doc_id", "_segment_ord", "severity", "timestamp"]
        );
    }

    #[test]
    fn test_translate_projection_uses_declared_field_names() {
        let requested_schema = schema(vec![
            Field::new("severity", DataType::Utf8, true),
            Field::new("_score", DataType::Float32, true),
            Field::new("timestamp", DataType::Int64, true),
        ]);
        let inner_fast_field_schema = build_inner_fast_field_schema(&requested_schema);
        let inner_schema = build_unified_schema(&inner_fast_field_schema);

        assert_eq!(
            translate_projection(&requested_schema, &inner_schema, None).unwrap(),
            vec![2, 4, 3]
        );
        assert_eq!(
            translate_projection(&requested_schema, &inner_schema, Some(&vec![0, 2])).unwrap(),
            vec![2, 3]
        );
    }

    #[test]
    fn test_extract_split_time_range_from_timestamp_filters() {
        let filters = vec![
            col("timestamp").gt_eq(lit(ScalarValue::TimestampMicrosecond(
                Some(1_704_074_400_000_000),
                None,
            ))),
            col("timestamp").lt(lit(ScalarValue::TimestampMicrosecond(
                Some(1_704_078_000_000_000),
                None,
            ))),
        ];

        let time_range = extract_split_time_range(&filters, Some("timestamp"));

        assert_eq!(
            time_range,
            SplitTimeRange {
                start_secs: Some(1_704_074_400),
                end_secs_exclusive: Some(1_704_078_000),
            }
        );
    }

    #[test]
    fn test_extract_split_time_range_handles_reversed_comparisons() {
        let filters = vec![
            lit(ScalarValue::TimestampMicrosecond(Some(1_500_000), None)).lt(col("timestamp")),
            lit(ScalarValue::TimestampMicrosecond(Some(3_000_000), None)).gt_eq(col("timestamp")),
        ];

        let time_range = extract_split_time_range(&filters, Some("timestamp"));

        assert_eq!(
            time_range,
            SplitTimeRange {
                start_secs: Some(2),
                end_secs_exclusive: Some(4),
            }
        );
    }
}
