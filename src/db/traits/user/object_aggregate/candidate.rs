use diesel::prelude::*;
use diesel::sql_types::{Jsonb, Nullable};
use diesel_async::RunQueryDsl;
use futures::TryStreamExt;
use serde::Serialize;

use super::bounded_json::ObjectAggregateJsonBound;
use super::filters::apply_object_aggregate_source_filters;
use crate::db::DbConnection;
use crate::db::traits::computed_field::ComputedQuerySnapshot;
use crate::db::traits::search::JsonPredicateExt;
use crate::errors::ApiError;
use crate::models::HubuumObject;
use crate::models::object_aggregate::ObjectAggregateSpec;
use crate::models::search::{FilterField, QueryOptions, QueryParamsExt, SortParam};
use crate::pagination::{Page, finalize_page, finalize_partial_page};
use crate::traits::{CursorPaginated, CursorValue};

#[derive(Debug, Clone, Queryable, Serialize)]
pub(super) struct ObjectAggregateCandidate {
    pub(super) id: i32,
    pub(super) name: String,
    pub(super) collection_id: i32,
    pub(super) hubuum_class_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) data: Option<serde_json::Value>,
    pub(super) description: String,
    pub(super) created_at: chrono::NaiveDateTime,
    pub(super) updated_at: chrono::NaiveDateTime,
}

impl CursorPaginated for ObjectAggregateCandidate {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(field, FilterField::Id)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        match field {
            FilterField::Id => Ok(CursorValue::Integer(i64::from(self.id))),
            _ => Err(ApiError::BadRequest(format!(
                "Field '{field}' is not orderable for object aggregate candidates"
            ))),
        }
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}

pub(super) struct ObjectAggregateCandidateBatch {
    items: Vec<ObjectAggregateCandidate>,
    stopped_by_size: bool,
}

pub(super) struct ObjectAggregateCandidateQuery<'a> {
    query_options: &'a QueryOptions,
    collection_id: i32,
    include_object_data: bool,
    computed_filter_snapshot: Option<&'a ComputedQuerySnapshot>,
}

impl<'a> ObjectAggregateCandidateQuery<'a> {
    pub(super) fn new(
        query_options: &'a QueryOptions,
        collection_id: i32,
        spec: &ObjectAggregateSpec,
    ) -> Self {
        Self {
            query_options,
            collection_id,
            include_object_data: spec.requires_object_data(),
            computed_filter_snapshot: None,
        }
    }

    pub(super) fn include_computed_filter_data(mut self) -> Self {
        self.include_object_data = true;
        self
    }

    pub(super) fn resolved_computed_filters(mut self, snapshot: &'a ComputedQuerySnapshot) -> Self {
        self.computed_filter_snapshot = Some(snapshot);
        self.include_object_data = true;
        self
    }
}

impl ObjectAggregateCandidateBatch {
    pub(super) fn into_page(
        self,
        query_options: &QueryOptions,
    ) -> Result<Page<ObjectAggregateCandidate>, ApiError> {
        if self.stopped_by_size {
            finalize_partial_page(self.items, query_options, true)
        } else {
            finalize_page(self.items, query_options)
        }
    }
}

pub(super) async fn load_aggregate_candidate_batch(
    connection: &mut DbConnection,
    candidate_query: ObjectAggregateCandidateQuery<'_>,
) -> Result<ObjectAggregateCandidateBatch, ApiError> {
    use crate::schema::hubuumobject::dsl::{
        collection_id as object_collection_id, created_at as object_created_at,
        description as object_description, hubuum_class_id, hubuumobject, id as object_id,
        name as object_name, updated_at as object_updated_at,
    };

    let ObjectAggregateCandidateQuery {
        query_options,
        collection_id,
        include_object_data,
        computed_filter_snapshot,
    } = candidate_query;
    let mut query = hubuumobject
        .filter(object_collection_id.eq(collection_id))
        .into_boxed();
    apply_object_aggregate_source_filters!(query, query_options, computed_filter_snapshot);
    crate::apply_query_options!(query, query_options, HubuumObject);
    let data_projection = if include_object_data {
        "data"
    } else {
        "NULL::jsonb"
    };
    let stream = query
        .select((
            object_id,
            object_name,
            object_collection_id,
            hubuum_class_id,
            diesel::dsl::sql::<Nullable<Jsonb>>(data_projection),
            object_description,
            object_created_at,
            object_updated_at,
        ))
        .distinct()
        .load_stream::<ObjectAggregateCandidate>(connection)
        .await?;
    futures::pin_mut!(stream);
    let bound = ObjectAggregateJsonBound::CandidateBatch;
    let mut items = Vec::new();
    let mut serialized_bytes = 2_usize;
    let mut stopped_by_size = false;
    while let Some(candidate) = stream.try_next().await? {
        let candidate_bytes = bound.measure(&candidate)?;
        let next_size = serialized_bytes
            .checked_add(candidate_bytes.saturating_add(1))
            .ok_or_else(|| bound.overflow_error())?;
        if next_size > bound.max_bytes() {
            if items.is_empty() {
                return Err(bound.overflow_error());
            }
            stopped_by_size = true;
            break;
        }
        items.push(candidate);
        serialized_bytes = next_size;
    }
    Ok(ObjectAggregateCandidateBatch {
        items,
        stopped_by_size,
    })
}
