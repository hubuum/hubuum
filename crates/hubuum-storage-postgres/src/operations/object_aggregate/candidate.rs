use diesel::prelude::*;
use diesel::sql_types::{Jsonb, Nullable};
use diesel_async::RunQueryDsl;
use futures_util::TryStreamExt;
use serde::Serialize;

use super::bounded_json::ObjectAggregateJsonBound;
use hubuum_query::{CursorValue, FilterField, QueryOptions, SortParam, encode_cursor_values};
use hubuum_storage_core::{StorageObjectAggregateSpec, UnifiedSearchResourceScope};

use crate::cursor::{CursorSqlField, CursorSqlType};
use crate::operations::catalog::{apply_object_filters, object_query};
use crate::operations::computed_objects::query::{
    ComputedQuerySnapshot, computed_filter_predicate,
};
use crate::{PostgresConnection, PostgresStorageError};

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

pub(super) struct ObjectAggregateCandidateBatch {
    items: Vec<ObjectAggregateCandidate>,
    stopped_by_size: bool,
}

pub(super) struct ObjectAggregateCandidateQuery<'a> {
    query_options: &'a QueryOptions,
    collection_id: i32,
    resource_scope: Option<&'a UnifiedSearchResourceScope>,
    include_object_data: bool,
    computed_filter_snapshot: Option<&'a ComputedQuerySnapshot>,
}

impl<'a> ObjectAggregateCandidateQuery<'a> {
    pub(super) fn new(
        query_options: &'a QueryOptions,
        collection_id: i32,
        spec: &StorageObjectAggregateSpec,
    ) -> Self {
        Self {
            query_options,
            collection_id,
            resource_scope: None,
            include_object_data: spec.requires_object_data(),
            computed_filter_snapshot: None,
        }
    }

    pub(super) fn resource_scope(
        mut self,
        resource_scope: Option<&'a UnifiedSearchResourceScope>,
    ) -> Self {
        self.resource_scope = resource_scope;
        self
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
    ) -> Result<ObjectAggregateCandidatePage, PostgresStorageError> {
        let limit = query_options.limit().ok_or_else(|| {
            PostgresStorageError::internal("aggregate candidate page is missing its limit")
        })?;
        if limit == 0 {
            return Err(PostgresStorageError::bad_request(
                "aggregate candidate page limit must be positive",
            ));
        }
        let mut items = self.items;
        let has_more = self.stopped_by_size || items.len() > limit;
        items.truncate(limit);
        let next_cursor = has_more
            .then(|| {
                let candidate = items.last().ok_or_else(|| {
                    PostgresStorageError::internal(
                        "partial aggregate candidate page cannot be empty",
                    )
                })?;
                encode_cursor_values(
                    &candidate_sorts(),
                    vec![CursorValue::Integer(i64::from(candidate.id))],
                )
                .map_err(|error| PostgresStorageError::internal(error.to_string()))
            })
            .transpose()?;
        Ok(ObjectAggregateCandidatePage { items, next_cursor })
    }
}

pub(super) struct ObjectAggregateCandidatePage {
    pub(super) items: Vec<ObjectAggregateCandidate>,
    pub(super) next_cursor: Option<String>,
}

pub(super) async fn load_aggregate_candidate_batch(
    connection: &mut PostgresConnection,
    candidate_query: ObjectAggregateCandidateQuery<'_>,
) -> Result<ObjectAggregateCandidateBatch, PostgresStorageError> {
    use crate::schema::hubuumobject::dsl::{
        collection_id as object_collection_id, created_at as object_created_at,
        description as object_description, hubuum_class_id, id as object_id, name as object_name,
        updated_at as object_updated_at,
    };

    let ObjectAggregateCandidateQuery {
        query_options,
        collection_id,
        resource_scope,
        include_object_data,
        computed_filter_snapshot,
    } = candidate_query;
    let collection_ids = [collection_id];
    let mut query = apply_object_filters(
        object_query(&collection_ids, resource_scope),
        query_options,
        None,
    )?;
    for parameter in query_options
        .filters()
        .iter()
        .filter(|parameter| parameter.field.computed_query().is_some())
    {
        let snapshot = computed_filter_snapshot.ok_or_else(|| {
            PostgresStorageError::internal(
                "Computed object aggregate filter is missing its resolved query snapshot",
            )
        })?;
        query = query.filter(computed_filter_predicate(parameter, snapshot)?);
    }
    let fields = [CursorSqlField {
        column: "hubuumobject.id",
        sql_type: CursorSqlType::Integer,
        nullable: false,
    }];
    crate::apply_query_options_with_fields!(query, query_options, fields);
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
    futures_util::pin_mut!(stream);
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

pub(super) fn candidate_sorts() -> Vec<SortParam> {
    vec![SortParam {
        field: FilterField::Id,
        descending: false,
    }]
}
