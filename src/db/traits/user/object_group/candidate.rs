use diesel::prelude::*;
use diesel::sql_types::{Jsonb, Nullable};
use diesel_async::RunQueryDsl;
use futures::TryStreamExt;
use serde::Serialize;

use super::bounded_json::ObjectGroupJsonBound;
use super::sql::apply_visible_object_filters;
use crate::db::DbConnection;
use crate::db::traits::search::JsonPredicateExt;
use crate::errors::ApiError;
use crate::models::HubuumObject;
use crate::models::object_group::ObjectGroupSpec;
use crate::models::search::{FilterField, QueryOptions, QueryParamsExt, SortParam};
use crate::pagination::{Page, finalize_page, finalize_partial_page};
use crate::traits::{CursorPaginated, CursorValue};

#[derive(Debug, Clone, Queryable, Serialize)]
pub(super) struct ObjectGroupCandidate {
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

impl CursorPaginated for ObjectGroupCandidate {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::Description
                | FilterField::Collections
                | FilterField::CollectionId
                | FilterField::ClassId
                | FilterField::Classes
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(i64::from(self.id)),
            FilterField::Name => CursorValue::String(self.name.clone()),
            FilterField::Description => CursorValue::String(self.description.clone()),
            FilterField::Collections | FilterField::CollectionId => {
                CursorValue::Integer(i64::from(self.collection_id))
            }
            FilterField::ClassId | FilterField::Classes => {
                CursorValue::Integer(i64::from(self.hubuum_class_id))
            }
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{field}' is not orderable for object group candidates"
                )));
            }
        })
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

pub(super) struct ObjectGroupCandidateBatch {
    items: Vec<ObjectGroupCandidate>,
    stopped_by_size: bool,
}

impl ObjectGroupCandidateBatch {
    pub(super) fn into_page(
        self,
        query_options: &QueryOptions,
    ) -> Result<Page<ObjectGroupCandidate>, ApiError> {
        if self.stopped_by_size {
            finalize_partial_page(self.items, query_options, true)
        } else {
            finalize_page(self.items, query_options)
        }
    }
}

pub(super) async fn load_group_candidate_batch(
    connection: &mut DbConnection,
    query_options: &QueryOptions,
    collection_id: i32,
    spec: &ObjectGroupSpec,
) -> Result<ObjectGroupCandidateBatch, ApiError> {
    use crate::schema::hubuumobject::dsl::{
        collection_id as object_collection_id, created_at as object_created_at,
        description as object_description, hubuum_class_id, hubuumobject, id as object_id,
        name as object_name, updated_at as object_updated_at,
    };

    let mut query = hubuumobject
        .filter(object_collection_id.eq(collection_id))
        .into_boxed();
    apply_visible_object_filters!(query, query_options);
    crate::apply_query_options!(query, query_options, HubuumObject);
    let data_projection = if spec.requires_object_data() {
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
        .load_stream::<ObjectGroupCandidate>(connection)
        .await?;
    futures::pin_mut!(stream);
    let bound = ObjectGroupJsonBound::CandidateBatch;
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
    Ok(ObjectGroupCandidateBatch {
        items,
        stopped_by_size,
    })
}
