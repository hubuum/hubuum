//! PostgreSQL computed-object filtering, sorting, paging, and enrichment.

use diesel::prelude::{ExpressionMethods, QueryDsl};
use diesel::{SelectableHelper, dsl::count_star};
use diesel_async::RunQueryDsl;
use hubuum_query::QueryOptions;
use hubuum_storage_core::{
    AuthorizationPermission, ComputedObjectEnrichmentQuery, ComputedObjectListQuery,
    ComputedObjectPage, ComputedObjectProjection, ComputedObjectVisibility, StorageObject,
    StorageVisibility,
};

use crate::operations::catalog::{apply_object_filters, object_query};
use crate::operations::dynamic_sql::BoundSqlPredicate;
use crate::operations::object::ObjectRow;
use crate::operations::related_filter::related_object_filter_predicate;
use crate::operations::visibility::{authorized_collection_ids, required_permissions};
use crate::{PostgresRuntime, PostgresStorageError};

mod enrichment;
pub(crate) mod query;

/// Execute a resolved computed-object query entirely inside one PostgreSQL
/// repeatable-read snapshot.
pub async fn list_computed_objects(
    runtime: &PostgresRuntime,
    query: ComputedObjectListQuery,
) -> Result<ComputedObjectPage, PostgresStorageError> {
    let include_total = query.options().include_total;
    let (class_id, personal_owner_id, options, visibility, projection) = query.into_parts();
    let (mut request_options, mut options) = options.into_parts();
    let snapshot_runtime = runtime.clone();

    runtime
        .with_read_only_snapshot(async move |connection| {
            let snapshot = query::resolve_computed_query_fields(
                connection,
                class_id,
                personal_owner_id,
                &mut options.filters,
                &mut options.sort,
            )
            .await?;
            query::resolve_query_option_types(&mut request_options, &snapshot)?;
            let resolved_options = request_options;
            let (visibility, authorized_object_ids) = query_visibility(&options, visibility)?;
            let Some(visibility) = visibility else {
                return Ok(ComputedObjectPage::new(
                    Vec::new(),
                    include_total.then_some(0),
                    Vec::new(),
                    resolved_options,
                ));
            };
            if authorized_object_ids.as_ref().is_some_and(Vec::is_empty) {
                return Ok(ComputedObjectPage::new(
                    Vec::new(),
                    include_total.then_some(0),
                    Vec::new(),
                    resolved_options,
                ));
            }

            let permissions = required_permissions(
                &options,
                [
                    AuthorizationPermission::ReadCollection,
                    AuthorizationPermission::ReadObject,
                ],
            )?;
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &permissions).await?;
            let related_predicate =
                related_object_filter_predicate(connection, &options.filters, &visibility).await?;

            let total = if include_total {
                let count_query = filtered_object_query(
                    &collection_ids,
                    &visibility,
                    class_id,
                    authorized_object_ids.as_deref(),
                    &options,
                    related_predicate.clone(),
                    &snapshot,
                )?;
                Some(
                    count_query
                        .select(count_star())
                        .first::<i64>(connection)
                        .await?,
                )
            } else {
                None
            };

            let mut row_query = filtered_object_query(
                &collection_ids,
                &visibility,
                class_id,
                authorized_object_ids.as_deref(),
                &options,
                related_predicate,
                &snapshot,
            )?;
            let fields = query::object_cursor_sql_fields(&options.sort, &snapshot)?;
            crate::apply_query_options_with_fields!(row_query, options, fields);
            tracing::debug!(
                operation = "list_computed_objects",
                filter_count = options.filters.len(),
                sort_count = options.sort.len(),
                has_cursor = options.cursor.is_some(),
                include_total,
                "executing PostgreSQL computed-object query"
            );
            let objects = row_query
                .select(ObjectRow::as_select())
                .load::<ObjectRow>(connection)
                .await?
                .into_iter()
                .map(ObjectRow::into_storage)
                .collect::<Vec<_>>();
            let projected = projected_objects(&objects, projection);
            let computed = enrichment::enrich_with_query_snapshot(
                &snapshot_runtime,
                connection,
                projected,
                personal_owner_id,
                &snapshot,
            )
            .await?;

            Ok::<_, PostgresStorageError>(ComputedObjectPage::new(
                objects,
                total,
                computed,
                resolved_options,
            ))
        })
        .await
}

/// Enrich an existing object page, including stale materialization fallback
/// and best-effort read repair, without exposing PostgreSQL to consumers.
pub async fn enrich_objects_with_computed(
    runtime: &PostgresRuntime,
    query: ComputedObjectEnrichmentQuery,
) -> Result<Vec<hubuum_storage_core::StorageComputedObject>, PostgresStorageError> {
    enrichment::enrich_objects(runtime, query).await
}

fn query_visibility(
    options: &QueryOptions,
    visibility: ComputedObjectVisibility,
) -> Result<(Option<StorageVisibility>, Option<Vec<i32>>), PostgresStorageError> {
    match visibility {
        ComputedObjectVisibility::Storage(visibility) => {
            let permissions = required_permissions(
                options,
                [
                    AuthorizationPermission::ReadCollection,
                    AuthorizationPermission::ReadObject,
                ],
            )?;
            if visibility.principal_id() <= 0 {
                return Err(PostgresStorageError::bad_request(
                    "computed object principal id must be greater than zero",
                ));
            }
            if !visibility.allows_permissions(&permissions) {
                return Ok((None, None));
            }
            Ok((Some(visibility), None))
        }
        ComputedObjectVisibility::AuthorizedObjectIds {
            principal_id,
            object_ids,
        } => {
            if principal_id <= 0 || object_ids.iter().any(|object_id| *object_id <= 0) {
                return Err(PostgresStorageError::bad_request(
                    "computed object principal and object ids must be greater than zero",
                ));
            }
            Ok((
                Some(StorageVisibility::new(
                    principal_id,
                    true,
                    None::<[AuthorizationPermission; 0]>,
                    None,
                )),
                Some(object_ids),
            ))
        }
    }
}

fn filtered_object_query<'query>(
    collection_ids: &'query [i32],
    visibility: &'query StorageVisibility,
    class_id: i32,
    authorized_object_ids: Option<&'query [i32]>,
    options: &QueryOptions,
    related_predicate: Option<BoundSqlPredicate>,
    snapshot: &query::ComputedQuerySnapshot,
) -> Result<crate::schema::hubuumobject::BoxedQuery<'query, diesel::pg::Pg>, PostgresStorageError> {
    use crate::schema::hubuumobject;

    let mut query = object_query(collection_ids, visibility.resources())
        .filter(hubuumobject::hubuum_class_id.eq(class_id));
    if let Some(object_ids) = authorized_object_ids {
        query = query.filter(hubuumobject::id.eq_any(object_ids));
    }
    query = apply_object_filters(query, options, related_predicate)?;
    for parameter in options
        .filters
        .iter()
        .filter(|parameter| parameter.field.computed_query().is_some())
    {
        query = query.filter(query::computed_filter_predicate(parameter, snapshot)?);
    }
    Ok(query)
}

fn projected_objects(
    objects: &[StorageObject],
    projection: ComputedObjectProjection,
) -> Vec<StorageObject> {
    match projection {
        ComputedObjectProjection::None => Vec::new(),
        ComputedObjectProjection::All => objects.to_vec(),
        ComputedObjectProjection::CursorBoundary { page_limit } if objects.len() > page_limit => {
            objects
                .get(page_limit.saturating_sub(1))
                .cloned()
                .into_iter()
                .collect()
        }
        ComputedObjectProjection::CursorBoundary { .. } => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDateTime;
    use hubuum_storage_core::StorageRecordMetadata;

    use super::*;

    fn object(id: i32) -> StorageObject {
        StorageObject::new(
            StorageRecordMetadata::new(id, NaiveDateTime::default(), NaiveDateTime::default(), 1),
            format!("object-{id}"),
            1,
            1,
            serde_json::json!({}),
            String::new(),
        )
    }

    #[test]
    fn cursor_projection_selects_the_last_returned_object() {
        let projected = projected_objects(
            &[object(1), object(2), object(3)],
            ComputedObjectProjection::CursorBoundary { page_limit: 2 },
        );

        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].id(), 2);
    }

    #[test]
    fn cursor_projection_is_empty_for_a_terminal_page() {
        assert!(
            projected_objects(
                &[object(1), object(2)],
                ComputedObjectProjection::CursorBoundary { page_limit: 2 }
            )
            .is_empty()
        );
    }
}
