//! PostgreSQL computed-object filtering, sorting, paging, and enrichment.

use diesel::prelude::{ExpressionMethods, QueryDsl};
use diesel::{SelectableHelper, dsl::count_star};
use diesel_async::RunQueryDsl;
use hubuum_query::QueryOptions;
use hubuum_storage_core::{
    StorageAuthorizationPermission, StorageComputedObjectEnrichmentQuery,
    StorageComputedObjectListQuery, StorageComputedObjectPage, StorageComputedObjectProjection,
    StorageComputedObjectVisibility, StorageObject, StorageVisibility,
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
    query: StorageComputedObjectListQuery,
) -> Result<StorageComputedObjectPage, PostgresStorageError> {
    let include_total = query.options().include_total();
    let (class_id, personal_owner_id, options, visibility, projection) = query.into_parts();
    let class_id = class_id.id();
    let personal_owner_id = personal_owner_id.map(|id| id.id());
    let (mut request_options, mut options, effective_page_limit) = options.into_parts();
    // The execution copy includes pagination tie-breakers. Enforce the public
    // limit against only the sort fields supplied by the caller.
    if request_options
        .sort()
        .iter()
        .any(|sort| sort.field.computed_query().is_some())
    {
        query::validate_explicit_sort_count(request_options.sort().len())?;
    }
    let snapshot_runtime = runtime.clone();

    runtime
        .with_read_only_snapshot(async move |connection| {
            let (filters, sort) = options.filters_and_sort_mut();
            let snapshot = query::resolve_computed_query_fields(
                connection,
                class_id,
                personal_owner_id,
                filters,
                sort,
            )
            .await?;
            query::resolve_query_option_types(&mut request_options, &snapshot)?;
            let resolved_options = request_options;
            let (visibility, authorized_object_ids) = query_visibility(&options, visibility)?;
            let Some(visibility) = visibility else {
                return crate::validate_persisted(
                    "computed object page",
                    StorageComputedObjectPage::try_new(
                        Vec::new(),
                        include_total.then_some(0),
                        Vec::new(),
                        resolved_options,
                    ),
                );
            };
            if authorized_object_ids.as_ref().is_some_and(Vec::is_empty) {
                return crate::validate_persisted(
                    "computed object page",
                    StorageComputedObjectPage::try_new(
                        Vec::new(),
                        include_total.then_some(0),
                        Vec::new(),
                        resolved_options,
                    ),
                );
            }

            let permissions = required_permissions(
                &options,
                [
                    StorageAuthorizationPermission::ReadCollection,
                    StorageAuthorizationPermission::ReadObject,
                ],
            )?;
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &permissions).await?;
            let related_predicate =
                related_object_filter_predicate(connection, options.filters(), &visibility).await?;

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
            let fields = query::object_cursor_sql_fields(options.sort(), &snapshot)?;
            crate::apply_query_options_with_fields!(
                row_query,
                options,
                fields,
                crate::cursor::CursorTieBreaker::new(
                    hubuum_query::FilterField::Id,
                    false,
                    crate::operations::catalog::object_cursor_field(
                        &hubuum_query::FilterField::Id,
                    )?
                    .into(),
                )
            );
            tracing::debug!(
                operation = "list_computed_objects",
                filter_count = options.filters().len(),
                sort_count = options.sort().len(),
                has_cursor = options.cursor().is_some(),
                include_total,
                "executing PostgreSQL computed-object query"
            );
            let objects = row_query
                .select(ObjectRow::as_select())
                .load::<ObjectRow>(connection)
                .await?
                .into_iter()
                .map(ObjectRow::into_storage)
                .collect::<Result<Vec<_>, _>>()?;
            let projected = projected_objects(&objects, projection, effective_page_limit);
            let computed = enrichment::enrich_with_query_snapshot(
                &snapshot_runtime,
                connection,
                projected,
                personal_owner_id,
                &snapshot,
            )
            .await?;

            crate::validate_persisted(
                "computed object page",
                StorageComputedObjectPage::try_new(objects, total, computed, resolved_options),
            )
        })
        .await
}

/// Enrich an existing object page, including stale materialization fallback
/// and best-effort read repair, without exposing PostgreSQL to consumers.
pub async fn enrich_objects_with_computed(
    runtime: &PostgresRuntime,
    query: StorageComputedObjectEnrichmentQuery,
) -> Result<Vec<hubuum_storage_core::StorageComputedObject>, PostgresStorageError> {
    enrichment::enrich_objects(runtime, query).await
}

fn query_visibility(
    options: &QueryOptions,
    visibility: StorageComputedObjectVisibility,
) -> Result<(Option<StorageVisibility>, Option<Vec<i32>>), PostgresStorageError> {
    match visibility {
        StorageComputedObjectVisibility::Storage(visibility) => {
            let permissions = required_permissions(
                options,
                [
                    StorageAuthorizationPermission::ReadCollection,
                    StorageAuthorizationPermission::ReadObject,
                ],
            )?;
            if !visibility.allows_permissions(&permissions) {
                return Ok((None, None));
            }
            Ok((Some(visibility), None))
        }
        StorageComputedObjectVisibility::AuthorizedObjectIds {
            principal_id,
            object_ids,
        } => Ok((
            Some(StorageVisibility::new(
                principal_id,
                true,
                None::<[StorageAuthorizationPermission; 0]>,
                None,
            )),
            Some(
                object_ids
                    .into_iter()
                    .map(|object_id| object_id.id())
                    .collect(),
            ),
        )),
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
        .filters()
        .iter()
        .filter(|parameter| parameter.field.computed_query().is_some())
    {
        query = query.filter(query::computed_filter_predicate(parameter, snapshot)?);
    }
    Ok(query)
}

fn projected_objects(
    objects: &[StorageObject],
    projection: StorageComputedObjectProjection,
    effective_page_limit: usize,
) -> Vec<StorageObject> {
    match projection {
        StorageComputedObjectProjection::None => Vec::new(),
        StorageComputedObjectProjection::All => objects.to_vec(),
        StorageComputedObjectProjection::CursorBoundary if objects.len() > effective_page_limit => {
            objects
                .get(effective_page_limit.saturating_sub(1))
                .cloned()
                .into_iter()
                .collect()
        }
        StorageComputedObjectProjection::CursorBoundary => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use crate::revision::record_metadata_from_raw_revision;
    use chrono::NaiveDateTime;
    use hubuum_domain::{ClassId, CollectionId, ObjectId};

    use super::*;

    fn object(id: i32) -> StorageObject {
        StorageObject::new(
            record_metadata_from_raw_revision(
                id,
                NaiveDateTime::default(),
                NaiveDateTime::default(),
                1,
            )
            .unwrap(),
            format!("object-{id}"),
            CollectionId::new(1).unwrap(),
            ClassId::new(1).unwrap(),
            serde_json::json!({}),
            String::new(),
        )
    }

    #[test]
    fn cursor_projection_selects_the_last_returned_object() {
        let projected = projected_objects(
            &[object(1), object(2), object(3)],
            StorageComputedObjectProjection::CursorBoundary,
            2,
        );

        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].id(), ObjectId::new(2).unwrap());
    }

    #[test]
    fn cursor_projection_is_empty_for_a_terminal_page() {
        assert!(
            projected_objects(
                &[object(1), object(2)],
                StorageComputedObjectProjection::CursorBoundary,
                2,
            )
            .is_empty()
        );
    }
}
