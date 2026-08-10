use crate::errors::ApiError;
use crate::models::{
    ComputedFieldErrorResponse, ComputedScopeResponse, HubuumObject, HubuumObjectComputedResponse,
    SharedComputedScopeResponse, UserID,
};
use crate::pagination::{count_query_options, prepare_db_pagination};
use crate::permissions::visibility::AuthorizedObjectIds;
use crate::storage::postgres::PostgresPool;
use crate::storage::postgres::operations::computed_field::{
    ComputedQuerySnapshot, enrich_objects_with_computed,
    enrich_objects_with_computed_query_snapshot, resolve_computed_query_fields,
};
use crate::storage::postgres::operations::resource_rows::{object_from_storage, object_to_storage};
use crate::storage::postgres::operations::user::UserSearchBackend;
use crate::storage::postgres::operations::user::search::{
    count_computed_objects_with_authorized_ids, search_computed_objects_with_authorized_ids,
};
use crate::storage::postgres::operations::visibility::{principal, token_scope};
use crate::storage::{
    AuthorizationPermission, ComputedObjectEnrichmentQuery, ComputedObjectListQuery,
    ComputedObjectPage, ComputedObjectProjection, ComputedObjectVisibility,
    StorageComputedFieldError, StorageComputedObject, StorageComputedScope,
    StorageSharedComputedScope,
};

pub(crate) async fn list_computed_objects(
    pool: &PostgresPool,
    query: ComputedObjectListQuery,
) -> Result<ComputedObjectPage, ApiError> {
    let include_total = query.options().include_total;
    let (class_id, personal_owner_id, mut options, visibility, projection) = query.into_parts();
    let computed_sorting = options
        .sort
        .iter()
        .any(|sort| sort.field.computed_query().is_some());
    let snapshot = resolve_computed_query_fields(
        pool,
        class_id,
        personal_owner_id,
        &mut options.filters,
        &mut options.sort,
    )
    .await?;
    let count_options = count_query_options(&options);
    let resolved_options = options.clone();
    let search_options = if computed_sorting {
        prepare_db_pagination::<HubuumObjectComputedResponse>(&options)?
    } else {
        prepare_db_pagination::<HubuumObject>(&options)?
    };

    let (objects, total) = match visibility {
        ComputedObjectVisibility::Storage(visibility) => {
            if !visibility.allows_permissions(&[
                AuthorizationPermission::ReadCollection,
                AuthorizationPermission::ReadObject,
            ]) {
                return Ok(ComputedObjectPage::new(
                    Vec::new(),
                    include_total.then_some(0),
                    Vec::new(),
                    resolved_options,
                ));
            }
            let principal = principal(&visibility)?;
            let scope = token_scope(&visibility)?;
            let total = if include_total {
                Some(
                    principal
                        .count_objects_with_computed_query_from_backend_with_admin_status(
                            pool,
                            count_options,
                            visibility.is_admin(),
                            scope.as_ref(),
                            &snapshot,
                        )
                        .await?,
                )
            } else {
                None
            };
            let objects = principal
                .search_objects_with_computed_query_from_backend_with_admin_status(
                    pool,
                    search_options,
                    visibility.is_admin(),
                    scope.as_ref(),
                    &snapshot,
                )
                .await?;
            (objects, total)
        }
        ComputedObjectVisibility::AuthorizedObjectIds {
            principal_id,
            object_ids,
        } => {
            let principal = UserID::new(principal_id)?;
            let authorized_ids = AuthorizedObjectIds::new(object_ids)?;
            let total = if include_total {
                Some(
                    count_computed_objects_with_authorized_ids(
                        &principal,
                        pool,
                        count_options,
                        &snapshot,
                        &authorized_ids,
                    )
                    .await?,
                )
            } else {
                None
            };
            let objects = search_computed_objects_with_authorized_ids(
                &principal,
                pool,
                search_options,
                &snapshot,
                &authorized_ids,
            )
            .await?;
            (objects, total)
        }
    };

    let projected = projected_objects(&objects, projection);
    let computed = enrich_with_snapshot(pool, projected, personal_owner_id, &snapshot).await?;
    Ok(ComputedObjectPage::new(
        objects.into_iter().map(object_to_storage).collect(),
        total,
        computed,
        resolved_options,
    ))
}

pub(crate) async fn enrich_computed_objects(
    pool: &PostgresPool,
    query: ComputedObjectEnrichmentQuery,
) -> Result<Vec<StorageComputedObject>, ApiError> {
    let (objects, personal_owner_id) = query.into_parts();
    let objects = objects
        .into_iter()
        .map(object_from_storage)
        .collect::<Result<Vec<_>, _>>()?;
    enrich_objects_with_computed(pool, objects, personal_owner_id)
        .await?
        .into_iter()
        .map(computed_object_to_storage)
        .collect()
}

fn projected_objects(
    objects: &[HubuumObject],
    projection: ComputedObjectProjection,
) -> Vec<HubuumObject> {
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

async fn enrich_with_snapshot(
    pool: &PostgresPool,
    objects: Vec<HubuumObject>,
    personal_owner_id: Option<i32>,
    snapshot: &ComputedQuerySnapshot,
) -> Result<Vec<StorageComputedObject>, ApiError> {
    enrich_objects_with_computed_query_snapshot(pool, objects, personal_owner_id, snapshot)
        .await?
        .into_iter()
        .map(computed_object_to_storage)
        .collect()
}

fn computed_object_to_storage(
    object: HubuumObjectComputedResponse,
) -> Result<StorageComputedObject, ApiError> {
    let HubuumObjectComputedResponse { object, computed } = object;
    Ok(StorageComputedObject::new(
        object_to_storage(object),
        shared_scope_to_storage(computed.shared),
        computed.personal.map(scope_to_storage),
    ))
}

fn shared_scope_to_storage(scope: SharedComputedScopeResponse) -> StorageSharedComputedScope {
    StorageSharedComputedScope::new(
        scope.revision,
        scope.materialization_stale,
        StorageComputedScope::new(
            scope.values,
            scope
                .errors
                .into_iter()
                .map(|(key, error)| (key, error_to_storage(error)))
                .collect(),
        ),
    )
}

fn scope_to_storage(scope: ComputedScopeResponse) -> StorageComputedScope {
    StorageComputedScope::new(
        scope.values,
        scope
            .errors
            .into_iter()
            .map(|(key, error)| (key, error_to_storage(error)))
            .collect(),
    )
}

fn error_to_storage(error: ComputedFieldErrorResponse) -> StorageComputedFieldError {
    StorageComputedFieldError::new(error.code, error.path, error.message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(id: i32) -> HubuumObject {
        HubuumObject {
            id,
            name: format!("object-{id}"),
            collection_id: 1,
            hubuum_class_id: 1,
            data: serde_json::json!({}),
            description: String::new(),
            created_at: chrono::NaiveDateTime::default(),
            updated_at: chrono::NaiveDateTime::default(),
            revision: crate::models::ResourceRevision::new(1).unwrap(),
        }
    }

    #[test]
    fn cursor_projection_selects_the_last_returned_object() {
        let projected = projected_objects(
            &[object(1), object(2), object(3)],
            ComputedObjectProjection::CursorBoundary { page_limit: 2 },
        );

        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].id, 2);
    }

    #[test]
    fn cursor_projection_is_empty_for_an_empty_page() {
        assert!(
            projected_objects(
                &[],
                ComputedObjectProjection::CursorBoundary { page_limit: 2 }
            )
            .is_empty()
        );
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
