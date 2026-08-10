use crate::errors::ApiError;
use crate::models::{
    Collection, HubuumClassExpanded, HubuumObject, ResourceRevision, TokenResourceScope,
    TokenScope, UnifiedSearchCursorToken, UnifiedSearchSpec,
};
use crate::permissions::permission_to_storage;
use crate::storage::{
    StorageContext, UnifiedSearchClass, UnifiedSearchCollection, UnifiedSearchCursor,
    UnifiedSearchObject, UnifiedSearchQuery, UnifiedSearchResourceScope, UnifiedSearchStorage,
    UnifiedSearchVisibility, storage_handle,
};

fn resource_scope(scope: &TokenScope) -> Result<Option<UnifiedSearchResourceScope>, ApiError> {
    let Some(resources) = scope.resources()? else {
        return Ok(None);
    };
    let mut collection_ids = Vec::new();
    let mut class_ids = Vec::new();
    let mut object_ids = Vec::new();
    for resource in resources {
        match resource {
            TokenResourceScope::Collection(id) => collection_ids.push(id.id()),
            TokenResourceScope::Class(id) => class_ids.push(id.id()),
            TokenResourceScope::Object(id) => object_ids.push(id.id()),
        }
    }
    Ok(Some(UnifiedSearchResourceScope::new(
        collection_ids,
        class_ids,
        object_ids,
    )))
}

fn visibility(
    principal_id: i32,
    is_admin: bool,
    scope: Option<&TokenScope>,
) -> Result<UnifiedSearchVisibility, ApiError> {
    let permissions = scope.and_then(TokenScope::permissions).map(|permissions| {
        permissions
            .iter()
            .copied()
            .map(permission_to_storage)
            .collect::<Vec<_>>()
    });
    let resources = scope.map(resource_scope).transpose()?.flatten();
    Ok(UnifiedSearchVisibility::new(
        principal_id,
        is_admin,
        permissions,
        resources,
    ))
}

fn cursor(cursor: Option<&UnifiedSearchCursorToken>) -> Option<UnifiedSearchCursor> {
    cursor.map(|cursor| UnifiedSearchCursor::new(cursor.rank, cursor.name.clone(), cursor.id))
}

fn query(
    principal_id: i32,
    is_admin: bool,
    scope: Option<&TokenScope>,
    spec: &UnifiedSearchSpec,
    cursor: Option<&UnifiedSearchCursorToken>,
    search_extended_document: bool,
) -> Result<UnifiedSearchQuery, ApiError> {
    Ok(UnifiedSearchQuery::new(
        spec.query.clone(),
        spec.limit_per_kind,
        visibility(principal_id, is_admin, scope)?,
    )
    .search_extended_document(search_extended_document)
    .cursor(self::cursor(cursor)))
}

fn collection_from_storage(row: UnifiedSearchCollection) -> Result<Collection, ApiError> {
    let (id, name, description, created_at, updated_at, parent_collection_id, revision) =
        row.into_parts();
    Ok(Collection {
        id,
        name,
        description,
        created_at,
        updated_at,
        parent_collection_id,
        revision: ResourceRevision::new(revision)?,
    })
}

fn class_from_storage(row: UnifiedSearchClass) -> Result<HubuumClassExpanded, ApiError> {
    let (
        id,
        name,
        collection,
        json_schema,
        validate_schema,
        description,
        created_at,
        updated_at,
        revision,
    ) = row.into_parts();
    Ok(HubuumClassExpanded {
        id,
        name,
        collection: collection_from_storage(collection)?,
        json_schema,
        validate_schema,
        description,
        created_at,
        updated_at,
        revision: ResourceRevision::new(revision)?,
    })
}

fn object_from_storage(row: UnifiedSearchObject) -> Result<HubuumObject, ApiError> {
    let (
        id,
        name,
        collection_id,
        hubuum_class_id,
        data,
        description,
        created_at,
        updated_at,
        revision,
    ) = row.into_parts();
    Ok(HubuumObject {
        id,
        name,
        collection_id,
        hubuum_class_id,
        data,
        description,
        created_at,
        updated_at,
        revision: ResourceRevision::new(revision)?,
    })
}

pub async fn search_collections(
    backend: &impl StorageContext,
    principal_id: i32,
    is_admin: bool,
    scope: Option<&TokenScope>,
    spec: &UnifiedSearchSpec,
) -> Result<Vec<Collection>, ApiError> {
    storage_handle(backend)
        .search_unified_collections(query(
            principal_id,
            is_admin,
            scope,
            spec,
            spec.collection_cursor.as_ref(),
            false,
        )?)
        .await?
        .into_iter()
        .map(collection_from_storage)
        .collect()
}

pub async fn search_classes(
    backend: &impl StorageContext,
    principal_id: i32,
    is_admin: bool,
    scope: Option<&TokenScope>,
    spec: &UnifiedSearchSpec,
) -> Result<Vec<HubuumClassExpanded>, ApiError> {
    storage_handle(backend)
        .search_unified_classes(query(
            principal_id,
            is_admin,
            scope,
            spec,
            spec.class_cursor.as_ref(),
            spec.search_class_schema,
        )?)
        .await?
        .into_iter()
        .map(class_from_storage)
        .collect()
}

pub async fn search_objects(
    backend: &impl StorageContext,
    principal_id: i32,
    is_admin: bool,
    scope: Option<&TokenScope>,
    spec: &UnifiedSearchSpec,
) -> Result<Vec<HubuumObject>, ApiError> {
    storage_handle(backend)
        .search_unified_objects(query(
            principal_id,
            is_admin,
            scope,
            spec,
            spec.object_cursor.as_ref(),
            spec.search_object_data,
        )?)
        .await?
        .into_iter()
        .map(object_from_storage)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{CollectionID, HubuumClassID, Permissions};

    #[test]
    fn visibility_preserves_independent_token_dimensions() {
        let scope = TokenScope::from_stored_parts(
            Some(vec![Permissions::ReadCollection, Permissions::ReadClass]),
            Some(vec![
                TokenResourceScope::Collection(CollectionID::new(7).unwrap()),
                TokenResourceScope::Class(HubuumClassID::new(9).unwrap()),
            ]),
        )
        .unwrap();

        let visibility = visibility(42, false, Some(&scope)).unwrap();

        assert!(visibility.allows_permissions(&[
            crate::storage::AuthorizationPermission::ReadCollection,
            crate::storage::AuthorizationPermission::ReadClass,
        ]));
        let resources = visibility.resources().unwrap();
        assert_eq!(resources.collection_ids(), &[7]);
        assert_eq!(resources.class_ids(), &[9]);
        assert!(resources.object_ids().is_empty());
    }
}
