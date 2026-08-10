use crate::errors::ApiError;
use crate::models::{
    Collection, HubuumClassExpanded, HubuumObject, ResourceRevision, TokenResourceScope, TokenScope,
};
use crate::permissions::permission_to_storage;
use crate::storage::{
    StorageClass, StorageCollection, StorageObject, StorageResourceScope, StorageVisibility,
};

pub(super) fn visibility(
    principal_id: i32,
    is_admin: bool,
    scope: Option<&TokenScope>,
) -> Result<StorageVisibility, ApiError> {
    let permissions = scope.and_then(TokenScope::permissions).map(|permissions| {
        permissions
            .iter()
            .copied()
            .map(permission_to_storage)
            .collect::<Vec<_>>()
    });
    let resources = scope.map(resource_scope).transpose()?.flatten();
    Ok(StorageVisibility::new(
        principal_id,
        is_admin,
        permissions,
        resources,
    ))
}

fn resource_scope(scope: &TokenScope) -> Result<Option<StorageResourceScope>, ApiError> {
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
    Ok(Some(StorageResourceScope::new(
        collection_ids,
        class_ids,
        object_ids,
    )))
}

pub(super) fn collection_from_storage(row: StorageCollection) -> Result<Collection, ApiError> {
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

pub(super) fn class_from_storage(row: StorageClass) -> Result<HubuumClassExpanded, ApiError> {
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

pub(super) fn object_from_storage(row: StorageObject) -> Result<HubuumObject, ApiError> {
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

pub(super) fn object_to_storage(object: HubuumObject) -> StorageObject {
    StorageObject::new(
        object.id,
        object.name,
        object.collection_id,
        object.hubuum_class_id,
        object.data,
        object.description,
        object.created_at,
        object.updated_at,
        object.revision.get(),
    )
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
