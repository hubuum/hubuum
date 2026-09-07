use crate::permissions::ClassResourceEndpoint;
use crate::permissions::ObjectResourceEndpoint;
use std::collections::HashMap;

use crate::errors::ApiError;
use crate::models::{HubuumClassRelation, HubuumObjectRelation};
use crate::permissions::ResourceRef;
use crate::services::storage_boundary::resource_id_to_storage;
use crate::storage::{
    AuthorizationDataStorage, StorageAuthorizationObjectResource, StorageAuthorizationResourceIds,
    StorageContext, storage_handle,
};

async fn load_classes(
    backend: &impl StorageContext,
    class_ids: impl IntoIterator<Item = i32>,
) -> Result<HashMap<i32, crate::storage::StorageAuthorizationClassResource>, ApiError> {
    Ok(storage_handle(backend)
        .list_authorization_classes(StorageAuthorizationResourceIds::new(
            class_ids.into_iter().map(resource_id_to_storage),
        ))
        .await?
        .into_iter()
        .map(|class| (class.id().id(), class))
        .collect())
}

async fn load_objects(
    backend: &impl StorageContext,
    object_ids: impl IntoIterator<Item = i32>,
) -> Result<HashMap<i32, StorageAuthorizationObjectResource>, ApiError> {
    Ok(storage_handle(backend)
        .list_authorization_objects(StorageAuthorizationResourceIds::new(
            object_ids.into_iter().map(resource_id_to_storage),
        ))
        .await?
        .into_iter()
        .map(|object| (object.id().id(), object))
        .collect())
}

pub(crate) async fn class_relation_authorization_resources(
    backend: &impl StorageContext,
    relations: &[HubuumClassRelation],
) -> Result<Vec<ResourceRef>, ApiError> {
    let classes = load_classes(
        backend,
        relations
            .iter()
            .flat_map(|relation| [relation.from_hubuum_class_id, relation.to_hubuum_class_id]),
    )
    .await?;

    relations
        .iter()
        .map(|relation| {
            let from = classes.get(&relation.from_hubuum_class_id).ok_or_else(|| {
                ApiError::InternalServerError(format!(
                    "class relation {} references missing class {}",
                    relation.id, relation.from_hubuum_class_id
                ))
            })?;
            let to = classes.get(&relation.to_hubuum_class_id).ok_or_else(|| {
                ApiError::InternalServerError(format!(
                    "class relation {} references missing class {}",
                    relation.id, relation.to_hubuum_class_id
                ))
            })?;
            Ok(ResourceRef::class_relation(
                Some(relation.id),
                ClassResourceEndpoint::new(from.collection_id().id(), from.id().id()),
                ClassResourceEndpoint::new(to.collection_id().id(), to.id().id()),
            ))
        })
        .collect()
}

pub(crate) async fn object_authorization_resources(
    backend: &impl StorageContext,
    object_ids: &[i32],
) -> Result<Vec<ResourceRef>, ApiError> {
    let objects = load_objects(backend, object_ids.iter().copied()).await?;
    object_ids
        .iter()
        .map(|object_id| {
            let object = objects.get(object_id).ok_or_else(|| {
                ApiError::InternalServerError(format!(
                    "authorization candidate references missing object {object_id}"
                ))
            })?;
            Ok(ResourceRef::object(
                object.id().id(),
                ClassResourceEndpoint::new(object.collection_id().id(), object.class_id().id()),
                Some(object.name().to_string()),
            ))
        })
        .collect()
}

pub(crate) async fn object_relation_authorization_resources(
    backend: &impl StorageContext,
    relations: &[HubuumObjectRelation],
) -> Result<Vec<ResourceRef>, ApiError> {
    let objects = load_objects(
        backend,
        relations
            .iter()
            .flat_map(|relation| [relation.from_hubuum_object_id, relation.to_hubuum_object_id]),
    )
    .await?;

    relations
        .iter()
        .map(|relation| {
            let from = objects
                .get(&relation.from_hubuum_object_id)
                .ok_or_else(|| {
                    ApiError::InternalServerError(format!(
                        "object relation {} references missing object {}",
                        relation.id, relation.from_hubuum_object_id
                    ))
                })?;
            let to = objects.get(&relation.to_hubuum_object_id).ok_or_else(|| {
                ApiError::InternalServerError(format!(
                    "object relation {} references missing object {}",
                    relation.id, relation.to_hubuum_object_id
                ))
            })?;
            Ok(ResourceRef::object_relation(
                Some(relation.id),
                ObjectResourceEndpoint::new(
                    from.collection_id().id(),
                    from.class_id().id(),
                    from.id().id(),
                ),
                ObjectResourceEndpoint::new(
                    to.collection_id().id(),
                    to.class_id().id(),
                    to.id().id(),
                ),
                relation.class_relation_id,
            ))
        })
        .collect()
}
