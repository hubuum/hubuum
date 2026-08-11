use std::collections::HashMap;

use crate::errors::ApiError;
use crate::models::{HubuumClassRelation, HubuumObjectRelation};
use crate::permissions::{ResourceAttrs, ResourceKind, ResourceRef};
use crate::storage::{
    AuthorizationObjectResource, AuthorizationResourceIds, AuthorizationStorage, StorageContext,
    storage_handle,
};

async fn load_classes(
    backend: &impl StorageContext,
    class_ids: impl IntoIterator<Item = i32>,
) -> Result<HashMap<i32, crate::storage::AuthorizationClassResource>, ApiError> {
    Ok(storage_handle(backend)
        .load_authorization_classes(AuthorizationResourceIds::new(class_ids))
        .await?
        .into_iter()
        .map(|class| (class.id(), class))
        .collect())
}

async fn load_objects(
    backend: &impl StorageContext,
    object_ids: impl IntoIterator<Item = i32>,
) -> Result<HashMap<i32, AuthorizationObjectResource>, ApiError> {
    Ok(storage_handle(backend)
        .load_authorization_objects(AuthorizationResourceIds::new(object_ids))
        .await?
        .into_iter()
        .map(|object| (object.id(), object))
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
            Ok(ResourceRef {
                kind: ResourceKind::ClassRelation,
                id: relation.id,
                attrs: ResourceAttrs {
                    collection_id: (from.collection_id() == to.collection_id())
                        .then_some(from.collection_id()),
                    from_collection_id: Some(from.collection_id()),
                    to_collection_id: Some(to.collection_id()),
                    from_class_id: Some(from.id()),
                    to_class_id: Some(to.id()),
                    ..Default::default()
                },
            })
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
            Ok(ResourceRef {
                kind: ResourceKind::Object,
                id: object.id(),
                attrs: ResourceAttrs {
                    collection_id: Some(object.collection_id()),
                    class_id: Some(object.class_id()),
                    name: Some(object.name().to_string()),
                    ..Default::default()
                },
            })
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
            Ok(ResourceRef {
                kind: ResourceKind::ObjectRelation,
                id: relation.id,
                attrs: ResourceAttrs {
                    collection_id: (from.collection_id() == to.collection_id())
                        .then_some(from.collection_id()),
                    from_collection_id: Some(from.collection_id()),
                    to_collection_id: Some(to.collection_id()),
                    from_class_id: Some(from.class_id()),
                    to_class_id: Some(to.class_id()),
                    from_object_id: Some(from.id()),
                    to_object_id: Some(to.id()),
                    class_relation_id: Some(relation.class_relation_id),
                    ..Default::default()
                },
            })
        })
        .collect()
}
