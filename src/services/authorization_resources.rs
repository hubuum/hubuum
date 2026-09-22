use crate::permissions::ClassResourceEndpoint;
use crate::permissions::ObjectResourceEndpoint;
use hubuum_domain::ResourceId;
use std::collections::HashMap;

use crate::errors::ApiError;
use crate::models::search::{FilterField, ParsedQueryParam, QueryOptions, SearchOperator};
use crate::models::{HubuumClassRelation, HubuumObjectRelation, Permissions, TokenScope};
use crate::permissions::visibility::authorize_all_candidates;
use crate::permissions::{PermissionBackend, PrincipalRef, ResourceRef};
use crate::services::catalog;

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
            class_ids
                .into_iter()
                .map(ResourceId::new)
                .collect::<Result<Vec<_>, _>>()?,
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
            object_ids
                .into_iter()
                .map(ResourceId::new)
                .collect::<Result<Vec<_>, _>>()?,
        ))
        .await?
        .into_iter()
        .map(|object| (object.id().id(), object))
        .collect())
}

pub(crate) async fn task_class_authorization_resources(
    backend: &impl StorageContext,
    principal_id: i32,
    class_ids: &[i32],
) -> Result<HashMap<i32, ResourceRef>, ApiError> {
    let mut resources = HashMap::new();
    // Equality filters accept at most 50 values, even for internal queries.
    for ids in class_ids.chunks(50) {
        let query = QueryOptions::new(
            vec![ParsedQueryParam {
                field: FilterField::Id,
                operator: SearchOperator::Equals { is_negated: false },
                value: ids.iter().map(i32::to_string).collect::<Vec<_>>().join(","),
            }],
            Vec::new(),
            Some(ids.len()),
            None,
            false,
        )?;
        let (classes, _) = catalog::list_classes(backend, principal_id, true, None, query).await?;
        resources.extend(classes.into_iter().map(|class| {
            (
                class.id,
                ResourceRef::class(class.id, class.collection.id, Some(class.name)),
            )
        }));
    }
    Ok(resources)
}

pub(crate) async fn schema_compliance_authorization_resources(
    backend: &impl StorageContext,
    object_ids: impl IntoIterator<Item = i32>,
) -> Result<HashMap<i32, ResourceRef>, ApiError> {
    Ok(load_objects(backend, object_ids)
        .await?
        .into_iter()
        .map(|(id, object)| {
            (
                id,
                ResourceRef::object(
                    id,
                    ClassResourceEndpoint::new(object.collection_id().id(), object.class_id().id()),
                    Some(object.name().to_string()),
                ),
            )
        })
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

pub(crate) async fn class_authorization_resources(
    backend: &impl StorageContext,
    principal_id: i32,
    class_ids: &[i32],
) -> Result<Vec<ResourceRef>, ApiError> {
    let resources = task_class_authorization_resources(backend, principal_id, class_ids).await?;
    class_ids
        .iter()
        .map(|id| {
            resources.get(id).cloned().ok_or_else(|| {
                ApiError::InternalServerError(format!(
                    "authorization candidate references missing class {id}"
                ))
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

/// Authorize one bounded relation page using its captured endpoint metadata.
pub(crate) async fn authorize_class_relation_candidates(
    storage: &impl StorageContext,
    backend: &dyn PermissionBackend,
    principal: &PrincipalRef,
    scope: Option<&TokenScope>,
    permissions: Vec<Permissions>,
    candidates: Vec<HubuumClassRelation>,
) -> Result<Vec<HubuumClassRelation>, ApiError> {
    let resources = class_relation_authorization_resources(storage, &candidates).await?;
    let candidates = candidates.into_iter().zip(resources).collect();
    Ok(authorize_all_candidates(
        backend,
        principal,
        candidates,
        scope,
        permissions,
        |(_, resource)| resource.clone(),
    )
    .await?
    .into_iter()
    .map(|(relation, _)| relation)
    .collect())
}

/// Authorize one bounded relation page using its captured endpoint metadata.
pub(crate) async fn authorize_object_relation_candidates(
    storage: &impl StorageContext,
    backend: &dyn PermissionBackend,
    principal: &PrincipalRef,
    scope: Option<&TokenScope>,
    permissions: Vec<Permissions>,
    candidates: Vec<HubuumObjectRelation>,
) -> Result<Vec<HubuumObjectRelation>, ApiError> {
    let resources = object_relation_authorization_resources(storage, &candidates).await?;
    let candidates = candidates.into_iter().zip(resources).collect();
    Ok(authorize_all_candidates(
        backend,
        principal,
        candidates,
        scope,
        permissions,
        |(_, resource)| resource.clone(),
    )
    .await?
    .into_iter()
    .map(|(relation, _)| relation)
    .collect())
}

/// Load a page's current authorization facts without per-resource lookups.
pub(crate) async fn task_authorization_resources(
    backend: &impl StorageContext,
    keys: impl IntoIterator<Item = hubuum_storage_core::StorageAuthorizationResourceKey>,
) -> Result<HashMap<hubuum_storage_core::StorageAuthorizationResourceKey, ResourceRef>, ApiError> {
    use hubuum_storage_core::{
        StorageAuthorizationResource as R, StorageAuthorizationResourcesQuery,
    };
    Ok(storage_handle(backend)
        .load_authorization_resources(StorageAuthorizationResourcesQuery::new(keys))
        .await?
        .into_iter()
        .map(|resource| {
            let key = resource.key();
            let resource = match resource {
                R::Class { resource, name } => ResourceRef::class(
                    resource.id().id(),
                    resource.collection_id().id(),
                    Some(name),
                ),
                R::Object(resource) => ResourceRef::object(
                    resource.id().id(),
                    ClassResourceEndpoint::new(
                        resource.collection_id().id(),
                        resource.class_id().id(),
                    ),
                    Some(resource.name().to_owned()),
                ),
                R::Collection { id, name } => ResourceRef::named_collection(id.id(), Some(name)),
                R::ExportTemplate {
                    id,
                    collection_id,
                    name,
                } => ResourceRef::template(id.id(), collection_id.id(), Some(name)),
                R::RemoteTarget {
                    id,
                    collection_id,
                    name,
                } => ResourceRef::remote_target(id.id(), collection_id.id(), Some(name)),
                R::ClassRelation { id, from, to } => ResourceRef::class_relation(
                    Some(id.id()),
                    ClassResourceEndpoint::new(from.collection_id().id(), from.id().id()),
                    ClassResourceEndpoint::new(to.collection_id().id(), to.id().id()),
                ),
                R::ObjectRelation {
                    id,
                    from,
                    to,
                    class_relation_id,
                } => ResourceRef::object_relation(
                    Some(id.id()),
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
                    class_relation_id.id(),
                ),
            };
            (key, resource)
        })
        .collect())
}
