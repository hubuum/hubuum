use super::helpers::{
    storage_class_to_resolution, storage_collection_to_resolution, storage_object_to_resolution,
};
use super::types::{
    CacheLookup, ClassResolution, CollectionResolution, ObjectResolution, PlanningState,
};
use crate::models::{ClassKey, CollectionKey, ImportCollectionInput, ObjectKey, Permissions};
use crate::permissions::{AuthorizationContext, ResourceRef};
use crate::traits::AuthzSubject;
use hubuum_domain::{ClassId, CollectionId};

use crate::storage::{ImportStorage, storage_handle};

pub(super) async fn visible_collection(
    pool: &impl AuthorizationContext,
    user: &impl AuthzSubject,
    state: &mut PlanningState,
    candidate: Option<CollectionResolution>,
) -> Result<Option<CollectionResolution>, String> {
    if let Some(collection) = &candidate
        && collection.exists_in_db
        && !state
            .authorization
            .visible(
                pool,
                user,
                ResourceRef::named_collection(collection.id, Some(collection.name.clone())),
                Permissions::ReadCollection,
            )
            .await?
    {
        return Ok(None);
    }
    Ok(candidate)
}

pub(super) async fn visible_class(
    pool: &impl AuthorizationContext,
    user: &impl AuthzSubject,
    state: &mut PlanningState,
    candidate: Option<ClassResolution>,
) -> Result<Option<ClassResolution>, String> {
    if let Some(class) = &candidate
        && class.exists_in_db
        && !state
            .authorization
            .visible(
                pool,
                user,
                ResourceRef::class(class.id, class.collection_id, Some(class.name.clone())),
                Permissions::ReadClass,
            )
            .await?
    {
        return Ok(None);
    }
    Ok(candidate)
}

fn validate_collection_key_path(key: &CollectionKey) -> Result<(), String> {
    if let Some(path) = &key.path {
        match path.last() {
            Some(last) if last == &key.name => Ok(()),
            Some(_) => Err(format!(
                "collection_key.path must end with collection name '{}'",
                key.name
            )),
            None if key.name == "root" => Ok(()),
            None => {
                Err("collection_key.path may be empty only for the root collection".to_string())
            }
        }
    } else {
        Ok(())
    }
}

fn collection_key_label(key: &CollectionKey) -> String {
    match &key.path {
        Some(path) => format!("/{}", path.join("/")),
        None => key.name.clone(),
    }
}

async fn find_collection_by_key_planning(
    pool: &impl AuthorizationContext,
    user: &impl AuthzSubject,
    state: &mut PlanningState,
    key: &CollectionKey,
) -> Result<Option<CollectionResolution>, String> {
    validate_collection_key_path(key)?;

    if key.path.is_some() {
        let root = storage_handle(pool)
            .get_import_root_collection()
            .await
            .map_err(|err| err.to_string())?;
        let mut collection = storage_collection_to_resolution(root);
        for segment in key.path.as_deref().unwrap_or_default() {
            let parent = CollectionId::new(collection.id).map_err(|err| err.to_string())?;
            let candidate = storage_handle(pool)
                .get_import_collection_child_by_name(parent, segment)
                .await
                .map_err(|err| err.to_string())?
                .map(storage_collection_to_resolution);
            let Some(visible) = visible_collection(pool, user, state, candidate).await? else {
                return Ok(None);
            };
            collection = visible;
        }
        let collection = visible_collection(pool, user, state, Some(collection)).await?;
        if let Some(collection) = &collection {
            state.remember_collection(None, collection.clone());
        }
        return Ok(collection);
    }

    if !state.loaded_collection_names.contains(&key.name) {
        let candidates = storage_handle(pool)
            .list_import_collections_by_name(&key.name)
            .await
            .map_err(|err| err.to_string())?;
        let needs_path = candidates.len() > 1;
        for candidate in candidates {
            if let Some(collection) = visible_collection(
                pool,
                user,
                state,
                Some(storage_collection_to_resolution(candidate)),
            )
            .await?
            {
                state.remember_collection(None, collection);
            }
        }
        if needs_path && let CacheLookup::Found([visible]) = state.collections_named(&key.name) {
            let id = CollectionId::new(visible.id).map_err(|err| err.to_string())?;
            let ancestors = storage_handle(pool)
                .collection_store()
                .list_collection_ancestors(id)
                .await
                .map_err(|err| err.to_string())?;
            let mut path: Vec<_> = ancestors
                .into_iter()
                .rev()
                .filter(|row| row.parent_collection_id().is_some())
                .map(|row| row.name().to_owned())
                .collect();
            if visible.parent_collection_id.is_some() {
                path.push(key.name.clone());
            }
            state.execution_paths.insert(key.name.clone(), path);
        }
        state.loaded_collection_names.insert(key.name.clone());
    }
    let matches = match state.collections_named(&key.name) {
        CacheLookup::Found(values) => values.to_vec(),
        _ => Vec::new(),
    };
    match matches.as_slice() {
        [] => {
            state.record_missing_collection_name(key.name.clone());
            Ok(None)
        }
        [collection] => {
            let collection = collection.clone();
            state.remember_collection(None, collection.clone());
            Ok(Some(collection))
        }
        _ => Err(format!(
            "Collection name '{}' is ambiguous; use collection_key.path",
            key.name
        )),
    }
}

async fn resolve_root_collection_planning(
    pool: &impl AuthorizationContext,
    user: &impl AuthzSubject,
    state: &mut PlanningState,
) -> Result<CollectionResolution, String> {
    if let Some(collection) = state.collection_under_parent(None, "root").cloned() {
        return Ok(collection);
    }

    let collection = storage_handle(pool)
        .get_import_root_collection()
        .await
        .map_err(|err| err.to_string())
        .map(storage_collection_to_resolution)?;
    // The implicit root is a fixed import anchor. Only readable roots enter
    // the cache used by explicit name, path and ID lookups.
    if visible_collection(pool, user, state, Some(collection.clone()))
        .await?
        .is_some()
    {
        state.remember_collection(None, collection.clone());
    }
    Ok(collection)
}

pub(super) async fn resolve_collection_planning(
    pool: &impl AuthorizationContext,
    user: &impl AuthzSubject,
    state: &mut PlanningState,
    reference: Option<&str>,
    key: Option<&CollectionKey>,
) -> Result<CollectionResolution, String> {
    match (reference, key) {
        (Some(reference), None) => state
            .collection_by_ref(reference)
            .cloned()
            .ok_or_else(|| format!("Unknown collection ref '{reference}'")),
        (None, Some(key)) => {
            let collection = find_collection_by_key_planning(pool, user, state, key)
                .await?
                .ok_or_else(|| format!("Collection '{}' not found", collection_key_label(key)))?;
            state.resolved_keys.insert(key.clone(), collection.id);
            Ok(collection)
        }
        _ => Err("Exactly one of collection_ref or collection_key must be provided".to_string()),
    }
}

pub(super) async fn resolve_collection_parent_planning(
    pool: &impl AuthorizationContext,
    user: &impl AuthzSubject,
    state: &mut PlanningState,
    input: &ImportCollectionInput,
) -> Result<CollectionResolution, String> {
    match (
        input.parent_collection_ref.as_deref(),
        input.parent_collection_key.as_ref(),
    ) {
        (None, None) => resolve_root_collection_planning(pool, user, state).await,
        (Some(reference), None) => state
            .collection_by_ref(reference)
            .cloned()
            .ok_or_else(|| format!("Unknown collection ref '{reference}'")),
        (None, Some(key)) => resolve_collection_planning(pool, user, state, None, Some(key)).await,
        (Some(_), Some(_)) => Err(
            "At most one of parent_collection_ref or parent_collection_key may be provided"
                .to_string(),
        ),
    }
}

pub(super) async fn resolve_collection_by_id_planning(
    pool: &impl AuthorizationContext,
    user: &impl AuthzSubject,
    state: &mut PlanningState,
    collection_id: i32,
) -> Result<CollectionResolution, String> {
    if let Some(collection) = state.collection_by_id(collection_id) {
        return Ok(collection.clone());
    }

    let collection = storage_handle(pool)
        .get_import_collection_by_id(
            CollectionId::new(collection_id).map_err(|error| error.to_string())?,
        )
        .await
        .map_err(|err| err.to_string())?
        .map(storage_collection_to_resolution);
    let collection = visible_collection(pool, user, state, collection)
        .await?
        .ok_or_else(|| format!("Collection id '{}' not found", collection_id))?;
    state.remember_collection(None, collection.clone());
    Ok(collection)
}

pub(super) async fn resolve_class_planning(
    pool: &impl AuthorizationContext,
    user: &impl AuthzSubject,
    state: &mut PlanningState,
    reference: Option<&str>,
    key: Option<&ClassKey>,
) -> Result<ClassResolution, String> {
    match (reference, key) {
        (Some(reference), None) => state
            .classes
            .by_ref(reference)
            .cloned()
            .ok_or_else(|| format!("Unknown class ref '{reference}'")),
        (None, Some(key)) => {
            let collection = resolve_collection_planning(
                pool,
                user,
                state,
                key.collection_ref.as_deref(),
                key.collection_key.as_ref(),
            )
            .await?;
            match state.classes.lookup(&(collection.id, key.name.clone())) {
                CacheLookup::Found(value) => return Ok(value.clone()),
                CacheLookup::Missing => {
                    return Err(format!(
                        "Class '{}' not found in collection '{}'",
                        key.name, collection.name
                    ));
                }
                CacheLookup::Unknown => {}
            }
            let class = storage_handle(pool)
                .get_import_class_by_name(
                    CollectionId::new(collection.id).map_err(|error| error.to_string())?,
                    &key.name,
                )
                .await
                .map_err(|err| err.to_string())?
                .map(storage_class_to_resolution);
            let class = visible_class(pool, user, state, class)
                .await?
                .ok_or_else(|| {
                    format!(
                        "Class '{}' not found in collection '{}'",
                        key.name, collection.name
                    )
                })?;
            state.remember_class(None, class.clone());
            Ok(class)
        }
        _ => Err("Exactly one of class_ref or class_key must be provided".to_string()),
    }
}

pub(super) async fn resolve_object_planning(
    pool: &impl AuthorizationContext,
    user: &impl AuthzSubject,
    state: &mut PlanningState,
    reference: Option<&str>,
    key: Option<&ObjectKey>,
) -> Result<ObjectResolution, String> {
    match (reference, key) {
        (Some(reference), None) => state
            .objects
            .by_ref(reference)
            .cloned()
            .ok_or_else(|| format!("Unknown object ref '{reference}'")),
        (None, Some(key)) => {
            let class = resolve_class_planning(
                pool,
                user,
                state,
                key.class_ref.as_deref(),
                key.class_key.as_ref(),
            )
            .await?;
            match state.objects.lookup(&(class.id, key.name.clone())) {
                CacheLookup::Found(value) => return Ok(value.clone()),
                CacheLookup::Missing => {
                    return Err(format!(
                        "Object '{}' not found in class '{}'",
                        key.name, class.name
                    ));
                }
                CacheLookup::Unknown => {}
            }
            let object = storage_handle(pool)
                .get_import_object_by_name(
                    ClassId::new(class.id).map_err(|error| error.to_string())?,
                    &key.name,
                )
                .await
                .map_err(|err| err.to_string())?
                .map(storage_object_to_resolution)
                .ok_or_else(|| {
                    format!("Object '{}' not found in class '{}'", key.name, class.name)
                })?;
            state.remember_object(None, object.clone());
            Ok(object)
        }
        _ => Err("Exactly one of object_ref or object_key must be provided".to_string()),
    }
}
