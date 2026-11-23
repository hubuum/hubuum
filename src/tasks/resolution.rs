use super::helpers::{
    storage_class_to_resolution, storage_collection_to_resolution, storage_object_to_resolution,
};
use super::types::{
    CacheLookup, ClassResolution, CollectionResolution, ObjectResolution, PlanningState,
};
use crate::models::{ClassKey, CollectionKey, ImportCollectionInput, ObjectKey};
use hubuum_domain::{ClassId, CollectionId};

use crate::storage::{ImportStorage, storage_handle};

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
    pool: &impl crate::storage::StorageContext,
    state: &mut PlanningState,
    key: &CollectionKey,
) -> Result<Option<CollectionResolution>, String> {
    validate_collection_key_path(key)?;

    if key.path.is_some() {
        let storage_key = crate::services::import_boundary::collection_key_to_storage(key.clone());
        let collection = storage_handle(pool)
            .get_import_collection_by_key(&storage_key)
            .await
            .map_err(|err| err.to_string())?
            .map(storage_collection_to_resolution);
        if let Some(collection) = &collection {
            state.remember_collection(None, collection.clone());
        }
        return Ok(collection);
    }

    // A name remembered through a path or reference may still have other database matches.
    let mut matches = match state.collections_named(&key.name) {
        CacheLookup::Found(known) => known.to_vec(),
        CacheLookup::Missing => return Ok(None),
        CacheLookup::Unknown => Vec::new(),
    };
    for collection in storage_handle(pool)
        .list_import_collections_by_name(&key.name)
        .await
        .map_err(|err| err.to_string())?
        .into_iter()
        .map(storage_collection_to_resolution)
    {
        if !matches.iter().any(|known| known.id == collection.id) {
            matches.push(collection);
        }
    }

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
    pool: &impl crate::storage::StorageContext,
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
    state.remember_collection(None, collection.clone());
    Ok(collection)
}

pub(super) async fn resolve_collection_planning(
    pool: &impl crate::storage::StorageContext,
    state: &mut PlanningState,
    reference: Option<&str>,
    key: Option<&CollectionKey>,
) -> Result<CollectionResolution, String> {
    match (reference, key) {
        (Some(reference), None) => state
            .collection_by_ref(reference)
            .cloned()
            .ok_or_else(|| format!("Unknown collection ref '{reference}'")),
        (None, Some(key)) => find_collection_by_key_planning(pool, state, key)
            .await?
            .ok_or_else(|| format!("Collection '{}' not found", collection_key_label(key))),
        _ => Err("Exactly one of collection_ref or collection_key must be provided".to_string()),
    }
}

pub(super) async fn resolve_collection_parent_planning(
    pool: &impl crate::storage::StorageContext,
    state: &mut PlanningState,
    input: &ImportCollectionInput,
) -> Result<CollectionResolution, String> {
    match (
        input.parent_collection_ref.as_deref(),
        input.parent_collection_key.as_ref(),
    ) {
        (None, None) => resolve_root_collection_planning(pool, state).await,
        (Some(reference), None) => state
            .collection_by_ref(reference)
            .cloned()
            .ok_or_else(|| format!("Unknown collection ref '{reference}'")),
        (None, Some(key)) => resolve_collection_planning(pool, state, None, Some(key)).await,
        (Some(_), Some(_)) => Err(
            "At most one of parent_collection_ref or parent_collection_key may be provided"
                .to_string(),
        ),
    }
}

pub(super) async fn resolve_collection_by_id_planning(
    pool: &impl crate::storage::StorageContext,
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
        .map(storage_collection_to_resolution)
        .ok_or_else(|| format!("Collection id '{}' not found", collection_id))?;
    state.remember_collection(None, collection.clone());
    Ok(collection)
}

pub(super) async fn resolve_class_planning(
    pool: &impl crate::storage::StorageContext,
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
                .map(storage_class_to_resolution)
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
    pool: &impl crate::storage::StorageContext,
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
