use super::helpers::{class_to_resolution, collection_to_resolution, object_to_resolution};
use super::types::{
    ClassResolution, CollectionResolution, ObjectResolution, PlanningState, RuntimeState,
};
use crate::db::DbPool;
use crate::db::traits::task_import::{
    lookup_class_by_collection_and_name, lookup_class_by_collection_and_name_db,
    lookup_collection_by_id, lookup_collection_by_key, lookup_collection_by_key_db,
    lookup_collection_child_by_name_db, lookup_collections_by_name,
    lookup_object_by_class_and_name, lookup_object_by_class_and_name_db, lookup_root_collection,
    lookup_root_collection_db,
};
use crate::errors::ApiError;
use crate::models::{
    ClassKey, Collection, CollectionKey, HubuumClass, HubuumObject, ImportCollectionInput,
    ObjectKey,
};

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
    pool: &DbPool,
    state: &mut PlanningState,
    key: &CollectionKey,
) -> Result<Option<CollectionResolution>, String> {
    validate_collection_key_path(key)?;

    if key.path.is_some() {
        let collection = lookup_collection_by_key(pool, key)
            .await
            .map_err(|err| err.to_string())?
            .map(collection_to_resolution);
        if let Some(collection) = &collection {
            remember_collection(state, None, collection.clone());
        }
        return Ok(collection);
    }

    if state.missing_collection_names.contains(&key.name) {
        return Ok(None);
    }

    let mut matches = state
        .collections_by_name
        .get(&key.name)
        .cloned()
        .unwrap_or_default();
    for collection in lookup_collections_by_name(pool, &key.name)
        .await
        .map_err(|err| err.to_string())?
        .into_iter()
        .map(collection_to_resolution)
    {
        if !matches.iter().any(|known| known.id == collection.id) {
            matches.push(collection);
        }
    }

    match matches.as_slice() {
        [] => {
            state.missing_collection_names.insert(key.name.clone());
            Ok(None)
        }
        [collection] => {
            let collection = collection.clone();
            remember_collection(state, None, collection.clone());
            Ok(Some(collection))
        }
        _ => Err(format!(
            "Collection name '{}' is ambiguous; use collection_key.path",
            key.name
        )),
    }
}

async fn resolve_root_collection_planning(
    pool: &DbPool,
    state: &mut PlanningState,
) -> Result<CollectionResolution, String> {
    if let Some(collection) = state
        .collections_by_parent_name
        .get(&(None, "root".to_string()))
        .cloned()
    {
        return Ok(collection);
    }

    let collection = lookup_root_collection(pool)
        .await
        .map_err(|err| err.to_string())
        .map(collection_to_resolution)?;
    remember_collection(state, None, collection.clone());
    Ok(collection)
}

pub(super) async fn resolve_collection_planning(
    pool: &DbPool,
    state: &mut PlanningState,
    reference: Option<&str>,
    key: Option<&CollectionKey>,
) -> Result<CollectionResolution, String> {
    match (reference, key) {
        (Some(reference), None) => state
            .collections_by_ref
            .get(reference)
            .cloned()
            .ok_or_else(|| format!("Unknown collection ref '{reference}'")),
        (None, Some(key)) => find_collection_by_key_planning(pool, state, key)
            .await?
            .ok_or_else(|| format!("Collection '{}' not found", collection_key_label(key))),
        _ => Err("Exactly one of collection_ref or collection_key must be provided".to_string()),
    }
}

pub(super) async fn resolve_collection_parent_planning(
    pool: &DbPool,
    state: &mut PlanningState,
    input: &ImportCollectionInput,
) -> Result<CollectionResolution, String> {
    match (
        input.parent_collection_ref.as_deref(),
        input.parent_collection_key.as_ref(),
    ) {
        (None, None) => resolve_root_collection_planning(pool, state).await,
        (Some(reference), None) => state
            .collections_by_ref
            .get(reference)
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
    pool: &DbPool,
    state: &mut PlanningState,
    collection_id: i32,
) -> Result<CollectionResolution, String> {
    if let Some(collection) = state.collections_by_id.get(&collection_id) {
        return Ok(collection.clone());
    }

    let collection = lookup_collection_by_id(pool, collection_id)
        .await
        .map_err(|err| err.to_string())?
        .map(collection_to_resolution)
        .ok_or_else(|| format!("Collection id '{}' not found", collection_id))?;
    remember_collection(state, None, collection.clone());
    Ok(collection)
}

pub(super) async fn resolve_class_planning(
    pool: &DbPool,
    state: &mut PlanningState,
    reference: Option<&str>,
    key: Option<&ClassKey>,
) -> Result<ClassResolution, String> {
    match (reference, key) {
        (Some(reference), None) => state
            .classes_by_ref
            .get(reference)
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
            if let Some(class) = state.classes_by_key.get(&(collection.id, key.name.clone())) {
                return Ok(class.clone());
            }
            if state
                .missing_class_keys
                .contains(&(collection.id, key.name.clone()))
            {
                return Err(format!(
                    "Class '{}' not found in collection '{}'",
                    key.name, collection.name
                ));
            }

            let class = lookup_class_by_collection_and_name(pool, collection.id, &key.name)
                .await
                .map_err(|err| err.to_string())?
                .map(class_to_resolution)
                .ok_or_else(|| {
                    format!(
                        "Class '{}' not found in collection '{}'",
                        key.name, collection.name
                    )
                })?;
            remember_class(state, None, class.clone());
            Ok(class)
        }
        _ => Err("Exactly one of class_ref or class_key must be provided".to_string()),
    }
}

pub(super) async fn resolve_object_planning(
    pool: &DbPool,
    state: &mut PlanningState,
    reference: Option<&str>,
    key: Option<&ObjectKey>,
) -> Result<ObjectResolution, String> {
    match (reference, key) {
        (Some(reference), None) => state
            .objects_by_ref
            .get(reference)
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
            if let Some(object) = state.objects_by_key.get(&(class.id, key.name.clone())) {
                return Ok(object.clone());
            }
            if state
                .missing_object_keys
                .contains(&(class.id, key.name.clone()))
            {
                return Err(format!(
                    "Object '{}' not found in class '{}'",
                    key.name, class.name
                ));
            }

            let object = lookup_object_by_class_and_name(pool, class.id, &key.name)
                .await
                .map_err(|err| err.to_string())?
                .map(object_to_resolution)
                .ok_or_else(|| {
                    format!("Object '{}' not found in class '{}'", key.name, class.name)
                })?;
            remember_object(state, None, object.clone());
            Ok(object)
        }
        _ => Err("Exactly one of object_ref or object_key must be provided".to_string()),
    }
}

pub(super) async fn resolve_collection_runtime(
    conn: &mut crate::db::DbConnection,
    runtime: &RuntimeState,
    reference: Option<&str>,
    key: Option<&CollectionKey>,
) -> Result<Collection, ApiError> {
    match (reference, key) {
        (Some(reference), None) => runtime
            .collections_by_ref
            .get(reference)
            .cloned()
            .ok_or_else(|| ApiError::BadRequest(format!("Unknown collection ref '{reference}'"))),
        (None, Some(key)) => lookup_collection_by_key_db(conn, key)
            .await?
            .ok_or_else(|| {
                ApiError::NotFound(format!(
                    "Collection '{}' not found during execution",
                    collection_key_label(key)
                ))
            }),
        _ => Err(ApiError::BadRequest(
            "Exactly one of collection_ref or collection_key must be provided".to_string(),
        )),
    }
}

pub(super) async fn resolve_collection_parent_runtime(
    conn: &mut crate::db::DbConnection,
    runtime: &RuntimeState,
    input: &ImportCollectionInput,
) -> Result<Collection, ApiError> {
    match (
        input.parent_collection_ref.as_deref(),
        input.parent_collection_key.as_ref(),
    ) {
        (None, None) => lookup_root_collection_db(conn).await,
        (Some(reference), None) => runtime
            .collections_by_ref
            .get(reference)
            .cloned()
            .ok_or_else(|| ApiError::BadRequest(format!("Unknown collection ref '{reference}'"))),
        (None, Some(key)) => lookup_collection_by_key_db(conn, key)
            .await?
            .ok_or_else(|| {
                ApiError::NotFound(format!(
                    "Collection '{}' not found during execution",
                    collection_key_label(key)
                ))
            }),
        (Some(_), Some(_)) => Err(ApiError::BadRequest(
            "At most one of parent_collection_ref or parent_collection_key may be provided"
                .to_string(),
        )),
    }
}

pub(super) async fn lookup_existing_collection_for_import_db(
    conn: &mut crate::db::DbConnection,
    parent_collection_id: i32,
    name: &str,
) -> Result<Option<Collection>, ApiError> {
    lookup_collection_child_by_name_db(conn, parent_collection_id, name).await
}

pub(super) async fn resolve_class_runtime(
    conn: &mut crate::db::DbConnection,
    runtime: &RuntimeState,
    reference: Option<&str>,
    key: Option<&ClassKey>,
) -> Result<HubuumClass, ApiError> {
    match (reference, key) {
        (Some(reference), None) => runtime
            .classes_by_ref
            .get(reference)
            .cloned()
            .ok_or_else(|| ApiError::BadRequest(format!("Unknown class ref '{reference}'"))),
        (None, Some(key)) => {
            let collection = resolve_collection_runtime(
                conn,
                runtime,
                key.collection_ref.as_deref(),
                key.collection_key.as_ref(),
            )
            .await?;
            lookup_class_by_collection_and_name_db(conn, collection.id, &key.name)
                .await?
                .ok_or_else(|| {
                    ApiError::NotFound(format!(
                        "Class '{}' not found in collection '{}' during execution",
                        key.name, collection.name
                    ))
                })
        }
        _ => Err(ApiError::BadRequest(
            "Exactly one of class_ref or class_key must be provided".to_string(),
        )),
    }
}

pub(super) async fn resolve_object_runtime(
    conn: &mut crate::db::DbConnection,
    runtime: &RuntimeState,
    reference: Option<&str>,
    key: Option<&ObjectKey>,
) -> Result<HubuumObject, ApiError> {
    match (reference, key) {
        (Some(reference), None) => runtime
            .objects_by_ref
            .get(reference)
            .cloned()
            .ok_or_else(|| ApiError::BadRequest(format!("Unknown object ref '{reference}'"))),
        (None, Some(key)) => {
            let class = resolve_class_runtime(
                conn,
                runtime,
                key.class_ref.as_deref(),
                key.class_key.as_ref(),
            )
            .await?;
            lookup_object_by_class_and_name_db(conn, class.id, &key.name)
                .await?
                .ok_or_else(|| {
                    ApiError::NotFound(format!(
                        "Object '{}' not found in class '{}' during execution",
                        key.name, class.name
                    ))
                })
        }
        _ => Err(ApiError::BadRequest(
            "Exactly one of object_ref or object_key must be provided".to_string(),
        )),
    }
}

pub(super) fn remember_collection(
    state: &mut PlanningState,
    reference: Option<String>,
    collection: CollectionResolution,
) {
    state
        .collections_by_id
        .insert(collection.id, collection.clone());
    state
        .collections_by_name
        .entry(collection.name.clone())
        .or_default()
        .retain(|known| known.id != collection.id);
    state
        .collections_by_name
        .entry(collection.name.clone())
        .or_default()
        .push(collection.clone());
    state.collections_by_parent_name.insert(
        (collection.parent_collection_id, collection.name.clone()),
        collection.clone(),
    );
    if let Some(reference) = reference {
        state.collections_by_ref.insert(reference, collection);
    }
}

pub(super) fn remember_class(
    state: &mut PlanningState,
    reference: Option<String>,
    class: ClassResolution,
) {
    state
        .classes_by_key
        .insert((class.collection_id, class.name.clone()), class.clone());
    if let Some(reference) = reference {
        state.classes_by_ref.insert(reference, class);
    }
}

pub(super) fn remember_object(
    state: &mut PlanningState,
    reference: Option<String>,
    object: ObjectResolution,
) {
    state
        .objects_by_key
        .insert((object.class_id, object.name.clone()), object.clone());
    if let Some(reference) = reference {
        state.objects_by_ref.insert(reference, object);
    }
}
