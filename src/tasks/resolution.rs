use super::helpers::{class_to_resolution, collection_to_resolution, object_to_resolution};
use super::types::{
    ClassResolution, CollectionResolution, ObjectResolution, PlanningState, RuntimeState,
};
use crate::db::DbPool;
use crate::db::traits::task_import::{
    lookup_class_by_collection_and_name, lookup_class_by_collection_and_name_db,
    lookup_collection_by_id, lookup_collection_by_name, lookup_collection_by_name_db,
    lookup_object_by_class_and_name, lookup_object_by_class_and_name_db,
};
use crate::errors::ApiError;
use crate::models::{ClassKey, Collection, CollectionKey, HubuumClass, HubuumObject, ObjectKey};

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
        (None, Some(key)) => {
            if let Some(collection) = state.collections_by_name.get(&key.name) {
                return Ok(collection.clone());
            }
            if state.missing_collection_names.contains(&key.name) {
                return Err(format!("Collection '{}' not found", key.name));
            }

            let collection = lookup_collection_by_name(pool, &key.name)
                .await
                .map_err(|err| err.to_string())?
                .map(collection_to_resolution)
                .ok_or_else(|| format!("Collection '{}' not found", key.name))?;
            remember_collection(state, None, collection.clone());
            Ok(collection)
        }
        _ => Err("Exactly one of collection_ref or collection_key must be provided".to_string()),
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

pub(super) fn resolve_collection_runtime(
    conn: &mut diesel::PgConnection,
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
        (None, Some(key)) => lookup_collection_by_name_db(conn, &key.name)?.ok_or_else(|| {
            ApiError::NotFound(format!(
                "Collection '{}' not found during execution",
                key.name
            ))
        }),
        _ => Err(ApiError::BadRequest(
            "Exactly one of collection_ref or collection_key must be provided".to_string(),
        )),
    }
}

pub(super) fn resolve_class_runtime(
    conn: &mut diesel::PgConnection,
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
            )?;
            lookup_class_by_collection_and_name_db(conn, collection.id, &key.name)?.ok_or_else(
                || {
                    ApiError::NotFound(format!(
                        "Class '{}' not found in collection '{}' during execution",
                        key.name, collection.name
                    ))
                },
            )
        }
        _ => Err(ApiError::BadRequest(
            "Exactly one of class_ref or class_key must be provided".to_string(),
        )),
    }
}

pub(super) fn resolve_object_runtime(
    conn: &mut diesel::PgConnection,
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
            )?;
            lookup_object_by_class_and_name_db(conn, class.id, &key.name)?.ok_or_else(|| {
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
        .insert(collection.name.clone(), collection.clone());
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
