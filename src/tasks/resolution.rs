use super::helpers::{class_to_resolution, namespace_to_resolution, object_to_resolution};
use super::types::{
    ClassResolution, NamespaceResolution, ObjectResolution, PlanningState, RuntimeState,
};
use crate::db::DbPool;
use crate::db::traits::task_import::{
    lookup_class_by_namespace_and_name, lookup_class_by_namespace_and_name_db,
    lookup_namespace_by_id, lookup_namespace_by_name, lookup_namespace_by_name_db,
    lookup_object_by_class_and_name, lookup_object_by_class_and_name_db,
};
use crate::errors::ApiError;
use crate::models::{ClassKey, HubuumClass, HubuumObject, Namespace, NamespaceKey, ObjectKey};

pub(super) async fn resolve_namespace_planning(
    pool: &DbPool,
    state: &mut PlanningState,
    reference: Option<&str>,
    key: Option<&NamespaceKey>,
) -> Result<NamespaceResolution, String> {
    match (reference, key) {
        (Some(reference), None) => state
            .namespaces_by_ref
            .get(reference)
            .cloned()
            .ok_or_else(|| format!("Unknown namespace ref '{reference}'")),
        (None, Some(key)) => {
            if let Some(namespace) = state.namespaces_by_name.get(&key.name) {
                return Ok(namespace.clone());
            }
            if state.missing_namespace_names.contains(&key.name) {
                return Err(format!("Namespace '{}' not found", key.name));
            }

            let namespace = lookup_namespace_by_name(pool, &key.name)
                .await
                .map_err(|err| err.to_string())?
                .map(namespace_to_resolution)
                .ok_or_else(|| format!("Namespace '{}' not found", key.name))?;
            remember_namespace(state, None, namespace.clone());
            Ok(namespace)
        }
        _ => Err("Exactly one of namespace_ref or namespace_key must be provided".to_string()),
    }
}

pub(super) async fn resolve_namespace_by_id_planning(
    pool: &DbPool,
    state: &mut PlanningState,
    namespace_id: i32,
) -> Result<NamespaceResolution, String> {
    if let Some(namespace) = state.namespaces_by_id.get(&namespace_id) {
        return Ok(namespace.clone());
    }

    let namespace = lookup_namespace_by_id(pool, namespace_id)
        .await
        .map_err(|err| err.to_string())?
        .map(namespace_to_resolution)
        .ok_or_else(|| format!("Namespace id '{}' not found", namespace_id))?;
    remember_namespace(state, None, namespace.clone());
    Ok(namespace)
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
            let namespace = resolve_namespace_planning(
                pool,
                state,
                key.namespace_ref.as_deref(),
                key.namespace_key.as_ref(),
            )
            .await?;
            if let Some(class) = state.classes_by_key.get(&(namespace.id, key.name.clone())) {
                return Ok(class.clone());
            }
            if state
                .missing_class_keys
                .contains(&(namespace.id, key.name.clone()))
            {
                return Err(format!(
                    "Class '{}' not found in namespace '{}'",
                    key.name, namespace.name
                ));
            }

            let class = lookup_class_by_namespace_and_name(pool, namespace.id, &key.name)
                .await
                .map_err(|err| err.to_string())?
                .map(class_to_resolution)
                .ok_or_else(|| {
                    format!(
                        "Class '{}' not found in namespace '{}'",
                        key.name, namespace.name
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
                return Err(format!("Object '{}' not found in class '{}'", key.name, class.name));
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

pub(super) fn resolve_namespace_runtime(
    conn: &mut diesel::PgConnection,
    runtime: &RuntimeState,
    reference: Option<&str>,
    key: Option<&NamespaceKey>,
) -> Result<Namespace, ApiError> {
    match (reference, key) {
        (Some(reference), None) => runtime
            .namespaces_by_ref
            .get(reference)
            .cloned()
            .ok_or_else(|| ApiError::BadRequest(format!("Unknown namespace ref '{reference}'"))),
        (None, Some(key)) => lookup_namespace_by_name_db(conn, &key.name)?.ok_or_else(|| {
            ApiError::NotFound(format!(
                "Namespace '{}' not found during execution",
                key.name
            ))
        }),
        _ => Err(ApiError::BadRequest(
            "Exactly one of namespace_ref or namespace_key must be provided".to_string(),
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
            let namespace = resolve_namespace_runtime(
                conn,
                runtime,
                key.namespace_ref.as_deref(),
                key.namespace_key.as_ref(),
            )?;
            lookup_class_by_namespace_and_name_db(conn, namespace.id, &key.name)?.ok_or_else(|| {
                ApiError::NotFound(format!(
                    "Class '{}' not found in namespace '{}' during execution",
                    key.name, namespace.name
                ))
            })
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

pub(super) fn remember_namespace(
    state: &mut PlanningState,
    reference: Option<String>,
    namespace: NamespaceResolution,
) {
    state
        .namespaces_by_id
        .insert(namespace.id, namespace.clone());
    state
        .namespaces_by_name
        .insert(namespace.name.clone(), namespace.clone());
    if let Some(reference) = reference {
        state.namespaces_by_ref.insert(reference, namespace);
    }
}

pub(super) fn remember_class(
    state: &mut PlanningState,
    reference: Option<String>,
    class: ClassResolution,
) {
    state
        .classes_by_key
        .insert((class.namespace_id, class.name.clone()), class.clone());
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
