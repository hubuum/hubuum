use sha2::{Digest, Sha256};
use tracing::debug;

use crate::db::DbPool;
use crate::db::traits::task::insert_import_results;
use crate::errors::ApiError;
use crate::models::{
    HubuumClass, HubuumObject, ImportAtomicity, ImportCollisionPolicy, ImportMode,
    ImportPermissionPolicy, Namespace,
};

use super::types::{
    ClassResolution, ExecutionAccumulator, FailureKind, IMPORT_RESULTS_BATCH_SIZE,
    NamespaceResolution, ObjectResolution, PlannedTaskResult,
};

pub fn request_hash(payload: &serde_json::Value) -> Result<String, ApiError> {
    let bytes = serde_json::to_vec(&canonicalize_json(payload))?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

fn canonicalize_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(canonicalize_json).collect())
        }
        serde_json::Value::Object(map) => {
            let mut keys = map.keys().cloned().collect::<Vec<_>>();
            keys.sort();

            let mut canonical = serde_json::Map::with_capacity(map.len());
            for key in keys {
                canonical.insert(key.clone(), canonicalize_json(&map[&key]));
            }

            serde_json::Value::Object(canonical)
        }
        _ => value.clone(),
    }
}

pub(super) fn sanitize_error_for_storage(err: &ApiError) -> String {
    debug!(message = "Detailed error for import execution", error = %err);

    match err {
        ApiError::Conflict(msg) => format!("Conflict: {}", msg),
        ApiError::Forbidden(msg) => format!("Permission denied: {}", msg),
        ApiError::NotFound(msg) => format!("Not found: {}", msg),
        ApiError::BadRequest(msg) => format!("Invalid input: {}", msg),
        ApiError::ValidationError(msg) => format!("Validation failed: {}", msg),
        ApiError::DatabaseError(_) | ApiError::DbConnectionError(_) => {
            "Database operation failed".to_string()
        }
        ApiError::InternalServerError(_) => "An internal error occurred".to_string(),
        ApiError::HashError(_) => "Hashing operation failed".to_string(),
        ApiError::Unauthorized(msg) => format!("Unauthorized: {}", msg),
        ApiError::NotAcceptable(msg) => format!("Not acceptable: {}", msg),
        ApiError::PayloadTooLarge(msg) => format!("Payload too large: {}", msg),
        ApiError::OperatorMismatch(msg) => format!("Invalid operation: {}", msg),
        ApiError::InvalidIntegerRange(msg) => format!("Invalid value: {}", msg),
    }
}

pub(super) fn should_abort_best_effort_execution(err: &ApiError, mode: &ImportMode) -> bool {
    match err {
        ApiError::Conflict(_) => matches!(
            mode.collision_policy
                .unwrap_or(ImportCollisionPolicy::Abort),
            ImportCollisionPolicy::Abort
        ),
        ApiError::Forbidden(_) | ApiError::Unauthorized(_) => matches!(
            mode.permission_policy
                .unwrap_or(ImportPermissionPolicy::Abort),
            ImportPermissionPolicy::Abort
        ),
        _ => false,
    }
}

pub(super) fn should_abort_import(
    atomicity: ImportAtomicity,
    permission_policy: ImportPermissionPolicy,
    collision_policy: ImportCollisionPolicy,
    kind: FailureKind,
) -> bool {
    if matches!(atomicity, ImportAtomicity::Strict) {
        return true;
    }

    match kind {
        FailureKind::Permission => matches!(permission_policy, ImportPermissionPolicy::Abort),
        FailureKind::Collision => matches!(collision_policy, ImportCollisionPolicy::Abort),
        FailureKind::Validation | FailureKind::Resolution | FailureKind::Runtime => false,
    }
}

pub(super) fn planned_result(
    entity_kind: &str,
    action: &str,
    item_ref: Option<String>,
    identifier: Option<String>,
) -> PlannedTaskResult {
    PlannedTaskResult {
        item_ref,
        entity_kind: entity_kind.to_string(),
        action: action.to_string(),
        identifier,
        details: None,
    }
}

pub(super) fn identifier_namespace(namespace: &NamespaceResolution) -> String {
    namespace.name.clone()
}

pub(super) fn namespace_to_resolution(namespace: Namespace) -> NamespaceResolution {
    NamespaceResolution {
        id: namespace.id,
        name: namespace.name,
        description: namespace.description,
        exists_in_db: true,
    }
}

pub(super) fn class_to_resolution(class: HubuumClass) -> ClassResolution {
    ClassResolution {
        id: class.id,
        name: class.name,
        namespace_id: class.namespace_id,
        json_schema: class.json_schema,
        validate_schema: class.validate_schema,
        exists_in_db: true,
    }
}

pub(super) fn object_to_resolution(object: HubuumObject) -> ObjectResolution {
    ObjectResolution {
        id: object.id,
        name: object.name,
        namespace_id: object.namespace_id,
        class_id: object.hubuum_class_id,
        exists_in_db: true,
    }
}

pub(super) async fn flush_import_result_batches(
    pool: &DbPool,
    accumulator: &mut ExecutionAccumulator,
    force: bool,
) -> Result<(), ApiError> {
    while accumulator.results.len() >= IMPORT_RESULTS_BATCH_SIZE {
        let batch = accumulator
            .results
            .drain(..IMPORT_RESULTS_BATCH_SIZE)
            .collect::<Vec<_>>();
        insert_import_results(pool, &batch).await?;
    }

    if force && !accumulator.results.is_empty() {
        let batch = accumulator.results.drain(..).collect::<Vec<_>>();
        insert_import_results(pool, &batch).await?;
    }

    Ok(())
}

pub(super) fn normalize_pair(left: i32, right: i32) -> (i32, i32) {
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}
