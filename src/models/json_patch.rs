use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::db::json::{
    MAX_POSTGRES_JSONB_NESTING_DEPTH, PostgresJsonbValidationError, validate_postgres_jsonb_value,
};
use crate::errors::ApiError;

pub(crate) const MAX_JSON_PATCH_OPERATIONS: usize = 1_000;
pub(crate) const MAX_JSON_PATCH_POINTER_DEPTH: usize = 128;
pub(crate) const MAX_JSON_PATCH_BYTES: usize = 2_097_152;
pub(crate) const MAX_JSON_PATCH_WORK_BYTES: usize = 32 * 1024 * 1024;
pub(crate) const MAX_JSON_PATCH_RESULT_NESTING_DEPTH: usize = MAX_POSTGRES_JSONB_NESTING_DEPTH;

/// Reusable RFC 6902 document with bounds suitable for JSONB-backed resources.
///
/// Domain wrappers keep their own public descriptions and final-root invariants
/// while this type owns operation, pointer, result, and application-work bounds.
#[derive(Clone, Debug)]
pub(crate) struct BoundedJsonPatch(json_patch::Patch);

impl BoundedJsonPatch {
    fn validate(patch: json_patch::Patch) -> Result<Self, String> {
        if patch.0.len() > MAX_JSON_PATCH_OPERATIONS {
            return Err(format!(
                "JSON Patch contains {} operations; at most {MAX_JSON_PATCH_OPERATIONS} are allowed",
                patch.0.len()
            ));
        }

        for (index, operation) in patch.0.iter().enumerate() {
            validate_patch_pointer_depth(index, "path", operation.path().count())?;
            let from_depth = match operation {
                json_patch::PatchOperation::Move(operation) => Some(operation.from.count()),
                json_patch::PatchOperation::Copy(operation) => Some(operation.from.count()),
                _ => None,
            };
            if let Some(depth) = from_depth {
                validate_patch_pointer_depth(index, "from", depth)?;
            }
        }

        Ok(Self(patch))
    }

    /// Apply every operation to a clone of `document` and return it only when
    /// the complete bounded patch succeeds.
    pub(crate) fn apply(
        &self,
        document: &serde_json::Value,
    ) -> Result<serde_json::Value, ApiError> {
        let mut cumulative_bytes = validate_json_patch_result(document, None)?;
        let mut patched = document.clone();
        for (operation_index, operation) in self.0.iter().enumerate() {
            json_patch::patch(&mut patched, std::slice::from_ref(operation)).map_err(|error| {
                ApiError::Conflict(format!(
                    "JSON Patch operation at index {operation_index} failed at path '{}': {}",
                    error.path, error.kind
                ))
            })?;
            let result_bytes = validate_json_patch_result(&patched, Some(operation_index))?;
            cumulative_bytes = cumulative_bytes
                .checked_add(result_bytes)
                .ok_or_else(json_patch_work_limit_error)?;
            if cumulative_bytes > MAX_JSON_PATCH_WORK_BYTES {
                return Err(json_patch_work_limit_error());
            }
        }
        Ok(patched)
    }
}

fn validate_json_patch_result(
    document: &serde_json::Value,
    operation_index: Option<usize>,
) -> Result<usize, ApiError> {
    match validate_postgres_jsonb_value(document) {
        Ok(()) => {}
        Err(PostgresJsonbValidationError::UnsupportedValue) => {
            return Err(ApiError::BadRequest(format!(
                "JSON Patch result after {} contains JSON that PostgreSQL JSONB cannot represent",
                patch_result_stage(operation_index)
            )));
        }
        Err(PostgresJsonbValidationError::NestingTooDeep) => {
            return Err(ApiError::PayloadTooLarge(format!(
                "JSON Patch result after {} exceeds the maximum nesting depth of {MAX_JSON_PATCH_RESULT_NESTING_DEPTH}",
                patch_result_stage(operation_index)
            )));
        }
    }

    let serialized_bytes = serde_json::to_vec(document)?.len();
    if serialized_bytes > MAX_JSON_PATCH_BYTES {
        return Err(ApiError::PayloadTooLarge(format!(
            "JSON Patch result after {} is {serialized_bytes} bytes; the limit is {MAX_JSON_PATCH_BYTES} bytes",
            patch_result_stage(operation_index)
        )));
    }
    Ok(serialized_bytes)
}

fn patch_result_stage(operation_index: Option<usize>) -> String {
    operation_index.map_or_else(
        || "loading the current document".to_string(),
        |index| format!("operation {index}"),
    )
}

fn json_patch_work_limit_error() -> ApiError {
    ApiError::PayloadTooLarge(format!(
        "JSON Patch exceeds the cumulative application-work limit of {MAX_JSON_PATCH_WORK_BYTES} bytes"
    ))
}

fn validate_patch_pointer_depth(
    operation_index: usize,
    pointer_name: &str,
    depth: usize,
) -> Result<(), String> {
    if depth > MAX_JSON_PATCH_POINTER_DEPTH {
        return Err(format!(
            "JSON Patch operation at index {operation_index} has a `{pointer_name}` pointer depth of {depth}; at most {MAX_JSON_PATCH_POINTER_DEPTH} segments are allowed"
        ));
    }
    Ok(())
}

impl<'de> Deserialize<'de> for BoundedJsonPatch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let patch = json_patch::Patch::deserialize(deserializer)?;
        Self::validate(patch).map_err(de::Error::custom)
    }
}

impl Serialize for BoundedJsonPatch {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}
