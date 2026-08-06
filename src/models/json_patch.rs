use std::cmp::Ordering;
use std::fmt::Display;

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
            apply_json_patch_operation(&mut patched, operation, operation_index)?;
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

fn apply_json_patch_operation(
    document: &mut serde_json::Value,
    operation: &json_patch::PatchOperation,
    operation_index: usize,
) -> Result<(), ApiError> {
    if let json_patch::PatchOperation::Test(test) = operation {
        let result = document
            .pointer(test.path.as_str())
            .ok_or(json_patch::PatchErrorKind::InvalidPointer)
            .and_then(|actual| {
                json_patch_values_equal(actual, &test.value)
                    .then_some(())
                    .ok_or(json_patch::PatchErrorKind::TestFailed)
            });
        return result
            .map_err(|kind| json_patch_operation_error(operation_index, &test.path, &kind));
    }

    json_patch::patch(document, std::slice::from_ref(operation))
        .map_err(|error| json_patch_operation_error(operation_index, &error.path, &error.kind))
}

fn json_patch_operation_error(
    operation_index: usize,
    path: &impl Display,
    kind: &impl Display,
) -> ApiError {
    ApiError::Conflict(format!(
        "JSON Patch operation at index {operation_index} failed at path '{path}': {kind}"
    ))
}

/// RFC 6902 defines `test` equality recursively and compares JSON numbers by
/// numeric value rather than their serialized representation.
fn json_patch_values_equal(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    match (left, right) {
        (serde_json::Value::Number(left), serde_json::Value::Number(right)) => {
            if json_number_is_zero(left) && json_number_is_zero(right) {
                return true;
            }
            hubuum_computed_fields::compare_decimal_strings(&left.to_string(), &right.to_string())
                == Some(Ordering::Equal)
        }
        (serde_json::Value::Array(left), serde_json::Value::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| json_patch_values_equal(left, right))
        }
        (serde_json::Value::Object(left), serde_json::Value::Object(right)) => {
            left.len() == right.len()
                && left.iter().all(|(key, left)| {
                    right
                        .get(key)
                        .is_some_and(|right| json_patch_values_equal(left, right))
                })
        }
        _ => left == right,
    }
}

fn json_number_is_zero(number: &serde_json::Number) -> bool {
    number
        .to_string()
        .trim_start_matches('-')
        .split(['e', 'E'])
        .next()
        .is_some_and(|mantissa| mantissa.bytes().all(|byte| matches!(byte, b'0' | b'.')))
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
