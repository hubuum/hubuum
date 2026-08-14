//! Bounded RFC 6902 behavior shared by API and storage boundaries.

use std::cmp::Ordering;
use std::fmt::{self, Display};
use std::str::FromStr;

use bigdecimal::BigDecimal;
use json_patch::{
    Patch, PatchErrorKind, PatchOperation, TestOperation, patch as apply_patch_operation,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use serde_json::{Number, Value};

use crate::{
    MAX_STORAGE_JSON_NESTING_DEPTH, StorageJsonValidationError, validate_storage_json_value,
};

/// Maximum number of operations in one Hubuum JSON Patch document.
pub const MAX_JSON_PATCH_OPERATIONS: usize = 1_000;
/// Maximum pointer depth in one Hubuum JSON Patch operation.
pub const MAX_JSON_PATCH_POINTER_DEPTH: usize = 128;
/// Maximum serialized size of a patch input or intermediate result.
pub const MAX_JSON_PATCH_BYTES: usize = 2_097_152;
/// Maximum cumulative intermediate-result bytes inspected while applying a patch.
pub const MAX_JSON_PATCH_WORK_BYTES: usize = 32 * 1024 * 1024;
/// Maximum nesting depth of a patch input or result.
pub const MAX_JSON_PATCH_RESULT_NESTING_DEPTH: usize = MAX_STORAGE_JSON_NESTING_DEPTH;

/// Stable classification of a bounded JSON Patch failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonPatchErrorKind {
    BadRequest,
    Conflict,
    PayloadTooLarge,
}

/// Backend-neutral bounded JSON Patch failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonPatchError {
    kind: JsonPatchErrorKind,
    message: String,
}

impl JsonPatchError {
    fn new(kind: JsonPatchErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(JsonPatchErrorKind::BadRequest, message)
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self::new(JsonPatchErrorKind::Conflict, message)
    }

    fn payload_too_large(message: impl Into<String>) -> Self {
        Self::new(JsonPatchErrorKind::PayloadTooLarge, message)
    }

    #[must_use]
    pub const fn kind(&self) -> JsonPatchErrorKind {
        self.kind
    }

    #[must_use]
    pub fn into_parts(self) -> (JsonPatchErrorKind, String) {
        (self.kind, self.message)
    }
}

impl fmt::Display for JsonPatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for JsonPatchError {}

/// RFC 6902 document with Hubuum's stable operation and resource bounds.
#[derive(Clone, Debug, PartialEq)]
pub struct BoundedJsonPatch(Patch);

impl BoundedJsonPatch {
    fn validate(patch: Patch) -> Result<Self, String> {
        if patch.0.len() > MAX_JSON_PATCH_OPERATIONS {
            return Err(format!(
                "JSON Patch contains {} operations; at most {MAX_JSON_PATCH_OPERATIONS} are allowed",
                patch.0.len()
            ));
        }

        for (index, operation) in patch.0.iter().enumerate() {
            validate_patch_pointer_depth(index, "path", operation.path().count())?;
            let from_depth = match operation {
                PatchOperation::Move(operation) => Some(operation.from.count()),
                PatchOperation::Copy(operation) => Some(operation.from.count()),
                _ => None,
            };
            if let Some(depth) = from_depth {
                validate_patch_pointer_depth(index, "from", depth)?;
            }
        }

        Ok(Self(patch))
    }

    /// Apply every operation to a clone of `document` and return the result
    /// only when the complete bounded patch succeeds.
    pub fn apply(&self, document: &Value) -> Result<Value, JsonPatchError> {
        let mut cumulative_bytes =
            validate_json_patch_result(document, PatchResultStage::InitialDocument)?;
        let mut patched = document.clone();
        for (operation_index, operation) in self.0.iter().enumerate() {
            apply_bounded_patch_operation(&mut patched, operation, operation_index)?;
            let result_bytes =
                validate_json_patch_result(&patched, PatchResultStage::Operation(operation_index))?;
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

fn apply_bounded_patch_operation(
    document: &mut Value,
    operation: &PatchOperation,
    operation_index: usize,
) -> Result<(), JsonPatchError> {
    if let PatchOperation::Test(test) = operation {
        return apply_test_operation(document, test)
            .map_err(|kind| json_patch_operation_error(operation_index, &test.path, &kind));
    }

    apply_patch_operation(document, std::slice::from_ref(operation))
        .map_err(|error| json_patch_operation_error(operation_index, &error.path, &error.kind))
}

fn apply_test_operation(document: &Value, test: &TestOperation) -> Result<(), PatchErrorKind> {
    let actual = document
        .pointer(test.path.as_str())
        .ok_or(PatchErrorKind::InvalidPointer)?;
    if json_patch_values_equal(actual, &test.value) {
        Ok(())
    } else {
        Err(PatchErrorKind::TestFailed)
    }
}

fn json_patch_operation_error(
    operation_index: usize,
    path: &impl Display,
    kind: &impl Display,
) -> JsonPatchError {
    JsonPatchError::conflict(format!(
        "JSON Patch operation at index {operation_index} failed at path '{path}': {kind}"
    ))
}

fn json_patch_values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => json_patch_numbers_equal(left, right),
        (Value::Array(left), Value::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| json_patch_values_equal(left, right))
        }
        (Value::Object(left), Value::Object(right)) => {
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

fn json_patch_numbers_equal(left: &Number, right: &Number) -> bool {
    let left = left.to_string();
    let right = right.to_string();
    if json_number_source_is_zero(&left) && json_number_source_is_zero(&right) {
        return true;
    }
    let Ok(left) = BigDecimal::from_str(&left) else {
        return false;
    };
    let Ok(right) = BigDecimal::from_str(&right) else {
        return false;
    };
    left.cmp(&right) == Ordering::Equal
}

fn json_number_source_is_zero(number: &str) -> bool {
    number
        .trim_start_matches('-')
        .split(['e', 'E'])
        .next()
        .is_some_and(|mantissa| mantissa.bytes().all(|byte| matches!(byte, b'0' | b'.')))
}

#[derive(Clone, Copy)]
enum PatchResultStage {
    InitialDocument,
    Operation(usize),
}

impl Display for PatchResultStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InitialDocument => formatter.write_str("loading the current document"),
            Self::Operation(index) => write!(formatter, "operation {index}"),
        }
    }
}

fn validate_json_patch_result(
    document: &Value,
    stage: PatchResultStage,
) -> Result<usize, JsonPatchError> {
    match validate_storage_json_value(document) {
        Ok(()) => {}
        Err(StorageJsonValidationError::UnsupportedValue) => {
            return Err(JsonPatchError::bad_request(format!(
                "JSON Patch result after {stage} contains JSON that Hubuum storage cannot represent"
            )));
        }
        Err(StorageJsonValidationError::NestingTooDeep) => {
            return Err(JsonPatchError::payload_too_large(format!(
                "JSON Patch result after {stage} exceeds the maximum nesting depth of {MAX_JSON_PATCH_RESULT_NESTING_DEPTH}"
            )));
        }
    }

    let serialized_bytes = serde_json::to_vec(document)
        .map_err(|error| JsonPatchError::bad_request(error.to_string()))?
        .len();
    if serialized_bytes > MAX_JSON_PATCH_BYTES {
        return Err(JsonPatchError::payload_too_large(format!(
            "JSON Patch result after {stage} is {serialized_bytes} bytes; the limit is {MAX_JSON_PATCH_BYTES} bytes"
        )));
    }
    Ok(serialized_bytes)
}

fn json_patch_work_limit_error() -> JsonPatchError {
    JsonPatchError::payload_too_large(format!(
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
        let patch = Patch::deserialize(deserializer)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_compares_numeric_representations_recursively() {
        let patch = serde_json::from_str::<BoundedJsonPatch>(
            r#"[
                {"op":"add","path":"/value","value":{"direct":1.0,"nested":[2e0]}},
                {"op":"test","path":"/value","value":{"nested":[2.00],"direct":1}},
                {"op":"add","path":"/test_passed","value":true}
            ]"#,
        )
        .unwrap();

        let patched = patch.apply(&serde_json::json!({})).unwrap();

        assert_eq!(patched["test_passed"], true);
    }

    #[test]
    fn failed_operations_are_classified_as_conflicts() {
        let patch = serde_json::from_value::<BoundedJsonPatch>(serde_json::json!([
            {"op": "replace", "path": "/missing", "value": true}
        ]))
        .unwrap();

        let error = patch.apply(&serde_json::json!({})).unwrap_err();

        assert_eq!(error.kind(), JsonPatchErrorKind::Conflict);
    }
}
