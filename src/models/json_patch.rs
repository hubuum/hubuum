use std::cmp::Ordering;
use std::fmt::Display;

use json_patch::{
    Patch, PatchErrorKind, PatchOperation, TestOperation, patch as apply_patch_operation,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use serde_json::{Number, Value};
use utoipa::openapi::{RefOr, schema::Schema};
use utoipa::{PartialSchema, ToSchema};

use crate::errors::ApiError;
use hubuum_storage_postgres::jsonb::{
    MAX_POSTGRES_JSONB_NESTING_DEPTH, PostgresJsonbValidationError, validate_postgres_jsonb_value,
};

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
pub(crate) struct BoundedJsonPatch(Patch);

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

    /// Apply every operation to a clone of `document` and return it only when
    /// the complete bounded patch succeeds.
    pub(crate) fn apply(&self, document: &Value) -> Result<Value, ApiError> {
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

    pub(crate) fn openapi_schema(description: &str, example: Value) -> RefOr<Schema> {
        utoipa::openapi::schema::ArrayBuilder::new()
            .items(PatchOperation::schema())
            .max_items(Some(MAX_JSON_PATCH_OPERATIONS))
            .description(Some(description))
            .examples([example])
            .build()
            .into()
    }

    pub(crate) fn register_openapi_schemas(schemas: &mut Vec<(String, RefOr<Schema>)>) {
        schemas.push((
            PatchOperation::name().into_owned(),
            PatchOperation::schema(),
        ));
        PatchOperation::schemas(schemas);
    }
}

fn apply_bounded_patch_operation(
    document: &mut Value,
    operation: &PatchOperation,
    operation_index: usize,
) -> Result<(), ApiError> {
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
) -> ApiError {
    ApiError::Conflict(format!(
        "JSON Patch operation at index {operation_index} failed at path '{path}': {kind}"
    ))
}

/// RFC 6902 defines `test` equality recursively and compares JSON numbers by
/// numeric value rather than their serialized representation.
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
    hubuum_computed_fields::compare_decimal_strings(&left, &right) == Some(Ordering::Equal)
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
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InitialDocument => formatter.write_str("loading the current document"),
            Self::Operation(index) => write!(formatter, "operation {index}"),
        }
    }
}

fn validate_json_patch_result(
    document: &Value,
    stage: PatchResultStage,
) -> Result<usize, ApiError> {
    match validate_postgres_jsonb_value(document) {
        Ok(()) => {}
        Err(PostgresJsonbValidationError::UnsupportedValue) => {
            return Err(ApiError::BadRequest(format!(
                "JSON Patch result after {stage} contains JSON that PostgreSQL JSONB cannot represent"
            )));
        }
        Err(PostgresJsonbValidationError::NestingTooDeep) => {
            return Err(ApiError::PayloadTooLarge(format!(
                "JSON Patch result after {stage} exceeds the maximum nesting depth of {MAX_JSON_PATCH_RESULT_NESTING_DEPTH}"
            )));
        }
    }

    let serialized_bytes = serde_json::to_vec(document)?.len();
    if serialized_bytes > MAX_JSON_PATCH_BYTES {
        return Err(ApiError::PayloadTooLarge(format!(
            "JSON Patch result after {stage} is {serialized_bytes} bytes; the limit is {MAX_JSON_PATCH_BYTES} bytes"
        )));
    }
    Ok(serialized_bytes)
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
}
