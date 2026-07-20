use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use utoipa::{PartialSchema, ToSchema};

use crate::errors::ApiError;

/// Maximum number of operations accepted in one object-data JSON Patch document.
pub const MAX_OBJECT_DATA_PATCH_OPERATIONS: usize = 1_000;

/// Maximum number of reference tokens accepted in a JSON Pointer used by a patch operation.
pub const MAX_OBJECT_DATA_PATCH_POINTER_DEPTH: usize = 128;

/// Maximum serialized size of a JSON Patch request or its resulting raw object data.
pub const MAX_OBJECT_DATA_PATCH_BYTES: usize = 2_097_152;

/// Maximum cumulative serialized result bytes inspected while applying one JSON Patch document.
pub const MAX_OBJECT_DATA_PATCH_WORK_BYTES: usize = 32 * 1024 * 1024;

/// Maximum number of nested JSON containers accepted in patched object data.
pub const MAX_OBJECT_DATA_PATCH_RESULT_NESTING_DEPTH: usize = 64;

/// An RFC 6902 patch document whose pointers are relative to an object's raw `data` value.
///
/// The private representation keeps the third-party patch implementation behind a small,
/// validating domain interface.
#[derive(Clone, Debug)]
pub struct ObjectDataPatchDocument(json_patch::Patch);

impl PartialSchema for ObjectDataPatchDocument {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        utoipa::openapi::schema::ArrayBuilder::new()
            .items(json_patch::PatchOperation::schema())
            .max_items(Some(MAX_OBJECT_DATA_PATCH_OPERATIONS))
            .description(Some(
                "RFC 6902 operations applied relative to the root of an object's raw data document. Supports add, remove, replace, move, copy, and test. The resulting document is limited to 2 MiB and 64 nested containers, with a bounded cumulative application-work budget.",
            ))
            .examples([serde_json::json!([
                {"op": "add", "path": "/facts", "value": {"source": "inventory"}}
            ])])
            .build()
            .into()
    }
}

impl ToSchema for ObjectDataPatchDocument {
    fn schemas(
        schemas: &mut Vec<(
            String,
            utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>,
        )>,
    ) {
        schemas.push((
            json_patch::PatchOperation::name().into_owned(),
            json_patch::PatchOperation::schema(),
        ));
        json_patch::PatchOperation::schemas(schemas);
    }
}

impl ObjectDataPatchDocument {
    fn validate(patch: json_patch::Patch) -> Result<Self, String> {
        if patch.0.len() > MAX_OBJECT_DATA_PATCH_OPERATIONS {
            return Err(format!(
                "JSON Patch contains {} operations; at most {MAX_OBJECT_DATA_PATCH_OPERATIONS} are allowed",
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

    /// Apply the complete patch to `data`, returning a new value only if every operation succeeds.
    pub fn apply(&self, data: &serde_json::Value) -> Result<serde_json::Value, ApiError> {
        let mut cumulative_bytes = validate_object_data_patch_result(data, None)?;
        let mut patched = data.clone();
        for (operation_index, operation) in self.0.iter().enumerate() {
            json_patch::patch(&mut patched, std::slice::from_ref(operation)).map_err(|error| {
                ApiError::Conflict(format!(
                    "JSON Patch operation at index {operation_index} failed at path '{}': {}",
                    error.path, error.kind
                ))
            })?;
            let result_bytes = validate_object_data_patch_result(&patched, Some(operation_index))?;
            cumulative_bytes = cumulative_bytes
                .checked_add(result_bytes)
                .ok_or_else(object_data_patch_work_limit_error)?;
            if cumulative_bytes > MAX_OBJECT_DATA_PATCH_WORK_BYTES {
                return Err(object_data_patch_work_limit_error());
            }
        }
        Ok(patched)
    }
}

fn validate_object_data_patch_result(
    data: &serde_json::Value,
    operation_index: Option<usize>,
) -> Result<usize, ApiError> {
    let mut pending = vec![(data, 0_usize)];
    while let Some((value, depth)) = pending.pop() {
        match value {
            serde_json::Value::Array(values) => {
                if depth >= MAX_OBJECT_DATA_PATCH_RESULT_NESTING_DEPTH {
                    return Err(ApiError::PayloadTooLarge(format!(
                        "JSON Patch result after {} exceeds the maximum nesting depth of {MAX_OBJECT_DATA_PATCH_RESULT_NESTING_DEPTH}",
                        patch_result_stage(operation_index)
                    )));
                }
                pending.extend(values.iter().map(|value| (value, depth + 1)));
            }
            serde_json::Value::Object(values) => {
                if depth >= MAX_OBJECT_DATA_PATCH_RESULT_NESTING_DEPTH {
                    return Err(ApiError::PayloadTooLarge(format!(
                        "JSON Patch result after {} exceeds the maximum nesting depth of {MAX_OBJECT_DATA_PATCH_RESULT_NESTING_DEPTH}",
                        patch_result_stage(operation_index)
                    )));
                }
                pending.extend(values.values().map(|value| (value, depth + 1)));
            }
            _ => {}
        }
    }

    let serialized_bytes = serde_json::to_vec(data)?.len();
    if serialized_bytes > MAX_OBJECT_DATA_PATCH_BYTES {
        return Err(ApiError::PayloadTooLarge(format!(
            "JSON Patch result after {} is {serialized_bytes} bytes; the limit is {MAX_OBJECT_DATA_PATCH_BYTES} bytes",
            patch_result_stage(operation_index)
        )));
    }
    Ok(serialized_bytes)
}

fn patch_result_stage(operation_index: Option<usize>) -> String {
    operation_index.map_or_else(
        || "loading the current object data".to_string(),
        |index| format!("operation {index}"),
    )
}

fn object_data_patch_work_limit_error() -> ApiError {
    ApiError::PayloadTooLarge(format!(
        "JSON Patch exceeds the cumulative application-work limit of {MAX_OBJECT_DATA_PATCH_WORK_BYTES} bytes"
    ))
}

fn validate_patch_pointer_depth(
    operation_index: usize,
    pointer_name: &str,
    depth: usize,
) -> Result<(), String> {
    if depth > MAX_OBJECT_DATA_PATCH_POINTER_DEPTH {
        return Err(format!(
            "JSON Patch operation at index {operation_index} has a `{pointer_name}` pointer depth of {depth}; at most {MAX_OBJECT_DATA_PATCH_POINTER_DEPTH} segments are allowed"
        ));
    }
    Ok(())
}

impl<'de> Deserialize<'de> for ObjectDataPatchDocument {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let patch = json_patch::Patch::deserialize(deserializer)?;
        Self::validate(patch).map_err(de::Error::custom)
    }
}

impl Serialize for ObjectDataPatchDocument {
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

    fn patch_document(value: serde_json::Value) -> ObjectDataPatchDocument {
        serde_json::from_value(value).expect("valid JSON Patch document")
    }

    #[rstest::rstest]
    #[case::add(
        serde_json::json!({"source": {"value": 1}}),
        serde_json::json!([{"op": "add", "path": "/added", "value": true}]),
        serde_json::json!({"source": {"value": 1}, "added": true})
    )]
    #[case::remove(
        serde_json::json!({"remove": 1, "keep": 2}),
        serde_json::json!([{"op": "remove", "path": "/remove"}]),
        serde_json::json!({"keep": 2})
    )]
    #[case::replace(
        serde_json::json!({"value": "before"}),
        serde_json::json!([{"op": "replace", "path": "/value", "value": "after"}]),
        serde_json::json!({"value": "after"})
    )]
    #[case::move_value(
        serde_json::json!({"source": {"value": 1}}),
        serde_json::json!([{"op": "move", "from": "/source", "path": "/moved"}]),
        serde_json::json!({"moved": {"value": 1}})
    )]
    #[case::copy(
        serde_json::json!({"source": {"value": 1}}),
        serde_json::json!([{"op": "copy", "from": "/source", "path": "/copied"}]),
        serde_json::json!({"source": {"value": 1}, "copied": {"value": 1}})
    )]
    #[case::test_success(
        serde_json::json!({"value": 1}),
        serde_json::json!([{"op": "test", "path": "/value", "value": 1}]),
        serde_json::json!({"value": 1})
    )]
    fn object_data_patch_supports_each_rfc_6902_operation(
        #[case] original: serde_json::Value,
        #[case] patch: serde_json::Value,
        #[case] expected: serde_json::Value,
    ) {
        let patched = patch_document(patch).apply(&original).unwrap();

        assert_eq!(patched, expected);
    }

    #[rstest::rstest]
    #[case::add_missing_parent(
        serde_json::json!([{"op": "add", "path": "/missing/child", "value": 1}])
    )]
    #[case::remove_missing_member(
        serde_json::json!([{"op": "remove", "path": "/missing"}])
    )]
    #[case::replace_missing_member(
        serde_json::json!([{"op": "replace", "path": "/missing", "value": 1}])
    )]
    #[case::move_missing_source(
        serde_json::json!([{"op": "move", "from": "/missing", "path": "/moved"}])
    )]
    #[case::copy_missing_source(
        serde_json::json!([{"op": "copy", "from": "/missing", "path": "/copied"}])
    )]
    #[case::test_mismatch(
        serde_json::json!([{"op": "test", "path": "/value", "value": 2}])
    )]
    fn object_data_patch_reports_each_failed_rfc_6902_operation(#[case] patch: serde_json::Value) {
        let error = patch_document(patch)
            .apply(&serde_json::json!({"value": 1}))
            .unwrap_err();

        assert!(matches!(error, ApiError::Conflict(_)));
    }

    #[test]
    fn object_data_patch_add_replaces_a_complete_existing_member_without_merging() {
        let original = serde_json::json!({
            "facts": {"source": "old", "hostname": "srv-01"},
            "keep": true
        });
        let patch = patch_document(serde_json::json!([
            {"op": "add", "path": "/facts", "value": {"source": "new"}}
        ]));

        assert_eq!(
            patch.apply(&original).unwrap(),
            serde_json::json!({"facts": {"source": "new"}, "keep": true})
        );
    }

    #[test]
    fn object_data_patch_empty_path_replaces_the_complete_document() {
        let patch = patch_document(serde_json::json!([
            {"op": "replace", "path": "", "value": ["complete", "replacement"]}
        ]));

        assert_eq!(
            patch.apply(&serde_json::json!({"old": true})).unwrap(),
            serde_json::json!(["complete", "replacement"])
        );
    }

    #[test]
    fn object_data_patch_decodes_json_pointer_escaping() {
        let patch = patch_document(serde_json::json!([
            {"op": "replace", "path": "/a~1b/~0key", "value": "after"}
        ]));

        assert_eq!(
            patch
                .apply(&serde_json::json!({"a/b": {"~key": "before"}}))
                .unwrap(),
            serde_json::json!({"a/b": {"~key": "after"}})
        );
    }

    #[rstest::rstest]
    #[case::insert(
        serde_json::json!([{"op": "add", "path": "/items/1", "value": "inserted"}]),
        serde_json::json!({"items": ["first", "inserted", "second"]})
    )]
    #[case::append(
        serde_json::json!([{"op": "add", "path": "/items/-", "value": "last"}]),
        serde_json::json!({"items": ["first", "second", "last"]})
    )]
    fn object_data_patch_preserves_array_add_behavior(
        #[case] patch: serde_json::Value,
        #[case] expected: serde_json::Value,
    ) {
        let original = serde_json::json!({"items": ["first", "second"]});

        assert_eq!(patch_document(patch).apply(&original).unwrap(), expected);
    }

    #[rstest::rstest]
    #[case::leading_zero("/items/01")]
    #[case::past_end("/items/3")]
    #[case::non_numeric("/items/nope")]
    fn object_data_patch_rejects_invalid_array_indices(#[case] path: &str) {
        let patch = patch_document(serde_json::json!([
            {"op": "add", "path": path, "value": "invalid"}
        ]));

        assert!(matches!(
            patch.apply(&serde_json::json!({"items": [1]})),
            Err(ApiError::Conflict(_))
        ));
    }

    #[test]
    fn object_data_patch_failure_restores_prior_operations() {
        let original = serde_json::json!({"value": "before"});
        let patch = patch_document(serde_json::json!([
            {"op": "replace", "path": "/value", "value": "intermediate"},
            {"op": "remove", "path": "/missing"}
        ]));

        assert!(patch.apply(&original).is_err());
        assert_eq!(original, serde_json::json!({"value": "before"}));
    }

    #[test]
    fn object_data_patch_rejects_excessive_operation_count() {
        let operations = (0..=MAX_OBJECT_DATA_PATCH_OPERATIONS)
            .map(|_| serde_json::json!({"op": "test", "path": "", "value": {}}))
            .collect::<Vec<_>>();

        let error =
            serde_json::from_value::<ObjectDataPatchDocument>(serde_json::Value::Array(operations))
                .unwrap_err();

        assert!(error.to_string().contains("at most 1000"));
    }

    #[test]
    fn object_data_patch_rejects_excessive_pointer_depth() {
        let path = format!("/{}", vec!["segment"; 129].join("/"));

        let error = serde_json::from_value::<ObjectDataPatchDocument>(serde_json::json!([
            {"op": "remove", "path": path}
        ]))
        .unwrap_err();

        assert!(error.to_string().contains("at most 128 segments"));
    }

    #[test]
    fn object_data_patch_rejects_a_result_larger_than_the_object_data_limit() {
        let blob = "x".repeat(MAX_OBJECT_DATA_PATCH_BYTES / 2 + 1);
        let original = serde_json::json!({"blob": blob});
        let patch = patch_document(serde_json::json!([
            {"op": "copy", "from": "/blob", "path": "/copy"}
        ]));

        let error = patch.apply(&original).unwrap_err();

        assert!(matches!(error, ApiError::PayloadTooLarge(_)));
    }

    #[test]
    fn object_data_patch_rejects_a_result_with_excessive_nesting() {
        let nested = (0..=MAX_OBJECT_DATA_PATCH_RESULT_NESTING_DEPTH)
            .fold(serde_json::Value::Null, |value, _| {
                serde_json::Value::Array(vec![value])
            });
        let patch = patch_document(serde_json::json!([
            {"op": "add", "path": "/nested", "value": nested}
        ]));

        let error = patch.apply(&serde_json::json!({})).unwrap_err();

        assert!(matches!(error, ApiError::PayloadTooLarge(_)));
    }

    #[test]
    fn object_data_patch_bounds_cumulative_application_work() {
        let original = serde_json::json!({
            "padding": "x".repeat(40 * 1024),
            "value": true
        });
        let operations = (0..MAX_OBJECT_DATA_PATCH_OPERATIONS)
            .map(|_| serde_json::json!({"op": "test", "path": "/value", "value": true}))
            .collect::<Vec<_>>();
        let patch = patch_document(serde_json::Value::Array(operations));

        let error = patch.apply(&original).unwrap_err();

        assert!(matches!(error, ApiError::PayloadTooLarge(_)));
    }
}
