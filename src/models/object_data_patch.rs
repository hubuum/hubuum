use serde::{Deserialize, Serialize};
use utoipa::{PartialSchema, ToSchema};

use crate::errors::ApiError;
use crate::models::json_patch::{
    BoundedJsonPatch, MAX_JSON_PATCH_BYTES, MAX_JSON_PATCH_OPERATIONS,
    MAX_JSON_PATCH_POINTER_DEPTH, MAX_JSON_PATCH_RESULT_NESTING_DEPTH, MAX_JSON_PATCH_WORK_BYTES,
};

/// Maximum number of operations accepted in one object-data JSON Patch document.
pub const MAX_OBJECT_DATA_PATCH_OPERATIONS: usize = MAX_JSON_PATCH_OPERATIONS;

/// Maximum number of reference tokens accepted in a JSON Pointer used by a patch operation.
pub const MAX_OBJECT_DATA_PATCH_POINTER_DEPTH: usize = MAX_JSON_PATCH_POINTER_DEPTH;

/// Maximum serialized size of a JSON Patch request or its resulting raw object data.
pub const MAX_OBJECT_DATA_PATCH_BYTES: usize = MAX_JSON_PATCH_BYTES;

/// Maximum cumulative serialized result bytes inspected while applying one JSON Patch document.
pub const MAX_OBJECT_DATA_PATCH_WORK_BYTES: usize = MAX_JSON_PATCH_WORK_BYTES;

/// Maximum number of nested JSON containers accepted in patched object data.
pub const MAX_OBJECT_DATA_PATCH_RESULT_NESTING_DEPTH: usize = MAX_JSON_PATCH_RESULT_NESTING_DEPTH;

/// An RFC 6902 patch document whose pointers are relative to an object's raw `data` value.
///
/// The private representation keeps the third-party patch implementation behind a small,
/// validating domain interface.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(transparent)]
pub struct ObjectDataPatchDocument(BoundedJsonPatch);

impl PartialSchema for ObjectDataPatchDocument {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        utoipa::openapi::schema::ArrayBuilder::new()
            .items(json_patch::PatchOperation::schema())
            .max_items(Some(MAX_OBJECT_DATA_PATCH_OPERATIONS))
            .description(Some(
                "RFC 6902 operations applied relative to the root of an object's raw data document. Supports add, remove, replace, move, copy, and test; test compares JSON numbers by numeric value. The resulting document is limited to 2 MiB and 64 nested containers, with a bounded cumulative application-work budget.",
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
    /// Apply the complete patch to `data`, returning a new value only if every operation succeeds.
    pub fn apply(&self, data: &serde_json::Value) -> Result<serde_json::Value, ApiError> {
        self.0.apply(data)
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

    #[test]
    fn object_data_patch_test_compares_numeric_representations_recursively() {
        let patch = serde_json::from_str::<ObjectDataPatchDocument>(
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
