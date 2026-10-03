//! Transport adapters for the backend-neutral bounded JSON Patch contract.

use serde_json::Value;
use utoipa::openapi::{
    Ref, RefOr,
    schema::{AllOfBuilder, ArrayBuilder, ObjectBuilder, OneOfBuilder, Schema, SchemaType, Type},
};

use crate::errors::ApiError;

pub(crate) use hubuum_domain::{
    BoundedJsonPatch, MAX_JSON_PATCH_BYTES, MAX_JSON_PATCH_OPERATIONS,
    MAX_JSON_PATCH_POINTER_DEPTH, MAX_JSON_PATCH_RESULT_NESTING_DEPTH, MAX_JSON_PATCH_WORK_BYTES,
};

pub(crate) fn apply_bounded_json_patch(
    patch: &BoundedJsonPatch,
    document: &Value,
) -> Result<Value, ApiError> {
    patch.apply(document).map_err(|error| {
        let (kind, message) = error.into_parts();
        match kind {
            hubuum_domain::JsonPatchErrorKind::BadRequest => ApiError::BadRequest(message),
            hubuum_domain::JsonPatchErrorKind::Conflict => ApiError::Conflict(message),
            hubuum_domain::JsonPatchErrorKind::PayloadTooLarge => {
                ApiError::PayloadTooLarge(message)
            }
        }
    })
}

pub(crate) fn bounded_json_patch_openapi_schema(
    description: &str,
    example: Value,
) -> RefOr<Schema> {
    ArrayBuilder::new()
        .items(patch_operation_schema())
        .max_items(Some(MAX_JSON_PATCH_OPERATIONS))
        .description(Some(description))
        .examples([example])
        .build()
        .into()
}

pub(crate) fn register_bounded_json_patch_openapi_schemas(
    schemas: &mut Vec<(String, RefOr<Schema>)>,
) {
    schemas.push(("PatchOperation".to_string(), patch_operation_schema()));
    for (operation, name) in PATCH_OPERATIONS {
        let mut schema = ObjectBuilder::new().description(Some(format!(
            "JSON Patch '{operation}' operation representation"
        )));
        if matches!(operation, "move" | "copy") {
            schema = schema
                .required("from")
                .property("from", ObjectBuilder::new().schema_type(Type::String).description(Some(
                    format!("JSON-Pointer value [RFC6901](https://tools.ietf.org/html/rfc6901) that references a location\nto {operation} value from."),
                )));
        }
        schema = schema
            .required("path")
            .property("path", ObjectBuilder::new().schema_type(Type::String).description(Some(
                "JSON-Pointer value [RFC6901](https://tools.ietf.org/html/rfc6901) that references a location\nwithin the target document where the operation is performed.",
            )));
        let value_description = match operation {
            "add" => Some("Value to add to the target location."),
            "replace" => Some("Value to replace with."),
            "test" => Some("Value to test against."),
            _ => None,
        };
        if let Some(description) = value_description {
            schema = schema.required("value").property(
                "value",
                ObjectBuilder::new()
                    .schema_type(SchemaType::AnyValue)
                    .description(Some(description)),
            );
        }
        schemas.push((name.to_string(), schema.into()));
    }
}

// json-patch's optional schema integration uses utoipa 5. Keep the RFC 6902
// transport schema here so application OpenAPI generation can use utoipa 6.
const PATCH_OPERATIONS: [(&str, &str); 6] = [
    ("add", "AddOperation"),
    ("remove", "RemoveOperation"),
    ("replace", "ReplaceOperation"),
    ("move", "MoveOperation"),
    ("copy", "CopyOperation"),
    ("test", "TestOperation"),
];

fn patch_operation_schema() -> RefOr<Schema> {
    let mut schema = OneOfBuilder::new().description(Some("JSON Patch single patch operation"));
    for (operation, name) in PATCH_OPERATIONS {
        let description = format!("'{operation}' operation");
        let mut reference = Ref::from_schema_name(name);
        reference.description = description.clone();
        schema = schema.item(
            AllOfBuilder::new()
                .description(Some(description))
                .item(reference)
                .item(
                    ObjectBuilder::new().required("op").property(
                        "op",
                        ObjectBuilder::new()
                            .schema_type(Type::String)
                            .enum_values(Some([operation])),
                    ),
                ),
        );
    }
    schema.into()
}

#[cfg(test)]
mod tests {
    use hubuum_domain::validate_json_value;
    use json_patch::PatchOperation;
    use rstest::rstest;
    use serde_json::{Map, from_value, json, to_value};

    use super::*;

    #[rstest]
    #[case::add(json!({"op": "add", "path": "/key", "value": null}), true)]
    #[case::remove(json!({"op": "remove", "path": "/key"}), true)]
    #[case::replace(json!({"op": "replace", "path": "/key", "value": {"nested": [1]}}), true)]
    #[case::move_value(json!({"op": "move", "from": "/source", "path": "/key"}), true)]
    #[case::copy(json!({"op": "copy", "from": "/source", "path": "/key"}), true)]
    #[case::test_value(json!({"op": "test", "path": "/key", "value": false}), true)]
    #[case::missing_value(json!({"op": "add", "path": "/key"}), false)]
    #[case::missing_from(json!({"op": "move", "path": "/key"}), false)]
    #[case::missing_path(json!({"op": "remove"}), false)]
    #[case::unknown_operation(json!({"op": "unknown", "path": "/key"}), false)]
    #[case::non_string_path(json!({"op": "remove", "path": 1}), false)]
    fn openapi_matches_patch_operation_wire_shape(#[case] value: Value, #[case] valid: bool) {
        let operation = from_value::<PatchOperation>(value.clone());
        assert_eq!(operation.is_ok(), valid);
        let serialized = operation.map_or(value, |operation| to_value(operation).unwrap());
        let mut schemas = Vec::new();
        register_bounded_json_patch_openapi_schemas(&mut schemas);
        let components: Map<String, Value> = schemas
            .into_iter()
            .map(|(name, schema)| (name, to_value(schema).unwrap()))
            .collect();
        let schema = json!({
            "$ref": "#/components/schemas/PatchOperation",
            "components": {"schemas": components},
        });

        assert_eq!(validate_json_value(&schema, &serialized).is_ok(), valid);
    }
}
