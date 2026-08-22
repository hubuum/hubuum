use hubuum_domain::{JsonSchemaError, JsonSchemaErrorKind};
use serde_json::Value;

use crate::errors::ApiError;

fn map_schema_error(error: JsonSchemaError) -> ApiError {
    let (kind, message) = error.into_parts();
    match kind {
        JsonSchemaErrorKind::InvalidSchema => ApiError::BadRequest(message),
        JsonSchemaErrorKind::InvalidValue => ApiError::ValidationError(message),
    }
}

pub fn compile_json_schema(schema: &Value) -> Result<(), ApiError> {
    hubuum_domain::validate_json_schema_for_instances(schema).map_err(map_schema_error)
}

pub fn validate_json_schema(schema: &Value) -> Result<(), ApiError> {
    hubuum_domain::validate_json_schema(schema).map_err(map_schema_error)
}

pub fn validate_json_value(schema: &Value, value: &Value) -> Result<(), ApiError> {
    hubuum_domain::validate_json_value(schema, value).map_err(map_schema_error)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;

    use super::*;

    #[rstest]
    #[case(json!({"type": "object"}), true)]
    #[case(json!({"type": 7}), false)]
    #[case(json!({"$ref": "https://example.com/schema.json"}), true)]
    #[case(json!({"$ref": "file:///etc/passwd"}), true)]
    #[case(json!({"$ref": "#/definitions/local"}), true)]
    fn schema_documents_are_validated_without_external_resolution(
        #[case] schema: Value,
        #[case] expected_valid: bool,
    ) {
        assert_eq!(validate_json_schema(&schema).is_ok(), expected_valid);
    }

    #[rstest]
    #[case(json!({"$ref": "https://example.com/schema.json"}))]
    #[case(json!({"$ref": "file:///etc/passwd"}))]
    fn external_references_cannot_be_compiled_for_validation(#[case] schema: Value) {
        assert!(compile_json_schema(&schema).is_err());
    }

    #[rstest]
    #[case(json!({"name": "hubuum"}), true)]
    #[case(json!({"name": 42}), false)]
    fn compiled_schemas_validate_instances(#[case] value: Value, #[case] expected_valid: bool) {
        let schema = json!({
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "required": ["name"]
        });
        assert_eq!(validate_json_value(&schema, &value).is_ok(), expected_valid);
    }
}
