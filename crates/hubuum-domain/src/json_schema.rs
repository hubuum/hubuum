//! JSON Schema invariants shared by application workflows and storage adapters.

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock, RwLock};

use lru::LruCache;
use serde_json::Value;
use sha2::{Digest, Sha256};

const JSON_SCHEMA_CACHE_MAX_ENTRIES: usize = 128;

type SchemaDigest = [u8; 32];

static JSON_SCHEMA_CACHE: OnceLock<RwLock<LruCache<SchemaDigest, Arc<jsonschema::Validator>>>> =
    OnceLock::new();

/// Stable classification of a JSON Schema failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonSchemaErrorKind {
    /// The schema document or its reference policy is invalid.
    InvalidSchema,
    /// The value does not satisfy a valid schema.
    InvalidValue,
}

/// Backend-neutral JSON Schema validation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonSchemaError {
    kind: JsonSchemaErrorKind,
    message: String,
}

impl JsonSchemaError {
    fn invalid_schema(message: impl Into<String>) -> Self {
        Self {
            kind: JsonSchemaErrorKind::InvalidSchema,
            message: message.into(),
        }
    }

    fn invalid_value(message: impl Into<String>) -> Self {
        Self {
            kind: JsonSchemaErrorKind::InvalidValue,
            message: message.into(),
        }
    }

    /// Return the stable failure classification.
    #[must_use]
    pub const fn kind(&self) -> JsonSchemaErrorKind {
        self.kind
    }

    /// Consume the error and return its classification and display message.
    #[must_use]
    pub fn into_parts(self) -> (JsonSchemaErrorKind, String) {
        (self.kind, self.message)
    }
}

impl fmt::Display for JsonSchemaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for JsonSchemaError {}

fn schema_cache() -> &'static RwLock<LruCache<SchemaDigest, Arc<jsonschema::Validator>>> {
    JSON_SCHEMA_CACHE.get_or_init(|| {
        let capacity = NonZeroUsize::new(JSON_SCHEMA_CACHE_MAX_ENTRIES)
            .expect("JSON_SCHEMA_CACHE_MAX_ENTRIES must be non-zero");
        RwLock::new(LruCache::new(capacity))
    })
}

fn schema_digest(schema: &Value) -> Result<SchemaDigest, JsonSchemaError> {
    let encoded = serde_json::to_vec(schema).map_err(|error| {
        JsonSchemaError::invalid_schema(format!("JSON schema could not be encoded: {error}"))
    })?;
    Ok(Sha256::digest(encoded).into())
}

fn validate_reference_policy(value: &Value) -> Result<(), JsonSchemaError> {
    match value {
        Value::Array(values) => {
            for value in values {
                validate_reference_policy(value)?;
            }
        }
        Value::Object(object) => {
            for (key, value) in object {
                if matches!(key.as_str(), "$ref" | "$dynamicRef" | "$recursiveRef") {
                    let reference = value.as_str().ok_or_else(|| {
                        JsonSchemaError::invalid_schema(format!(
                            "JSON schema {key} must be a string"
                        ))
                    })?;
                    if !reference.starts_with('#') {
                        return Err(JsonSchemaError::invalid_schema(format!(
                            "JSON schema {key} must be a local fragment reference"
                        )));
                    }
                }
                validate_reference_policy(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn compile_json_schema(schema: &Value) -> Result<Arc<jsonschema::Validator>, JsonSchemaError> {
    validate_reference_policy(schema)?;
    let digest = schema_digest(schema)?;

    if let Some(validator) = schema_cache()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&digest)
        .cloned()
    {
        return Ok(validator);
    }

    let validator = Arc::new(jsonschema::options().build(schema).map_err(|error| {
        JsonSchemaError::invalid_schema(format!("Invalid JSON schema: {error}"))
    })?);
    schema_cache()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .put(digest, validator.clone());
    Ok(validator)
}

/// Validate that a document is structurally valid JSON Schema.
pub fn validate_json_schema(schema: &Value) -> Result<(), JsonSchemaError> {
    jsonschema::meta::validate(schema)
        .map_err(|error| JsonSchemaError::invalid_schema(format!("Invalid JSON schema: {error}")))
}

/// Validate that a schema can safely be used for instance validation.
///
/// Hubuum intentionally permits only local fragment references so validating
/// data cannot perform network or filesystem resolution.
pub fn validate_json_schema_for_instances(schema: &Value) -> Result<(), JsonSchemaError> {
    compile_json_schema(schema).map(|_| ())
}

/// Validate one JSON value against a safe, compiled schema.
pub fn validate_json_value(schema: &Value, value: &Value) -> Result<(), JsonSchemaError> {
    compile_json_schema(schema)?
        .validate(value)
        .map_err(|error| JsonSchemaError::invalid_value(error.to_string()))
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
    fn schema_documents_are_validated_without_reference_resolution(
        #[case] schema: Value,
        #[case] expected_valid: bool,
    ) {
        assert_eq!(validate_json_schema(&schema).is_ok(), expected_valid);
    }

    #[rstest]
    #[case(json!({"$ref": "https://example.com/schema.json"}))]
    #[case(json!({"$ref": "file:///etc/passwd"}))]
    fn external_references_cannot_be_compiled_for_validation(#[case] schema: Value) {
        let error = validate_json_schema_for_instances(&schema).unwrap_err();
        assert_eq!(error.kind(), JsonSchemaErrorKind::InvalidSchema);
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
