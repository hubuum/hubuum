use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock, RwLock};

use lru::LruCache;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::errors::ApiError;

const JSON_SCHEMA_CACHE_MAX_ENTRIES: usize = 128;

type SchemaDigest = [u8; 32];

static JSON_SCHEMA_CACHE: OnceLock<RwLock<LruCache<SchemaDigest, Arc<jsonschema::Validator>>>> =
    OnceLock::new();

fn schema_cache() -> &'static RwLock<LruCache<SchemaDigest, Arc<jsonschema::Validator>>> {
    JSON_SCHEMA_CACHE.get_or_init(|| {
        let capacity = NonZeroUsize::new(JSON_SCHEMA_CACHE_MAX_ENTRIES)
            .expect("JSON_SCHEMA_CACHE_MAX_ENTRIES must be non-zero");
        RwLock::new(LruCache::new(capacity))
    })
}

fn schema_digest(schema: &Value) -> Result<SchemaDigest, ApiError> {
    let encoded = serde_json::to_vec(schema).map_err(|error| {
        ApiError::BadRequest(format!("JSON schema could not be encoded: {error}"))
    })?;
    Ok(Sha256::digest(encoded).into())
}

fn validate_reference_policy(value: &Value) -> Result<(), ApiError> {
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
                        ApiError::BadRequest(format!("JSON schema {key} must be a string"))
                    })?;
                    if !reference.starts_with('#') {
                        return Err(ApiError::BadRequest(format!(
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

pub fn compile_json_schema(schema: &Value) -> Result<Arc<jsonschema::Validator>, ApiError> {
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

    let validator = Arc::new(
        jsonschema::options()
            .build(schema)
            .map_err(|error| ApiError::BadRequest(format!("Invalid JSON schema: {error}")))?,
    );
    schema_cache()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .put(digest, validator.clone());
    Ok(validator)
}

pub fn validate_json_schema(schema: &Value) -> Result<(), ApiError> {
    jsonschema::meta::validate(schema)
        .map_err(|error| ApiError::BadRequest(format!("Invalid JSON schema: {error}")))
}

pub fn validate_json_value(schema: &Value, value: &Value) -> Result<(), ApiError> {
    compile_json_schema(schema)?
        .validate(value)
        .map_err(|error| ApiError::ValidationError(error.to_string()))
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
