//! JSON Schema invariants shared by application workflows and storage adapters.

mod budget;

use budget::{SchemaBudget, validate_document_size};
use jsonschema::PatternOptions;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock, RwLock};

use lru::LruCache;
use serde_json::Value;
use sha2::{Digest, Sha256};

const JSON_SCHEMA_CACHE_MAX_ENTRIES: usize = 128;

type SchemaDigest = [u8; 32];

struct CompiledSchema {
    validator: jsonschema::Validator,
    budget: SchemaBudget,
}

impl CompiledSchema {
    fn validate(&self, value: &Value) -> Result<(), JsonSchemaError> {
        self.budget.check_instance(value)?;
        self.validator
            .validate(value)
            .map_err(|error| JsonSchemaError::invalid_value(error.to_string()))
    }
}

static JSON_SCHEMA_CACHE: OnceLock<RwLock<LruCache<SchemaDigest, Arc<CompiledSchema>>>> =
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

fn schema_cache() -> &'static RwLock<LruCache<SchemaDigest, Arc<CompiledSchema>>> {
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

fn compile_json_schema(schema: &Value) -> Result<Arc<CompiledSchema>, JsonSchemaError> {
    let budget = SchemaBudget::new(schema)?;
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

    let validator = jsonschema::options()
        .with_pattern_options(
            PatternOptions::fancy_regex()
                .backtrack_limit(10_000)
                .size_limit(65_536)
                .dfa_size_limit(65_536),
        )
        .build(schema)
        .map_err(|error| {
            JsonSchemaError::invalid_schema(format!(
                "Invalid or over-budget JSON schema: {error}; simplify patterns or schema structure"
            ))
        })?;
    let validator = Arc::new(CompiledSchema { validator, budget });
    schema_cache()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .put(digest, validator.clone());
    Ok(validator)
}

/// Validate that a document is structurally valid JSON Schema.
pub fn validate_json_schema(schema: &Value) -> Result<(), JsonSchemaError> {
    validate_document_size(schema)?;
    jsonschema::meta::validate(schema)
        .map_err(|error| JsonSchemaError::invalid_schema(format!("Invalid JSON schema: {error}")))
}

/// Validate that a schema can safely be used for instance validation.
///
/// Hubuum permits acyclic local JSON Pointer references and applies conservative
/// compilation/evaluation budgets. Validation cannot resolve network or filesystem
/// resources; unsupported structures return actionable schema errors.
pub fn validate_json_schema_for_instances(schema: &Value) -> Result<(), JsonSchemaError> {
    compile_json_schema(schema).map(|_| ())
}

/// Validate one JSON value against a safe, compiled schema.
/// Instance size and work budgets also apply when the validator is cached.
pub fn validate_json_value(schema: &Value, value: &Value) -> Result<(), JsonSchemaError> {
    compile_json_schema(schema)?.validate(value)
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

    fn repeated_reference_schema(depth: usize, combinator: &str) -> Value {
        let mut definitions = serde_json::Map::new();
        definitions.insert("s0".into(), json!({"type": "integer"}));
        for level in 1..=depth {
            let reference = json!({"$ref": format!("#/$defs/s{}", level - 1)});
            definitions.insert(
                format!("s{level}"),
                json!({combinator: [reference.clone(), reference]}),
            );
        }
        json!({"$defs": definitions, "$ref": format!("#/$defs/s{depth}")})
    }

    #[rstest]
    #[case::valid_integer(json!(1), true)]
    #[case::invalid_string(json!("one"), false)]
    fn bounded_local_references_validate_instances(#[case] value: Value, #[case] valid: bool) {
        let schema = repeated_reference_schema(3, "allOf");
        assert_eq!(validate_json_value(&schema, &value).is_ok(), valid);
    }

    #[rstest]
    #[case::cycle(json!({"$defs": {"node": {"$ref": "#/$defs/node"}}, "$ref": "#/$defs/node"}))]
    #[case::root_recursion(json!({"properties": {"child": {"$ref": "#"}}}))]
    #[case::dynamic_reference(json!({"$dynamicRef": "#node"}))]
    #[case::nested_resource(json!({"$defs": {"node": {"$id": "nested", "type": "integer"}}}))]
    #[case::annotation_reentry(json!({"unevaluatedProperties": false}))]
    #[case::unbounded_pattern_compilation(json!({"pattern": "a{1000000000}"}))]
    #[case::numeric_exponent(serde_json::from_str(r#"{"multipleOf":1e-1000000}"#).unwrap())]
    fn unsupported_evaluation_structures_fail_before_use(#[case] schema: Value) {
        assert_eq!(
            validate_json_schema_for_instances(&schema)
                .unwrap_err()
                .kind(),
            JsonSchemaErrorKind::InvalidSchema
        );
    }

    #[test]
    fn reference_ancestors_cannot_change_the_resolution_base() {
        let schema = json!({
            "$defs": {"leaf": {"type": "integer"}},
            "custom": {
                "$id": "inner",
                "$defs": {"leaf": {"$ref": "#/$defs/leaf"}},
                "properties": {"value": {"$ref": "#/$defs/leaf"}}
            },
            "$ref": "#/custom/properties/value"
        });
        assert!(SchemaBudget::new(&schema).is_err());
    }

    #[rstest]
    #[case(json!(1), true)]
    #[case(json!("wrong"), false)]
    fn escaped_local_pointers_keep_their_semantics(#[case] value: Value, #[case] valid: bool) {
        let schema = json!({"$defs": {"a/b~c": {"type": "integer"}}, "$ref": "#/$defs/a~1b~0c"});
        assert_eq!(validate_json_value(&schema, &value).is_ok(), valid);
    }

    #[test]
    fn repeated_combinators_without_references_are_bounded() {
        let mut schema = json!({"type": "integer"});
        for _ in 0..20 {
            schema = json!({"anyOf": [schema, {"type": "string"}]});
        }
        let error = validate_json_schema_for_instances(&schema).unwrap_err();
        assert!(error.to_string().contains("expansion"));
    }

    #[test]
    fn instance_budget_is_checked_on_cached_validators() {
        let schema = json!({"type": "array", "uniqueItems": true, "items": {"type": "string"}});
        validate_json_value(&schema, &json!(["small"])).unwrap();
        let value = json!((0..1000).map(|i| format!("item-{i}")).collect::<Vec<_>>());
        let error = validate_json_value(&schema, &value).unwrap_err();
        assert_eq!(error.kind(), JsonSchemaErrorKind::InvalidValue);
        assert!(error.to_string().contains("work budget"));
    }

    #[test]
    fn oversized_schema_is_rejected_before_meta_validation() {
        let schema = json!({"description": "x".repeat(70_000)});
        let error = validate_json_schema(&schema).unwrap_err();
        assert_eq!(error.kind(), JsonSchemaErrorKind::InvalidSchema);
        assert!(error.to_string().contains("document limits"));
    }

    #[test]
    #[ignore = "run by scripts/check-json-schema-budget.py under process resource limits"]
    fn schema_budget_resource_probe() {
        use std::time::Instant;

        for combinator in ["allOf", "anyOf", "oneOf"] {
            for depth in [15, 20, 25] {
                let schema = repeated_reference_schema(depth, combinator);
                let started = Instant::now();
                validate_json_schema(&schema).unwrap();
                let compile_error = validate_json_schema_for_instances(&schema).unwrap_err();
                assert_eq!(compile_error.kind(), JsonSchemaErrorKind::InvalidSchema);
                let value_error = validate_json_value(&schema, &json!(1)).unwrap_err();
                assert_eq!(value_error.kind(), JsonSchemaErrorKind::InvalidSchema);
                println!(
                    "SCHEMA_BUDGET_EVIDENCE {}",
                    json!({
                        "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
                        "combinator": combinator, "depth": depth,
                        "schema_bytes": serde_json::to_vec(&schema).unwrap().len(),
                        "rejected": true, "elapsed_us": started.elapsed().as_micros(),
                    })
                );
            }
        }
        let schema = repeated_reference_schema(3, "allOf");
        let started = Instant::now();
        validate_json_schema_for_instances(&schema).unwrap();
        let compile_us = started.elapsed().as_micros();
        let started = Instant::now();
        for _ in 0..100 {
            validate_json_value(&schema, &json!(1)).unwrap();
            assert_eq!(
                validate_json_value(&schema, &json!("wrong"))
                    .unwrap_err()
                    .kind(),
                JsonSchemaErrorKind::InvalidValue
            );
        }
        println!(
            "SCHEMA_BUDGET_EVIDENCE {}",
            json!({
                "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
                "scenario": "supported_references", "compile_us": compile_us,
                "valid_and_invalid_instances": 200, "elapsed_us": started.elapsed().as_micros(),
            })
        );
    }
}
