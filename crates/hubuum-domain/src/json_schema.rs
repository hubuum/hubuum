//! JSON Schema invariants shared by application workflows and storage adapters.

mod budget;
mod limits;

pub use limits::{JsonSchemaLimits, JsonSchemaLimitsBuilder, JsonSchemaLimitsError};

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
type SchemaCacheKey = (SchemaDigest, JsonSchemaLimits);

pub(crate) struct BudgetedSchema {
    validator: jsonschema::Validator,
    budget: SchemaBudget,
}

impl BudgetedSchema {
    pub(crate) fn validate(&self, value: &Value) -> Result<(), JsonSchemaError> {
        self.budget.check_instance(value)?;
        self.validator
            .validate(value)
            .map_err(|error| JsonSchemaError::invalid_value(error.to_string()))
    }
}

static JSON_SCHEMA_CACHE: OnceLock<RwLock<LruCache<SchemaCacheKey, Arc<BudgetedSchema>>>> =
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

fn schema_cache() -> &'static RwLock<LruCache<SchemaCacheKey, Arc<BudgetedSchema>>> {
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

pub(crate) fn compile_json_schema(
    schema: &Value,
    limits: JsonSchemaLimits,
) -> Result<Arc<BudgetedSchema>, JsonSchemaError> {
    let budget = SchemaBudget::new(schema, limits)?;
    validate_reference_policy(schema)?;
    let key = (schema_digest(schema)?, limits);

    if let Some(validator) = schema_cache()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&key)
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
    let validator = Arc::new(BudgetedSchema { validator, budget });
    schema_cache()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .put(key, validator.clone());
    Ok(validator)
}

/// Validate that a document is structurally valid JSON Schema.
pub fn validate_json_schema(schema: &Value) -> Result<(), JsonSchemaError> {
    JsonSchemaLimits::default().validate_schema(schema)
}

/// Compile under the default budgets and local-reference policy.
pub fn validate_json_schema_for_instances(schema: &Value) -> Result<(), JsonSchemaError> {
    JsonSchemaLimits::default().validate_schema_for_instances(schema)
}

/// Validate one value under the default budgets, including cache hits.
pub fn validate_json_value(schema: &Value, value: &Value) -> Result<(), JsonSchemaError> {
    JsonSchemaLimits::default().validate_value(schema, value)
}

impl JsonSchemaLimits {
    /// Validate a schema document before meta-schema evaluation.
    pub fn validate_schema(self, schema: &Value) -> Result<(), JsonSchemaError> {
        validate_document_size(schema, self)?;
        jsonschema::meta::validate(schema).map_err(|error| {
            JsonSchemaError::invalid_schema(format!("Invalid JSON schema: {error}"))
        })
    }

    /// Compile with these budgets, retaining the fixed reference and regex guards.
    pub fn validate_schema_for_instances(self, schema: &Value) -> Result<(), JsonSchemaError> {
        compile_json_schema(schema, self).map(|_| ())
    }

    /// Validate using a cache entry bound to these exact deployment budgets.
    pub fn validate_value(self, schema: &Value, value: &Value) -> Result<(), JsonSchemaError> {
        compile_json_schema(schema, self)?.validate(value)
    }
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
        assert!(SchemaBudget::new(&schema, JsonSchemaLimits::default()).is_err());
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
        let value = json!((0..4000).map(|i| format!("item-{i}")).collect::<Vec<_>>());
        let error = validate_json_value(&schema, &value).unwrap_err();
        assert_eq!(error.kind(), JsonSchemaErrorKind::InvalidValue);
        assert!(error.to_string().contains("work budget"));
    }

    #[rstest]
    #[case(16 * 1024)]
    #[case(256 * 1024)]
    #[case(1024 * 1024)]
    fn original_batch_fixtures_fit_default_budgets(#[case] bytes: usize) {
        let schema = json!({
            "type": "object", "required": ["payload", "samples"],
            "properties": {"payload": {"type": "string", "minLength": 1},
                "samples": {"type": "array", "items": {"type": "integer"}}}
        });
        let value = json!({"payload": "x".repeat(bytes), "samples": (0..128).collect::<Vec<_>>()});
        validate_json_value(&schema, &value).unwrap();
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    fn cache_separates_deployment_budgets(#[case] permissive_first: bool) {
        let permissive = JsonSchemaLimits::default();
        let strict = JsonSchemaLimits::builder()
            .instance_work(1024)
            .build()
            .unwrap();
        let schema = json!({"type": "string", "minLength": 1});
        let value = json!("x".repeat(2048));
        let budgets = if permissive_first {
            [permissive, strict]
        } else {
            [strict, permissive]
        };
        for limits in budgets {
            assert_eq!(
                limits.validate_value(&schema, &value).is_ok(),
                limits == permissive
            );
        }
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

        let maximum = JsonSchemaLimits::builder()
            .schema_bytes(JsonSchemaLimits::MAX_SCHEMA_BYTES)
            .expanded_work(JsonSchemaLimits::MAX_EXPANDED_WORK)
            .instance_bytes(JsonSchemaLimits::MAX_INSTANCE_BYTES)
            .instance_work(JsonSchemaLimits::MAX_INSTANCE_WORK)
            .build()
            .unwrap();
        for limits in [JsonSchemaLimits::default(), maximum] {
            for combinator in ["allOf", "anyOf", "oneOf"] {
                for depth in [15, 20, 25] {
                    let schema = repeated_reference_schema(depth, combinator);
                    let started = Instant::now();
                    limits.validate_schema(&schema).unwrap();
                    let compile_error = limits.validate_schema_for_instances(&schema).unwrap_err();
                    assert_eq!(compile_error.kind(), JsonSchemaErrorKind::InvalidSchema);
                    let value_error = limits.validate_value(&schema, &json!(1)).unwrap_err();
                    assert_eq!(value_error.kind(), JsonSchemaErrorKind::InvalidSchema);
                    println!(
                        "SCHEMA_BUDGET_EVIDENCE {}",
                        json!({
                            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
                            "combinator": combinator, "depth": depth, "expanded_work_limit": limits.expanded_work(),
                            "schema_bytes": serde_json::to_vec(&schema).unwrap().len(),
                            "rejected": true, "elapsed_us": started.elapsed().as_micros(),
                        })
                    );
                }
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
