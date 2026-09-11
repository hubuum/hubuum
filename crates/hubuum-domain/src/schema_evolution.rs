use std::{fmt, sync::Arc};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::json_schema::{BudgetedSchema, JsonSchemaLimits, compile_json_schema};
use crate::{ClassId, JsonSchemaError, ResourceRevision, ResourceRevisionError};

/// Positive immutable schema identity, allocated monotonically within a class.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "i64", into = "i64")]
pub struct SchemaRevision(ResourceRevision);

impl SchemaRevision {
    pub const INITIAL: Self = Self(ResourceRevision::INITIAL);

    pub const fn new(value: i64) -> Result<Self, ResourceRevisionError> {
        match ResourceRevision::new(value) {
            Ok(value) => Ok(Self(value)),
            Err(error) => Err(error),
        }
    }

    #[must_use]
    pub const fn get(self) -> i64 {
        self.0.get()
    }

    pub const fn checked_advance(self) -> Result<Self, ResourceRevisionError> {
        match self.0.checked_advance() {
            Ok(value) => Ok(Self(value)),
            Err(error) => Err(error),
        }
    }
}

impl TryFrom<i64> for SchemaRevision {
    type Error = ResourceRevisionError;
    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<SchemaRevision> for i64 {
    fn from(value: SchemaRevision) -> Self {
        value.get()
    }
}

impl fmt::Display for SchemaRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[cfg(feature = "openapi")]
impl utoipa::PartialSchema for SchemaRevision {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        <ResourceRevision as utoipa::PartialSchema>::schema()
    }
}
#[cfg(feature = "openapi")]
impl utoipa::ToSchema for SchemaRevision {}

/// A dependency on one exact schema document; resource revisions are unrelated.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SchemaReference {
    class_id: ClassId,
    revision: SchemaRevision,
}

impl SchemaReference {
    #[must_use]
    pub const fn new(class_id: ClassId, revision: SchemaRevision) -> Self {
        Self { class_id, revision }
    }
    #[must_use]
    pub const fn class_id(self) -> ClassId {
        self.class_id
    }
    #[must_use]
    pub const fn revision(self) -> SchemaRevision {
        self.revision
    }
}

/// Value-redacted, byte-bounded diagnostic. Never uses a validator's Display output.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SchemaDiagnostic {
    category: String,
}

impl SchemaDiagnostic {
    #[must_use]
    pub fn category(&self) -> &str {
        &self.category
    }
}

/// Proof that a schema passed document validation, compilation and reference policy.
#[derive(Clone)]
pub struct CompiledSchema {
    document: Value,
    validator: Arc<BudgetedSchema>,
}

impl CompiledSchema {
    pub fn try_new(document: Value) -> Result<Self, JsonSchemaError> {
        Self::try_new_with_limits(document, JsonSchemaLimits::default())
    }

    pub fn try_new_with_limits(
        document: Value,
        limits: JsonSchemaLimits,
    ) -> Result<Self, JsonSchemaError> {
        limits.validate_schema(&document)?;
        let validator = compile_json_schema(&document, limits)?;
        Ok(Self {
            document,
            validator,
        })
    }

    #[must_use]
    pub const fn document(&self) -> &Value {
        &self.document
    }

    /// Return one fixed category without exposing paths or instance values.
    pub fn inspect(&self, value: &Value) -> Result<(), SchemaDiagnostic> {
        if self.validator.validate(value).is_ok() {
            Ok(())
        } else {
            Err(SchemaDiagnostic {
                category: "schema_mismatch".to_string(),
            })
        }
    }
}

impl fmt::Debug for CompiledSchema {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CompiledSchema { document: [redacted] }")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use serde_json::json;

    #[rstest]
    #[case(0)]
    #[case(-1)]
    fn schema_revision_rejects_nonpositive_wire_values(#[case] value: i64) {
        assert!(serde_json::from_value::<SchemaRevision>(json!(value)).is_err());
    }

    #[test]
    fn schema_revision_does_not_wrap() {
        assert!(
            SchemaRevision::new(i64::MAX)
                .unwrap()
                .checked_advance()
                .is_err()
        );
    }

    #[rstest]
    #[case(json!({"type": 7}))]
    #[case(json!({"$ref": "#"}))]
    #[case(json!({"$ref": "https://example.org/schema"}))]
    #[case(json!({"$ref": "file:///private/schema"}))]
    fn proof_rejects_unsafe_schema(#[case] schema: Value) {
        assert!(CompiledSchema::try_new(schema).is_err());
    }

    #[test]
    fn proven_schema_inspection_retains_instance_work_limits() {
        let limits = JsonSchemaLimits::builder()
            .instance_work(1024)
            .build()
            .unwrap();
        let schema = CompiledSchema::try_new_with_limits(json!({"type":"string"}), limits).unwrap();
        assert!(schema.inspect(&json!("small")).is_ok());
        let error = schema.inspect(&json!("x".repeat(1_048_576))).unwrap_err();
        assert_eq!(error.category(), "schema_mismatch");
    }

    #[test]
    fn inspection_never_exposes_instance_values() {
        let schema = CompiledSchema::try_new(json!({"type":"integer"})).unwrap();
        let error = schema.inspect(&json!("secret-value")).unwrap_err();
        assert_eq!(
            serde_json::to_value(error).unwrap(),
            json!({"category":"schema_mismatch"})
        );
    }
}
