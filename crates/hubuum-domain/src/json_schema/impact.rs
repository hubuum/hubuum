use jsonschema::error::ValidationErrorKind;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::BudgetedSchema;

/// One failure, containing only a keyword and bounded schema-owned metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "FailureSnapshot")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SchemaFailure {
    keyword: String,
    schema_path: Option<String>,
    missing_property: Option<String>,
}

#[derive(Deserialize)]
struct FailureSnapshot {
    keyword: String,
    schema_path: Option<String>,
    missing_property: Option<String>,
}

impl TryFrom<FailureSnapshot> for SchemaFailure {
    type Error = &'static str;

    fn try_from(raw: FailureSnapshot) -> Result<Self, Self::Error> {
        if raw.keyword.is_empty()
            || raw.keyword.len() > 64
            || raw
                .schema_path
                .as_ref()
                .is_some_and(|path| path.len() > 512)
            || raw
                .missing_property
                .as_ref()
                .is_some_and(|name| name.len() > 128)
        {
            return Err("Schema failure exceeds diagnostic bounds");
        }
        Ok(Self {
            keyword: raw.keyword,
            schema_path: raw.schema_path,
            missing_property: raw.missing_property,
        })
    }
}

/// Budget rejection is inconclusive, not evidence of a schema mismatch.
#[derive(Clone, Debug)]
pub enum SchemaImpactInspection {
    Valid,
    Invalid(SchemaFailure),
    Uninspectable,
}

impl BudgetedSchema {
    pub(crate) fn inspect_impact(&self, document: &Value, value: &Value) -> SchemaImpactInspection {
        if self.budget.check_instance(value).is_err() {
            return SchemaImpactInspection::Uninspectable;
        }
        let Err(error) = self.validator.validate(value) else {
            return SchemaImpactInspection::Valid;
        };
        if evaluation_failed(error.kind()) {
            return SchemaImpactInspection::Uninspectable;
        }
        // Never copy instance paths, validator messages, unexpected keys, or values.
        let path = error.schema_path().as_str();
        let location = document.pointer(path);
        let missing_property = match (error.kind(), location) {
            (ValidationErrorKind::Required { property }, Some(Value::Array(required)))
                if required.contains(property) =>
            {
                property
                    .as_str()
                    .filter(|name| name.len() <= 128)
                    .map(str::to_owned)
            }
            _ => None,
        };
        SchemaImpactInspection::Invalid(SchemaFailure {
            keyword: error.kind().keyword().to_owned(),
            schema_path: (path.len() <= 512 && location.is_some()).then(|| path.to_owned()),
            missing_property,
        })
    }
}

fn evaluation_failed(kind: &ValidationErrorKind) -> bool {
    match kind {
        ValidationErrorKind::BacktrackLimitExceeded { .. }
        | ValidationErrorKind::RegexEngineFailure { .. }
        | ValidationErrorKind::Referencing(_) => true,
        ValidationErrorKind::AnyOf { context }
        | ValidationErrorKind::OneOfMultipleValid { context }
        | ValidationErrorKind::OneOfNotValid { context } => context
            .iter()
            .flatten()
            .any(|error| evaluation_failed(error.kind())),
        ValidationErrorKind::PropertyNames { error } => evaluation_failed(error.kind()),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CompiledSchema, JsonSchemaLimits};
    use rstest::rstest;
    use serde_json::json;

    #[rstest]
    #[case::extra_keys(json!({"additionalProperties":false}),json!({"private-key":"private-value"}),"falseSchema")]
    #[case::nested_value(json!({"additionalProperties":{"type":"integer"}}),json!({"private-key":"private-value"}),"type")]
    #[case::constraint(json!({"properties":{"name":{"minLength":100}}}),json!({"name":"private-value"}),"minLength")]
    #[case::required(json!({"required":["name"]}),json!({"private-key":"private-value"}),"required")]
    fn failures_do_not_copy_instance_values_or_paths(
        #[case] schema: Value,
        #[case] value: Value,
        #[case] keyword: &str,
    ) {
        let schema = CompiledSchema::try_new(schema).unwrap();
        let SchemaImpactInspection::Invalid(failure) = schema.inspect_impact(&value) else {
            panic!("expected mismatch");
        };
        let encoded = serde_json::to_value(failure).unwrap();
        assert_eq!(encoded["keyword"], keyword);
        assert!(!encoded.to_string().contains("private-"));
    }

    #[test]
    fn long_schema_locations_are_omitted_without_splitting_unicode() {
        let key = "é".repeat(300);
        let schema =
            CompiledSchema::try_new(json!({"properties":{key.clone():{"required":[key.clone()]}}}))
                .unwrap();
        let SchemaImpactInspection::Invalid(failure) = schema.inspect_impact(&json!({key:{}}))
        else {
            panic!("expected mismatch");
        };
        assert_eq!(
            serde_json::to_value(failure).unwrap(),
            json!({"keyword":"required","schema_path":null,"missing_property":null})
        );
    }

    #[test]
    fn budget_failure_is_not_a_constraint_failure() {
        let limits = JsonSchemaLimits::builder()
            .instance_work(1024)
            .build()
            .unwrap();
        let schema = CompiledSchema::try_new_with_limits(json!({"type":"string"}), limits).unwrap();
        assert!(matches!(
            schema.inspect_impact(&json!("x".repeat(2048))),
            SchemaImpactInspection::Uninspectable
        ));
    }
}
