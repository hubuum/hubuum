use hubuum_schema_diagnostics::{SchemaFailure, evaluation_failed};
use serde_json::Value;

use super::BudgetedSchema;

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
        SchemaImpactInspection::Invalid(SchemaFailure::from_error(document, &error))
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
