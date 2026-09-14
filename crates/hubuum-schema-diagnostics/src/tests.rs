use super::*;

use rstest::rstest;
use serde_json::json;

fn inspect(schema: Value, data: Value) -> Value {
    let SchemaDiagnosticInspection::Invalid(diagnostics) = SchemaDiagnosticInspection::from_errors(
        &schema,
        &data,
        jsonschema::validator_for(&schema)
            .unwrap()
            .iter_errors(&data),
    ) else {
        panic!("expected diagnostics");
    };
    serde_json::to_value(diagnostics).unwrap()
}

#[test]
fn reports_multiple_repairs_with_precise_array_locations() {
    let result = inspect(
        json!({"required":["hostname"],"properties":{"interfaces":{"type":"array","items":{"properties":{"address":{"type":"string"}}}}}}),
        json!({"interfaces":[{},{},{},{"address":123}]}),
    );
    let issues = result["issues"].as_array().unwrap();
    assert_eq!(issues.len(), 2);
    assert!(
        issues
            .iter()
            .any(|issue| issue["reason"]["missing_property"] == "hostname"
                && issue["instance_path"] == "")
    );
    assert!(issues.iter().any(|issue| {
        issue["instance_path"] == "/interfaces/3/address"
            && issue["expected"]["value"] == "string"
            && issue["message"]
                .as_str()
                .unwrap()
                .contains("wrong JSON type")
    }));
}

#[rstest]
#[case::maximum(json!({"maximum":10}),json!(11),"/maximum",json!(10),"exceeds")]
#[case::escaped_key(json!({"properties":{"a/b~c":{"type":"string"}}}),json!({"a/b~c":false}),"/properties/a~1b~0c/type",json!("string"),"wrong JSON type")]
#[case::local_ref(json!({"$defs":{"number":{"maximum":10}},"properties":{"value":{"$ref":"#/$defs/number"}}}),json!({"value":11}),"/$defs/number/maximum",json!(10),"exceeds")]
fn explains_constraints(
    #[case] schema: Value,
    #[case] data: Value,
    #[case] schema_path: &str,
    #[case] expected: Value,
    #[case] text: &str,
) {
    let result = inspect(schema, data);
    let issue = &result["issues"][0];
    assert_eq!(issue["reason"]["schema_path"], schema_path);
    assert_eq!(issue["expected"]["value"], expected);
    assert!(issue["message"].as_str().unwrap().contains(text));
}

#[test]
fn sensitive_dynamic_keys_and_values_are_redacted_explicitly() {
    let result = inspect(
        json!({"additionalProperties":{"type":"integer"}}),
        json!({"secret-key":"secret-value"}),
    );
    assert!(!result.to_string().contains("secret-"));
    assert_eq!(result["issues"][0]["instance_path"], Value::Null);
    assert_eq!(
        result["issues"][0]["omissions"],
        json!([
            "actual_value_redacted",
            "instance_path_redacted_or_too_long"
        ])
    );
}

#[test]
fn diagnostics_do_not_claim_completeness_after_reaching_limit() {
    let result = inspect(json!({"items":{"type":"string"}}), json!(vec![0; 33]));
    assert_eq!(result["issues"].as_array().unwrap().len(), MAX_ISSUES);
    assert_eq!(result["truncated"], true);
}

#[test]
fn exact_limit_remains_complete() {
    let result = inspect(
        json!({"items":{"type":"string"}}),
        json!(vec![0; MAX_ISSUES]),
    );
    assert_eq!(result["truncated"], false);
}

#[test]
fn alternative_errors_are_qualified() {
    let result = inspect(
        json!({"anyOf":[{"type":"integer"},{"type":"boolean"}]}),
        json!("secret"),
    );
    assert_eq!(result["issues"].as_array().unwrap().len(), 3);
    assert_eq!(result["issues"][0]["alternative"], false);
    assert!(
        result["issues"].as_array().unwrap()[1..]
            .iter()
            .all(|issue| issue["alternative"] == true)
    );
}

#[test]
fn persisted_diagnostics_reject_empty_issue_lists() {
    assert!(
        serde_json::from_value::<SchemaDiagnostics>(json!({"issues":[],"truncated":false}))
            .is_err()
    );
}

#[test]
fn expected_null_survives_diagnostic_round_trip() {
    let result = inspect(json!({"const":null}), json!(false));
    let decoded: SchemaDiagnostics = serde_json::from_value(result).unwrap();
    assert!(matches!(
        decoded.issues()[0].expected(),
        SchemaExpectedValue::Available(Value::Null)
    ));
}

#[rstest]
#[case::missing_redaction(json!([]))]
#[case::false_path_omission(json!(["actual_value_redacted", "instance_path_redacted_or_too_long"]))]
#[case::duplicate_redaction(json!(["actual_value_redacted", "actual_value_redacted"]))]
fn persisted_omissions_must_describe_the_saved_issue(#[case] omissions: Value) {
    let mut result = inspect(json!({"type":"string"}), json!(42));
    result["issues"][0]["omissions"] = omissions;
    assert!(serde_json::from_value::<SchemaDiagnostics>(result).is_err());
}
