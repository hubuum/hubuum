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

#[rstest]
#[case::relative_resource("child")]
#[case::absolute_resource("https://example.org/child")]
fn referenced_resource_metadata_is_not_read_from_the_root(#[case] reference: &str) {
    let schema = json!({
        "$id": "https://example.org/root",
        "$defs": {"child": {"$id": "child", "type": "integer"}},
        "type": "object",
        "properties": {"value": {"$ref": reference}}
    });
    let result = inspect(schema, json!({"value": "private-value"}));
    let issue = &result["issues"][0];
    assert_eq!(issue["instance_path"], "/value");
    assert_eq!(issue["reason"]["schema_path"], Value::Null);
    assert_eq!(issue["expected"], json!({"status": "omitted"}));
    assert!(
        issue["omissions"]
            .as_array()
            .unwrap()
            .contains(&json!("schema_constraint_unavailable_or_too_large"))
    );
}

#[test]
fn referenced_required_property_is_not_attributed_to_the_root() {
    let schema = json!({
        "$id": "https://example.org/root",
        "$defs": {"child": {"$id": "child", "required": ["child-required"]}},
        "required": ["child-required"],
        "properties": {"value": {"$ref": "child"}}
    });
    let value = json!({"child-required": true, "value": {}});
    let validator = jsonschema::validator_for(&schema).unwrap();
    let failure =
        SchemaFailure::from_error(&schema, &validator.iter_errors(&value).next().unwrap());
    assert_eq!(
        serde_json::to_value(failure).unwrap(),
        json!({
            "keyword": "required", "schema_path": null, "missing_property": null
        })
    );
}

#[test]
fn property_name_wrapper_preserves_the_child_resource_boundary() {
    let result = inspect(
        json!({
            "$id": "https://example.org/root",
            "$defs": {"child": {"$id": "child", "maxLength": 2}},
            "maxLength": 99,
            "propertyNames": {"$ref": "child"}
        }),
        json!({"private-key": 1}),
    );
    let issue = &result["issues"][0];
    assert_eq!(issue["reason"]["keyword"], "propertyNames");
    assert_eq!(issue["reason"]["schema_path"], Value::Null);
    assert_eq!(issue["expected"], json!({"status": "omitted"}));
}

#[test]
fn external_reference_in_an_alternative_omits_foreign_metadata() {
    let registry = jsonschema::Registry::new()
        .add("urn:child", json!({"type": "integer"}))
        .unwrap()
        .prepare()
        .unwrap();
    let schema = json!({
        "type": "object",
        "properties": {"value": {"anyOf": [{"$ref": "urn:child"}, {"type": "boolean"}]}}
    });
    let value = json!({"value": "private-value"});
    let validator = jsonschema::options()
        .with_registry(&registry)
        .build(&schema)
        .unwrap();
    let SchemaDiagnosticInspection::Invalid(diagnostics) =
        SchemaDiagnosticInspection::from_errors(&schema, &value, validator.iter_errors(&value))
    else {
        panic!("expected diagnostics");
    };
    let issue = &diagnostics.issues()[1];
    assert!(issue.alternative());
    assert_eq!(issue.reason().keyword(), "type");
    assert_eq!(issue.reason().schema_path(), None);
    assert!(matches!(issue.expected(), SchemaExpectedValue::Omitted));
}

#[rstest]
#[case::root(json!({"propertyNames": {"maxLength": 2}}), json!({"secret-key": 1}), "")]
#[case::nested(json!({"properties": {"payload": {"propertyNames": {"maxLength": 2}}}}), json!({"payload": {"secret-key": 1}}), "/payload")]
#[case::alternatives(json!({"propertyNames": {"anyOf": [{"maxLength": 2}, {"pattern": "^a"}]}}), json!({"secret-key": 1}), "")]
fn property_name_repairs_keep_the_containing_object_context(
    #[case] schema: Value,
    #[case] value: Value,
    #[case] path: &str,
) {
    let result = inspect(schema, value);
    let decoded: SchemaDiagnostics = serde_json::from_value(result).unwrap();
    let [issue] = decoded.issues() else {
        panic!("a property-name failure must remain one qualified issue");
    };
    assert_eq!(issue.reason().keyword(), "propertyNames");
    assert_eq!(issue.instance_path(), Some(path));
    assert!(matches!(
        issue.actual(),
        SchemaActualValue::Object { properties: 1 }
    ));
    assert!(issue.message().contains("rename the property"));
    assert!(
        !serde_json::to_string(&decoded)
            .unwrap()
            .contains("secret-key")
    );
}
