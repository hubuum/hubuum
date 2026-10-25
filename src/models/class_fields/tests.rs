use super::*;
use rstest::rstest;
use serde_json::json;

fn inventory(data: Value, depth: usize, containers: bool) -> Value {
    let mut builder =
        FieldInventoryBuilder::new(FieldDiscoveryOptions::new(100, depth, containers).unwrap());
    builder.observe_data(0, &data);
    serde_json::to_value(builder.finish(1).unwrap()).unwrap()
}

#[rstest]
#[case(json!({"items":[{"v":null},{"v":7},{"v":7}]}), "data.items[*].v", 3, json!({"null":1,"number":2}), "null")]
#[case(json!({"value":true}), "data.value", 1, json!({"bool":1}), "true")]
#[case(json!({"value":"hello"}), "data.value", 1, json!({"string":1}), "hello")]
fn cli_leaf_observations(
    #[case] data: Value,
    #[case] field: &str,
    #[case] count: usize,
    #[case] types: Value,
    #[case] example: &str,
) {
    let result = inventory(data, 6, false);
    assert_eq!(
        result["fields"],
        json!([{
            "field":field,"source":"data","path":if field.contains("items") {json!([{"kind":"property","name":"items"},{"kind":"array_item"},{"kind":"property","name":"v"}])} else {json!([{"kind":"property","name":"value"}])},
            "observation_count":count,"object_count":1,"types":types,"example":example,"example_truncated":false,"error_count":0
        }])
    );
}

#[test]
fn distinct_object_counts_differ_from_array_occurrences() {
    let mut builder = FieldInventoryBuilder::new(FieldDiscoveryOptions::new(10, 6, false).unwrap());
    for (index, data) in [json!({"x":[1,2]}), json!({}), json!({"x":[null]})]
        .iter()
        .enumerate()
    {
        builder.observe_data(index, data);
    }
    let result = builder.finish(3).unwrap();
    let field = &result.fields[0];
    assert_eq!((field.observation_count, field.object_count), (3, 2));
}

#[rstest]
#[case(0, false, json!([]))]
#[case(1, false, json!([]))]
#[case(1, true, json!(["data.x"]))]
#[case(2, false, json!([]))]
#[case(2, true, json!(["data.x", "data.x.y"]))]
#[case(3, false, json!(["data.x.y[*]"]))]
fn cli_depth_and_container_rules(
    #[case] depth: usize,
    #[case] containers: bool,
    #[case] expected: Value,
) {
    let result = inventory(json!({"x":{"y":[1]}}), depth, containers);
    let names = result["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|field| field["field"].clone())
        .collect::<Vec<_>>();
    assert_eq!(json!(names), expected);
    assert_eq!(
        result["truncation"],
        if depth < 3 {
            json!(["depth"])
        } else {
            json!([])
        }
    );
}

#[test]
fn literal_property_names_do_not_merge_with_nested_or_array_paths() {
    let result = inventory(
        json!({"a.b": 1, "a":{"b":2}, "items[*]":3, "items":[4]}),
        6,
        false,
    );
    let paths = result["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|field| field["path"].to_string())
        .collect::<BTreeSet<_>>();
    assert_eq!(paths.len(), 4);
}

#[rstest]
#[case(json!({}), "", false)]
#[case(json!({"a":1,"b":[true]}), "a=1, b=[true]", false)]
#[case(json!("ø".repeat(80)), &format!("{}...", "ø".repeat(69)), true)]
#[case(json!(["x".repeat(1000)]), &format!("[\"{}...", "x".repeat(67)), true)]
fn bounded_preview_matches_cli(
    #[case] value: Value,
    #[case] expected: &str,
    #[case] truncated: bool,
) {
    assert_eq!(preview(&value), (expected.to_string(), truncated));
}

#[test]
fn computed_null_errors_count_but_unobserved_definitions_remain_empty() {
    let mut builder = FieldInventoryBuilder::new(FieldDiscoveryOptions::new(10, 6, false).unwrap());
    builder.add_definition("v", false);
    builder.add_definition("v", true);
    builder.observe_computed(0, "v", false, &Value::Null, true);
    let result = serde_json::to_value(builder.finish(1).unwrap()).unwrap();
    assert_eq!(result["fields"][0]["error_count"], 1);
    assert_eq!(result["fields"][0]["types"], json!({"null":1}));
    assert_eq!(result["fields"][1]["observation_count"], 0);
    assert_eq!(result["fields"][1]["example"], Value::Null);
}

#[test]
fn wide_objects_stop_at_field_limit() {
    let data = Value::Object(
        (0..MAX_FIELDS + 10)
            .map(|i| (format!("key{i:04}"), json!(i)))
            .collect(),
    );
    let result = inventory(data, 6, false);
    assert_eq!(result["fields"].as_array().unwrap().len(), MAX_FIELDS);
    assert_eq!(result["truncation"], json!(["field_limit"]));
}

#[test]
fn repeated_array_values_stop_at_work_limit() {
    let result = inventory(json!({"items":vec![0; MAX_WORK+1]}), 6, false);
    assert_eq!(result["fields"][0]["observation_count"], MAX_WORK - 2);
    assert_eq!(result["truncation"], json!(["work_limit"]));
}

#[test]
fn oversized_paths_are_explicitly_omitted() {
    let result = inventory(json!({"x".repeat(MAX_PATH_BYTES + 1):true}), 6, false);
    assert_eq!(result["fields"], json!([]));
    assert_eq!(result["truncation"], json!(["path_limit"]));
}

#[test]
fn escaped_paths_cannot_exceed_response_budget() {
    let data = Value::Object(
        (0..MAX_FIELDS)
            .map(|i| (format!("{}{:04}", "\n".repeat(500), i), json!(true)))
            .collect(),
    );
    let result = inventory(data, 6, false);
    assert_eq!(result["truncation"], json!(["response_limit"]));
    assert!(serde_json::to_vec(&result).unwrap().len() <= MAX_RESPONSE_BYTES);
}

#[rstest]
#[case(0, 6)]
#[case(251, 6)]
#[case(10, 33)]
fn invalid_options_fail_at_boundary(#[case] limit: usize, #[case] depth: usize) {
    assert!(FieldDiscoveryOptions::new(limit, depth, false).is_err());
}
