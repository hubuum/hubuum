use super::*;
use chrono::Duration;
use hubuum_domain::HistoryRecordId;
use hubuum_storage_core::{StorageComputedFieldHistory, StorageHistoricalComputedField};
use rstest::rstest;
use serde_json::{Value, json};

fn object(data: Value) -> HubuumObjectHistory {
    let at = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .unwrap()
        .to_utc();
    HubuumObjectHistory {
        id: 1,
        name: "object".into(),
        collection_id: 1,
        hubuum_class_id: 2,
        data,
        description: String::new(),
        created_at: at.naive_utc(),
        updated_at: at.naive_utc(),
        op: "I".into(),
        valid_from: at,
        valid_to: None,
        actor_id: None,
        history_id: 3,
        actor_kind: Some("system".into()),
        initiator_user_id: None,
        task_id: None,
        revision: crate::models::ResourceRevision::INITIAL,
    }
}

fn field(id: i32, enabled: bool, semantics: i16) -> StorageHistoricalComputedField {
    serde_json::from_value(json!({"id": id, "class_id": 2, "revision": 4,
        "definition": {"key": format!("field_{id}"), "label": "Value", "description": "",
        "operation": {"type": "first_non_null", "paths": ["/value"]},
        "result_type": "integer", "enabled": enabled, "semantics_version": semantics}}))
    .unwrap()
}

fn run(data: Value, version: i16, fields: Vec<StorageHistoricalComputedField>) -> Value {
    let object = object(data);
    let query = StorageComputedFieldHistoryQuery::new(
        ClassId::new(2).unwrap(),
        object.valid_from + Duration::seconds(1),
    );
    let definitions = StorageComputedFieldHistory::builder(
        query.class_id(),
        HistoryRecordId::new(7).unwrap(),
        object.valid_from,
    )
    .evaluator_version(version)
    .definitions(fields)
    .build()
    .unwrap()
    .select(query)
    .unwrap();
    serde_json::to_value(recompute(&object, definitions).unwrap()).unwrap()
}

#[rstest]
#[case::wrong_type(json!({"value":"text"}), "result_type_mismatch")]
#[case::input_budget(json!({"value": 1, "oversized": "x".repeat(1_048_576)}), "input_too_large")]
fn historical_errors_never_invent_values(#[case] data: Value, #[case] code: &str) {
    let result = run(data, 1, vec![field(1, true, 1)]);
    assert_eq!(result["fields"][0]["error"]["code"], code);
    assert!(result["fields"][0].get("value").is_none());
}

#[rstest]
#[case::engine(2, 1, "unsupported_evaluator_version")]
#[case::definition(1, 2, "unsupported_definition_semantics")]
fn unsupported_semantics_do_not_run_current_evaluator(
    #[case] engine: i16,
    #[case] semantics: i16,
    #[case] reason: &str,
) {
    let result = run(
        json!({"value":7}),
        engine,
        vec![field(1, true, semantics), field(2, true, 1)],
    );
    for field in result["fields"].as_array().unwrap() {
        assert_eq!(field["status"], "unavailable");
        assert_eq!(field["reason"], reason);
        assert!(field.get("value").is_none());
    }
}

#[test]
fn disabled_historical_field_does_not_run_or_consume_the_scope_budget() {
    let result = run(
        json!({"value":7}),
        1,
        vec![field(1, false, 99), field(2, true, 1)],
    );
    assert_eq!(result["fields"][0]["status"], "disabled");
    assert_eq!(result["fields"][1]["value"], 7);
}

#[test]
fn historical_null_is_distinct_from_an_evaluation_error() {
    let result = run(json!({}), 1, vec![field(1, true, 1)]);
    assert_eq!(result["fields"][0]["status"], "evaluated");
    assert_eq!(result["fields"][0].get("value"), Some(&Value::Null));
}
