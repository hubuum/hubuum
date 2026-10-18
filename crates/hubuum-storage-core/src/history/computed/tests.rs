use super::*;
use chrono::Duration;
use rstest::rstest;
use serde_json::json;

fn field() -> Value {
    json!({"id":1,"class_id":2,"revision":3,"definition":{
        "key":"value","label":"Value","description":"",
        "operation":{"type":"first_non_null","paths":["/value"]},
        "result_type":"integer","enabled":true,"semantics_version":1
    }})
}

#[rstest]
#[case::before(-1, false)]
#[case::start(0, true)]
#[case::within(1, true)]
#[case::end(2, false)]
fn computed_history_selection_proves_half_open_validity(#[case] seconds: i64, #[case] valid: bool) {
    let start = DateTime::UNIX_EPOCH;
    let class = ClassId::new(2).unwrap();
    let row = StorageComputedFieldHistory::builder(class, HistoryRecordId::new(1).unwrap(), start)
        .valid_until(Some(start + Duration::seconds(2)))
        .build()
        .unwrap();
    assert_eq!(
        row.select(StorageComputedFieldHistoryQuery::new(
            class,
            start + Duration::seconds(seconds)
        ))
        .is_ok(),
        valid
    );
}

#[rstest]
#[case::wrong_class(3, 2)]
#[case::duplicate_identity(2, 1)]
#[case::duplicate_key(2, 2)]
fn computed_definition_sets_reject_inconsistent_membership(#[case] class: i32, #[case] id: i32) {
    let first: StorageHistoricalComputedField = serde_json::from_value(field()).unwrap();
    let mut second = field();
    second["class_id"] = json!(class);
    second["id"] = json!(id);
    if id == 1 {
        second["definition"]["key"] = json!("other");
    }
    let second = serde_json::from_value(second).unwrap();
    assert!(
        StorageComputedFieldHistory::builder(
            ClassId::new(2).unwrap(),
            HistoryRecordId::new(1).unwrap(),
            DateTime::UNIX_EPOCH
        )
        .definitions(vec![first, second])
        .build()
        .is_err()
    );
}

#[rstest]
#[case::owner("owner_user_id")]
#[case::creator("created_by")]
#[case::visibility("visibility")]
fn historical_field_wire_rejects_personal_or_actor_metadata(#[case] private: &str) {
    let mut value = field();
    value[private] = json!(1);
    assert!(serde_json::from_value::<StorageHistoricalComputedField>(value).is_err());
}

#[test]
fn unknown_semantics_are_retained_without_current_catalog_interpretation() {
    let mut value = field();
    value["definition"]["semantics_version"] = json!(99);
    value["definition"]["operation"] = json!({"type":"retired_operation"});
    let parsed: StorageHistoricalComputedField = serde_json::from_value(value.clone()).unwrap();
    assert!(parsed.evaluator_definition().is_none());
    assert_eq!(serde_json::to_value(parsed).unwrap(), value);
}

#[test]
fn computed_history_rejects_a_different_class_at_the_same_time() {
    let row = StorageComputedFieldHistory::builder(
        ClassId::new(2).unwrap(),
        HistoryRecordId::new(1).unwrap(),
        DateTime::UNIX_EPOCH,
    )
    .build()
    .unwrap();
    assert!(
        row.select(StorageComputedFieldHistoryQuery::new(
            ClassId::new(3).unwrap(),
            DateTime::UNIX_EPOCH
        ))
        .is_err()
    );
}

#[test]
fn historical_definition_accepts_maximum_current_strings_with_json_escaping() {
    let mut value = field();
    value["definition"]["label"] = json!("\u{1}".repeat(MAX_LABEL_BYTES));
    value["definition"]["description"] = json!("\u{1}".repeat(MAX_DESCRIPTION_BYTES));
    value["definition"]["operation"]["paths"] = json!(
        (0..16)
            .map(|index| format!("/{index:02}{}", "\u{1}".repeat(509)))
            .collect::<Vec<_>>()
    );
    let definition: StorageHistoricalComputedField = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(definition).unwrap(), value);
}
