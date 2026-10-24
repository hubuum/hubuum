use super::{
    StorageBackupHistorySection, StorageBackupHistorySections, StorageBackupRow,
    StorageBackupStateSection, StorageBackupStateSections,
};
use serde_json::Value;

pub fn normalize_legacy_event_configuration(
    backup_version: i32,
    state: &mut StorageBackupStateSections,
    history: Option<&mut StorageBackupHistorySections>,
) {
    if backup_version != 6 {
        return;
    }
    fn defaults(rows: Option<&mut Vec<StorageBackupRow>>, fields: &[(&str, Value)]) {
        if let Some(rows) = rows {
            for row in rows {
                let mut value = row.fields().clone();
                for (name, default) in fields {
                    value
                        .entry((*name).to_owned())
                        .or_insert_with(|| default.clone());
                }
                *row = StorageBackupRow::try_from_value(Value::Object(value))
                    .expect("object remains an object");
            }
        }
    }
    defaults(
        state.get_mut(&StorageBackupStateSection::EventSinks),
        &[(
            "delivery_policy",
            serde_json::json!({"min_interval_ms":null}),
        )],
    );
    defaults(
        history.and_then(|history| {
            history.get_mut(&StorageBackupHistorySection::TerminalEventDeliveries)
        }),
        &[
            ("purpose", Value::String("event".into())),
            ("deferred_reason", Value::Null),
        ],
    );
}

pub fn normalize_legacy_class_schema_policies(
    _backup_version: i32,
    state: &mut StorageBackupStateSections,
    history: Option<&mut StorageBackupHistorySections>,
) {
    fn normalize_rows(rows: Option<&mut Vec<StorageBackupRow>>) {
        let Some(rows) = rows else {
            return;
        };
        for row in rows {
            let validates_without_schema = row
                .get("validate_schema")
                .and_then(Value::as_bool)
                .unwrap_or(false)
                && row.get("json_schema").is_none_or(Value::is_null);
            if !validates_without_schema {
                continue;
            }
            let mut fields = row.fields().clone();
            fields.insert("validate_schema".to_string(), Value::Bool(false));
            *row = StorageBackupRow::try_from_value(Value::Object(fields))
                .expect("normalizing a backup row preserves its object shape");
        }
    }

    normalize_rows(state.get_mut(&StorageBackupStateSection::Classes));
    normalize_rows(
        history.and_then(|history| history.get_mut(&StorageBackupHistorySection::ClassHistory)),
    );
}

pub fn normalize_legacy_relation_labels(
    _backup_version: i32,
    state: &mut StorageBackupStateSections,
    history: Option<&mut StorageBackupHistorySections>,
) {
    let current = state.get_mut(&StorageBackupStateSection::ClassRelations);
    let history = history
        .and_then(|history| history.get_mut(&StorageBackupHistorySection::ClassRelationHistory));
    for row in current.into_iter().chain(history).flatten() {
        let mut fields = row.fields().clone();
        for label in ["forward_label", "reverse_label"] {
            fields.entry(label.to_owned()).or_insert(Value::Null);
        }
        *row = StorageBackupRow::try_from_value(Value::Object(fields))
            .expect("normalizing a backup row preserves its object shape");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn format_six_notifications_restore_with_legacy_defaults() {
        let mut state = BTreeMap::from([(
            StorageBackupStateSection::EventSinks,
            vec![StorageBackupRow::try_from_value(json!({"id": 1, "kind": "webhook"})).unwrap()],
        )]);
        let mut history = BTreeMap::from([(
            StorageBackupHistorySection::TerminalEventDeliveries,
            vec![
                StorageBackupRow::try_from_value(json!({"id": 1, "status": "succeeded"})).unwrap(),
            ],
        )]);
        normalize_legacy_event_configuration(6, &mut state, Some(&mut history));
        assert_eq!(
            state[&StorageBackupStateSection::EventSinks][0].fields()["delivery_policy"],
            json!({"min_interval_ms": null}),
        );
        let delivery = &history[&StorageBackupHistorySection::TerminalEventDeliveries][0];
        assert_eq!(delivery.fields()["purpose"], "event");
        assert_eq!(delivery.fields()["deferred_reason"], json!(null));
    }

    #[rstest]
    #[case::current(false)]
    #[case::history(true)]
    fn legacy_class_without_schema_disables_validation(#[case] historical: bool) {
        let row = StorageBackupRow::try_from_value(json!({
            "id": 2, "json_schema": null, "validate_schema": true
        }))
        .unwrap();
        let mut state = BTreeMap::new();
        let mut history = BTreeMap::new();
        if historical {
            history.insert(StorageBackupHistorySection::ClassHistory, vec![row]);
        } else {
            state.insert(StorageBackupStateSection::Classes, vec![row]);
        }
        normalize_legacy_class_schema_policies(10, &mut state, Some(&mut history));
        let class = if historical {
            &history[&StorageBackupHistorySection::ClassHistory][0]
        } else {
            &state[&StorageBackupStateSection::Classes][0]
        };
        assert_eq!(class.get("validate_schema"), Some(&json!(false)));
    }
}
