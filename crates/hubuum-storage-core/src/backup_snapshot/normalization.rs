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
