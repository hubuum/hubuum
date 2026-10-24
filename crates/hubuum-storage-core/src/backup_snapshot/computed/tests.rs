use super::*;
use crate::StorageBackupSnapshot;
use chrono::Duration;
use rstest::rstest;

fn snapshot() -> StorageBackupSnapshot {
    let mut state = State::ALL
        .iter()
        .copied()
        .map(|section| (section, Vec::new()))
        .collect::<super::super::super::StorageBackupStateSections>();
    state.get_mut(&State::Classes).unwrap().push(StorageBackupRow::try_from_value(json!({
        "id": 2, "revision": 1, "name": "class", "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z"
    })).unwrap());
    state.get_mut(&State::ComputedFieldDefinitions).unwrap().push(StorageBackupRow::try_from_value(json!({
        "id": 3, "class_id": 2, "revision": 4, "visibility": "shared", "owner_user_id": null,
        "key": "value", "label": "Value", "description": "", "operation": {"type":"first_non_null","paths":["/value"]},
        "result_type":"integer", "enabled": true, "semantics_version":1
    })).unwrap());
    StorageBackupSnapshot::try_new(super::super::with_test_schema_sections(state), None).unwrap()
}

#[rstest]
#[case::history_free(false)]
#[case::legacy_retained(true)]
fn restore_observes_shared_definitions_only_at_restore_boundary(#[case] legacy: bool) {
    let at = Utc::now();
    let mut source = snapshot();
    if legacy {
        source = source.prepare_for_test(at - Duration::days(1));
        source
            .history_sections
            .as_mut()
            .unwrap()
            .get_mut(&History::ComputedFieldHistory)
            .unwrap()
            .clear();
    }
    let restored = source.prepare_for_test(at);
    let records = records(&restored).unwrap();
    assert_eq!(records.len(), 1);
    assert!(!records[0].is_effective_at(at - Duration::seconds(1)));
    assert_eq!(records[0].definitions()[0].revision().get(), 4);
    assert!(records[0].is_effective_at(at));
    validate(&restored).unwrap();
}

#[rstest]
#[case::overlap("overlap")]
#[case::duplicate_id("duplicate")]
#[case::wrong_live_revision("revision")]
#[case::actor_metadata("actor")]
#[case::exhausted_sequence("sequence")]
fn backup_rejects_inconsistent_computed_history(#[case] fault: &str) {
    let mut snapshot = snapshot().prepare_for_test(Utc::now());
    let rows = snapshot
        .history_sections
        .as_mut()
        .unwrap()
        .get_mut(&History::ComputedFieldHistory)
        .unwrap();
    match fault {
        "overlap" | "duplicate" => {
            let mut extra = rows[0].clone().into_value();
            if fault == "overlap" {
                extra["history_entry_id"] = json!(2);
            }
            rows.push(StorageBackupRow::try_from_value(extra).unwrap());
        }
        _ => {
            let mut value = rows[0].clone().into_value();
            match fault {
                "revision" => value["definitions"][0]["revision"] = json!(99),
                "actor" => value["definitions"][0]["created_by"] = json!(12),
                _ => value["history_entry_id"] = json!(i64::MAX),
            }
            rows[0] = StorageBackupRow::try_from_value(value).unwrap();
        }
    }
    assert!(validate(&snapshot).is_err());
}

#[test]
fn retained_computed_history_survives_restore_without_rebasing() {
    let original = snapshot().prepare_for_test(Utc::now());
    let history = original.history_sections.clone();
    let restored = original.prepare_for_test(Utc::now() + Duration::days(1));
    assert!(history == restored.history_sections);
}

#[test]
fn restored_history_preserves_missing_intervals_instead_of_backfilling() {
    let at = Utc::now();
    let mut source = snapshot().prepare_for_test(at);
    let rows = source
        .history_sections
        .as_mut()
        .unwrap()
        .get_mut(&History::ComputedFieldHistory)
        .unwrap();
    let mut record: StorageComputedFieldHistory =
        serde_json::from_value(rows[0].clone().into_value()).unwrap();
    record.close_at(at + Duration::seconds(1)).unwrap();
    rows[0] = StorageBackupRow::try_from_value(serde_json::to_value(record).unwrap()).unwrap();
    let restored = source.prepare_for_test(at + Duration::days(1));
    let records = records(&restored).unwrap();
    assert!(
        !records
            .iter()
            .any(|record| record.is_effective_at(at + Duration::hours(1)))
    );
    validate(&restored).unwrap();
}
