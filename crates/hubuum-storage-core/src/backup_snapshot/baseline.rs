use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::json;

use super::StorageBackupRow;

pub(super) fn baseline(
    row: &StorageBackupRow,
    history_entry_id: i64,
    restored_at: DateTime<Utc>,
) -> StorageBackupRow {
    let mut fields = row.fields().clone();
    fields.extend(
        json!({
            "history_entry_id": history_entry_id,
            "operation": "create",
            "valid_from": restored_at.to_rfc3339_opts(SecondsFormat::AutoSi, true),
            "valid_to": null,
            "actor_principal_id": null,
            "actor_kind": "system",
            "initiator_principal_id": null,
            "task_id": null
        })
        .as_object()
        .expect("literal object")
        .clone(),
    );
    StorageBackupRow(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{StorageBackupBudget, StorageBackupReplay, StorageBackupStateSection};
    use crate::{StorageBackupHistorySection, StorageBackupSnapshot};
    use rstest::rstest;
    use serde_json::Value;

    fn state_with_resource(section: StorageBackupStateSection) -> StorageBackupSnapshot {
        let mut state = StorageBackupStateSection::ALL
            .iter()
            .copied()
            .map(|section| (section, Vec::new()))
            .collect::<super::super::StorageBackupStateSections>();
        state.get_mut(&section).unwrap().push(
            StorageBackupRow::try_from_value(json!({
                "id": 17, "revision": 42, "name": "unchanged", "data": null,
                "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-02-01T00:00:00Z"
            }))
            .unwrap(),
        );
        if section == StorageBackupStateSection::Collections {
            state
                .get_mut(&StorageBackupStateSection::CollectionAuthorization)
                .unwrap()
                .push(
                    StorageBackupRow::try_from_value(json!({"collection_id": 17, "revision": 9}))
                        .unwrap(),
                );
        }
        StorageBackupSnapshot::try_new(super::super::with_test_schema_sections(state), None)
            .unwrap()
    }

    #[test]
    fn schema_baseline_identity_does_not_depend_on_snapshot_row_order() {
        let (mut state, _) = state_with_resource(StorageBackupStateSection::Classes).into_parts();
        let mut second = state[&StorageBackupStateSection::Classes][0]
            .fields()
            .clone();
        second.insert("id".into(), json!(2));
        state
            .get_mut(&StorageBackupStateSection::Classes)
            .unwrap()
            .push(StorageBackupRow::try_from_value(Value::Object(second)).unwrap());
        state
            .get_mut(&StorageBackupStateSection::ClassSchemaRevisions)
            .unwrap()
            .clear();
        state
            .get_mut(&StorageBackupStateSection::ClassSchemaState)
            .unwrap()
            .clear();
        let state = super::super::with_test_schema_sections(state);
        let source = StorageBackupSnapshot::try_new(state.clone(), None).unwrap();
        let mut reordered = state;
        for rows in reordered.values_mut() {
            rows.reverse();
        }
        let reordered = StorageBackupSnapshot::try_new(reordered, None).unwrap();
        let at = Utc::now();
        let original = source.prepare_for_test(at).into_parts().1.unwrap();
        let reordered = reordered.prepare_for_test(at).into_parts().1.unwrap();
        assert_eq!(
            original[&StorageBackupHistorySection::ClassSchemaHistory],
            reordered[&StorageBackupHistorySection::ClassSchemaHistory],
        );
    }

    #[test]
    fn schema_baselines_share_temporal_history_timestamp_precision() {
        let at = DateTime::parse_from_rfc3339("2026-09-11T12:00:00.123456789Z")
            .unwrap()
            .to_utc();
        let history = state_with_resource(StorageBackupStateSection::Classes)
            .prepare_for_test(at)
            .into_parts()
            .1
            .unwrap();
        assert_eq!(
            history[&StorageBackupHistorySection::ClassSchemaHistory][0].get("occurred_at"),
            history[&StorageBackupHistorySection::ClassHistory][0].get("valid_from"),
        );
    }

    #[rstest]
    #[case(
        StorageBackupStateSection::Collections,
        StorageBackupHistorySection::CollectionHistory
    )]
    #[case(
        StorageBackupStateSection::Classes,
        StorageBackupHistorySection::ClassHistory
    )]
    #[case(
        StorageBackupStateSection::ClassRelations,
        StorageBackupHistorySection::ClassRelationHistory
    )]
    #[case(
        StorageBackupStateSection::Objects,
        StorageBackupHistorySection::ObjectHistory
    )]
    #[case(
        StorageBackupStateSection::ObjectRelations,
        StorageBackupHistorySection::ObjectRelationHistory
    )]
    #[case(
        StorageBackupStateSection::ExportTemplates,
        StorageBackupHistorySection::ExportTemplateHistory
    )]
    #[case(
        StorageBackupStateSection::RemoteTargets,
        StorageBackupHistorySection::RemoteTargetHistory
    )]
    fn history_free_restore_starts_each_temporal_resource_at_its_existing_revision(
        #[case] state_section: StorageBackupStateSection,
        #[case] history_section: StorageBackupHistorySection,
    ) {
        let source = state_with_resource(state_section);
        let expected_state = source.clone().into_parts().0;
        let at = DateTime::parse_from_rfc3339("2026-09-09T12:00:00Z")
            .unwrap()
            .to_utc();
        let prepared = StorageBackupReplay::from_snapshot(
            source,
            StorageBackupBudget::new(16 * 1024 * 1024, 10000).unwrap(),
        )
        .unwrap();
        assert!(!prepared.includes_history());
        let snapshot = prepared
            .at_restore_boundary(at)
            .unwrap()
            .materialize()
            .unwrap();
        let (state, history) = snapshot.clone().into_parts();
        assert_eq!(state, expected_state);
        let history = history.unwrap();
        let mut expected = expected_state[&state_section][0].fields().clone();
        expected.extend(
            json!({
                "history_entry_id": 1, "operation": "create", "valid_from": "2026-09-09T12:00:00Z",
                "valid_to": null, "actor_kind": "system", "actor_principal_id": null,
                "initiator_principal_id": null, "task_id": null,
            })
            .as_object()
            .unwrap()
            .clone(),
        );
        assert_eq!(
            history[&history_section],
            vec![StorageBackupRow::try_from_value(Value::Object(expected)).unwrap()]
        );
        assert_eq!(
            history.values().map(Vec::len).sum::<usize>(),
            1 + expected_state[&StorageBackupStateSection::ClassSchemaRevisions].len()
                + expected_state[&StorageBackupStateSection::Classes].len()
        );

        let repeated = StorageBackupReplay::from_snapshot(
            snapshot.clone(),
            StorageBackupBudget::new(16 * 1024 * 1024, 10000).unwrap(),
        )
        .unwrap();
        assert!(repeated.includes_history());
        assert_eq!(
            repeated
                .at_restore_boundary(at + chrono::Duration::hours(1))
                .unwrap()
                .materialize()
                .unwrap(),
            snapshot
        );
    }

    #[test]
    fn history_inclusive_capture_rejects_missing_history() {
        let (state, _) = state_with_resource(StorageBackupStateSection::Classes).into_parts();
        let history = StorageBackupHistorySection::ALL
            .iter()
            .copied()
            .map(|section| (section, Vec::new()))
            .collect();
        let error = StorageBackupSnapshot::try_new(state, Some(history)).unwrap_err();
        assert!(error.to_string().contains("live revisions disagree"));
    }
}
