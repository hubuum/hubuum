use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Value, json};

use super::revisions::REVISION_HISTORY_SECTIONS;
use super::{StorageBackupHistorySection, StorageBackupRow, StorageBackupSnapshot};

impl StorageBackupSnapshot {
    /// Establish the first observed version in a newly restored timeline.
    /// Existing history is preserved. Baselines retain authoritative revisions
    /// and timestamps; only temporal validity begins at the restore boundary.
    pub(crate) fn restart_history(self, restored_at: DateTime<Utc>) -> Self {
        if self.history_sections.is_some() {
            return self;
        }
        // Every history section shares the timestamp precision supported by all adapters.
        let restored_at = DateTime::from_timestamp_micros(restored_at.timestamp_micros())
            .expect("a UTC timestamp remains representable at microsecond precision");
        let mut history = StorageBackupHistorySection::ALL
            .iter()
            .copied()
            .map(|section| (section, Vec::new()))
            .collect::<super::StorageBackupHistorySections>();
        for &(history_section, state_section) in REVISION_HISTORY_SECTIONS {
            let mut rows = self.state_sections[&state_section]
                .iter()
                .collect::<Vec<_>>();
            rows.sort_by_key(|row| row.get("id").and_then(Value::as_i64));
            let baselines = rows
                .into_iter()
                .enumerate()
                .map(|(index, row)| {
                    // A Vec of non-zero-sized rows cannot hold i64::MAX
                    // entries: its entire allocation is bounded by isize::MAX.
                    let id = i64::try_from(index + 1).expect("allocated row count fits i64");
                    baseline(row, id, restored_at)
                })
                .collect();
            history.insert(history_section, baselines);
        }
        let mut schemas = self.state_sections
            [&super::StorageBackupStateSection::ClassSchemaRevisions]
            .iter()
            .collect::<Vec<_>>();
        schemas.sort_by_key(|row| {
            (
                row.get("class_id").and_then(Value::as_i64),
                row.get("revision").and_then(Value::as_i64),
            )
        });
        let schema_baselines = schemas.into_iter().enumerate().map(|(index, row)| {
                let id = i64::try_from(index + 1).expect("allocated row count fits i64");
                StorageBackupRow::try_from_value(json!({
                    "id": id, "class_id": row.get("class_id"), "revision": row.get("revision"),
                    "snapshot": row.clone().into_value(), "operation": "create", "occurred_at": restored_at,
                    "actor_principal_id": null, "task_id": null
                })).expect("schema baseline is an object")
            }).collect();
        history.insert(
            StorageBackupHistorySection::ClassSchemaHistory,
            schema_baselines,
        );
        Self {
            schema_limits: self.schema_limits,
            state_sections: self.state_sections,
            history_sections: Some(history),
        }
    }
}

fn baseline(
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
    use crate::{
        StorageBackupStateSection, StorageRestoreDocument, StorageRestoreDocumentMetadata,
    };
    use rstest::rstest;

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
        let original = source.restart_history(at).into_parts().1.unwrap();
        let reordered = reordered.restart_history(at).into_parts().1.unwrap();
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
            .restart_history(at)
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
        let prepared = StorageRestoreDocument::at_restore_boundary(
            StorageRestoreDocumentMetadata::new(5, at, "test"),
            source,
            at,
        );
        assert!(!prepared.source_includes_history());
        let (_, snapshot) = prepared.into_parts();
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
        );

        let repeated = StorageRestoreDocument::at_restore_boundary(
            StorageRestoreDocumentMetadata::new(5, at, "test"),
            snapshot.clone(),
            at + chrono::Duration::hours(1),
        );
        assert!(repeated.source_includes_history());
        assert_eq!(repeated.into_parts().1, snapshot);
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
