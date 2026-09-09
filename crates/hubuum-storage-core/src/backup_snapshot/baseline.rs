use std::collections::{HashMap, HashSet};

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Value, json};

use super::revisions::{REVISION_HISTORY_SECTIONS, row_revision};
use super::{StorageBackupHistorySection, StorageBackupRow, StorageBackupSnapshot};
use crate::StorageValidationError;

impl StorageBackupSnapshot {
    /// Explicitly repair an artifact with missing current temporal snapshots.
    /// Existing history is never replaced. Contradictory revisions, duplicate
    /// open snapshots, and incomplete sections still fail normal validation.
    /// This operation is deliberately separate from ordinary snapshot capture.
    pub fn try_repair_missing_history(
        state: super::StorageBackupStateSections,
        mut history: super::StorageBackupHistorySections,
        observed_at: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        for &(history_section, state_section) in REVISION_HISTORY_SECTIONS {
            let live = state.get(&state_section).ok_or_else(|| {
                StorageValidationError::invalid(format!("Missing state section '{state_section}'"))
            })?;
            let rows = history.get_mut(&history_section).ok_or_else(|| {
                StorageValidationError::invalid(format!(
                    "Missing history section '{history_section}'"
                ))
            })?;
            let mut next_id = 0;
            let mut entry_ids = HashSet::new();
            let mut open_ids = HashSet::new();
            let mut latest_revisions = HashMap::<i64, i64>::new();
            for existing in rows.iter() {
                let id = existing
                    .get("history_entry_id")
                    .and_then(Value::as_i64)
                    .filter(|id| *id > 0)
                    .ok_or_else(|| {
                        StorageValidationError::invalid("Invalid temporal history entry identifier")
                    })?;
                if !entry_ids.insert(id) {
                    return Err(StorageValidationError::invalid(
                        "Duplicate temporal history entry identifier",
                    ));
                }
                next_id = next_id.max(id);
                let resource_id = existing
                    .get("id")
                    .and_then(Value::as_i64)
                    .filter(|id| *id > 0)
                    .ok_or_else(|| {
                        StorageValidationError::invalid("Invalid temporal resource identifier")
                    })?;
                let revision = row_revision(history_section.as_str(), existing)?;
                latest_revisions
                    .entry(resource_id)
                    .and_modify(|latest| *latest = (*latest).max(revision))
                    .or_insert(revision);
                if existing.get("valid_to").is_some_and(Value::is_null) {
                    open_ids.insert(resource_id);
                }
            }
            for current in live {
                let id = current.get("id").and_then(Value::as_i64).ok_or_else(|| {
                    StorageValidationError::invalid("Invalid temporal resource identifier")
                })?;
                if open_ids.contains(&id) {
                    continue;
                }
                let revision = row_revision(state_section.as_str(), current)?;
                if latest_revisions
                    .get(&id)
                    .is_some_and(|previous| *previous >= revision)
                {
                    return Err(StorageValidationError::invalid(
                        "Missing current snapshot contradicts retained history revisions",
                    ));
                }
                next_id = next_id.checked_add(1).ok_or_else(|| {
                    StorageValidationError::too_large("History entry identifier overflow")
                })?;
                rows.push(baseline(current, next_id, observed_at));
            }
        }
        Self::try_new(state, Some(history))
    }

    /// Establish the first observed version in a newly restored timeline.
    /// Existing history is preserved. Baselines retain authoritative revisions
    /// and timestamps; only temporal validity begins at the restore boundary.
    pub(crate) fn restart_history(self, restored_at: DateTime<Utc>) -> Self {
        if self.history_sections.is_some() {
            return self;
        }
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
        Self {
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
    // The logical restore boundary uses the precision shared by adapters.
    let restored_at = DateTime::from_timestamp_micros(restored_at.timestamp_micros())
        .expect("a UTC timestamp remains representable at microsecond precision");
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
        StorageBackupSnapshot::try_new(state, None).unwrap()
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
        assert_eq!(history.values().map(Vec::len).sum::<usize>(), 1);

        let repeated = StorageRestoreDocument::at_restore_boundary(
            StorageRestoreDocumentMetadata::new(5, at, "test"),
            snapshot.clone(),
            at + chrono::Duration::hours(1),
        );
        assert!(repeated.source_includes_history());
        assert_eq!(repeated.into_parts().1, snapshot);
    }

    #[test]
    fn history_inclusive_capture_does_not_repair_missing_history() {
        let (state, _) = state_with_resource(StorageBackupStateSection::Classes).into_parts();
        let history = StorageBackupHistorySection::ALL
            .iter()
            .copied()
            .map(|section| (section, Vec::new()))
            .collect();
        let error = StorageBackupSnapshot::try_new(state, Some(history)).unwrap_err();
        assert!(error.to_string().contains("live revisions disagree"));
    }

    #[test]
    fn explicit_repair_retains_closed_history_and_is_idempotent() {
        let at = DateTime::parse_from_rfc3339("2026-09-09T12:00:00Z")
            .unwrap()
            .to_utc();
        let (state, _) = state_with_resource(StorageBackupStateSection::Classes).into_parts();
        let old = StorageBackupRow::try_from_value(json!({"id": 17, "revision": 41, "history_entry_id": 77,
            "operation": "update", "valid_from": "2026-01-01T00:00:00Z", "valid_to": "2026-02-01T00:00:00Z", "name": "old"})).unwrap();
        let mut history = StorageBackupHistorySection::ALL
            .iter()
            .copied()
            .map(|section| (section, Vec::new()))
            .collect::<super::super::StorageBackupHistorySections>();
        history.insert(StorageBackupHistorySection::ClassHistory, vec![old.clone()]);
        let repaired =
            StorageBackupSnapshot::try_repair_missing_history(state.clone(), history, at).unwrap();
        let (actual_state, history) = repaired.clone().into_parts();
        assert_eq!(actual_state, state);
        let history = history.unwrap();
        let rows = &history[&StorageBackupHistorySection::ClassHistory];
        assert_eq!(
            rows,
            &vec![
                old,
                baseline(&state[&StorageBackupStateSection::Classes][0], 78, at)
            ]
        );
        let repeated = StorageBackupSnapshot::try_repair_missing_history(
            state,
            history,
            at + chrono::Duration::days(1),
        )
        .unwrap();
        assert_eq!(repeated, repaired);
    }

    #[rstest]
    #[case::same_revision(42)]
    #[case::later_revision(43)]
    fn explicit_repair_rejects_closed_history_at_or_after_the_current_revision(
        #[case] revision: i64,
    ) {
        let at = DateTime::parse_from_rfc3339("2026-09-09T12:00:00Z")
            .unwrap()
            .to_utc();
        let (state, history) = state_with_resource(StorageBackupStateSection::Classes)
            .restart_history(at)
            .into_parts();
        let mut history = history.unwrap();
        let rows = history
            .get_mut(&StorageBackupHistorySection::ClassHistory)
            .unwrap();
        let mut fields = rows[0].fields().clone();
        fields.insert("revision".to_string(), json!(revision));
        fields.insert("valid_to".to_string(), json!(at));
        rows[0] = StorageBackupRow::try_from_value(Value::Object(fields)).unwrap();
        assert!(StorageBackupSnapshot::try_repair_missing_history(state, history, at).is_err());
    }

    #[rstest]
    #[case::stale(41, "update", false)]
    #[case::ahead(43, "update", false)]
    #[case::open_tombstone(42, "delete", false)]
    #[case::duplicate(42, "create", true)]
    fn explicit_repair_rejects_contradictory_open_history(
        #[case] revision: i64,
        #[case] operation: &str,
        #[case] duplicate: bool,
    ) {
        let at = DateTime::parse_from_rfc3339("2026-09-09T12:00:00Z")
            .unwrap()
            .to_utc();
        let (state, history) = state_with_resource(StorageBackupStateSection::Classes)
            .restart_history(at)
            .into_parts();
        let mut history = history.unwrap();
        let rows = history
            .get_mut(&StorageBackupHistorySection::ClassHistory)
            .unwrap();
        let mut fields = rows[0].fields().clone();
        fields.insert("revision".to_string(), json!(revision));
        fields.insert("operation".to_string(), json!(operation));
        rows[0] = StorageBackupRow::try_from_value(Value::Object(fields)).unwrap();
        if duplicate {
            rows.push(rows[0].clone());
        }
        assert!(StorageBackupSnapshot::try_repair_missing_history(state, history, at).is_err());
    }
}
