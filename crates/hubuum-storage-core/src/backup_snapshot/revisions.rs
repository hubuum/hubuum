use std::collections::{HashMap, HashSet};

use serde_json::Value;

use super::view::{BackupView, ReadError, Rows};
use super::{StorageBackupHistorySection, StorageBackupRow, StorageBackupStateSection};

fn required_state_section(
    document: &impl BackupView,
    section: StorageBackupStateSection,
) -> Result<Rows<'_>, ReadError> {
    document.state_rows(section)
}

const REVISION_STATE_SECTIONS: &[StorageBackupStateSection] = &[
    StorageBackupStateSection::IdentityScopes,
    StorageBackupStateSection::Groups,
    StorageBackupStateSection::Principals,
    StorageBackupStateSection::GroupMemberships,
    StorageBackupStateSection::Collections,
    StorageBackupStateSection::CollectionAuthorization,
    StorageBackupStateSection::Classes,
    StorageBackupStateSection::ComputedFieldDefinitions,
    StorageBackupStateSection::QueryUsageDeclarations,
    StorageBackupStateSection::ClassRelations,
    StorageBackupStateSection::Objects,
    StorageBackupStateSection::ObjectRelations,
    StorageBackupStateSection::ExportTemplates,
    StorageBackupStateSection::RemoteTargets,
    StorageBackupStateSection::EventSinks,
    StorageBackupStateSection::EventSubscriptions,
];

pub(super) const REVISION_HISTORY_SECTIONS: &[(
    StorageBackupHistorySection,
    StorageBackupStateSection,
)] = &[
    (
        StorageBackupHistorySection::CollectionHistory,
        StorageBackupStateSection::Collections,
    ),
    (
        StorageBackupHistorySection::ClassHistory,
        StorageBackupStateSection::Classes,
    ),
    (
        StorageBackupHistorySection::ClassRelationHistory,
        StorageBackupStateSection::ClassRelations,
    ),
    (
        StorageBackupHistorySection::ObjectHistory,
        StorageBackupStateSection::Objects,
    ),
    (
        StorageBackupHistorySection::ObjectRelationHistory,
        StorageBackupStateSection::ObjectRelations,
    ),
    (
        StorageBackupHistorySection::ExportTemplateHistory,
        StorageBackupStateSection::ExportTemplates,
    ),
    (
        StorageBackupHistorySection::RemoteTargetHistory,
        StorageBackupStateSection::RemoteTargets,
    ),
];

fn row_revision(section: &str, row: &StorageBackupRow) -> Result<i64, ReadError> {
    row.get("revision")
        .and_then(Value::as_i64)
        .filter(|revision| (1..i64::MAX).contains(revision))
        .ok_or_else(|| {
            ReadError::invalid(format!(
                "Full backup section '{section}' contains an invalid resource revision"
            ))
        })
}

fn row_i64(section: &str, row: &StorageBackupRow, field: &str) -> Result<i64, ReadError> {
    row.get(field).and_then(Value::as_i64).ok_or_else(|| {
        ReadError::invalid(format!(
            "Full backup section '{section}' contains an invalid {field}"
        ))
    })
}

pub(super) fn validate_backup_revisions(document: &impl BackupView) -> Result<(), ReadError> {
    for section in REVISION_STATE_SECTIONS {
        for row in required_state_section(document, *section)? {
            row_revision(section.as_str(), row?.as_ref())?;
        }
    }

    validate_authorization_state_revisions(document)?;

    for &(_, section) in REVISION_HISTORY_SECTIONS {
        let mut ids = HashSet::new();
        for row in required_state_section(document, section)? {
            let id = row_i64(section.as_str(), row?.as_ref(), "id")?;
            if id <= 0 || !ids.insert(id) {
                return Err(ReadError::invalid(format!(
                    "Full backup section '{section}' contains an invalid or duplicate resource identifier"
                )));
            }
        }
    }

    if !document.includes_history() {
        return validate_event_revisions(document);
    }
    for (history_section, state_section) in REVISION_HISTORY_SECTIONS {
        let rows = document.history_rows(*history_section)?;
        for row in rows.iter() {
            row_revision(history_section.as_str(), row?.as_ref())?;
        }
        validate_live_history_revisions(document, *history_section, *state_section, rows)?;
    }
    validate_event_revisions(document)
}

fn validate_authorization_state_revisions(document: &impl BackupView) -> Result<(), ReadError> {
    let collection_ids = required_state_section(document, StorageBackupStateSection::Collections)?
        .iter()
        .map(|row| row_i64("collections", row?.as_ref(), "id"))
        .collect::<Result<HashSet<_>, _>>()?;
    let authorization_ids =
        required_state_section(document, StorageBackupStateSection::CollectionAuthorization)?
            .iter()
            .map(|row| row_i64("collection_authorization", row?.as_ref(), "collection_id"))
            .collect::<Result<Vec<_>, _>>()?;
    let unique_authorization_ids = authorization_ids.iter().copied().collect::<HashSet<_>>();
    if authorization_ids.len() != unique_authorization_ids.len()
        || unique_authorization_ids != collection_ids
    {
        return Err(ReadError::invalid(
            "Full backup collection authorization revisions do not match collections".to_string(),
        ));
    }
    Ok(())
}

fn validate_live_history_revisions(
    document: &impl BackupView,
    history_section: StorageBackupHistorySection,
    state_section: StorageBackupStateSection,
    history_rows: Rows<'_>,
) -> Result<(), ReadError> {
    let live = required_state_section(document, state_section)?
        .iter()
        .map(|row| {
            let row = row?;
            Ok((
                row_i64(state_section.as_str(), &row, "id")?,
                row_revision(state_section.as_str(), &row)?,
            ))
        })
        .collect::<Result<HashMap<_, _>, ReadError>>()?;
    let mut open = HashMap::new();
    for row in history_rows {
        let row = row?;
        if !row.get("valid_to").is_some_and(Value::is_null) {
            continue;
        }
        let id = row_i64(history_section.as_str(), &row, "id")?;
        let revision = row_revision(history_section.as_str(), &row)?;
        if row.get("operation").and_then(Value::as_str) == Some("delete")
            || open.insert(id, revision).is_some()
        {
            return Err(ReadError::invalid(format!(
                "Full backup history section '{history_section}' has an invalid open snapshot"
            )));
        }
    }
    if open != live {
        return Err(ReadError::invalid(format!(
            "Full backup live revisions disagree with '{history_section}'"
        )));
    }
    Ok(())
}

fn validate_event_revisions(document: &impl BackupView) -> Result<(), ReadError> {
    let events = document.history_rows(StorageBackupHistorySection::AuditEvents)?;
    for event in events {
        let event = event?;
        for (column, snapshot) in [("before_revision", "before"), ("after_revision", "after")] {
            let stored = match event.get(column) {
                None | Some(Value::Null) => None,
                Some(value) => Some(
                    value
                        .as_i64()
                        .filter(|revision| (1..i64::MAX).contains(revision))
                        .ok_or_else(|| {
                            ReadError::invalid(format!(
                                "Full backup event contains an invalid {column}"
                            ))
                        })?,
                ),
            };
            let snapshot_revision = event
                .get(snapshot)
                .filter(|value| !value.is_null())
                .and_then(|value| value.get("revision"))
                .and_then(Value::as_i64);
            if stored.is_some() && stored != snapshot_revision {
                return Err(ReadError::invalid(format!(
                    "Full backup event {column} disagrees with its {snapshot} snapshot"
                )));
            }
        }
        if event.get("schema_version").and_then(Value::as_i64) == Some(2) {
            let before = event
                .get("before_revision")
                .is_some_and(|value| !value.is_null());
            let after = event
                .get("after_revision")
                .is_some_and(|value| !value.is_null());
            let action = event.get("action").and_then(Value::as_str);
            let valid_shape = match action {
                Some("created" | "queued" | "added") => !before && after,
                Some("deleted" | "removed" | "purged") => before && !after,
                _ => before && after,
            };
            if !valid_shape {
                return Err(ReadError::invalid(
                    "Full backup revision-aware event has inconsistent before/after revisions"
                        .to_string(),
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StorageBackupSnapshot;
    use rstest::rstest;
    use serde_json::json;

    #[rstest]
    #[case::deleted("deleted")]
    #[case::removed("removed")]
    #[case::purged("purged")]
    fn snapshots_accept_revisioned_deletion_event_shapes(#[case] action: &str) {
        let state = StorageBackupStateSection::ALL
            .iter()
            .copied()
            .map(|section| (section, Vec::new()))
            .collect();
        let mut history = StorageBackupHistorySection::ALL
            .iter()
            .copied()
            .map(|section| (section, Vec::new()))
            .collect::<super::super::StorageBackupHistorySections>();
        history
            .get_mut(&StorageBackupHistorySection::AuditEvents)
            .unwrap()
            .push(
                StorageBackupRow::try_from_value(json!({
                    "schema_version": 2, "action": action, "before": {"revision": 7}, "after": null,
                    "before_revision": 7, "after_revision": null,
                }))
                .unwrap(),
            );
        assert!(StorageBackupSnapshot::try_new(state, Some(history)).is_ok());
    }
}
