use std::collections::{HashMap, HashSet};

use serde_json::Value;

use super::{
    StorageBackupHistorySection, StorageBackupRow, StorageBackupSnapshot, StorageBackupStateSection,
};
use crate::StorageValidationError;

fn required_state_section(
    document: &StorageBackupSnapshot,
    section: StorageBackupStateSection,
) -> Result<&[StorageBackupRow], StorageValidationError> {
    document
        .state_sections
        .get(&section)
        .map(Vec::as_slice)
        .ok_or_else(|| {
            StorageValidationError::invalid(format!(
                "Backup snapshot is missing required state section '{section}'"
            ))
        })
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

pub(super) fn row_revision(
    section: &str,
    row: &StorageBackupRow,
) -> Result<i64, StorageValidationError> {
    row.get("revision")
        .and_then(Value::as_i64)
        .filter(|revision| (1..i64::MAX).contains(revision))
        .ok_or_else(|| {
            StorageValidationError::invalid(format!(
                "Full backup section '{section}' contains an invalid resource revision"
            ))
        })
}

fn row_i64(
    section: &str,
    row: &StorageBackupRow,
    field: &str,
) -> Result<i64, StorageValidationError> {
    row.get(field).and_then(Value::as_i64).ok_or_else(|| {
        StorageValidationError::invalid(format!(
            "Full backup section '{section}' contains an invalid {field}"
        ))
    })
}

pub(super) fn validate_backup_revisions(
    document: &StorageBackupSnapshot,
) -> Result<(), StorageValidationError> {
    for section in REVISION_STATE_SECTIONS {
        for row in required_state_section(document, *section)? {
            row_revision(section.as_str(), row)?;
        }
    }

    validate_authorization_state_revisions(document)?;

    for &(_, section) in REVISION_HISTORY_SECTIONS {
        let mut ids = HashSet::new();
        for row in required_state_section(document, section)? {
            let id = row_i64(section.as_str(), row, "id")?;
            if id <= 0 || !ids.insert(id) {
                return Err(StorageValidationError::invalid(format!(
                    "Full backup section '{section}' contains an invalid or duplicate resource identifier"
                )));
            }
        }
    }

    let Some(history) = &document.history_sections else {
        return validate_event_revisions(document);
    };
    for (history_section, state_section) in REVISION_HISTORY_SECTIONS {
        let rows = history.get(history_section).ok_or_else(|| {
            StorageValidationError::invalid(format!(
                "Full backup history is missing required section '{history_section}'"
            ))
        })?;
        for row in rows {
            row_revision(history_section.as_str(), row)?;
        }
        validate_live_history_revisions(document, *history_section, *state_section, rows)?;
    }
    validate_event_revisions(document)
}

fn validate_authorization_state_revisions(
    document: &StorageBackupSnapshot,
) -> Result<(), StorageValidationError> {
    let collection_ids = required_state_section(document, StorageBackupStateSection::Collections)?
        .iter()
        .map(|row| row_i64("collections", row, "id"))
        .collect::<Result<HashSet<_>, _>>()?;
    let authorization_ids =
        required_state_section(document, StorageBackupStateSection::CollectionAuthorization)?
            .iter()
            .map(|row| row_i64("collection_authorization", row, "collection_id"))
            .collect::<Result<Vec<_>, _>>()?;
    let unique_authorization_ids = authorization_ids.iter().copied().collect::<HashSet<_>>();
    if authorization_ids.len() != unique_authorization_ids.len()
        || unique_authorization_ids != collection_ids
    {
        return Err(StorageValidationError::invalid(
            "Full backup collection authorization revisions do not match collections".to_string(),
        ));
    }
    Ok(())
}

fn validate_live_history_revisions(
    document: &StorageBackupSnapshot,
    history_section: StorageBackupHistorySection,
    state_section: StorageBackupStateSection,
    history_rows: &[StorageBackupRow],
) -> Result<(), StorageValidationError> {
    let live = required_state_section(document, state_section)?
        .iter()
        .map(|row| {
            Ok((
                row_i64(state_section.as_str(), row, "id")?,
                row_revision(state_section.as_str(), row)?,
            ))
        })
        .collect::<Result<HashMap<_, _>, StorageValidationError>>()?;
    let mut open = HashMap::new();
    for row in history_rows
        .iter()
        .filter(|row| row.get("valid_to").is_some_and(Value::is_null))
    {
        let id = row_i64(history_section.as_str(), row, "id")?;
        let revision = row_revision(history_section.as_str(), row)?;
        if row.get("operation").and_then(Value::as_str) == Some("delete")
            || open.insert(id, revision).is_some()
        {
            return Err(StorageValidationError::invalid(format!(
                "Full backup history section '{history_section}' has an invalid open snapshot"
            )));
        }
    }
    if open != live {
        return Err(StorageValidationError::invalid(format!(
            "Full backup live revisions disagree with '{history_section}'"
        )));
    }
    Ok(())
}

fn validate_event_revisions(
    document: &StorageBackupSnapshot,
) -> Result<(), StorageValidationError> {
    let Some(events) = document
        .history_sections
        .as_ref()
        .and_then(|history| history.get(&StorageBackupHistorySection::AuditEvents))
    else {
        return Ok(());
    };
    for event in events {
        for (column, snapshot) in [("before_revision", "before"), ("after_revision", "after")] {
            let stored = match event.get(column) {
                None | Some(Value::Null) => None,
                Some(value) => Some(
                    value
                        .as_i64()
                        .filter(|revision| (1..i64::MAX).contains(revision))
                        .ok_or_else(|| {
                            StorageValidationError::invalid(format!(
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
                return Err(StorageValidationError::invalid(format!(
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
                return Err(StorageValidationError::invalid(
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
