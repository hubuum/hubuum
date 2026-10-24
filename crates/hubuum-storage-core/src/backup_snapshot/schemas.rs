use super::view::{BackupView, ReadError};
use std::collections::BTreeMap;

use serde_json::Value;

use crate::{
    StorageBackupStateSection as Section, StorageSchemaRevision, StorageSchemaRevisionStatus,
};

fn invalid() -> ReadError {
    ReadError::invalid(
        "Backup schema revision, active projection or object evidence is inconsistent",
    )
}
fn integer(value: Option<&Value>) -> Result<i64, ReadError> {
    value.and_then(Value::as_i64).ok_or_else(invalid)
}

pub(super) fn validate(snapshot: &impl BackupView) -> Result<(), ReadError> {
    let mut classes = BTreeMap::new();
    for row in snapshot.state_rows(Section::Classes)? {
        let row = row?;
        classes.insert(
            integer(row.get("id"))?,
            (
                row.get("validate_schema").and_then(Value::as_bool),
                row.get("json_schema")
                    .filter(|value| !value.is_null())
                    .cloned(),
            ),
        );
    }
    // Object payloads stay in the replay source. Only identity, placement and
    // revision are needed to resolve evidence and class counts.
    let mut objects = BTreeMap::new();
    for row in snapshot.state_rows(Section::Objects)? {
        let row = row?;
        objects.insert(
            integer(row.get("id"))?,
            (
                row.get("class_id").and_then(Value::as_i64),
                row.get("revision").and_then(Value::as_i64),
            ),
        );
    }
    let mut object_counts = BTreeMap::<i64, i64>::new();
    for (class_id, _) in objects.values() {
        if let Some(class_id) = class_id {
            *object_counts.entry(*class_id).or_default() += 1;
        }
    }
    let mut revisions = BTreeMap::new();
    for row in snapshot.state_rows(Section::ClassSchemaRevisions)? {
        let row = row?;
        let revision = StorageSchemaRevision::from_snapshot_with_limits(
            row.into_owned().into_value(),
            snapshot.schema_limits(),
        )?;
        let key = (
            i64::from(revision.reference().class_id().id()),
            revision.reference().revision().get(),
        );
        if !classes.contains_key(&key.0) || revisions.insert(key, revision).is_some() {
            return Err(invalid());
        }
    }
    let mut states = BTreeMap::new();
    for state in snapshot.state_rows(Section::ClassSchemaState)? {
        let state = state?;
        let id = integer(state.get("class_id"))?;
        let active = integer(state.get("active_revision"))?;
        let last = integer(state.get("last_revision"))?;
        let epoch = integer(state.get("object_epoch"))?;
        let class = classes.get(&id).ok_or_else(invalid)?;
        let revision = revisions.get(&(id, active)).ok_or_else(invalid)?;
        let policy = revision.policy().policy();
        if integer(state.get("object_count"))? != object_counts.get(&id).copied().unwrap_or(0)
            || epoch < 0
            || states.insert(id, active).is_some()
            || revision.status() != StorageSchemaRevisionStatus::Active
            || revisions
                .range((id, 0)..=(id, i64::MAX))
                .any(|((_, number), _)| *number > last)
            || revisions
                .range((id, 0)..=(id, i64::MAX))
                .filter(|(_, revision)| revision.status() == StorageSchemaRevisionStatus::Active)
                .count()
                != 1
            || class.0 != Some(policy.validates_schema())
            || class.1.as_ref() != policy.json_schema().filter(|value| !value.is_null())
        {
            return Err(invalid());
        }
    }
    if states.len() != classes.len() {
        return Err(invalid());
    }
    let mut payload_checks = BTreeMap::new();
    let mut evidence_ids = std::collections::BTreeSet::new();
    for row in snapshot.state_rows(Section::ObjectSchemaEvidence)? {
        let row = row?;
        let object_id = integer(row.get("object_id"))?;
        let class_id = integer(row.get("class_id"))?;
        let revision = integer(row.get("schema_revision"))?;
        let inspected = integer(row.get("object_revision"))?;
        let object = objects.get(&object_id).ok_or_else(invalid)?;
        revisions.get(&(class_id, revision)).ok_or_else(invalid)?;
        let current = object.1.ok_or_else(invalid)?;
        let valid = row
            .get("valid")
            .and_then(Value::as_bool)
            .ok_or_else(invalid)?;
        let timestamp = row
            .get("validated_at")
            .and_then(Value::as_str)
            .ok_or_else(invalid)?;
        if chrono::DateTime::parse_from_rfc3339(timestamp).is_err()
            || !evidence_ids.insert(object_id)
            || Some(class_id) != object.0
            || inspected <= 0
            || inspected > current
        {
            return Err(invalid());
        }
        if inspected == current && valid {
            payload_checks.insert(object_id, (class_id, revision));
        }
    }
    if !payload_checks.is_empty() {
        for object in snapshot.state_rows(Section::Objects)? {
            let object = object?;
            if let Some(reference) = payload_checks.get(&integer(object.get("id"))?)
                && revisions[reference]
                    .policy()
                    .inspect(object.get("data").ok_or_else(invalid)?)
                    != crate::StorageComplianceStatus::Valid
            {
                return Err(invalid());
            }
        }
    }
    if snapshot.includes_history() {
        let mut ids = std::collections::BTreeSet::new();
        let mut documents = BTreeMap::new();
        for row in snapshot.history_rows(crate::StorageBackupHistorySection::ClassSchemaHistory)? {
            let row = row?;
            let id = integer(row.get("id"))?;
            let reference = (integer(row.get("class_id"))?, integer(row.get("revision"))?);
            let revision = StorageSchemaRevision::from_snapshot_with_limits(
                row.get("snapshot").cloned().ok_or_else(invalid)?,
                snapshot.schema_limits(),
            )?;
            let timestamp = row
                .get("occurred_at")
                .and_then(Value::as_str)
                .ok_or_else(invalid)?;
            if id <= 0
                || !ids.insert(id)
                || reference
                    != (
                        i64::from(revision.reference().class_id().id()),
                        revision.reference().revision().get(),
                    )
                || !matches!(
                    row.get("operation").and_then(Value::as_str),
                    Some("create" | "update" | "delete")
                )
                || chrono::DateTime::parse_from_rfc3339(timestamp).is_err()
            {
                return Err(invalid());
            }
            let document = (
                revision.policy().policy().clone(),
                revision.created_at(),
                revision.created_by(),
            );
            if documents
                .insert(reference, document.clone())
                .is_some_and(|previous| previous != document)
            {
                return Err(invalid());
            }
            if let Some(live) = revisions.get(&reference)
                && (live.policy().policy(), live.created_at(), live.created_by())
                    != (&document.0, document.1, document.2)
            {
                return Err(invalid());
            }
        }
    }
    Ok(())
}
