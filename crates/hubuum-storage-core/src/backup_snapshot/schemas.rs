use std::collections::BTreeMap;

use serde_json::Value;

use crate::{
    StorageBackupSnapshot, StorageBackupStateSection as Section, StorageSchemaRevision,
    StorageSchemaRevisionStatus, StorageValidationError,
};

fn invalid() -> StorageValidationError {
    StorageValidationError::invalid(
        "Backup schema revision, active projection or object evidence is inconsistent",
    )
}
fn integer(value: Option<&Value>) -> Result<i64, StorageValidationError> {
    value.and_then(Value::as_i64).ok_or_else(invalid)
}

pub(super) fn validate(snapshot: &StorageBackupSnapshot) -> Result<(), StorageValidationError> {
    let sections = &snapshot.state_sections;
    let classes = sections[&Section::Classes]
        .iter()
        .map(|row| Ok((integer(row.get("id"))?, row)))
        .collect::<Result<BTreeMap<_, _>, StorageValidationError>>()?;
    let objects = sections[&Section::Objects]
        .iter()
        .map(|row| Ok((integer(row.get("id"))?, row)))
        .collect::<Result<BTreeMap<_, _>, StorageValidationError>>()?;
    let mut object_counts = BTreeMap::<i64, i64>::new();
    for object in objects.values() {
        if let Some(class_id) = object.get("class_id").and_then(Value::as_i64) {
            *object_counts.entry(class_id).or_default() += 1;
        }
    }
    let mut revisions = BTreeMap::new();
    for row in &sections[&Section::ClassSchemaRevisions] {
        let revision = StorageSchemaRevision::from_snapshot(row.clone().into_value())?;
        let key = (
            i64::from(revision.reference().class_id().id()),
            revision.reference().revision().get(),
        );
        if !classes.contains_key(&key.0) || revisions.insert(key, revision).is_some() {
            return Err(invalid());
        }
    }
    let mut states = BTreeMap::new();
    for state in &sections[&Section::ClassSchemaState] {
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
            || class.get("validate_schema").and_then(Value::as_bool)
                != Some(policy.validates_schema())
            || class.get("json_schema").filter(|value| !value.is_null())
                != policy.json_schema().filter(|value| !value.is_null())
        {
            return Err(invalid());
        }
    }
    if states.len() != classes.len() {
        return Err(invalid());
    }
    let mut evidence_ids = std::collections::BTreeSet::new();
    for row in &sections[&Section::ObjectSchemaEvidence] {
        let object_id = integer(row.get("object_id"))?;
        let class_id = integer(row.get("class_id"))?;
        let revision = integer(row.get("schema_revision"))?;
        let inspected = integer(row.get("object_revision"))?;
        let object = objects.get(&object_id).ok_or_else(invalid)?;
        let schema = revisions.get(&(class_id, revision)).ok_or_else(invalid)?;
        let current = integer(object.get("revision"))?;
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
            || class_id != integer(object.get("class_id"))?
            || inspected <= 0
            || inspected > current
        {
            return Err(invalid());
        }
        if inspected == current
            && valid
            && schema
                .policy()
                .inspect(object.get("data").ok_or_else(invalid)?)
                != crate::StorageComplianceStatus::Valid
        {
            return Err(invalid());
        }
    }
    if let Some(history) = &snapshot.history_sections {
        let mut ids = std::collections::BTreeSet::new();
        let mut documents = BTreeMap::new();
        for row in &history[&crate::StorageBackupHistorySection::ClassSchemaHistory] {
            let id = integer(row.get("id"))?;
            let reference = (integer(row.get("class_id"))?, integer(row.get("revision"))?);
            let revision = StorageSchemaRevision::from_snapshot(
                row.get("snapshot").cloned().ok_or_else(invalid)?,
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
