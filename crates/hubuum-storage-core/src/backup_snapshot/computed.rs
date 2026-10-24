use super::view::{BackupView, ReadError};
use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use hubuum_domain::{ClassId, HistoryRecordId};
use serde_json::{Value, json};

use super::{
    StorageBackupHistorySection as History, StorageBackupRow, StorageBackupStateSection as State,
};
use crate::{StorageComputedFieldHistory, StorageHistoricalComputedField};

fn invalid(message: impl Into<String>) -> ReadError {
    ReadError::invalid(message)
}

fn shared_definitions(
    snapshot: &impl BackupView,
) -> Result<BTreeMap<ClassId, Vec<StorageHistoricalComputedField>>, ReadError> {
    let mut result: BTreeMap<ClassId, Vec<StorageHistoricalComputedField>> = BTreeMap::new();
    for row in snapshot.state_rows(State::ComputedFieldDefinitions)? {
        let row = row?;
        if row.get("visibility").and_then(Value::as_str) != Some("shared") {
            continue;
        }
        let definition: StorageHistoricalComputedField = serde_json::from_value(json!({
            "id": row.get("id"), "class_id": row.get("class_id"), "revision": row.get("revision"),
            "definition": {
                "key": row.get("key"), "label": row.get("label"), "description": row.get("description"),
                "operation": row.get("operation"), "result_type": row.get("result_type"),
                "enabled": row.get("enabled"), "semantics_version": row.get("semantics_version")
            }
        })).map_err(|error| invalid(format!("Invalid shared computed definition: {error}")))?;
        result
            .entry(definition.class_id())
            .or_default()
            .push(definition);
    }
    for definitions in result.values_mut() {
        definitions.sort_by_key(StorageHistoricalComputedField::id);
    }
    Ok(result)
}

fn records(snapshot: &impl BackupView) -> Result<Vec<StorageComputedFieldHistory>, ReadError> {
    snapshot
        .history_rows(History::ComputedFieldHistory)?
        .iter()
        .map(|row| {
            serde_json::from_value(row?.into_owned().into_value())
                .map_err(|error| invalid(format!("Invalid computed definition history: {error}")))
        })
        .collect()
}

pub(super) fn validate(snapshot: &impl BackupView) -> Result<(), ReadError> {
    let definitions = shared_definitions(snapshot)?;
    let mut classes = BTreeSet::new();
    for row in snapshot.state_rows(State::Classes)? {
        if let Some(id) = row?.get("id").and_then(Value::as_i64) {
            classes.insert(id);
        }
    }
    for (&class_id, fields) in &definitions {
        if !classes.contains(&i64::from(class_id.id())) {
            return Err(invalid(
                "Shared computed definition refers to a missing class",
            ));
        }
        // Reuse the complete-set invariant even for history-free documents.
        StorageComputedFieldHistory::builder(
            class_id,
            HistoryRecordId::new(1).unwrap(),
            DateTime::UNIX_EPOCH,
        )
        .definitions(fields.clone())
        .build()?;
    }
    if classes.iter().any(|id| {
        i32::try_from(*id)
            .ok()
            .and_then(|id| ClassId::new(id).ok())
            .is_none()
    }) {
        return Err(invalid(
            "Invalid class identity in computed history baseline",
        ));
    }
    let mut records = records(snapshot)?;
    let remaining =
        i64::MAX - i64::try_from(classes.len()).map_err(|_| invalid("Too many classes"))?;
    if records
        .iter()
        .any(|record| record.history_entry_id().id() >= remaining)
    {
        return Err(invalid(
            "Computed history identity cannot advance for restore baselines",
        ));
    }
    records.sort_by_key(|record| (record.class_id(), record.valid_from()));
    let mut ids = BTreeSet::new();
    let mut previous: Option<&StorageComputedFieldHistory> = None;
    for record in &records {
        if !ids.insert(record.history_entry_id()) {
            return Err(invalid("Duplicate computed history identity"));
        }
        if let Some(previous) = previous.filter(|previous| previous.class_id() == record.class_id())
            && previous
                .valid_to()
                .is_none_or(|end| end > record.valid_from())
        {
            return Err(invalid("Overlapping computed definition history intervals"));
        }
        if record.valid_to().is_none() {
            let expected = definitions
                .get(&record.class_id())
                .map_or(&[][..], Vec::as_slice);
            if !classes.contains(&i64::from(record.class_id().id()))
                || record.definitions() != expected
            {
                return Err(invalid(
                    "Open computed definition history does not match current shared definitions",
                ));
            }
        }
        previous = Some(record);
    }
    Ok(())
}

pub(super) fn missing_baselines(
    snapshot: &impl BackupView,
    restored_at: DateTime<Utc>,
) -> Result<Vec<StorageBackupRow>, ReadError> {
    let definitions = shared_definitions(snapshot)?;
    let records = records(snapshot)?;
    let mut next_id = records
        .iter()
        .map(|record| record.history_entry_id().id())
        .max()
        .unwrap_or(0);
    let mut intervals: BTreeMap<ClassId, (bool, Option<DateTime<Utc>>)> = BTreeMap::new();
    for record in &records {
        let (open, end) = intervals.entry(record.class_id()).or_default();
        *open |= record.valid_to().is_none();
        *end = (*end).max(record.valid_to());
    }
    let restored_at =
        DateTime::from_timestamp_micros(restored_at.timestamp_micros()).expect("valid timestamp");
    let mut classes = Vec::new();
    for row in snapshot.state_rows(State::Classes)? {
        if let Some(id) = row?.get("id").and_then(Value::as_i64) {
            classes.push(id);
        }
    }
    classes.sort_unstable();
    let mut baselines = Vec::new();
    for id in classes {
        let class_id = ClassId::new(i32::try_from(id).expect("validated class identity"))
            .expect("validated class identity");
        let (open, last_end) = intervals.get(&class_id).copied().unwrap_or_default();
        if open {
            continue;
        }
        next_id = next_id
            .checked_add(1)
            .expect("validated history identity has room for baselines");
        let boundary = last_end.unwrap_or(restored_at).max(restored_at);
        let record = StorageComputedFieldHistory::builder(
            class_id,
            HistoryRecordId::new(next_id).unwrap(),
            boundary,
        )
        .definitions(definitions.get(&class_id).cloned().unwrap_or_default())
        .build()
        .expect("validated shared definition set");
        baselines.push(
            StorageBackupRow::try_from_value(
                serde_json::to_value(record).expect("history serializes"),
            )
            .expect("history is an object"),
        );
    }
    Ok(baselines)
}

#[cfg(test)]
mod tests;
