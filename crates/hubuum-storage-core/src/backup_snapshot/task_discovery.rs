//! Canonical discovery facts derivable from retained artifacts in older backups.
use super::{
    StorageBackupHistorySection as Section, StorageBackupHistorySections, StorageBackupSnapshot,
};
use crate::{StorageTaskKind, StorageTaskMetadata, StorageValidationError};
use chrono::{DateTime, NaiveDateTime, Utc};
use serde_json::Value;

impl StorageBackupSnapshot {
    /// Older format-5 backups lack discovery metadata. Normalize only facts
    /// proved by retained output rows, consistently before restore and comparison.
    pub fn canonicalize_discovery_history(
        history: &mut StorageBackupHistorySections,
    ) -> Result<(), StorageValidationError> {
        let mut discovery = DiscoveryIndex::default();
        if let Some(rows) = history.get(&Section::ExportOutputs) {
            for row in rows {
                discovery.insert(row)?;
            }
        }
        if let Some(tasks) = history.get_mut(&Section::TerminalTasks) {
            for task in tasks {
                discovery.apply(task)?;
            }
        }
        Ok(())
    }
}
#[derive(Default)]
pub(super) struct DiscoveryIndex {
    outputs: std::collections::BTreeMap<i32, (i32, bool, DateTime<Utc>)>,
}
impl DiscoveryIndex {
    pub(super) fn insert(
        &mut self,
        row: &super::StorageBackupRow,
    ) -> Result<(), StorageValidationError> {
        let fields = row.fields();
        let integer = |name: &str| {
            fields
                .get(name)
                .and_then(Value::as_i64)
                .and_then(|value| i32::try_from(value).ok())
                .ok_or_else(|| invalid(name))
        };
        let task_id = integer("task_id")?;
        let warnings = integer("warning_count")?;
        let truncated = fields
            .get("truncated")
            .and_then(Value::as_bool)
            .ok_or_else(|| invalid("truncated"))?;
        let expiry = fields
            .get("output_expires_at")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("output_expires_at"))?;
        let expiry = DateTime::parse_from_rfc3339(expiry)
            .map(|at| at.with_timezone(&Utc))
            .or_else(|_| expiry.parse::<NaiveDateTime>().map(|at| at.and_utc()))
            .map_err(|_| invalid("output_expires_at"))?;
        if self
            .outputs
            .insert(task_id, (warnings, truncated, expiry))
            .is_some()
        {
            return Err(invalid("duplicate export output task_id"));
        }
        Ok(())
    }
    pub(super) fn apply(
        &self,
        task: &mut super::StorageBackupRow,
    ) -> Result<(), StorageValidationError> {
        if task.0.get("kind").and_then(Value::as_str) != Some("export") {
            return Ok(());
        }
        let Some(id) = task
            .0
            .get("id")
            .and_then(Value::as_i64)
            .and_then(|id| i32::try_from(id).ok())
        else {
            return Ok(());
        };
        let Some((warnings, truncated, expiry)) = self.outputs.get(&id) else {
            return Ok(());
        };
        let mut metadata = match task
            .0
            .get("discovery_metadata")
            .filter(|value| !value.is_null())
        {
            Some(value) => {
                StorageTaskMetadata::from_persisted(StorageTaskKind::Export, value.clone())?
            }
            None => StorageTaskMetadata::unknown(StorageTaskKind::Export),
        };
        metadata.record_export(*warnings, *truncated, *expiry)?;
        task.0
            .insert("discovery_metadata".into(), metadata.to_value());
        Ok(())
    }
}

fn invalid(field: &str) -> StorageValidationError {
    StorageValidationError::invalid(format!("Invalid retained export discovery field '{field}'"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StorageBackupRow;
    use serde_json::json;
    #[test]
    fn old_backup_artifacts_prove_outcomes_without_inventing_targets() {
        let mut history = StorageBackupHistorySections::from([
            (Section::TerminalTasks,vec![StorageBackupRow::try_from_value(json!({"id":7,"kind":"export"})).unwrap()]),
            (Section::ExportOutputs,vec![StorageBackupRow::try_from_value(json!({"task_id":7,"warning_count":0,"truncated":false,"output_expires_at":"2026-09-18T00:00:00Z"})).unwrap()]),
        ]);
        StorageBackupSnapshot::canonicalize_discovery_history(&mut history).unwrap();
        let value = history[&Section::TerminalTasks][0].fields()["discovery_metadata"].clone();
        let metadata = StorageTaskMetadata::from_persisted(StorageTaskKind::Export, value).unwrap();
        assert!(metadata.details().target().is_none());
        assert_eq!(metadata.to_value()["data"]["warning_count"], 0);
        assert_eq!(metadata.to_value()["data"]["output"]["state"], "produced");
    }
}
