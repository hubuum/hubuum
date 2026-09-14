use super::*;
use crate::StorageBackupRow;
use chrono::NaiveDateTime;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};

pub(super) const FIELDS: &[&str] = &[
    "cancel_requested_at",
    "cancel_requested_by",
    "cancel_reason",
    "execution_deadline_at",
    "import_effects_committed_at",
    "remote_dispatched_at",
    "terminal_reason",
];

impl StorageTaskControl {
    /// Validate optional control metadata in a logical backup. Older backups
    /// omit these fields; any supplied values must satisfy the current invariants.
    pub fn from_snapshot(
        kind: StorageTaskKind,
        row: &StorageBackupRow,
    ) -> Result<Self, StorageValidationError> {
        let at = timestamp(row, "cancel_requested_at")?;
        let by = optional::<i32>(row, "cancel_requested_by")?
            .map(PrincipalId::new)
            .transpose()
            .map_err(invalid)?;
        let reason = optional::<String>(row, "cancel_reason")?
            .map(TaskCancellationReason::new)
            .transpose()
            .map_err(invalid)?;
        if at.is_none() && (by.is_some() || reason.is_some()) {
            return Err(invalid(
                "Cancellation metadata requires a request timestamp",
            ));
        }
        let cancellation = at.map(|at| StorageTaskCancellation::new(at, by, reason));
        let import = timestamp(row, "import_effects_committed_at")?;
        let remote = timestamp(row, "remote_dispatched_at")?;
        let phase = match (import, remote) {
            (Some(at), None) => StorageTaskExecutionPhase::ImportCommitted { at },
            (None, Some(at)) => StorageTaskExecutionPhase::RemoteDispatched { at },
            (None, None) => StorageTaskExecutionPhase::Uncommitted,
            _ => return Err(invalid("Task cannot have both import and remote effects")),
        };
        let terminal_reason = optional::<String>(row, "terminal_reason")?
            .map(|value| {
                TaskStopReason::from_persisted(&value)
                    .ok_or_else(|| invalid("Unknown terminal reason"))
            })
            .transpose()?;
        if terminal_reason.is_some()
            && row.get("status").and_then(Value::as_str) != Some("cancelled")
        {
            return Err(invalid(
                "Stop acknowledgement requires cancelled task status",
            ));
        }
        Self::try_new(
            kind,
            cancellation,
            timestamp(row, "execution_deadline_at")?,
            phase,
            terminal_reason,
        )
    }

    /// Backend-neutral logical fields used by backup capture and restore.
    pub fn snapshot_fields(&self) -> Map<String, Value> {
        let (import, remote) = match self.phase {
            StorageTaskExecutionPhase::Uncommitted => (None, None),
            StorageTaskExecutionPhase::ImportCommitted { at } => (Some(at), None),
            StorageTaskExecutionPhase::RemoteDispatched { at } => (None, Some(at)),
        };
        json!({
            "cancel_requested_at": self.cancellation.as_ref().map(StorageTaskCancellation::requested_at),
            "cancel_requested_by": self.cancellation.as_ref().and_then(StorageTaskCancellation::requested_by),
            "cancel_reason": self.cancellation.as_ref().and_then(StorageTaskCancellation::reason).map(TaskCancellationReason::as_str),
            "execution_deadline_at": self.deadline,
            "import_effects_committed_at": import, "remote_dispatched_at": remote,
            "terminal_reason": self.terminal_reason.map(TaskStopReason::as_str),
        }).as_object().expect("literal object").clone()
    }
}

fn optional<T: DeserializeOwned>(
    row: &StorageBackupRow,
    field: &str,
) -> Result<Option<T>, StorageValidationError> {
    row.get(field)
        .filter(|value| !value.is_null())
        .map(|value| {
            serde_json::from_value(value.clone())
                .map_err(|_| invalid(format!("Invalid task control field '{field}'")))
        })
        .transpose()
}
fn timestamp(
    row: &StorageBackupRow,
    field: &str,
) -> Result<Option<DateTime<Utc>>, StorageValidationError> {
    let Some(value) = row.get(field).filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    serde_json::from_value::<DateTime<Utc>>(value.clone())
        .or_else(|_| {
            serde_json::from_value::<NaiveDateTime>(value.clone()).map(|value| value.and_utc())
        })
        .map(Some)
        .map_err(|_| invalid(format!("Invalid finite task timestamp '{field}'")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[test]
    fn cancelled_control_roundtrips_through_logical_backup_fields() {
        let now = Utc::now();
        let mut control = StorageTaskControl::new(StorageTaskKind::RemoteCall);
        control.record_remote_dispatch(now).unwrap();
        control.request_cancellation(StorageTaskCancellation::new(
            now,
            Some(PrincipalId::new(7).unwrap()),
            Some(TaskCancellationReason::new("operator withdrawal").unwrap()),
        ));
        control.acknowledge_stop(now).unwrap();
        let mut fields = control.snapshot_fields();
        fields.insert("status".into(), json!("cancelled"));
        let row = StorageBackupRow::try_from_value(Value::Object(fields)).unwrap();
        assert_eq!(
            StorageTaskControl::from_snapshot(StorageTaskKind::RemoteCall, &row).unwrap(),
            control
        );
    }

    #[rstest]
    #[case::actor_without_intent(json!({"cancel_requested_by":7}))]
    #[case::invalid_reason(json!({"cancel_requested_at":"2026-09-14T00:00:00Z", "cancel_reason":"line\nbreak"}))]
    #[case::ack_without_deadline(json!({"status":"cancelled", "terminal_reason":"deadline_exceeded"}))]
    #[case::wrong_kind_effect(json!({"remote_dispatched_at":"2026-09-14T00:00:00Z"}))]
    #[case::not_terminal(json!({"status":"running", "terminal_reason":"cancel_requested", "cancel_requested_at":"2026-09-14T00:00:00Z"}))]
    fn backup_rejects_invalid_control_metadata(#[case] value: Value) {
        let row = StorageBackupRow::try_from_value(value).unwrap();
        assert!(StorageTaskControl::from_snapshot(StorageTaskKind::Import, &row).is_err());
    }
}
fn invalid(error: impl std::fmt::Display) -> StorageValidationError {
    StorageValidationError::invalid(error.to_string())
}
