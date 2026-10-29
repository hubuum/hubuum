//! Bounded continuation data and fenced, durable task waits.
//!
//! Checkpoints are adapter-owned state, never public task payloads. A worker
//! must recognize their format before continuing; an unknown format is not
//! permission to restart the original operation.

use std::fmt;

use hubuum_budget::{ByteLimit, LimitedWriter};
use hubuum_domain::TaskId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{StorageTask, StorageTaskLease, StorageTaskStatus, StorageValidationError};

/// A versioned, bounded checkpoint whose encoded bytes count as resident input.
#[derive(Clone, PartialEq)]
pub struct StorageTaskCheckpoint {
    format: String,
    version: u32,
    data: Value,
    encoded: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointWire<T> {
    format: String,
    version: u32,
    data: T,
}

impl StorageTaskCheckpoint {
    pub const MAX_BYTES: usize = 64 * 1024;

    pub fn try_new(
        format: impl Into<String>,
        version: u32,
        data: Value,
    ) -> Result<Self, StorageValidationError> {
        let format = format.into();
        if format.is_empty()
            || format.len() > 64
            || !format.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'.' | b'_' | b'-')
            })
            || !format.as_bytes()[0].is_ascii_alphanumeric()
        {
            return Err(StorageValidationError::invalid(
                "Checkpoint format must contain 1 to 64 lowercase ASCII letters, digits, dots, underscores or hyphens and start with a letter or digit",
            ));
        }
        if version == 0 {
            return Err(StorageValidationError::invalid(
                "Checkpoint version must be positive",
            ));
        }
        if !data.is_object() {
            return Err(StorageValidationError::invalid(
                "Checkpoint data must be a JSON object",
            ));
        }
        let mut writer = LimitedWriter::new(Vec::new(), ByteLimit::new(Self::MAX_BYTES));
        serde_json::to_writer(
            &mut writer,
            &CheckpointWire {
                format: format.clone(),
                version,
                data: &data,
            },
        )
        .map_err(|_| StorageValidationError::invalid("Encoded checkpoint exceeds 65536 bytes"))?;
        let encoded = String::from_utf8(writer.into_inner()).map_err(|_| {
            StorageValidationError::invalid("Encoded checkpoint must contain valid UTF-8")
        })?;
        Ok(Self {
            format,
            version,
            data,
            encoded,
        })
    }

    /// Validate persisted bytes once at an adapter boundary.
    pub fn from_persisted(encoded: &str) -> Result<Self, StorageValidationError> {
        if encoded.len() > Self::MAX_BYTES {
            return Err(StorageValidationError::invalid(
                "Encoded checkpoint exceeds 65536 bytes",
            ));
        }
        let wire: CheckpointWire<Value> = serde_json::from_str(encoded)
            .map_err(|_| StorageValidationError::invalid("Invalid persisted task checkpoint"))?;
        Self::try_new(wire.format, wire.version, wire.data)
    }

    #[must_use]
    pub fn format(&self) -> &str {
        &self.format
    }

    #[must_use]
    pub const fn version(&self) -> u32 {
        self.version
    }

    #[must_use]
    pub const fn data(&self) -> &Value {
        &self.data
    }

    #[must_use]
    pub fn encoded(&self) -> &str {
        &self.encoded
    }

    #[must_use]
    pub fn byte_size(&self) -> u64 {
        self.encoded.len() as u64
    }
}

impl fmt::Debug for StorageTaskCheckpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageTaskCheckpoint")
            .field("version", &self.version)
            .field("byte_size", &self.byte_size())
            .field("format_and_data", &"[redacted]")
            .finish()
    }
}

/// Opaque identity of one wait. Adapters mint a fresh token for every wait.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageTaskWaitToken(String);

impl StorageTaskWaitToken {
    pub fn try_new(value: impl Into<String>) -> Result<Self, StorageValidationError> {
        let parsed = Uuid::parse_str(&value.into())
            .ok()
            .filter(|value| !value.is_nil())
            .ok_or_else(|| {
                StorageValidationError::invalid("Task wait token must be a non-nil UUID")
            })?;
        Ok(Self(parsed.to_string()))
    }

    #[must_use]
    pub fn adapter_value(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for StorageTaskWaitToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StorageTaskWaitToken([redacted])")
    }
}

/// Atomically relinquish a live execution lease while retaining admission.
#[derive(Clone, Debug)]
pub struct StorageTaskWaitRequest {
    lease: StorageTaskLease,
    checkpoint: StorageTaskCheckpoint,
}

impl StorageTaskWaitRequest {
    #[must_use]
    pub const fn new(lease: StorageTaskLease, checkpoint: StorageTaskCheckpoint) -> Self {
        Self { lease, checkpoint }
    }
    #[must_use]
    pub const fn lease(&self) -> &StorageTaskLease {
        &self.lease
    }
    #[must_use]
    pub const fn checkpoint(&self) -> &StorageTaskCheckpoint {
        &self.checkpoint
    }
    #[must_use]
    pub fn into_parts(self) -> (StorageTaskLease, StorageTaskCheckpoint) {
        (self.lease, self.checkpoint)
    }
}

/// A committed wait and the token required to wake that exact continuation.
#[derive(Clone, Debug, PartialEq)]
pub struct StorageTaskWait {
    task: StorageTask,
    token: StorageTaskWaitToken,
}

impl StorageTaskWait {
    pub fn try_new(
        task: StorageTask,
        token: StorageTaskWaitToken,
    ) -> Result<Self, StorageValidationError> {
        if task.status() != StorageTaskStatus::Waiting || task.checkpoint().is_none() {
            return Err(StorageValidationError::invalid(
                "Task wait requires a waiting task with a checkpoint",
            ));
        }
        Ok(Self { task, token })
    }
    #[must_use]
    pub const fn task(&self) -> &StorageTask {
        &self.task
    }
    #[must_use]
    pub const fn token(&self) -> &StorageTaskWaitToken {
        &self.token
    }
    #[must_use]
    pub fn into_parts(self) -> (StorageTask, StorageTaskWaitToken) {
        (self.task, self.token)
    }
}

/// Wake an exact wait. This does not grant execution authority or a new lease.
#[derive(Clone, Debug)]
pub struct StorageTaskResumeRequest {
    task_id: TaskId,
    token: StorageTaskWaitToken,
}

impl StorageTaskResumeRequest {
    #[must_use]
    pub const fn new(task_id: TaskId, token: StorageTaskWaitToken) -> Self {
        Self { task_id, token }
    }
    #[must_use]
    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }
    #[must_use]
    pub const fn token(&self) -> &StorageTaskWaitToken {
        &self.token
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum StorageTaskResumeOutcome {
    /// The same task is queued; only a subsequent normal claim can execute it.
    Queued(StorageTask),
    /// A durable cancellation or the original deadline won the wake-up race.
    Stopped(StorageTask),
    /// Missing, stale or already consumed notification; nothing was changed.
    Unchanged,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use serde_json::json;

    #[rstest]
    #[case("")]
    #[case("Format")]
    #[case(".format")]
    #[case("format/type")]
    #[case("format space")]
    #[case("é")]
    #[case("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")]
    fn checkpoint_rejects_invalid_format(#[case] format: &str) {
        assert!(StorageTaskCheckpoint::try_new(format, 1, json!({})).is_err());
    }

    #[rstest]
    #[case(0, json!({}))]
    #[case(1, json!([]))]
    #[case(1, json!(null))]
    fn checkpoint_rejects_invalid_structure(#[case] version: u32, #[case] data: Value) {
        assert!(StorageTaskCheckpoint::try_new("export.v1", version, data).is_err());
    }

    #[rstest]
    #[case(0, true)]
    #[case(1, false)]
    fn checkpoint_enforces_encoded_byte_limit(#[case] excess: usize, #[case] accepted: bool) {
        let empty = StorageTaskCheckpoint::try_new("export", 1, json!({"value": ""})).unwrap();
        let count = StorageTaskCheckpoint::MAX_BYTES - empty.byte_size() as usize + excess;
        let result =
            StorageTaskCheckpoint::try_new("export", 1, json!({"value": "x".repeat(count)}));
        assert_eq!(result.is_ok(), accepted);
    }

    #[test]
    fn checkpoint_round_trip_preserves_the_validated_representation() {
        let checkpoint =
            StorageTaskCheckpoint::try_new("backup.v1", 2, json!({"cursor": 42})).unwrap();
        assert_eq!(
            StorageTaskCheckpoint::from_persisted(checkpoint.encoded()).unwrap(),
            checkpoint
        );
    }

    #[test]
    fn persisted_checkpoint_rejects_unknown_fields() {
        assert!(
            StorageTaskCheckpoint::from_persisted(
                r#"{"format":"export","version":1,"data":{},"extra":true}"#
            )
            .is_err()
        );
    }

    #[test]
    fn checkpoint_debug_redacts_sensitive_state() {
        let checkpoint =
            StorageTaskCheckpoint::try_new("secret-format", 1, json!({"secret": "secret-data"}))
                .unwrap();
        let debug = format!("{checkpoint:?}");
        assert!(!debug.contains("secret"));
    }

    #[rstest]
    #[case("")]
    #[case("not-a-uuid")]
    #[case("00000000-0000-0000-0000-000000000000")]
    fn wait_token_rejects_invalid_identity(#[case] token: &str) {
        assert!(StorageTaskWaitToken::try_new(token).is_err());
    }

    #[test]
    fn wait_token_debug_redacts_identity() {
        let token = StorageTaskWaitToken::try_new("20000000-0000-4000-8000-000000000001").unwrap();
        assert!(!format!("{token:?}").contains(token.adapter_value()));
    }

    #[rstest]
    #[case(StorageTaskStatus::Queued)]
    #[case(StorageTaskStatus::Waiting)]
    #[case(StorageTaskStatus::Running)]
    fn continuation_preserves_valid_lifecycle_states(#[case] status: StorageTaskStatus) {
        let now = chrono::Utc::now();
        let mut control = crate::StorageTaskControl::new(crate::StorageTaskKind::Export);
        control.admit_deadline(now + chrono::Duration::minutes(5));
        let task = StorageTask::builder(
            TaskId::new(1).unwrap(),
            crate::StorageTaskKind::Export,
            status,
            now,
            now,
        )
        .checkpoint(Some(
            StorageTaskCheckpoint::try_new("export", 1, json!({})).unwrap(),
        ))
        .started_at(Some(now))
        .attempt_count(1)
        .control(control)
        .lease_expires_at(
            status
                .is_active()
                .then_some(now + chrono::Duration::minutes(1)),
        )
        .try_build()
        .unwrap();
        assert_eq!(task.status(), status);
        assert!(!StorageTaskStatus::Waiting.is_active());
        assert!(!StorageTaskStatus::Waiting.is_terminal());
    }

    #[rstest]
    #[case(StorageTaskStatus::Waiting, false, true, 1, true, false)]
    #[case(StorageTaskStatus::Waiting, true, false, 1, true, false)]
    #[case(StorageTaskStatus::Waiting, true, true, 0, true, false)]
    #[case(StorageTaskStatus::Waiting, true, true, 1, false, false)]
    #[case(StorageTaskStatus::Waiting, true, true, 1, true, true)]
    #[case(StorageTaskStatus::Succeeded, true, true, 1, true, false)]
    fn continuation_rejects_inconsistent_lifecycle(
        #[case] status: StorageTaskStatus,
        #[case] checkpoint: bool,
        #[case] started: bool,
        #[case] attempts: i32,
        #[case] deadline: bool,
        #[case] lease: bool,
    ) {
        let now = chrono::Utc::now();
        let mut control = crate::StorageTaskControl::new(crate::StorageTaskKind::Export);
        if deadline {
            control.admit_deadline(now + chrono::Duration::minutes(5));
        }
        let result = StorageTask::builder(
            TaskId::new(1).unwrap(),
            crate::StorageTaskKind::Export,
            status,
            now,
            now,
        )
        .checkpoint(
            checkpoint.then(|| StorageTaskCheckpoint::try_new("export", 1, json!({})).unwrap()),
        )
        .started_at(started.then_some(now))
        .finished_at(status.is_terminal().then_some(now))
        .attempt_count(attempts)
        .control(control)
        .lease_expires_at(lease.then_some(now + chrono::Duration::minutes(1)))
        .try_build();
        assert!(result.is_err());
    }
}
