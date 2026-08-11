use std::fmt;

use async_trait::async_trait;
use chrono::NaiveDateTime;
use serde_json::Value;

use crate::{StorageError, StorageTask, StorageTaskDurations, StorageTaskStatus};

/// Validated lease duration shared with a storage adapter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageTaskLeaseDuration {
    milliseconds: i64,
}

impl StorageTaskLeaseDuration {
    #[must_use]
    pub const fn from_milliseconds(milliseconds: i64) -> Option<Self> {
        if milliseconds > 0 {
            Some(Self { milliseconds })
        } else {
            None
        }
    }

    #[must_use]
    pub const fn milliseconds(self) -> i64 {
        self.milliseconds
    }
}

/// Backend-owned claim token that application code only carries between calls.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageTaskClaimToken(String);

impl StorageTaskClaimToken {
    /// Construct an opaque token at the adapter boundary.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Read the opaque value inside a storage adapter.
    #[must_use]
    pub fn adapter_value(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for StorageTaskClaimToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StorageTaskClaimToken([redacted])")
    }
}

/// Proof that a worker owns a task until the backend-managed lease expires.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageTaskLease {
    task_id: i32,
    token: StorageTaskClaimToken,
}

impl StorageTaskLease {
    #[must_use]
    pub const fn new(task_id: i32, token: StorageTaskClaimToken) -> Self {
        Self { task_id, token }
    }

    #[must_use]
    pub const fn task_id(&self) -> i32 {
        self.task_id
    }

    #[must_use]
    pub const fn token(&self) -> &StorageTaskClaimToken {
        &self.token
    }
}

impl fmt::Debug for StorageTaskLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageTaskLease")
            .field("task_id", &self.task_id)
            .field("token", &self.token)
            .finish()
    }
}

/// A claimed task and its opaque ownership proof.
#[derive(Clone, PartialEq)]
pub struct StorageTaskClaim {
    task: StorageTask,
    lease: StorageTaskLease,
}

impl StorageTaskClaim {
    #[must_use]
    pub const fn new(task: StorageTask, lease: StorageTaskLease) -> Self {
        Self { task, lease }
    }

    #[must_use]
    pub const fn task(&self) -> &StorageTask {
        &self.task
    }

    #[must_use]
    pub const fn lease(&self) -> &StorageTaskLease {
        &self.lease
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageTask, StorageTaskLease) {
        (self.task, self.lease)
    }
}

impl fmt::Debug for StorageTaskClaim {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageTaskClaim")
            .field("task", &self.task)
            .field("lease", &self.lease)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageTaskResultCounts {
    processed: i32,
    succeeded: i32,
    failed: i32,
}

impl StorageTaskResultCounts {
    #[must_use]
    pub const fn new(processed: i32, succeeded: i32, failed: i32) -> Self {
        Self {
            processed,
            succeeded,
            failed,
        }
    }

    #[must_use]
    pub const fn processed(self) -> i32 {
        self.processed
    }

    #[must_use]
    pub const fn succeeded(self) -> i32 {
        self.succeeded
    }

    #[must_use]
    pub const fn failed(self) -> i32 {
        self.failed
    }
}

/// Lifecycle event emitted while a worker owns a task.
#[derive(Clone, Debug, PartialEq)]
pub struct StorageTaskEventInput {
    event_type: String,
    message: String,
    data: Option<Value>,
}

impl StorageTaskEventInput {
    #[must_use]
    pub fn new(event_type: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            event_type: event_type.into(),
            message: message.into(),
            data: None,
        }
    }

    #[must_use]
    pub fn with_data(mut self, data: Option<Value>) -> Self {
        self.data = data;
        self
    }

    #[must_use]
    pub fn event_type(&self) -> &str {
        &self.event_type
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    #[must_use]
    pub const fn data(&self) -> Option<&Value> {
        self.data.as_ref()
    }

    #[must_use]
    pub fn into_parts(self) -> (String, String, Option<Value>) {
        (self.event_type, self.message, self.data)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StorageTaskEventAppend {
    lease: StorageTaskLease,
    event: StorageTaskEventInput,
}

impl StorageTaskEventAppend {
    #[must_use]
    pub const fn new(lease: StorageTaskLease, event: StorageTaskEventInput) -> Self {
        Self { lease, event }
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageTaskLease, StorageTaskEventInput) {
        (self.lease, self.event)
    }
}

/// Non-terminal or terminal state values supplied by task execution.
#[derive(Clone, Debug, PartialEq)]
pub struct StorageTaskStateUpdate {
    lease: StorageTaskLease,
    status: StorageTaskStatus,
    summary: Option<String>,
    counts: StorageTaskResultCounts,
    started_at: Option<NaiveDateTime>,
}

impl StorageTaskStateUpdate {
    #[must_use]
    pub const fn new(
        lease: StorageTaskLease,
        status: StorageTaskStatus,
        counts: StorageTaskResultCounts,
    ) -> Self {
        Self {
            lease,
            status,
            summary: None,
            counts,
            started_at: None,
        }
    }

    #[must_use]
    pub fn summary(mut self, summary: Option<String>) -> Self {
        self.summary = summary;
        self
    }

    #[must_use]
    pub const fn started_at(mut self, started_at: Option<NaiveDateTime>) -> Self {
        self.started_at = started_at;
        self
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        StorageTaskLease,
        StorageTaskStatus,
        Option<String>,
        StorageTaskResultCounts,
        Option<NaiveDateTime>,
    ) {
        (
            self.lease,
            self.status,
            self.summary,
            self.counts,
            self.started_at,
        )
    }
}

/// Export artifact stored atomically with the terminal task transition.
#[derive(Clone, PartialEq)]
pub struct StorageExportTaskArtifact {
    template_name: Option<String>,
    content_type: String,
    json_output: Option<Value>,
    text_output: Option<String>,
    metadata: Value,
    warnings: Value,
    warning_count: i32,
    truncated: bool,
    output_expires_at: NaiveDateTime,
    durations: StorageTaskDurations,
}

/// Template identity and response media type for a persisted export artifact.
pub struct StorageExportTaskArtifactIdentity {
    template_name: Option<String>,
    content_type: String,
}

impl StorageExportTaskArtifactIdentity {
    #[must_use]
    pub fn into_parts(self) -> (Option<String>, String) {
        (self.template_name, self.content_type)
    }
}

/// Mutually exclusive structured or rendered content for an export artifact.
pub struct StorageExportTaskArtifactContent {
    json_output: Option<Value>,
    text_output: Option<String>,
}

impl StorageExportTaskArtifactContent {
    #[must_use]
    pub fn into_parts(self) -> (Option<Value>, Option<String>) {
        (self.json_output, self.text_output)
    }
}

/// Bounded rendering metadata stored with an export artifact.
pub struct StorageExportTaskArtifactReport {
    metadata: Value,
    warnings: Value,
    warning_count: i32,
    truncated: bool,
}

impl StorageExportTaskArtifactReport {
    #[must_use]
    pub fn into_parts(self) -> (Value, Value, i32, bool) {
        (
            self.metadata,
            self.warnings,
            self.warning_count,
            self.truncated,
        )
    }
}

impl StorageExportTaskArtifact {
    #[must_use]
    pub fn builder(
        content_type: impl Into<String>,
        metadata: Value,
        warnings: Value,
        output_expires_at: NaiveDateTime,
    ) -> StorageExportTaskArtifactBuilder {
        StorageExportTaskArtifactBuilder {
            artifact: Self {
                template_name: None,
                content_type: content_type.into(),
                json_output: None,
                text_output: None,
                metadata,
                warnings,
                warning_count: 0,
                truncated: false,
                output_expires_at,
                durations: StorageTaskDurations::default(),
            },
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        StorageExportTaskArtifactIdentity,
        StorageExportTaskArtifactContent,
        StorageExportTaskArtifactReport,
        NaiveDateTime,
        StorageTaskDurations,
    ) {
        (
            StorageExportTaskArtifactIdentity {
                template_name: self.template_name,
                content_type: self.content_type,
            },
            StorageExportTaskArtifactContent {
                json_output: self.json_output,
                text_output: self.text_output,
            },
            StorageExportTaskArtifactReport {
                metadata: self.metadata,
                warnings: self.warnings,
                warning_count: self.warning_count,
                truncated: self.truncated,
            },
            self.output_expires_at,
            self.durations,
        )
    }
}

impl fmt::Debug for StorageExportTaskArtifact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageExportTaskArtifact")
            .field("content_type", &self.content_type)
            .field("warning_count", &self.warning_count)
            .field("truncated", &self.truncated)
            .field("output", &"[redacted]")
            .finish()
    }
}

pub struct StorageExportTaskArtifactBuilder {
    artifact: StorageExportTaskArtifact,
}

impl StorageExportTaskArtifactBuilder {
    #[must_use]
    pub fn template_name(mut self, template_name: Option<String>) -> Self {
        self.artifact.template_name = template_name;
        self
    }

    #[must_use]
    pub fn output(mut self, json_output: Option<Value>, text_output: Option<String>) -> Self {
        self.artifact.json_output = json_output;
        self.artifact.text_output = text_output;
        self
    }

    #[must_use]
    pub const fn warning_state(mut self, warning_count: i32, truncated: bool) -> Self {
        self.artifact.warning_count = warning_count;
        self.artifact.truncated = truncated;
        self
    }

    #[must_use]
    pub const fn durations(mut self, durations: StorageTaskDurations) -> Self {
        self.artifact.durations = durations;
        self
    }

    #[must_use]
    pub fn build(self) -> StorageExportTaskArtifact {
        self.artifact
    }
}

/// Backup artifact stored atomically with the terminal task transition.
#[derive(Clone, PartialEq)]
pub struct StorageBackupTaskArtifact {
    document: Vec<u8>,
    byte_size: i64,
    sha256: String,
    output_expires_at: NaiveDateTime,
}

impl StorageBackupTaskArtifact {
    #[must_use]
    pub fn new(
        document: Vec<u8>,
        byte_size: i64,
        sha256: impl Into<String>,
        output_expires_at: NaiveDateTime,
    ) -> Self {
        Self {
            document,
            byte_size,
            sha256: sha256.into(),
            output_expires_at,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<u8>, i64, String, NaiveDateTime) {
        (
            self.document,
            self.byte_size,
            self.sha256,
            self.output_expires_at,
        )
    }
}

impl fmt::Debug for StorageBackupTaskArtifact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageBackupTaskArtifact")
            .field("byte_size", &self.byte_size)
            .field("document", &"[redacted]")
            .field("sha256", &"[redacted]")
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum StorageTaskCompletionArtifact {
    None,
    Export(StorageExportTaskArtifact),
    Backup(StorageBackupTaskArtifact),
}

#[derive(Clone, Debug, PartialEq)]
pub struct StorageTaskCompletion {
    update: StorageTaskStateUpdate,
    event: StorageTaskEventInput,
    artifact: StorageTaskCompletionArtifact,
}

impl StorageTaskCompletion {
    #[must_use]
    pub const fn new(update: StorageTaskStateUpdate, event: StorageTaskEventInput) -> Self {
        Self {
            update,
            event,
            artifact: StorageTaskCompletionArtifact::None,
        }
    }

    #[must_use]
    pub fn artifact(mut self, artifact: StorageTaskCompletionArtifact) -> Self {
        self.artifact = artifact;
        self
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        StorageTaskStateUpdate,
        StorageTaskEventInput,
        StorageTaskCompletionArtifact,
    ) {
        (self.update, self.event, self.artifact)
    }
}

/// Failure request whose result counts are derived by the backend.
#[derive(Clone, Debug, PartialEq)]
pub struct StorageTaskFailure {
    lease: StorageTaskLease,
    summary: String,
    event: StorageTaskEventInput,
}

impl StorageTaskFailure {
    #[must_use]
    pub fn new(
        lease: StorageTaskLease,
        summary: impl Into<String>,
        event: StorageTaskEventInput,
    ) -> Self {
        Self {
            lease,
            summary: summary.into(),
            event,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageTaskLease, String, StorageTaskEventInput) {
        (self.lease, self.summary, self.event)
    }
}

/// Mandatory worker state-machine behavior for every selectable backend.
#[async_trait]
pub trait TaskExecutionStorage: Send + Sync {
    async fn claim_next_task(
        &self,
        lease_duration: StorageTaskLeaseDuration,
    ) -> Result<Option<StorageTaskClaim>, StorageError>;

    async fn renew_task_lease(
        &self,
        lease: StorageTaskLease,
        lease_duration: StorageTaskLeaseDuration,
    ) -> Result<bool, StorageError>;

    async fn recover_expired_task_leases(
        &self,
        batch_size: usize,
    ) -> Result<Vec<StorageTask>, StorageError>;

    async fn append_task_event(&self, event: StorageTaskEventAppend) -> Result<(), StorageError>;

    async fn update_task_state(
        &self,
        update: StorageTaskStateUpdate,
    ) -> Result<StorageTask, StorageError>;

    async fn complete_task(
        &self,
        completion: StorageTaskCompletion,
    ) -> Result<StorageTask, StorageError>;

    async fn fail_task(&self, failure: StorageTaskFailure) -> Result<StorageTask, StorageError>;

    async fn purge_expired_export_outputs(&self) -> Result<usize, StorageError>;

    async fn purge_expired_backup_outputs(&self) -> Result<usize, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{StorageTaskKind, StorageTaskProgress, StorageTaskScopeSnapshot};

    #[test]
    fn worker_dtos_redact_claims_and_artifacts() {
        let now = chrono::Utc::now().naive_utc();
        let task = StorageTask::builder(
            88_001,
            StorageTaskKind::Export,
            StorageTaskStatus::Running,
            now,
            now,
        )
        .request_payload(Some(serde_json::json!({"secret": "payload"})))
        .progress(StorageTaskProgress::new(1, 0, 0, 0))
        .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
        .build();
        let claim = StorageTaskClaim::new(
            task,
            StorageTaskLease::new(88_001, StorageTaskClaimToken::new("secret-backend-claim")),
        );
        let artifact = StorageExportTaskArtifact::builder(
            "application/json",
            serde_json::json!({"secret": "metadata"}),
            serde_json::json!([]),
            now,
        )
        .output(Some(serde_json::json!({"secret": "output"})), None)
        .build();

        let debug = format!("{claim:?} {artifact:?}");

        for secret in [
            "secret-backend-claim",
            "secret\": \"payload",
            "secret\": \"metadata",
            "secret\": \"output",
        ] {
            assert!(!debug.contains(secret));
        }
    }
}
