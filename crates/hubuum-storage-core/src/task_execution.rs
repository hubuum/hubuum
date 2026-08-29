use std::fmt;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hubuum_domain::{RemoteTargetId, ResourceId, TaskId};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    StorageError, StorageRemoteHttpMethod, StorageRemoteTargetSubjectType, StorageTask,
    StorageTaskDurations, StorageTaskKind, StorageTaskStatus,
};

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
    task_id: TaskId,
    token: StorageTaskClaimToken,
}

impl StorageTaskLease {
    #[must_use]
    pub const fn new(task_id: TaskId, token: StorageTaskClaimToken) -> Self {
        Self { task_id, token }
    }

    #[must_use]
    pub const fn task_id(&self) -> TaskId {
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
    /// Pair a task projection with the ownership proof for that exact task.
    pub fn try_new(task: StorageTask, lease: StorageTaskLease) -> Result<Self, StorageError> {
        if task.id() != lease.task_id() {
            return Err(StorageError::backend_failure(
                "Storage adapter returned a task claim with mismatched task and lease identifiers",
            ));
        }
        if !task.status().is_active() {
            return Err(StorageError::backend_failure(
                "Storage adapter returned a task claim with a non-active status",
            ));
        }
        if !task.has_lease() {
            return Err(StorageError::backend_failure(
                "Storage adapter returned a task claim without a lease expiry",
            ));
        }
        Ok(Self { task, lease })
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
    /// Construct non-negative task progress counts.
    ///
    /// `processed` may differ from `succeeded + failed` while work is running
    /// or recovering, so only the per-field invariant is enforced here.
    pub fn try_new(processed: i32, succeeded: i32, failed: i32) -> Result<Self, StorageError> {
        if processed < 0 {
            return Err(StorageError::invalid_input(
                "Task processed count must not be negative",
            ));
        }
        if succeeded < 0 {
            return Err(StorageError::invalid_input(
                "Task succeeded count must not be negative",
            ));
        }
        if failed < 0 {
            return Err(StorageError::invalid_input(
                "Task failed count must not be negative",
            ));
        }
        Ok(Self {
            processed,
            succeeded,
            failed,
        })
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

/// Active state values supplied by task execution.
#[derive(Clone, Debug, PartialEq)]
pub struct StorageTaskActiveUpdate {
    lease: StorageTaskLease,
    status: StorageTaskStatus,
    summary: Option<String>,
    counts: StorageTaskResultCounts,
    started_at: Option<DateTime<Utc>>,
}

impl StorageTaskActiveUpdate {
    pub fn try_new(
        lease: StorageTaskLease,
        status: StorageTaskStatus,
        counts: StorageTaskResultCounts,
    ) -> Result<Self, StorageError> {
        if !status.is_active() {
            return Err(StorageError::invalid_input(
                "Task state updates require an active status",
            ));
        }
        Ok(Self {
            lease,
            status,
            summary: None,
            counts,
            started_at: None,
        })
    }

    #[must_use]
    pub fn summary(mut self, summary: Option<String>) -> Self {
        self.summary = summary;
        self
    }

    #[must_use]
    pub const fn started_at(mut self, started_at: Option<DateTime<Utc>>) -> Self {
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
        Option<DateTime<Utc>>,
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

/// Terminal state values supplied when task execution completes.
#[derive(Clone, Debug, PartialEq)]
pub struct StorageTaskTerminalUpdate {
    lease: StorageTaskLease,
    status: StorageTaskStatus,
    summary: Option<String>,
    counts: StorageTaskResultCounts,
    started_at: Option<DateTime<Utc>>,
}

impl StorageTaskTerminalUpdate {
    pub fn try_new(
        lease: StorageTaskLease,
        status: StorageTaskStatus,
        counts: StorageTaskResultCounts,
    ) -> Result<Self, StorageError> {
        if !status.is_terminal() {
            return Err(StorageError::invalid_input(
                "Task completion updates require a terminal status",
            ));
        }
        Ok(Self {
            lease,
            status,
            summary: None,
            counts,
            started_at: None,
        })
    }

    #[must_use]
    pub fn summary(mut self, summary: Option<String>) -> Self {
        self.summary = summary;
        self
    }

    #[must_use]
    pub const fn started_at(mut self, started_at: Option<DateTime<Utc>>) -> Self {
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
        Option<DateTime<Utc>>,
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
    content: StorageExportTaskArtifactContent,
    metadata: Value,
    warnings: Value,
    warning_count: i32,
    truncated: bool,
    output_expires_at: DateTime<Utc>,
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

/// Structured or rendered content for an export artifact.
#[derive(Clone, PartialEq)]
pub enum StorageExportTaskArtifactContent {
    Json(Value),
    Text(String),
}

impl StorageExportTaskArtifactContent {
    #[must_use]
    pub fn into_parts(self) -> (Option<Value>, Option<String>) {
        match self {
            Self::Json(output) => (Some(output), None),
            Self::Text(output) => (None, Some(output)),
        }
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
        content: StorageExportTaskArtifactContent,
        metadata: Value,
        warnings: Value,
        output_expires_at: DateTime<Utc>,
    ) -> StorageExportTaskArtifactBuilder {
        StorageExportTaskArtifactBuilder {
            artifact: Self {
                template_name: None,
                content_type: content_type.into(),
                content,
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
        DateTime<Utc>,
        StorageTaskDurations,
    ) {
        (
            StorageExportTaskArtifactIdentity {
                template_name: self.template_name,
                content_type: self.content_type,
            },
            self.content,
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

    pub fn try_build(self) -> Result<StorageExportTaskArtifact, StorageError> {
        if self.artifact.warning_count < 0 {
            return Err(StorageError::invalid_input(
                "Export artifact warning count must not be negative",
            ));
        }
        Ok(self.artifact)
    }
}

/// Backup artifact stored atomically with the terminal task transition.
#[derive(Clone, PartialEq)]
pub struct StorageBackupTaskArtifact {
    document: Vec<u8>,
    byte_size: i64,
    sha256: String,
    output_expires_at: DateTime<Utc>,
}

impl StorageBackupTaskArtifact {
    pub fn try_new(
        document: Vec<u8>,
        output_expires_at: DateTime<Utc>,
    ) -> Result<Self, StorageError> {
        let byte_size = i64::try_from(document.len()).map_err(|_| {
            StorageError::input_too_large("Backup artifact exceeds the supported byte-size range")
        })?;
        let sha256 = Sha256::digest(&document)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        Ok(Self {
            document,
            byte_size,
            sha256,
            output_expires_at,
        })
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<u8>, i64, String, DateTime<Utc>) {
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

/// Target and rendered request facts stored for a remote-call task.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageRemoteCallArtifactTarget {
    target_id: Option<RemoteTargetId>,
    subject_type: StorageRemoteTargetSubjectType,
    subject_id: ResourceId,
    method: Option<StorageRemoteHttpMethod>,
    rendered_url: String,
}

/// Named remote-call target components consumed by persistence adapters.
pub struct StorageRemoteCallArtifactTargetParts {
    target_id: Option<RemoteTargetId>,
    subject_type: StorageRemoteTargetSubjectType,
    subject_id: ResourceId,
    method: Option<StorageRemoteHttpMethod>,
    rendered_url: String,
}

impl StorageRemoteCallArtifactTargetParts {
    #[must_use]
    pub const fn target_id(&self) -> Option<RemoteTargetId> {
        self.target_id
    }

    #[must_use]
    pub const fn subject_type(&self) -> StorageRemoteTargetSubjectType {
        self.subject_type
    }

    #[must_use]
    pub const fn subject_id(&self) -> ResourceId {
        self.subject_id
    }

    #[must_use]
    pub const fn method(&self) -> Option<StorageRemoteHttpMethod> {
        self.method
    }

    #[must_use]
    pub fn rendered_url(&self) -> &str {
        &self.rendered_url
    }
}

impl StorageRemoteCallArtifactTarget {
    #[must_use]
    pub fn new(
        target_id: Option<RemoteTargetId>,
        subject_type: StorageRemoteTargetSubjectType,
        subject_id: ResourceId,
        method: Option<StorageRemoteHttpMethod>,
        rendered_url: impl Into<String>,
    ) -> Self {
        Self {
            target_id,
            subject_type,
            subject_id,
            method,
            rendered_url: rendered_url.into(),
        }
    }

    #[must_use]
    pub fn into_parts(self) -> StorageRemoteCallArtifactTargetParts {
        StorageRemoteCallArtifactTargetParts {
            target_id: self.target_id,
            subject_type: self.subject_type,
            subject_id: self.subject_id,
            method: self.method,
            rendered_url: self.rendered_url,
        }
    }
}

impl fmt::Debug for StorageRemoteCallArtifactTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRemoteCallArtifactTarget")
            .field("has_target", &self.target_id.is_some())
            .field("method", &self.method)
            .field("identity", &"[redacted]")
            .field("rendered_url", &"[redacted]")
            .finish()
    }
}

/// Sanitized response material retained for a remote-call task.
#[derive(Clone, PartialEq)]
pub struct StorageRemoteCallArtifactResponse {
    status: Option<i32>,
    headers: Option<Value>,
    body_preview: Option<String>,
}

impl StorageRemoteCallArtifactResponse {
    #[must_use]
    pub const fn new(
        status: Option<i32>,
        headers: Option<Value>,
        body_preview: Option<String>,
    ) -> Self {
        Self {
            status,
            headers,
            body_preview,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (Option<i32>, Option<Value>, Option<String>) {
        (self.status, self.headers, self.body_preview)
    }
}

impl fmt::Debug for StorageRemoteCallArtifactResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRemoteCallArtifactResponse")
            .field("status", &self.status)
            .field("has_headers", &self.headers.is_some())
            .field("has_body_preview", &self.body_preview.is_some())
            .finish()
    }
}

/// Terminal outcome stored with a remote-call task.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageRemoteCallArtifactOutcome {
    duration_ms: i32,
    success: bool,
    error: Option<String>,
}

impl StorageRemoteCallArtifactOutcome {
    #[must_use]
    pub const fn new(duration_ms: i32, success: bool, error: Option<String>) -> Self {
        Self {
            duration_ms,
            success,
            error,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, bool, Option<String>) {
        (self.duration_ms, self.success, self.error)
    }
}

impl fmt::Debug for StorageRemoteCallArtifactOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRemoteCallArtifactOutcome")
            .field("duration_ms", &self.duration_ms)
            .field("success", &self.success)
            .field("has_error", &self.error.is_some())
            .finish()
    }
}

/// Remote-call result stored atomically with the terminal task transition.
#[derive(Clone, Debug, PartialEq)]
pub struct StorageRemoteCallTaskArtifact {
    target: StorageRemoteCallArtifactTarget,
    response: StorageRemoteCallArtifactResponse,
    outcome: StorageRemoteCallArtifactOutcome,
}

impl StorageRemoteCallTaskArtifact {
    #[must_use]
    pub const fn new(
        target: StorageRemoteCallArtifactTarget,
        response: StorageRemoteCallArtifactResponse,
        outcome: StorageRemoteCallArtifactOutcome,
    ) -> Self {
        Self {
            target,
            response,
            outcome,
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        StorageRemoteCallArtifactTarget,
        StorageRemoteCallArtifactResponse,
        StorageRemoteCallArtifactOutcome,
    ) {
        (self.target, self.response, self.outcome)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum StorageTaskCompletionArtifact {
    None,
    Export(StorageExportTaskArtifact),
    Backup(StorageBackupTaskArtifact),
    RemoteCall(StorageRemoteCallTaskArtifact),
}

#[derive(Clone, Debug, PartialEq)]
pub struct StorageTaskCompletion {
    task_kind: StorageTaskKind,
    update: StorageTaskTerminalUpdate,
    event: StorageTaskEventInput,
    artifact: StorageTaskCompletionArtifact,
}

impl StorageTaskCompletion {
    /// Construct a terminal task transition with the artifact required by its task kind.
    pub fn try_new(
        task_kind: StorageTaskKind,
        update: StorageTaskTerminalUpdate,
        event: StorageTaskEventInput,
        artifact: StorageTaskCompletionArtifact,
    ) -> Result<Self, StorageError> {
        let valid_artifact = matches!(
            (task_kind, &artifact),
            (
                StorageTaskKind::Import | StorageTaskKind::Reindex,
                StorageTaskCompletionArtifact::None
            ) | (
                StorageTaskKind::Export,
                StorageTaskCompletionArtifact::Export(_)
            ) | (
                StorageTaskKind::Backup,
                StorageTaskCompletionArtifact::Backup(_)
            ) | (
                StorageTaskKind::RemoteCall,
                StorageTaskCompletionArtifact::RemoteCall(_)
            )
        );
        if !valid_artifact {
            return Err(StorageError::invalid_input(format!(
                "Task completion artifact does not match task kind {}",
                task_kind.as_str()
            )));
        }
        Ok(Self {
            task_kind,
            update,
            event,
            artifact,
        })
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        StorageTaskKind,
        StorageTaskTerminalUpdate,
        StorageTaskEventInput,
        StorageTaskCompletionArtifact,
    ) {
        (self.task_kind, self.update, self.event, self.artifact)
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
        update: StorageTaskActiveUpdate,
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
    fn export_artifact_rejects_a_negative_warning_count() {
        let error = StorageExportTaskArtifact::builder(
            "text/plain",
            StorageExportTaskArtifactContent::Text("output".to_string()),
            serde_json::json!({}),
            serde_json::json!([]),
            chrono::Utc::now(),
        )
        .warning_state(-1, false)
        .try_build()
        .expect_err("negative warning counts must be rejected");

        assert_eq!(error.kind(), crate::StorageErrorKind::InvalidInput);
    }

    #[test]
    fn backup_artifact_derives_size_and_digest_from_its_document() {
        let artifact = StorageBackupTaskArtifact::try_new(b"{}".to_vec(), chrono::Utc::now())
            .expect("small backup artifact should be valid");
        let (_, byte_size, sha256, _) = artifact.into_parts();

        assert_eq!(byte_size, 2);
        assert_eq!(
            sha256,
            "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
        );
    }

    #[test]
    fn worker_dtos_redact_claims_and_artifacts() {
        let now = chrono::Utc::now();
        let task_id = TaskId::new(88_001).unwrap();
        let task = StorageTask::builder(
            task_id,
            StorageTaskKind::Export,
            StorageTaskStatus::Running,
            now,
            now,
        )
        .request_payload(Some(serde_json::json!({"secret": "payload"})))
        .progress(StorageTaskProgress::try_new(1, 0, 0, 0).unwrap())
        .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
        .lease_expires_at(Some(now))
        .try_build()
        .unwrap();
        let claim = StorageTaskClaim::try_new(
            task,
            StorageTaskLease::new(task_id, StorageTaskClaimToken::new("secret-backend-claim")),
        )
        .expect("matching task and lease ids should form a claim");
        let artifact = StorageExportTaskArtifact::builder(
            "application/json",
            StorageExportTaskArtifactContent::Json(serde_json::json!({
                "secret": "output"
            })),
            serde_json::json!({"secret": "metadata"}),
            serde_json::json!([]),
            now,
        )
        .try_build()
        .unwrap();
        let remote_artifact = StorageRemoteCallTaskArtifact::new(
            StorageRemoteCallArtifactTarget::new(
                Some(RemoteTargetId::new(7).unwrap()),
                StorageRemoteTargetSubjectType::Object,
                ResourceId::new(8).unwrap(),
                Some(StorageRemoteHttpMethod::Post),
                "https://example.invalid/?secret=url",
            ),
            StorageRemoteCallArtifactResponse::new(
                Some(200),
                Some(serde_json::json!({"secret": "headers"})),
                Some("secret response body".to_string()),
            ),
            StorageRemoteCallArtifactOutcome::new(
                9,
                false,
                Some("secret remote error".to_string()),
            ),
        );

        let debug = format!("{claim:?} {artifact:?} {remote_artifact:?}");

        for secret in [
            "secret-backend-claim",
            "secret\": \"payload",
            "secret\": \"metadata",
            "secret\": \"output",
            "object-secret",
            "secret=url",
            "secret\": \"headers",
            "secret response body",
            "secret remote error",
        ] {
            assert!(!debug.contains(secret));
        }
    }

    fn task_with_id(task_id: TaskId) -> StorageTask {
        let now = chrono::Utc::now();
        StorageTask::builder(
            task_id,
            StorageTaskKind::Export,
            StorageTaskStatus::Running,
            now,
            now,
        )
        .lease_expires_at(Some(now))
        .try_build()
        .unwrap()
    }

    #[test]
    fn task_claim_rejects_mismatched_task_and_lease_ids() {
        let task_id = TaskId::new(88_101).unwrap();
        let lease_task_id = TaskId::new(88_102).unwrap();

        let error = StorageTaskClaim::try_new(
            task_with_id(task_id),
            StorageTaskLease::new(
                lease_task_id,
                StorageTaskClaimToken::new("mismatched-claim"),
            ),
        )
        .expect_err("a lease for another task must not form a claim");

        assert_eq!(error.kind(), crate::StorageErrorKind::Backend);
    }

    #[test]
    fn task_claim_rejects_non_active_tasks() {
        for status in [
            StorageTaskStatus::Queued,
            StorageTaskStatus::Succeeded,
            StorageTaskStatus::Failed,
            StorageTaskStatus::PartiallySucceeded,
            StorageTaskStatus::Cancelled,
        ] {
            let task_id = TaskId::new(88_103).unwrap();
            let now = chrono::Utc::now();
            let task = StorageTask::builder(task_id, StorageTaskKind::Export, status, now, now)
                .lease_expires_at(Some(now))
                .try_build()
                .unwrap();

            let error = StorageTaskClaim::try_new(
                task,
                StorageTaskLease::new(task_id, StorageTaskClaimToken::new("inactive-claim")),
            )
            .expect_err("a non-active task must not form a claim");

            assert_eq!(error.kind(), crate::StorageErrorKind::Backend);
        }
    }

    #[test]
    fn task_claim_rejects_a_projection_without_lease_expiry() {
        let task_id = TaskId::new(88_104).unwrap();
        let now = chrono::Utc::now();
        let task = StorageTask::builder(
            task_id,
            StorageTaskKind::Export,
            StorageTaskStatus::Running,
            now,
            now,
        )
        .try_build()
        .unwrap();

        let error = StorageTaskClaim::try_new(
            task,
            StorageTaskLease::new(task_id, StorageTaskClaimToken::new("missing-expiry")),
        )
        .expect_err("a claim without a projected lease expiry must be rejected");

        assert_eq!(error.kind(), crate::StorageErrorKind::Backend);
    }

    #[test]
    fn active_task_updates_reject_non_active_statuses() {
        let task_id = TaskId::new(88_105).unwrap();
        let counts = StorageTaskResultCounts::try_new(0, 0, 0).unwrap();
        for status in [
            StorageTaskStatus::Queued,
            StorageTaskStatus::Succeeded,
            StorageTaskStatus::Failed,
            StorageTaskStatus::PartiallySucceeded,
            StorageTaskStatus::Cancelled,
        ] {
            let error = StorageTaskActiveUpdate::try_new(
                StorageTaskLease::new(task_id, StorageTaskClaimToken::new("active-update")),
                status,
                counts,
            )
            .expect_err("active updates must reject non-active statuses");

            assert_eq!(error.kind(), crate::StorageErrorKind::InvalidInput);
        }
    }

    #[test]
    fn terminal_task_updates_reject_non_terminal_statuses() {
        let task_id = TaskId::new(88_106).unwrap();
        let counts = StorageTaskResultCounts::try_new(0, 0, 0).unwrap();
        for status in [
            StorageTaskStatus::Queued,
            StorageTaskStatus::Validating,
            StorageTaskStatus::Running,
        ] {
            let error = StorageTaskTerminalUpdate::try_new(
                StorageTaskLease::new(task_id, StorageTaskClaimToken::new("terminal-update")),
                status,
                counts,
            )
            .expect_err("terminal updates must reject non-terminal statuses");

            assert_eq!(error.kind(), crate::StorageErrorKind::InvalidInput);
        }
    }

    #[test]
    fn task_completion_rejects_artifacts_that_do_not_match_the_task_kind() {
        let task_id = TaskId::new(88_107).unwrap();
        for kind in [
            StorageTaskKind::Export,
            StorageTaskKind::Backup,
            StorageTaskKind::RemoteCall,
        ] {
            let update = StorageTaskTerminalUpdate::try_new(
                StorageTaskLease::new(task_id, StorageTaskClaimToken::new("completion")),
                StorageTaskStatus::Succeeded,
                StorageTaskResultCounts::try_new(1, 1, 0).unwrap(),
            )
            .unwrap();
            let error = StorageTaskCompletion::try_new(
                kind,
                update,
                StorageTaskEventInput::new("succeeded", "done"),
                StorageTaskCompletionArtifact::None,
            )
            .expect_err("artifact-producing task kinds must require their artifact");

            assert_eq!(error.kind(), crate::StorageErrorKind::InvalidInput);
        }

        for kind in [StorageTaskKind::Import, StorageTaskKind::Reindex] {
            let update = StorageTaskTerminalUpdate::try_new(
                StorageTaskLease::new(task_id, StorageTaskClaimToken::new("completion")),
                StorageTaskStatus::Succeeded,
                StorageTaskResultCounts::try_new(1, 1, 0).unwrap(),
            )
            .unwrap();
            let error = StorageTaskCompletion::try_new(
                kind,
                update,
                StorageTaskEventInput::new("succeeded", "done"),
                StorageTaskCompletionArtifact::Backup(
                    StorageBackupTaskArtifact::try_new(Vec::new(), chrono::Utc::now()).unwrap(),
                ),
            )
            .expect_err("non-artifact task kinds must reject completion artifacts");

            assert_eq!(error.kind(), crate::StorageErrorKind::InvalidInput);
        }
    }

    #[test]
    fn task_result_counts_reject_negative_processed_count() {
        let error = StorageTaskResultCounts::try_new(-1, 0, 0)
            .expect_err("a negative processed count must be rejected");

        assert_eq!(error.kind(), crate::StorageErrorKind::InvalidInput);
    }

    #[test]
    fn task_result_counts_reject_negative_succeeded_count() {
        let error = StorageTaskResultCounts::try_new(0, -1, 0)
            .expect_err("a negative succeeded count must be rejected");

        assert_eq!(error.kind(), crate::StorageErrorKind::InvalidInput);
    }

    #[test]
    fn task_result_counts_reject_negative_failed_count() {
        let error = StorageTaskResultCounts::try_new(0, 0, -1)
            .expect_err("a negative failed count must be rejected");

        assert_eq!(error.kind(), crate::StorageErrorKind::InvalidInput);
    }

    #[test]
    fn task_result_counts_allow_in_progress_totals() {
        let counts = StorageTaskResultCounts::try_new(5, 2, 1)
            .expect("in-progress counts need not sum to the processed total");

        assert_eq!(counts.processed(), 5);
        assert_eq!(counts.succeeded(), 2);
        assert_eq!(counts.failed(), 1);
    }
}
