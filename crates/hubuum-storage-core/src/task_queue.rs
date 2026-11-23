use crate::artifacts::ArtifactReference;
use std::fmt;

use chrono::{DateTime, Utc};
use hubuum_domain::{
    GroupId, ImportTaskResultId, PrincipalId, RemoteTargetId, ResourceId, TaskId, TokenId,
};
use hubuum_events_core::{EventContext, EventSequence, TraceLink};
use hubuum_query::QueryOptions;
use hubuum_task_core::IdempotencyKey;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{StorageError, StorageTaskControl, StorageValidationError};
use crate::{StorageTaskCheckpoint, StorageTaskExecutionPhase};

use crate::{StorageTaskKind, StorageTaskStatus};

/// Invocation provenance stored with fresh admission and emitted only by its
/// activation winner. Retry classification must precede construction.
#[derive(Clone, PartialEq)]
pub struct StorageTaskRemoteInvocation {
    target_id: RemoteTargetId,
    subject_type: crate::StorageRemoteTargetSubjectType,
    subject_id: ResourceId,
    event_context: EventContext,
}

impl StorageTaskRemoteInvocation {
    pub const fn new(
        target_id: RemoteTargetId,
        subject_type: crate::StorageRemoteTargetSubjectType,
        subject_id: ResourceId,
        event_context: EventContext,
    ) -> Self {
        Self {
            target_id,
            subject_type,
            subject_id,
            event_context,
        }
    }
    pub const fn target_id(&self) -> RemoteTargetId {
        self.target_id
    }
    pub const fn subject_type(&self) -> crate::StorageRemoteTargetSubjectType {
        self.subject_type
    }
    pub const fn subject_id(&self) -> ResourceId {
        self.subject_id
    }
    pub const fn event_context(&self) -> &EventContext {
        &self.event_context
    }
    pub fn with_task(self, task_id: TaskId) -> crate::StorageRemoteTargetInvocation {
        crate::StorageRemoteTargetInvocation::new(
            self.target_id,
            task_id,
            self.subject_type,
            self.subject_id,
            self.event_context,
        )
    }
}

impl fmt::Debug for StorageTaskRemoteInvocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageTaskRemoteInvocation")
            .field("target_id", &self.target_id)
            .field("subject_type", &self.subject_type)
            .field("subject_id", &self.subject_id)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageTaskProgress {
    total: i32,
    processed: i32,
    succeeded: i32,
    failed: i32,
}

impl StorageTaskProgress {
    /// Construct a task progress projection whose persisted counters are valid.
    pub fn try_new(
        total: i32,
        processed: i32,
        succeeded: i32,
        failed: i32,
    ) -> Result<Self, StorageValidationError> {
        if total < 0 {
            return Err(StorageValidationError::invalid(
                "Task total count must not be negative",
            ));
        }
        if processed < 0 {
            return Err(StorageValidationError::invalid(
                "Task processed count must not be negative",
            ));
        }
        if succeeded < 0 {
            return Err(StorageValidationError::invalid(
                "Task succeeded count must not be negative",
            ));
        }
        if failed < 0 {
            return Err(StorageValidationError::invalid(
                "Task failed count must not be negative",
            ));
        }
        Ok(Self {
            total,
            processed,
            succeeded,
            failed,
        })
    }

    #[must_use]
    pub const fn total(self) -> i32 {
        self.total
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

#[derive(Clone, PartialEq)]
pub struct StorageTaskScopeSnapshot {
    token_id: Option<TokenId>,
    scoped: bool,
    scopes: Value,
}

impl StorageTaskScopeSnapshot {
    #[must_use]
    pub const fn new(token_id: Option<TokenId>, scoped: bool, scopes: Value) -> Self {
        Self {
            token_id,
            scoped,
            scopes,
        }
    }

    #[must_use]
    pub const fn unscoped() -> Self {
        Self {
            token_id: None,
            scoped: false,
            scopes: Value::Array(Vec::new()),
        }
    }

    #[must_use]
    pub const fn token_id(&self) -> Option<TokenId> {
        self.token_id
    }

    #[must_use]
    pub const fn scoped(&self) -> bool {
        self.scoped
    }

    #[must_use]
    pub const fn scopes(&self) -> &Value {
        &self.scopes
    }
}

impl fmt::Debug for StorageTaskScopeSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageTaskScopeSnapshot")
            .field("has_token", &self.token_id.is_some())
            .field("scoped", &self.scoped)
            .field("scopes", &"[redacted]")
            .finish()
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageTaskCreateRequest {
    approval: Option<crate::StorageCredentialUse>,
    kind: StorageTaskKind,
    submitted_by: PrincipalId,
    request_payload: Value,
    metadata: Option<crate::StorageTaskMetadata>,
    total_items: i32,
    idempotency_key: Option<IdempotencyKey>,
    request_hash: Option<String>,
    scope_snapshot: StorageTaskScopeSnapshot,
    trace_link: Option<TraceLink>,
    receipt_binding: Option<crate::TaskReceiptBinding>,
    remote_invocation: Option<StorageTaskRemoteInvocation>,
}

impl StorageTaskCreateRequest {
    #[must_use]
    pub fn with_approval(mut self, approval: Option<crate::StorageCredentialUse>) -> Self {
        self.approval = approval;
        self
    }
    #[must_use]
    pub const fn approval(&self) -> Option<&crate::StorageCredentialUse> {
        self.approval.as_ref()
    }

    #[must_use]
    pub const fn metadata(&self) -> Option<&crate::StorageTaskMetadata> {
        self.metadata.as_ref()
    }

    #[must_use]
    pub fn builder(
        kind: StorageTaskKind,
        submitted_by: PrincipalId,
        request_payload: Value,
        total_items: i32,
    ) -> StorageTaskCreateRequestBuilder {
        StorageTaskCreateRequestBuilder {
            kind,
            submitted_by,
            request_payload,
            metadata: None,
            total_items,
            idempotency_key: None,
            request_hash: None,
            scope_snapshot: StorageTaskScopeSnapshot::unscoped(),
            trace_link: None,
            receipt_binding: None,
            remote_invocation: None,
        }
    }

    #[must_use]
    pub const fn kind(&self) -> StorageTaskKind {
        self.kind
    }

    #[must_use]
    pub const fn submitted_by(&self) -> PrincipalId {
        self.submitted_by
    }

    #[must_use]
    pub const fn request_payload(&self) -> &Value {
        &self.request_payload
    }

    #[must_use]
    pub const fn total_items(&self) -> i32 {
        self.total_items
    }

    #[must_use]
    pub const fn idempotency_key(&self) -> Option<&IdempotencyKey> {
        self.idempotency_key.as_ref()
    }

    #[must_use]
    pub fn request_hash(&self) -> Option<&str> {
        self.request_hash.as_deref()
    }

    #[must_use]
    pub const fn scope_snapshot(&self) -> &StorageTaskScopeSnapshot {
        &self.scope_snapshot
    }

    #[must_use]
    pub const fn trace_link(&self) -> Option<&TraceLink> {
        self.trace_link.as_ref()
    }

    pub const fn receipt_binding(&self) -> Option<&crate::TaskReceiptBinding> {
        self.receipt_binding.as_ref()
    }

    pub const fn remote_invocation(&self) -> Option<&StorageTaskRemoteInvocation> {
        self.remote_invocation.as_ref()
    }
}

impl fmt::Debug for StorageTaskCreateRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageTaskCreateRequest")
            .field("kind", &self.kind)
            .field("total_items", &self.total_items)
            .field("has_idempotency_key", &self.idempotency_key.is_some())
            .field("has_request_hash", &self.request_hash.is_some())
            .field("scope", &self.scope_snapshot)
            .field("has_trace_link", &self.trace_link.is_some())
            .field("identity_and_payload", &"[redacted]")
            .finish()
    }
}

pub struct StorageTaskCreateRequestBuilder {
    kind: StorageTaskKind,
    submitted_by: PrincipalId,
    request_payload: Value,
    metadata: Option<crate::StorageTaskMetadata>,
    total_items: i32,
    idempotency_key: Option<IdempotencyKey>,
    request_hash: Option<String>,
    scope_snapshot: StorageTaskScopeSnapshot,
    trace_link: Option<TraceLink>,
    receipt_binding: Option<crate::TaskReceiptBinding>,
    remote_invocation: Option<StorageTaskRemoteInvocation>,
}

impl StorageTaskCreateRequestBuilder {
    #[must_use]
    pub fn receipt_binding(mut self, binding: Option<crate::TaskReceiptBinding>) -> Self {
        self.receipt_binding = binding;
        self
    }

    #[must_use]
    pub fn remote_invocation(mut self, invocation: Option<StorageTaskRemoteInvocation>) -> Self {
        self.remote_invocation = invocation;
        self
    }
    #[must_use]
    pub fn metadata(mut self, metadata: Option<crate::StorageTaskMetadata>) -> Self {
        self.metadata = metadata;
        self
    }

    #[must_use]
    pub fn idempotency_key(mut self, idempotency_key: Option<IdempotencyKey>) -> Self {
        self.idempotency_key = idempotency_key;
        self
    }

    #[must_use]
    pub fn request_hash(mut self, request_hash: Option<String>) -> Self {
        self.request_hash = request_hash;
        self
    }

    #[must_use]
    pub fn scope_snapshot(mut self, scope_snapshot: StorageTaskScopeSnapshot) -> Self {
        self.scope_snapshot = scope_snapshot;
        self
    }

    #[must_use]
    pub fn trace_link(mut self, trace_link: Option<TraceLink>) -> Self {
        self.trace_link = trace_link;
        self
    }

    pub fn try_build(self) -> Result<StorageTaskCreateRequest, StorageError> {
        if self.receipt_binding.as_ref().is_some_and(|binding| {
            binding.lookup().kind() != self.kind || self.idempotency_key.is_none()
        }) {
            return Err(StorageError::invalid_input(
                "Task receipt binding requires its matching keyed task kind",
            ));
        }
        if self.remote_invocation.is_some() && self.kind != StorageTaskKind::RemoteCall {
            return Err(StorageError::invalid_input(
                "Invocation event intent requires a remote task",
            ));
        }
        if self
            .metadata
            .as_ref()
            .is_some_and(|value| value.kind() != self.kind)
        {
            return Err(StorageError::invalid_input("Task metadata kind mismatch"));
        }
        if self.total_items < 0 {
            return Err(StorageError::invalid_input(
                "Task total_items must not be negative",
            ));
        }

        Ok(StorageTaskCreateRequest {
            approval: None,
            kind: self.kind,
            submitted_by: self.submitted_by,
            request_payload: self.request_payload,
            metadata: self.metadata,
            total_items: self.total_items,
            idempotency_key: self.idempotency_key,
            request_hash: self.request_hash,
            scope_snapshot: self.scope_snapshot,
            trace_link: self.trace_link,
            receipt_binding: self.receipt_binding,
            remote_invocation: self.remote_invocation,
        })
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageTask {
    id: TaskId,
    kind: StorageTaskKind,
    status: StorageTaskStatus,
    submitted_by: Option<PrincipalId>,
    idempotency_key: Option<String>,
    request_hash: Option<String>,
    request_payload: Option<Value>,
    checkpoint: Option<StorageTaskCheckpoint>,
    metadata: Option<crate::StorageTaskMetadata>,
    discovery_state: crate::StorageTaskDiscoveryState,
    summary: Option<String>,
    progress: StorageTaskProgress,
    scope_snapshot: StorageTaskScopeSnapshot,
    request_redacted_at: Option<DateTime<Utc>>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    deleted_at: Option<DateTime<Utc>>,
    deleted_by: Option<PrincipalId>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    lease_expires_at: Option<DateTime<Utc>>,
    attempt_count: i32,
    initiator_principal_id: Option<PrincipalId>,
    trace_link: Option<TraceLink>,
    control: StorageTaskControl,
}

impl StorageTask {
    #[must_use]
    pub const fn discovery_state(&self) -> &crate::StorageTaskDiscoveryState {
        &self.discovery_state
    }
    pub fn set_discovery_state(&mut self, state: crate::StorageTaskDiscoveryState) {
        self.discovery_state = state;
    }

    #[must_use]
    pub const fn metadata(&self) -> Option<&crate::StorageTaskMetadata> {
        self.metadata.as_ref()
    }

    #[must_use]
    pub fn builder(
        id: TaskId,
        kind: StorageTaskKind,
        status: StorageTaskStatus,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> StorageTaskBuilder {
        StorageTaskBuilder {
            task: Self {
                id,
                kind,
                status,
                submitted_by: None,
                idempotency_key: None,
                request_hash: None,
                request_payload: None,
                checkpoint: None,
                metadata: None,
                discovery_state: crate::StorageTaskDiscoveryState::default(),
                summary: None,
                progress: StorageTaskProgress::default(),
                scope_snapshot: StorageTaskScopeSnapshot::unscoped(),
                request_redacted_at: None,
                started_at: None,
                finished_at: None,
                deleted_at: None,
                deleted_by: None,
                created_at,
                updated_at,
                lease_expires_at: None,
                attempt_count: 0,
                initiator_principal_id: None,
                trace_link: None,
                control: StorageTaskControl::new(kind),
            },
        }
    }

    #[must_use]
    pub const fn id(&self) -> TaskId {
        self.id
    }

    #[must_use]
    pub const fn control(&self) -> &StorageTaskControl {
        &self.control
    }

    #[must_use]
    pub const fn kind(&self) -> StorageTaskKind {
        self.kind
    }

    #[must_use]
    pub const fn status(&self) -> StorageTaskStatus {
        self.status
    }

    #[must_use]
    pub const fn submitted_by(&self) -> Option<PrincipalId> {
        self.submitted_by
    }

    #[must_use]
    pub fn summary(&self) -> Option<&str> {
        self.summary.as_deref()
    }

    #[must_use]
    pub const fn progress(&self) -> StorageTaskProgress {
        self.progress
    }

    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    #[must_use]
    pub const fn started_at(&self) -> Option<DateTime<Utc>> {
        self.started_at
    }

    #[must_use]
    pub const fn finished_at(&self) -> Option<DateTime<Utc>> {
        self.finished_at
    }

    #[must_use]
    pub const fn request_redacted_at(&self) -> Option<DateTime<Utc>> {
        self.request_redacted_at
    }

    #[must_use]
    pub fn idempotency_key(&self) -> Option<&str> {
        self.idempotency_key.as_deref()
    }

    #[must_use]
    pub fn request_hash(&self) -> Option<&str> {
        self.request_hash.as_deref()
    }

    #[must_use]
    pub const fn request_payload(&self) -> Option<&Value> {
        self.request_payload.as_ref()
    }

    #[must_use]
    pub const fn checkpoint(&self) -> Option<&StorageTaskCheckpoint> {
        self.checkpoint.as_ref()
    }

    #[must_use]
    pub const fn scope_snapshot(&self) -> &StorageTaskScopeSnapshot {
        &self.scope_snapshot
    }

    #[must_use]
    pub const fn deleted_at(&self) -> Option<DateTime<Utc>> {
        self.deleted_at
    }

    #[must_use]
    pub const fn deleted_by(&self) -> Option<PrincipalId> {
        self.deleted_by
    }

    #[must_use]
    pub const fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    #[must_use]
    pub const fn has_lease(&self) -> bool {
        self.lease_expires_at.is_some()
    }

    #[must_use]
    pub const fn lease_expires_at(&self) -> Option<DateTime<Utc>> {
        self.lease_expires_at
    }

    #[must_use]
    pub const fn attempt_count(&self) -> i32 {
        self.attempt_count
    }

    #[must_use]
    pub const fn initiator_principal_id(&self) -> Option<PrincipalId> {
        self.initiator_principal_id
    }

    #[must_use]
    pub const fn trace_link(&self) -> Option<&TraceLink> {
        self.trace_link.as_ref()
    }
}

impl fmt::Debug for StorageTask {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageTask")
            .field("kind", &self.kind)
            .field("status", &self.status)
            .field("progress", &self.progress)
            .field("has_submitter", &self.submitted_by.is_some())
            .field("has_summary", &self.summary.is_some())
            .field("has_request_payload", &self.request_payload.is_some())
            .field("has_checkpoint", &self.checkpoint.is_some())
            .field("has_lease", &self.has_lease())
            .field("has_trace_link", &self.trace_link.is_some())
            .field("identity_and_payload", &"[redacted]")
            .finish_non_exhaustive()
    }
}

pub struct StorageTaskBuilder {
    task: StorageTask,
}

impl StorageTaskBuilder {
    #[must_use]
    pub fn metadata(mut self, metadata: Option<crate::StorageTaskMetadata>) -> Self {
        self.task.metadata = metadata;
        self
    }

    #[must_use]
    pub fn control(mut self, control: StorageTaskControl) -> Self {
        self.task.control = control;
        self
    }

    #[must_use]
    pub const fn submitted_by(mut self, submitted_by: Option<PrincipalId>) -> Self {
        self.task.submitted_by = submitted_by;
        self
    }

    #[must_use]
    pub fn idempotency_key(mut self, idempotency_key: Option<String>) -> Self {
        self.task.idempotency_key = idempotency_key;
        self
    }

    #[must_use]
    pub fn request_hash(mut self, request_hash: Option<String>) -> Self {
        self.task.request_hash = request_hash;
        self
    }

    #[must_use]
    pub fn request_payload(mut self, request_payload: Option<Value>) -> Self {
        self.task.request_payload = request_payload;
        self
    }

    #[must_use]
    pub fn checkpoint(mut self, checkpoint: Option<StorageTaskCheckpoint>) -> Self {
        self.task.checkpoint = checkpoint;
        self
    }

    #[must_use]
    pub fn summary(mut self, summary: Option<String>) -> Self {
        self.task.summary = summary;
        self
    }

    #[must_use]
    pub const fn progress(mut self, progress: StorageTaskProgress) -> Self {
        self.task.progress = progress;
        self
    }

    #[must_use]
    pub fn scope_snapshot(mut self, scope_snapshot: StorageTaskScopeSnapshot) -> Self {
        self.task.scope_snapshot = scope_snapshot;
        self
    }

    #[must_use]
    pub const fn request_redacted_at(mut self, request_redacted_at: Option<DateTime<Utc>>) -> Self {
        self.task.request_redacted_at = request_redacted_at;
        self
    }

    #[must_use]
    pub const fn started_at(mut self, started_at: Option<DateTime<Utc>>) -> Self {
        self.task.started_at = started_at;
        self
    }

    #[must_use]
    pub const fn finished_at(mut self, finished_at: Option<DateTime<Utc>>) -> Self {
        self.task.finished_at = finished_at;
        self
    }

    #[must_use]
    pub const fn deletion(
        mut self,
        deleted_at: Option<DateTime<Utc>>,
        deleted_by: Option<PrincipalId>,
    ) -> Self {
        self.task.deleted_at = deleted_at;
        self.task.deleted_by = deleted_by;
        self
    }

    #[must_use]
    pub const fn lease_expires_at(mut self, lease_expires_at: Option<DateTime<Utc>>) -> Self {
        self.task.lease_expires_at = lease_expires_at;
        self
    }

    #[must_use]
    pub const fn attempt_count(mut self, attempt_count: i32) -> Self {
        self.task.attempt_count = attempt_count;
        self
    }

    #[must_use]
    pub const fn initiator_principal_id(
        mut self,
        initiator_principal_id: Option<PrincipalId>,
    ) -> Self {
        self.task.initiator_principal_id = initiator_principal_id;
        self
    }

    #[must_use]
    pub fn trace_link(mut self, trace_link: Option<TraceLink>) -> Self {
        self.task.trace_link = trace_link;
        self
    }

    /// Validate and build a task projection returned by a storage adapter.
    pub fn try_build(self) -> Result<StorageTask, StorageValidationError> {
        if self
            .task
            .metadata
            .as_ref()
            .is_some_and(|value| value.kind() != self.task.kind)
        {
            return Err(StorageValidationError::invalid(
                "Task metadata kind mismatch",
            ));
        }

        if self.task.control.kind() != self.task.kind {
            return Err(StorageValidationError::invalid(
                "Task control kind must match the task",
            ));
        }
        if self.task.control.terminal_reason().is_some()
            && self.task.status != StorageTaskStatus::Cancelled
        {
            return Err(StorageValidationError::invalid(
                "A task stop reason requires cancelled status",
            ));
        }
        if self.task.attempt_count < 0 {
            return Err(StorageValidationError::invalid(
                "Task attempt count must not be negative",
            ));
        }
        if self.task.checkpoint.is_some()
            && (!matches!(
                self.task.kind,
                StorageTaskKind::Export | StorageTaskKind::Backup
            ) || self.task.status.is_terminal()
                || self.task.started_at.is_none()
                || self.task.attempt_count == 0
                || self.task.control.deadline().is_none()
                || self.task.control.phase() != StorageTaskExecutionPhase::Uncommitted)
        {
            return Err(StorageValidationError::invalid(
                "A checkpoint requires a started, uncommitted export or backup task with its original deadline",
            ));
        }
        if self.task.updated_at < self.task.created_at {
            return Err(StorageValidationError::invalid(
                "Task update timestamp must not precede its creation timestamp",
            ));
        }
        if self
            .task
            .started_at
            .is_some_and(|started_at| started_at < self.task.created_at)
        {
            return Err(StorageValidationError::invalid(
                "Task start timestamp must not precede its creation timestamp",
            ));
        }
        if self
            .task
            .started_at
            .is_some_and(|started_at| started_at > self.task.updated_at)
        {
            return Err(StorageValidationError::invalid(
                "Task start timestamp must not follow its update timestamp",
            ));
        }
        if self
            .task
            .finished_at
            .is_some_and(|finished_at| finished_at < self.task.created_at)
        {
            return Err(StorageValidationError::invalid(
                "Task finish timestamp must not precede its creation timestamp",
            ));
        }
        if self
            .task
            .finished_at
            .is_some_and(|finished_at| finished_at > self.task.updated_at)
        {
            return Err(StorageValidationError::invalid(
                "Task finish timestamp must not follow its update timestamp",
            ));
        }
        if matches!(
            (self.task.started_at, self.task.finished_at),
            (Some(started_at), Some(finished_at)) if finished_at < started_at
        ) {
            return Err(StorageValidationError::invalid(
                "Task finish timestamp must not precede its start timestamp",
            ));
        }
        let lifecycle_is_consistent = match self.task.status {
            StorageTaskStatus::Queued => {
                // Durable schema and reindex work can retain its original start
                // timestamp while awaiting a replacement claim after recovery.
                (self.task.started_at.is_none()
                    || (matches!(
                        self.task.kind,
                        StorageTaskKind::SchemaValidation | StorageTaskKind::Reindex
                    ) && self.task.attempt_count > 0)
                    || self.task.checkpoint.is_some())
                    && self.task.finished_at.is_none()
                    && self.task.lease_expires_at.is_none()
            }
            StorageTaskStatus::Waiting => {
                self.task.checkpoint.is_some()
                    && self.task.started_at.is_some()
                    && self.task.finished_at.is_none()
                    && self.task.lease_expires_at.is_none()
            }
            StorageTaskStatus::Validating | StorageTaskStatus::Running => {
                self.task.started_at.is_some()
                    && self.task.finished_at.is_none()
                    && self.task.lease_expires_at.is_some()
            }
            StorageTaskStatus::Succeeded
            | StorageTaskStatus::Failed
            | StorageTaskStatus::PartiallySucceeded
            | StorageTaskStatus::Cancelled => {
                self.task.finished_at.is_some() && self.task.lease_expires_at.is_none()
            }
        };
        if !lifecycle_is_consistent {
            return Err(StorageValidationError::invalid(
                "Task status, lifecycle timestamps, and lease state are inconsistent",
            ));
        }
        for (label, timestamp) in [
            ("redaction", self.task.request_redacted_at),
            ("deletion", self.task.deleted_at),
        ] {
            if timestamp.is_some_and(|timestamp| timestamp < self.task.created_at) {
                return Err(StorageValidationError::invalid(format!(
                    "Task {label} timestamp must not precede its creation timestamp"
                )));
            }
            if timestamp.is_some_and(|timestamp| timestamp > self.task.updated_at) {
                return Err(StorageValidationError::invalid(format!(
                    "Task {label} timestamp must not follow its update timestamp"
                )));
            }
        }
        Ok(self.task)
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageTaskAccess {
    task: StorageTask,
    submitter_owner_group_id: Option<GroupId>,
}

impl StorageTaskAccess {
    #[must_use]
    pub const fn new(task: StorageTask, submitter_owner_group_id: Option<GroupId>) -> Self {
        Self {
            task,
            submitter_owner_group_id,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageTask, Option<GroupId>) {
        (self.task, self.submitter_owner_group_id)
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageTaskChildListQuery {
    task_id: TaskId,
    options: QueryOptions,
}

impl StorageTaskChildListQuery {
    #[must_use]
    pub const fn new(task_id: TaskId, options: QueryOptions) -> Self {
        Self { task_id, options }
    }

    #[must_use]
    pub fn into_parts(self) -> (TaskId, QueryOptions) {
        (self.task_id, self.options)
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageTaskEvent {
    logical_identity: Option<crate::StorageIdentityEvidence>,
    id: EventSequence,
    task_id: TaskId,
    event_type: String,
    message: String,
    data: Option<Value>,
    created_at: DateTime<Utc>,
    actor_principal_id: Option<PrincipalId>,
    actor_kind: String,
    initiator_principal_id: Option<PrincipalId>,
    provenance_task_id: Option<TaskId>,
}

impl StorageTaskEvent {
    #[must_use]
    pub const fn logical_identity(&self) -> Option<&crate::StorageIdentityEvidence> {
        self.logical_identity.as_ref()
    }
    #[must_use]
    pub fn builder(
        id: EventSequence,
        task_id: TaskId,
        event_type: impl Into<String>,
        message: impl Into<String>,
        created_at: DateTime<Utc>,
        actor_kind: impl Into<String>,
    ) -> StorageTaskEventBuilder {
        StorageTaskEventBuilder {
            event: Self {
                logical_identity: None,
                id,
                task_id,
                event_type: event_type.into(),
                message: message.into(),
                data: None,
                created_at,
                actor_principal_id: None,
                actor_kind: actor_kind.into(),
                initiator_principal_id: None,
                provenance_task_id: None,
            },
        }
    }

    #[must_use]
    pub const fn id(&self) -> EventSequence {
        self.id
    }

    #[must_use]
    pub const fn task_id(&self) -> TaskId {
        self.task_id
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
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    #[must_use]
    pub const fn actor_principal_id(&self) -> Option<PrincipalId> {
        self.actor_principal_id
    }

    #[must_use]
    pub fn actor_kind(&self) -> &str {
        &self.actor_kind
    }

    #[must_use]
    pub const fn initiator_principal_id(&self) -> Option<PrincipalId> {
        self.initiator_principal_id
    }

    #[must_use]
    pub const fn provenance_task_id(&self) -> Option<TaskId> {
        self.provenance_task_id
    }
}

pub struct StorageTaskEventBuilder {
    event: StorageTaskEvent,
}

impl StorageTaskEventBuilder {
    #[must_use]
    pub fn logical_identity(mut self, evidence: crate::StorageIdentityEvidence) -> Self {
        self.event.logical_identity = Some(evidence);
        self
    }

    #[must_use]
    pub fn data(mut self, data: Option<Value>) -> Self {
        self.event.data = data;
        self
    }

    #[must_use]
    pub const fn actor_principal_id(mut self, actor_principal_id: Option<PrincipalId>) -> Self {
        self.event.actor_principal_id = actor_principal_id;
        self
    }

    #[must_use]
    pub const fn provenance(
        mut self,
        initiator_principal_id: Option<PrincipalId>,
        provenance_task_id: Option<TaskId>,
    ) -> Self {
        self.event.initiator_principal_id = initiator_principal_id;
        self.event.provenance_task_id = provenance_task_id;
        self
    }

    #[must_use]
    pub fn build(self) -> StorageTaskEvent {
        self.event
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageImportTaskResult {
    id: ImportTaskResultId,
    task_id: TaskId,
    item_ref: Option<String>,
    entity_kind: String,
    action: String,
    identifier: Option<String>,
    outcome: String,
    error: Option<String>,
    details: Option<Value>,
    created_at: DateTime<Utc>,
}

impl StorageImportTaskResult {
    #[must_use]
    pub fn builder(
        id: ImportTaskResultId,
        task_id: TaskId,
        entity_kind: impl Into<String>,
        action: impl Into<String>,
        outcome: impl Into<String>,
        created_at: DateTime<Utc>,
    ) -> StorageImportTaskResultBuilder {
        StorageImportTaskResultBuilder {
            result: Self {
                id,
                task_id,
                item_ref: None,
                entity_kind: entity_kind.into(),
                action: action.into(),
                identifier: None,
                outcome: outcome.into(),
                error: None,
                details: None,
                created_at,
            },
        }
    }

    #[must_use]
    pub const fn id(&self) -> ImportTaskResultId {
        self.id
    }

    #[must_use]
    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    #[must_use]
    pub fn item_ref(&self) -> Option<&str> {
        self.item_ref.as_deref()
    }

    #[must_use]
    pub fn entity_kind(&self) -> &str {
        &self.entity_kind
    }

    #[must_use]
    pub fn action(&self) -> &str {
        &self.action
    }

    #[must_use]
    pub fn identifier(&self) -> Option<&str> {
        self.identifier.as_deref()
    }

    #[must_use]
    pub fn outcome(&self) -> &str {
        &self.outcome
    }

    #[must_use]
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    #[must_use]
    pub const fn details(&self) -> Option<&Value> {
        self.details.as_ref()
    }

    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}

pub struct StorageImportTaskResultBuilder {
    result: StorageImportTaskResult,
}

impl StorageImportTaskResultBuilder {
    #[must_use]
    pub fn item_ref(mut self, item_ref: Option<String>) -> Self {
        self.result.item_ref = item_ref;
        self
    }

    #[must_use]
    pub fn identifier(mut self, identifier: Option<String>) -> Self {
        self.result.identifier = identifier;
        self
    }

    #[must_use]
    pub fn error(mut self, error: Option<String>) -> Self {
        self.result.error = error;
        self
    }

    #[must_use]
    pub fn details(mut self, details: Option<Value>) -> Self {
        self.result.details = details;
        self
    }

    #[must_use]
    pub fn build(self) -> StorageImportTaskResult {
        self.result
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageTaskDurations {
    total_ms: i32,
    query_ms: i32,
    hydration_ms: i32,
    render_ms: i32,
}

impl StorageTaskDurations {
    /// Construct non-negative phase durations for a task output.
    pub fn try_new(
        total_ms: i32,
        query_ms: i32,
        hydration_ms: i32,
        render_ms: i32,
    ) -> Result<Self, StorageValidationError> {
        if [total_ms, query_ms, hydration_ms, render_ms]
            .into_iter()
            .any(|duration| duration < 0)
        {
            return Err(StorageValidationError::invalid(
                "Task output durations must not be negative",
            ));
        }
        Ok(Self {
            total_ms,
            query_ms,
            hydration_ms,
            render_ms,
        })
    }

    #[must_use]
    pub const fn total_ms(self) -> i32 {
        self.total_ms
    }

    #[must_use]
    pub const fn query_ms(self) -> i32 {
        self.query_ms
    }

    #[must_use]
    pub const fn hydration_ms(self) -> i32 {
        self.hydration_ms
    }

    #[must_use]
    pub const fn render_ms(self) -> i32 {
        self.render_ms
    }
}

macro_rules! task_output_accessors {
    () => {
        #[must_use]
        pub const fn task_id(&self) -> TaskId {
            self.task_id
        }

        #[must_use]
        pub fn template_name(&self) -> Option<&str> {
            self.template_name.as_deref()
        }

        #[must_use]
        pub fn content_type(&self) -> &str {
            &self.content_type
        }

        #[must_use]
        pub const fn warning_count(&self) -> i32 {
            self.warning_count
        }

        #[must_use]
        pub const fn truncated(&self) -> bool {
            self.truncated
        }

        #[must_use]
        pub const fn output_expires_at(&self) -> DateTime<Utc> {
            self.output_expires_at
        }

        #[must_use]
        pub const fn durations(&self) -> StorageTaskDurations {
            self.durations
        }
    };
}

macro_rules! backup_output_accessors {
    () => {
        #[must_use]
        pub const fn task_id(&self) -> TaskId {
            self.task_id
        }

        #[must_use]
        pub const fn byte_size(&self) -> i64 {
            self.byte_size
        }

        #[must_use]
        pub fn sha256(&self) -> &str {
            &self.sha256
        }

        #[must_use]
        pub const fn output_expires_at(&self) -> DateTime<Utc> {
            self.output_expires_at
        }
    };
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageExportOutputSummary {
    task_id: TaskId,
    template_name: Option<String>,
    content_type: String,
    warning_count: i32,
    truncated: bool,
    output_expires_at: DateTime<Utc>,
    durations: StorageTaskDurations,
}

impl StorageExportOutputSummary {
    pub fn try_new(
        task_id: TaskId,
        template_name: Option<String>,
        content_type: impl Into<String>,
        warning_count: i32,
        truncated: bool,
        output_expires_at: DateTime<Utc>,
        durations: StorageTaskDurations,
    ) -> Result<Self, StorageValidationError> {
        if warning_count < 0 {
            return Err(StorageValidationError::invalid(
                "Export output warning count must not be negative",
            ));
        }
        Ok(Self {
            task_id,
            template_name,
            content_type: content_type.into(),
            warning_count,
            truncated,
            output_expires_at,
            durations,
        })
    }

    task_output_accessors!();
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageBackupOutputSummary {
    task_id: TaskId,
    byte_size: i64,
    sha256: String,
    output_expires_at: DateTime<Utc>,
}

impl StorageBackupOutputSummary {
    pub fn try_new(
        task_id: TaskId,
        byte_size: i64,
        sha256: impl Into<String>,
        output_expires_at: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        let sha256 = sha256.into();
        validate_backup_output_identity(byte_size, &sha256)?;
        Ok(Self {
            task_id,
            byte_size,
            sha256,
            output_expires_at,
        })
    }

    backup_output_accessors!();
}

#[derive(Clone, PartialEq)]
pub struct StorageExportOutput {
    artifact_reference: Option<ArtifactReference>,
    task_id: TaskId,
    template_name: Option<String>,
    content_type: String,
    json_output: Option<Value>,
    text_output: Option<String>,
    metadata: Value,
    warnings: Value,
    warning_count: i32,
    truncated: bool,
    output_expires_at: DateTime<Utc>,
    durations: StorageTaskDurations,
    created_at: DateTime<Utc>,
}

impl StorageExportOutput {
    pub fn artifact_reference(&self) -> Option<&ArtifactReference> {
        self.artifact_reference.as_ref()
    }
    #[must_use]
    pub fn builder(
        task_id: TaskId,
        content_type: impl Into<String>,
        metadata: Value,
        warnings: Value,
        output_expires_at: DateTime<Utc>,
        created_at: DateTime<Utc>,
    ) -> StorageExportOutputBuilder {
        StorageExportOutputBuilder {
            output: Self {
                artifact_reference: None,
                task_id,
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
                created_at,
            },
        }
    }

    task_output_accessors!();

    #[must_use]
    pub const fn json_output(&self) -> Option<&Value> {
        self.json_output.as_ref()
    }

    #[must_use]
    pub fn text_output(&self) -> Option<&str> {
        self.text_output.as_deref()
    }

    #[must_use]
    pub const fn metadata(&self) -> &Value {
        &self.metadata
    }

    #[must_use]
    pub const fn warnings(&self) -> &Value {
        &self.warnings
    }

    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}

pub struct StorageExportOutputBuilder {
    output: StorageExportOutput,
}

impl StorageExportOutputBuilder {
    pub fn artifact_reference(mut self, reference: Option<ArtifactReference>) -> Self {
        self.output.artifact_reference = reference;
        self
    }
    #[must_use]
    pub fn template_name(mut self, template_name: Option<String>) -> Self {
        self.output.template_name = template_name;
        self
    }

    #[must_use]
    pub fn output(mut self, json_output: Option<Value>, text_output: Option<String>) -> Self {
        self.output.json_output = json_output;
        self.output.text_output = text_output;
        self
    }

    #[must_use]
    pub const fn warning_state(mut self, warning_count: i32, truncated: bool) -> Self {
        self.output.warning_count = warning_count;
        self.output.truncated = truncated;
        self
    }

    #[must_use]
    pub const fn durations(mut self, durations: StorageTaskDurations) -> Self {
        self.output.durations = durations;
        self
    }

    pub fn try_build(self) -> Result<StorageExportOutput, StorageValidationError> {
        if self.output.warning_count < 0 {
            return Err(StorageValidationError::invalid(
                "Export output warning count must not be negative",
            ));
        }
        match (
            &self.output.json_output,
            &self.output.text_output,
            &self.output.artifact_reference,
        ) {
            (Some(_), None, None) | (None, Some(_), None) | (None, None, Some(_)) => {
                Ok(self.output)
            }
            _ => Err(StorageValidationError::invalid(
                "Export output must contain exactly one of JSON or text output",
            )),
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageBackupOutput {
    artifact_reference: Option<ArtifactReference>,
    task_id: TaskId,
    document: Vec<u8>,
    byte_size: i64,
    sha256: String,
    output_expires_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
}

impl StorageBackupOutput {
    pub fn artifact_reference(&self) -> Option<&ArtifactReference> {
        self.artifact_reference.as_ref()
    }
    pub fn from_reference(
        task_id: TaskId,
        reference: ArtifactReference,
        byte_size: i64,
        sha256: String,
        output_expires_at: DateTime<Utc>,
        created_at: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        validate_backup_output_identity(byte_size, &sha256)?;
        Ok(Self {
            task_id,
            artifact_reference: Some(reference),
            document: Vec::new(),
            byte_size,
            sha256,
            output_expires_at,
            created_at,
        })
    }
    pub fn try_new(
        task_id: TaskId,
        document: Vec<u8>,
        byte_size: i64,
        sha256: impl Into<String>,
        output_expires_at: DateTime<Utc>,
        created_at: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        let sha256 = sha256.into();
        validate_backup_output_identity(byte_size, &sha256)?;
        let document_size = i64::try_from(document.len()).map_err(|_| {
            StorageValidationError::too_large(
                "Backup output document exceeds the supported byte-size range",
            )
        })?;
        if byte_size != document_size {
            return Err(StorageValidationError::invalid(
                "Backup output byte size must match the document length",
            ));
        }
        let expected_sha256 = Sha256::digest(&document)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        if sha256 != expected_sha256 {
            return Err(StorageValidationError::invalid(
                "Backup output SHA-256 digest must match the document",
            ));
        }
        Ok(Self {
            artifact_reference: None,
            task_id,
            document,
            byte_size,
            sha256,
            output_expires_at,
            created_at,
        })
    }

    backup_output_accessors!();

    #[must_use]
    pub fn document(&self) -> Option<&[u8]> {
        self.artifact_reference
            .is_none()
            .then_some(self.document.as_slice())
    }

    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}

fn validate_backup_output_identity(
    byte_size: i64,
    sha256: &str,
) -> Result<(), StorageValidationError> {
    if byte_size < 0 {
        return Err(StorageValidationError::invalid(
            "Backup output byte size must not be negative",
        ));
    }
    if sha256.len() != 64
        || !sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(StorageValidationError::invalid(
            "Backup output SHA-256 digest must contain exactly 64 lowercase hexadecimal characters",
        ));
    }
    Ok(())
}

#[derive(Clone, PartialEq)]
pub enum StorageTaskOutputLookup<T> {
    Available(T),
    Expired { expires_at: DateTime<Utc> },
    Missing,
}

#[cfg(test)]
mod tests;
