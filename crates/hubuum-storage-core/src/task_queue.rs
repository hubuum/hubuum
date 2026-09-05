use std::fmt;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hubuum_domain::{GroupId, ImportTaskResultId, PrincipalId, TaskId, TokenId};
use hubuum_events_core::{EventSequence, TraceLink};
use hubuum_query::QueryOptions;
use hubuum_task_core::IdempotencyKey;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{StorageError, StoragePage, StorageValidationError};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StorageTaskKind {
    Import,
    Export,
    Backup,
    Reindex,
    RemoteCall,
}

impl StorageTaskKind {
    pub const ALL: [Self; 5] = [
        Self::Import,
        Self::Export,
        Self::Backup,
        Self::Reindex,
        Self::RemoteCall,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Import => "import",
            Self::Export => "export",
            Self::Backup => "backup",
            Self::Reindex => "reindex",
            Self::RemoteCall => "remote_call",
        }
    }

    #[must_use]
    pub fn from_persisted(value: &str) -> Option<Self> {
        match value {
            "import" => Some(Self::Import),
            "export" => Some(Self::Export),
            "backup" => Some(Self::Backup),
            "reindex" => Some(Self::Reindex),
            "remote_call" => Some(Self::RemoteCall),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StorageTaskStatus {
    Queued,
    Validating,
    Running,
    Succeeded,
    Failed,
    PartiallySucceeded,
    Cancelled,
}

impl StorageTaskStatus {
    pub const ALL: [Self; 7] = [
        Self::Queued,
        Self::Validating,
        Self::Running,
        Self::Succeeded,
        Self::Failed,
        Self::PartiallySucceeded,
        Self::Cancelled,
    ];
    pub const ACTIVE: [Self; 2] = [Self::Validating, Self::Running];
    pub const TERMINAL: [Self; 4] = [
        Self::Succeeded,
        Self::Failed,
        Self::PartiallySucceeded,
        Self::Cancelled,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Validating => "validating",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::PartiallySucceeded => "partially_succeeded",
            Self::Cancelled => "cancelled",
        }
    }

    #[must_use]
    pub fn from_persisted(value: &str) -> Option<Self> {
        match value {
            "queued" => Some(Self::Queued),
            "validating" => Some(Self::Validating),
            "running" => Some(Self::Running),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "partially_succeeded" => Some(Self::PartiallySucceeded),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }

    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Validating | Self::Running)
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::PartiallySucceeded | Self::Cancelled
        )
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
    kind: StorageTaskKind,
    submitted_by: PrincipalId,
    request_payload: Value,
    total_items: i32,
    idempotency_key: Option<IdempotencyKey>,
    request_hash: Option<String>,
    scope_snapshot: StorageTaskScopeSnapshot,
    trace_link: Option<TraceLink>,
    maximum_active_tasks: usize,
}

impl StorageTaskCreateRequest {
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
            total_items,
            idempotency_key: None,
            request_hash: None,
            scope_snapshot: StorageTaskScopeSnapshot::unscoped(),
            trace_link: None,
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

    #[must_use]
    pub const fn maximum_active_tasks(&self) -> usize {
        self.maximum_active_tasks
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
            .field("maximum_active_tasks", &self.maximum_active_tasks)
            .field("identity_and_payload", &"[redacted]")
            .finish()
    }
}

pub struct StorageTaskCreateRequestBuilder {
    kind: StorageTaskKind,
    submitted_by: PrincipalId,
    request_payload: Value,
    total_items: i32,
    idempotency_key: Option<IdempotencyKey>,
    request_hash: Option<String>,
    scope_snapshot: StorageTaskScopeSnapshot,
    trace_link: Option<TraceLink>,
}

impl StorageTaskCreateRequestBuilder {
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

    pub fn try_build(
        self,
        maximum_active_tasks: usize,
    ) -> Result<StorageTaskCreateRequest, StorageError> {
        if self.total_items < 0 {
            return Err(StorageError::invalid_input(
                "Task total_items must not be negative",
            ));
        }
        if maximum_active_tasks == 0 {
            return Err(StorageError::invalid_input(
                "Task maximum_active_tasks must be greater than zero",
            ));
        }

        Ok(StorageTaskCreateRequest {
            kind: self.kind,
            submitted_by: self.submitted_by,
            request_payload: self.request_payload,
            total_items: self.total_items,
            idempotency_key: self.idempotency_key,
            request_hash: self.request_hash,
            scope_snapshot: self.scope_snapshot,
            trace_link: self.trace_link,
            maximum_active_tasks,
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
}

impl StorageTask {
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
            },
        }
    }

    #[must_use]
    pub const fn id(&self) -> TaskId {
        self.id
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
        if self.task.attempt_count < 0 {
            return Err(StorageValidationError::invalid(
                "Task attempt count must not be negative",
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
                self.task.started_at.is_none()
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
pub struct StorageTaskListQuery {
    submitted_by: Option<PrincipalId>,
    kind: Option<StorageTaskKind>,
    status: Option<StorageTaskStatus>,
    options: QueryOptions,
}

impl StorageTaskListQuery {
    #[must_use]
    pub const fn new(
        submitted_by: Option<PrincipalId>,
        kind: Option<StorageTaskKind>,
        status: Option<StorageTaskStatus>,
        options: QueryOptions,
    ) -> Self {
        Self {
            submitted_by,
            kind,
            status,
            options,
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        Option<PrincipalId>,
        Option<StorageTaskKind>,
        Option<StorageTaskStatus>,
        QueryOptions,
    ) {
        (self.submitted_by, self.kind, self.status, self.options)
    }
}

impl fmt::Debug for StorageTaskListQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageTaskListQuery")
            .field("has_submitter", &self.submitted_by.is_some())
            .field("kind", &self.kind)
            .field("status", &self.status)
            .field("filter_count", &self.options.filters().len())
            .field("sort_count", &self.options.sort().len())
            .field("limit", &self.options.limit())
            .field("has_cursor", &self.options.cursor().is_some())
            .field("include_total", &self.options.include_total())
            .finish_non_exhaustive()
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
        match (&self.output.json_output, &self.output.text_output) {
            (Some(_), None) | (None, Some(_)) => Ok(self.output),
            _ => Err(StorageValidationError::invalid(
                "Export output must contain exactly one of JSON or text output",
            )),
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageBackupOutput {
    task_id: TaskId,
    document: Vec<u8>,
    byte_size: i64,
    sha256: String,
    output_expires_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
}

impl StorageBackupOutput {
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
    pub fn document(&self) -> &[u8] {
        &self.document
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

#[async_trait]
pub trait TaskQueueStorage: Send + Sync {
    async fn create_task(
        &self,
        request: StorageTaskCreateRequest,
    ) -> Result<StorageTask, StorageError>;

    async fn get_task_access(&self, task_id: TaskId) -> Result<StorageTaskAccess, StorageError>;

    async fn list_tasks(
        &self,
        query: StorageTaskListQuery,
    ) -> Result<StoragePage<StorageTask>, StorageError>;

    async fn list_task_events(
        &self,
        query: StorageTaskChildListQuery,
    ) -> Result<StoragePage<StorageTaskEvent>, StorageError>;

    async fn list_import_task_results(
        &self,
        query: StorageTaskChildListQuery,
    ) -> Result<StoragePage<StorageImportTaskResult>, StorageError>;

    async fn list_export_output_summaries(
        &self,
        task_ids: Vec<TaskId>,
    ) -> Result<Vec<StorageExportOutputSummary>, StorageError>;

    async fn list_backup_output_summaries(
        &self,
        task_ids: Vec<TaskId>,
    ) -> Result<Vec<StorageBackupOutputSummary>, StorageError>;

    async fn get_export_output_summary(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageExportOutputSummary>, StorageError>;

    async fn get_backup_output_summary(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageBackupOutputSummary>, StorageError>;

    async fn get_export_output(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageExportOutput>, StorageError>;

    async fn get_backup_output(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageBackupOutput>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn export_output_builder() -> StorageExportOutputBuilder {
        let now = chrono::Utc::now();
        StorageExportOutput::builder(
            TaskId::new(91_000).unwrap(),
            "application/json",
            serde_json::json!({}),
            serde_json::json!([]),
            now,
            now,
        )
        .output(Some(serde_json::json!({})), None)
    }

    #[test]
    fn export_output_summary_rejects_a_negative_warning_count() {
        let error = StorageExportOutputSummary::try_new(
            TaskId::new(91_000).unwrap(),
            None,
            "application/json",
            -1,
            false,
            chrono::Utc::now(),
            StorageTaskDurations::default(),
        )
        .err()
        .expect("negative warning counts must be rejected");

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn export_output_rejects_a_negative_warning_count() {
        let error = export_output_builder()
            .warning_state(-1, false)
            .try_build()
            .err()
            .expect("negative warning counts must be rejected");

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn export_output_rejects_missing_content() {
        let error = export_output_builder()
            .output(None, None)
            .try_build()
            .err()
            .expect("missing export output must be rejected");

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn export_output_rejects_multiple_content_representations() {
        let error = export_output_builder()
            .output(
                Some(serde_json::json!({})),
                Some("duplicate output".to_string()),
            )
            .try_build()
            .err()
            .expect("ambiguous export output must be rejected");

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn backup_output_summary_rejects_a_negative_size() {
        let error = StorageBackupOutputSummary::try_new(
            TaskId::new(91_000).unwrap(),
            -1,
            "0000000000000000000000000000000000000000000000000000000000000000",
            chrono::Utc::now(),
        )
        .err()
        .expect("negative backup output size must be rejected");

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn backup_output_summary_rejects_a_malformed_digest() {
        let error = StorageBackupOutputSummary::try_new(
            TaskId::new(91_000).unwrap(),
            0,
            "not-a-digest",
            chrono::Utc::now(),
        )
        .err()
        .expect("malformed backup output digest must be rejected");

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn backup_output_rejects_a_size_that_disagrees_with_the_document() {
        let error = StorageBackupOutput::try_new(
            TaskId::new(91_000).unwrap(),
            b"{}".to_vec(),
            3,
            "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
            chrono::Utc::now(),
            chrono::Utc::now(),
        )
        .err()
        .expect("mismatched backup output size must be rejected");

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn backup_output_rejects_a_digest_that_disagrees_with_the_document() {
        let error = StorageBackupOutput::try_new(
            TaskId::new(91_000).unwrap(),
            b"{}".to_vec(),
            2,
            "0000000000000000000000000000000000000000000000000000000000000000",
            chrono::Utc::now(),
            chrono::Utc::now(),
        )
        .err()
        .expect("mismatched backup output digest must be rejected");

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn task_durations_reject_negative_phases() {
        for durations in [(-1, 0, 0, 0), (0, -1, 0, 0), (0, 0, -1, 0), (0, 0, 0, -1)] {
            let error =
                StorageTaskDurations::try_new(durations.0, durations.1, durations.2, durations.3)
                    .expect_err("negative task durations must be rejected");

            assert_eq!(
                error.kind(),
                crate::StorageValidationErrorKind::InvalidValue
            );
        }
    }

    #[test]
    fn task_create_rejects_negative_total_items() {
        let error = StorageTaskCreateRequest::builder(
            StorageTaskKind::Import,
            PrincipalId::new(91_001).unwrap(),
            serde_json::json!({}),
            -1,
        )
        .try_build(1)
        .expect_err("negative task totals must be rejected");

        assert_eq!(error.kind(), crate::StorageErrorKind::InvalidInput);
    }

    #[test]
    fn task_create_rejects_zero_capacity() {
        let error = StorageTaskCreateRequest::builder(
            StorageTaskKind::Import,
            PrincipalId::new(91_001).unwrap(),
            serde_json::json!({}),
            0,
        )
        .try_build(0)
        .expect_err("zero task capacity must be rejected");

        assert_eq!(error.kind(), crate::StorageErrorKind::InvalidInput);
    }

    #[test]
    fn task_create_and_projection_preserve_only_a_typed_trace_link() {
        let link =
            TraceLink::new("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", 1, 0).unwrap();
        let request = StorageTaskCreateRequest::builder(
            StorageTaskKind::Import,
            PrincipalId::new(91_001).unwrap(),
            serde_json::json!({}),
            0,
        )
        .trace_link(Some(link.clone()))
        .try_build(1)
        .unwrap();
        assert_eq!(request.trace_link(), Some(&link));

        let now = chrono::Utc::now();
        let task = StorageTask::builder(
            TaskId::new(91_002).unwrap(),
            StorageTaskKind::Import,
            StorageTaskStatus::Queued,
            now,
            now,
        )
        .trace_link(Some(link.clone()))
        .try_build()
        .unwrap();
        assert_eq!(task.trace_link(), Some(&link));
        assert!(!format!("{task:?}").contains(link.trace_id()));
    }

    #[test]
    fn task_progress_rejects_negative_persisted_counts() {
        for counts in [(-1, 0, 0, 0), (0, -1, 0, 0), (0, 0, -1, 0), (0, 0, 0, -1)] {
            let error = StorageTaskProgress::try_new(counts.0, counts.1, counts.2, counts.3)
                .expect_err("negative persisted progress must be rejected");

            assert_eq!(
                error.kind(),
                crate::StorageValidationErrorKind::InvalidValue
            );
        }
    }

    #[test]
    fn task_projection_rejects_negative_attempt_count() {
        let now = chrono::Utc::now();
        let error = StorageTask::builder(
            TaskId::new(91_010).unwrap(),
            StorageTaskKind::Import,
            StorageTaskStatus::Queued,
            now,
            now,
        )
        .attempt_count(-1)
        .try_build()
        .expect_err("a negative persisted attempt count must be rejected");

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn task_projection_rejects_reversed_timestamps() {
        let created_at = chrono::Utc::now();
        let earlier = created_at - chrono::Duration::seconds(1);
        let error = StorageTask::builder(
            TaskId::new(91_011).unwrap(),
            StorageTaskKind::Import,
            StorageTaskStatus::Queued,
            created_at,
            earlier,
        )
        .try_build()
        .expect_err("a reversed persisted task interval must be rejected");

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn queued_task_projection_rejects_a_start_timestamp() {
        let now = chrono::Utc::now();
        let error = StorageTask::builder(
            TaskId::new(91_012).unwrap(),
            StorageTaskKind::Import,
            StorageTaskStatus::Queued,
            now,
            now,
        )
        .started_at(Some(now))
        .try_build()
        .unwrap_err();

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn active_task_projection_requires_a_start_timestamp_and_lease() {
        let now = chrono::Utc::now();
        let error = StorageTask::builder(
            TaskId::new(91_013).unwrap(),
            StorageTaskKind::Import,
            StorageTaskStatus::Running,
            now,
            now,
        )
        .try_build()
        .unwrap_err();

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn terminal_task_projection_requires_a_finish_timestamp() {
        let now = chrono::Utc::now();
        let error = StorageTask::builder(
            TaskId::new(91_014).unwrap(),
            StorageTaskKind::Import,
            StorageTaskStatus::Succeeded,
            now,
            now,
        )
        .try_build()
        .unwrap_err();

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn terminal_task_projection_rejects_a_lease() {
        let now = chrono::Utc::now();
        let error = StorageTask::builder(
            TaskId::new(91_015).unwrap(),
            StorageTaskKind::Import,
            StorageTaskStatus::Succeeded,
            now,
            now,
        )
        .finished_at(Some(now))
        .lease_expires_at(Some(now))
        .try_build()
        .unwrap_err();

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn cancelled_task_projection_allows_cancellation_before_claim() {
        let now = chrono::Utc::now();
        let task = StorageTask::builder(
            TaskId::new(91_016).unwrap(),
            StorageTaskKind::Import,
            StorageTaskStatus::Cancelled,
            now,
            now,
        )
        .finished_at(Some(now))
        .try_build();

        assert!(task.is_ok());
    }

    #[test]
    fn task_debug_redacts_identity_payload_and_scope() {
        let now = chrono::Utc::now();
        let task = StorageTask::builder(
            TaskId::new(91_001).unwrap(),
            StorageTaskKind::Import,
            StorageTaskStatus::Running,
            now,
            now,
        )
        .submitted_by(Some(PrincipalId::new(91_002).unwrap()))
        .idempotency_key(Some("secret idempotency".to_string()))
        .request_hash(Some("secret hash".to_string()))
        .request_payload(Some(serde_json::json!({"secret": "payload"})))
        .summary(Some("summary".to_string()))
        .progress(StorageTaskProgress::try_new(2, 1, 1, 0).unwrap())
        .scope_snapshot(StorageTaskScopeSnapshot::new(
            Some(TokenId::new(91_003).unwrap()),
            true,
            serde_json::json!({"permissions": ["secret"]}),
        ))
        .started_at(Some(now))
        .lease_expires_at(Some(now))
        .attempt_count(1)
        .initiator_principal_id(Some(PrincipalId::new(91_004).unwrap()))
        .try_build()
        .unwrap();

        let debug = format!("{task:?}");

        for secret in [
            "91001",
            "91002",
            "91003",
            "91004",
            "secret idempotency",
            "secret hash",
            "secret\": \"payload",
            "permissions\": [\"secret",
        ] {
            assert!(!debug.contains(secret));
        }
        assert!(debug.contains("Import"));
        assert!(debug.contains("Running"));
        assert!(debug.contains("has_lease: true"));
    }
}
