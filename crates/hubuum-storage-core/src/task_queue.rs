use std::fmt;

use async_trait::async_trait;
use chrono::NaiveDateTime;
use hubuum_query::QueryOptions;
use hubuum_task_core::IdempotencyKey;
use serde_json::Value;
use uuid::Uuid;

use crate::StorageError;

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
    #[must_use]
    pub const fn new(total: i32, processed: i32, succeeded: i32, failed: i32) -> Self {
        Self {
            total,
            processed,
            succeeded,
            failed,
        }
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
    token_id: Option<i32>,
    scoped: bool,
    scopes: Value,
}

impl StorageTaskScopeSnapshot {
    #[must_use]
    pub const fn new(token_id: Option<i32>, scoped: bool, scopes: Value) -> Self {
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
    pub const fn token_id(&self) -> Option<i32> {
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
    submitted_by: i32,
    request_payload: Value,
    total_items: i32,
    idempotency_key: Option<IdempotencyKey>,
    request_hash: Option<String>,
    scope_snapshot: StorageTaskScopeSnapshot,
    maximum_active_tasks: usize,
}

impl StorageTaskCreateRequest {
    #[must_use]
    pub fn builder(
        kind: StorageTaskKind,
        submitted_by: i32,
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
        }
    }

    #[must_use]
    pub const fn kind(&self) -> StorageTaskKind {
        self.kind
    }

    #[must_use]
    pub const fn submitted_by(&self) -> i32 {
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
            .field("maximum_active_tasks", &self.maximum_active_tasks)
            .field("identity_and_payload", &"[redacted]")
            .finish()
    }
}

pub struct StorageTaskCreateRequestBuilder {
    kind: StorageTaskKind,
    submitted_by: i32,
    request_payload: Value,
    total_items: i32,
    idempotency_key: Option<IdempotencyKey>,
    request_hash: Option<String>,
    scope_snapshot: StorageTaskScopeSnapshot,
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
    pub fn build(self, maximum_active_tasks: usize) -> StorageTaskCreateRequest {
        StorageTaskCreateRequest {
            kind: self.kind,
            submitted_by: self.submitted_by,
            request_payload: self.request_payload,
            total_items: self.total_items,
            idempotency_key: self.idempotency_key,
            request_hash: self.request_hash,
            scope_snapshot: self.scope_snapshot,
            maximum_active_tasks,
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageTask {
    id: i32,
    kind: StorageTaskKind,
    status: StorageTaskStatus,
    submitted_by: Option<i32>,
    idempotency_key: Option<String>,
    request_hash: Option<String>,
    request_payload: Option<Value>,
    summary: Option<String>,
    progress: StorageTaskProgress,
    scope_snapshot: StorageTaskScopeSnapshot,
    request_redacted_at: Option<NaiveDateTime>,
    started_at: Option<NaiveDateTime>,
    finished_at: Option<NaiveDateTime>,
    deleted_at: Option<NaiveDateTime>,
    deleted_by: Option<i32>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    lease_token: Option<Uuid>,
    lease_expires_at: Option<NaiveDateTime>,
    attempt_count: i32,
    initiator_principal_id: Option<i32>,
}

impl StorageTask {
    #[must_use]
    pub fn builder(
        id: i32,
        kind: StorageTaskKind,
        status: StorageTaskStatus,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
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
                lease_token: None,
                lease_expires_at: None,
                attempt_count: 0,
                initiator_principal_id: None,
            },
        }
    }

    #[must_use]
    pub const fn id(&self) -> i32 {
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
    pub const fn submitted_by(&self) -> Option<i32> {
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
    pub const fn created_at(&self) -> NaiveDateTime {
        self.created_at
    }

    #[must_use]
    pub const fn started_at(&self) -> Option<NaiveDateTime> {
        self.started_at
    }

    #[must_use]
    pub const fn finished_at(&self) -> Option<NaiveDateTime> {
        self.finished_at
    }

    #[must_use]
    pub const fn request_redacted_at(&self) -> Option<NaiveDateTime> {
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
    pub const fn deleted_at(&self) -> Option<NaiveDateTime> {
        self.deleted_at
    }

    #[must_use]
    pub const fn deleted_by(&self) -> Option<i32> {
        self.deleted_by
    }

    #[must_use]
    pub const fn updated_at(&self) -> NaiveDateTime {
        self.updated_at
    }

    #[must_use]
    pub const fn lease_token(&self) -> Option<Uuid> {
        self.lease_token
    }

    #[must_use]
    pub const fn lease_expires_at(&self) -> Option<NaiveDateTime> {
        self.lease_expires_at
    }

    #[must_use]
    pub const fn attempt_count(&self) -> i32 {
        self.attempt_count
    }

    #[must_use]
    pub const fn initiator_principal_id(&self) -> Option<i32> {
        self.initiator_principal_id
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
            .field("has_lease", &self.lease_token.is_some())
            .field("identity_and_payload", &"[redacted]")
            .finish_non_exhaustive()
    }
}

pub struct StorageTaskBuilder {
    task: StorageTask,
}

impl StorageTaskBuilder {
    #[must_use]
    pub const fn submitted_by(mut self, submitted_by: Option<i32>) -> Self {
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
    pub const fn request_redacted_at(mut self, request_redacted_at: Option<NaiveDateTime>) -> Self {
        self.task.request_redacted_at = request_redacted_at;
        self
    }

    #[must_use]
    pub const fn started_at(mut self, started_at: Option<NaiveDateTime>) -> Self {
        self.task.started_at = started_at;
        self
    }

    #[must_use]
    pub const fn finished_at(mut self, finished_at: Option<NaiveDateTime>) -> Self {
        self.task.finished_at = finished_at;
        self
    }

    #[must_use]
    pub const fn deletion(
        mut self,
        deleted_at: Option<NaiveDateTime>,
        deleted_by: Option<i32>,
    ) -> Self {
        self.task.deleted_at = deleted_at;
        self.task.deleted_by = deleted_by;
        self
    }

    #[must_use]
    pub const fn lease(
        mut self,
        lease_token: Option<Uuid>,
        lease_expires_at: Option<NaiveDateTime>,
    ) -> Self {
        self.task.lease_token = lease_token;
        self.task.lease_expires_at = lease_expires_at;
        self
    }

    #[must_use]
    pub const fn attempt_count(mut self, attempt_count: i32) -> Self {
        self.task.attempt_count = attempt_count;
        self
    }

    #[must_use]
    pub const fn initiator_principal_id(mut self, initiator_principal_id: Option<i32>) -> Self {
        self.task.initiator_principal_id = initiator_principal_id;
        self
    }

    #[must_use]
    pub fn build(self) -> StorageTask {
        self.task
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageTaskAccess {
    task: StorageTask,
    submitter_owner_group_id: Option<i32>,
}

impl StorageTaskAccess {
    #[must_use]
    pub const fn new(task: StorageTask, submitter_owner_group_id: Option<i32>) -> Self {
        Self {
            task,
            submitter_owner_group_id,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageTask, Option<i32>) {
        (self.task, self.submitter_owner_group_id)
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageTaskListQuery {
    submitted_by: Option<i32>,
    kind: Option<StorageTaskKind>,
    status: Option<StorageTaskStatus>,
    options: QueryOptions,
}

impl StorageTaskListQuery {
    #[must_use]
    pub const fn new(
        submitted_by: Option<i32>,
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
        Option<i32>,
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
pub struct StorageTaskPage {
    tasks: Vec<StorageTask>,
    total: Option<i64>,
}

impl StorageTaskPage {
    #[must_use]
    pub const fn new(tasks: Vec<StorageTask>, total: Option<i64>) -> Self {
        Self { tasks, total }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<StorageTask>, Option<i64>) {
        (self.tasks, self.total)
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageTaskPageQuery {
    task_id: i32,
    options: QueryOptions,
}

impl StorageTaskPageQuery {
    #[must_use]
    pub const fn new(task_id: i32, options: QueryOptions) -> Self {
        Self { task_id, options }
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, QueryOptions) {
        (self.task_id, self.options)
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageTaskEvent {
    id: i64,
    task_id: i32,
    event_type: String,
    message: String,
    data: Option<Value>,
    created_at: NaiveDateTime,
    actor_principal_id: Option<i32>,
    actor_kind: String,
    initiator_principal_id: Option<i32>,
    provenance_task_id: Option<i32>,
}

impl StorageTaskEvent {
    #[must_use]
    pub fn builder(
        id: i64,
        task_id: i32,
        event_type: impl Into<String>,
        message: impl Into<String>,
        created_at: NaiveDateTime,
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
    pub const fn id(&self) -> i64 {
        self.id
    }

    #[must_use]
    pub const fn task_id(&self) -> i32 {
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
    pub const fn created_at(&self) -> NaiveDateTime {
        self.created_at
    }

    #[must_use]
    pub const fn actor_principal_id(&self) -> Option<i32> {
        self.actor_principal_id
    }

    #[must_use]
    pub fn actor_kind(&self) -> &str {
        &self.actor_kind
    }

    #[must_use]
    pub const fn initiator_principal_id(&self) -> Option<i32> {
        self.initiator_principal_id
    }

    #[must_use]
    pub const fn provenance_task_id(&self) -> Option<i32> {
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
    pub const fn actor_principal_id(mut self, actor_principal_id: Option<i32>) -> Self {
        self.event.actor_principal_id = actor_principal_id;
        self
    }

    #[must_use]
    pub const fn provenance(
        mut self,
        initiator_principal_id: Option<i32>,
        provenance_task_id: Option<i32>,
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
pub struct StorageTaskEventPage {
    events: Vec<StorageTaskEvent>,
    total: Option<i64>,
}

impl StorageTaskEventPage {
    #[must_use]
    pub const fn new(events: Vec<StorageTaskEvent>, total: Option<i64>) -> Self {
        Self { events, total }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<StorageTaskEvent>, Option<i64>) {
        (self.events, self.total)
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageImportTaskResult {
    id: i32,
    task_id: i32,
    item_ref: Option<String>,
    entity_kind: String,
    action: String,
    identifier: Option<String>,
    outcome: String,
    error: Option<String>,
    details: Option<Value>,
    created_at: NaiveDateTime,
}

impl StorageImportTaskResult {
    #[must_use]
    pub fn builder(
        id: i32,
        task_id: i32,
        entity_kind: impl Into<String>,
        action: impl Into<String>,
        outcome: impl Into<String>,
        created_at: NaiveDateTime,
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
    pub const fn id(&self) -> i32 {
        self.id
    }

    #[must_use]
    pub const fn task_id(&self) -> i32 {
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
    pub const fn created_at(&self) -> NaiveDateTime {
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

#[derive(Clone, PartialEq)]
pub struct StorageImportTaskResultPage {
    results: Vec<StorageImportTaskResult>,
    total: Option<i64>,
}

impl StorageImportTaskResultPage {
    #[must_use]
    pub const fn new(results: Vec<StorageImportTaskResult>, total: Option<i64>) -> Self {
        Self { results, total }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<StorageImportTaskResult>, Option<i64>) {
        (self.results, self.total)
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
    #[must_use]
    pub const fn new(total_ms: i32, query_ms: i32, hydration_ms: i32, render_ms: i32) -> Self {
        Self {
            total_ms,
            query_ms,
            hydration_ms,
            render_ms,
        }
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
        pub const fn task_id(&self) -> i32 {
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
        pub const fn output_expires_at(&self) -> NaiveDateTime {
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
        pub const fn task_id(&self) -> i32 {
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
        pub const fn output_expires_at(&self) -> NaiveDateTime {
            self.output_expires_at
        }
    };
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageExportOutputSummary {
    task_id: i32,
    template_name: Option<String>,
    content_type: String,
    warning_count: i32,
    truncated: bool,
    output_expires_at: NaiveDateTime,
    durations: StorageTaskDurations,
}

impl StorageExportOutputSummary {
    #[must_use]
    pub fn new(
        task_id: i32,
        template_name: Option<String>,
        content_type: impl Into<String>,
        warning_count: i32,
        truncated: bool,
        output_expires_at: NaiveDateTime,
        durations: StorageTaskDurations,
    ) -> Self {
        Self {
            task_id,
            template_name,
            content_type: content_type.into(),
            warning_count,
            truncated,
            output_expires_at,
            durations,
        }
    }

    task_output_accessors!();
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageBackupOutputSummary {
    task_id: i32,
    byte_size: i64,
    sha256: String,
    output_expires_at: NaiveDateTime,
}

impl StorageBackupOutputSummary {
    #[must_use]
    pub fn new(
        task_id: i32,
        byte_size: i64,
        sha256: impl Into<String>,
        output_expires_at: NaiveDateTime,
    ) -> Self {
        Self {
            task_id,
            byte_size,
            sha256: sha256.into(),
            output_expires_at,
        }
    }

    backup_output_accessors!();
}

#[derive(Clone, PartialEq)]
pub struct StorageExportOutput {
    task_id: i32,
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
    created_at: NaiveDateTime,
}

impl StorageExportOutput {
    #[must_use]
    pub fn builder(
        task_id: i32,
        content_type: impl Into<String>,
        metadata: Value,
        warnings: Value,
        output_expires_at: NaiveDateTime,
        created_at: NaiveDateTime,
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
    pub const fn created_at(&self) -> NaiveDateTime {
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

    #[must_use]
    pub fn build(self) -> StorageExportOutput {
        self.output
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageBackupOutput {
    task_id: i32,
    document: Vec<u8>,
    byte_size: i64,
    sha256: String,
    output_expires_at: NaiveDateTime,
    created_at: NaiveDateTime,
}

impl StorageBackupOutput {
    #[must_use]
    pub fn new(
        task_id: i32,
        document: Vec<u8>,
        byte_size: i64,
        sha256: impl Into<String>,
        output_expires_at: NaiveDateTime,
        created_at: NaiveDateTime,
    ) -> Self {
        Self {
            task_id,
            document,
            byte_size,
            sha256: sha256.into(),
            output_expires_at,
            created_at,
        }
    }

    backup_output_accessors!();

    #[must_use]
    pub fn document(&self) -> &[u8] {
        &self.document
    }

    #[must_use]
    pub const fn created_at(&self) -> NaiveDateTime {
        self.created_at
    }
}

#[derive(Clone, PartialEq)]
pub enum StorageTaskOutputLookup<T> {
    Available(T),
    Expired { expires_at: NaiveDateTime },
    Missing,
}

#[async_trait]
pub trait TaskQueueStorage: Send + Sync {
    async fn create_task(
        &self,
        request: StorageTaskCreateRequest,
    ) -> Result<StorageTask, StorageError>;

    async fn get_task_access(&self, task_id: i32) -> Result<StorageTaskAccess, StorageError>;

    async fn list_tasks(
        &self,
        query: StorageTaskListQuery,
    ) -> Result<StorageTaskPage, StorageError>;

    async fn list_task_events(
        &self,
        query: StorageTaskPageQuery,
    ) -> Result<StorageTaskEventPage, StorageError>;

    async fn list_import_task_results(
        &self,
        query: StorageTaskPageQuery,
    ) -> Result<StorageImportTaskResultPage, StorageError>;

    async fn list_export_output_summaries(
        &self,
        task_ids: Vec<i32>,
    ) -> Result<Vec<StorageExportOutputSummary>, StorageError>;

    async fn list_backup_output_summaries(
        &self,
        task_ids: Vec<i32>,
    ) -> Result<Vec<StorageBackupOutputSummary>, StorageError>;

    async fn get_export_output_summary(
        &self,
        task_id: i32,
    ) -> Result<StorageTaskOutputLookup<StorageExportOutputSummary>, StorageError>;

    async fn get_backup_output_summary(
        &self,
        task_id: i32,
    ) -> Result<StorageTaskOutputLookup<StorageBackupOutputSummary>, StorageError>;

    async fn get_export_output(
        &self,
        task_id: i32,
    ) -> Result<StorageTaskOutputLookup<StorageExportOutput>, StorageError>;

    async fn get_backup_output(
        &self,
        task_id: i32,
    ) -> Result<StorageTaskOutputLookup<StorageBackupOutput>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_debug_redacts_identity_payload_scope_and_claim() {
        let now = chrono::Utc::now().naive_utc();
        let task = StorageTask::builder(
            91_001,
            StorageTaskKind::Import,
            StorageTaskStatus::Running,
            now,
            now,
        )
        .submitted_by(Some(91_002))
        .idempotency_key(Some("secret idempotency".to_string()))
        .request_hash(Some("secret hash".to_string()))
        .request_payload(Some(serde_json::json!({"secret": "payload"})))
        .summary(Some("summary".to_string()))
        .progress(StorageTaskProgress::new(2, 1, 1, 0))
        .scope_snapshot(StorageTaskScopeSnapshot::new(
            Some(91_003),
            true,
            serde_json::json!({"permissions": ["secret"]}),
        ))
        .started_at(Some(now))
        .lease(
            Some(Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap()),
            Some(now),
        )
        .attempt_count(1)
        .initiator_principal_id(Some(91_004))
        .build();

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
            "11111111",
        ] {
            assert!(!debug.contains(secret));
        }
        assert!(debug.contains("Import"));
        assert!(debug.contains("Running"));
    }
}
