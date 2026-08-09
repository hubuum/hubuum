use std::fmt;

use crate::db::prelude::*;
use async_trait::async_trait;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::db::DbPool;
use crate::db::traits::task::TaskBackend;
use crate::errors::ApiError;
use crate::events::{Event, MutationProvenance, PrincipalNames, Provenance, StoredProvenance};
use crate::models::principal::PrincipalID;
use crate::models::search::{FilterField, SortParam};
use crate::models::{
    BackupOutputLookup, REDACTED_DEBUG_VALUE, ResourceRevision, redacted_debug_option,
};
use crate::permissions::{AuthzTarget, ResourceAttrs, ResourceKind, ResourceRef};
use crate::schema::{backup_task_outputs, export_task_outputs, import_task_results, tasks};
use crate::traits::SelfAccessors;
use crate::traits::accessors::{IdAccessor, InstanceAdapter};
use crate::traits::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    Import,
    Export,
    Backup,
    Reindex,
    RemoteCall,
}

impl TaskKind {
    pub const ALL: [Self; 5] = [
        Self::Import,
        Self::Export,
        Self::Backup,
        Self::Reindex,
        Self::RemoteCall,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            TaskKind::Import => "import",
            TaskKind::Export => "export",
            TaskKind::Backup => "backup",
            TaskKind::Reindex => "reindex",
            TaskKind::RemoteCall => "remote_call",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, ApiError> {
        match value {
            "import" => Ok(TaskKind::Import),
            "export" => Ok(TaskKind::Export),
            "backup" => Ok(TaskKind::Backup),
            "reindex" => Ok(TaskKind::Reindex),
            "remote_call" => Ok(TaskKind::RemoteCall),
            _ => Err(ApiError::InternalServerError(format!(
                "Unknown task kind '{value}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Queued,
    Validating,
    Running,
    Succeeded,
    Failed,
    PartiallySucceeded,
    Cancelled,
}

impl TaskStatus {
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
    pub const NON_TERMINAL: [Self; 3] = [Self::Queued, Self::Validating, Self::Running];

    pub const TERMINAL: [Self; 4] = [
        Self::Succeeded,
        Self::Failed,
        Self::PartiallySucceeded,
        Self::Cancelled,
    ];

    pub const fn is_terminal(self) -> bool {
        match self {
            Self::Queued | Self::Validating | Self::Running => false,
            Self::Succeeded | Self::Failed | Self::PartiallySucceeded | Self::Cancelled => true,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            TaskStatus::Queued => "queued",
            TaskStatus::Validating => "validating",
            TaskStatus::Running => "running",
            TaskStatus::Succeeded => "succeeded",
            TaskStatus::Failed => "failed",
            TaskStatus::PartiallySucceeded => "partially_succeeded",
            TaskStatus::Cancelled => "cancelled",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, ApiError> {
        match value {
            "queued" => Ok(TaskStatus::Queued),
            "validating" => Ok(TaskStatus::Validating),
            "running" => Ok(TaskStatus::Running),
            "succeeded" => Ok(TaskStatus::Succeeded),
            "failed" => Ok(TaskStatus::Failed),
            "partially_succeeded" => Ok(TaskStatus::PartiallySucceeded),
            "cancelled" => Ok(TaskStatus::Cancelled),
            _ => Err(ApiError::InternalServerError(format!(
                "Unknown task status '{value}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TaskResultCounts {
    processed: i32,
    success: i32,
    failed: i32,
}

impl TaskResultCounts {
    /// Build counts loaded from durable task state, where `processed` may not
    /// equal `success + failed` while a task is still running or recovering.
    pub fn from_stored<T, U, V>(processed: T, success: U, failed: V) -> Result<Self, ApiError>
    where
        T: TryInto<i32>,
        U: TryInto<i32>,
        V: TryInto<i32>,
    {
        Ok(Self {
            processed: task_result_count("processed", processed)?,
            success: task_result_count("success", success)?,
            failed: task_result_count("failed", failed)?,
        })
    }

    /// Build terminal counts and derive the processed total from the two
    /// mutually exclusive outcomes.
    pub fn from_outcomes<U, V>(success: U, failed: V) -> Result<Self, ApiError>
    where
        U: TryInto<i32>,
        V: TryInto<i32>,
    {
        let success = task_result_count("success", success)?;
        let failed = task_result_count("failed", failed)?;
        let processed = success.checked_add(failed).ok_or_else(|| {
            ApiError::InternalServerError("processed count is out of range".to_string())
        })?;
        Ok(Self {
            processed,
            success,
            failed,
        })
    }

    pub fn processed(self) -> i32 {
        self.processed
    }

    pub fn success(self) -> i32 {
        self.success
    }

    pub fn failed(self) -> i32 {
        self.failed
    }
}

fn task_result_count<T>(name: &str, value: T) -> Result<i32, ApiError>
where
    T: TryInto<i32>,
{
    let value = value
        .try_into()
        .map_err(|_| ApiError::InternalServerError(format!("{name} count is out of range")))?;
    if value < 0 {
        return Err(ApiError::InternalServerError(format!(
            "{name} count must not be negative"
        )));
    }
    Ok(value)
}

impl From<TaskResultCounts> for (i32, i32, i32) {
    fn from(value: TaskResultCounts) -> Self {
        (value.processed, value.success, value.failed)
    }
}

crate::int_id_newtype! {
    /// Identifier wrapper for a task.
    pub struct TaskID;
    noun = "task id";
}

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = tasks)]
pub struct TaskRecord {
    pub id: i32,
    pub kind: String,
    pub status: String,
    pub submitted_by: Option<i32>,
    pub idempotency_key: Option<String>,
    pub request_hash: Option<String>,
    pub request_payload: Option<serde_json::Value>,
    pub summary: Option<String>,
    pub total_items: i32,
    pub processed_items: i32,
    pub success_items: i32,
    pub failed_items: i32,
    /// Scope snapshot of the submitting token (principal-centric). Captured at
    /// enqueue time so async execution can never exceed the token's scope.
    pub submitted_token_id: Option<i32>,
    pub submitted_token_scoped: bool,
    pub submitted_token_scopes: serde_json::Value,
    pub request_redacted_at: Option<NaiveDateTime>,
    pub started_at: Option<NaiveDateTime>,
    pub finished_at: Option<NaiveDateTime>,
    pub deleted_at: Option<NaiveDateTime>,
    pub deleted_by: Option<i32>,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
    pub lease_token: Option<uuid::Uuid>,
    pub lease_expires_at: Option<NaiveDateTime>,
    pub attempt_count: i32,
    pub initiator_user_id: Option<i32>,
}

impl fmt::Debug for TaskRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskRecord")
            .field("id", &self.id)
            .field("kind", &self.kind)
            .field("status", &self.status)
            .field("submitted_by", &self.submitted_by)
            .field(
                "idempotency_key",
                &redacted_debug_option(&self.idempotency_key),
            )
            .field("request_hash", &redacted_debug_option(&self.request_hash))
            .field(
                "request_payload",
                &redacted_debug_option(&self.request_payload),
            )
            .field("summary", &self.summary)
            .field("total_items", &self.total_items)
            .field("processed_items", &self.processed_items)
            .field("success_items", &self.success_items)
            .field("failed_items", &self.failed_items)
            .field("submitted_token_id", &self.submitted_token_id)
            .field("submitted_token_scoped", &self.submitted_token_scoped)
            .field("submitted_token_scopes", &REDACTED_DEBUG_VALUE)
            .field("request_redacted_at", &self.request_redacted_at)
            .field("started_at", &self.started_at)
            .field("finished_at", &self.finished_at)
            .field("deleted_at", &self.deleted_at)
            .field("deleted_by", &self.deleted_by)
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .field("lease_token", &redacted_debug_option(&self.lease_token))
            .field("lease_expires_at", &self.lease_expires_at)
            .field("attempt_count", &self.attempt_count)
            .field("initiator_user_id", &self.initiator_user_id)
            .finish()
    }
}

impl TaskRecord {
    pub(crate) fn worker_provenance(&self) -> MutationProvenance {
        MutationProvenance::worker(self.initiator_user_id, self.id)
    }

    pub(crate) fn system_provenance(&self) -> MutationProvenance {
        MutationProvenance::system_for_task(self.initiator_user_id, self.id)
    }

    pub(crate) fn user_provenance(&self, actor: PrincipalID) -> MutationProvenance {
        MutationProvenance::user_for_task(actor.id(), self.initiator_user_id, self.id)
    }
}

#[derive(Insertable)]
#[diesel(table_name = tasks)]
pub struct NewTaskRecord {
    pub kind: String,
    pub status: String,
    pub submitted_by: Option<i32>,
    pub idempotency_key: Option<String>,
    pub request_hash: Option<String>,
    pub request_payload: Option<serde_json::Value>,
    pub summary: Option<String>,
    pub total_items: i32,
    pub processed_items: i32,
    pub success_items: i32,
    pub failed_items: i32,
    #[diesel(column_name = submitted_token_id)]
    pub submitted_token_id: Option<i32>,
    #[diesel(column_name = submitted_token_scoped)]
    pub submitted_token_scoped: bool,
    #[diesel(column_name = submitted_token_scopes)]
    pub submitted_token_scopes: serde_json::Value,
    pub request_redacted_at: Option<NaiveDateTime>,
    pub started_at: Option<NaiveDateTime>,
    pub finished_at: Option<NaiveDateTime>,
}

#[derive(Clone)]
pub struct TaskEventRecord {
    pub id: i64,
    pub task_id: i32,
    pub event_type: String,
    pub message: String,
    pub data: Option<serde_json::Value>,
    pub created_at: NaiveDateTime,
    pub actor_user_id: Option<i32>,
    pub actor_kind: String,
    pub initiator_user_id: Option<i32>,
    pub provenance_task_id: Option<i32>,
}

pub struct NewTaskEventRecord {
    pub task_id: i32,
    pub event_type: String,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = import_task_results)]
pub struct ImportTaskResultRecord {
    pub id: i32,
    pub task_id: i32,
    pub item_ref: Option<String>,
    pub entity_kind: String,
    pub action: String,
    pub identifier: Option<String>,
    pub outcome: String,
    pub error: Option<String>,
    pub details: Option<serde_json::Value>,
    pub created_at: NaiveDateTime,
}

#[derive(Clone, Insertable)]
#[diesel(table_name = import_task_results)]
pub struct NewImportTaskResultRecord {
    pub task_id: i32,
    pub item_ref: Option<String>,
    pub entity_kind: String,
    pub action: String,
    pub identifier: Option<String>,
    pub outcome: String,
    pub error: Option<String>,
    pub details: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct TaskProgress {
    pub total_items: i32,
    pub processed_items: i32,
    pub success_items: i32,
    pub failed_items: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct TaskLinks {
    pub task: String,
    pub events: String,
    pub import: Option<String>,
    pub import_results: Option<String>,
    pub export: Option<String>,
    pub export_output: Option<String>,
    pub backup: Option<String>,
    pub backup_output: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ImportTaskDetails {
    pub results_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ExportTaskDetails {
    pub output_url: String,
    pub output_available: bool,
    /// True when output was produced but has since passed its retention window. Distinguishes an
    /// expired export from one that was never generated (both have `output_available = false`).
    pub output_expired: bool,
    pub output_expires_at: Option<NaiveDateTime>,
    pub template_name: Option<String>,
    pub output_content_type: Option<String>,
    pub warning_count: Option<i32>,
    pub truncated: Option<bool>,
    pub total_duration_ms: Option<i32>,
    pub query_duration_ms: Option<i32>,
    pub hydration_duration_ms: Option<i32>,
    pub render_duration_ms: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct BackupTaskDetails {
    pub output_url: String,
    pub output_available: bool,
    pub output_expired: bool,
    pub output_expires_at: Option<NaiveDateTime>,
    pub byte_size: Option<i64>,
    pub sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct TaskDetails {
    pub import: Option<ImportTaskDetails>,
    pub export: Option<ExportTaskDetails>,
    pub backup: Option<BackupTaskDetails>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct TaskResponse {
    pub id: i32,
    pub kind: TaskKind,
    pub status: TaskStatus,
    pub submitted_by: Option<i32>,
    pub created_at: NaiveDateTime,
    pub started_at: Option<NaiveDateTime>,
    pub finished_at: Option<NaiveDateTime>,
    pub progress: TaskProgress,
    pub summary: Option<String>,
    pub request_redacted_at: Option<NaiveDateTime>,
    pub links: TaskLinks,
    pub details: Option<TaskDetails>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct TaskEventResponse {
    pub id: i64,
    pub task_id: i32,
    pub event_type: String,
    pub message: String,
    pub data: Option<serde_json::Value>,
    pub created_at: NaiveDateTime,
    pub provenance: Provenance,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct ImportTaskResultResponse {
    pub id: i32,
    pub task_id: i32,
    pub item_ref: Option<String>,
    pub entity_kind: String,
    pub action: String,
    pub identifier: Option<String>,
    pub outcome: String,
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_revision: Option<ResourceRevision>,
    pub details: Option<serde_json::Value>,
    pub created_at: NaiveDateTime,
}

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = export_task_outputs)]
pub struct ExportTaskOutputRecord {
    pub id: i32,
    pub task_id: i32,
    pub template_name: Option<String>,
    pub content_type: String,
    pub json_output: Option<serde_json::Value>,
    pub text_output: Option<String>,
    pub meta_json: serde_json::Value,
    pub warnings_json: serde_json::Value,
    pub warning_count: i32,
    pub truncated: bool,
    pub output_expires_at: NaiveDateTime,
    pub total_duration_ms: i32,
    pub query_duration_ms: i32,
    pub hydration_duration_ms: i32,
    pub render_duration_ms: i32,
    pub created_at: NaiveDateTime,
}

#[derive(Clone, Insertable)]
#[diesel(table_name = export_task_outputs)]
pub struct NewExportTaskOutputRecord {
    pub task_id: i32,
    pub template_name: Option<String>,
    pub content_type: String,
    pub json_output: Option<serde_json::Value>,
    pub text_output: Option<String>,
    pub meta_json: serde_json::Value,
    pub warnings_json: serde_json::Value,
    pub warning_count: i32,
    pub truncated: bool,
    pub output_expires_at: NaiveDateTime,
    pub total_duration_ms: i32,
    pub query_duration_ms: i32,
    pub hydration_duration_ms: i32,
    pub render_duration_ms: i32,
}

/// Outcome of looking up an export task's stored output.
///
/// Retention can lapse before the cleanup job purges the row, so a present-but-expired output is
/// distinct from one that never existed. Callers map `Expired` to `410 Gone` and `Missing` to
/// `404 Not Found` rather than collapsing both into a 404 that looks like data loss.
#[derive(Debug, Clone)]
pub enum ExportOutputLookup<T> {
    Available(T),
    Expired { expires_at: NaiveDateTime },
    Missing,
}

impl<T> ExportOutputLookup<T> {
    /// Borrow the contained value, mirroring `Option::as_ref`.
    pub fn as_ref(&self) -> ExportOutputLookup<&T> {
        match self {
            ExportOutputLookup::Available(value) => ExportOutputLookup::Available(value),
            ExportOutputLookup::Expired { expires_at } => ExportOutputLookup::Expired {
                expires_at: *expires_at,
            },
            ExportOutputLookup::Missing => ExportOutputLookup::Missing,
        }
    }
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = export_task_outputs)]
pub struct ExportTaskOutputSummaryRecord {
    pub id: i32,
    pub task_id: i32,
    pub template_name: Option<String>,
    pub content_type: String,
    pub warning_count: i32,
    pub truncated: bool,
    pub output_expires_at: NaiveDateTime,
    pub total_duration_ms: i32,
    pub query_duration_ms: i32,
    pub hydration_duration_ms: i32,
    pub render_duration_ms: i32,
    pub created_at: NaiveDateTime,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = backup_task_outputs)]
pub struct BackupTaskOutputSummaryRecord {
    pub task_id: i32,
    pub byte_size: i64,
    pub sha256: String,
    pub output_expires_at: NaiveDateTime,
}

impl TaskRecord {
    pub fn to_response(&self) -> Result<TaskResponse, ApiError> {
        self.to_response_with_outputs(ExportOutputLookup::Missing, BackupOutputLookup::Missing)
    }

    pub fn to_response_with_export_output(
        &self,
        export_output: ExportOutputLookup<&ExportTaskOutputSummaryRecord>,
    ) -> Result<TaskResponse, ApiError> {
        self.to_response_with_outputs(export_output, BackupOutputLookup::Missing)
    }

    pub fn to_response_with_backup_output(
        &self,
        backup_output: BackupOutputLookup<&BackupTaskOutputSummaryRecord>,
    ) -> Result<TaskResponse, ApiError> {
        self.to_response_with_outputs(ExportOutputLookup::Missing, backup_output)
    }

    pub(crate) fn to_response_with_outputs(
        &self,
        export_output: ExportOutputLookup<&ExportTaskOutputSummaryRecord>,
        backup_output: BackupOutputLookup<&BackupTaskOutputSummaryRecord>,
    ) -> Result<TaskResponse, ApiError> {
        let kind = TaskKind::from_db(&self.kind)?;
        let status = TaskStatus::from_db(&self.status)?;
        let task_url = format!("/api/v1/tasks/{}", self.id);
        let import_url = (kind == TaskKind::Import).then(|| format!("/api/v1/imports/{}", self.id));
        let import_results =
            (kind == TaskKind::Import).then(|| format!("/api/v1/imports/{}/results", self.id));
        let export_url = (kind == TaskKind::Export).then(|| format!("/api/v1/exports/{}", self.id));
        let export_output_url =
            (kind == TaskKind::Export).then(|| format!("/api/v1/exports/{}/output", self.id));
        let details = match kind {
            TaskKind::Import => import_results.clone().map(|results_url| TaskDetails {
                import: Some(ImportTaskDetails { results_url }),
                export: None,
                backup: None,
            }),
            TaskKind::Export => export_output_url.clone().map(|output_url| {
                let (output_summary, output_expired, expired_expires_at) = match export_output {
                    ExportOutputLookup::Available(summary) => (Some(summary), false, None),
                    ExportOutputLookup::Expired { expires_at } => (None, true, Some(expires_at)),
                    ExportOutputLookup::Missing => (None, false, None),
                };
                TaskDetails {
                    import: None,
                    export: Some(ExportTaskDetails {
                        output_url,
                        output_available: output_summary.is_some(),
                        output_expired,
                        output_expires_at: output_summary
                            .map(|summary| summary.output_expires_at)
                            .or(expired_expires_at),
                        template_name: output_summary
                            .and_then(|summary| summary.template_name.clone()),
                        output_content_type: output_summary
                            .map(|summary| summary.content_type.clone()),
                        warning_count: output_summary.map(|summary| summary.warning_count),
                        truncated: output_summary.map(|summary| summary.truncated),
                        total_duration_ms: output_summary.map(|summary| summary.total_duration_ms),
                        query_duration_ms: output_summary.map(|summary| summary.query_duration_ms),
                        hydration_duration_ms: output_summary
                            .map(|summary| summary.hydration_duration_ms),
                        render_duration_ms: output_summary
                            .map(|summary| summary.render_duration_ms),
                    }),
                    backup: None,
                }
            }),
            TaskKind::Backup => {
                let output_url = format!("/api/v1/backups/{}/output", self.id);
                let (output_summary, output_expired, expired_expires_at) = match backup_output {
                    BackupOutputLookup::Available(summary) => (Some(summary), false, None),
                    BackupOutputLookup::Expired { expires_at } => (None, true, Some(expires_at)),
                    BackupOutputLookup::Missing => (None, false, None),
                };
                Some(TaskDetails {
                    import: None,
                    export: None,
                    backup: Some(BackupTaskDetails {
                        output_url,
                        output_available: output_summary.is_some(),
                        output_expired,
                        output_expires_at: output_summary
                            .map(|summary| summary.output_expires_at)
                            .or(expired_expires_at),
                        byte_size: output_summary.map(|summary| summary.byte_size),
                        sha256: output_summary.map(|summary| summary.sha256.clone()),
                    }),
                })
            }
            _ => None,
        };

        Ok(TaskResponse {
            id: self.id,
            kind,
            status,
            submitted_by: self.submitted_by,
            created_at: self.created_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
            progress: TaskProgress {
                total_items: self.total_items,
                processed_items: self.processed_items,
                success_items: self.success_items,
                failed_items: self.failed_items,
            },
            summary: self.summary.clone(),
            request_redacted_at: self.request_redacted_at,
            links: TaskLinks {
                task: task_url.clone(),
                events: format!("{task_url}/events"),
                import: import_url.clone(),
                import_results: import_results.clone(),
                export: export_url,
                export_output: export_output_url,
                backup: (kind == TaskKind::Backup).then(|| format!("/api/v1/backups/{}", self.id)),
                backup_output: (kind == TaskKind::Backup)
                    .then(|| format!("/api/v1/backups/{}/output", self.id)),
            },
            details,
        })
    }
}

impl From<TaskEventRecord> for TaskEventResponse {
    fn from(value: TaskEventRecord) -> Self {
        Self::from_record_with_names(value, &PrincipalNames::default())
    }
}

impl TaskEventResponse {
    pub(crate) fn from_record_with_names(
        value: TaskEventRecord,
        principal_names: &PrincipalNames,
    ) -> Self {
        let provenance = StoredProvenance::from_actor_kind(Some(&value.actor_kind))
            .with_actor_user_id(value.actor_user_id)
            .with_initiator_user_id(value.initiator_user_id)
            .with_task_id(value.provenance_task_id)
            .resolve(principal_names);
        Self {
            id: value.id,
            task_id: value.task_id,
            event_type: value.event_type,
            message: value.message,
            data: value.data,
            created_at: value.created_at,
            provenance,
        }
    }
}

impl TryFrom<Event> for TaskEventRecord {
    type Error = ApiError;

    fn try_from(value: Event) -> Result<Self, Self::Error> {
        let Some(task_id) = value.entity_id else {
            return Err(ApiError::InternalServerError(
                "Task event is missing task id".to_string(),
            ));
        };
        let data = match value.metadata.get("data") {
            Some(serde_json::Value::Null) | None => None,
            Some(data) => Some(data.clone()),
        };

        Ok(Self {
            id: value.id,
            task_id,
            event_type: value.action,
            message: value.summary,
            data,
            created_at: value.occurred_at,
            actor_user_id: value.actor_user_id,
            actor_kind: value.actor_kind,
            initiator_user_id: value.initiator_user_id,
            provenance_task_id: value.task_id.or(Some(task_id)),
        })
    }
}

impl From<ImportTaskResultRecord> for ImportTaskResultResponse {
    fn from(value: ImportTaskResultRecord) -> Self {
        let mut details = value.details;
        let observed_revision = details
            .as_mut()
            .and_then(serde_json::Value::as_object_mut)
            .and_then(|details| details.remove("observed_revision"))
            .and_then(|revision| serde_json::from_value(revision).ok());
        if details
            .as_ref()
            .and_then(serde_json::Value::as_object)
            .is_some_and(serde_json::Map::is_empty)
        {
            details = None;
        }
        Self {
            id: value.id,
            task_id: value.task_id,
            item_ref: value.item_ref,
            entity_kind: value.entity_kind,
            action: value.action,
            identifier: value.identifier,
            outcome: value.outcome,
            error: value.error,
            observed_revision,
            details,
            created_at: value.created_at,
        }
    }
}

impl CursorPaginated for TaskResponse {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Kind
                | FilterField::Status
                | FilterField::SubmittedBy
                | FilterField::CreatedAt
                | FilterField::StartedAt
                | FilterField::FinishedAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        match field {
            FilterField::Id => Ok(CursorValue::Integer(self.id as i64)),
            FilterField::Kind => Ok(CursorValue::String(self.kind.as_str().to_string())),
            FilterField::Status => Ok(CursorValue::String(self.status.as_str().to_string())),
            FilterField::SubmittedBy => Ok(match self.submitted_by {
                Some(value) => CursorValue::Integer(value as i64),
                None => CursorValue::Null,
            }),
            FilterField::CreatedAt => Ok(CursorValue::DateTime(self.created_at)),
            FilterField::StartedAt => Ok(match self.started_at {
                Some(value) => CursorValue::DateTime(value),
                None => CursorValue::Null,
            }),
            FilterField::FinishedAt => Ok(match self.finished_at {
                Some(value) => CursorValue::DateTime(value),
                None => CursorValue::Null,
            }),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported sort field '{}' for tasks",
                field
            ))),
        }
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}

impl CursorPaginated for TaskRecord {
    fn supports_sort(field: &FilterField) -> bool {
        TaskResponse::supports_sort(field)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::Kind => CursorValue::String(self.kind.clone()),
            FilterField::Status => CursorValue::String(self.status.clone()),
            FilterField::SubmittedBy => self
                .submitted_by
                .map(|value| CursorValue::Integer(value as i64))
                .unwrap_or(CursorValue::Null),
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::StartedAt => self
                .started_at
                .map(CursorValue::DateTime)
                .unwrap_or(CursorValue::Null),
            FilterField::FinishedAt => self
                .finished_at
                .map(CursorValue::DateTime)
                .unwrap_or(CursorValue::Null),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Unsupported sort field '{}' for tasks",
                    field
                )));
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        TaskResponse::default_sort()
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        TaskResponse::tie_breaker_sort()
    }
}

impl CursorSqlMapping for TaskResponse {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "tasks.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Kind => CursorSqlField {
                column: "tasks.kind",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Status => CursorSqlField {
                column: "tasks.status",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::SubmittedBy => CursorSqlField {
                column: "tasks.submitted_by",
                sql_type: CursorSqlType::Integer,
                nullable: true,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "tasks.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::StartedAt => CursorSqlField {
                column: "tasks.started_at",
                sql_type: CursorSqlType::DateTime,
                nullable: true,
            },
            FilterField::FinishedAt => CursorSqlField {
                column: "tasks.finished_at",
                sql_type: CursorSqlType::DateTime,
                nullable: true,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for tasks",
                    field
                )));
            }
        })
    }
}

impl CursorPaginated for TaskEventResponse {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(field, FilterField::Id)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        match field {
            FilterField::Id => Ok(CursorValue::Integer(self.id)),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported sort field '{}' for task events",
                field
            ))),
        }
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}

impl CursorPaginated for ImportTaskResultResponse {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(field, FilterField::Id)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        match field {
            FilterField::Id => Ok(CursorValue::Integer(self.id as i64)),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported sort field '{}' for import results",
                field
            ))),
        }
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}

impl IdAccessor for TaskRecord {
    fn accessor_id(&self) -> i32 {
        self.id
    }
}

impl IdAccessor for TaskID {
    fn accessor_id(&self) -> i32 {
        self.id()
    }
}

impl InstanceAdapter<TaskRecord> for TaskRecord {
    async fn instance_adapter(&self, _pool: &DbPool) -> Result<TaskRecord, ApiError> {
        Ok(self.clone())
    }
}

impl InstanceAdapter<TaskRecord> for TaskID {
    async fn instance_adapter(&self, pool: &DbPool) -> Result<TaskRecord, ApiError> {
        self.find_record(pool).await
    }
}

#[async_trait]
impl AuthzTarget for TaskRecord {
    async fn to_resource_ref(
        &self,
        _pool: &dyn crate::traits::BackendContext,
    ) -> Result<ResourceRef, ApiError> {
        Ok(ResourceRef {
            kind: ResourceKind::Task,
            id: self.id,
            attrs: ResourceAttrs {
                submitted_by: self.submitted_by,
                ..Default::default()
            },
        })
    }
}

#[async_trait]
impl AuthzTarget for TaskID {
    async fn to_resource_ref(
        &self,
        pool: &dyn crate::traits::BackendContext,
    ) -> Result<ResourceRef, ApiError> {
        self.instance(pool).await?.to_resource_ref(pool).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_timestamp() -> NaiveDateTime {
        chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .unwrap()
            .naive_utc()
    }

    #[test]
    fn task_result_counts_derive_processed_from_terminal_outcomes() {
        let counts = TaskResultCounts::from_outcomes(2, 1).unwrap();

        assert_eq!(
            (counts.processed(), counts.success(), counts.failed()),
            (3, 2, 1)
        );
    }

    #[test]
    fn task_result_counts_reject_negative_stored_values() {
        assert!(TaskResultCounts::from_stored(-1, 0, 0).is_err());
    }

    #[test]
    fn task_result_counts_reject_processed_overflow() {
        assert!(TaskResultCounts::from_outcomes(i32::MAX, 1).is_err());
    }

    #[test]
    fn terminal_task_statuses_are_stable() {
        assert_eq!(
            TaskStatus::TERMINAL.map(TaskStatus::as_str),
            ["succeeded", "failed", "partially_succeeded", "cancelled"]
        );
        assert_eq!(
            TaskStatus::ALL.map(TaskStatus::is_terminal),
            [false, false, false, true, true, true, true]
        );
    }

    #[test]
    fn active_task_statuses_are_stable() {
        assert_eq!(
            TaskStatus::ACTIVE.map(TaskStatus::as_str),
            ["validating", "running"]
        );
        assert_eq!(
            TaskStatus::NON_TERMINAL.map(TaskStatus::as_str),
            ["queued", "validating", "running"]
        );
    }

    #[test]
    fn task_id_new_accepts_positive() {
        assert_eq!(TaskID::new(1).unwrap().id(), 1);
        assert_eq!(TaskID::new(i32::MAX).unwrap().id(), i32::MAX);
    }

    #[test]
    fn task_id_new_rejects_non_positive() {
        for invalid in [0, -1, i32::MIN] {
            let err = TaskID::new(invalid).unwrap_err();
            assert!(matches!(err, ApiError::BadRequest(_)), "got {err:?}");
        }
    }

    #[test]
    fn task_id_deserialize_routes_through_new() {
        // A valid id deserializes; a non-positive id is rejected by the validating constructor,
        // so an invalid path segment never produces a TaskID.
        assert_eq!(serde_json::from_str::<TaskID>("7").unwrap().id(), 7);
        assert!(serde_json::from_str::<TaskID>("0").is_err());
        assert!(serde_json::from_str::<TaskID>("-3").is_err());
    }

    #[test]
    fn task_record_debug_redacts_request_and_lease_material() {
        let timestamp = test_timestamp();
        let lease_token = uuid::Uuid::parse_str("de305d54-75b4-431b-adb2-eb6b9e546014").unwrap();
        let task = TaskRecord {
            id: 7,
            kind: TaskKind::Import.as_str().to_string(),
            status: TaskStatus::Running.as_str().to_string(),
            submitted_by: Some(1),
            idempotency_key: Some("idempotency-secret".to_string()),
            request_hash: Some("request-hash-secret".to_string()),
            request_payload: Some(serde_json::json!({"password": "payload-secret"})),
            summary: Some("safe summary".to_string()),
            total_items: 1,
            processed_items: 0,
            success_items: 0,
            failed_items: 0,
            submitted_token_id: Some(3),
            submitted_token_scoped: true,
            submitted_token_scopes: serde_json::json!({"token": "scope-secret"}),
            request_redacted_at: None,
            started_at: Some(timestamp),
            finished_at: None,
            deleted_at: None,
            deleted_by: None,
            created_at: timestamp,
            updated_at: timestamp,
            lease_token: Some(lease_token),
            lease_expires_at: Some(timestamp),
            attempt_count: 1,
            initiator_user_id: Some(1),
        };

        let debug = format!("{task:?}");

        assert!(debug.contains("safe summary"));
        assert!(debug.contains(REDACTED_DEBUG_VALUE));
        for secret in [
            "idempotency-secret",
            "request-hash-secret",
            "payload-secret",
            "scope-secret",
            "de305d54-75b4-431b-adb2-eb6b9e546014",
        ] {
            assert!(!debug.contains(secret), "debug output exposed {secret}");
        }
    }

    #[test]
    fn export_task_details_include_persisted_phase_timings() {
        let timestamp = test_timestamp();
        let task = TaskRecord {
            id: 7,
            kind: TaskKind::Export.as_str().to_string(),
            status: TaskStatus::Succeeded.as_str().to_string(),
            submitted_by: Some(1),
            idempotency_key: None,
            request_hash: None,
            request_payload: None,
            summary: None,
            total_items: 1,
            processed_items: 1,
            success_items: 1,
            failed_items: 0,
            submitted_token_id: None,
            submitted_token_scoped: false,
            submitted_token_scopes: serde_json::json!([]),
            request_redacted_at: Some(timestamp),
            started_at: Some(timestamp),
            finished_at: Some(timestamp),
            deleted_at: None,
            deleted_by: None,
            created_at: timestamp,
            updated_at: timestamp,
            lease_token: None,
            lease_expires_at: None,
            attempt_count: 1,
            initiator_user_id: Some(1),
        };
        let output = ExportTaskOutputSummaryRecord {
            id: 3,
            task_id: task.id,
            template_name: Some("inventory".to_string()),
            content_type: "text/plain".to_string(),
            warning_count: 0,
            truncated: false,
            output_expires_at: timestamp,
            total_duration_ms: 150,
            query_duration_ms: 40,
            hydration_duration_ms: 30,
            render_duration_ms: 70,
            created_at: timestamp,
        };

        let response = task
            .to_response_with_export_output(ExportOutputLookup::Available(&output))
            .unwrap();
        let details = response.details.unwrap().export.unwrap();

        assert_eq!(
            (
                details.total_duration_ms,
                details.query_duration_ms,
                details.hydration_duration_ms,
                details.render_duration_ms,
            ),
            (Some(150), Some(40), Some(30), Some(70))
        );
    }
}
