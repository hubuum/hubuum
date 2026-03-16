use chrono::NaiveDateTime;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};
use crate::schema::{import_task_results, task_events, tasks};
use crate::traits::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    Import,
    Report,
    Export,
    Reindex,
}

impl TaskKind {
    pub fn as_str(self) -> &'static str {
        match self {
            TaskKind::Import => "import",
            TaskKind::Report => "report",
            TaskKind::Export => "export",
            TaskKind::Reindex => "reindex",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, ApiError> {
        match value {
            "import" => Ok(TaskKind::Import),
            "report" => Ok(TaskKind::Report),
            "export" => Ok(TaskKind::Export),
            "reindex" => Ok(TaskKind::Reindex),
            _ => Err(ApiError::InternalServerError(format!(
                "Unknown task kind '{value}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
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

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskResultCounts {
    pub processed: i32,
    pub success: i32,
    pub failed: i32,
}

impl TaskResultCounts {
    pub fn new<T, U, V>(processed: T, success: U, failed: V) -> Result<Self, ApiError>
    where
        T: TryInto<i32>,
        U: TryInto<i32>,
        V: TryInto<i32>,
    {
        Ok(Self {
            processed: processed.try_into().map_err(|_| {
                ApiError::InternalServerError("processed count is out of range".to_string())
            })?,
            success: success.try_into().map_err(|_| {
                ApiError::InternalServerError("success count is out of range".to_string())
            })?,
            failed: failed.try_into().map_err(|_| {
                ApiError::InternalServerError("failed count is out of range".to_string())
            })?,
        })
    }
}

impl From<TaskResultCounts> for (i32, i32, i32) {
    fn from(value: TaskResultCounts) -> Self {
        (value.processed, value.success, value.failed)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Queryable, Selectable)]
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
    pub request_redacted_at: Option<NaiveDateTime>,
    pub started_at: Option<NaiveDateTime>,
    pub finished_at: Option<NaiveDateTime>,
    pub deleted_at: Option<NaiveDateTime>,
    pub deleted_by: Option<i32>,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Insertable)]
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
    pub request_redacted_at: Option<NaiveDateTime>,
    pub started_at: Option<NaiveDateTime>,
    pub finished_at: Option<NaiveDateTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Queryable, Selectable)]
#[diesel(table_name = task_events)]
pub struct TaskEventRecord {
    pub id: i32,
    pub task_id: i32,
    pub event_type: String,
    pub message: String,
    pub data: Option<serde_json::Value>,
    pub created_at: NaiveDateTime,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = task_events)]
pub struct NewTaskEventRecord {
    pub task_id: i32,
    pub event_type: String,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Queryable, Selectable)]
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

#[derive(Debug, Clone, Insertable)]
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ImportTaskDetails {
    pub results_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct TaskDetails {
    pub import: Option<ImportTaskDetails>,
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
    pub id: i32,
    pub task_id: i32,
    pub event_type: String,
    pub message: String,
    pub data: Option<serde_json::Value>,
    pub created_at: NaiveDateTime,
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
    pub details: Option<serde_json::Value>,
    pub created_at: NaiveDateTime,
}

impl TaskRecord {
    pub fn to_response(&self) -> Result<TaskResponse, ApiError> {
        let kind = TaskKind::from_db(&self.kind)?;
        let status = TaskStatus::from_db(&self.status)?;
        let task_url = format!("/api/v1/tasks/{}", self.id);
        let import_url = (kind == TaskKind::Import).then(|| format!("/api/v1/imports/{}", self.id));
        let import_results =
            (kind == TaskKind::Import).then(|| format!("/api/v1/imports/{}/results", self.id));

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
            },
            details: import_results.map(|results_url| TaskDetails {
                import: Some(ImportTaskDetails { results_url }),
            }),
        })
    }
}

impl From<TaskEventRecord> for TaskEventResponse {
    fn from(value: TaskEventRecord) -> Self {
        Self {
            id: value.id,
            task_id: value.task_id,
            event_type: value.event_type,
            message: value.message,
            data: value.data,
            created_at: value.created_at,
        }
    }
}

impl From<ImportTaskResultRecord> for ImportTaskResultResponse {
    fn from(value: ImportTaskResultRecord) -> Self {
        Self {
            id: value.id,
            task_id: value.task_id,
            item_ref: value.item_ref,
            entity_kind: value.entity_kind,
            action: value.action,
            identifier: value.identifier,
            outcome: value.outcome,
            error: value.error,
            details: value.details,
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
            FilterField::Id => Ok(CursorValue::Integer(self.id as i64)),
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
