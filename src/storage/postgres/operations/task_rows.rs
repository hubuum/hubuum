use chrono::NaiveDateTime;
use diesel::{Insertable, Queryable, Selectable};
use std::fmt;

use crate::errors::ApiError;
use crate::events::{MutationProvenance, PrincipalId, TaskId};
use crate::models::search::FilterField;
use crate::models::{PrincipalID, TaskResponse};
use crate::pagination::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};
use crate::schema::{backup_task_outputs, export_task_outputs, import_task_results, tasks};

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = tasks)]
pub struct TaskRow {
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

impl TaskRow {
    pub fn worker_provenance(&self) -> MutationProvenance {
        MutationProvenance::worker(
            self.initiator_user_id
                .map(|id| PrincipalId::new(id).expect("stored principal id must be positive")),
            TaskId::new(self.id).expect("stored task id must be positive"),
        )
    }

    pub fn system_provenance(&self) -> MutationProvenance {
        MutationProvenance::system_for_task(
            self.initiator_user_id
                .map(|id| PrincipalId::new(id).expect("stored principal id must be positive")),
            TaskId::new(self.id).expect("stored task id must be positive"),
        )
    }

    pub fn user_provenance(&self, actor: PrincipalID) -> MutationProvenance {
        MutationProvenance::user_for_task(
            PrincipalId::new(actor.id()).expect("validated principal id must be positive"),
            self.initiator_user_id
                .map(|id| PrincipalId::new(id).expect("stored principal id must be positive")),
            TaskId::new(self.id).expect("stored task id must be positive"),
        )
    }
}

impl CursorPaginated for TaskRow {
    fn supports_sort(field: &FilterField) -> bool {
        TaskResponse::supports_sort(field)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        crate::models::TaskRecord::from(self.clone()).cursor_value(field)
    }

    fn default_sort() -> Vec<crate::models::search::SortParam> {
        TaskResponse::default_sort()
    }

    fn tie_breaker_sort() -> Vec<crate::models::search::SortParam> {
        TaskResponse::tie_breaker_sort()
    }
}

impl CursorSqlMapping for TaskRow {
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
                    "Field '{field}' is not orderable for tasks"
                )));
            }
        })
    }
}

impl From<TaskRow> for crate::models::TaskRecord {
    fn from(row: TaskRow) -> Self {
        Self {
            id: row.id,
            kind: row.kind,
            status: row.status,
            submitted_by: row.submitted_by,
            idempotency_key: row.idempotency_key,
            request_hash: row.request_hash,
            request_payload: row.request_payload,
            summary: row.summary,
            total_items: row.total_items,
            processed_items: row.processed_items,
            success_items: row.success_items,
            failed_items: row.failed_items,
            submitted_token_id: row.submitted_token_id,
            submitted_token_scoped: row.submitted_token_scoped,
            submitted_token_scopes: row.submitted_token_scopes,
            request_redacted_at: row.request_redacted_at,
            started_at: row.started_at,
            finished_at: row.finished_at,
            deleted_at: row.deleted_at,
            deleted_by: row.deleted_by,
            created_at: row.created_at,
            updated_at: row.updated_at,
            lease_token: row.lease_token,
            lease_expires_at: row.lease_expires_at,
            attempt_count: row.attempt_count,
            initiator_user_id: row.initiator_user_id,
        }
    }
}

impl From<crate::models::TaskRecord> for TaskRow {
    fn from(record: crate::models::TaskRecord) -> Self {
        Self {
            id: record.id,
            kind: record.kind,
            status: record.status,
            submitted_by: record.submitted_by,
            idempotency_key: record.idempotency_key,
            request_hash: record.request_hash,
            request_payload: record.request_payload,
            summary: record.summary,
            total_items: record.total_items,
            processed_items: record.processed_items,
            success_items: record.success_items,
            failed_items: record.failed_items,
            submitted_token_id: record.submitted_token_id,
            submitted_token_scoped: record.submitted_token_scoped,
            submitted_token_scopes: record.submitted_token_scopes,
            request_redacted_at: record.request_redacted_at,
            started_at: record.started_at,
            finished_at: record.finished_at,
            deleted_at: record.deleted_at,
            deleted_by: record.deleted_by,
            created_at: record.created_at,
            updated_at: record.updated_at,
            lease_token: record.lease_token,
            lease_expires_at: record.lease_expires_at,
            attempt_count: record.attempt_count,
            initiator_user_id: record.initiator_user_id,
        }
    }
}

impl fmt::Debug for TaskRow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        crate::models::TaskRecord::from(self.clone()).fmt(formatter)
    }
}

#[derive(Insertable)]
#[diesel(table_name = tasks)]
pub struct NewTaskRow {
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

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = import_task_results)]
pub struct ImportTaskResultRow {
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
pub struct NewImportTaskResultRow {
    pub task_id: i32,
    pub item_ref: Option<String>,
    pub entity_kind: String,
    pub action: String,
    pub identifier: Option<String>,
    pub outcome: String,
    pub error: Option<String>,
    pub details: Option<serde_json::Value>,
}

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = export_task_outputs)]
pub struct ExportTaskOutputRow {
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
pub struct NewExportTaskOutputRow {
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

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = export_task_outputs)]
pub struct ExportTaskOutputSummaryRow {
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
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = backup_task_outputs)]
pub struct BackupTaskOutputRow {
    pub task_id: i32,
    pub document: Vec<u8>,
    pub byte_size: i64,
    pub sha256: String,
    pub output_expires_at: NaiveDateTime,
    pub created_at: NaiveDateTime,
}

#[derive(Insertable)]
#[diesel(table_name = backup_task_outputs)]
pub struct NewBackupTaskOutputRow {
    pub task_id: i32,
    pub document: Vec<u8>,
    pub byte_size: i64,
    pub sha256: String,
    pub output_expires_at: NaiveDateTime,
}

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = backup_task_outputs)]
pub struct BackupTaskOutputSummaryRow {
    pub task_id: i32,
    pub byte_size: i64,
    pub sha256: String,
    pub output_expires_at: NaiveDateTime,
}
