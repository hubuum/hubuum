//! Shared PostgreSQL task projections used by queue and worker operations.

use chrono::NaiveDateTime;
use diesel::{Queryable, Selectable};
use hubuum_storage_core::{
    StorageTask, StorageTaskKind, StorageTaskProgress, StorageTaskScopeSnapshot, StorageTaskStatus,
};
use serde_json::Value;
use uuid::Uuid;

use crate::PostgresStorageError;

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::tasks)]
pub(super) struct TaskRow {
    pub(super) id: i32,
    pub(super) kind: String,
    pub(super) status: String,
    pub(super) submitted_by: Option<i32>,
    pub(super) idempotency_key: Option<String>,
    pub(super) request_hash: Option<String>,
    pub(super) request_payload: Option<Value>,
    pub(super) summary: Option<String>,
    pub(super) total_items: i32,
    pub(super) processed_items: i32,
    pub(super) success_items: i32,
    pub(super) failed_items: i32,
    pub(super) submitted_token_id: Option<i32>,
    pub(super) submitted_token_scoped: bool,
    pub(super) submitted_token_scopes: Value,
    pub(super) request_redacted_at: Option<NaiveDateTime>,
    pub(super) started_at: Option<NaiveDateTime>,
    pub(super) finished_at: Option<NaiveDateTime>,
    pub(super) deleted_at: Option<NaiveDateTime>,
    pub(super) deleted_by: Option<i32>,
    pub(super) created_at: NaiveDateTime,
    pub(super) updated_at: NaiveDateTime,
    pub(super) lease_token: Option<Uuid>,
    pub(super) lease_expires_at: Option<NaiveDateTime>,
    pub(super) attempt_count: i32,
    pub(super) initiator_user_id: Option<i32>,
}

impl TaskRow {
    pub(super) fn into_storage(self) -> Result<StorageTask, PostgresStorageError> {
        let kind = StorageTaskKind::from_persisted(&self.kind).ok_or_else(|| {
            PostgresStorageError::database(format!("Unknown stored task kind '{}'", self.kind))
        })?;
        let status = StorageTaskStatus::from_persisted(&self.status).ok_or_else(|| {
            PostgresStorageError::database(format!("Unknown stored task status '{}'", self.status))
        })?;
        Ok(
            StorageTask::builder(self.id, kind, status, self.created_at, self.updated_at)
                .submitted_by(self.submitted_by)
                .idempotency_key(self.idempotency_key)
                .request_hash(self.request_hash)
                .request_payload(self.request_payload)
                .summary(self.summary)
                .progress(StorageTaskProgress::new(
                    self.total_items,
                    self.processed_items,
                    self.success_items,
                    self.failed_items,
                ))
                .scope_snapshot(StorageTaskScopeSnapshot::new(
                    self.submitted_token_id,
                    self.submitted_token_scoped,
                    self.submitted_token_scopes,
                ))
                .request_redacted_at(self.request_redacted_at)
                .started_at(self.started_at)
                .finished_at(self.finished_at)
                .deletion(self.deleted_at, self.deleted_by)
                .lease(self.lease_token, self.lease_expires_at)
                .attempt_count(self.attempt_count)
                .initiator_principal_id(self.initiator_user_id)
                .build(),
        )
    }
}
