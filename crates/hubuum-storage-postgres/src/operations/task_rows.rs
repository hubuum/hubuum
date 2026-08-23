//! Shared PostgreSQL task projections used by queue and worker operations.

use chrono::NaiveDateTime;
use diesel::{Queryable, Selectable};
use hubuum_domain::{PrincipalId, TaskId, TokenId};
use hubuum_storage_core::{
    StorageTask, StorageTaskKind, StorageTaskProgress, StorageTaskScopeSnapshot, StorageTaskStatus,
};
use serde_json::Value;
use uuid::Uuid;

use crate::PostgresStorageError;

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::tasks)]
pub(crate) struct TaskRow {
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
    pub(crate) fn into_storage(self) -> Result<StorageTask, PostgresStorageError> {
        let kind = StorageTaskKind::from_persisted(&self.kind).ok_or_else(|| {
            PostgresStorageError::database(format!("Unknown stored task kind '{}'", self.kind))
        })?;
        let status = StorageTaskStatus::from_persisted(&self.status).ok_or_else(|| {
            PostgresStorageError::database(format!("Unknown stored task status '{}'", self.status))
        })?;
        let lease_expires_at = projected_lease_expiry(self.lease_token, self.lease_expires_at)?;
        Ok(StorageTask::builder(
            TaskId::new(self.id)?,
            kind,
            status,
            self.created_at.and_utc(),
            self.updated_at.and_utc(),
        )
        .submitted_by(self.submitted_by.map(PrincipalId::new).transpose()?)
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
            self.submitted_token_id.map(TokenId::new).transpose()?,
            self.submitted_token_scoped,
            self.submitted_token_scopes,
        ))
        .request_redacted_at(
            self.request_redacted_at
                .map(|timestamp| timestamp.and_utc()),
        )
        .started_at(self.started_at.map(|timestamp| timestamp.and_utc()))
        .finished_at(self.finished_at.map(|timestamp| timestamp.and_utc()))
        .deletion(
            self.deleted_at.map(|timestamp| timestamp.and_utc()),
            self.deleted_by.map(PrincipalId::new).transpose()?,
        )
        .lease_expires_at(lease_expires_at)
        .attempt_count(self.attempt_count)
        .initiator_principal_id(self.initiator_user_id.map(PrincipalId::new).transpose()?)
        .build())
    }
}

fn projected_lease_expiry(
    token: Option<Uuid>,
    expires_at: Option<NaiveDateTime>,
) -> Result<Option<chrono::DateTime<chrono::Utc>>, PostgresStorageError> {
    match (token, expires_at) {
        (None, None) => Ok(None),
        (Some(_), Some(expires_at)) => Ok(Some(expires_at.and_utc())),
        _ => Err(PostgresStorageError::database(
            "Persisted task lease token and expiry must both be present or absent",
        )),
    }
}

#[cfg(test)]
mod tests {
    use hubuum_storage_core::StorageErrorKind;

    use super::*;

    #[test]
    fn task_projection_hides_the_native_lease_token() {
        let expires_at = chrono::Utc::now().naive_utc();

        let projected = projected_lease_expiry(Some(Uuid::new_v4()), Some(expires_at))
            .expect("a complete persisted lease should project successfully");

        assert_eq!(projected, Some(expires_at.and_utc()));
    }

    #[test]
    fn task_projection_rejects_a_lease_token_without_an_expiry() {
        let error = projected_lease_expiry(Some(Uuid::new_v4()), None)
            .expect_err("a partial persisted lease must be rejected");

        assert_eq!(error.kind(), StorageErrorKind::Backend);
    }

    #[test]
    fn task_projection_rejects_a_lease_expiry_without_a_token() {
        let error = projected_lease_expiry(None, Some(chrono::Utc::now().naive_utc()))
            .expect_err("a partial persisted lease must be rejected");

        assert_eq!(error.kind(), StorageErrorKind::Backend);
    }
}
