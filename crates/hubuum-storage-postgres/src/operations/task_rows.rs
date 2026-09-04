//! Shared PostgreSQL task projections used by queue and worker operations.

use chrono::NaiveDateTime;
use diesel::{Queryable, Selectable};
use hubuum_domain::{PrincipalId, TaskId, TokenId};
use hubuum_events_core::TraceLink;
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
    pub(super) trace_id: Option<String>,
    pub(super) trace_span_id: Option<String>,
    pub(super) trace_flags: Option<i16>,
    pub(super) trace_context_version: Option<i16>,
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
        let progress = crate::validate_persisted(
            "task progress",
            StorageTaskProgress::try_new(
                self.total_items,
                self.processed_items,
                self.success_items,
                self.failed_items,
            ),
        )?;
        let trace_link = self.trace_link()?;
        let task = StorageTask::builder(
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
        .progress(progress)
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
        .trace_link(trace_link);
        crate::validate_persisted("task projection", task.try_build())
    }

    pub(crate) fn trace_link(&self) -> Result<Option<TraceLink>, PostgresStorageError> {
        task_trace_link(
            self.trace_id.clone(),
            self.trace_span_id.clone(),
            self.trace_flags,
            self.trace_context_version,
        )
    }
}

fn task_trace_link(
    trace_id: Option<String>,
    span_id: Option<String>,
    trace_flags: Option<i16>,
    version: Option<i16>,
) -> Result<Option<TraceLink>, PostgresStorageError> {
    match (trace_id, span_id, trace_flags, version) {
        (None, None, None, None) => Ok(None),
        (Some(trace_id), Some(span_id), Some(trace_flags), Some(version)) => {
            let trace_flags = u8::try_from(trace_flags).map_err(|error| {
                PostgresStorageError::invalid_persisted_value("task trace link", error)
            })?;
            let version = u8::try_from(version).map_err(|error| {
                PostgresStorageError::invalid_persisted_value("task trace link", error)
            })?;
            TraceLink::new(trace_id, span_id, trace_flags, version)
                .map(Some)
                .map_err(|error| {
                    PostgresStorageError::invalid_persisted_value("task trace link", error)
                })
        }
        _ => Err(PostgresStorageError::database(
            "Persisted task trace link must contain either all fields or no fields",
        )),
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

    fn valid_task_row() -> TaskRow {
        let now = chrono::Utc::now().naive_utc();
        TaskRow {
            id: 1,
            kind: StorageTaskKind::Import.as_str().to_string(),
            status: StorageTaskStatus::Queued.as_str().to_string(),
            submitted_by: None,
            idempotency_key: None,
            request_hash: None,
            request_payload: Some(serde_json::json!({})),
            summary: None,
            total_items: 0,
            processed_items: 0,
            success_items: 0,
            failed_items: 0,
            submitted_token_id: None,
            submitted_token_scoped: false,
            submitted_token_scopes: serde_json::json!([]),
            request_redacted_at: None,
            started_at: None,
            finished_at: None,
            deleted_at: None,
            deleted_by: None,
            created_at: now,
            updated_at: now,
            lease_token: None,
            lease_expires_at: None,
            attempt_count: 0,
            initiator_user_id: None,
            trace_id: None,
            trace_span_id: None,
            trace_flags: None,
            trace_context_version: None,
        }
    }

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
    fn task_projection_classifies_negative_progress_as_backend_corruption() {
        let mut row = valid_task_row();
        row.processed_items = -1;

        let error = row
            .into_storage()
            .expect_err("negative persisted progress must be rejected");

        assert_eq!(error.kind(), StorageErrorKind::Backend);
    }

    #[test]
    fn task_projection_classifies_negative_attempts_as_backend_corruption() {
        let mut row = valid_task_row();
        row.attempt_count = -1;

        let error = row
            .into_storage()
            .expect_err("negative persisted attempts must be rejected");

        assert_eq!(error.kind(), StorageErrorKind::Backend);
    }

    #[test]
    fn task_projection_classifies_reversed_timestamps_as_backend_corruption() {
        let mut row = valid_task_row();
        row.updated_at = row.created_at - chrono::Duration::seconds(1);

        let error = row
            .into_storage()
            .expect_err("reversed persisted task timestamps must be rejected");

        assert_eq!(error.kind(), StorageErrorKind::Backend);
    }

    #[test]
    fn task_projection_rejects_a_lease_expiry_without_a_token() {
        let error = projected_lease_expiry(None, Some(chrono::Utc::now().naive_utc()))
            .expect_err("a partial persisted lease must be rejected");

        assert_eq!(error.kind(), StorageErrorKind::Backend);
    }

    #[test]
    fn task_projection_reconstructs_a_valid_trace_link() {
        let mut row = valid_task_row();
        row.trace_id = Some("4bf92f3577b34da6a3ce929d0e0e4736".to_string());
        row.trace_span_id = Some("00f067aa0ba902b7".to_string());
        row.trace_flags = Some(1);
        row.trace_context_version = Some(0);

        let task = row.into_storage().unwrap();
        assert_eq!(
            task.trace_link().map(TraceLink::trace_id),
            Some("4bf92f3577b34da6a3ce929d0e0e4736")
        );
    }
}
