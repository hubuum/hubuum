//! Authorization-aware task loads, as methods on the `TaskID` newtype.
//!
//! Tasks are user-owned rather than namespace-scoped, so they do not use the `PermissionController`
//! path the other resources do. Authorization is "the submitter or an admin"; denial (and a kind
//! mismatch) returns a `404` rather than a `403` so the existence of another user's task is not
//! revealed. These methods replace the per-handler free functions that previously took a bare id.

use crate::db::DbPool;
use crate::db::traits::task::TaskBackend;
use crate::errors::ApiError;
use crate::models::{TaskID, TaskKind, TaskRecord, User};
use crate::traits::GroupMemberships;

impl TaskID {
    /// Load this task for `user`, enforcing ownership-or-admin authorization.
    pub async fn load_authorized(
        &self,
        pool: &DbPool,
        user: &User,
    ) -> Result<TaskRecord, ApiError> {
        self.load_authorized_of_kind(pool, user, None, "Task").await
    }

    /// Load this task, additionally requiring it to be a report task.
    pub async fn load_authorized_report(
        &self,
        pool: &DbPool,
        user: &User,
    ) -> Result<TaskRecord, ApiError> {
        self.load_authorized_of_kind(pool, user, Some(TaskKind::Report), "Report task")
            .await
    }

    /// Load this task, additionally requiring it to be an import task.
    pub async fn load_authorized_import(
        &self,
        pool: &DbPool,
        user: &User,
    ) -> Result<TaskRecord, ApiError> {
        self.load_authorized_of_kind(pool, user, Some(TaskKind::Import), "Import task")
            .await
    }

    async fn load_authorized_of_kind(
        &self,
        pool: &DbPool,
        user: &User,
        kind: Option<TaskKind>,
        label: &str,
    ) -> Result<TaskRecord, ApiError> {
        let task = self.find_record(pool).await?;

        if let Some(kind) = kind
            && task.kind != kind.as_str()
        {
            return Err(ApiError::NotFound(format!(
                "{label} {} not found",
                self.id()
            )));
        }

        if task.submitted_by == Some(user.id) || user.is_admin(pool).await? {
            Ok(task)
        } else {
            Err(ApiError::NotFound(format!("{label} not found")))
        }
    }
}
