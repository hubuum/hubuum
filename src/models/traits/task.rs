//! Authorization-aware task loads, as methods on the `TaskID` newtype.
//!
//! Tasks are user-owned rather than collection-scoped, so they do not use the `PermissionController`
//! path the other resources do. Authorization is "the submitter or an admin"; denial (and a kind
//! mismatch) returns a `404` rather than a `403` so the existence of another user's task is not
//! revealed. These methods replace the per-handler free functions that previously took a bare id.

use crate::db::DbPool;
use crate::db::traits::service_account::{is_human_owner_group_member, load_service_account_by_id};
use crate::db::traits::task::TaskBackend;
use crate::errors::ApiError;
use crate::models::{TaskID, TaskKind, TaskRecord};
use crate::traits::AuthzSubject;

impl TaskID {
    /// Load this task for `requestor`, enforcing principal-centric authorization.
    pub async fn load_authorized(
        &self,
        pool: &DbPool,
        requestor: &impl AuthzSubject,
    ) -> Result<TaskRecord, ApiError> {
        self.load_authorized_of_kind(pool, requestor, None, "Task")
            .await
    }

    /// Load this task, additionally requiring it to be an export task.
    pub async fn load_authorized_export(
        &self,
        pool: &DbPool,
        requestor: &impl AuthzSubject,
    ) -> Result<TaskRecord, ApiError> {
        self.load_authorized_of_kind(pool, requestor, Some(TaskKind::Export), "Export task")
            .await
    }

    pub async fn load_authorized_backup(
        &self,
        pool: &DbPool,
        requestor: &impl crate::db::traits::authz::AuthzSubject,
    ) -> Result<TaskRecord, ApiError> {
        self.load_authorized_of_kind(pool, requestor, Some(TaskKind::Backup), "Backup task")
            .await
    }

    /// Load this task, additionally requiring it to be an import task.
    pub async fn load_authorized_import(
        &self,
        pool: &DbPool,
        requestor: &impl AuthzSubject,
    ) -> Result<TaskRecord, ApiError> {
        self.load_authorized_of_kind(pool, requestor, Some(TaskKind::Import), "Import task")
            .await
    }

    /// Authorization (denial returns `404` to avoid revealing other principals'
    /// tasks): an **admin**, the **submitting principal itself**, or — when the
    /// task was submitted by a service account — a **human member of that SA's
    /// owner group**.
    async fn load_authorized_of_kind(
        &self,
        pool: &DbPool,
        requestor: &impl AuthzSubject,
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

        let not_found = || ApiError::NotFound(format!("{label} not found"));

        if requestor.is_admin(pool).await? {
            return Ok(task);
        }

        let Some(submitter_id) = task.submitted_by else {
            return Err(not_found());
        };

        // Submitting principal itself.
        if submitter_id == requestor.principal_id() {
            return Ok(task);
        }

        // SA-submitted task: a human member of the SA's owner group may manage it.
        if let Ok(sa) = load_service_account_by_id(pool, submitter_id).await
            && is_human_owner_group_member(pool, requestor.principal_id(), sa.owner_group_id)
                .await?
        {
            return Ok(task);
        }

        Err(not_found())
    }
}
