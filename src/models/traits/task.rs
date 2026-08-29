//! Application behavior for backend-neutral task identifiers.

use crate::errors::ApiError;
use crate::models::{TaskID, TaskRecord};
use crate::permissions::AuthorizationContext;
use crate::traits::AuthzSubject;

/// Authorization-aware task loads supplied by the application layer.
///
/// The identifier remains a persistence- and authorization-free domain type;
/// this extension trait owns behavior that needs application services.
pub trait TaskAuthorizationExt {
    async fn load_authorized(
        &self,
        backend: &impl AuthorizationContext,
        requestor: &(impl AuthzSubject + ?Sized),
    ) -> Result<TaskRecord, ApiError>;

    async fn load_authorized_export(
        &self,
        backend: &impl AuthorizationContext,
        requestor: &(impl AuthzSubject + ?Sized),
    ) -> Result<TaskRecord, ApiError>;

    async fn load_authorized_backup(
        &self,
        backend: &impl AuthorizationContext,
        requestor: &(impl AuthzSubject + ?Sized),
    ) -> Result<TaskRecord, ApiError>;

    async fn load_authorized_import(
        &self,
        backend: &impl AuthorizationContext,
        requestor: &(impl AuthzSubject + ?Sized),
    ) -> Result<TaskRecord, ApiError>;
}

impl TaskAuthorizationExt for TaskID {
    async fn load_authorized(
        &self,
        backend: &impl AuthorizationContext,
        requestor: &(impl AuthzSubject + ?Sized),
    ) -> Result<TaskRecord, ApiError> {
        crate::services::tasks::load_authorized_task(backend, requestor, *self).await
    }

    async fn load_authorized_export(
        &self,
        backend: &impl AuthorizationContext,
        requestor: &(impl AuthzSubject + ?Sized),
    ) -> Result<TaskRecord, ApiError> {
        crate::services::tasks::load_authorized_export(backend, requestor, *self).await
    }

    async fn load_authorized_backup(
        &self,
        backend: &impl AuthorizationContext,
        requestor: &(impl AuthzSubject + ?Sized),
    ) -> Result<TaskRecord, ApiError> {
        crate::services::tasks::load_authorized_backup(backend, requestor, *self).await
    }

    async fn load_authorized_import(
        &self,
        backend: &impl AuthorizationContext,
        requestor: &(impl AuthzSubject + ?Sized),
    ) -> Result<TaskRecord, ApiError> {
        crate::services::tasks::load_authorized_import(backend, requestor, *self).await
    }
}
