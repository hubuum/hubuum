//! Authorization-aware task loads backed by the neutral task application service.

use crate::errors::ApiError;
use crate::models::{TaskID, TaskRecord};
use crate::storage::StorageContext;
use crate::traits::AuthzSubject;

impl TaskID {
    pub async fn load_authorized(
        &self,
        backend: &impl StorageContext,
        requestor: &(impl AuthzSubject + ?Sized),
    ) -> Result<TaskRecord, ApiError> {
        crate::services::tasks::load_authorized_task(backend, requestor, *self).await
    }

    pub async fn load_authorized_export(
        &self,
        backend: &impl StorageContext,
        requestor: &(impl AuthzSubject + ?Sized),
    ) -> Result<TaskRecord, ApiError> {
        crate::services::tasks::load_authorized_export(backend, requestor, *self).await
    }

    pub async fn load_authorized_backup(
        &self,
        backend: &impl StorageContext,
        requestor: &(impl AuthzSubject + ?Sized),
    ) -> Result<TaskRecord, ApiError> {
        crate::services::tasks::load_authorized_backup(backend, requestor, *self).await
    }

    pub async fn load_authorized_import(
        &self,
        backend: &impl StorageContext,
        requestor: &(impl AuthzSubject + ?Sized),
    ) -> Result<TaskRecord, ApiError> {
        crate::services::tasks::load_authorized_import(backend, requestor, *self).await
    }
}
