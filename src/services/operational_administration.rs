//! Backend-neutral administrator diagnostics.

use crate::errors::ApiError;
use crate::storage::{
    OperationalExportTemplateAuditEntry, OperationalExportTemplateHealth, OperationalStateStorage,
    OperationalStorageSnapshot, OperationalTaskQueueSnapshot, StorageHandle,
};

pub(crate) async fn storage_snapshot(
    storage: &StorageHandle,
) -> Result<OperationalStorageSnapshot, ApiError> {
    Ok(storage.storage_snapshot().await?)
}

pub(crate) async fn task_queue_snapshot(
    storage: &StorageHandle,
) -> Result<OperationalTaskQueueSnapshot, ApiError> {
    Ok(storage.task_queue_snapshot().await?)
}

pub(crate) async fn export_template_health(
    storage: &StorageHandle,
) -> Result<Vec<OperationalExportTemplateHealth>, ApiError> {
    Ok(storage.export_template_health().await?)
}

pub(crate) async fn export_templates_for_audit(
    storage: &StorageHandle,
) -> Result<Vec<OperationalExportTemplateAuditEntry>, ApiError> {
    Ok(storage.export_templates_for_audit().await?)
}
