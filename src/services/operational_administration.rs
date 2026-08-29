//! Backend-neutral administrator diagnostics.

use crate::errors::ApiError;
use crate::storage::{
    DatabaseStorageSnapshot, OperationalStateStorage, StorageHandle,
    StorageOperationalExportTemplateAuditEntry, StorageOperationalExportTemplateHealth,
    StorageOperationalTaskQueueSnapshot,
};

pub(crate) async fn storage_snapshot(
    storage: &StorageHandle,
) -> Result<Option<DatabaseStorageSnapshot>, ApiError> {
    Ok(storage.database_storage_snapshot().await?)
}

pub(crate) async fn get_task_queue_snapshot(
    storage: &StorageHandle,
) -> Result<StorageOperationalTaskQueueSnapshot, ApiError> {
    Ok(storage.get_task_queue_snapshot().await?)
}

pub(crate) async fn load_export_template_health(
    storage: &StorageHandle,
) -> Result<Vec<StorageOperationalExportTemplateHealth>, ApiError> {
    Ok(storage.load_export_template_health().await?)
}

pub(crate) async fn load_export_templates_for_audit(
    storage: &StorageHandle,
) -> Result<Vec<StorageOperationalExportTemplateAuditEntry>, ApiError> {
    Ok(storage.load_export_templates_for_audit().await?)
}
