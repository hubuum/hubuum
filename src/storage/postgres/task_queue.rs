use async_trait::async_trait;
use hubuum_storage_postgres::operations::task_queue as postgres_task_queue;

use crate::errors::ApiError;
use crate::storage::{
    StorageBackupOutput, StorageBackupOutputSummary, StorageError, StorageExportOutput,
    StorageExportOutputSummary, StorageImportTaskResultPage, StorageTask, StorageTaskAccess,
    StorageTaskCreateRequest, StorageTaskEventPage, StorageTaskKind, StorageTaskListQuery,
    StorageTaskOutputLookup, StorageTaskPage, StorageTaskPageQuery, StorageTaskProgress,
    StorageTaskScopeSnapshot, StorageTaskStatus, TaskQueueStorage,
};

use super::PostgresStorage;
use super::operations::task_rows::TaskRow as TaskRecord;

#[async_trait]
impl TaskQueueStorage for PostgresStorage {
    async fn create_task(
        &self,
        request: StorageTaskCreateRequest,
    ) -> Result<StorageTask, StorageError> {
        postgres_task_queue::create_task(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn get_task_access(&self, task_id: i32) -> Result<StorageTaskAccess, StorageError> {
        postgres_task_queue::get_task_access(self.runtime(), task_id)
            .await
            .map_err(StorageError::from)
    }

    async fn list_tasks(
        &self,
        query: StorageTaskListQuery,
    ) -> Result<StorageTaskPage, StorageError> {
        postgres_task_queue::list_tasks(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_task_events(
        &self,
        query: StorageTaskPageQuery,
    ) -> Result<StorageTaskEventPage, StorageError> {
        postgres_task_queue::list_task_events(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_import_task_results(
        &self,
        query: StorageTaskPageQuery,
    ) -> Result<StorageImportTaskResultPage, StorageError> {
        postgres_task_queue::list_import_task_results(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_export_output_summaries(
        &self,
        task_ids: Vec<i32>,
    ) -> Result<Vec<StorageExportOutputSummary>, StorageError> {
        postgres_task_queue::list_export_output_summaries(self.runtime(), task_ids)
            .await
            .map_err(StorageError::from)
    }

    async fn list_backup_output_summaries(
        &self,
        task_ids: Vec<i32>,
    ) -> Result<Vec<StorageBackupOutputSummary>, StorageError> {
        postgres_task_queue::list_backup_output_summaries(self.runtime(), task_ids)
            .await
            .map_err(StorageError::from)
    }

    async fn get_export_output_summary(
        &self,
        task_id: i32,
    ) -> Result<StorageTaskOutputLookup<StorageExportOutputSummary>, StorageError> {
        postgres_task_queue::get_export_output_summary(self.runtime(), task_id)
            .await
            .map_err(StorageError::from)
    }

    async fn get_backup_output_summary(
        &self,
        task_id: i32,
    ) -> Result<StorageTaskOutputLookup<StorageBackupOutputSummary>, StorageError> {
        postgres_task_queue::get_backup_output_summary(self.runtime(), task_id)
            .await
            .map_err(StorageError::from)
    }

    async fn get_export_output(
        &self,
        task_id: i32,
    ) -> Result<StorageTaskOutputLookup<StorageExportOutput>, StorageError> {
        postgres_task_queue::get_export_output(self.runtime(), task_id)
            .await
            .map_err(StorageError::from)
    }

    async fn get_backup_output(
        &self,
        task_id: i32,
    ) -> Result<StorageTaskOutputLookup<StorageBackupOutput>, StorageError> {
        postgres_task_queue::get_backup_output(self.runtime(), task_id)
            .await
            .map_err(StorageError::from)
    }
}

pub(super) fn task_to_storage(task: TaskRecord) -> Result<StorageTask, ApiError> {
    let kind = StorageTaskKind::from_persisted(&task.kind).ok_or_else(|| {
        ApiError::InternalServerError(format!("Unknown stored task kind '{}'", task.kind))
    })?;
    let status = StorageTaskStatus::from_persisted(&task.status).ok_or_else(|| {
        ApiError::InternalServerError(format!("Unknown stored task status '{}'", task.status))
    })?;
    Ok(
        StorageTask::builder(task.id, kind, status, task.created_at, task.updated_at)
            .submitted_by(task.submitted_by)
            .idempotency_key(task.idempotency_key)
            .request_hash(task.request_hash)
            .request_payload(task.request_payload)
            .summary(task.summary)
            .progress(StorageTaskProgress::new(
                task.total_items,
                task.processed_items,
                task.success_items,
                task.failed_items,
            ))
            .scope_snapshot(StorageTaskScopeSnapshot::new(
                task.submitted_token_id,
                task.submitted_token_scoped,
                task.submitted_token_scopes,
            ))
            .request_redacted_at(task.request_redacted_at)
            .started_at(task.started_at)
            .finished_at(task.finished_at)
            .deletion(task.deleted_at, task.deleted_by)
            .lease(task.lease_token, task.lease_expires_at)
            .attempt_count(task.attempt_count)
            .initiator_principal_id(task.initiator_user_id)
            .build(),
    )
}
