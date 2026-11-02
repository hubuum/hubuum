use crate::operations::task_queue as postgres_task_queue;
use async_trait::async_trait;
use hubuum_domain::TaskId;

use hubuum_storage_core::{
    StorageBackupOutput, StorageBackupOutputSummary, StorageError, StorageExportOutput,
    StorageExportOutputSummary, StorageImportTaskResult, StoragePage, StorageTask,
    StorageTaskAccess, StorageTaskChildListQuery, StorageTaskCreateRequest, StorageTaskEvent,
    StorageTaskListQuery, StorageTaskOutputLookup, TaskQueueStorage,
};

use super::PostgresStorage;

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

    async fn get_task_access(&self, task_id: TaskId) -> Result<StorageTaskAccess, StorageError> {
        postgres_task_queue::get_task_access(self.runtime(), task_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn list_tasks(
        &self,
        query: StorageTaskListQuery,
    ) -> Result<StoragePage<StorageTask>, StorageError> {
        postgres_task_queue::list_tasks(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_task_events(
        &self,
        query: StorageTaskChildListQuery,
    ) -> Result<StoragePage<StorageTaskEvent>, StorageError> {
        postgres_task_queue::list_task_events(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_import_task_results(
        &self,
        query: StorageTaskChildListQuery,
    ) -> Result<StoragePage<StorageImportTaskResult>, StorageError> {
        postgres_task_queue::list_import_task_results(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_export_output_summaries(
        &self,
        task_ids: Vec<TaskId>,
    ) -> Result<Vec<StorageExportOutputSummary>, StorageError> {
        let task_ids = task_ids.into_iter().map(TaskId::id).collect();
        postgres_task_queue::list_export_output_summaries(self.runtime(), task_ids)
            .await
            .map_err(StorageError::from)
    }

    async fn list_backup_output_summaries(
        &self,
        task_ids: Vec<TaskId>,
    ) -> Result<Vec<StorageBackupOutputSummary>, StorageError> {
        let task_ids = task_ids.into_iter().map(TaskId::id).collect();
        postgres_task_queue::list_backup_output_summaries(self.runtime(), task_ids)
            .await
            .map_err(StorageError::from)
    }

    async fn get_export_output_summary(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageExportOutputSummary>, StorageError> {
        postgres_task_queue::get_export_output_summary(self.runtime(), task_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn get_backup_output_summary(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageBackupOutputSummary>, StorageError> {
        postgres_task_queue::get_backup_output_summary(self.runtime(), task_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn get_export_output(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageExportOutput>, StorageError> {
        postgres_task_queue::get_export_output(self.runtime(), task_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn get_backup_output(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageBackupOutput>, StorageError> {
        postgres_task_queue::get_backup_output(self.runtime(), task_id.id())
            .await
            .map_err(StorageError::from)
    }
}
