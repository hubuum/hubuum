use crate::operations::task_queue as postgres_task_queue;
use async_trait::async_trait;

use hubuum_storage_core::{
    StorageBackupOutput, StorageBackupOutputSummary, StorageError, StorageExportOutput,
    StorageExportOutputSummary, StorageImportTaskResultPage, StorageTask, StorageTaskAccess,
    StorageTaskCreateRequest, StorageTaskEventPage, StorageTaskListQuery, StorageTaskOutputLookup,
    StorageTaskPage, StorageTaskPageQuery, TaskQueueStorage,
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
