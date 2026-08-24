use crate::operations::task_execution as postgres_task_execution;
use async_trait::async_trait;

use hubuum_storage_core::{
    StorageError, StorageTask, StorageTaskActiveUpdate, StorageTaskClaim, StorageTaskCompletion,
    StorageTaskEventAppend, StorageTaskFailure, StorageTaskLease, StorageTaskLeaseDuration,
    TaskExecutionStorage,
};

use super::PostgresStorage;

#[async_trait]
impl TaskExecutionStorage for PostgresStorage {
    async fn claim_next_task(
        &self,
        lease_duration: StorageTaskLeaseDuration,
    ) -> Result<Option<StorageTaskClaim>, StorageError> {
        postgres_task_execution::claim_next_task(self.runtime(), lease_duration)
            .await
            .map_err(StorageError::from)
    }

    async fn renew_task_lease(
        &self,
        lease: StorageTaskLease,
        lease_duration: StorageTaskLeaseDuration,
    ) -> Result<bool, StorageError> {
        postgres_task_execution::renew_task_lease(self.runtime(), lease, lease_duration)
            .await
            .map_err(StorageError::from)
    }

    async fn recover_expired_task_leases(
        &self,
        batch_size: usize,
    ) -> Result<Vec<StorageTask>, StorageError> {
        postgres_task_execution::recover_expired_task_leases(self.runtime(), batch_size)
            .await
            .map_err(StorageError::from)
    }

    async fn append_task_event(&self, event: StorageTaskEventAppend) -> Result<(), StorageError> {
        postgres_task_execution::append_task_event(self.runtime(), event)
            .await
            .map_err(StorageError::from)
    }

    async fn update_task_state(
        &self,
        update: StorageTaskActiveUpdate,
    ) -> Result<StorageTask, StorageError> {
        postgres_task_execution::update_task_state(self.runtime(), update)
            .await
            .map_err(StorageError::from)
    }

    async fn complete_task(
        &self,
        completion: StorageTaskCompletion,
    ) -> Result<StorageTask, StorageError> {
        postgres_task_execution::complete_task(self.runtime(), completion)
            .await
            .map_err(StorageError::from)
    }

    async fn fail_task(&self, failure: StorageTaskFailure) -> Result<StorageTask, StorageError> {
        postgres_task_execution::fail_task(self.runtime(), failure)
            .await
            .map_err(StorageError::from)
    }

    async fn purge_expired_export_outputs(&self) -> Result<usize, StorageError> {
        postgres_task_execution::purge_expired_export_outputs(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn purge_expired_backup_outputs(&self) -> Result<usize, StorageError> {
        postgres_task_execution::purge_expired_backup_outputs(self.runtime())
            .await
            .map_err(StorageError::from)
    }
}
