use crate::operations::task_control as postgres_task_control;
use crate::operations::task_execution as postgres_task_execution;
use async_trait::async_trait;
use hubuum_storage_core::{
    StorageTaskCancellationOutcome, StorageTaskCancellationRequest, StorageTaskExecutionAdmission,
    StorageTaskExecutionObservation, StorageTaskRemoteDispatch,
};

use hubuum_storage_core::{
    StorageError, StorageTask, StorageTaskActiveUpdate, StorageTaskClaim, StorageTaskCompletion,
    StorageTaskEventAppend, StorageTaskFailure, StorageTaskLease, StorageTaskLeaseDuration,
    TaskExecutionStorage,
};

use super::PostgresStorage;

#[async_trait]
impl TaskExecutionStorage for PostgresStorage {
    async fn request_task_cancellation(
        &self,
        request: StorageTaskCancellationRequest,
    ) -> Result<StorageTaskCancellationOutcome, StorageError> {
        postgres_task_control::request_task_cancellation(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn admit_task_execution(
        &self,
        request: StorageTaskExecutionAdmission,
    ) -> Result<StorageTaskExecutionObservation, StorageError> {
        postgres_task_control::admit_task_execution(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn poll_task_execution(
        &self,
        lease: StorageTaskLease,
    ) -> Result<StorageTaskExecutionObservation, StorageError> {
        postgres_task_control::poll_task_execution(self.runtime(), lease)
            .await
            .map_err(StorageError::from)
    }

    async fn acknowledge_task_stop(
        &self,
        lease: StorageTaskLease,
    ) -> Result<StorageTask, StorageError> {
        postgres_task_control::acknowledge_task_stop(self.runtime(), lease)
            .await
            .map_err(StorageError::from)
    }

    async fn begin_remote_dispatch(
        &self,
        request: StorageTaskRemoteDispatch,
    ) -> Result<(), StorageError> {
        postgres_task_control::begin_remote_dispatch(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

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
