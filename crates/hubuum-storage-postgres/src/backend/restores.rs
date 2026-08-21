use async_trait::async_trait;
use chrono::NaiveDateTime;
use hubuum_domain::RestoreJobId;
use uuid::Uuid;

use hubuum_storage_core::{
    RestoreStorage, StorageError, StorageRestoreApply, StorageRestoreCompletion,
    StorageRestoreCoordinatorSnapshot, StorageRestoreDrainState, StorageRestoreFailure,
    StorageRestoreJob, StorageRestoreStageCreate, StorageRestoreStatus,
};

use super::PostgresStorage;

#[async_trait]
impl RestoreStorage for PostgresStorage {
    async fn stage_restore(
        &self,
        request: StorageRestoreStageCreate,
    ) -> Result<StorageRestoreJob, StorageError> {
        crate::operations::restore_lifecycle::stage_restore(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn get_restore_job(
        &self,
        job_id: RestoreJobId,
    ) -> Result<StorageRestoreJob, StorageError> {
        crate::operations::restore_lifecycle::get_restore_job(self.runtime(), job_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn get_restore_status(
        &self,
        job_id: RestoreJobId,
    ) -> Result<StorageRestoreStatus, StorageError> {
        crate::operations::restore_lifecycle::get_restore_status(self.runtime(), job_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn expire_restore_stage(&self, job_id: RestoreJobId) -> Result<bool, StorageError> {
        crate::operations::restore_lifecycle::expire_restore_stage(self.runtime(), job_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn start_restore_draining(
        &self,
        job_id: RestoreJobId,
    ) -> Result<NaiveDateTime, StorageError> {
        crate::operations::restore_lifecycle::start_restore_draining(self.runtime(), job_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn apply_restore(
        &self,
        request: StorageRestoreApply,
    ) -> Result<StorageRestoreCompletion, StorageError> {
        crate::operations::restore_lifecycle::apply_restore(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn fail_restore_and_resume(
        &self,
        request: StorageRestoreFailure,
    ) -> Result<(), StorageError> {
        crate::operations::restore_lifecycle::fail_restore_and_resume(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn get_restore_coordinator_snapshot(
        &self,
    ) -> Result<StorageRestoreCoordinatorSnapshot, StorageError> {
        crate::operations::restore_lifecycle::get_restore_coordinator_snapshot(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn resume_maintenance_without_restore(&self) -> Result<(), StorageError> {
        crate::operations::restore_lifecycle::resume_maintenance_without_restore(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn resume_terminal_restore(&self, job_id: RestoreJobId) -> Result<(), StorageError> {
        crate::operations::restore_lifecycle::resume_terminal_restore(self.runtime(), job_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn tick_restore_coordinator(
        &self,
        instance_id: Uuid,
        local_work_is_idle: &(dyn Fn() -> bool + Send + Sync),
        expire_validated_jobs: bool,
    ) -> Result<StorageRestoreCoordinatorSnapshot, StorageError> {
        crate::operations::restore_lifecycle::tick_restore_coordinator(
            self.runtime(),
            instance_id,
            local_work_is_idle,
            expire_validated_jobs,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn get_restore_drain_state(
        &self,
        heartbeat_cutoff: NaiveDateTime,
    ) -> Result<StorageRestoreDrainState, StorageError> {
        crate::operations::restore_lifecycle::get_restore_drain_state(
            self.runtime(),
            heartbeat_cutoff,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn remove_restore_instance(&self, instance_id: Uuid) -> Result<(), StorageError> {
        crate::operations::restore_lifecycle::remove_restore_instance(self.runtime(), instance_id)
            .await
            .map_err(StorageError::from)
    }
}
