use async_trait::async_trait;
use chrono::NaiveDateTime;
use uuid::Uuid;

use crate::storage::{
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
        hubuum_storage_postgres::operations::restore_lifecycle::stage_restore(
            self.runtime(),
            request,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn get_restore_job(&self, job_id: i64) -> Result<StorageRestoreJob, StorageError> {
        hubuum_storage_postgres::operations::restore_lifecycle::get_restore_job(
            self.runtime(),
            job_id,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn get_restore_status(&self, job_id: i64) -> Result<StorageRestoreStatus, StorageError> {
        hubuum_storage_postgres::operations::restore_lifecycle::get_restore_status(
            self.runtime(),
            job_id,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn expire_restore_stage(&self, job_id: i64) -> Result<bool, StorageError> {
        hubuum_storage_postgres::operations::restore_lifecycle::expire_restore_stage(
            self.runtime(),
            job_id,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn start_restore_draining(&self, job_id: i64) -> Result<NaiveDateTime, StorageError> {
        hubuum_storage_postgres::operations::restore_lifecycle::start_restore_draining(
            self.runtime(),
            job_id,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn apply_restore(
        &self,
        request: StorageRestoreApply,
    ) -> Result<StorageRestoreCompletion, StorageError> {
        hubuum_storage_postgres::operations::restore_lifecycle::apply_restore(
            self.runtime(),
            request,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn fail_restore_and_resume(
        &self,
        request: StorageRestoreFailure,
    ) -> Result<(), StorageError> {
        hubuum_storage_postgres::operations::restore_lifecycle::fail_restore_and_resume(
            self.runtime(),
            request,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn restore_coordinator_snapshot(
        &self,
    ) -> Result<StorageRestoreCoordinatorSnapshot, StorageError> {
        hubuum_storage_postgres::operations::restore_lifecycle::restore_coordinator_snapshot(
            self.runtime(),
        )
        .await
        .map_err(StorageError::from)
    }

    async fn resume_maintenance_without_restore(&self) -> Result<(), StorageError> {
        hubuum_storage_postgres::operations::restore_lifecycle::resume_maintenance_without_restore(
            self.runtime(),
        )
        .await
        .map_err(StorageError::from)
    }

    async fn resume_terminal_restore(&self, job_id: i64) -> Result<(), StorageError> {
        hubuum_storage_postgres::operations::restore_lifecycle::resume_terminal_restore(
            self.runtime(),
            job_id,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn tick_restore_coordinator(
        &self,
        instance_id: Uuid,
        local_work_is_idle: &(dyn Fn() -> bool + Send + Sync),
        expire_validated_jobs: bool,
    ) -> Result<StorageRestoreCoordinatorSnapshot, StorageError> {
        hubuum_storage_postgres::operations::restore_lifecycle::tick_restore_coordinator(
            self.runtime(),
            instance_id,
            local_work_is_idle,
            expire_validated_jobs,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn restore_drain_state(
        &self,
        heartbeat_cutoff: NaiveDateTime,
    ) -> Result<StorageRestoreDrainState, StorageError> {
        hubuum_storage_postgres::operations::restore_lifecycle::restore_drain_state(
            self.runtime(),
            heartbeat_cutoff,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn remove_restore_instance(&self, instance_id: Uuid) -> Result<(), StorageError> {
        hubuum_storage_postgres::operations::restore_lifecycle::remove_restore_instance(
            self.runtime(),
            instance_id,
        )
        .await
        .map_err(StorageError::from)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use hubuum_domain::MaintenanceState;
    use hubuum_storage_postgres::PostgresRuntime;
    use uuid::Uuid;

    use crate::storage::postgres::capture_queries;
    use crate::tests::get_test_pool;

    #[actix_rt::test]
    async fn restore_coordinator_tick_uses_one_pool_checkout() {
        let pool = get_test_pool();
        let runtime = PostgresRuntime::new(pool.get_ref().clone());
        let instance_id = Uuid::new_v4();
        let local_work_is_idle = || true;

        let (snapshot, queries) = capture_queries(
            hubuum_storage_postgres::operations::restore_lifecycle::tick_restore_coordinator(
                &runtime,
                instance_id,
                &local_work_is_idle,
                false,
            ),
        )
        .await;

        assert!(
            snapshot.is_ok(),
            "restore coordinator tick failed: {snapshot:?}"
        );
        assert_eq!(queries.connection_checkouts(), 1);
        hubuum_storage_postgres::operations::restore_lifecycle::remove_restore_instance(
            &runtime,
            instance_id,
        )
        .await
        .unwrap();
    }

    #[actix_rt::test]
    async fn restore_coordinator_does_not_sample_activity_before_observing_draining() {
        let pool = get_test_pool();
        let runtime = PostgresRuntime::new(pool.get_ref().clone());
        let instance_id = Uuid::new_v4();
        let sampled = Arc::new(AtomicBool::new(false));
        let sampled_by_tick = sampled.clone();
        let local_work_is_idle = move || {
            sampled_by_tick.store(true, Ordering::Release);
            true
        };

        let snapshot =
            hubuum_storage_postgres::operations::restore_lifecycle::tick_restore_coordinator(
                &runtime,
                instance_id,
                &local_work_is_idle,
                false,
            )
            .await
            .unwrap();

        assert_eq!(snapshot.maintenance_state(), MaintenanceState::Normal);
        assert!(!sampled.load(Ordering::Acquire));
        hubuum_storage_postgres::operations::restore_lifecycle::remove_restore_instance(
            &runtime,
            instance_id,
        )
        .await
        .unwrap();
    }
}
