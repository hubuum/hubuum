use async_trait::async_trait;
use chrono::NaiveDateTime;
use uuid::Uuid;

use crate::storage::{
    RestoreStorage, StorageError, StorageRestoreApply, StorageRestoreArtifactSummary,
    StorageRestoreCompletion, StorageRestoreCoordinatorSnapshot, StorageRestoreDrainState,
    StorageRestoreFailure, StorageRestoreInitiator, StorageRestoreInstance, StorageRestoreJob,
    StorageRestoreJobStatus, StorageRestoreJobSummary, StorageRestoreStageCreate,
    StorageRestoreStatus, StorageRestoreTimestamps,
};

use super::error::map_postgres_error;
use super::operations::restore::{
    NewRestoreJobRow, RestoreJobRow, RestoreJobStatusRecord, ServerInstanceRow, apply_restore_db,
    delete_server_instance_db, expire_restore_stage_db, fail_restore_and_resume_db,
    insert_restore_job_db, load_restore_coordinator_snapshot_db, load_restore_job_db,
    load_restore_status_job_db, maintenance_generation_and_instances_db,
    restore_coordinator_tick_db, resume_maintenance_without_job_db, resume_terminal_restore_db,
    start_restore_draining_db,
};
use super::{PostgresStorage, StorageCallSite, with_storage_call_site};

struct RestoreSummaryParts {
    id: i64,
    status: String,
    requested_by: Option<i32>,
    requested_by_identity_scope: String,
    requested_by_name: String,
    byte_size: i64,
    sha256: String,
    error: Option<String>,
    expires_at: NaiveDateTime,
    confirmed_at: Option<NaiveDateTime>,
    finished_at: Option<NaiveDateTime>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

fn summary_from_parts(
    parts: RestoreSummaryParts,
) -> Result<StorageRestoreJobSummary, StorageError> {
    let RestoreSummaryParts {
        id,
        status,
        requested_by,
        requested_by_identity_scope,
        requested_by_name,
        byte_size,
        sha256,
        error,
        expires_at,
        confirmed_at,
        finished_at,
        created_at,
        updated_at,
    } = parts;
    Ok(StorageRestoreJobSummary::new(
        id,
        StorageRestoreJobStatus::from_stored(&status)?,
        StorageRestoreInitiator::new(requested_by, requested_by_identity_scope, requested_by_name),
        StorageRestoreArtifactSummary::new(byte_size, sha256),
        error,
        StorageRestoreTimestamps::new(
            expires_at,
            confirmed_at,
            finished_at,
            created_at,
            updated_at,
        ),
    ))
}

fn job_to_storage(row: RestoreJobRow) -> Result<StorageRestoreJob, StorageError> {
    let RestoreJobRow {
        id,
        status,
        requested_by,
        requested_by_identity_scope,
        requested_by_name,
        document,
        byte_size,
        sha256,
        capability_hash,
        error,
        expires_at,
        confirmed_at,
        finished_at,
        created_at,
        updated_at,
    } = row;
    let summary = summary_from_parts(RestoreSummaryParts {
        id,
        status,
        requested_by,
        requested_by_identity_scope,
        requested_by_name,
        byte_size,
        sha256,
        error,
        expires_at,
        confirmed_at,
        finished_at,
        created_at,
        updated_at,
    })?;
    Ok(StorageRestoreJob::new(summary, document, capability_hash))
}

fn status_to_storage(row: RestoreJobStatusRecord) -> Result<StorageRestoreStatus, StorageError> {
    let RestoreJobStatusRecord {
        id,
        status,
        requested_by,
        requested_by_identity_scope,
        requested_by_name,
        byte_size,
        sha256,
        capability_hash,
        validation_summary,
        error,
        expires_at,
        confirmed_at,
        finished_at,
        created_at,
        updated_at,
    } = row;
    let summary = summary_from_parts(RestoreSummaryParts {
        id,
        status,
        requested_by,
        requested_by_identity_scope,
        requested_by_name,
        byte_size,
        sha256,
        error,
        expires_at,
        confirmed_at,
        finished_at,
        created_at,
        updated_at,
    })?;
    Ok(StorageRestoreStatus::new(
        summary,
        capability_hash,
        validation_summary,
    ))
}

fn instance_to_storage(instance: ServerInstanceRow) -> StorageRestoreInstance {
    StorageRestoreInstance::new(
        instance.instance_id,
        instance.maintenance_generation,
        instance.drained,
    )
}

#[async_trait]
impl RestoreStorage for PostgresStorage {
    async fn stage_restore(
        &self,
        request: StorageRestoreStageCreate,
    ) -> Result<StorageRestoreJob, StorageError> {
        let (initiator, document, artifact, capability_hash, validation_summary, expires_at) =
            request.into_parts();
        let (requested_by, requested_by_identity_scope, requested_by_name) = initiator.into_parts();
        let (byte_size, sha256) = artifact.into_parts();
        insert_restore_job_db(
            self.pool(),
            NewRestoreJobRow {
                status: StorageRestoreJobStatus::Validated.as_str().to_string(),
                requested_by,
                requested_by_identity_scope,
                requested_by_name,
                document,
                byte_size,
                sha256,
                capability_hash,
                validation_summary,
                expires_at,
            },
        )
        .await
        .map_err(map_postgres_error)
        .and_then(job_to_storage)
    }

    async fn get_restore_job(&self, job_id: i64) -> Result<StorageRestoreJob, StorageError> {
        load_restore_job_db(self.pool(), job_id)
            .await
            .map_err(map_postgres_error)
            .and_then(job_to_storage)
    }

    async fn get_restore_status(&self, job_id: i64) -> Result<StorageRestoreStatus, StorageError> {
        load_restore_status_job_db(self.pool(), job_id)
            .await
            .map_err(map_postgres_error)
            .and_then(status_to_storage)
    }

    async fn expire_restore_stage(&self, job_id: i64) -> Result<bool, StorageError> {
        expire_restore_stage_db(self.pool(), job_id)
            .await
            .map(|changed| changed == 1)
            .map_err(map_postgres_error)
    }

    async fn start_restore_draining(&self, job_id: i64) -> Result<NaiveDateTime, StorageError> {
        start_restore_draining_db(self.pool(), job_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn apply_restore(
        &self,
        request: StorageRestoreApply,
    ) -> Result<StorageRestoreCompletion, StorageError> {
        let (job_id, document) = request.into_parts();
        apply_restore_db(self.pool(), job_id, document)
            .await
            .map_err(map_postgres_error)
    }

    async fn fail_restore_and_resume(
        &self,
        request: StorageRestoreFailure,
    ) -> Result<(), StorageError> {
        let (job_id, stored_error) = request.into_parts();
        fail_restore_and_resume_db(self.pool(), job_id, &stored_error)
            .await
            .map_err(map_postgres_error)
    }

    async fn restore_coordinator_snapshot(
        &self,
    ) -> Result<StorageRestoreCoordinatorSnapshot, StorageError> {
        load_restore_coordinator_snapshot_db(self.pool())
            .await
            .map_err(map_postgres_error)
    }

    async fn resume_maintenance_without_restore(&self) -> Result<(), StorageError> {
        resume_maintenance_without_job_db(self.pool())
            .await
            .map_err(map_postgres_error)
    }

    async fn resume_terminal_restore(&self, job_id: i64) -> Result<(), StorageError> {
        resume_terminal_restore_db(self.pool(), job_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn tick_restore_coordinator(
        &self,
        instance_id: Uuid,
        local_work_is_idle: &(dyn Fn() -> bool + Send + Sync),
        expire_validated_jobs: bool,
    ) -> Result<StorageRestoreCoordinatorSnapshot, StorageError> {
        with_storage_call_site(
            StorageCallSite::RestoreCoordinator,
            restore_coordinator_tick_db(
                self.pool(),
                instance_id,
                local_work_is_idle,
                expire_validated_jobs,
            ),
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn restore_drain_state(
        &self,
        heartbeat_cutoff: NaiveDateTime,
    ) -> Result<StorageRestoreDrainState, StorageError> {
        let (generation, instances) =
            maintenance_generation_and_instances_db(self.pool(), heartbeat_cutoff)
                .await
                .map_err(map_postgres_error)?;
        Ok(StorageRestoreDrainState::new(
            generation,
            instances.into_iter().map(instance_to_storage).collect(),
        ))
    }

    async fn remove_restore_instance(&self, instance_id: Uuid) -> Result<(), StorageError> {
        delete_server_instance_db(self.pool(), instance_id)
            .await
            .map_err(map_postgres_error)
    }
}
