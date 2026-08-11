#[cfg(not(test))]
use std::sync::OnceLock;
use std::time::Duration;

use async_trait::async_trait;
use uuid::Uuid;

use crate::config::get_config;
use crate::errors::ApiError;
use crate::models::{
    NewBackupTaskOutputRecord, NewExportTaskOutputRecord, NewRemoteCallResult, NewTaskEventRecord,
    TaskKind, TaskResultCounts, TaskStatus,
};
use crate::storage::{
    StorageBackupTaskArtifact, StorageError, StorageExportTaskArtifact,
    StorageRemoteCallTaskArtifact, StorageTask, StorageTaskClaim, StorageTaskClaimToken,
    StorageTaskCompletion, StorageTaskCompletionArtifact, StorageTaskEventAppend,
    StorageTaskEventInput, StorageTaskFailure, StorageTaskLease, StorageTaskLeaseDuration,
    StorageTaskResultCounts, StorageTaskStateUpdate, StorageTaskStatus, TaskExecutionStorage,
};
use crate::tasks::TaskLeaseDuration;

use super::PostgresStorage;
use super::error::map_postgres_error;
use super::operations::computed_field::mark_computed_reindex_failed_conn;
use super::operations::task::{
    TaskBackend, TaskIdentifier, TaskStateUpdate, append_task_event_while_claimed,
    claim_next_queued_task, finalize_terminal_conn, live_claimed_task_conn,
    purge_expired_backup_outputs, purge_expired_export_outputs, record_task_terminal,
    recover_expired_task_leases, renew_task_lease,
};
use super::task_queue::task_to_storage;
use super::{
    PostgresPool, PostgresPoolSettings, init_postgres_pool_with_settings, with_transaction,
};

pub(super) struct ClaimedTaskIdentifier {
    task_id: i32,
    token: Uuid,
}

const TASK_LEASE_POOL_SIZE: u32 = 1;

#[cfg(not(test))]
static TASK_LEASE_POOL: OnceLock<PostgresPool> = OnceLock::new();

fn new_task_lease_pool() -> PostgresPool {
    let config = get_config().expect("task lease renewal requires database configuration");
    let settings = PostgresPoolSettings::builder(config.database_url.clone())
        .max_size(TASK_LEASE_POOL_SIZE)
        .statement_timeout_ms(config.db_statement_timeout_ms)
        .acquire_timeout_ms(config.db_pool_acquire_timeout_ms)
        .build()
        .expect("task lease pool settings must be valid");
    init_postgres_pool_with_settings(&settings)
}

#[cfg(not(test))]
fn task_lease_pool() -> PostgresPool {
    TASK_LEASE_POOL.get_or_init(new_task_lease_pool).clone()
}

// Test runtimes are short-lived, so do not retain async PostgreSQL connections
// after the runtime that established them has stopped.
#[cfg(test)]
fn task_lease_pool() -> PostgresPool {
    new_task_lease_pool()
}

impl TaskIdentifier for ClaimedTaskIdentifier {
    fn task_id(&self) -> i32 {
        self.task_id
    }

    fn task_lease_token(&self) -> Option<Uuid> {
        Some(self.token)
    }
}

fn lease_duration_from_storage(
    duration: StorageTaskLeaseDuration,
) -> Result<TaskLeaseDuration, ApiError> {
    let milliseconds = u64::try_from(duration.milliseconds()).map_err(|_| {
        ApiError::BadRequest("Task lease duration must be greater than zero".to_string())
    })?;
    TaskLeaseDuration::new(Duration::from_millis(milliseconds)).map_err(ApiError::BadRequest)
}

fn claim_token_from_storage(token: &StorageTaskClaimToken) -> Result<Uuid, ApiError> {
    Uuid::parse_str(token.adapter_value()).map_err(|_| {
        ApiError::BadRequest("Task claim token is not valid for this backend".to_string())
    })
}

pub(super) fn claimed_identifier(
    lease: &StorageTaskLease,
) -> Result<ClaimedTaskIdentifier, ApiError> {
    Ok(ClaimedTaskIdentifier {
        task_id: lease.task_id(),
        token: claim_token_from_storage(lease.token())?,
    })
}

fn task_status_from_storage(status: StorageTaskStatus) -> TaskStatus {
    match status {
        StorageTaskStatus::Queued => TaskStatus::Queued,
        StorageTaskStatus::Validating => TaskStatus::Validating,
        StorageTaskStatus::Running => TaskStatus::Running,
        StorageTaskStatus::Succeeded => TaskStatus::Succeeded,
        StorageTaskStatus::Failed => TaskStatus::Failed,
        StorageTaskStatus::PartiallySucceeded => TaskStatus::PartiallySucceeded,
        StorageTaskStatus::Cancelled => TaskStatus::Cancelled,
    }
}

fn state_update_from_storage(
    update: StorageTaskStateUpdate,
) -> Result<(ClaimedTaskIdentifier, TaskStateUpdate, TaskStatus), ApiError> {
    let (lease, status, summary, counts, started_at) = update.into_parts();
    let counts = task_result_counts_from_storage(counts)?;
    let status = task_status_from_storage(status);
    let mut update = TaskStateUpdate::new(status, counts).with_started_at(started_at);
    if let Some(summary) = summary {
        update = update.with_summary(summary);
    }
    Ok((claimed_identifier(&lease)?, update, status))
}

fn task_result_counts_from_storage(
    counts: StorageTaskResultCounts,
) -> Result<TaskResultCounts, ApiError> {
    TaskResultCounts::from_stored(counts.processed(), counts.succeeded(), counts.failed())
}

fn event_from_storage(task_id: i32, event: StorageTaskEventInput) -> NewTaskEventRecord {
    let (event_type, message, data) = event.into_parts();
    NewTaskEventRecord {
        task_id,
        event_type,
        message,
        data,
    }
}

fn export_artifact_from_storage(
    task_id: i32,
    artifact: StorageExportTaskArtifact,
) -> NewExportTaskOutputRecord {
    let (identity, content, report, output_expires_at, durations) = artifact.into_parts();
    let (template_name, content_type) = identity.into_parts();
    let (json_output, text_output) = content.into_parts();
    let (meta_json, warnings_json, warning_count, truncated) = report.into_parts();
    NewExportTaskOutputRecord {
        task_id,
        template_name,
        content_type,
        json_output,
        text_output,
        meta_json,
        warnings_json,
        warning_count,
        truncated,
        output_expires_at,
        total_duration_ms: durations.total_ms(),
        query_duration_ms: durations.query_ms(),
        hydration_duration_ms: durations.hydration_ms(),
        render_duration_ms: durations.render_ms(),
    }
}

fn backup_artifact_from_storage(
    task_id: i32,
    artifact: StorageBackupTaskArtifact,
) -> NewBackupTaskOutputRecord {
    let (document, byte_size, sha256, output_expires_at) = artifact.into_parts();
    NewBackupTaskOutputRecord {
        task_id,
        document,
        byte_size,
        sha256,
        output_expires_at,
    }
}

fn remote_call_artifact_from_storage(
    task_id: i32,
    artifact: StorageRemoteCallTaskArtifact,
) -> NewRemoteCallResult {
    let (target, response, outcome) = artifact.into_parts();
    let (target_id, subject_type, subject_id, method, rendered_url) = target.into_parts();
    let (response_status, response_headers, response_body_preview) = response.into_parts();
    let (duration_ms, success, error) = outcome.into_parts();
    NewRemoteCallResult {
        task_id,
        target_id,
        subject_type,
        subject_id,
        method,
        rendered_url,
        response_status,
        response_headers,
        response_body_preview,
        duration_ms,
        success,
        error,
    }
}

#[async_trait]
impl TaskExecutionStorage for PostgresStorage {
    async fn claim_next_task(
        &self,
        lease_duration: StorageTaskLeaseDuration,
    ) -> Result<Option<StorageTaskClaim>, StorageError> {
        let task = claim_next_queued_task(
            self.pool(),
            lease_duration_from_storage(lease_duration).map_err(map_postgres_error)?,
        )
        .await
        .map_err(map_postgres_error)?;
        task.map(|task| {
            let token = task.lease_token.ok_or_else(|| {
                ApiError::InternalServerError(
                    "Claimed task did not include a backend claim token".to_string(),
                )
            })?;
            let lease =
                StorageTaskLease::new(task.id, StorageTaskClaimToken::new(token.to_string()));
            Ok(StorageTaskClaim::new(task_to_storage(task)?, lease))
        })
        .transpose()
        .map_err(map_postgres_error)
    }

    async fn renew_task_lease(
        &self,
        lease: StorageTaskLease,
        lease_duration: StorageTaskLeaseDuration,
    ) -> Result<bool, StorageError> {
        let task_id = lease.task_id();
        let token = claim_token_from_storage(lease.token()).map_err(map_postgres_error)?;
        let duration = lease_duration_from_storage(lease_duration).map_err(map_postgres_error)?;
        renew_task_lease(&task_lease_pool(), task_id, token, duration)
            .await
            .map_err(map_postgres_error)
    }

    async fn recover_expired_task_leases(
        &self,
        batch_size: usize,
    ) -> Result<Vec<StorageTask>, StorageError> {
        let batch_size = i64::try_from(batch_size)
            .map_err(|_| StorageError::bad_request("Task recovery batch size is too large"))?;
        recover_expired_task_leases(self.pool(), batch_size)
            .await
            .map_err(map_postgres_error)?
            .into_iter()
            .map(task_to_storage)
            .collect::<Result<Vec<_>, _>>()
            .map_err(map_postgres_error)
    }

    async fn append_task_event(&self, event: StorageTaskEventAppend) -> Result<(), StorageError> {
        let (lease, event) = event.into_parts();
        let task_id = lease.task_id();
        let token = claim_token_from_storage(lease.token()).map_err(map_postgres_error)?;
        append_task_event_while_claimed(
            self.pool(),
            task_id,
            token,
            event_from_storage(task_id, event),
        )
        .await
        .map(|_| ())
        .map_err(map_postgres_error)
    }

    async fn update_task_state(
        &self,
        update: StorageTaskStateUpdate,
    ) -> Result<StorageTask, StorageError> {
        let (task, update, status) =
            state_update_from_storage(update).map_err(map_postgres_error)?;
        if !TaskStatus::ACTIVE.contains(&status) {
            return Err(map_postgres_error(ApiError::BadRequest(format!(
                "Task state updates require an active status, received '{}'",
                status.as_str()
            ))));
        }
        task.update_state(self.pool(), update)
            .await
            .and_then(task_to_storage)
            .map_err(map_postgres_error)
    }

    async fn complete_task(
        &self,
        completion: StorageTaskCompletion,
    ) -> Result<StorageTask, StorageError> {
        let (update, event, artifact) = completion.into_parts();
        let (task, update, status) =
            state_update_from_storage(update).map_err(map_postgres_error)?;
        if !status.is_terminal() {
            return Err(map_postgres_error(ApiError::BadRequest(format!(
                "Task completion requires a terminal status, received '{}'",
                status.as_str()
            ))));
        }
        let task_id = task.task_id;
        let stored_kind = task
            .find_record(self.pool())
            .await
            .and_then(|record| TaskKind::from_db(&record.kind))
            .map_err(map_postgres_error)?;
        let artifact_matches_kind = matches!(
            (&artifact, stored_kind),
            (
                StorageTaskCompletionArtifact::None,
                TaskKind::Import | TaskKind::Reindex
            ) | (StorageTaskCompletionArtifact::Export(_), TaskKind::Export)
                | (StorageTaskCompletionArtifact::Backup(_), TaskKind::Backup)
                | (
                    StorageTaskCompletionArtifact::RemoteCall(_),
                    TaskKind::RemoteCall
                )
        );
        if !artifact_matches_kind {
            return Err(map_postgres_error(ApiError::BadRequest(format!(
                "Task completion artifact does not match task kind '{}'",
                stored_kind.as_str()
            ))));
        }
        let event = event_from_storage(task_id, event);
        let result = match artifact {
            StorageTaskCompletionArtifact::None => {
                task.finalize_terminal(self.pool(), update, event).await
            }
            StorageTaskCompletionArtifact::Export(artifact) => {
                task.finalize_export_with_output(
                    self.pool(),
                    update,
                    event,
                    export_artifact_from_storage(task_id, artifact),
                )
                .await
            }
            StorageTaskCompletionArtifact::Backup(artifact) => {
                task.finalize_backup_with_output(
                    self.pool(),
                    update,
                    event,
                    backup_artifact_from_storage(task_id, artifact),
                )
                .await
            }
            StorageTaskCompletionArtifact::RemoteCall(artifact) => {
                task.finalize_remote_call_with_result(
                    self.pool(),
                    update,
                    event,
                    remote_call_artifact_from_storage(task_id, artifact),
                )
                .await
            }
        };
        result.and_then(task_to_storage).map_err(map_postgres_error)
    }

    async fn fail_task(&self, failure: StorageTaskFailure) -> Result<StorageTask, StorageError> {
        let (lease, summary, event) = failure.into_parts();
        let task = claimed_identifier(&lease).map_err(map_postgres_error)?;
        let record = task
            .find_record(self.pool())
            .await
            .map_err(map_postgres_error)?;
        let kind = TaskKind::from_db(&record.kind).map_err(map_postgres_error)?;
        let counts = match kind {
            TaskKind::Import => task
                .count_import_results(self.pool())
                .await
                .map_err(map_postgres_error)?,
            TaskKind::Export | TaskKind::Backup | TaskKind::RemoteCall => {
                TaskResultCounts::from_outcomes(0, 1).map_err(map_postgres_error)?
            }
            TaskKind::Reindex => {
                TaskResultCounts::from_stored(record.processed_items, record.success_items, 1)
                    .map_err(map_postgres_error)?
            }
        };
        let update = TaskStateUpdate::new(TaskStatus::Failed, counts)
            .with_summary(summary.clone())
            .with_started_at(record.started_at);
        let event = event_from_storage(record.id, event);
        let finalized = if kind == TaskKind::Reindex {
            let finalized = with_transaction(self.pool(), async |conn| {
                live_claimed_task_conn(conn, task.task_id, task.token).await?;
                mark_computed_reindex_failed_conn(conn, &record, &summary).await?;
                finalize_terminal_conn(conn, task.task_id, Some(task.token), update, event).await
            })
            .await
            .map_err(map_postgres_error)?;
            record_task_terminal(&finalized);
            crate::observability::metrics::computed_rebuild_finished(
                "failed",
                std::time::Duration::ZERO,
            );
            finalized
        } else {
            task.finalize_terminal(self.pool(), update, event)
                .await
                .map_err(map_postgres_error)?
        };
        task_to_storage(finalized).map_err(map_postgres_error)
    }

    async fn purge_expired_export_outputs(&self) -> Result<usize, StorageError> {
        purge_expired_export_outputs(self.pool())
            .await
            .map(|task_ids| task_ids.len())
            .map_err(map_postgres_error)
    }

    async fn purge_expired_backup_outputs(&self) -> Result<usize, StorageError> {
        purge_expired_backup_outputs(self.pool())
            .await
            .map(|task_ids| task_ids.len())
            .map_err(map_postgres_error)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

    #[tokio::test]
    async fn lease_pool_remains_available_when_an_execution_pool_is_exhausted() {
        let config = get_config().expect("test requires database configuration");
        let settings = PostgresPoolSettings::builder(config.database_url.clone())
            .max_size(1)
            .statement_timeout_ms(config.db_statement_timeout_ms)
            .acquire_timeout_ms(config.db_pool_acquire_timeout_ms)
            .build()
            .expect("single-connection execution pool settings should be valid");
        let execution_pool = init_postgres_pool_with_settings(&settings);
        let _execution_connection = execution_pool
            .get()
            .await
            .expect("execution connection should be available");

        let lease_pool = task_lease_pool();
        timeout(Duration::from_secs(5), lease_pool.get())
            .await
            .expect("lease checkout must not wait for the execution pool")
            .expect("lease pool should connect to the test database");
    }
}
