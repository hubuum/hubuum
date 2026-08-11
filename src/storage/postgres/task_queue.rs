use async_trait::async_trait;

use crate::errors::ApiError;
use crate::models::{
    BackupOutputLookup, BackupTaskOutputRecord, BackupTaskOutputSummaryRecord, ExportOutputLookup,
    ExportTaskOutputRecord, ExportTaskOutputSummaryRecord, ImportTaskResultRecord, PrincipalID,
    TaskEventRecord, TaskID, TaskKind, TaskRecord,
};
use crate::pagination::SKIPPED_TOTAL_COUNT;
use crate::storage::{
    StorageBackupOutput, StorageBackupOutputSummary, StorageError, StorageExportOutput,
    StorageExportOutputSummary, StorageImportTaskResult, StorageImportTaskResultPage, StorageTask,
    StorageTaskAccess, StorageTaskCreateRequest, StorageTaskDurations, StorageTaskEvent,
    StorageTaskEventPage, StorageTaskKind, StorageTaskListQuery, StorageTaskOutputLookup,
    StorageTaskPage, StorageTaskPageQuery, StorageTaskProgress, StorageTaskScopeSnapshot,
    StorageTaskStatus, TaskQueueStorage,
};

use super::PostgresStorage;
use super::error::map_postgres_error;
use super::operations::service_account::load_service_account_by_id;
use super::operations::task::{
    TaskBackend, TaskCreateRequest, TaskScopeSnapshot, enrich_legacy_task_event_initiators,
    list_backup_task_output_summaries, list_export_task_output_summaries,
    list_tasks_with_total_count,
};

#[async_trait]
impl TaskQueueStorage for PostgresStorage {
    async fn create_task(
        &self,
        request: StorageTaskCreateRequest,
    ) -> Result<StorageTask, StorageError> {
        let snapshot = request.scope_snapshot();
        let snapshot = TaskScopeSnapshot::from_persisted(
            snapshot.token_id(),
            snapshot.scoped(),
            snapshot.scopes().clone(),
        )
        .map_err(map_postgres_error)?;
        let legacy = TaskCreateRequest::builder(
            task_kind_from_storage(request.kind()),
            PrincipalID::new(request.submitted_by()).map_err(map_postgres_error)?,
            request.request_payload().clone(),
            request.total_items(),
        )
        .idempotency_key(request.idempotency_key().cloned())
        .request_hash(request.request_hash().map(str::to_string))
        .scope_snapshot(snapshot)
        .build();
        legacy
            .create_idempotently_with_active_limit(self.pool(), request.maximum_active_tasks())
            .await
            .and_then(task_to_storage)
            .map_err(map_postgres_error)
    }

    async fn get_task_access(&self, task_id: i32) -> Result<StorageTaskAccess, StorageError> {
        let task_id = TaskID::new(task_id).map_err(map_postgres_error)?;
        let task = task_id
            .find_record(self.pool())
            .await
            .map_err(map_postgres_error)?;
        let submitter_owner_group_id = match task.submitted_by {
            Some(submitter_id) => match load_service_account_by_id(self.pool(), submitter_id).await
            {
                Ok(account) => Some(account.owner_group_id),
                Err(ApiError::NotFound(_)) => None,
                Err(error) => return Err(map_postgres_error(error)),
            },
            None => None,
        };
        task_to_storage(task)
            .map(|task| StorageTaskAccess::new(task, submitter_owner_group_id))
            .map_err(map_postgres_error)
    }

    async fn list_tasks(
        &self,
        query: StorageTaskListQuery,
    ) -> Result<StorageTaskPage, StorageError> {
        let (submitted_by, kind, status, options) = query.into_parts();
        let (tasks, total) = list_tasks_with_total_count(
            self.pool(),
            submitted_by,
            kind.map(StorageTaskKind::as_str),
            status.map(StorageTaskStatus::as_str),
            &options,
        )
        .await
        .map_err(map_postgres_error)?;
        let tasks = tasks
            .into_iter()
            .map(task_to_storage)
            .collect::<Result<Vec<_>, _>>()
            .map_err(map_postgres_error)?;
        Ok(StorageTaskPage::new(
            tasks,
            (total != SKIPPED_TOTAL_COUNT).then_some(total),
        ))
    }

    async fn list_task_events(
        &self,
        query: StorageTaskPageQuery,
    ) -> Result<StorageTaskEventPage, StorageError> {
        let (task_id, options) = query.into_parts();
        let task_id = TaskID::new(task_id).map_err(map_postgres_error)?;
        let (events, total) = task_id
            .list_events_with_total_count(self.pool(), &options)
            .await
            .map_err(map_postgres_error)?;
        let events = enrich_legacy_task_event_initiators(self.pool(), events)
            .await
            .map_err(map_postgres_error)?;
        Ok(StorageTaskEventPage::new(
            events.into_iter().map(event_to_storage).collect(),
            (total != SKIPPED_TOTAL_COUNT).then_some(total),
        ))
    }

    async fn list_import_task_results(
        &self,
        query: StorageTaskPageQuery,
    ) -> Result<StorageImportTaskResultPage, StorageError> {
        let (task_id, options) = query.into_parts();
        let task_id = TaskID::new(task_id).map_err(map_postgres_error)?;
        let (results, total) = task_id
            .list_import_results_with_total_count(self.pool(), &options)
            .await
            .map_err(map_postgres_error)?;
        Ok(StorageImportTaskResultPage::new(
            results.into_iter().map(import_result_to_storage).collect(),
            (total != SKIPPED_TOTAL_COUNT).then_some(total),
        ))
    }

    async fn list_export_output_summaries(
        &self,
        task_ids: Vec<i32>,
    ) -> Result<Vec<StorageExportOutputSummary>, StorageError> {
        list_export_task_output_summaries(self.pool(), &task_ids)
            .await
            .map(|outputs| outputs.into_iter().map(export_summary_to_storage).collect())
            .map_err(map_postgres_error)
    }

    async fn list_backup_output_summaries(
        &self,
        task_ids: Vec<i32>,
    ) -> Result<Vec<StorageBackupOutputSummary>, StorageError> {
        list_backup_task_output_summaries(self.pool(), &task_ids)
            .await
            .map(|outputs| outputs.into_iter().map(backup_summary_to_storage).collect())
            .map_err(map_postgres_error)
    }

    async fn get_export_output_summary(
        &self,
        task_id: i32,
    ) -> Result<StorageTaskOutputLookup<StorageExportOutputSummary>, StorageError> {
        let task_id = TaskID::new(task_id).map_err(map_postgres_error)?;
        task_id
            .find_export_output_summary(self.pool())
            .await
            .map(|lookup| map_export_lookup(lookup, export_summary_to_storage))
            .map_err(map_postgres_error)
    }

    async fn get_backup_output_summary(
        &self,
        task_id: i32,
    ) -> Result<StorageTaskOutputLookup<StorageBackupOutputSummary>, StorageError> {
        let task_id = TaskID::new(task_id).map_err(map_postgres_error)?;
        task_id
            .find_backup_output_summary(self.pool())
            .await
            .map(|lookup| map_backup_lookup(lookup, backup_summary_to_storage))
            .map_err(map_postgres_error)
    }

    async fn get_export_output(
        &self,
        task_id: i32,
    ) -> Result<StorageTaskOutputLookup<StorageExportOutput>, StorageError> {
        let task_id = TaskID::new(task_id).map_err(map_postgres_error)?;
        task_id
            .find_export_output(self.pool())
            .await
            .map(|lookup| map_export_lookup(lookup, export_output_to_storage))
            .map_err(map_postgres_error)
    }

    async fn get_backup_output(
        &self,
        task_id: i32,
    ) -> Result<StorageTaskOutputLookup<StorageBackupOutput>, StorageError> {
        let task_id = TaskID::new(task_id).map_err(map_postgres_error)?;
        task_id
            .find_backup_output(self.pool())
            .await
            .map(|lookup| map_backup_lookup(lookup, backup_output_to_storage))
            .map_err(map_postgres_error)
    }
}

fn task_kind_from_storage(kind: StorageTaskKind) -> TaskKind {
    match kind {
        StorageTaskKind::Import => TaskKind::Import,
        StorageTaskKind::Export => TaskKind::Export,
        StorageTaskKind::Backup => TaskKind::Backup,
        StorageTaskKind::Reindex => TaskKind::Reindex,
        StorageTaskKind::RemoteCall => TaskKind::RemoteCall,
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

fn event_to_storage(event: TaskEventRecord) -> StorageTaskEvent {
    StorageTaskEvent::builder(
        event.id,
        event.task_id,
        event.event_type,
        event.message,
        event.created_at,
        event.actor_kind,
    )
    .data(event.data)
    .actor_principal_id(event.actor_user_id)
    .provenance(event.initiator_user_id, event.provenance_task_id)
    .build()
}

fn import_result_to_storage(result: ImportTaskResultRecord) -> StorageImportTaskResult {
    StorageImportTaskResult::builder(
        result.id,
        result.task_id,
        result.entity_kind,
        result.action,
        result.outcome,
        result.created_at,
    )
    .item_ref(result.item_ref)
    .identifier(result.identifier)
    .error(result.error)
    .details(result.details)
    .build()
}

fn export_summary_to_storage(output: ExportTaskOutputSummaryRecord) -> StorageExportOutputSummary {
    StorageExportOutputSummary::new(
        output.task_id,
        output.template_name,
        output.content_type,
        output.warning_count,
        output.truncated,
        output.output_expires_at,
        StorageTaskDurations::new(
            output.total_duration_ms,
            output.query_duration_ms,
            output.hydration_duration_ms,
            output.render_duration_ms,
        ),
    )
}

fn backup_summary_to_storage(output: BackupTaskOutputSummaryRecord) -> StorageBackupOutputSummary {
    StorageBackupOutputSummary::new(
        output.task_id,
        output.byte_size,
        output.sha256,
        output.output_expires_at,
    )
}

fn export_output_to_storage(output: ExportTaskOutputRecord) -> StorageExportOutput {
    StorageExportOutput::builder(
        output.task_id,
        output.content_type,
        output.meta_json,
        output.warnings_json,
        output.output_expires_at,
        output.created_at,
    )
    .template_name(output.template_name)
    .output(output.json_output, output.text_output)
    .warning_state(output.warning_count, output.truncated)
    .durations(StorageTaskDurations::new(
        output.total_duration_ms,
        output.query_duration_ms,
        output.hydration_duration_ms,
        output.render_duration_ms,
    ))
    .build()
}

fn backup_output_to_storage(output: BackupTaskOutputRecord) -> StorageBackupOutput {
    StorageBackupOutput::new(
        output.task_id,
        output.document,
        output.byte_size,
        output.sha256,
        output.output_expires_at,
        output.created_at,
    )
}

fn map_export_lookup<T, U>(
    lookup: ExportOutputLookup<T>,
    map: impl FnOnce(T) -> U,
) -> StorageTaskOutputLookup<U> {
    match lookup {
        ExportOutputLookup::Available(value) => StorageTaskOutputLookup::Available(map(value)),
        ExportOutputLookup::Expired { expires_at } => {
            StorageTaskOutputLookup::Expired { expires_at }
        }
        ExportOutputLookup::Missing => StorageTaskOutputLookup::Missing,
    }
}

fn map_backup_lookup<T, U>(
    lookup: BackupOutputLookup<T>,
    map: impl FnOnce(T) -> U,
) -> StorageTaskOutputLookup<U> {
    match lookup {
        BackupOutputLookup::Available(value) => StorageTaskOutputLookup::Available(map(value)),
        BackupOutputLookup::Expired { expires_at } => {
            StorageTaskOutputLookup::Expired { expires_at }
        }
        BackupOutputLookup::Missing => StorageTaskOutputLookup::Missing,
    }
}
