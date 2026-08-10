use hubuum_task_core::IdempotencyKey;

use crate::errors::ApiError;
use crate::models::search::QueryOptions;
use crate::models::{
    BackupOutputLookup, BackupTaskOutput, BackupTaskOutputSummary, ExportOutputLookup,
    ExportTaskOutput, ExportTaskOutputSummary, ImportTaskResultRecord, PrincipalID,
    TaskEventRecord, TaskID, TaskKind, TaskRecord, TaskStatus, TokenID, TokenScope,
};
use crate::pagination::SKIPPED_TOTAL_COUNT;
use crate::permissions::{
    PermissionDecision, PrincipalRef, ResourceAttrs, ResourceKind, ResourceRef,
};
use crate::storage::{
    AuthenticationStorage, StorageBackupOutput, StorageBackupOutputSummary, StorageContext,
    StorageExportOutput, StorageExportOutputSummary, StorageImportTaskResult, StorageTask,
    StorageTaskCreateRequest, StorageTaskEvent, StorageTaskKind, StorageTaskListQuery,
    StorageTaskOutputLookup, StorageTaskPageQuery, StorageTaskScopeSnapshot, StorageTaskStatus,
    TaskQueueStorage, storage_handle,
};
use crate::traits::AuthzSubject;

pub(crate) struct TaskSubmission {
    kind: TaskKind,
    submitted_by: PrincipalID,
    payload: serde_json::Value,
    total_items: i32,
    maximum_active_tasks: usize,
    idempotency_key: Option<IdempotencyKey>,
    request_hash: Option<String>,
    scope_snapshot: StorageTaskScopeSnapshot,
}

impl TaskSubmission {
    pub(crate) fn new(
        kind: TaskKind,
        submitted_by: PrincipalID,
        payload: serde_json::Value,
        total_items: i32,
        maximum_active_tasks: usize,
    ) -> Self {
        Self {
            kind,
            submitted_by,
            payload,
            total_items,
            maximum_active_tasks,
            idempotency_key: None,
            request_hash: None,
            scope_snapshot: StorageTaskScopeSnapshot::unscoped(),
        }
    }

    pub(crate) fn idempotency_key(mut self, idempotency_key: Option<IdempotencyKey>) -> Self {
        self.idempotency_key = idempotency_key;
        self
    }

    pub(crate) fn request_hash(mut self, request_hash: Option<String>) -> Self {
        self.request_hash = request_hash;
        self
    }

    pub(crate) fn scope_snapshot(mut self, scope_snapshot: StorageTaskScopeSnapshot) -> Self {
        self.scope_snapshot = scope_snapshot;
        self
    }
}

pub(crate) fn task_scope_snapshot(
    token_id: Option<TokenID>,
    scopes: Option<&TokenScope>,
) -> StorageTaskScopeSnapshot {
    StorageTaskScopeSnapshot::new(
        token_id.map(TokenID::id),
        scopes.is_some(),
        scopes
            .map(TokenScope::snapshot_json)
            .unwrap_or_else(|| serde_json::json!([])),
    )
}

pub(crate) async fn submit_task(
    backend: &impl StorageContext,
    submission: TaskSubmission,
) -> Result<TaskRecord, ApiError> {
    let request = StorageTaskCreateRequest::builder(
        task_kind_to_storage(submission.kind),
        submission.submitted_by.id(),
        submission.payload,
        submission.total_items,
    )
    .idempotency_key(submission.idempotency_key)
    .request_hash(submission.request_hash)
    .scope_snapshot(submission.scope_snapshot)
    .build(submission.maximum_active_tasks);
    storage_handle(backend)
        .create_task(request)
        .await
        .map_err(ApiError::from)
        .and_then(task_from_storage)
}

pub(crate) async fn find_task(
    backend: &impl StorageContext,
    task_id: TaskID,
) -> Result<TaskRecord, ApiError> {
    let (task, _) = storage_handle(backend)
        .get_task_access(task_id.id())
        .await?
        .into_parts();
    task_from_storage(task)
}

pub(crate) async fn load_authorized_task<S>(
    backend: &impl StorageContext,
    requestor: &S,
    task_id: TaskID,
) -> Result<TaskRecord, ApiError>
where
    S: AuthzSubject + ?Sized,
{
    load_authorized_task_of_kind(backend, requestor, task_id, None, "Task", false).await
}

pub(crate) async fn load_authorized_import<S>(
    backend: &impl StorageContext,
    requestor: &S,
    task_id: TaskID,
) -> Result<TaskRecord, ApiError>
where
    S: AuthzSubject + ?Sized,
{
    load_authorized_task_of_kind(
        backend,
        requestor,
        task_id,
        Some(TaskKind::Import),
        "Import task",
        true,
    )
    .await
}

pub(crate) async fn load_authorized_export<S>(
    backend: &impl StorageContext,
    requestor: &S,
    task_id: TaskID,
) -> Result<TaskRecord, ApiError>
where
    S: AuthzSubject + ?Sized,
{
    load_authorized_task_of_kind(
        backend,
        requestor,
        task_id,
        Some(TaskKind::Export),
        "Export task",
        true,
    )
    .await
}

pub(crate) async fn load_authorized_backup<S>(
    backend: &impl StorageContext,
    requestor: &S,
    task_id: TaskID,
) -> Result<TaskRecord, ApiError>
where
    S: AuthzSubject + ?Sized,
{
    load_authorized_task_of_kind(
        backend,
        requestor,
        task_id,
        Some(TaskKind::Backup),
        "Backup task",
        true,
    )
    .await
}

async fn load_authorized_task_of_kind<S>(
    backend: &impl StorageContext,
    requestor: &S,
    task_id: TaskID,
    required_kind: Option<TaskKind>,
    label: &str,
    hide_external_denial: bool,
) -> Result<TaskRecord, ApiError>
where
    S: AuthzSubject + ?Sized,
{
    let (stored, submitter_owner_group_id) = storage_handle(backend)
        .get_task_access(task_id.id())
        .await?
        .into_parts();
    let task = task_from_storage(stored)?;
    if required_kind.is_some_and(|kind| task.kind != kind.as_str()) {
        return Err(ApiError::NotFound(format!(
            "{label} {} not found",
            task_id.id()
        )));
    }

    let principal = PrincipalRef::load(backend, requestor).await?;
    let permissions = backend.permission_backend();
    let local = permissions.is_none_or(|backend| backend.uses_local_permission_store());
    let allowed = if local {
        let (identity, _) = storage_handle(backend)
            .load_authentication_identity(requestor.principal_id())
            .await?
            .into_parts();
        requestor.is_admin(backend).await?
            || task.submitted_by == Some(principal.user_id)
            || (identity.is_human()
                && submitter_owner_group_id
                    .is_some_and(|group_id| principal.group_ids.contains(&group_id)))
    } else {
        permissions
            .expect("external authorization path requires a permission backend")
            .authorize_task(&principal, &task_resource(&task))
            .await?
            == PermissionDecision::Allow
    };
    if allowed {
        Ok(task)
    } else if local || hide_external_denial {
        Err(ApiError::NotFound(format!("{label} not found")))
    } else {
        Err(ApiError::Forbidden("Permission denied".to_string()))
    }
}

pub(crate) async fn list_tasks(
    backend: &impl StorageContext,
    submitted_by: Option<i32>,
    kind: Option<TaskKind>,
    status: Option<TaskStatus>,
    options: QueryOptions,
) -> Result<(Vec<TaskRecord>, i64), ApiError> {
    let (tasks, total) = storage_handle(backend)
        .list_tasks(StorageTaskListQuery::new(
            submitted_by,
            kind.map(task_kind_to_storage),
            status.map(task_status_to_storage),
            options,
        ))
        .await?
        .into_parts();
    Ok((
        tasks
            .into_iter()
            .map(task_from_storage)
            .collect::<Result<_, _>>()?,
        total.unwrap_or(SKIPPED_TOTAL_COUNT),
    ))
}

pub(crate) async fn list_task_events(
    backend: &impl StorageContext,
    task_id: TaskID,
    options: QueryOptions,
) -> Result<(Vec<crate::models::TaskEventResponse>, i64), ApiError> {
    let (events, total) = storage_handle(backend)
        .list_task_events(StorageTaskPageQuery::new(task_id.id(), options))
        .await?
        .into_parts();
    let records = events
        .into_iter()
        .map(task_event_from_storage)
        .collect::<Vec<_>>();
    let principal_ids = records
        .iter()
        .flat_map(|record| [record.actor_user_id, record.initiator_user_id])
        .flatten()
        .collect();
    let principal_names =
        crate::services::history::resolve_principal_names(backend, principal_ids).await?;
    Ok((
        records
            .into_iter()
            .map(|record| {
                crate::models::TaskEventResponse::from_record_with_names(record, &principal_names)
            })
            .collect(),
        total.unwrap_or(SKIPPED_TOTAL_COUNT),
    ))
}

pub(crate) async fn list_import_results(
    backend: &impl StorageContext,
    task_id: TaskID,
    options: QueryOptions,
) -> Result<(Vec<ImportTaskResultRecord>, i64), ApiError> {
    let (results, total) = storage_handle(backend)
        .list_import_task_results(StorageTaskPageQuery::new(task_id.id(), options))
        .await?
        .into_parts();
    Ok((
        results
            .into_iter()
            .map(import_result_from_storage)
            .collect(),
        total.unwrap_or(SKIPPED_TOTAL_COUNT),
    ))
}

pub(crate) async fn list_export_output_summaries(
    backend: &impl StorageContext,
    task_ids: Vec<i32>,
) -> Result<Vec<ExportTaskOutputSummary>, ApiError> {
    Ok(storage_handle(backend)
        .list_export_output_summaries(task_ids)
        .await?
        .into_iter()
        .map(export_summary_from_storage)
        .collect())
}

pub(crate) async fn list_backup_output_summaries(
    backend: &impl StorageContext,
    task_ids: Vec<i32>,
) -> Result<Vec<BackupTaskOutputSummary>, ApiError> {
    Ok(storage_handle(backend)
        .list_backup_output_summaries(task_ids)
        .await?
        .into_iter()
        .map(backup_summary_from_storage)
        .collect())
}

pub(crate) async fn export_output_summary(
    backend: &impl StorageContext,
    task_id: TaskID,
) -> Result<ExportOutputLookup<ExportTaskOutputSummary>, ApiError> {
    Ok(map_export_lookup(
        storage_handle(backend)
            .get_export_output_summary(task_id.id())
            .await?,
        export_summary_from_storage,
    ))
}

pub(crate) async fn backup_output_summary(
    backend: &impl StorageContext,
    task_id: TaskID,
) -> Result<BackupOutputLookup<BackupTaskOutputSummary>, ApiError> {
    Ok(map_backup_lookup(
        storage_handle(backend)
            .get_backup_output_summary(task_id.id())
            .await?,
        backup_summary_from_storage,
    ))
}

pub(crate) async fn export_output(
    backend: &impl StorageContext,
    task_id: TaskID,
) -> Result<ExportOutputLookup<ExportTaskOutput>, ApiError> {
    Ok(map_export_lookup(
        storage_handle(backend)
            .get_export_output(task_id.id())
            .await?,
        export_output_from_storage,
    ))
}

pub(crate) async fn backup_output(
    backend: &impl StorageContext,
    task_id: TaskID,
) -> Result<BackupOutputLookup<BackupTaskOutput>, ApiError> {
    Ok(map_backup_lookup(
        storage_handle(backend)
            .get_backup_output(task_id.id())
            .await?,
        backup_output_from_storage,
    ))
}

pub(crate) fn task_resource(task: &TaskRecord) -> ResourceRef {
    ResourceRef {
        kind: ResourceKind::Task,
        id: task.id,
        attrs: ResourceAttrs {
            submitted_by: task.submitted_by,
            ..Default::default()
        },
    }
}

fn task_kind_to_storage(kind: TaskKind) -> StorageTaskKind {
    match kind {
        TaskKind::Import => StorageTaskKind::Import,
        TaskKind::Export => StorageTaskKind::Export,
        TaskKind::Backup => StorageTaskKind::Backup,
        TaskKind::Reindex => StorageTaskKind::Reindex,
        TaskKind::RemoteCall => StorageTaskKind::RemoteCall,
    }
}

fn task_status_to_storage(status: TaskStatus) -> StorageTaskStatus {
    match status {
        TaskStatus::Queued => StorageTaskStatus::Queued,
        TaskStatus::Validating => StorageTaskStatus::Validating,
        TaskStatus::Running => StorageTaskStatus::Running,
        TaskStatus::Succeeded => StorageTaskStatus::Succeeded,
        TaskStatus::Failed => StorageTaskStatus::Failed,
        TaskStatus::PartiallySucceeded => StorageTaskStatus::PartiallySucceeded,
        TaskStatus::Cancelled => StorageTaskStatus::Cancelled,
    }
}

fn task_from_storage(task: StorageTask) -> Result<TaskRecord, ApiError> {
    let kind = TaskKind::from_db(task.kind().as_str())?;
    let status = TaskStatus::from_db(task.status().as_str())?;
    let scope = task.scope_snapshot();
    let progress = task.progress();
    Ok(TaskRecord {
        id: task.id(),
        kind: kind.as_str().to_string(),
        status: status.as_str().to_string(),
        submitted_by: task.submitted_by(),
        idempotency_key: task.idempotency_key().map(str::to_string),
        request_hash: task.request_hash().map(str::to_string),
        request_payload: task.request_payload().cloned(),
        summary: task.summary().map(str::to_string),
        total_items: progress.total(),
        processed_items: progress.processed(),
        success_items: progress.succeeded(),
        failed_items: progress.failed(),
        submitted_token_id: scope.token_id(),
        submitted_token_scoped: scope.scoped(),
        submitted_token_scopes: scope.scopes().clone(),
        request_redacted_at: task.request_redacted_at(),
        started_at: task.started_at(),
        finished_at: task.finished_at(),
        deleted_at: task.deleted_at(),
        deleted_by: task.deleted_by(),
        created_at: task.created_at(),
        updated_at: task.updated_at(),
        lease_token: task.lease_token(),
        lease_expires_at: task.lease_expires_at(),
        attempt_count: task.attempt_count(),
        initiator_user_id: task.initiator_principal_id(),
    })
}

fn task_event_from_storage(event: StorageTaskEvent) -> TaskEventRecord {
    TaskEventRecord {
        id: event.id(),
        task_id: event.task_id(),
        event_type: event.event_type().to_string(),
        message: event.message().to_string(),
        data: event.data().cloned(),
        created_at: event.created_at(),
        actor_user_id: event.actor_principal_id(),
        actor_kind: event.actor_kind().to_string(),
        initiator_user_id: event.initiator_principal_id(),
        provenance_task_id: event.provenance_task_id(),
    }
}

fn import_result_from_storage(result: StorageImportTaskResult) -> ImportTaskResultRecord {
    ImportTaskResultRecord {
        id: result.id(),
        task_id: result.task_id(),
        item_ref: result.item_ref().map(str::to_string),
        entity_kind: result.entity_kind().to_string(),
        action: result.action().to_string(),
        identifier: result.identifier().map(str::to_string),
        outcome: result.outcome().to_string(),
        error: result.error().map(str::to_string),
        details: result.details().cloned(),
        created_at: result.created_at(),
    }
}

fn export_summary_from_storage(output: StorageExportOutputSummary) -> ExportTaskOutputSummary {
    let durations = output.durations();
    ExportTaskOutputSummary {
        task_id: output.task_id(),
        template_name: output.template_name().map(str::to_string),
        content_type: output.content_type().to_string(),
        warning_count: output.warning_count(),
        truncated: output.truncated(),
        output_expires_at: output.output_expires_at(),
        total_duration_ms: durations.total_ms(),
        query_duration_ms: durations.query_ms(),
        hydration_duration_ms: durations.hydration_ms(),
        render_duration_ms: durations.render_ms(),
    }
}

fn backup_summary_from_storage(output: StorageBackupOutputSummary) -> BackupTaskOutputSummary {
    BackupTaskOutputSummary {
        task_id: output.task_id(),
        byte_size: output.byte_size(),
        sha256: output.sha256().to_string(),
        output_expires_at: output.output_expires_at(),
    }
}

fn export_output_from_storage(output: StorageExportOutput) -> ExportTaskOutput {
    ExportTaskOutput {
        content_type: output.content_type().to_string(),
        json_output: output.json_output().cloned(),
        text_output: output.text_output().map(str::to_string),
        meta_json: output.metadata().clone(),
        warnings_json: output.warnings().clone(),
        truncated: output.truncated(),
    }
}

fn backup_output_from_storage(output: StorageBackupOutput) -> BackupTaskOutput {
    BackupTaskOutput {
        document: output.document().to_vec(),
        sha256: output.sha256().to_string(),
    }
}

fn map_export_lookup<T, U>(
    lookup: StorageTaskOutputLookup<T>,
    map: impl FnOnce(T) -> U,
) -> ExportOutputLookup<U> {
    match lookup {
        StorageTaskOutputLookup::Available(value) => ExportOutputLookup::Available(map(value)),
        StorageTaskOutputLookup::Expired { expires_at } => {
            ExportOutputLookup::Expired { expires_at }
        }
        StorageTaskOutputLookup::Missing => ExportOutputLookup::Missing,
    }
}

fn map_backup_lookup<T, U>(
    lookup: StorageTaskOutputLookup<T>,
    map: impl FnOnce(T) -> U,
) -> BackupOutputLookup<U> {
    match lookup {
        StorageTaskOutputLookup::Available(value) => BackupOutputLookup::Available(map(value)),
        StorageTaskOutputLookup::Expired { expires_at } => {
            BackupOutputLookup::Expired { expires_at }
        }
        StorageTaskOutputLookup::Missing => BackupOutputLookup::Missing,
    }
}
