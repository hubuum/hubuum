//! PostgreSQL implementation of the durable task-queue contract.
//!
//! This module owns every Diesel projection used by [`TaskQueueStorage`]. The
//! application receives only storage-core DTOs and never needs to translate a
//! PostgreSQL row or classify output-retention state.

use std::collections::HashMap;

use chrono::{NaiveDateTime, Utc};
use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel::sql_types::{BigInt, Bool};
use diesel::{Insertable, Queryable, QueryableByName, Selectable, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_domain::{GroupId, ImportTaskResultId, PrincipalId, TaskId};
use hubuum_events_core::{
    Action, ActorKind, AuditDocument, EntityType, EventSequence, MutationProvenance, NewEvent,
};
use hubuum_query::{CursorValue, FilterField, QueryOptions};
use hubuum_storage_core::{
    StorageBackupOutput, StorageBackupOutputSummary, StorageExportOutput,
    StorageExportOutputSummary, StorageImportTaskResult, StoragePage, StorageTask,
    StorageTaskAccess, StorageTaskChildListQuery, StorageTaskCreateRequest, StorageTaskDurations,
    StorageTaskEvent, StorageTaskKind, StorageTaskListQuery, StorageTaskOutputLookup,
    StorageTaskStatus,
};
use serde_json::{Value, json};

use crate::cursor::{CursorSqlField, CursorSqlType};
use crate::operations::event_record::append_event;
use crate::worker_notifications::notify_task_queue;
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

use super::task_rows::TaskRow;

const DEFAULT_PAGE_WITH_LOOKAHEAD: usize = 101;

#[derive(Insertable)]
#[diesel(table_name = crate::schema::tasks)]
struct NewTaskRow {
    kind: String,
    status: String,
    submitted_by: Option<i32>,
    idempotency_key: Option<String>,
    request_hash: Option<String>,
    request_payload: Option<Value>,
    summary: Option<String>,
    total_items: i32,
    processed_items: i32,
    success_items: i32,
    failed_items: i32,
    submitted_token_id: Option<i32>,
    submitted_token_scoped: bool,
    submitted_token_scopes: Value,
    request_redacted_at: Option<NaiveDateTime>,
    started_at: Option<NaiveDateTime>,
    finished_at: Option<NaiveDateTime>,
    initiator_user_id: Option<i32>,
}

impl NewTaskRow {
    fn from_request(request: &StorageTaskCreateRequest) -> Result<Self, PostgresStorageError> {
        let scope = request.scope_snapshot();
        Ok(Self {
            kind: request.kind().as_str().to_string(),
            status: StorageTaskStatus::Queued.as_str().to_string(),
            submitted_by: Some(request.submitted_by().id()),
            idempotency_key: request
                .idempotency_key()
                .map(|key| key.as_str().to_string()),
            request_hash: request.request_hash().map(str::to_string),
            request_payload: Some(request.request_payload().clone()),
            summary: None,
            total_items: request.total_items(),
            processed_items: 0,
            success_items: 0,
            failed_items: 0,
            submitted_token_id: scope.token_id().map(|id| id.id()),
            submitted_token_scoped: scope.scoped(),
            submitted_token_scopes: scope.scopes().clone(),
            request_redacted_at: None,
            started_at: None,
            finished_at: None,
            initiator_user_id: Some(request.submitted_by().id()),
        })
    }
}

#[derive(Queryable)]
struct TaskEventRow {
    id: i64,
    entity_id: Option<i32>,
    action: String,
    summary: String,
    metadata: Value,
    occurred_at: NaiveDateTime,
    actor_user_id: Option<i32>,
    actor_kind: String,
    initiator_user_id: Option<i32>,
    provenance_task_id: Option<i32>,
}

impl TaskEventRow {
    fn into_storage(self) -> Result<StorageTaskEvent, PostgresStorageError> {
        let task_id = self.entity_id.ok_or_else(|| {
            PostgresStorageError::database("Stored task event is missing its task id")
        })?;
        let data = match self.metadata.get("data") {
            Some(Value::Null) | None => None,
            Some(data) => Some(data.clone()),
        };
        Ok(StorageTaskEvent::builder(
            EventSequence::new(self.id)?,
            TaskId::new(task_id)?,
            self.action,
            self.summary,
            self.occurred_at.and_utc(),
            self.actor_kind,
        )
        .data(data)
        .actor_principal_id(self.actor_user_id.map(PrincipalId::new).transpose()?)
        .provenance(
            self.initiator_user_id.map(PrincipalId::new).transpose()?,
            self.provenance_task_id
                .or(Some(task_id))
                .map(TaskId::new)
                .transpose()?,
        )
        .build())
    }
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::import_task_results)]
struct ImportTaskResultRow {
    id: i32,
    task_id: i32,
    item_ref: Option<String>,
    entity_kind: String,
    action: String,
    identifier: Option<String>,
    outcome: String,
    error: Option<String>,
    details: Option<Value>,
    created_at: NaiveDateTime,
}

impl ImportTaskResultRow {
    fn into_storage(self) -> Result<StorageImportTaskResult, PostgresStorageError> {
        Ok(StorageImportTaskResult::builder(
            ImportTaskResultId::new(self.id)?,
            TaskId::new(self.task_id)?,
            self.entity_kind,
            self.action,
            self.outcome,
            self.created_at.and_utc(),
        )
        .item_ref(self.item_ref)
        .identifier(self.identifier)
        .error(self.error)
        .details(self.details)
        .build())
    }
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::export_task_outputs)]
struct ExportOutputRow {
    task_id: i32,
    template_name: Option<String>,
    content_type: String,
    json_output: Option<Value>,
    text_output: Option<String>,
    meta_json: Value,
    warnings_json: Value,
    warning_count: i32,
    truncated: bool,
    output_expires_at: NaiveDateTime,
    total_duration_ms: i32,
    query_duration_ms: i32,
    hydration_duration_ms: i32,
    render_duration_ms: i32,
    created_at: NaiveDateTime,
}

impl ExportOutputRow {
    fn into_storage(self) -> Result<StorageExportOutput, PostgresStorageError> {
        let durations = crate::validate_persisted(
            "export output durations",
            StorageTaskDurations::try_new(
                self.total_duration_ms,
                self.query_duration_ms,
                self.hydration_duration_ms,
                self.render_duration_ms,
            ),
        )?;
        crate::validate_persisted(
            "export output",
            StorageExportOutput::builder(
                TaskId::new(self.task_id)?,
                self.content_type,
                self.meta_json,
                self.warnings_json,
                self.output_expires_at.and_utc(),
                self.created_at.and_utc(),
            )
            .template_name(self.template_name)
            .output(self.json_output, self.text_output)
            .warning_state(self.warning_count, self.truncated)
            .durations(durations)
            .try_build(),
        )
    }
}

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::export_task_outputs)]
struct ExportOutputSummaryRow {
    task_id: i32,
    template_name: Option<String>,
    content_type: String,
    warning_count: i32,
    truncated: bool,
    output_expires_at: NaiveDateTime,
    total_duration_ms: i32,
    query_duration_ms: i32,
    hydration_duration_ms: i32,
    render_duration_ms: i32,
}

impl ExportOutputSummaryRow {
    fn into_storage(self) -> Result<StorageExportOutputSummary, PostgresStorageError> {
        let durations = crate::validate_persisted(
            "export output summary durations",
            StorageTaskDurations::try_new(
                self.total_duration_ms,
                self.query_duration_ms,
                self.hydration_duration_ms,
                self.render_duration_ms,
            ),
        )?;
        crate::validate_persisted(
            "export output summary",
            StorageExportOutputSummary::try_new(
                TaskId::new(self.task_id)?,
                self.template_name,
                self.content_type,
                self.warning_count,
                self.truncated,
                self.output_expires_at.and_utc(),
                durations,
            ),
        )
    }
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::backup_task_outputs)]
struct BackupOutputRow {
    task_id: i32,
    document: Vec<u8>,
    byte_size: i64,
    sha256: String,
    output_expires_at: NaiveDateTime,
    created_at: NaiveDateTime,
}

impl BackupOutputRow {
    fn into_storage(self) -> Result<StorageBackupOutput, PostgresStorageError> {
        crate::validate_persisted(
            "backup output",
            StorageBackupOutput::try_new(
                TaskId::new(self.task_id)?,
                self.document,
                self.byte_size,
                self.sha256,
                self.output_expires_at.and_utc(),
                self.created_at.and_utc(),
            ),
        )
    }
}

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::backup_task_outputs)]
struct BackupOutputSummaryRow {
    task_id: i32,
    byte_size: i64,
    sha256: String,
    output_expires_at: NaiveDateTime,
}

impl BackupOutputSummaryRow {
    fn into_storage(self) -> Result<StorageBackupOutputSummary, PostgresStorageError> {
        crate::validate_persisted(
            "backup output summary",
            StorageBackupOutputSummary::try_new(
                TaskId::new(self.task_id)?,
                self.byte_size,
                self.sha256,
                self.output_expires_at.and_utc(),
            ),
        )
    }
}

#[derive(QueryableByName)]
struct AdvisoryLockRow {
    #[diesel(sql_type = Bool)]
    locked: bool,
}

pub async fn create_task(
    runtime: &PostgresRuntime,
    request: StorageTaskCreateRequest,
) -> Result<StorageTask, PostgresStorageError> {
    let new_task = NewTaskRow::from_request(&request)?;
    let submitted_by = request.submitted_by().id();
    let kind = request.kind();
    let idempotency_key = new_task.idempotency_key.clone();
    let request_hash = new_task.request_hash.clone();

    if let Some(key) = idempotency_key.as_deref()
        && let Some(existing) = find_task_by_idempotency(runtime, submitted_by, key).await?
    {
        return matching_idempotent_task(existing, kind, &request_hash, key);
    }

    let maximum_active_tasks = i64::try_from(request.maximum_active_tasks()).unwrap_or(i64::MAX);
    let create_result = runtime
        .with_transaction(async move |connection| {
            acquire_capacity_lock(connection, submitted_by, kind).await?;
            let active_count = count_active_tasks(connection, submitted_by, kind).await?;
            if active_count >= maximum_active_tasks {
                return Err(PostgresStorageError::rate_limited(format!(
                    "Too many active {} tasks for user ({active_count} >= {maximum_active_tasks}); wait for queued or running tasks to finish",
                    kind.as_str()
                )));
            }
            insert_queued_task(connection, new_task).await
        })
        .await;

    match create_result {
        Ok(task) => {
            log_task_queued(&task);
            task.into_storage()
        }
        Err(error) if error.kind() == hubuum_storage_core::StorageErrorKind::Conflict => {
            if let Some(key) = idempotency_key.as_deref()
                && let Some(existing) = find_task_by_idempotency(runtime, submitted_by, key).await?
                && existing.kind == kind.as_str()
                && existing.request_hash == request_hash
            {
                return existing.into_storage();
            }
            Err(PostgresStorageError::conflict(
                "Idempotency-Key is already in use for a different task submission",
            ))
        }
        Err(error) => Err(error),
    }
}

pub async fn get_task_access(
    runtime: &PostgresRuntime,
    task_id: i32,
) -> Result<StorageTaskAccess, PostgresStorageError> {
    validate_positive_task_id(task_id)?;
    let (task, owner_group_id) = runtime
        .with_connection(async move |connection| {
            use crate::schema::service_accounts::dsl as accounts;
            use crate::schema::tasks::dsl as stored;

            let task = stored::tasks
                .filter(stored::id.eq(task_id))
                .select(TaskRow::as_select())
                .first::<TaskRow>(connection)
                .await?;
            let owner_group_id = match task.submitted_by {
                Some(submitter_id) => accounts::service_accounts
                    .filter(accounts::id.eq(submitter_id))
                    .select(accounts::owner_group_id)
                    .first::<i32>(connection)
                    .await
                    .optional()?,
                None => None,
            };
            Ok::<_, PostgresStorageError>((task, owner_group_id))
        })
        .await?;
    Ok(StorageTaskAccess::new(
        task.into_storage()?,
        owner_group_id.map(GroupId::new).transpose()?,
    ))
}

pub async fn list_tasks(
    runtime: &PostgresRuntime,
    query: StorageTaskListQuery,
) -> Result<StoragePage<StorageTask>, PostgresStorageError> {
    let (submitted_by, kind, status, options) = query.into_parts();
    let submitted_by = submitted_by.map(PrincipalId::id);
    reject_query_filters(&options, "tasks")?;
    let options = crate::cursor::normalize_query_options(options, FilterField::Id, false)?;
    if options.include_total() {
        runtime
            .with_read_only_snapshot(async |connection| {
                list_tasks_on(connection, submitted_by, kind, status, &options).await
            })
            .await
    } else {
        runtime
            .with_connection(async |connection| {
                list_tasks_on(connection, submitted_by, kind, status, &options).await
            })
            .await
    }
}

pub async fn list_task_events(
    runtime: &PostgresRuntime,
    query: StorageTaskChildListQuery,
) -> Result<StoragePage<StorageTaskEvent>, PostgresStorageError> {
    let (task_id, options) = query.into_parts();
    let task_id = task_id.id();
    let options = normalize_task_child_page_options(options, "task events")?;
    if options.include_total() {
        runtime
            .with_read_only_snapshot(async |connection| {
                list_task_events_on(connection, task_id, &options).await
            })
            .await
    } else {
        runtime
            .with_connection(async |connection| {
                list_task_events_on(connection, task_id, &options).await
            })
            .await
    }
}

pub async fn list_import_task_results(
    runtime: &PostgresRuntime,
    query: StorageTaskChildListQuery,
) -> Result<StoragePage<StorageImportTaskResult>, PostgresStorageError> {
    let (task_id, options) = query.into_parts();
    let task_id = task_id.id();
    let options = normalize_task_child_page_options(options, "import task results")?;
    if options.include_total() {
        runtime
            .with_read_only_snapshot(async |connection| {
                list_import_task_results_on(connection, task_id, &options).await
            })
            .await
    } else {
        runtime
            .with_connection(async |connection| {
                list_import_task_results_on(connection, task_id, &options).await
            })
            .await
    }
}

pub async fn list_export_output_summaries(
    runtime: &PostgresRuntime,
    task_ids: Vec<i32>,
) -> Result<Vec<StorageExportOutputSummary>, PostgresStorageError> {
    if task_ids.is_empty() {
        return Ok(Vec::new());
    }
    runtime
        .with_connection(async move |connection| {
            use crate::schema::export_task_outputs::dsl as outputs;
            outputs::export_task_outputs
                .filter(outputs::task_id.eq_any(task_ids))
                .select(ExportOutputSummaryRow::as_select())
                .load::<ExportOutputSummaryRow>(connection)
                .await
        })
        .await
        .and_then(|rows| {
            rows.into_iter()
                .map(ExportOutputSummaryRow::into_storage)
                .collect()
        })
}

pub async fn list_backup_output_summaries(
    runtime: &PostgresRuntime,
    task_ids: Vec<i32>,
) -> Result<Vec<StorageBackupOutputSummary>, PostgresStorageError> {
    if task_ids.is_empty() {
        return Ok(Vec::new());
    }
    runtime
        .with_connection(async move |connection| {
            use crate::schema::backup_task_outputs::dsl as outputs;
            outputs::backup_task_outputs
                .filter(outputs::task_id.eq_any(task_ids))
                .select(BackupOutputSummaryRow::as_select())
                .load::<BackupOutputSummaryRow>(connection)
                .await
        })
        .await
        .and_then(|rows| {
            rows.into_iter()
                .map(BackupOutputSummaryRow::into_storage)
                .collect()
        })
}

pub async fn get_export_output_summary(
    runtime: &PostgresRuntime,
    task_id: i32,
) -> Result<StorageTaskOutputLookup<StorageExportOutputSummary>, PostgresStorageError> {
    validate_positive_task_id(task_id)?;
    let row = runtime
        .with_connection(async move |connection| {
            use crate::schema::export_task_outputs::dsl as outputs;
            outputs::export_task_outputs
                .filter(outputs::task_id.eq(task_id))
                .select(ExportOutputSummaryRow::as_select())
                .first::<ExportOutputSummaryRow>(connection)
                .await
                .optional()
        })
        .await?;
    classify_output(row, |row| row.output_expires_at, |row| row.into_storage())
}

pub async fn get_backup_output_summary(
    runtime: &PostgresRuntime,
    task_id: i32,
) -> Result<StorageTaskOutputLookup<StorageBackupOutputSummary>, PostgresStorageError> {
    validate_positive_task_id(task_id)?;
    let row = runtime
        .with_connection(async move |connection| {
            use crate::schema::backup_task_outputs::dsl as outputs;
            outputs::backup_task_outputs
                .filter(outputs::task_id.eq(task_id))
                .select(BackupOutputSummaryRow::as_select())
                .first::<BackupOutputSummaryRow>(connection)
                .await
                .optional()
        })
        .await?;
    classify_output(row, |row| row.output_expires_at, |row| row.into_storage())
}

pub async fn get_export_output(
    runtime: &PostgresRuntime,
    task_id: i32,
) -> Result<StorageTaskOutputLookup<StorageExportOutput>, PostgresStorageError> {
    validate_positive_task_id(task_id)?;
    let row = runtime
        .with_connection(async move |connection| {
            use crate::schema::export_task_outputs::dsl as outputs;
            outputs::export_task_outputs
                .filter(outputs::task_id.eq(task_id))
                .select(ExportOutputRow::as_select())
                .first::<ExportOutputRow>(connection)
                .await
                .optional()
        })
        .await?;
    classify_output(row, |row| row.output_expires_at, |row| row.into_storage())
}

pub async fn get_backup_output(
    runtime: &PostgresRuntime,
    task_id: i32,
) -> Result<StorageTaskOutputLookup<StorageBackupOutput>, PostgresStorageError> {
    validate_positive_task_id(task_id)?;
    let row = runtime
        .with_connection(async move |connection| {
            use crate::schema::backup_task_outputs::dsl as outputs;
            outputs::backup_task_outputs
                .filter(outputs::task_id.eq(task_id))
                .select(BackupOutputRow::as_select())
                .first::<BackupOutputRow>(connection)
                .await
                .optional()
        })
        .await?;
    classify_output(row, |row| row.output_expires_at, |row| row.into_storage())
}

fn build_task_query(
    submitted_by: Option<i32>,
    kind: Option<StorageTaskKind>,
    status: Option<StorageTaskStatus>,
) -> crate::schema::tasks::BoxedQuery<'static, diesel::pg::Pg> {
    use crate::schema::tasks::dsl as stored;
    let mut query = stored::tasks.into_boxed();
    if let Some(submitted_by) = submitted_by {
        query = query.filter(stored::submitted_by.eq(Some(submitted_by)));
    }
    if let Some(kind) = kind {
        query = query.filter(stored::kind.eq(kind.as_str()));
    }
    if let Some(status) = status {
        query = query.filter(stored::status.eq(status.as_str()));
    }
    query
}

async fn list_tasks_on(
    connection: &mut PostgresConnection,
    submitted_by: Option<i32>,
    kind: Option<StorageTaskKind>,
    status: Option<StorageTaskStatus>,
    options: &QueryOptions,
) -> Result<StoragePage<StorageTask>, PostgresStorageError> {
    let total = if options.include_total() {
        let total = build_task_query(submitted_by, kind, status)
            .count()
            .get_result::<i64>(connection)
            .await?;
        crate::reach_fault_point(crate::PostgresFaultPoint::PageAfterCount, Some(connection))
            .await?;
        Some(total)
    } else {
        None
    };
    let mut storage_query = build_task_query(submitted_by, kind, status);
    let sql_fields = task_cursor_fields(options)?;
    crate::apply_query_options_with_fields!(
        storage_query,
        options,
        sql_fields,
        crate::cursor::CursorTieBreaker::new(
            FilterField::Id,
            false,
            task_cursor_field(&FilterField::Id)?,
        )
    );
    let tasks = storage_query
        .load::<TaskRow>(connection)
        .await?
        .into_iter()
        .map(TaskRow::into_storage)
        .collect::<Result<Vec<_>, _>>()?;
    crate::persisted_page(tasks, total)
}

fn task_cursor_fields(options: &QueryOptions) -> Result<Vec<CursorSqlField>, PostgresStorageError> {
    options
        .sort()
        .iter()
        .map(|sort| task_cursor_field(&sort.field))
        .collect()
}

fn task_cursor_field(field: &FilterField) -> Result<CursorSqlField, PostgresStorageError> {
    Ok(match field {
        FilterField::Id => CursorSqlField {
            column: "tasks.id",
            sql_type: CursorSqlType::Integer,
            nullable: false,
        },
        FilterField::Kind => CursorSqlField {
            column: "tasks.kind",
            sql_type: CursorSqlType::String,
            nullable: false,
        },
        FilterField::Status => CursorSqlField {
            column: "tasks.status",
            sql_type: CursorSqlType::String,
            nullable: false,
        },
        FilterField::SubmittedBy => CursorSqlField {
            column: "tasks.submitted_by",
            sql_type: CursorSqlType::Integer,
            nullable: true,
        },
        FilterField::CreatedAt => CursorSqlField {
            column: "tasks.created_at",
            sql_type: CursorSqlType::DateTime,
            nullable: false,
        },
        FilterField::StartedAt => CursorSqlField {
            column: "tasks.started_at",
            sql_type: CursorSqlType::DateTime,
            nullable: true,
        },
        FilterField::FinishedAt => CursorSqlField {
            column: "tasks.finished_at",
            sql_type: CursorSqlType::DateTime,
            nullable: true,
        },
        field => {
            return Err(PostgresStorageError::invalid_input(format!(
                "Field '{field}' is not orderable for tasks"
            )));
        }
    })
}

async fn find_task_by_idempotency(
    runtime: &PostgresRuntime,
    submitted_by: i32,
    key: &str,
) -> Result<Option<TaskRow>, PostgresStorageError> {
    let key = key.to_string();
    runtime
        .with_connection(async move |connection| {
            use crate::schema::tasks::dsl as stored;
            stored::tasks
                .filter(stored::submitted_by.eq(Some(submitted_by)))
                .filter(stored::idempotency_key.eq(key))
                .select(TaskRow::as_select())
                .first::<TaskRow>(connection)
                .await
                .optional()
        })
        .await
}

fn matching_idempotent_task(
    task: TaskRow,
    kind: StorageTaskKind,
    request_hash: &Option<String>,
    key: &str,
) -> Result<StorageTask, PostgresStorageError> {
    if task.kind == kind.as_str() && task.request_hash == *request_hash {
        task.into_storage()
    } else {
        Err(PostgresStorageError::conflict(format!(
            "Idempotency-Key '{key}' is already in use for a different task submission"
        )))
    }
}

async fn acquire_capacity_lock(
    connection: &mut PostgresConnection,
    submitted_by: i32,
    kind: StorageTaskKind,
) -> Result<(), PostgresStorageError> {
    let lock = diesel::sql_query("SELECT TRUE AS locked FROM pg_advisory_xact_lock($1)")
        .bind::<BigInt, _>(task_capacity_lock_key(submitted_by, kind))
        .get_result::<AdvisoryLockRow>(connection)
        .await?;
    if lock.locked {
        Ok(())
    } else {
        Err(PostgresStorageError::database(
            "Failed to acquire task capacity lock",
        ))
    }
}

fn task_capacity_lock_key(submitted_by: i32, kind: StorageTaskKind) -> i64 {
    const BASE_KEY: i64 = 4_801_000_000_000;
    const KIND_STRIDE: i64 = 1_i64 << 32;
    let kind_slot = match kind {
        StorageTaskKind::Export => 1,
        StorageTaskKind::RemoteCall => 2,
        StorageTaskKind::Backup => 3,
        StorageTaskKind::Import | StorageTaskKind::Reindex => 9,
    };
    BASE_KEY + (kind_slot * KIND_STRIDE) + i64::from(submitted_by)
}

async fn count_active_tasks(
    connection: &mut PostgresConnection,
    submitted_by: i32,
    task_kind: StorageTaskKind,
) -> Result<i64, PostgresStorageError> {
    use crate::schema::tasks::dsl as stored;
    let non_terminal = [
        StorageTaskStatus::Queued.as_str(),
        StorageTaskStatus::Validating.as_str(),
        StorageTaskStatus::Running.as_str(),
    ];
    Ok(stored::tasks
        .filter(stored::kind.eq(task_kind.as_str()))
        .filter(stored::submitted_by.eq(Some(submitted_by)))
        .filter(stored::status.eq_any(non_terminal))
        .filter(stored::deleted_at.is_null())
        .count()
        .get_result::<i64>(connection)
        .await?)
}

async fn insert_queued_task(
    connection: &mut PostgresConnection,
    row: NewTaskRow,
) -> Result<TaskRow, PostgresStorageError> {
    use crate::schema::tasks::dsl as stored;
    let task = diesel::insert_into(stored::tasks)
        .values(row)
        .returning(TaskRow::as_returning())
        .get_result::<TaskRow>(connection)
        .await?;
    let actor_user_id = task.submitted_by.ok_or_else(|| {
        PostgresStorageError::database("Newly queued task is missing its submitter")
    })?;
    let provenance = MutationProvenance::user_for_task(
        PrincipalId::new(actor_user_id)?,
        task.initiator_user_id.map(PrincipalId::new).transpose()?,
        TaskId::new(task.id)?,
    );
    let document = AuditDocument::try_new(
        "Task queued",
        None,
        None,
        json!({
            "task_id": task.id,
            "task_kind": task.kind,
        }),
    )?;
    let event =
        NewEvent::from_document(EntityType::Task, Action::Queued, ActorKind::User, document)
            .map_err(|error| PostgresStorageError::internal(error.to_string()))?
            .with_entity_id(hubuum_events_core::EventEntityId::new(task.id)?)
            .with_mutation_provenance(&provenance);
    append_event(connection, &event).await?;
    notify_task_queue(connection, task.id).await?;
    Ok(task)
}

fn log_task_queued(task: &TaskRow) {
    tracing::info!(
        message = "Task queued",
        backend = "postgresql",
        task_id = task.id,
        task_kind = task.kind,
        status = task.status,
        submitted_by = ?task.submitted_by,
        total_items = task.total_items,
        idempotency_key_present = task.idempotency_key.is_some(),
    );
}

async fn list_task_events_on(
    connection: &mut PostgresConnection,
    task_id: i32,
    options: &QueryOptions,
) -> Result<StoragePage<StorageTaskEvent>, PostgresStorageError> {
    let total = if options.include_total() {
        use crate::schema::events::dsl as stored;
        let total = stored::events
            .filter(stored::entity_type.eq(EntityType::Task.as_str()))
            .filter(stored::entity_id.eq(Some(task_id)))
            .count()
            .get_result::<i64>(connection)
            .await?;
        crate::reach_fault_point(crate::PostgresFaultPoint::PageAfterCount, Some(connection))
            .await?;
        Some(total)
    } else {
        None
    };
    let mut events = load_task_events(connection, task_id, options).await?;
    enrich_legacy_event_initiators(connection, &mut events).await?;
    let events = events
        .into_iter()
        .map(TaskEventRow::into_storage)
        .collect::<Result<Vec<_>, _>>()?;
    crate::persisted_page(events, total)
}

async fn load_task_events(
    connection: &mut PostgresConnection,
    task_id: i32,
    options: &QueryOptions,
) -> Result<Vec<TaskEventRow>, PostgresStorageError> {
    let cursor_id = decode_i64_page_cursor(options, "task events")?;
    let descending = page_descending(options);
    let limit = page_limit(options)?;
    use crate::schema::events::dsl as stored;
    let mut query = stored::events
        .filter(stored::entity_type.eq(EntityType::Task.as_str()))
        .filter(stored::entity_id.eq(Some(task_id)))
        .into_boxed();
    if let Some(cursor_id) = cursor_id {
        query = if descending {
            query.filter(stored::id.lt(cursor_id))
        } else {
            query.filter(stored::id.gt(cursor_id))
        };
    }
    let selection = (
        stored::id,
        stored::entity_id,
        stored::action,
        stored::summary,
        stored::metadata,
        stored::occurred_at,
        stored::actor_user_id,
        stored::actor_kind,
        stored::initiator_user_id,
        stored::task_id,
    );
    if descending {
        Ok(query
            .order(stored::id.desc())
            .limit(limit)
            .select(selection)
            .load::<TaskEventRow>(connection)
            .await?)
    } else {
        Ok(query
            .order(stored::id.asc())
            .limit(limit)
            .select(selection)
            .load::<TaskEventRow>(connection)
            .await?)
    }
}

async fn enrich_legacy_event_initiators(
    connection: &mut PostgresConnection,
    events: &mut [TaskEventRow],
) -> Result<(), PostgresStorageError> {
    let task_ids = events
        .iter()
        .filter(|event| event.initiator_user_id.is_none())
        .filter_map(|event| event.entity_id)
        .collect::<Vec<_>>();
    if task_ids.is_empty() {
        return Ok(());
    }
    use crate::schema::events::dsl as stored;
    let queued = stored::events
        .filter(stored::entity_type.eq(EntityType::Task.as_str()))
        .filter(stored::action.eq(Action::Queued.as_str()))
        .filter(stored::entity_id.eq_any(task_ids.into_iter().map(Some)))
        .order(stored::id.asc())
        .select((
            stored::entity_id,
            stored::initiator_user_id,
            stored::actor_user_id,
        ))
        .load::<(Option<i32>, Option<i32>, Option<i32>)>(connection)
        .await?;
    let initiators = queued
        .into_iter()
        .filter_map(|(task_id, initiator, actor)| {
            task_id.map(|task_id| (task_id, initiator.or(actor)))
        })
        .collect::<HashMap<_, _>>();
    for event in events {
        if event.initiator_user_id.is_none()
            && let Some(task_id) = event.entity_id
        {
            event.initiator_user_id = initiators.get(&task_id).copied().flatten();
        }
    }
    Ok(())
}

async fn list_import_task_results_on(
    connection: &mut PostgresConnection,
    task_id: i32,
    options: &QueryOptions,
) -> Result<StoragePage<StorageImportTaskResult>, PostgresStorageError> {
    use crate::schema::import_task_results::dsl as stored;
    let total = if options.include_total() {
        let total = stored::import_task_results
            .filter(stored::task_id.eq(task_id))
            .count()
            .get_result::<i64>(connection)
            .await?;
        crate::reach_fault_point(crate::PostgresFaultPoint::PageAfterCount, Some(connection))
            .await?;
        Some(total)
    } else {
        None
    };
    let cursor_id = decode_i32_page_cursor(options, "import task results")?;
    let descending = page_descending(options);
    let limit = page_limit(options)?;
    let mut storage_query = stored::import_task_results
        .filter(stored::task_id.eq(task_id))
        .into_boxed();
    if let Some(cursor_id) = cursor_id {
        storage_query = if descending {
            storage_query.filter(stored::id.lt(cursor_id))
        } else {
            storage_query.filter(stored::id.gt(cursor_id))
        };
    }
    let results = if descending {
        storage_query
            .order(stored::id.desc())
            .limit(limit)
            .select(ImportTaskResultRow::as_select())
            .load::<ImportTaskResultRow>(connection)
            .await?
    } else {
        storage_query
            .order(stored::id.asc())
            .limit(limit)
            .select(ImportTaskResultRow::as_select())
            .load::<ImportTaskResultRow>(connection)
            .await?
    };
    let results = results
        .into_iter()
        .map(ImportTaskResultRow::into_storage)
        .collect::<Result<Vec<_>, _>>()?;
    crate::persisted_page(results, total)
}

fn normalize_task_child_page_options(
    options: QueryOptions,
    resource: &str,
) -> Result<QueryOptions, PostgresStorageError> {
    reject_query_filters(&options, resource)?;
    let options = crate::cursor::normalize_query_options(options, FilterField::Id, false)?;
    if options
        .sort()
        .iter()
        .any(|sort| sort.field != FilterField::Id)
    {
        return Err(PostgresStorageError::invalid_input(format!(
            "Only the id field is orderable for {resource}"
        )));
    }
    Ok(options)
}

fn reject_query_filters(
    options: &QueryOptions,
    resource: &str,
) -> Result<(), PostgresStorageError> {
    if options.filters().is_empty() {
        return Ok(());
    }
    Err(PostgresStorageError::invalid_input(format!(
        "QueryOptions filters are not supported for {resource}; use the typed query scope"
    )))
}

fn decode_i64_page_cursor(
    options: &QueryOptions,
    resource: &str,
) -> Result<Option<i64>, PostgresStorageError> {
    let Some(cursor) = options.cursor().map(|cursor| cursor.as_str()) else {
        return Ok(None);
    };
    let values = hubuum_query::decode_cursor_values(cursor, options.sort())
        .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?;
    match values.as_slice() {
        [CursorValue::Integer(value)] => Ok(Some(*value)),
        _ => Err(PostgresStorageError::invalid_input(format!(
            "{resource} cursor does not match the current sort order"
        ))),
    }
}

fn decode_i32_page_cursor(
    options: &QueryOptions,
    resource: &str,
) -> Result<Option<i32>, PostgresStorageError> {
    decode_i64_page_cursor(options, resource)?
        .map(|value| {
            i32::try_from(value)
                .map_err(|_| PostgresStorageError::invalid_input("cursor id is out of range"))
        })
        .transpose()
}

fn page_descending(options: &QueryOptions) -> bool {
    options
        .sort()
        .as_slice()
        .first()
        .is_some_and(|sort| sort.descending)
}

fn page_limit(options: &QueryOptions) -> Result<i64, PostgresStorageError> {
    Ok(crate::cursor::validated_query_limit(options.limit())?
        .unwrap_or(DEFAULT_PAGE_WITH_LOOKAHEAD as i64))
}

fn classify_output<T, U>(
    row: Option<T>,
    expires_at: impl FnOnce(&T) -> NaiveDateTime,
    convert: impl FnOnce(T) -> Result<U, PostgresStorageError>,
) -> Result<StorageTaskOutputLookup<U>, PostgresStorageError> {
    Ok(match row {
        Some(row) => {
            let expires_at = expires_at(&row);
            if expires_at > Utc::now().naive_utc() {
                StorageTaskOutputLookup::Available(convert(row)?)
            } else {
                StorageTaskOutputLookup::Expired {
                    expires_at: expires_at.and_utc(),
                }
            }
        }
        None => StorageTaskOutputLookup::Missing,
    })
}

fn validate_positive_task_id(task_id: i32) -> Result<(), PostgresStorageError> {
    if task_id > 0 {
        Ok(())
    } else {
        Err(PostgresStorageError::invalid_input(
            "task id must be positive",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hubuum_query::{ParsedQueryParam, SearchOperator, SortParam};
    use hubuum_storage_core::StorageErrorKind;

    #[test]
    fn task_cursor_mapping_covers_the_public_sort_contract() {
        let options = QueryOptions::new(
            Vec::new(),
            [
                FilterField::Id,
                FilterField::Kind,
                FilterField::Status,
                FilterField::SubmittedBy,
                FilterField::CreatedAt,
                FilterField::StartedAt,
                FilterField::FinishedAt,
            ]
            .into_iter()
            .map(|field| SortParam {
                field,
                descending: false,
            })
            .collect::<Vec<_>>(),
            None,
            None,
            false,
        )
        .unwrap();

        assert_eq!(task_cursor_fields(&options).unwrap().len(), 7);
    }

    #[test]
    fn task_pages_reject_untyped_query_filters() {
        let options = QueryOptions::new(
            vec![ParsedQueryParam::from_parts(
                FilterField::Status,
                SearchOperator::Equals { is_negated: false },
                "queued",
            )],
            Vec::new(),
            None,
            None,
            false,
        )
        .unwrap();

        let error = reject_query_filters(&options, "tasks").unwrap_err();

        assert_eq!(error.kind(), StorageErrorKind::InvalidInput);
    }

    #[test]
    fn task_child_pages_reject_query_filters() {
        let options = QueryOptions::new(
            vec![ParsedQueryParam::from_parts(
                FilterField::Id,
                SearchOperator::Equals { is_negated: false },
                "1",
            )],
            Vec::new(),
            None,
            None,
            false,
        )
        .unwrap();

        let error = normalize_task_child_page_options(options, "task events").unwrap_err();

        assert_eq!(error.kind(), StorageErrorKind::InvalidInput);
    }

    #[test]
    fn capacity_lock_partitions_kinds_and_submitters() {
        let export = task_capacity_lock_key(7, StorageTaskKind::Export);
        let backup = task_capacity_lock_key(7, StorageTaskKind::Backup);
        let other_submitter = task_capacity_lock_key(8, StorageTaskKind::Export);

        assert_ne!(export, backup);
        assert_ne!(export, other_submitter);
    }

    #[test]
    fn invalid_persisted_export_output_is_a_backend_failure() {
        let timestamp = chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .unwrap()
            .naive_utc();
        let row = ExportOutputRow {
            task_id: 1,
            template_name: None,
            content_type: "application/json".to_string(),
            json_output: Some(serde_json::json!({})),
            text_output: None,
            meta_json: serde_json::json!({}),
            warnings_json: serde_json::json!([]),
            warning_count: -1,
            truncated: false,
            output_expires_at: timestamp,
            total_duration_ms: 0,
            query_duration_ms: 0,
            hydration_duration_ms: 0,
            render_duration_ms: 0,
            created_at: timestamp,
        };

        let error = row
            .into_storage()
            .err()
            .expect("invalid persisted export output must fail conversion");

        assert_eq!(error.kind(), StorageErrorKind::Backend);
    }

    #[test]
    fn invalid_persisted_backup_output_is_a_backend_failure() {
        let timestamp = chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .unwrap()
            .naive_utc();
        let row = BackupOutputRow {
            task_id: 1,
            document: b"{}".to_vec(),
            byte_size: 3,
            sha256: "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a".to_string(),
            output_expires_at: timestamp,
            created_at: timestamp,
        };

        let error = row
            .into_storage()
            .err()
            .expect("invalid persisted backup output must fail conversion");

        assert_eq!(error.kind(), StorageErrorKind::Backend);
    }
}
