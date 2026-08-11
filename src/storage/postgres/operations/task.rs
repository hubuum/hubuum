use crate::models::token_scope::TokenScope;
use crate::storage::postgres::prelude::*;
use chrono::{NaiveDateTime, Utc};
use diesel::dsl::sql;
use diesel::expression::AsExpression;
use diesel::sql_types::{Array, BigInt, Bool, Integer, Nullable, Text, Timestamp};
use hubuum_task_core::IdempotencyKey;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::info;
use uuid::Uuid;

use crate::apply_query_options;
use crate::config::get_config;
use crate::errors::ApiError;
use crate::events::{
    Action, EntityType, Event, MutationProvenance, NewEvent, emit_event, notify_task_queue,
};
use crate::models::search::QueryOptions;
use crate::models::{
    BackupOutputLookup, BackupTaskOutputRecord, BackupTaskOutputSummaryRecord, ExportOutputLookup,
    ExportTaskOutputRecord, ExportTaskOutputSummaryRecord, ImportTaskResultRecord,
    NewBackupTaskOutputRecord, NewExportTaskOutputRecord, NewImportTaskResultRecord,
    NewRemoteCallResult, NewTaskEventRecord, NewTaskRecord, PrincipalID, TaskEventRecord, TaskID,
    TaskKind, TaskRecord, TaskResponse, TaskResultCounts, TaskStatus, TokenID,
};
use crate::observability::metrics;
use crate::pagination::{CursorValue, decode_cursor_values, page_limits_or_defaults};
#[cfg(test)]
use crate::storage::postgres::operations::history::resolve_principal_names;
use crate::storage::postgres::operations::maintenance::maintenance_state_conn;
use crate::storage::postgres::{with_connection, with_transaction};
use crate::tasks::TaskLeaseDuration;

const DATABASE_UTC_NOW_SQL: &str = "clock_timestamp() AT TIME ZONE 'UTC'";
const DATABASE_UTC_NOW_QUERY: &str = "SELECT clock_timestamp() AT TIME ZONE 'UTC' AS now";
const DATABASE_UTC_LEASE_EXPIRY_SQL_PREFIX: &str = "((clock_timestamp() AT TIME ZONE 'UTC') + (";
const DATABASE_LEASE_EXPIRY_SQL_SUFFIX: &str = " * INTERVAL '1 millisecond'))";

pub struct TaskStateUpdate {
    status: TaskStatus,
    summary: Option<String>,
    counts: TaskResultCounts,
    started_at: Option<NaiveDateTime>,
    finished_at: Option<NaiveDateTime>,
}

impl TaskStateUpdate {
    pub fn new(status: TaskStatus, counts: TaskResultCounts) -> Self {
        Self {
            status,
            summary: None,
            counts,
            started_at: None,
            finished_at: None,
        }
    }

    pub fn with_summary(mut self, summary: impl Into<String>) -> Self {
        self.summary = Some(summary.into());
        self
    }

    pub fn with_started_at(mut self, started_at: Option<NaiveDateTime>) -> Self {
        self.started_at = started_at;
        self
    }
}

pub struct TaskCreateRequest {
    kind: TaskKind,
    /// Principal id of the submitter.
    submitted_by: PrincipalID,
    idempotency_key: Option<IdempotencyKey>,
    request_hash: Option<String>,
    request_payload: serde_json::Value,
    total_items: i32,
    /// Persisted scope snapshot fields derived atomically from
    /// [`TaskScopeSnapshot`] by [`TaskCreateRequest::builder`].
    submitted_token_id: Option<i32>,
    submitted_token_scoped: bool,
    submitted_token_scopes: serde_json::Value,
}

/// Encode a token scope for asynchronous execution. Unscoped callers retain the
/// historical empty-array marker; scoped callers use an object that preserves
/// independent permission and resource dimensions.
pub fn scope_snapshot_json(scopes: Option<&TokenScope>) -> serde_json::Value {
    scopes
        .map(TokenScope::snapshot_json)
        .unwrap_or_else(|| serde_json::json!([]))
}

/// The submitting token's scope boundary, captured at task-creation time and
/// persisted so async execution can never exceed it.
#[derive(Debug, Clone)]
pub struct TaskScopeSnapshot {
    token_id: Option<TokenID>,
    /// Whether the submitting token was scoped. This remains explicit for task
    /// metadata and for compatibility with legacy permission-array snapshots.
    scoped: bool,
    scopes: serde_json::Value,
}

pub struct TaskCreateRequestBuilder {
    kind: TaskKind,
    submitted_by: PrincipalID,
    request_payload: serde_json::Value,
    total_items: i32,
    idempotency_key: Option<IdempotencyKey>,
    request_hash: Option<String>,
    scope_snapshot: TaskScopeSnapshot,
}

impl TaskScopeSnapshot {
    /// Build from the submitting token id and its live scope set.
    pub fn from_request(token_id: Option<TokenID>, scopes: Option<&TokenScope>) -> Self {
        Self {
            token_id,
            scoped: scopes.is_some(),
            scopes: scope_snapshot_json(scopes),
        }
    }

    pub fn unscoped() -> Self {
        Self::from_request(None, None)
    }

    pub(in crate::storage::postgres) fn from_persisted(
        token_id: Option<i32>,
        scoped: bool,
        scopes: serde_json::Value,
    ) -> Result<Self, ApiError> {
        Ok(Self {
            token_id: token_id.map(TokenID::new).transpose()?,
            scoped,
            scopes,
        })
    }
}

impl TaskCreateRequest {
    pub fn builder(
        kind: TaskKind,
        submitted_by: PrincipalID,
        request_payload: serde_json::Value,
        total_items: i32,
    ) -> TaskCreateRequestBuilder {
        TaskCreateRequestBuilder {
            kind,
            submitted_by,
            request_payload,
            total_items,
            idempotency_key: None,
            request_hash: None,
            scope_snapshot: TaskScopeSnapshot::unscoped(),
        }
    }
}

impl TaskCreateRequestBuilder {
    pub fn idempotency_key(mut self, idempotency_key: Option<IdempotencyKey>) -> Self {
        self.idempotency_key = idempotency_key;
        self
    }

    pub fn request_hash(mut self, request_hash: Option<String>) -> Self {
        self.request_hash = request_hash;
        self
    }

    pub fn scope_snapshot(mut self, scope_snapshot: TaskScopeSnapshot) -> Self {
        self.scope_snapshot = scope_snapshot;
        self
    }

    pub fn build(self) -> TaskCreateRequest {
        let TaskScopeSnapshot {
            token_id,
            scoped,
            scopes,
        } = self.scope_snapshot;
        TaskCreateRequest {
            kind: self.kind,
            submitted_by: self.submitted_by,
            idempotency_key: self.idempotency_key,
            request_hash: self.request_hash,
            request_payload: self.request_payload,
            total_items: self.total_items,
            submitted_token_id: token_id.map(TokenID::id),
            submitted_token_scoped: scoped,
            submitted_token_scopes: scopes,
        }
    }
}

#[derive(QueryableByName)]
struct AdvisoryLockRow {
    #[diesel(sql_type = Bool)]
    locked: bool,
}

#[derive(QueryableByName)]
struct DatabaseTimeRow {
    #[diesel(sql_type = Timestamp)]
    now: chrono::NaiveDateTime,
}

async fn database_now(
    conn: &mut crate::storage::postgres::PostgresConnection,
) -> Result<chrono::NaiveDateTime, ApiError> {
    diesel::sql_query(DATABASE_UTC_NOW_QUERY)
        .get_result::<DatabaseTimeRow>(conn)
        .await
        .map(|row| row.now)
        .map_err(ApiError::from)
}

#[derive(Debug, Clone)]
pub(crate) struct QueuedTaskCancellation {
    summary: String,
    event_message: String,
    actor: Option<PrincipalID>,
    emit_event: bool,
}

impl QueuedTaskCancellation {
    pub(crate) fn new(summary: impl Into<String>) -> Self {
        let summary = summary.into();
        Self {
            event_message: summary.clone(),
            summary,
            actor: None,
            emit_event: true,
        }
    }

    pub(crate) fn with_event_message(mut self, message: impl Into<String>) -> Self {
        self.event_message = message.into();
        self
    }

    pub(crate) fn with_actor(mut self, actor: Option<PrincipalID>) -> Self {
        self.actor = actor;
        self
    }

    pub(crate) fn with_event_emission(mut self, emit_event: bool) -> Self {
        self.emit_event = emit_event;
        self
    }
}

/// Apply the complete queued-to-cancelled persistence transition.
///
/// Callers must invoke this inside a transaction when cancellation is part of a
/// larger mutation. The status predicate protects tasks claimed after their ids
/// were selected, while the shared transition keeps terminal timestamps,
/// request redaction, leases, and lifecycle events consistent.
pub(crate) async fn cancel_queued_tasks_conn(
    conn: &mut crate::storage::postgres::PostgresConnection,
    task_ids: &[i32],
    cancellation: &QueuedTaskCancellation,
) -> Result<Vec<TaskRecord>, ApiError> {
    use crate::schema::tasks::dsl::{
        finished_at, id, lease_expires_at, lease_token, request_payload, request_redacted_at,
        status, summary, tasks, updated_at,
    };

    if task_ids.is_empty() {
        return Ok(Vec::new());
    }

    let terminal_at = database_now(conn).await?;
    let cancelled = diesel::update(
        tasks
            .filter(id.eq_any(task_ids))
            .filter(status.eq(TaskStatus::Queued.as_str())),
    )
    .set((
        status.eq(TaskStatus::Cancelled.as_str()),
        summary.eq(Some(cancellation.summary.clone())),
        finished_at.eq(Some(terminal_at)),
        request_payload.eq::<Option<serde_json::Value>>(None),
        request_redacted_at.eq(Some(terminal_at)),
        lease_token.eq::<Option<Uuid>>(None),
        lease_expires_at.eq::<Option<NaiveDateTime>>(None),
        updated_at.eq(terminal_at),
    ))
    .get_results::<TaskRecord>(conn)
    .await?;

    if cancellation.emit_event {
        for task in &cancelled {
            let provenance = if let Some(actor) = cancellation.actor {
                task.user_provenance(actor)
            } else {
                task.system_provenance()
            };
            emit_task_lifecycle_event(
                conn,
                task,
                &NewTaskEventRecord {
                    task_id: task.id,
                    event_type: TaskStatus::Cancelled.as_str().to_string(),
                    message: cancellation.event_message.clone(),
                    data: None,
                },
                &provenance,
            )
            .await?;
        }
    }

    Ok(cancelled)
}

/// Anything that can name a task for a backend query: a [`TaskID`] from a request path or an
/// already-loaded [`TaskRecord`] (and references to either). The required `task_id` resolves the
/// raw id at the persistence boundary so it never leaks into the domain.
pub trait TaskIdentifier {
    fn task_id(&self) -> i32;

    fn task_lease_token(&self) -> Option<Uuid> {
        None
    }
}

impl TaskIdentifier for TaskID {
    fn task_id(&self) -> i32 {
        self.id()
    }
}

impl TaskIdentifier for TaskRecord {
    fn task_id(&self) -> i32 {
        self.id
    }

    fn task_lease_token(&self) -> Option<Uuid> {
        self.lease_token
    }
}

impl<T: TaskIdentifier + ?Sized> TaskIdentifier for &T {
    fn task_id(&self) -> i32 {
        (**self).task_id()
    }

    fn task_lease_token(&self) -> Option<Uuid> {
        (**self).task_lease_token()
    }
}

/// Single-task backend persistence, as self-methods on any [`TaskIdentifier`]. Callers write
/// `task.find_record(pool)` / `task.update_state(pool, ..)` rather than passing a bare id to a free
/// function; all Diesel query construction stays here in the backend layer.
pub trait TaskBackend: TaskIdentifier {
    async fn find_record(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<TaskRecord, ApiError> {
        use crate::schema::tasks::dsl::{id, tasks};

        let task_id_value = self.task_id();
        with_connection(pool, async |conn| {
            tasks
                .filter(id.eq(task_id_value))
                .first::<TaskRecord>(conn)
                .await
        })
        .await
    }

    async fn find_claimed_record(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<TaskRecord, ApiError> {
        let claim_token = self.task_lease_token().ok_or_else(|| {
            ApiError::BadRequest("A live task claim is required for this operation".to_string())
        })?;
        let task_id = self.task_id();
        with_transaction(pool, async |conn| {
            live_claimed_task_conn(conn, task_id, claim_token).await
        })
        .await
    }

    async fn list_events_with_total_count(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: &QueryOptions,
    ) -> Result<(Vec<TaskEventRecord>, i64), ApiError> {
        use crate::schema::events::dsl::{entity_id, entity_type, events, id};

        let task_id_value = self.task_id();
        let limit = query_options
            .limit
            .unwrap_or(page_limits_or_defaults().default_limit().saturating_add(1));
        let descending = query_options
            .sort
            .as_slice()
            .first()
            .map(|sort| sort.descending)
            .unwrap_or(false);
        let cursor_id = decode_task_event_cursor_id(query_options)?;

        let total_count = crate::pagination::exact_count_or_skipped(query_options, async || {
            with_connection(pool, async |conn| {
                events
                    .filter(entity_type.eq(EntityType::Task.as_str()))
                    .filter(entity_id.eq(Some(task_id_value)))
                    .count()
                    .get_result::<i64>(conn)
                    .await
            })
            .await
        })
        .await?;

        let items = with_connection(pool, async |conn| {
            let mut query = events
                .filter(entity_type.eq(EntityType::Task.as_str()))
                .filter(entity_id.eq(Some(task_id_value)))
                .into_boxed();
            if let Some(cursor_id) = cursor_id {
                query = if descending {
                    query.filter(id.lt(cursor_id))
                } else {
                    query.filter(id.gt(cursor_id))
                };
            }

            if descending {
                query
                    .order(id.desc())
                    .limit(limit as i64)
                    .load::<Event>(conn)
                    .await
            } else {
                query
                    .order(id.asc())
                    .limit(limit as i64)
                    .load::<Event>(conn)
                    .await
            }
        })
        .await?
        .into_iter()
        .map(TaskEventRecord::try_from)
        .collect::<Result<Vec<_>, _>>()?;

        Ok((items, total_count))
    }

    async fn list_import_results_with_total_count(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: &QueryOptions,
    ) -> Result<(Vec<ImportTaskResultRecord>, i64), ApiError> {
        use crate::schema::import_task_results::dsl::{id, import_task_results, task_id};

        let task_id_value = self.task_id();
        let limit = query_options
            .limit
            .unwrap_or(page_limits_or_defaults().default_limit().saturating_add(1));
        let descending = query_options
            .sort
            .as_slice()
            .first()
            .map(|sort| sort.descending)
            .unwrap_or(false);
        let cursor_id = decode_int_history_cursor_id(query_options)?;

        let total_count = crate::pagination::exact_count_or_skipped(query_options, async || {
            with_connection(pool, async |conn| {
                import_task_results
                    .filter(task_id.eq(task_id_value))
                    .count()
                    .get_result::<i64>(conn)
                    .await
            })
            .await
        })
        .await?;

        let items = with_connection(pool, async |conn| {
            let mut query = import_task_results
                .filter(task_id.eq(task_id_value))
                .into_boxed();
            if let Some(cursor_id) = cursor_id {
                query = if descending {
                    query.filter(id.lt(cursor_id))
                } else {
                    query.filter(id.gt(cursor_id))
                };
            }

            if descending {
                query
                    .order(id.desc())
                    .limit(limit as i64)
                    .load::<ImportTaskResultRecord>(conn)
                    .await
            } else {
                query
                    .order(id.asc())
                    .limit(limit as i64)
                    .load::<ImportTaskResultRecord>(conn)
                    .await
            }
        })
        .await?;

        Ok((items, total_count))
    }

    async fn find_export_output(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<ExportOutputLookup<ExportTaskOutputRecord>, ApiError> {
        use crate::schema::export_task_outputs::dsl::{export_task_outputs, task_id};

        let task_id_value = self.task_id();
        let now = Utc::now().naive_utc();
        // Fetch without the expiry filter so an expired-but-present row is exported as `Expired`
        // (410) rather than silently looking like a row that never existed (404).
        let record = with_connection(pool, async |conn| {
            export_task_outputs
                .filter(task_id.eq(task_id_value))
                .first::<ExportTaskOutputRecord>(conn)
                .await
                .optional()
        })
        .await?;

        Ok(match record {
            Some(record) if record.output_expires_at > now => ExportOutputLookup::Available(record),
            Some(record) => ExportOutputLookup::Expired {
                expires_at: record.output_expires_at,
            },
            None => ExportOutputLookup::Missing,
        })
    }

    async fn find_export_output_summary(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<ExportOutputLookup<ExportTaskOutputSummaryRecord>, ApiError> {
        use crate::schema::export_task_outputs::dsl::{export_task_outputs, task_id};

        let task_id_value = self.task_id();
        let now = Utc::now().naive_utc();
        let record = with_connection(pool, async |conn| {
            export_task_outputs
                .filter(task_id.eq(task_id_value))
                .select(ExportTaskOutputSummaryRecord::as_select())
                .first::<ExportTaskOutputSummaryRecord>(conn)
                .await
                .optional()
        })
        .await?;

        Ok(match record {
            Some(record) if record.output_expires_at > now => ExportOutputLookup::Available(record),
            Some(record) => ExportOutputLookup::Expired {
                expires_at: record.output_expires_at,
            },
            None => ExportOutputLookup::Missing,
        })
    }

    async fn find_backup_output(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<BackupOutputLookup<BackupTaskOutputRecord>, ApiError> {
        use crate::schema::backup_task_outputs::dsl::{backup_task_outputs, task_id};

        let task_id_value = self.task_id();
        let now = Utc::now().naive_utc();
        let record = with_connection(pool, async |conn| {
            backup_task_outputs
                .filter(task_id.eq(task_id_value))
                .first::<BackupTaskOutputRecord>(conn)
                .await
                .optional()
        })
        .await?;

        Ok(match record {
            Some(record) if record.output_expires_at > now => BackupOutputLookup::Available(record),
            Some(record) => BackupOutputLookup::Expired {
                expires_at: record.output_expires_at,
            },
            None => BackupOutputLookup::Missing,
        })
    }

    async fn find_backup_output_summary(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<BackupOutputLookup<BackupTaskOutputSummaryRecord>, ApiError> {
        use crate::schema::backup_task_outputs::dsl::{backup_task_outputs, task_id};

        let task_id_value = self.task_id();
        let now = Utc::now().naive_utc();
        let record = with_connection(pool, async |conn| {
            backup_task_outputs
                .filter(task_id.eq(task_id_value))
                .select(BackupTaskOutputSummaryRecord::as_select())
                .first::<BackupTaskOutputSummaryRecord>(conn)
                .await
                .optional()
        })
        .await?;

        Ok(match record {
            Some(record) if record.output_expires_at > now => BackupOutputLookup::Available(record),
            Some(record) => BackupOutputLookup::Expired {
                expires_at: record.output_expires_at,
            },
            None => BackupOutputLookup::Missing,
        })
    }

    async fn count_import_results(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<TaskResultCounts, ApiError> {
        use crate::schema::import_task_results::dsl::{import_task_results, outcome, task_id};

        let task_id_value = self.task_id();
        with_connection(pool, async |conn| -> Result<TaskResultCounts, ApiError> {
            let processed = import_task_results
                .filter(task_id.eq(task_id_value))
                .count()
                .get_result::<i64>(conn)
                .await?;
            let failed = import_task_results
                .filter(task_id.eq(task_id_value))
                .filter(outcome.eq("failed"))
                .count()
                .get_result::<i64>(conn)
                .await?;
            TaskResultCounts::from_outcomes(processed - failed, failed)
        })
        .await
    }

    async fn update_state(
        &self,
        pool: &impl crate::storage::StorageContext,
        update: TaskStateUpdate,
    ) -> Result<TaskRecord, ApiError> {
        use crate::schema::tasks::dsl::{
            failed_items, finished_at, id, lease_expires_at, lease_token, processed_items,
            started_at, status, success_items, summary, tasks, updated_at,
        };

        let task_id_value = self.task_id();
        let task_lease_token = self.task_lease_token();
        let record = with_connection(pool, async |conn| -> Result<TaskRecord, ApiError> {
            let no_lease_token: diesel::dsl::AsExprOf<bool, Bool> =
                <bool as AsExpression<Bool>>::as_expression(task_lease_token.is_none());
            Ok(diesel::update(
                tasks.filter(id.eq(task_id_value)).filter(
                    lease_token
                        .eq(task_lease_token)
                        .and(lease_expires_at.gt(sql::<Nullable<Timestamp>>(DATABASE_UTC_NOW_SQL)))
                        .or(lease_token.is_null().and(no_lease_token)),
                ),
            )
            .set((
                status.eq(update.status.as_str()),
                summary.eq(update.summary),
                processed_items.eq(update.counts.processed()),
                success_items.eq(update.counts.success()),
                failed_items.eq(update.counts.failed()),
                started_at.eq(update.started_at),
                finished_at.eq(update.finished_at),
                updated_at.eq(sql::<Timestamp>(DATABASE_UTC_NOW_SQL)),
            ))
            .get_result::<TaskRecord>(conn)
            .await?)
        })
        .await?;

        info!(
            message = "Task state updated",
            task_id = record.id,
            task_kind = record.kind.as_str(),
            status = record.status.as_str(),
            processed_items = record.processed_items,
            success_items = record.success_items,
            failed_items = record.failed_items
        );

        Ok(record)
    }

    async fn finalize_terminal(
        &self,
        pool: &impl crate::storage::StorageContext,
        update: TaskStateUpdate,
        event: NewTaskEventRecord,
    ) -> Result<TaskRecord, ApiError> {
        let task_id_value = self.task_id();
        let task_lease_token = self.task_lease_token();
        let record = with_transaction(pool, async |conn| -> Result<TaskRecord, ApiError> {
            finalize_terminal_conn(conn, task_id_value, task_lease_token, update, event).await
        })
        .await?;

        record_task_terminal(&record);

        Ok(record)
    }

    async fn finalize_export_with_output(
        &self,
        pool: &impl crate::storage::StorageContext,
        update: TaskStateUpdate,
        event: NewTaskEventRecord,
        output: NewExportTaskOutputRecord,
    ) -> Result<TaskRecord, ApiError> {
        use crate::schema::export_task_outputs::dsl::{
            export_task_outputs, task_id as export_output_task_id,
        };
        let task_id_value = self.task_id();
        let task_lease_token = self.task_lease_token();
        let record = with_transaction(pool, async |conn| -> Result<TaskRecord, ApiError> {
            // Idempotent so a future requeue / manual re-claim that re-finalizes the same task
            // cannot trip the `export_task_outputs.task_id` UNIQUE constraint and roll back the
            // transaction, which would otherwise leave the task stuck mid-flight.
            diesel::insert_into(export_task_outputs)
                .values(output)
                .on_conflict(export_output_task_id)
                .do_nothing()
                .execute(conn)
                .await?;
            finalize_terminal_conn(conn, task_id_value, task_lease_token, update, event).await
        })
        .await?;

        record_task_terminal(&record);

        Ok(record)
    }

    async fn finalize_backup_with_output(
        &self,
        pool: &impl crate::storage::StorageContext,
        update: TaskStateUpdate,
        event: NewTaskEventRecord,
        output: NewBackupTaskOutputRecord,
    ) -> Result<TaskRecord, ApiError> {
        use crate::schema::backup_task_outputs::dsl::{
            backup_task_outputs, task_id as backup_output_task_id,
        };
        let task_id_value = self.task_id();
        let task_lease_token = self.task_lease_token();
        let record = with_transaction(pool, async |conn| -> Result<TaskRecord, ApiError> {
            diesel::insert_into(backup_task_outputs)
                .values(output)
                .on_conflict(backup_output_task_id)
                .do_nothing()
                .execute(conn)
                .await?;
            finalize_terminal_conn(conn, task_id_value, task_lease_token, update, event).await
        })
        .await?;

        record_task_terminal(&record);
        Ok(record)
    }

    async fn finalize_remote_call_with_result(
        &self,
        pool: &impl crate::storage::StorageContext,
        update: TaskStateUpdate,
        event: NewTaskEventRecord,
        result: NewRemoteCallResult,
    ) -> Result<TaskRecord, ApiError> {
        let task_id_value = self.task_id();
        let task_lease_token = self.task_lease_token();
        let record = with_transaction(pool, async |conn| -> Result<TaskRecord, ApiError> {
            super::remote_target::upsert_remote_call_result_conn(conn, result).await?;
            finalize_terminal_conn(conn, task_id_value, task_lease_token, update, event).await
        })
        .await?;

        record_task_terminal(&record);
        Ok(record)
    }
}

impl<T: TaskIdentifier + ?Sized> TaskBackend for T {}

pub(crate) async fn finalize_terminal_conn(
    conn: &mut crate::storage::postgres::PostgresConnection,
    task_id_value: i32,
    task_lease_token: Option<Uuid>,
    update: TaskStateUpdate,
    event: NewTaskEventRecord,
) -> Result<TaskRecord, ApiError> {
    use crate::schema::tasks::dsl::{
        failed_items, finished_at, id, lease_expires_at, lease_token, processed_items,
        request_payload, request_redacted_at, started_at, status, success_items, summary, tasks,
        updated_at,
    };

    let task = tasks
        .filter(id.eq(task_id_value))
        .first::<TaskRecord>(conn)
        .await?;
    let event_record =
        emit_task_lifecycle_event(conn, &task, &event, &task.worker_provenance()).await?;
    let no_lease_token: diesel::dsl::AsExprOf<bool, Bool> =
        <bool as AsExpression<Bool>>::as_expression(task_lease_token.is_none());

    Ok(diesel::update(
        tasks.filter(id.eq(task_id_value)).filter(
            lease_token
                .eq(task_lease_token)
                .and(lease_expires_at.gt(sql::<Nullable<Timestamp>>(DATABASE_UTC_NOW_SQL)))
                .or(lease_token.is_null().and(no_lease_token)),
        ),
    )
    .set((
        status.eq(update.status.as_str()),
        summary.eq(update.summary),
        processed_items.eq(update.counts.processed()),
        success_items.eq(update.counts.success()),
        failed_items.eq(update.counts.failed()),
        started_at.eq(update.started_at),
        finished_at.eq(Some(event_record.occurred_at)),
        request_payload.eq::<Option<serde_json::Value>>(None),
        request_redacted_at.eq(event_record.occurred_at),
        lease_token.eq::<Option<Uuid>>(None),
        lease_expires_at.eq::<Option<chrono::NaiveDateTime>>(None),
        updated_at.eq(event_record.occurred_at),
    ))
    .get_result::<TaskRecord>(conn)
    .await?)
}

/// Load and lock a task only while the caller owns its unexpired active lease.
///
/// Backend workflows use this inside the same transaction as their mutations,
/// preventing a stale worker from committing domain data after losing its
/// claim.
pub(crate) async fn live_claimed_task_conn(
    conn: &mut crate::storage::postgres::PostgresConnection,
    task_id_value: i32,
    claim_token: Uuid,
) -> Result<TaskRecord, ApiError> {
    use crate::schema::tasks::dsl::{id, lease_expires_at, lease_token, status, tasks};

    tasks
        .filter(id.eq(task_id_value))
        .filter(lease_token.eq(Some(claim_token)))
        .filter(lease_expires_at.gt(sql::<Nullable<Timestamp>>(DATABASE_UTC_NOW_SQL)))
        .filter(status.eq_any(TaskStatus::ACTIVE.map(TaskStatus::as_str)))
        .for_update()
        .first::<TaskRecord>(conn)
        .await
        .map_err(ApiError::from)
}

pub(crate) fn record_task_terminal(record: &TaskRecord) {
    info!(
        message = "Task reached terminal state",
        task_id = record.id,
        task_kind = record.kind.as_str(),
        status = record.status.as_str(),
        processed_items = record.processed_items,
        success_items = record.success_items,
        failed_items = record.failed_items,
        summary = record.summary.as_deref()
    );
    record_task_completion_metrics(record);
}

fn record_task_completion_metrics(record: &TaskRecord) {
    metrics::task_completed(
        &record.kind,
        &record.status,
        record
            .started_at
            .and_then(|started_at| duration_between(started_at, record.finished_at)),
    );
}

fn duration_between(
    start: chrono::NaiveDateTime,
    end: Option<chrono::NaiveDateTime>,
) -> Option<std::time::Duration> {
    let elapsed = end?.signed_duration_since(start).num_milliseconds();
    (elapsed >= 0).then(|| std::time::Duration::from_millis(elapsed as u64))
}

#[cfg(any(test, feature = "integration-test-support"))]
impl NewTaskRecord {
    /// Insert this new task row and return the persisted record.
    pub async fn create(
        self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<TaskRecord, ApiError> {
        use crate::schema::tasks::dsl::tasks;

        with_connection(pool, async |conn| {
            diesel::insert_into(tasks)
                .values(&self)
                .get_result::<TaskRecord>(conn)
                .await
        })
        .await
    }
}

impl TaskRecord {
    /// Find the task submitted by `submitter_id` carrying the given idempotency key, if any.
    pub async fn find_by_idempotency(
        pool: &impl crate::storage::StorageContext,
        submitter_id: PrincipalID,
        key: &str,
    ) -> Result<Option<TaskRecord>, ApiError> {
        use crate::schema::tasks::dsl::{idempotency_key, submitted_by, tasks};

        with_connection(pool, async |conn| {
            tasks
                .filter(submitted_by.eq(Some(submitter_id.id())))
                .filter(idempotency_key.eq(key))
                .first::<TaskRecord>(conn)
                .await
                .optional()
        })
        .await
    }
}

fn build_task_query<'a>(
    submitted_by_filter: Option<i32>,
    kind_filter: Option<&'a str>,
    status_filter: Option<&'a str>,
) -> crate::schema::tasks::BoxedQuery<'a, diesel::pg::Pg> {
    use crate::schema::tasks::dsl::{kind, status, submitted_by, tasks};

    let mut query = tasks.into_boxed();

    if let Some(submitter_id) = submitted_by_filter {
        query = query.filter(submitted_by.eq(Some(submitter_id)));
    }

    if let Some(task_kind) = kind_filter {
        query = query.filter(kind.eq(task_kind));
    }

    if let Some(task_status) = status_filter {
        query = query.filter(status.eq(task_status));
    }

    query
}

pub async fn list_tasks_with_total_count(
    pool: &impl crate::storage::StorageContext,
    submitted_by_filter: Option<i32>,
    kind_filter: Option<&str>,
    status_filter: Option<&str>,
    query_options: &QueryOptions,
) -> Result<(Vec<TaskRecord>, i64), ApiError> {
    let total_count = crate::pagination::exact_count_or_skipped(query_options, async || {
        with_connection(pool, async |conn| {
            build_task_query(submitted_by_filter, kind_filter, status_filter)
                .count()
                .get_result::<i64>(conn)
                .await
        })
        .await
    })
    .await?;

    let items = with_connection(pool, async |conn| -> Result<Vec<TaskRecord>, ApiError> {
        let mut query = build_task_query(submitted_by_filter, kind_filter, status_filter);
        apply_query_options!(query, query_options, TaskResponse);
        Ok(query.load::<TaskRecord>(conn).await?)
    })
    .await?;

    Ok((items, total_count))
}

/// Enrich one task-event page with legacy queued-event initiators and one
/// batched principal-name lookup.
#[cfg(test)]
pub(crate) async fn task_event_responses(
    pool: &impl crate::storage::StorageContext,
    records: Vec<TaskEventRecord>,
) -> Result<Vec<crate::models::TaskEventResponse>, ApiError> {
    let records = enrich_legacy_task_event_initiators(pool, records).await?;
    let principal_ids = records
        .iter()
        .flat_map(|record| [record.actor_user_id, record.initiator_user_id])
        .flatten()
        .collect();
    let principal_names = resolve_principal_names(pool, principal_ids).await?;
    Ok(records
        .into_iter()
        .map(|record| {
            crate::models::TaskEventResponse::from_record_with_names(record, &principal_names)
        })
        .collect())
}

pub(crate) async fn enrich_legacy_task_event_initiators(
    pool: &impl crate::storage::StorageContext,
    mut records: Vec<TaskEventRecord>,
) -> Result<Vec<TaskEventRecord>, ApiError> {
    use crate::schema::events::dsl as stored;

    let legacy_task_ids = records
        .iter()
        .filter(|record| record.initiator_user_id.is_none())
        .map(|record| record.task_id)
        .collect::<Vec<_>>();
    if !legacy_task_ids.is_empty() {
        let queued = with_connection(pool, async |conn| {
            stored::events
                .filter(stored::entity_type.eq(EntityType::Task.as_str()))
                .filter(stored::action.eq(Action::Queued.as_str()))
                .filter(stored::entity_id.eq_any(legacy_task_ids.iter().copied().map(Some)))
                .order(stored::id.asc())
                .select((
                    stored::entity_id,
                    stored::initiator_user_id,
                    stored::actor_user_id,
                ))
                .load::<(Option<i32>, Option<i32>, Option<i32>)>(conn)
                .await
        })
        .await?;
        let queued_initiators = queued
            .into_iter()
            .filter_map(|(task_id, initiator_user_id, actor_user_id)| {
                task_id.map(|task_id| (task_id, initiator_user_id.or(actor_user_id)))
            })
            .collect::<HashMap<_, _>>();
        for record in &mut records {
            if record.initiator_user_id.is_none() {
                record.initiator_user_id =
                    queued_initiators.get(&record.task_id).copied().flatten();
            }
        }
    }

    Ok(records)
}

pub async fn list_export_task_output_summaries(
    pool: &impl crate::storage::StorageContext,
    task_ids: &[i32],
) -> Result<Vec<ExportTaskOutputSummaryRecord>, ApiError> {
    use crate::schema::export_task_outputs::dsl::{export_task_outputs, task_id};

    if task_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Return expired-but-present rows too; the caller classifies each against `now` so the
    // `output_expired` flag is consistent with the single-task lookups rather than silently
    // collapsing expired rows into "no output" on the task-list endpoint.
    with_connection(pool, async |conn| {
        export_task_outputs
            .filter(task_id.eq_any(task_ids))
            .select(ExportTaskOutputSummaryRecord::as_select())
            .load(conn)
            .await
    })
    .await
}

pub async fn list_backup_task_output_summaries(
    pool: &impl crate::storage::StorageContext,
    task_ids: &[i32],
) -> Result<Vec<BackupTaskOutputSummaryRecord>, ApiError> {
    use crate::schema::backup_task_outputs::dsl::{backup_task_outputs, task_id};

    if task_ids.is_empty() {
        return Ok(Vec::new());
    }

    with_connection(pool, async |conn| {
        backup_task_outputs
            .filter(task_id.eq_any(task_ids))
            .select(BackupTaskOutputSummaryRecord::as_select())
            .load(conn)
            .await
    })
    .await
}

pub async fn purge_expired_export_outputs(
    pool: &impl crate::storage::StorageContext,
) -> Result<Vec<i32>, ApiError> {
    use crate::schema::export_task_outputs::dsl::{
        export_task_outputs, output_expires_at, task_id,
    };

    let now = Utc::now().naive_utc();
    let expired_task_ids = with_transaction(pool, async |conn| {
        let expired_task_ids =
            diesel::delete(export_task_outputs.filter(output_expires_at.le(now)))
                .returning(task_id)
                .get_results::<i32>(conn)
                .await?;

        if !expired_task_ids.is_empty() {
            use crate::schema::tasks::dsl as task_rows;
            let expired_tasks = task_rows::tasks
                .filter(task_rows::id.eq_any(&expired_task_ids))
                .load::<TaskRecord>(conn)
                .await?;
            for task in &expired_tasks {
                emit_task_lifecycle_event(
                    conn,
                    task,
                    &NewTaskEventRecord {
                        task_id: task.id,
                        event_type: "cleanup".to_string(),
                        message: "Stored export output expired and was cleaned up".to_string(),
                        data: Some(serde_json::json!({
                            "cleaned_at": now,
                        })),
                    },
                    &task.system_provenance(),
                )
                .await?;
            }
        }

        Ok::<_, ApiError>(expired_task_ids)
    })
    .await?;

    if !expired_task_ids.is_empty() {
        info!(
            message = "Expired export outputs cleaned up",
            cleaned_count = expired_task_ids.len(),
            retention_hours = get_config()
                .map(|config| config.export_output_retention_hours)
                .unwrap_or(168)
        );
    }

    Ok(expired_task_ids)
}

pub async fn purge_expired_backup_outputs(
    pool: &impl crate::storage::StorageContext,
) -> Result<Vec<i32>, ApiError> {
    use crate::schema::backup_task_outputs::dsl::{
        backup_task_outputs, output_expires_at, task_id,
    };

    let now = Utc::now().naive_utc();
    with_transaction(pool, async |conn| -> Result<Vec<i32>, ApiError> {
        let expired_task_ids =
            diesel::delete(backup_task_outputs.filter(output_expires_at.le(now)))
                .returning(task_id)
                .get_results::<i32>(conn)
                .await?;
        use crate::schema::tasks::dsl as task_rows;
        let expired_tasks = task_rows::tasks
            .filter(task_rows::id.eq_any(&expired_task_ids))
            .load::<TaskRecord>(conn)
            .await?;
        for task in &expired_tasks {
            emit_task_lifecycle_event(
                conn,
                task,
                &NewTaskEventRecord {
                    task_id: task.id,
                    event_type: "cleanup".to_string(),
                    message: "Stored backup output expired and was cleaned up".to_string(),
                    data: Some(serde_json::json!({ "cleaned_at": now })),
                },
                &task.system_provenance(),
            )
            .await?;
        }
        Ok(expired_task_ids)
    })
    .await
}

fn decode_task_event_cursor_id(query_options: &QueryOptions) -> Result<Option<i64>, ApiError> {
    let Some(cursor) = &query_options.cursor else {
        return Ok(None);
    };

    let values = decode_cursor_values(cursor, &query_options.sort)?;
    match values.as_slice() {
        [CursorValue::Integer(value)] => Ok(Some(*value)),
        _ => Err(ApiError::BadRequest(
            "task history cursor does not match the current sort order".to_string(),
        )),
    }
}

fn decode_int_history_cursor_id(query_options: &QueryOptions) -> Result<Option<i32>, ApiError> {
    let Some(cursor) = &query_options.cursor else {
        return Ok(None);
    };

    let values = decode_cursor_values(cursor, &query_options.sort)?;
    match values.as_slice() {
        [CursorValue::Integer(value)] => i32::try_from(*value)
            .map(Some)
            .map_err(|_| ApiError::BadRequest("cursor id is out of range".to_string())),
        _ => Err(ApiError::BadRequest(
            "task history cursor does not match the current sort order".to_string(),
        )),
    }
}

fn task_event_action(event_type: &str) -> Result<Action, ApiError> {
    Action::from_db(event_type).map_err(|_| {
        ApiError::InternalServerError(format!("Unknown task event type '{event_type}'"))
    })
}

fn task_lifecycle_event(
    task: &TaskRecord,
    event: &NewTaskEventRecord,
    provenance: &MutationProvenance,
) -> Result<NewEvent, ApiError> {
    let mut metadata = serde_json::json!({
        "task_id": task.id,
        "task_kind": task.kind,
    });
    if let Some(data) = &event.data {
        metadata["data"] = data.clone();
    }

    Ok(NewEvent::new(
        EntityType::Task,
        task_event_action(&event.event_type)?,
        provenance.actor_kind(),
        event.message.clone(),
    )?
    .with_entity_id(task.id)
    .with_metadata(metadata)
    .with_mutation_provenance(provenance))
}

async fn emit_task_lifecycle_event(
    conn: &mut crate::storage::postgres::PostgresConnection,
    task: &TaskRecord,
    event: &NewTaskEventRecord,
    provenance: &MutationProvenance,
) -> Result<Event, ApiError> {
    if event.task_id != task.id {
        return Err(ApiError::InternalServerError(format!(
            "Task lifecycle event id {} does not match loaded task {}",
            event.task_id, task.id
        )));
    }
    let lifecycle_event = task_lifecycle_event(task, event, provenance)?;
    emit_event(conn, &lifecycle_event)
        .await
        .map_err(ApiError::from)
}

impl NewTaskEventRecord {
    /// Append this event to its task's history and return the persisted event.
    pub async fn append(
        self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<TaskEventRecord, ApiError> {
        with_connection(pool, async |conn| -> Result<TaskEventRecord, ApiError> {
            use crate::schema::tasks::dsl::{id, tasks};
            let task = tasks
                .filter(id.eq(self.task_id))
                .first::<TaskRecord>(conn)
                .await?;
            emit_task_lifecycle_event(conn, &task, &self, &task.worker_provenance())
                .await?
                .try_into()
        })
        .await
    }
}

/// Append an in-flight lifecycle event only while the supplied worker still
/// owns the task's live lease.
pub(crate) async fn append_task_event_while_claimed(
    pool: &impl crate::storage::StorageContext,
    task_id_value: i32,
    claim_token: Uuid,
    event: NewTaskEventRecord,
) -> Result<TaskEventRecord, ApiError> {
    with_transaction(pool, async |conn| -> Result<TaskEventRecord, ApiError> {
        use crate::schema::tasks::dsl::{id, lease_expires_at, lease_token, status, tasks};

        let active_statuses = TaskStatus::ACTIVE.map(TaskStatus::as_str);
        let task = tasks
            .filter(id.eq(task_id_value))
            .filter(lease_token.eq(Some(claim_token)))
            .filter(lease_expires_at.gt(sql::<Nullable<Timestamp>>(DATABASE_UTC_NOW_SQL)))
            .filter(status.eq_any(active_statuses))
            .for_update()
            .first::<TaskRecord>(conn)
            .await?;
        emit_task_lifecycle_event(conn, &task, &event, &task.worker_provenance())
            .await?
            .try_into()
    })
    .await
}

pub async fn insert_import_results(
    pool: &impl crate::storage::StorageContext,
    entries: &[NewImportTaskResultRecord],
) -> Result<usize, ApiError> {
    use crate::schema::import_task_results::dsl::import_task_results;

    if entries.is_empty() {
        return Ok(0);
    }

    with_connection(pool, async |conn| {
        diesel::insert_into(import_task_results)
            .values(entries)
            .execute(conn)
            .await
    })
    .await
}

pub(crate) fn executable_task_kind_values() -> [&'static str; TaskKind::ALL.len()] {
    TaskKind::ALL.map(TaskKind::as_str)
}

static NEXT_TASK_KIND: AtomicUsize = AtomicUsize::new(0);

const CLAIM_NEXT_QUEUED_TASK_SQL: &str = "\
    SELECT candidate.id \
    FROM unnest($2::text[]) WITH ORDINALITY AS claim_order(kind, priority) \
    CROSS JOIN LATERAL ( \
        SELECT id \
        FROM tasks \
        WHERE status = $1 \
          AND tasks.kind = claim_order.kind \
        ORDER BY created_at ASC \
        FOR UPDATE SKIP LOCKED \
        LIMIT 1 \
    ) AS candidate \
    ORDER BY claim_order.priority \
    LIMIT 1";

#[derive(QueryableByName)]
struct ClaimableTaskId {
    #[diesel(sql_type = Integer)]
    id: i32,
}

fn task_kind_claim_order(start: usize) -> [&'static str; TaskKind::ALL.len()] {
    let kinds = executable_task_kind_values();
    std::array::from_fn(|offset| kinds[(start + offset) % kinds.len()])
}

pub(crate) async fn claim_next_queued_task(
    pool: &impl crate::storage::StorageContext,
    lease_duration: TaskLeaseDuration,
) -> Result<Option<TaskRecord>, ApiError> {
    use crate::schema::tasks::dsl::{
        attempt_count, id, lease_expires_at, lease_token, started_at, status, tasks, updated_at,
    };

    let record = with_transaction(pool, async |conn| -> Result<Option<TaskRecord>, ApiError> {
        if !maintenance_state_conn(conn).await?.is_normal() {
            return Ok(None);
        }

        let task_kinds = executable_task_kind_values();
        let first_kind = NEXT_TASK_KIND.fetch_add(1, Ordering::Relaxed) % task_kinds.len();
        let claim_order = task_kind_claim_order(first_kind);
        let selected_task_id = diesel::sql_query(CLAIM_NEXT_QUEUED_TASK_SQL)
            .bind::<Text, _>(TaskStatus::Queued.as_str())
            .bind::<Array<Text>, _>(claim_order.to_vec())
            .get_result::<ClaimableTaskId>(conn)
            .await
            .optional()?
            .map(|row| row.id);
        let Some(task_id_value) = selected_task_id else {
            return Ok(None);
        };

        let claim_token = Uuid::new_v4();
        let lease_milliseconds = lease_duration.database_milliseconds();
        let record = diesel::update(tasks.filter(id.eq(task_id_value)))
            .set((
                status.eq(TaskStatus::Validating.as_str()),
                started_at.eq(sql::<Nullable<Timestamp>>(DATABASE_UTC_NOW_SQL)),
                lease_token.eq(Some(claim_token)),
                lease_expires_at.eq(sql::<Nullable<Timestamp>>(
                    DATABASE_UTC_LEASE_EXPIRY_SQL_PREFIX,
                )
                .bind::<BigInt, _>(lease_milliseconds)
                .sql(DATABASE_LEASE_EXPIRY_SQL_SUFFIX)),
                attempt_count.eq(attempt_count + 1),
                updated_at.eq(sql::<Timestamp>(DATABASE_UTC_NOW_SQL)),
            ))
            .get_result::<TaskRecord>(conn)
            .await?;

        emit_task_lifecycle_event(
            conn,
            &record,
            &NewTaskEventRecord {
                task_id: record.id,
                event_type: "validating".to_string(),
                message: "Task claimed for validation".to_string(),
                data: None,
            },
            &record.worker_provenance(),
        )
        .await?;

        Ok(Some(record))
    })
    .await?;

    if let Some(record) = &record {
        info!(
            message = "Task claimed for validation",
            task_id = record.id,
            task_kind = record.kind.as_str(),
            previous_status = TaskStatus::Queued.as_str(),
            status = record.status.as_str(),
            submitted_by = ?record.submitted_by,
            total_items = record.total_items
        );
    }

    Ok(record)
}

/// Claim one known task for adapter integration tests that exercise a backend
/// workflow directly without running the process-global worker.
#[cfg(feature = "integration-test-support")]
#[doc(hidden)]
pub async fn claim_task_for_backend_test(
    pool: &impl crate::storage::StorageContext,
    task_id_value: i32,
) -> Result<TaskRecord, ApiError> {
    use crate::schema::tasks::dsl::{
        attempt_count, id, lease_expires_at, lease_token, started_at, status, tasks, updated_at,
    };

    with_connection(pool, async |conn| {
        diesel::update(
            tasks
                .filter(id.eq(task_id_value))
                .filter(status.eq(TaskStatus::Queued.as_str())),
        )
        .set((
            status.eq(TaskStatus::Validating.as_str()),
            started_at.eq(sql::<Nullable<Timestamp>>(DATABASE_UTC_NOW_SQL)),
            lease_token.eq(Some(Uuid::new_v4())),
            lease_expires_at.eq(sql::<Nullable<Timestamp>>(
                "((clock_timestamp() AT TIME ZONE 'UTC') + INTERVAL '1 minute')",
            )),
            attempt_count.eq(attempt_count + 1),
            updated_at.eq(sql::<Timestamp>(DATABASE_UTC_NOW_SQL)),
        ))
        .get_result::<TaskRecord>(conn)
        .await
    })
    .await
}

/// Extend an active task lease if this worker still owns it.
pub(crate) async fn renew_task_lease(
    pool: &impl crate::storage::StorageContext,
    task_id_value: i32,
    claim_token: Uuid,
    lease_duration: TaskLeaseDuration,
) -> Result<bool, ApiError> {
    use crate::schema::tasks::dsl::{id, lease_expires_at, lease_token, status, tasks, updated_at};

    let active_statuses = TaskStatus::ACTIVE.map(TaskStatus::as_str);
    let updated = with_connection(pool, async |conn| -> Result<usize, ApiError> {
        let lease_milliseconds = lease_duration.database_milliseconds();
        diesel::update(
            tasks
                .filter(id.eq(task_id_value))
                .filter(lease_token.eq(Some(claim_token)))
                .filter(lease_expires_at.gt(sql::<Nullable<Timestamp>>(DATABASE_UTC_NOW_SQL)))
                .filter(status.eq_any(active_statuses)),
        )
        .set((
            lease_expires_at.eq(
                sql::<Nullable<Timestamp>>(DATABASE_UTC_LEASE_EXPIRY_SQL_PREFIX)
                    .bind::<BigInt, _>(lease_milliseconds)
                    .sql(DATABASE_LEASE_EXPIRY_SQL_SUFFIX),
            ),
            updated_at.eq(sql::<Timestamp>(DATABASE_UTC_NOW_SQL)),
        ))
        .execute(conn)
        .await
        .map_err(ApiError::from)
    })
    .await?;

    Ok(updated == 1)
}

/// Recover tasks whose owning process stopped renewing its durable lease.
///
/// Recovery is deliberately terminal rather than an automatic retry. Import and
/// remote-call tasks can have external side effects, so replaying them without an
/// operator first inspecting the task history could duplicate those effects.
pub async fn recover_expired_task_leases(
    pool: &impl crate::storage::StorageContext,
    batch_size: i64,
) -> Result<Vec<TaskRecord>, ApiError> {
    recover_expired_task_leases_matching(pool, batch_size, None).await
}

async fn recovered_task_result_counts(
    conn: &mut crate::storage::postgres::PostgresConnection,
    task: &TaskRecord,
) -> Result<TaskResultCounts, ApiError> {
    match TaskKind::from_db(&task.kind)? {
        TaskKind::Import => {
            use crate::schema::import_task_results::dsl::{
                import_task_results, outcome as result_outcome, task_id as result_task_id,
            };

            let processed = import_task_results
                .filter(result_task_id.eq(task.id))
                .count()
                .get_result::<i64>(conn)
                .await?;
            let failed = import_task_results
                .filter(result_task_id.eq(task.id))
                .filter(result_outcome.eq("failed"))
                .count()
                .get_result::<i64>(conn)
                .await?;
            TaskResultCounts::from_outcomes(processed - failed, failed)
        }
        TaskKind::Export | TaskKind::Backup | TaskKind::RemoteCall => {
            TaskResultCounts::from_outcomes(0, 1)
        }
        TaskKind::Reindex => TaskResultCounts::from_stored(
            task.processed_items,
            task.success_items,
            task.failed_items,
        ),
    }
}

async fn recover_expired_task_leases_matching(
    pool: &impl crate::storage::StorageContext,
    batch_size: i64,
    task_id_filter: Option<i32>,
) -> Result<Vec<TaskRecord>, ApiError> {
    use crate::schema::tasks::dsl::{
        deleted_at, failed_items, finished_at, id, lease_expires_at, lease_token, processed_items,
        request_payload, request_redacted_at, status, success_items, summary, tasks, updated_at,
    };

    let active_statuses = TaskStatus::ACTIVE.map(TaskStatus::as_str);
    let recovered = with_transaction(pool, async |conn| -> Result<Vec<TaskRecord>, ApiError> {
        let now = database_now(conn).await?;
        let stale_tasks = if let Some(task_id_filter) = task_id_filter {
            tasks
                .filter(status.eq_any(active_statuses))
                .filter(deleted_at.is_null())
                .filter(lease_expires_at.is_null().or(lease_expires_at.le(now)))
                .filter(id.eq(task_id_filter))
                .order(id.asc())
                .limit(batch_size)
                .for_update()
                .skip_locked()
                .load::<TaskRecord>(conn)
                .await?
        } else {
            tasks
                .filter(status.eq_any(active_statuses))
                .filter(deleted_at.is_null())
                .filter(lease_expires_at.is_null().or(lease_expires_at.le(now)))
                .order(id.asc())
                .limit(batch_size)
                .for_update()
                .skip_locked()
                .load::<TaskRecord>(conn)
                .await?
        };

        let mut recovered = Vec::with_capacity(stale_tasks.len());
        for stale_task in stale_tasks {
            let previous_status = stale_task.status.clone();
            let message = "Task worker lease expired; task failed without automatic replay";
            let counts = recovered_task_result_counts(conn, &stale_task).await?;
            if TaskKind::from_db(&stale_task.kind)? == TaskKind::Reindex {
                crate::storage::postgres::operations::computed_field::mark_recovered_computed_reindex_failed(
                    conn,
                    stale_task.id,
                    message,
                )
                .await?;
            }
            emit_task_lifecycle_event(
                conn,
                &stale_task,
                &NewTaskEventRecord {
                    task_id: stale_task.id,
                    event_type: TaskStatus::Failed.as_str().to_string(),
                    message: message.to_string(),
                    data: Some(serde_json::json!({
                        "previous_status": previous_status,
                        "lease_expires_at": stale_task.lease_expires_at,
                        "attempt_count": stale_task.attempt_count,
                        "operator_action": "inspect task history and submit a new task if replay is safe",
                    })),
                },
                &stale_task.system_provenance(),
            )
            .await?;

            let record = diesel::update(tasks.filter(id.eq(stale_task.id)))
                .set((
                    status.eq(TaskStatus::Failed.as_str()),
                    summary.eq(Some(message.to_string())),
                    processed_items.eq(counts.processed()),
                    success_items.eq(counts.success()),
                    failed_items.eq(counts.failed()),
                    finished_at.eq(Some(now)),
                    request_payload.eq::<Option<serde_json::Value>>(None),
                    request_redacted_at.eq(Some(now)),
                    lease_token.eq::<Option<Uuid>>(None),
                    lease_expires_at.eq::<Option<chrono::NaiveDateTime>>(None),
                    updated_at.eq(now),
                ))
                .get_result::<TaskRecord>(conn)
                .await?;
            recovered.push(record);
        }

        Ok(recovered)
    })
    .await?;

    for record in &recovered {
        record_task_completion_metrics(record);
    }
    Ok(recovered)
}

#[cfg(test)]
async fn recover_expired_task_lease(
    pool: &impl crate::storage::StorageContext,
    task_id: i32,
) -> Result<Vec<TaskRecord>, ApiError> {
    recover_expired_task_leases_matching(pool, 1, Some(task_id)).await
}

impl TaskCreateRequest {
    /// Return an existing task for an identical idempotent submission or create a
    /// new one under the per-user active-task limit. The post-conflict lookup
    /// closes the race between concurrent requests carrying the same key.
    pub async fn create_idempotently_with_active_limit(
        self,
        pool: &impl crate::storage::StorageContext,
        max_active_tasks: usize,
    ) -> Result<TaskRecord, ApiError> {
        let kind = self.kind;
        let submitted_by = self.submitted_by;
        let idempotency_key = self.idempotency_key.clone();
        let request_hash = self.request_hash.clone();
        let matches_request =
            |task: &TaskRecord| task.kind == kind.as_str() && task.request_hash == request_hash;

        if let Some(key) = idempotency_key.as_ref()
            && let Some(existing) =
                TaskRecord::find_by_idempotency(pool, submitted_by, key.as_str()).await?
        {
            if matches_request(&existing) {
                return Ok(existing);
            }
            return Err(ApiError::Conflict(format!(
                "Idempotency-Key '{key}' is already in use for a different task submission"
            )));
        }

        match self
            .create_with_active_kind_limit(pool, kind, max_active_tasks)
            .await
        {
            Ok(task) => Ok(task),
            Err(ApiError::Conflict(_)) => {
                if let Some(key) = idempotency_key.as_ref()
                    && let Some(existing) =
                        TaskRecord::find_by_idempotency(pool, submitted_by, key.as_str()).await?
                    && matches_request(&existing)
                {
                    return Ok(existing);
                }
                Err(ApiError::Conflict(
                    "Idempotency-Key is already in use for a different task submission".to_string(),
                ))
            }
            Err(error) => Err(error),
        }
    }

    async fn create_with_active_kind_limit(
        self,
        pool: &impl crate::storage::StorageContext,
        limited_kind: TaskKind,
        max_active_tasks: usize,
    ) -> Result<TaskRecord, ApiError> {
        if self.kind != limited_kind {
            return Err(ApiError::BadRequest(format!(
                "active task limit only accepts {} tasks",
                limited_kind.as_str()
            )));
        }

        let max_active_tasks = i64::try_from(max_active_tasks).unwrap_or(i64::MAX);
        let submitter = self.submitted_by;
        let task = with_transaction(pool, async |conn| -> Result<TaskRecord, ApiError> {
            acquire_task_capacity_lock(conn, submitter, limited_kind).await?;
            let active_count =
                count_active_tasks_for_user_in_transaction(conn, submitter, limited_kind).await?;
            if active_count >= max_active_tasks {
                return Err(ApiError::TooManyRequests(format!(
                    "Too many active {} tasks for user ({active_count} >= {max_active_tasks}); wait for queued or running tasks to finish",
                    limited_kind.as_str()
                )));
            }

            insert_queued_task_with_event(conn, self).await
        }).await?;

        log_task_queued(&task);

        Ok(task)
    }
}

async fn insert_queued_task_with_event(
    conn: &mut crate::storage::postgres::PostgresConnection,
    request: TaskCreateRequest,
) -> Result<TaskRecord, ApiError> {
    use crate::schema::tasks::dsl::{initiator_user_id, tasks};

    let submitter = request.submitted_by;
    let submitted_by = submitter.id();
    let task_kind = request.kind;
    let task = diesel::insert_into(tasks)
        .values((
            NewTaskRecord {
                kind: task_kind.as_str().to_string(),
                status: TaskStatus::Queued.as_str().to_string(),
                submitted_by: Some(submitted_by),
                idempotency_key: request.idempotency_key.map(IdempotencyKey::into_inner),
                request_hash: request.request_hash,
                request_payload: Some(request.request_payload),
                summary: None,
                total_items: request.total_items,
                processed_items: 0,
                success_items: 0,
                failed_items: 0,
                submitted_token_id: request.submitted_token_id,
                submitted_token_scoped: request.submitted_token_scoped,
                submitted_token_scopes: request.submitted_token_scopes,
                request_redacted_at: None,
                started_at: None,
                finished_at: None,
            },
            initiator_user_id.eq(Some(submitted_by)),
        ))
        .get_result::<TaskRecord>(conn)
        .await?;

    emit_task_lifecycle_event(
        conn,
        &task,
        &NewTaskEventRecord {
            task_id: task.id,
            event_type: "queued".to_string(),
            message: "Task queued".to_string(),
            data: None,
        },
        &task.user_provenance(submitter),
    )
    .await?;
    notify_task_queue(conn, task.id).await?;

    Ok(task)
}

/// Insert an internal task as part of a caller-owned transaction.
///
/// Internal maintenance tasks may outlive the principal that triggered them,
/// so execution cannot depend on reloading or reauthorizing that principal.
pub(crate) async fn insert_internal_queued_task(
    conn: &mut crate::storage::postgres::PostgresConnection,
    kind: TaskKind,
    payload: serde_json::Value,
    total_items_value: i32,
    submitted_by_value: Option<i32>,
) -> Result<TaskRecord, ApiError> {
    use crate::schema::tasks::dsl::{initiator_user_id, tasks};

    let task = diesel::insert_into(tasks)
        .values((
            NewTaskRecord {
                kind: kind.as_str().to_string(),
                status: TaskStatus::Queued.as_str().to_string(),
                submitted_by: submitted_by_value,
                idempotency_key: None,
                request_hash: None,
                request_payload: Some(payload),
                summary: None,
                total_items: total_items_value,
                processed_items: 0,
                success_items: 0,
                failed_items: 0,
                submitted_token_id: None,
                submitted_token_scoped: false,
                submitted_token_scopes: serde_json::json!([]),
                request_redacted_at: None,
                started_at: None,
                finished_at: None,
            },
            initiator_user_id.eq(submitted_by_value),
        ))
        .get_result::<TaskRecord>(conn)
        .await?;

    let provenance = if let Some(submitted_by) = submitted_by_value {
        task.user_provenance(PrincipalID::new(submitted_by)?)
    } else {
        task.system_provenance()
    };
    emit_task_lifecycle_event(
        conn,
        &task,
        &NewTaskEventRecord {
            task_id: task.id,
            event_type: TaskStatus::Queued.as_str().to_string(),
            message: "Internal task queued".to_string(),
            data: None,
        },
        &provenance,
    )
    .await?;
    notify_task_queue(conn, task.id).await?;

    Ok(task)
}

async fn acquire_task_capacity_lock(
    conn: &mut crate::storage::postgres::PostgresConnection,
    submitted_by: PrincipalID,
    kind: TaskKind,
) -> Result<(), ApiError> {
    let lock_key = task_capacity_lock_key(submitted_by, kind);
    let lock = diesel::sql_query("SELECT TRUE AS locked FROM pg_advisory_xact_lock($1)")
        .bind::<BigInt, _>(lock_key)
        .get_result::<AdvisoryLockRow>(conn)
        .await?;
    if !lock.locked {
        return Err(ApiError::InternalServerError(
            "Failed to acquire task capacity lock".to_string(),
        ));
    }

    Ok(())
}

fn task_capacity_lock_key(submitted_by: PrincipalID, kind: TaskKind) -> i64 {
    const BASE_KEY: i64 = 4_801_000_000_000_i64;
    const KIND_STRIDE: i64 = 1_i64 << 32;

    let kind_slot = match kind {
        TaskKind::Export => 1_i64,
        TaskKind::RemoteCall => 2_i64,
        TaskKind::Backup => 3_i64,
        TaskKind::Import | TaskKind::Reindex => 9_i64,
    };
    BASE_KEY + (kind_slot * KIND_STRIDE) + i64::from(submitted_by.id())
}

async fn count_active_tasks_for_user_in_transaction(
    conn: &mut crate::storage::postgres::PostgresConnection,
    submitter: PrincipalID,
    task_kind: TaskKind,
) -> Result<i64, ApiError> {
    use crate::schema::tasks::dsl::{deleted_at, kind, status, submitted_by, tasks};

    let active_statuses = TaskStatus::NON_TERMINAL.map(TaskStatus::as_str);

    tasks
        .filter(kind.eq(task_kind.as_str()))
        .filter(submitted_by.eq(Some(submitter.id())))
        .filter(status.eq_any(active_statuses))
        .filter(deleted_at.is_null())
        .count()
        .get_result::<i64>(conn)
        .await
        .map_err(ApiError::from)
}

fn log_task_queued(task: &TaskRecord) {
    info!(
        message = "Task queued",
        task_id = task.id,
        task_kind = task.kind.as_str(),
        status = task.status.as_str(),
        submitted_by = ?task.submitted_by,
        total_items = task.total_items,
        idempotency_key_present = task.idempotency_key.is_some()
    );
}

#[cfg(test)]
mod tests {
    use crate::storage::postgres::prelude::*;
    use chrono::{Duration as ChronoDuration, Utc};
    use hubuum_task_core::IdempotencyKey;
    use rstest::rstest;
    use tokio::sync::oneshot;
    use uuid::Uuid;

    use super::{
        CLAIM_NEXT_QUEUED_TASK_SQL, TaskBackend, TaskCreateRequest, TaskScopeSnapshot,
        TaskStateUpdate, claim_next_queued_task, database_now, insert_import_results,
        insert_internal_queued_task, recover_expired_task_lease, renew_task_lease,
        task_capacity_lock_key, task_event_responses, task_kind_claim_order,
    };
    use crate::errors::ApiError;
    use crate::events::{Action, ActorKind, EntityType, NewEvent, emit_event};
    use crate::models::search::QueryOptions;
    use crate::models::{
        CollectionID, NewBackupTaskOutputRecord, NewImportTaskResultRecord, NewRemoteCallResult,
        NewTaskEventRecord, NewTaskRecord, Permissions, PrincipalID, RemoteInvocationBodyOverride,
        RemoteInvocationParameters, RemoteInvocationSubject, RemoteTargetID,
        StoredRemoteCallTaskPayload, TaskID, TaskKind, TaskResultCounts, TaskStatus, TokenID,
        TokenScope,
    };
    use crate::storage::postgres::operations::user::DeleteUserRecord;
    use crate::storage::postgres::{capture_queries, with_connection, with_transaction};
    use crate::tasks::TaskLeaseDuration;
    use crate::tests::{TestContext, create_test_user};

    fn test_lease_duration() -> TaskLeaseDuration {
        TaskLeaseDuration::new(std::time::Duration::from_secs(60)).unwrap()
    }

    #[derive(QueryableByName)]
    struct TaskCapacityIndex {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        valid: bool,
        #[diesel(sql_type = diesel::sql_types::Text)]
        definition: String,
    }

    #[test]
    fn task_request_builder_derives_unscoped_persistence_fields() {
        let request = TaskCreateRequest::builder(
            TaskKind::Export,
            PrincipalID::new(7).unwrap(),
            serde_json::json!({}),
            1,
        )
        .build();

        assert_eq!(request.submitted_token_id, None);
        assert!(!request.submitted_token_scoped);
        assert_eq!(request.submitted_token_scopes, serde_json::json!([]));
    }

    #[test]
    fn task_request_builder_derives_scoped_persistence_fields() {
        let scope =
            TokenScope::from_stored_parts(Some(vec![Permissions::ReadObject]), None).unwrap();
        let request = TaskCreateRequest::builder(
            TaskKind::Export,
            PrincipalID::new(7).unwrap(),
            serde_json::json!({}),
            1,
        )
        .scope_snapshot(TaskScopeSnapshot::from_request(
            Some(TokenID::new(11).unwrap()),
            Some(&scope),
        ))
        .build();

        assert_eq!(request.submitted_token_id, Some(11));
        assert!(request.submitted_token_scoped);
        assert_eq!(request.submitted_token_scopes, scope.snapshot_json());
    }

    #[tokio::test]
    async fn database_time_is_naive_utc_under_non_utc_session_timezone() {
        let context = TestContext::new().await;
        let now = with_transaction(
            &context.pool,
            async |conn| -> Result<chrono::NaiveDateTime, ApiError> {
                diesel::sql_query("SET LOCAL TIME ZONE 'Pacific/Honolulu'")
                    .execute(conn)
                    .await?;
                database_now(conn).await
            },
        )
        .await
        .unwrap();

        let skew = (Utc::now().naive_utc() - now).num_seconds().abs();
        assert!(skew < 5, "database UTC clock skew was {skew} seconds");
    }

    #[tokio::test]
    async fn legacy_task_insert_copies_submitter_to_initiator() {
        let context = TestContext::new().await;
        let task = NewTaskRecord {
            kind: TaskKind::Import.as_str().to_string(),
            status: TaskStatus::Queued.as_str().to_string(),
            submitted_by: Some(context.admin_user.id),
            idempotency_key: Some(context.scoped_name("legacy-task-insert")),
            request_hash: None,
            request_payload: Some(serde_json::json!({"items": []})),
            summary: None,
            total_items: 0,
            processed_items: 0,
            success_items: 0,
            failed_items: 0,
            submitted_token_id: None,
            submitted_token_scoped: false,
            submitted_token_scopes: serde_json::json!([]),
            request_redacted_at: None,
            started_at: None,
            finished_at: None,
        }
        .create(&context.pool)
        .await
        .unwrap();

        assert_eq!(task.initiator_user_id, Some(context.admin_user.id));
    }

    #[tokio::test]
    async fn legacy_task_event_page_uses_bounded_provenance_queries() {
        let context = TestContext::new().await;
        let initiator = create_test_user(&context.pool).await;
        let task_id = (Uuid::new_v4().as_u128() as i32 & (i32::MAX - 1)) + 1;
        let queued = NewEvent::new(
            EntityType::Task,
            Action::Queued,
            ActorKind::User,
            "legacy task queued",
        )
        .unwrap()
        .with_entity_id(task_id)
        .with_actor_user_id(initiator.id);
        let running = NewEvent::new(
            EntityType::Task,
            Action::Running,
            ActorKind::Worker,
            "legacy task running",
        )
        .unwrap()
        .with_entity_id(task_id);
        with_transaction(&context.pool, async |conn| {
            emit_event(conn, &queued).await?;
            emit_event(conn, &running).await?;
            Ok::<_, diesel::result::Error>(())
        })
        .await
        .unwrap();

        let task = TaskID::new(task_id).unwrap();
        let query_options = QueryOptions {
            filters: Vec::new(),
            sort: Vec::new(),
            limit: None,
            cursor: None,
            include_total: false,
        };
        let (result, queries) = capture_queries(async {
            let (records, _) = task
                .list_events_with_total_count(&context.pool, &query_options)
                .await?;
            task_event_responses(&context.pool, records).await
        })
        .await;
        let responses = result.unwrap();

        assert_eq!(responses.len(), 2);
        for response in responses {
            assert_eq!(response.provenance.task_id, Some(task_id));
            assert_eq!(
                response
                    .provenance
                    .initiator
                    .as_ref()
                    .map(|principal| principal.principal_id),
                Some(initiator.id)
            );
        }
        assert_eq!(queries.queries_matching("FROM \"events\""), 2);
        assert_eq!(queries.queries_matching("FROM \"principals\""), 1);
    }

    #[tokio::test]
    async fn system_owned_task_queue_event_has_system_provenance() {
        let context = TestContext::new().await;
        let task = with_transaction(&context.pool, async |conn| -> Result<_, ApiError> {
            let task = insert_internal_queued_task(
                conn,
                TaskKind::Reindex,
                serde_json::json!({}),
                0,
                None,
            )
            .await?;
            use crate::schema::tasks::dsl::{id, tasks};
            diesel::delete(tasks.filter(id.eq(task.id)))
                .execute(conn)
                .await?;
            Ok(task)
        })
        .await
        .unwrap();

        let (events, _) = TaskID::new(task.id)
            .unwrap()
            .list_events_with_total_count(
                &context.pool,
                &QueryOptions {
                    filters: Vec::new(),
                    sort: Vec::new(),
                    limit: None,
                    cursor: None,
                    include_total: false,
                },
            )
            .await
            .unwrap();
        let queued = events
            .iter()
            .find(|event| event.event_type == TaskStatus::Queued.as_str())
            .unwrap();

        assert_eq!(queued.actor_kind, "system");
        assert_eq!(queued.actor_user_id, None);
        assert_eq!(queued.initiator_user_id, None);
        assert_eq!(queued.provenance_task_id, Some(task.id));
    }

    async fn create_leased_task(
        context: &TestContext,
        name: &str,
        lease_expires_at_value: chrono::NaiveDateTime,
    ) -> crate::models::TaskRecord {
        create_leased_task_of_kind(context, name, TaskKind::Import, lease_expires_at_value).await
    }

    async fn create_leased_task_of_kind(
        context: &TestContext,
        name: &str,
        kind: TaskKind,
        lease_expires_at_value: chrono::NaiveDateTime,
    ) -> crate::models::TaskRecord {
        let task = NewTaskRecord {
            kind: kind.as_str().to_string(),
            status: TaskStatus::Validating.as_str().to_string(),
            submitted_by: Some(context.admin_user.id),
            submitted_token_id: None,
            submitted_token_scoped: false,
            submitted_token_scopes: serde_json::json!([]),
            idempotency_key: Some(context.scoped_name(name)),
            request_hash: None,
            request_payload: Some(serde_json::json!({"items": []})),
            summary: None,
            total_items: i32::from(matches!(kind, TaskKind::Export | TaskKind::RemoteCall)),
            processed_items: 0,
            success_items: 0,
            failed_items: 0,
            request_redacted_at: None,
            started_at: Some(Utc::now().naive_utc()),
            finished_at: None,
        }
        .create(&context.pool)
        .await
        .unwrap();
        let claim_token = Uuid::new_v4();
        with_connection(&context.pool, async |conn| {
            use crate::schema::tasks::dsl::{
                attempt_count, id, initiator_user_id, lease_expires_at, lease_token, tasks,
            };

            diesel::update(tasks.filter(id.eq(task.id)))
                .set((
                    lease_token.eq(Some(claim_token)),
                    lease_expires_at.eq(Some(lease_expires_at_value)),
                    attempt_count.eq(1),
                    initiator_user_id.eq(Some(context.admin_user.id)),
                ))
                .get_result::<crate::models::TaskRecord>(conn)
                .await
        })
        .await
        .unwrap()
    }

    #[test]
    fn test_task_capacity_lock_keys_do_not_collide_between_kind_slots() {
        assert_ne!(
            task_capacity_lock_key(PrincipalID::new(1_000_000_000).unwrap(), TaskKind::Export),
            task_capacity_lock_key(PrincipalID::new(1).unwrap(), TaskKind::RemoteCall)
        );

        let user_id = PrincipalID::new(42).unwrap();
        let export_key = task_capacity_lock_key(user_id, TaskKind::Export);
        let remote_call_key = task_capacity_lock_key(user_id, TaskKind::RemoteCall);
        let fallback_key = task_capacity_lock_key(user_id, TaskKind::Import);

        assert_ne!(export_key, remote_call_key);
        assert_ne!(export_key, fallback_key);
        assert_ne!(remote_call_key, fallback_key);
        assert_eq!(
            fallback_key,
            task_capacity_lock_key(user_id, TaskKind::Reindex)
        );
    }

    #[tokio::test]
    async fn active_task_capacity_index_matches_the_admission_query() {
        let context = TestContext::new().await;
        let index = with_connection(&context.pool, async |conn| {
            diesel::sql_query(
                "SELECT pg_index.indisvalid AS valid,
                        pg_get_indexdef(pg_index.indexrelid) AS definition
                 FROM pg_index
                 JOIN pg_class AS index_class
                   ON index_class.oid = pg_index.indexrelid
                 JOIN pg_class AS table_class
                   ON table_class.oid = pg_index.indrelid
                 JOIN pg_namespace
                   ON pg_namespace.oid = table_class.relnamespace
                 WHERE pg_namespace.nspname = 'public'
                   AND table_class.relname = 'tasks'
                   AND index_class.relname = 'idx_tasks_active_capacity'",
            )
            .get_result::<TaskCapacityIndex>(conn)
            .await
        })
        .await
        .unwrap();

        assert!(index.valid, "task capacity index must be valid");
        assert!(
            index.definition.contains("(submitted_by, kind)"),
            "task capacity index has unexpected columns: {}",
            index.definition
        );
        for predicate in [
            "submitted_by IS NOT NULL",
            "deleted_at IS NULL",
            "queued",
            "validating",
            "running",
        ] {
            assert!(
                index.definition.contains(predicate),
                "task capacity index is missing predicate `{predicate}`: {}",
                index.definition
            );
        }
    }

    #[tokio::test]
    async fn task_claim_order_rotates_every_executable_kind_to_the_front() {
        assert_eq!(
            task_kind_claim_order(0),
            [
                TaskKind::Import.as_str(),
                TaskKind::Export.as_str(),
                TaskKind::Backup.as_str(),
                TaskKind::Reindex.as_str(),
                TaskKind::RemoteCall.as_str(),
            ]
        );
        assert_eq!(task_kind_claim_order(1)[0], TaskKind::Export.as_str());
        assert_eq!(task_kind_claim_order(2)[0], TaskKind::Backup.as_str());
        assert_eq!(task_kind_claim_order(3)[0], TaskKind::Reindex.as_str());
        assert_eq!(task_kind_claim_order(4)[0], TaskKind::RemoteCall.as_str());
    }

    #[test]
    fn task_claim_query_limits_each_kind_before_priority_ordering() {
        assert!(CLAIM_NEXT_QUEUED_TASK_SQL.contains("CROSS JOIN LATERAL"));
        assert!(CLAIM_NEXT_QUEUED_TASK_SQL.contains("FOR UPDATE SKIP LOCKED"));
        assert!(!CLAIM_NEXT_QUEUED_TASK_SQL.contains("array_position"));
    }

    #[tokio::test]
    async fn test_claim_next_queued_task_is_safe_under_concurrency() {
        let context = TestContext::new().await;
        let mut created_ids = Vec::new();
        let claim_prefix = context.scoped_name("claim");

        for index in 0..3 {
            let task = NewTaskRecord {
                kind: TaskKind::Import.as_str().to_string(),
                status: TaskStatus::Queued.as_str().to_string(),
                submitted_by: Some(context.admin_user.id),
                submitted_token_id: None,
                submitted_token_scoped: false,
                submitted_token_scopes: serde_json::json!([]),
                idempotency_key: Some(format!("{claim_prefix}-{index}")),
                request_hash: None,
                request_payload: None,
                summary: None,
                total_items: 0,
                processed_items: 0,
                success_items: 0,
                failed_items: 0,
                request_redacted_at: None,
                started_at: None,
                finished_at: None,
            }
            .create(&context.pool)
            .await
            .unwrap();
            created_ids.push(task.id);
        }

        let (locked_tx, locked_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let pool = context.pool.clone();
        let claim_prefix_for_locker = claim_prefix.clone();
        let locker = tokio::spawn(async move {
            use crate::schema::tasks::dsl::{created_at, id, idempotency_key, status, tasks};

            with_transaction(
                &pool,
                async move |conn| -> Result<(), crate::errors::ApiError> {
                    let locked_id = tasks
                        .filter(status.eq(TaskStatus::Queued.as_str()))
                        .filter(idempotency_key.like(format!("{claim_prefix_for_locker}-%")))
                        .order(created_at.asc())
                        .for_update()
                        .select(id)
                        .first::<i32>(conn)
                        .await?;
                    locked_tx.send(locked_id).unwrap();
                    release_rx.await.unwrap();
                    Ok(())
                },
            )
            .await
            .unwrap();
        });

        let locked_id = locked_rx.await.unwrap();
        let (claimed, queries) =
            capture_queries(claim_next_queued_task(&context.pool, test_lease_duration())).await;
        let claimed = claimed.unwrap().map(|task| task.id);
        release_tx.send(()).unwrap();
        locker.await.unwrap();

        assert_eq!(queries.connection_checkouts(), 1);
        assert_eq!(
            queries.queries_matching("SELECT candidate.id FROM unnest($2::text[])"),
            1
        );
        assert!(claimed.is_some());
        assert_ne!(claimed.unwrap(), locked_id);
        assert!(created_ids.contains(&locked_id));

        let claimed_record = TaskID::new(claimed.unwrap())
            .unwrap()
            .find_record(&context.pool)
            .await
            .unwrap();
        assert!(claimed_record.lease_token.is_some());
        assert!(claimed_record.lease_expires_at.is_some());
        assert_eq!(claimed_record.attempt_count, 1);

        let (claimed_events, _) = (TaskID::new(claimed.unwrap())
            .unwrap()
            .list_events_with_total_count(
                &context.pool,
                &QueryOptions {
                    filters: Vec::new(),
                    sort: Vec::new(),
                    limit: None,
                    cursor: None,
                    include_total: true,
                },
            ))
        .await
        .unwrap();
        assert_eq!(
            claimed_events
                .iter()
                .filter(|event| event.event_type == "validating")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn expired_task_lease_is_failed_without_replay() {
        let context = TestContext::new().await;
        let leased = create_leased_task(
            &context,
            "expired-lease",
            Utc::now().naive_utc() - ChronoDuration::seconds(1),
        )
        .await;

        let recovered = recover_expired_task_lease(&context.pool, leased.id)
            .await
            .unwrap();

        assert_eq!(recovered.len(), 1);
        let recovered = &recovered[0];
        assert_eq!(recovered.status, TaskStatus::Failed.as_str());
        assert_eq!(recovered.attempt_count, 1);
        assert!(recovered.lease_token.is_none());
        assert!(recovered.lease_expires_at.is_none());
        assert!(recovered.request_payload.is_none());
        assert!(recovered.finished_at.is_some());
        assert!(
            recovered
                .summary
                .as_deref()
                .is_some_and(|summary| summary.contains("without automatic replay"))
        );
        let (events, _) = TaskID::new(recovered.id)
            .unwrap()
            .list_events_with_total_count(
                &context.pool,
                &QueryOptions {
                    filters: Vec::new(),
                    sort: Vec::new(),
                    limit: None,
                    cursor: None,
                    include_total: false,
                },
            )
            .await
            .unwrap();
        let failed = events
            .iter()
            .find(|event| event.event_type == TaskStatus::Failed.as_str())
            .unwrap();
        assert_eq!(failed.actor_kind, "system");
        assert_eq!(failed.actor_user_id, None);
        assert_eq!(failed.initiator_user_id, Some(context.admin_user.id));
        assert_eq!(failed.provenance_task_id, Some(recovered.id));
    }

    #[tokio::test]
    async fn expired_import_task_recomputes_counts_from_durable_results() {
        let context = TestContext::new().await;
        let leased = create_leased_task(
            &context,
            "expired-import-progress",
            Utc::now().naive_utc() - ChronoDuration::seconds(1),
        )
        .await;
        insert_import_results(
            &context.pool,
            &[
                NewImportTaskResultRecord {
                    task_id: leased.id,
                    item_ref: Some("one".to_string()),
                    entity_kind: "collection".to_string(),
                    action: "create".to_string(),
                    identifier: Some("one".to_string()),
                    outcome: "succeeded".to_string(),
                    error: None,
                    details: None,
                },
                NewImportTaskResultRecord {
                    task_id: leased.id,
                    item_ref: Some("two".to_string()),
                    entity_kind: "class".to_string(),
                    action: "create".to_string(),
                    identifier: Some("two".to_string()),
                    outcome: "failed".to_string(),
                    error: Some("failed".to_string()),
                    details: None,
                },
            ],
        )
        .await
        .unwrap();

        let recovered = recover_expired_task_lease(&context.pool, leased.id)
            .await
            .unwrap();

        assert_eq!(recovered[0].processed_items, 2);
        assert_eq!(recovered[0].success_items, 1);
        assert_eq!(recovered[0].failed_items, 1);
    }

    #[rstest]
    #[case(TaskKind::Export)]
    #[case(TaskKind::Backup)]
    #[case(TaskKind::RemoteCall)]
    #[tokio::test]
    async fn expired_single_item_task_records_terminal_failure(#[case] kind: TaskKind) {
        let context = TestContext::new().await;
        let leased = create_leased_task_of_kind(
            &context,
            &format!("expired-{}-progress", kind.as_str()),
            kind,
            Utc::now().naive_utc() - ChronoDuration::seconds(1),
        )
        .await;

        let recovered = recover_expired_task_lease(&context.pool, leased.id)
            .await
            .unwrap();

        assert_eq!(recovered[0].processed_items, 1);
        assert_eq!(recovered[0].success_items, 0);
        assert_eq!(recovered[0].failed_items, 1);
    }

    #[tokio::test]
    async fn stale_worker_cannot_update_recovered_task() {
        let context = TestContext::new().await;
        let leased = create_leased_task(
            &context,
            "stale-worker-fence",
            Utc::now().naive_utc() - ChronoDuration::seconds(1),
        )
        .await;
        recover_expired_task_lease(&context.pool, leased.id)
            .await
            .unwrap();

        let result = leased
            .update_state(
                &context.pool,
                TaskStateUpdate::new(TaskStatus::Running, TaskResultCounts::default())
                    .with_started_at(leased.started_at),
            )
            .await;

        assert!(result.is_err());
        assert_eq!(
            leased.find_record(&context.pool).await.unwrap().status,
            TaskStatus::Failed.as_str()
        );
    }

    #[tokio::test]
    async fn stale_backup_worker_cannot_finalize_recovered_task() {
        let context = TestContext::new().await;
        let stale_document = b"stale backup".to_vec();
        let leased = create_leased_task_of_kind(
            &context,
            "stale-backup-finalization-fence",
            TaskKind::Backup,
            Utc::now().naive_utc() - ChronoDuration::seconds(1),
        )
        .await;
        recover_expired_task_lease(&context.pool, leased.id)
            .await
            .unwrap();

        let result = leased
            .finalize_backup_with_output(
                &context.pool,
                TaskStateUpdate::new(
                    TaskStatus::Succeeded,
                    TaskResultCounts::from_outcomes(1, 0).unwrap(),
                )
                .with_summary("stale backup completion")
                .with_started_at(leased.started_at),
                NewTaskEventRecord {
                    task_id: leased.id,
                    event_type: TaskStatus::Succeeded.as_str().to_string(),
                    message: "stale backup completion".to_string(),
                    data: None,
                },
                NewBackupTaskOutputRecord {
                    task_id: leased.id,
                    byte_size: i64::try_from(stale_document.len()).unwrap(),
                    document: stale_document,
                    sha256: "0".repeat(64),
                    output_expires_at: Utc::now().naive_utc() + ChronoDuration::hours(1),
                },
            )
            .await;

        let persisted = leased.find_record(&context.pool).await.unwrap();
        let output_count = with_connection(&context.pool, async |conn| {
            use crate::schema::backup_task_outputs::dsl::{backup_task_outputs, task_id};

            backup_task_outputs
                .filter(task_id.eq(leased.id))
                .count()
                .get_result::<i64>(conn)
                .await
        })
        .await
        .unwrap();

        assert_eq!(
            (result.is_err(), persisted.status.as_str(), output_count),
            (true, TaskStatus::Failed.as_str(), 0)
        );
    }

    #[tokio::test]
    async fn stale_remote_call_worker_cannot_persist_a_result() {
        let context = TestContext::new().await;
        let leased = create_leased_task_of_kind(
            &context,
            "stale-remote-call-finalization-fence",
            TaskKind::RemoteCall,
            Utc::now().naive_utc() - ChronoDuration::seconds(1),
        )
        .await;
        recover_expired_task_lease(&context.pool, leased.id)
            .await
            .unwrap();

        let result = leased
            .finalize_remote_call_with_result(
                &context.pool,
                TaskStateUpdate::new(
                    TaskStatus::Succeeded,
                    TaskResultCounts::from_outcomes(1, 0).unwrap(),
                )
                .with_summary("stale remote-call completion")
                .with_started_at(leased.started_at),
                NewTaskEventRecord {
                    task_id: leased.id,
                    event_type: TaskStatus::Succeeded.as_str().to_string(),
                    message: "stale remote-call completion".to_string(),
                    data: None,
                },
                NewRemoteCallResult {
                    task_id: leased.id,
                    target_id: None,
                    subject_type: "collection".to_string(),
                    subject_id: 1,
                    method: "GET".to_string(),
                    rendered_url: "https://compatibility.invalid".to_string(),
                    response_status: Some(200),
                    response_headers: Some(serde_json::json!({})),
                    response_body_preview: Some("stale".to_string()),
                    duration_ms: 1,
                    success: true,
                    error: None,
                },
            )
            .await;

        let persisted = leased.find_record(&context.pool).await.unwrap();
        let result_count = with_connection(&context.pool, async |conn| {
            use crate::schema::remote_call_results::dsl::{remote_call_results, task_id};

            remote_call_results
                .filter(task_id.eq(leased.id))
                .count()
                .get_result::<i64>(conn)
                .await
        })
        .await
        .unwrap();

        assert_eq!(
            (result.is_err(), persisted.status.as_str(), result_count),
            (true, TaskStatus::Failed.as_str(), 0)
        );
    }

    #[tokio::test]
    async fn task_lease_renewal_requires_the_claim_token() {
        let context = TestContext::new().await;
        let leased = create_leased_task(
            &context,
            "lease-renewal-token",
            Utc::now().naive_utc() + ChronoDuration::minutes(1),
        )
        .await;

        assert!(
            !renew_task_lease(
                &context.pool,
                leased.id,
                Uuid::new_v4(),
                test_lease_duration(),
            )
            .await
            .unwrap()
        );
        assert!(
            renew_task_lease(
                &context.pool,
                leased.id,
                leased.lease_token.unwrap(),
                test_lease_duration(),
            )
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn expired_task_lease_cannot_be_renewed() {
        let context = TestContext::new().await;
        let leased = create_leased_task(
            &context,
            "expired-lease-renewal",
            Utc::now().naive_utc() - ChronoDuration::seconds(1),
        )
        .await;

        assert!(
            !renew_task_lease(
                &context.pool,
                leased.id,
                leased.lease_token.unwrap(),
                test_lease_duration(),
            )
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn expired_task_lease_cannot_update_state_before_recovery() {
        let context = TestContext::new().await;
        let leased = create_leased_task(
            &context,
            "expired-lease-state-update",
            Utc::now().naive_utc() - ChronoDuration::seconds(1),
        )
        .await;

        let result = leased
            .update_state(
                &context.pool,
                TaskStateUpdate::new(TaskStatus::Running, TaskResultCounts::default())
                    .with_started_at(leased.started_at),
            )
            .await;

        assert!(result.is_err());
        assert_eq!(
            leased.find_record(&context.pool).await.unwrap().status,
            TaskStatus::Validating.as_str()
        );
    }

    #[tokio::test]
    async fn test_task_history_survives_user_deletion() {
        let context = (TestContext::new()).await;
        let task_owner = (create_test_user(&context.pool)).await;
        let task = TaskCreateRequest::builder(
            TaskKind::Import,
            PrincipalID::new(task_owner.id).unwrap(),
            serde_json::json!({"items": []}),
            0,
        )
        .idempotency_key(Some(
            IdempotencyKey::new(context.scoped_name("deleted-owner-task")).unwrap(),
        ))
        .build()
        .create_idempotently_with_active_limit(&context.pool, 1)
        .await
        .unwrap();

        (task_owner.delete_user_record_without_events(&context.pool))
            .await
            .unwrap();

        let stored = (task.find_record(&context.pool)).await.unwrap();
        assert_eq!(stored.submitted_by, None);
        assert_eq!(stored.initiator_user_id, Some(task_owner.id));

        let (events, _) = task
            .list_events_with_total_count(
                &context.pool,
                &QueryOptions {
                    filters: Vec::new(),
                    sort: Vec::new(),
                    limit: None,
                    cursor: None,
                    include_total: false,
                },
            )
            .await
            .unwrap();
        let responses = task_event_responses(&context.pool, events).await.unwrap();
        let provenance = &responses[0].provenance;
        assert_eq!(
            provenance
                .initiator
                .as_ref()
                .map(|principal| principal.principal_id),
            Some(task_owner.id)
        );
        assert_eq!(
            provenance
                .initiator
                .as_ref()
                .and_then(|principal| principal.name.as_deref()),
            None
        );
    }

    #[tokio::test]
    async fn test_export_task_active_limit_blocks_new_work_for_same_user() {
        let context = (TestContext::new()).await;
        let first = (TaskCreateRequest::builder(
            TaskKind::Export,
            PrincipalID::new(context.admin_user.id).unwrap(),
            serde_json::json!({"export": "first"}),
            1,
        )
        .idempotency_key(Some(
            IdempotencyKey::new(context.scoped_name("export-cap-first")).unwrap(),
        ))
        .request_hash(Some(context.scoped_name("export-cap-first-hash")))
        .build()
        .create_idempotently_with_active_limit(&context.pool, 1))
        .await
        .unwrap();

        assert_eq!(first.status, TaskStatus::Queued.as_str());

        let error = (TaskCreateRequest::builder(
            TaskKind::Export,
            PrincipalID::new(context.admin_user.id).unwrap(),
            serde_json::json!({"export": "second"}),
            1,
        )
        .idempotency_key(Some(
            IdempotencyKey::new(context.scoped_name("export-cap-second")).unwrap(),
        ))
        .request_hash(Some(context.scoped_name("export-cap-second-hash")))
        .build()
        .create_idempotently_with_active_limit(&context.pool, 1))
        .await
        .unwrap_err();

        match error {
            ApiError::TooManyRequests(message) => {
                assert!(message.contains("Too many active export tasks for user"));
            }
            other => panic!("expected TooManyRequests, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_import_task_active_limit_blocks_new_work_for_same_user() {
        let context = (TestContext::new()).await;
        let create_request = |suffix: &str| {
            TaskCreateRequest::builder(
                TaskKind::Import,
                PrincipalID::new(context.admin_user.id).unwrap(),
                serde_json::json!({"import": suffix}),
                1,
            )
            .idempotency_key(Some(
                IdempotencyKey::new(context.scoped_name(&format!("import-cap-{suffix}"))).unwrap(),
            ))
            .request_hash(Some(
                context.scoped_name(&format!("import-cap-{suffix}-hash")),
            ))
            .build()
        };

        let first = (create_request("first")
            .create_idempotently_with_active_limit(&context.pool, 1))
        .await
        .unwrap();
        assert_eq!(first.status, TaskStatus::Queued.as_str());

        let error = (create_request("second")
            .create_idempotently_with_active_limit(&context.pool, 1))
        .await
        .unwrap_err();
        match error {
            ApiError::TooManyRequests(message) => {
                assert!(message.contains("Too many active import tasks for user"));
            }
            other => panic!("expected TooManyRequests, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_remote_call_task_active_limit_blocks_new_work_for_same_user() {
        let context = (TestContext::new()).await;
        let payload = serde_json::to_value(StoredRemoteCallTaskPayload {
            target_id: RemoteTargetID::new(1).unwrap(),
            subject: RemoteInvocationSubject::Collection {
                collection_id: CollectionID::new(1).unwrap(),
            },
            parameters: RemoteInvocationParameters::default(),
            body_override: RemoteInvocationBodyOverride::default(),
        })
        .unwrap();

        let first = (TaskCreateRequest::builder(
            TaskKind::RemoteCall,
            PrincipalID::new(context.admin_user.id).unwrap(),
            payload.clone(),
            1,
        )
        .idempotency_key(Some(
            IdempotencyKey::new(context.scoped_name("remote-cap-first")).unwrap(),
        ))
        .request_hash(Some(context.scoped_name("remote-cap-first-hash")))
        .build()
        .create_idempotently_with_active_limit(&context.pool, 1))
        .await
        .unwrap();

        assert_eq!(first.status, TaskStatus::Queued.as_str());

        let error = (TaskCreateRequest::builder(
            TaskKind::RemoteCall,
            PrincipalID::new(context.admin_user.id).unwrap(),
            payload,
            1,
        )
        .idempotency_key(Some(
            IdempotencyKey::new(context.scoped_name("remote-cap-second")).unwrap(),
        ))
        .request_hash(Some(context.scoped_name("remote-cap-second-hash")))
        .build()
        .create_idempotently_with_active_limit(&context.pool, 1))
        .await
        .unwrap_err();

        match error {
            ApiError::TooManyRequests(message) => {
                assert!(message.contains("Too many active remote_call tasks for user"));
            }
            other => panic!("expected TooManyRequests, got {other:?}"),
        }
    }

    #[rstest]
    #[case(TaskKind::Import)]
    #[case(TaskKind::Export)]
    #[case(TaskKind::Backup)]
    #[case(TaskKind::RemoteCall)]
    #[tokio::test]
    async fn concurrent_active_task_admission_preserves_the_per_kind_limit(#[case] kind: TaskKind) {
        let context = TestContext::new().await;
        let request = |suffix: &str| {
            TaskCreateRequest::builder(
                kind,
                PrincipalID::new(context.admin_user.id).unwrap(),
                serde_json::json!({"kind": kind.as_str(), "case": suffix}),
                1,
            )
            .idempotency_key(Some(
                IdempotencyKey::new(
                    context.scoped_name(&format!("{}-concurrent-cap-{suffix}", kind.as_str())),
                )
                .unwrap(),
            ))
            .request_hash(Some(context.scoped_name(&format!(
                "{}-concurrent-cap-{suffix}-hash",
                kind.as_str()
            ))))
            .build()
        };

        let first = request("first").create_idempotently_with_active_limit(&context.pool, 1);
        let second = request("second").create_idempotently_with_active_limit(&context.pool, 1);
        let (first, second) = tokio::join!(first, second);
        let results = [first, second];

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        let error = results
            .into_iter()
            .find_map(Result::err)
            .expect("one concurrent task submission must exceed the active limit");
        assert!(
            matches!(error, ApiError::TooManyRequests(_)),
            "expected TooManyRequests, got {error:?}"
        );
    }
}
