use crate::db::prelude::*;
use chrono::Utc;
use diesel::dsl::sql;
use diesel::expression::AsExpression;
use diesel::sql_types::{BigInt, Bool, Nullable, Timestamp};
use hubuum_task_core::IdempotencyKey;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::info;
use uuid::Uuid;

use crate::apply_query_options;
use crate::config::get_config;
use crate::db::{DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::events::{Action, ActorKind, EntityType, NewEvent, emit_event};
use crate::models::search::QueryOptions;
use crate::models::{
    BackupOutputLookup, BackupTaskOutputRecord, BackupTaskOutputSummaryRecord, ExportOutputLookup,
    ExportTaskOutputRecord, ExportTaskOutputSummaryRecord, ImportTaskResultRecord,
    NewBackupTaskOutputRecord, NewExportTaskOutputRecord, NewImportTaskResultRecord,
    NewTaskEventRecord, NewTaskRecord, Permissions, TaskEventRecord, TaskID, TaskKind, TaskRecord,
    TaskResponse, TaskResultCounts, TaskStatus,
};
use crate::observability::metrics;
use crate::pagination::{CursorValue, decode_cursor_values, page_limits_or_defaults};

const DATABASE_UTC_NOW_SQL: &str = "clock_timestamp() AT TIME ZONE 'UTC'";
const DATABASE_UTC_NOW_QUERY: &str = "SELECT clock_timestamp() AT TIME ZONE 'UTC' AS now";
const DATABASE_UTC_LEASE_EXPIRY_SQL_PREFIX: &str = "((clock_timestamp() AT TIME ZONE 'UTC') + (";
const DATABASE_LEASE_EXPIRY_SQL_SUFFIX: &str = " * INTERVAL '1 millisecond'))";

pub struct TaskStateUpdate {
    pub status: TaskStatus,
    pub summary: Option<String>,
    pub processed_items: i32,
    pub success_items: i32,
    pub failed_items: i32,
    pub started_at: Option<chrono::NaiveDateTime>,
    pub finished_at: Option<chrono::NaiveDateTime>,
}

pub struct TaskCreateRequest {
    pub kind: TaskKind,
    /// Principal id of the submitter.
    pub submitted_by: i32,
    pub idempotency_key: Option<IdempotencyKey>,
    pub request_hash: Option<String>,
    pub request_payload: serde_json::Value,
    pub total_items: i32,
    /// Scope snapshot of the submitting token (see `TaskRecord`).
    pub submitted_token_id: Option<i32>,
    pub submitted_token_scoped: bool,
    pub submitted_token_scopes: serde_json::Value,
}

/// Encode a token scope set as the persisted snapshot JSON (an array of
/// permission strings; empty array for unscoped or deny-all).
pub fn scope_snapshot_json(scopes: Option<&[Permissions]>) -> serde_json::Value {
    let strings: Vec<String> = scopes
        .map(|s| s.iter().map(|p| p.to_string()).collect())
        .unwrap_or_default();
    serde_json::Value::Array(strings.into_iter().map(serde_json::Value::String).collect())
}

/// The submitting token's scope boundary, captured at task-creation time and
/// persisted so async execution can never exceed it.
#[derive(Debug, Clone)]
pub struct TaskScopeSnapshot {
    pub token_id: Option<i32>,
    /// Whether the submitting token was scoped. This is NOT derivable from
    /// `scopes`: an unscoped token (`None`) and a deny-all scoped token
    /// (`Some(&[])`) both serialize to an empty array, so the boolean is the
    /// only thing that distinguishes "full authority" from "deny everything".
    pub scoped: bool,
    pub scopes: serde_json::Value,
}

impl TaskScopeSnapshot {
    /// Build from the submitting token id and its live scope set.
    pub fn from_request(token_id: Option<i32>, scopes: Option<&[Permissions]>) -> Self {
        Self {
            token_id,
            scoped: scopes.is_some(),
            scopes: scope_snapshot_json(scopes),
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
    conn: &mut crate::db::DbConnection,
) -> Result<chrono::NaiveDateTime, ApiError> {
    diesel::sql_query(DATABASE_UTC_NOW_QUERY)
        .get_result::<DatabaseTimeRow>(conn)
        .await
        .map(|row| row.now)
        .map_err(ApiError::from)
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
    async fn find_record(&self, pool: &DbPool) -> Result<TaskRecord, ApiError> {
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

    async fn list_events_with_total_count(
        &self,
        pool: &DbPool,
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
                    .load::<crate::events::Event>(conn)
                    .await
            } else {
                query
                    .order(id.asc())
                    .limit(limit as i64)
                    .load::<crate::events::Event>(conn)
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
        pool: &DbPool,
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
        pool: &DbPool,
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
        pool: &DbPool,
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
        pool: &DbPool,
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
        pool: &DbPool,
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

    async fn count_import_results(&self, pool: &DbPool) -> Result<TaskResultCounts, ApiError> {
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
            TaskResultCounts::new(processed, processed - failed, failed)
        })
        .await
    }

    async fn update_state(
        &self,
        pool: &DbPool,
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
                processed_items.eq(update.processed_items),
                success_items.eq(update.success_items),
                failed_items.eq(update.failed_items),
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
        pool: &DbPool,
        update: TaskStateUpdate,
        event: NewTaskEventRecord,
    ) -> Result<TaskRecord, ApiError> {
        use crate::schema::tasks::dsl::{
            failed_items, finished_at, id, lease_expires_at, lease_token, processed_items,
            request_payload, request_redacted_at, started_at, status, success_items, summary,
            tasks, updated_at,
        };

        let task_id_value = self.task_id();
        let task_lease_token = self.task_lease_token();
        let record = with_transaction(pool, async |conn| -> Result<TaskRecord, ApiError> {
            let event_record =
                emit_task_lifecycle_event(conn, &event, ActorKind::Worker, None, None).await?;
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
                processed_items.eq(update.processed_items),
                success_items.eq(update.success_items),
                failed_items.eq(update.failed_items),
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
        })
        .await?;

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
        record_task_completion_metrics(&record);

        Ok(record)
    }

    async fn finalize_export_with_output(
        &self,
        pool: &DbPool,
        update: TaskStateUpdate,
        event: NewTaskEventRecord,
        output: NewExportTaskOutputRecord,
    ) -> Result<TaskRecord, ApiError> {
        use crate::schema::export_task_outputs::dsl::{
            export_task_outputs, task_id as export_output_task_id,
        };
        use crate::schema::tasks::dsl::{
            failed_items, finished_at, id, lease_expires_at, lease_token, processed_items,
            request_payload, request_redacted_at, started_at, status, success_items, summary,
            tasks, updated_at,
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

            let event_record =
                emit_task_lifecycle_event(conn, &event, ActorKind::Worker, None, None).await?;
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
                processed_items.eq(update.processed_items),
                success_items.eq(update.success_items),
                failed_items.eq(update.failed_items),
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
        })
        .await?;

        info!(
            message = "Export task output stored and task finalized",
            task_id = record.id,
            task_kind = record.kind.as_str(),
            status = record.status.as_str(),
            processed_items = record.processed_items,
            success_items = record.success_items,
            failed_items = record.failed_items,
            summary = record.summary.as_deref()
        );
        record_task_completion_metrics(&record);

        Ok(record)
    }

    async fn finalize_backup_with_output(
        &self,
        pool: &DbPool,
        update: TaskStateUpdate,
        event: NewTaskEventRecord,
        output: NewBackupTaskOutputRecord,
    ) -> Result<TaskRecord, ApiError> {
        use crate::schema::backup_task_outputs::dsl::{
            backup_task_outputs, task_id as backup_output_task_id,
        };
        use crate::schema::tasks::dsl::{
            failed_items, finished_at, id, lease_expires_at, lease_token, processed_items,
            request_payload, request_redacted_at, started_at, status, success_items, summary,
            tasks, updated_at,
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

            let event_record =
                emit_task_lifecycle_event(conn, &event, ActorKind::Worker, None, None).await?;
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
                processed_items.eq(update.processed_items),
                success_items.eq(update.success_items),
                failed_items.eq(update.failed_items),
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
        })
        .await?;

        record_task_completion_metrics(&record);
        Ok(record)
    }
}

impl<T: TaskIdentifier + ?Sized> TaskBackend for T {}

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
    pub async fn create(self, pool: &DbPool) -> Result<TaskRecord, ApiError> {
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
        pool: &DbPool,
        submitter_id: i32,
        key: &str,
    ) -> Result<Option<TaskRecord>, ApiError> {
        use crate::schema::tasks::dsl::{idempotency_key, submitted_by, tasks};

        with_connection(pool, async |conn| {
            tasks
                .filter(submitted_by.eq(Some(submitter_id)))
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
    pool: &DbPool,
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

pub async fn list_export_task_output_summaries(
    pool: &DbPool,
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
    pool: &DbPool,
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

pub async fn purge_expired_export_outputs(pool: &DbPool) -> Result<Vec<i32>, ApiError> {
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
            for expired_task_id in &expired_task_ids {
                emit_task_lifecycle_event(
                    conn,
                    &NewTaskEventRecord {
                        task_id: *expired_task_id,
                        event_type: "cleanup".to_string(),
                        message: "Stored export output expired and was cleaned up".to_string(),
                        data: Some(serde_json::json!({
                            "cleaned_at": now,
                        })),
                    },
                    ActorKind::System,
                    None,
                    Some(TaskKind::Export.as_str()),
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

pub async fn purge_expired_backup_outputs(pool: &DbPool) -> Result<Vec<i32>, ApiError> {
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
        for expired_task_id in &expired_task_ids {
            emit_task_lifecycle_event(
                conn,
                &NewTaskEventRecord {
                    task_id: *expired_task_id,
                    event_type: "cleanup".to_string(),
                    message: "Stored backup output expired and was cleaned up".to_string(),
                    data: Some(serde_json::json!({ "cleaned_at": now })),
                },
                ActorKind::System,
                None,
                Some(TaskKind::Backup.as_str()),
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
    event: &NewTaskEventRecord,
    actor_kind: ActorKind,
    actor_user_id: Option<i32>,
    task_kind: Option<&str>,
) -> Result<NewEvent, ApiError> {
    let mut metadata = serde_json::json!({
        "task_id": event.task_id,
    });
    if let Some(task_kind) = task_kind {
        metadata["task_kind"] = serde_json::json!(task_kind);
    }
    if let Some(data) = &event.data {
        metadata["data"] = data.clone();
    }

    let mut lifecycle_event = NewEvent::new(
        EntityType::Task,
        task_event_action(&event.event_type)?,
        actor_kind,
        event.message.clone(),
    )?
    .with_entity_id(event.task_id)
    .with_metadata(metadata);
    if let Some(actor_user_id) = actor_user_id {
        lifecycle_event = lifecycle_event.with_actor_user_id(actor_user_id);
    }
    Ok(lifecycle_event)
}

async fn emit_task_lifecycle_event(
    conn: &mut crate::db::DbConnection,
    event: &NewTaskEventRecord,
    actor_kind: ActorKind,
    actor_user_id: Option<i32>,
    task_kind: Option<&str>,
) -> Result<crate::events::Event, ApiError> {
    let lifecycle_event = task_lifecycle_event(event, actor_kind, actor_user_id, task_kind)?;
    emit_event(conn, &lifecycle_event)
        .await
        .map_err(ApiError::from)
}

pub(crate) async fn emit_internal_task_event(
    conn: &mut crate::db::DbConnection,
    event: &NewTaskEventRecord,
    actor_user_id: Option<i32>,
    task_kind: TaskKind,
) -> Result<crate::events::Event, ApiError> {
    let actor_kind = if actor_user_id.is_some() {
        ActorKind::User
    } else {
        ActorKind::System
    };
    emit_task_lifecycle_event(
        conn,
        event,
        actor_kind,
        actor_user_id,
        Some(task_kind.as_str()),
    )
    .await
}

impl NewTaskEventRecord {
    /// Append this event to its task's history and return the persisted event.
    pub async fn append(self, pool: &DbPool) -> Result<TaskEventRecord, ApiError> {
        with_connection(pool, async |conn| -> Result<TaskEventRecord, ApiError> {
            emit_task_lifecycle_event(conn, &self, ActorKind::Worker, None, None)
                .await?
                .try_into()
        })
        .await
    }
}

pub async fn insert_import_results(
    pool: &DbPool,
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

pub(crate) fn executable_task_kind_values() -> [&'static str; 5] {
    [
        TaskKind::Import.as_str(),
        TaskKind::Export.as_str(),
        TaskKind::Backup.as_str(),
        TaskKind::Reindex.as_str(),
        TaskKind::RemoteCall.as_str(),
    ]
}

static NEXT_TASK_KIND: AtomicUsize = AtomicUsize::new(0);

fn task_kind_claim_order(start: usize) -> [&'static str; 5] {
    let kinds = executable_task_kind_values();
    std::array::from_fn(|offset| kinds[(start + offset) % kinds.len()])
}

pub async fn claim_next_queued_task(
    pool: &DbPool,
    lease_duration: std::time::Duration,
) -> Result<Option<TaskRecord>, ApiError> {
    use crate::schema::tasks::dsl::{
        attempt_count, created_at, id, kind, lease_expires_at, lease_token, started_at, status,
        tasks, updated_at,
    };

    let record = with_transaction(pool, async |conn| -> Result<Option<TaskRecord>, ApiError> {
        let task_kinds = executable_task_kind_values();
        let first_kind = NEXT_TASK_KIND.fetch_add(1, Ordering::Relaxed) % task_kinds.len();
        let claim_order = task_kind_claim_order(first_kind);
        let mut selected_task_id = None;
        for selected_kind in claim_order {
            selected_task_id = tasks
                .filter(status.eq(TaskStatus::Queued.as_str()))
                .filter(kind.eq(selected_kind))
                .order(created_at.asc())
                .for_update()
                .skip_locked()
                .select(id)
                .first::<i32>(conn)
                .await
                .optional()?;
            if selected_task_id.is_some() {
                break;
            }
        }
        let Some(task_id_value) = selected_task_id else {
            return Ok(None);
        };

        let claim_token = Uuid::new_v4();
        let lease_milliseconds = lease_duration_milliseconds(lease_duration);
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
            &NewTaskEventRecord {
                task_id: record.id,
                event_type: "validating".to_string(),
                message: "Task claimed for validation".to_string(),
                data: None,
            },
            ActorKind::Worker,
            None,
            Some(record.kind.as_str()),
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

fn lease_duration_milliseconds(lease_duration: std::time::Duration) -> i64 {
    i64::try_from(lease_duration.as_millis()).unwrap_or(i64::MAX)
}

/// Extend an active task lease if this worker still owns it.
pub async fn renew_task_lease(
    pool: &DbPool,
    task_id_value: i32,
    claim_token: Uuid,
    lease_duration: std::time::Duration,
) -> Result<bool, ApiError> {
    use crate::schema::tasks::dsl::{id, lease_expires_at, lease_token, status, tasks, updated_at};

    let active_statuses = [
        TaskStatus::Validating.as_str(),
        TaskStatus::Running.as_str(),
    ];
    let updated = with_connection(pool, async |conn| -> Result<usize, ApiError> {
        let lease_milliseconds = lease_duration_milliseconds(lease_duration);
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
    pool: &DbPool,
    batch_size: i64,
) -> Result<Vec<TaskRecord>, ApiError> {
    recover_expired_task_leases_matching(pool, batch_size, None).await
}

async fn recovered_task_result_counts(
    conn: &mut crate::db::DbConnection,
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
            TaskResultCounts::new(processed, processed - failed, failed)
        }
        TaskKind::Export | TaskKind::Backup | TaskKind::RemoteCall => {
            TaskResultCounts::new(1, 0, 1)
        }
        TaskKind::Reindex => Ok(TaskResultCounts {
            processed: task.processed_items,
            success: task.success_items,
            failed: task.failed_items,
        }),
    }
}

async fn recover_expired_task_leases_matching(
    pool: &DbPool,
    batch_size: i64,
    task_id_filter: Option<i32>,
) -> Result<Vec<TaskRecord>, ApiError> {
    use crate::schema::tasks::dsl::{
        deleted_at, failed_items, finished_at, id, lease_expires_at, lease_token, processed_items,
        request_payload, request_redacted_at, status, success_items, summary, tasks, updated_at,
    };

    let active_statuses = [
        TaskStatus::Validating.as_str(),
        TaskStatus::Running.as_str(),
    ];
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
                crate::db::traits::computed_field::mark_recovered_computed_reindex_failed(
                    conn,
                    stale_task.id,
                    message,
                )
                .await?;
            }
            emit_task_lifecycle_event(
                conn,
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
                ActorKind::System,
                None,
                Some(stale_task.kind.as_str()),
            )
            .await?;

            let record = diesel::update(tasks.filter(id.eq(stale_task.id)))
                .set((
                    status.eq(TaskStatus::Failed.as_str()),
                    summary.eq(Some(message.to_string())),
                    processed_items.eq(counts.processed),
                    success_items.eq(counts.success),
                    failed_items.eq(counts.failed),
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
    pool: &DbPool,
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
        pool: &DbPool,
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
        pool: &DbPool,
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
        let submitter_id = self.submitted_by;
        let task = with_transaction(pool, async |conn| -> Result<TaskRecord, ApiError> {
            acquire_task_capacity_lock(conn, submitter_id, limited_kind).await?;
            let active_count =
                count_active_tasks_for_user_in_transaction(conn, submitter_id, limited_kind).await?;
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
    conn: &mut crate::db::DbConnection,
    request: TaskCreateRequest,
) -> Result<TaskRecord, ApiError> {
    use crate::schema::tasks::dsl::tasks;

    let submitted_by = request.submitted_by;
    let task_kind = request.kind;
    let task = diesel::insert_into(tasks)
        .values(NewTaskRecord {
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
        })
        .get_result::<TaskRecord>(conn)
        .await?;

    emit_task_lifecycle_event(
        conn,
        &NewTaskEventRecord {
            task_id: task.id,
            event_type: "queued".to_string(),
            message: "Task queued".to_string(),
            data: None,
        },
        ActorKind::User,
        Some(submitted_by),
        Some(task_kind.as_str()),
    )
    .await?;

    Ok(task)
}

/// Insert an internal task as part of a caller-owned transaction.
///
/// Internal maintenance tasks may outlive the principal that triggered them,
/// so execution cannot depend on reloading or reauthorizing that principal.
pub(crate) async fn insert_internal_queued_task(
    conn: &mut crate::db::DbConnection,
    kind: TaskKind,
    payload: serde_json::Value,
    total_items_value: i32,
    submitted_by_value: Option<i32>,
) -> Result<TaskRecord, ApiError> {
    use crate::schema::tasks::dsl::tasks;

    let task = diesel::insert_into(tasks)
        .values(NewTaskRecord {
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
        })
        .get_result::<TaskRecord>(conn)
        .await?;

    let actor_kind = if submitted_by_value.is_some() {
        ActorKind::User
    } else {
        ActorKind::System
    };
    emit_task_lifecycle_event(
        conn,
        &NewTaskEventRecord {
            task_id: task.id,
            event_type: TaskStatus::Queued.as_str().to_string(),
            message: "Internal task queued".to_string(),
            data: None,
        },
        actor_kind,
        submitted_by_value,
        Some(kind.as_str()),
    )
    .await?;

    Ok(task)
}

async fn acquire_task_capacity_lock(
    conn: &mut crate::db::DbConnection,
    submitted_by: i32,
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

fn task_capacity_lock_key(submitted_by: i32, kind: TaskKind) -> i64 {
    const BASE_KEY: i64 = 4_801_000_000_000_i64;
    const KIND_STRIDE: i64 = 1_i64 << 32;

    let kind_slot = match kind {
        TaskKind::Export => 1_i64,
        TaskKind::RemoteCall => 2_i64,
        TaskKind::Backup => 3_i64,
        TaskKind::Import | TaskKind::Reindex => 9_i64,
    };
    BASE_KEY + (kind_slot * KIND_STRIDE) + i64::from(submitted_by)
}

async fn count_active_tasks_for_user_in_transaction(
    conn: &mut crate::db::DbConnection,
    submitted_by_value: i32,
    task_kind: TaskKind,
) -> Result<i64, ApiError> {
    use crate::schema::tasks::dsl::{deleted_at, kind, status, submitted_by, tasks};

    let active_statuses = [
        TaskStatus::Queued.as_str(),
        TaskStatus::Validating.as_str(),
        TaskStatus::Running.as_str(),
    ];

    tasks
        .filter(kind.eq(task_kind.as_str()))
        .filter(submitted_by.eq(Some(submitted_by_value)))
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
    use crate::db::prelude::*;
    use chrono::{Duration as ChronoDuration, Utc};
    use hubuum_task_core::IdempotencyKey;
    use rstest::rstest;
    use tokio::sync::oneshot;
    use uuid::Uuid;

    use super::{
        TaskBackend, TaskCreateRequest, TaskStateUpdate, claim_next_queued_task, database_now,
        insert_import_results, recover_expired_task_lease, renew_task_lease,
        task_capacity_lock_key, task_kind_claim_order,
    };
    use crate::db::traits::user::DeleteUserRecord;
    use crate::db::{with_connection, with_transaction};
    use crate::errors::ApiError;
    use crate::models::search::QueryOptions;
    use crate::models::{
        CollectionID, NewBackupTaskOutputRecord, NewImportTaskResultRecord, NewTaskEventRecord,
        NewTaskRecord, RemoteInvocationBodyOverride, RemoteInvocationParameters,
        RemoteInvocationSubject, RemoteTargetID, StoredRemoteCallTaskPayload, TaskID, TaskKind,
        TaskStatus,
    };
    use crate::tests::{TestContext, create_test_user};

    #[derive(QueryableByName)]
    struct TaskCapacityIndex {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        valid: bool,
        #[diesel(sql_type = diesel::sql_types::Text)]
        definition: String,
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
                attempt_count, id, lease_expires_at, lease_token, tasks,
            };

            diesel::update(tasks.filter(id.eq(task.id)))
                .set((
                    lease_token.eq(Some(claim_token)),
                    lease_expires_at.eq(Some(lease_expires_at_value)),
                    attempt_count.eq(1),
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
            task_capacity_lock_key(1_000_000_000, TaskKind::Export),
            task_capacity_lock_key(0, TaskKind::RemoteCall)
        );

        let user_id = 42;
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
        let claimed = claim_next_queued_task(&context.pool, std::time::Duration::from_secs(60))
            .await
            .unwrap()
            .map(|task| task.id);
        release_tx.send(()).unwrap();
        locker.await.unwrap();

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
                TaskStateUpdate {
                    status: TaskStatus::Running,
                    summary: None,
                    processed_items: 0,
                    success_items: 0,
                    failed_items: 0,
                    started_at: leased.started_at,
                    finished_at: None,
                },
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
                TaskStateUpdate {
                    status: TaskStatus::Succeeded,
                    summary: Some("stale backup completion".to_string()),
                    processed_items: 1,
                    success_items: 1,
                    failed_items: 0,
                    started_at: leased.started_at,
                    finished_at: None,
                },
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
                std::time::Duration::from_secs(60),
            )
            .await
            .unwrap()
        );
        assert!(
            renew_task_lease(
                &context.pool,
                leased.id,
                leased.lease_token.unwrap(),
                std::time::Duration::from_secs(60),
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
                std::time::Duration::from_secs(60),
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
                TaskStateUpdate {
                    status: TaskStatus::Running,
                    summary: None,
                    processed_items: 0,
                    success_items: 0,
                    failed_items: 0,
                    started_at: leased.started_at,
                    finished_at: None,
                },
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
        let task = (NewTaskRecord {
            kind: TaskKind::Import.as_str().to_string(),
            status: TaskStatus::Succeeded.as_str().to_string(),
            submitted_by: Some(task_owner.id),
            submitted_token_id: None,
            submitted_token_scoped: false,
            submitted_token_scopes: serde_json::json!([]),
            idempotency_key: Some(context.scoped_name("deleted-owner-task")),
            request_hash: None,
            request_payload: None,
            summary: Some("completed".to_string()),
            total_items: 0,
            processed_items: 0,
            success_items: 0,
            failed_items: 0,
            request_redacted_at: None,
            started_at: None,
            finished_at: None,
        }
        .create(&context.pool))
        .await
        .unwrap();

        (task_owner.delete_user_record_without_events(&context.pool))
            .await
            .unwrap();

        let stored = (task.find_record(&context.pool)).await.unwrap();
        assert_eq!(stored.submitted_by, None);
    }

    #[tokio::test]
    async fn test_export_task_active_limit_blocks_new_work_for_same_user() {
        let context = (TestContext::new()).await;
        let first = (TaskCreateRequest {
            kind: TaskKind::Export,
            submitted_by: context.admin_user.id,
            submitted_token_id: None,
            submitted_token_scoped: false,
            submitted_token_scopes: serde_json::json!([]),
            idempotency_key: Some(
                IdempotencyKey::new(context.scoped_name("export-cap-first")).unwrap(),
            ),
            request_hash: Some(context.scoped_name("export-cap-first-hash")),
            request_payload: serde_json::json!({"export": "first"}),
            total_items: 1,
        }
        .create_idempotently_with_active_limit(&context.pool, 1))
        .await
        .unwrap();

        assert_eq!(first.status, TaskStatus::Queued.as_str());

        let error = (TaskCreateRequest {
            kind: TaskKind::Export,
            submitted_by: context.admin_user.id,
            submitted_token_id: None,
            submitted_token_scoped: false,
            submitted_token_scopes: serde_json::json!([]),
            idempotency_key: Some(
                IdempotencyKey::new(context.scoped_name("export-cap-second")).unwrap(),
            ),
            request_hash: Some(context.scoped_name("export-cap-second-hash")),
            request_payload: serde_json::json!({"export": "second"}),
            total_items: 1,
        }
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
        let create_request = |suffix: &str| TaskCreateRequest {
            kind: TaskKind::Import,
            submitted_by: context.admin_user.id,
            submitted_token_id: None,
            submitted_token_scoped: false,
            submitted_token_scopes: serde_json::json!([]),
            idempotency_key: Some(
                IdempotencyKey::new(context.scoped_name(&format!("import-cap-{suffix}"))).unwrap(),
            ),
            request_hash: Some(context.scoped_name(&format!("import-cap-{suffix}-hash"))),
            request_payload: serde_json::json!({"import": suffix}),
            total_items: 1,
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

        let first = (TaskCreateRequest {
            kind: TaskKind::RemoteCall,
            submitted_by: context.admin_user.id,
            submitted_token_id: None,
            submitted_token_scoped: false,
            submitted_token_scopes: serde_json::json!([]),
            idempotency_key: Some(
                IdempotencyKey::new(context.scoped_name("remote-cap-first")).unwrap(),
            ),
            request_hash: Some(context.scoped_name("remote-cap-first-hash")),
            request_payload: payload.clone(),
            total_items: 1,
        }
        .create_idempotently_with_active_limit(&context.pool, 1))
        .await
        .unwrap();

        assert_eq!(first.status, TaskStatus::Queued.as_str());

        let error = (TaskCreateRequest {
            kind: TaskKind::RemoteCall,
            submitted_by: context.admin_user.id,
            submitted_token_id: None,
            submitted_token_scoped: false,
            submitted_token_scopes: serde_json::json!([]),
            idempotency_key: Some(
                IdempotencyKey::new(context.scoped_name("remote-cap-second")).unwrap(),
            ),
            request_hash: Some(context.scoped_name("remote-cap-second-hash")),
            request_payload: payload,
            total_items: 1,
        }
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
        let request = |suffix: &str| TaskCreateRequest {
            kind,
            submitted_by: context.admin_user.id,
            submitted_token_id: None,
            submitted_token_scoped: false,
            submitted_token_scopes: serde_json::json!([]),
            idempotency_key: Some(
                IdempotencyKey::new(
                    context.scoped_name(&format!("{}-concurrent-cap-{suffix}", kind.as_str())),
                )
                .unwrap(),
            ),
            request_hash: Some(
                context.scoped_name(&format!("{}-concurrent-cap-{suffix}-hash", kind.as_str())),
            ),
            request_payload: serde_json::json!({"kind": kind.as_str(), "case": suffix}),
            total_items: 1,
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
