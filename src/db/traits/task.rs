use chrono::Utc;
use diesel::PgConnection;
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Bool};
use tracing::info;

use crate::apply_query_options;
use crate::config::get_config;
use crate::db::{DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::models::search::QueryOptions;
use crate::models::{
    ImportTaskResultRecord, NewImportTaskResultRecord, NewReportTaskOutputRecord,
    NewTaskEventRecord, NewTaskRecord, ReportOutputLookup, ReportTaskOutputRecord,
    ReportTaskOutputSummaryRecord, TaskEventRecord, TaskID, TaskKind, TaskRecord, TaskResponse,
    TaskResultCounts, TaskStatus,
};
use crate::pagination::{CursorValue, decode_cursor_values, page_limits_or_defaults};

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
    pub submitted_by: i32,
    pub idempotency_key: Option<String>,
    pub request_hash: Option<String>,
    pub request_payload: serde_json::Value,
    pub total_items: i32,
}

#[derive(QueryableByName)]
struct AdvisoryLockRow {
    #[diesel(sql_type = Bool)]
    locked: bool,
}

/// Anything that can name a task for a backend query: a [`TaskID`] from a request path or an
/// already-loaded [`TaskRecord`] (and references to either). The required `task_id` resolves the
/// raw id at the persistence boundary so it never leaks into the domain.
pub trait TaskIdentifier {
    fn task_id(&self) -> i32;
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
}

impl<T: TaskIdentifier + ?Sized> TaskIdentifier for &T {
    fn task_id(&self) -> i32 {
        (**self).task_id()
    }
}

/// Single-task backend persistence, as self-methods on any [`TaskIdentifier`]. Callers write
/// `task.find_record(pool)` / `task.update_state(pool, ..)` rather than passing a bare id to a free
/// function; all Diesel query construction stays here in the backend layer.
pub trait TaskBackend: TaskIdentifier {
    async fn find_record(&self, pool: &DbPool) -> Result<TaskRecord, ApiError> {
        use crate::schema::tasks::dsl::{id, tasks};

        let task_id_value = self.task_id();
        with_connection(pool, |conn| {
            tasks.filter(id.eq(task_id_value)).first::<TaskRecord>(conn)
        })
    }

    async fn list_events_with_total_count(
        &self,
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<(Vec<TaskEventRecord>, i64), ApiError> {
        use crate::schema::task_events::dsl::{id, task_events, task_id};

        let task_id_value = self.task_id();
        let limit = query_options
            .limit
            .unwrap_or(page_limits_or_defaults().0.saturating_add(1));
        let descending = query_options
            .sort
            .first()
            .map(|sort| sort.descending)
            .unwrap_or(false);
        let cursor_id = decode_history_cursor_id(query_options)?;

        let total_count = with_connection(pool, |conn| {
            task_events
                .filter(task_id.eq(task_id_value))
                .count()
                .get_result::<i64>(conn)
        })?;

        let items = with_connection(pool, |conn| {
            let mut query = task_events.filter(task_id.eq(task_id_value)).into_boxed();
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
                    .load::<TaskEventRecord>(conn)
            } else {
                query
                    .order(id.asc())
                    .limit(limit as i64)
                    .load::<TaskEventRecord>(conn)
            }
        })?;

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
            .unwrap_or(page_limits_or_defaults().0.saturating_add(1));
        let descending = query_options
            .sort
            .first()
            .map(|sort| sort.descending)
            .unwrap_or(false);
        let cursor_id = decode_history_cursor_id(query_options)?;

        let total_count = with_connection(pool, |conn| {
            import_task_results
                .filter(task_id.eq(task_id_value))
                .count()
                .get_result::<i64>(conn)
        })?;

        let items = with_connection(pool, |conn| {
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
            } else {
                query
                    .order(id.asc())
                    .limit(limit as i64)
                    .load::<ImportTaskResultRecord>(conn)
            }
        })?;

        Ok((items, total_count))
    }

    async fn find_report_output(
        &self,
        pool: &DbPool,
    ) -> Result<ReportOutputLookup<ReportTaskOutputRecord>, ApiError> {
        use crate::schema::report_task_outputs::dsl::{report_task_outputs, task_id};

        let task_id_value = self.task_id();
        let now = Utc::now().naive_utc();
        // Fetch without the expiry filter so an expired-but-present row is reported as `Expired`
        // (410) rather than silently looking like a row that never existed (404).
        let record = with_connection(pool, |conn| {
            report_task_outputs
                .filter(task_id.eq(task_id_value))
                .first::<ReportTaskOutputRecord>(conn)
                .optional()
        })?;

        Ok(match record {
            Some(record) if record.output_expires_at > now => ReportOutputLookup::Available(record),
            Some(record) => ReportOutputLookup::Expired {
                expires_at: record.output_expires_at,
            },
            None => ReportOutputLookup::Missing,
        })
    }

    async fn find_report_output_summary(
        &self,
        pool: &DbPool,
    ) -> Result<ReportOutputLookup<ReportTaskOutputSummaryRecord>, ApiError> {
        use crate::schema::report_task_outputs::dsl::{report_task_outputs, task_id};

        let task_id_value = self.task_id();
        let now = Utc::now().naive_utc();
        let record = with_connection(pool, |conn| {
            report_task_outputs
                .filter(task_id.eq(task_id_value))
                .select(ReportTaskOutputSummaryRecord::as_select())
                .first::<ReportTaskOutputSummaryRecord>(conn)
                .optional()
        })?;

        Ok(match record {
            Some(record) if record.output_expires_at > now => ReportOutputLookup::Available(record),
            Some(record) => ReportOutputLookup::Expired {
                expires_at: record.output_expires_at,
            },
            None => ReportOutputLookup::Missing,
        })
    }

    async fn count_import_results(&self, pool: &DbPool) -> Result<TaskResultCounts, ApiError> {
        use crate::schema::import_task_results::dsl::{import_task_results, outcome, task_id};

        let task_id_value = self.task_id();
        with_connection(pool, |conn| -> Result<TaskResultCounts, ApiError> {
            let processed = import_task_results
                .filter(task_id.eq(task_id_value))
                .count()
                .get_result::<i64>(conn)?;
            let failed = import_task_results
                .filter(task_id.eq(task_id_value))
                .filter(outcome.eq("failed"))
                .count()
                .get_result::<i64>(conn)?;
            TaskResultCounts::new(processed, processed - failed, failed)
        })
    }

    async fn update_state(
        &self,
        pool: &DbPool,
        update: TaskStateUpdate,
    ) -> Result<TaskRecord, ApiError> {
        use crate::schema::tasks::dsl::{
            failed_items, finished_at, id, processed_items, started_at, status, success_items,
            summary, tasks, updated_at,
        };

        let task_id_value = self.task_id();
        let now = Utc::now().naive_utc();
        let record = with_connection(pool, |conn| {
            diesel::update(tasks.filter(id.eq(task_id_value)))
                .set((
                    status.eq(update.status.as_str()),
                    summary.eq(update.summary),
                    processed_items.eq(update.processed_items),
                    success_items.eq(update.success_items),
                    failed_items.eq(update.failed_items),
                    started_at.eq(update.started_at),
                    finished_at.eq(update.finished_at),
                    updated_at.eq(now),
                ))
                .get_result::<TaskRecord>(conn)
        })?;

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
        use crate::schema::task_events::dsl::task_events;
        use crate::schema::tasks::dsl::{
            failed_items, finished_at, id, processed_items, request_payload, request_redacted_at,
            started_at, status, success_items, summary, tasks, updated_at,
        };

        let task_id_value = self.task_id();
        let record = with_transaction(pool, |conn| {
            let event_record = diesel::insert_into(task_events)
                .values(event)
                .get_result::<TaskEventRecord>(conn)?;

            diesel::update(tasks.filter(id.eq(task_id_value)))
                .set((
                    status.eq(update.status.as_str()),
                    summary.eq(update.summary),
                    processed_items.eq(update.processed_items),
                    success_items.eq(update.success_items),
                    failed_items.eq(update.failed_items),
                    started_at.eq(update.started_at),
                    finished_at.eq(Some(event_record.created_at)),
                    request_payload.eq::<Option<serde_json::Value>>(None),
                    request_redacted_at.eq(event_record.created_at),
                    updated_at.eq(event_record.created_at),
                ))
                .get_result::<TaskRecord>(conn)
        })?;

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

        Ok(record)
    }

    async fn finalize_report_with_output(
        &self,
        pool: &DbPool,
        update: TaskStateUpdate,
        event: NewTaskEventRecord,
        output: NewReportTaskOutputRecord,
    ) -> Result<TaskRecord, ApiError> {
        use crate::schema::report_task_outputs::dsl::{
            report_task_outputs, task_id as report_output_task_id,
        };
        use crate::schema::task_events::dsl::task_events;
        use crate::schema::tasks::dsl::{
            failed_items, finished_at, id, processed_items, request_payload, request_redacted_at,
            started_at, status, success_items, summary, tasks, updated_at,
        };

        let task_id_value = self.task_id();
        let record = with_transaction(pool, |conn| {
            // Idempotent so a future requeue / manual re-claim that re-finalizes the same task
            // cannot trip the `report_task_outputs.task_id` UNIQUE constraint and roll back the
            // transaction, which would otherwise leave the task stuck mid-flight.
            diesel::insert_into(report_task_outputs)
                .values(output)
                .on_conflict(report_output_task_id)
                .do_nothing()
                .execute(conn)?;

            let event_record = diesel::insert_into(task_events)
                .values(event)
                .get_result::<TaskEventRecord>(conn)?;

            diesel::update(tasks.filter(id.eq(task_id_value)))
                .set((
                    status.eq(update.status.as_str()),
                    summary.eq(update.summary),
                    processed_items.eq(update.processed_items),
                    success_items.eq(update.success_items),
                    failed_items.eq(update.failed_items),
                    started_at.eq(update.started_at),
                    finished_at.eq(Some(event_record.created_at)),
                    request_payload.eq::<Option<serde_json::Value>>(None),
                    request_redacted_at.eq(event_record.created_at),
                    updated_at.eq(event_record.created_at),
                ))
                .get_result::<TaskRecord>(conn)
        })?;

        info!(
            message = "Report task output stored and task finalized",
            task_id = record.id,
            task_kind = record.kind.as_str(),
            status = record.status.as_str(),
            processed_items = record.processed_items,
            success_items = record.success_items,
            failed_items = record.failed_items,
            summary = record.summary.as_deref()
        );

        Ok(record)
    }
}

impl<T: TaskIdentifier + ?Sized> TaskBackend for T {}

#[cfg(test)]
impl NewTaskRecord {
    /// Insert this new task row and return the persisted record.
    pub async fn create(self, pool: &DbPool) -> Result<TaskRecord, ApiError> {
        use crate::schema::tasks::dsl::tasks;

        with_connection(pool, |conn| {
            diesel::insert_into(tasks)
                .values(&self)
                .get_result::<TaskRecord>(conn)
        })
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

        with_connection(pool, |conn| {
            tasks
                .filter(submitted_by.eq(Some(submitter_id)))
                .filter(idempotency_key.eq(key))
                .first::<TaskRecord>(conn)
                .optional()
        })
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
    let total_count = with_connection(pool, |conn| {
        build_task_query(submitted_by_filter, kind_filter, status_filter)
            .count()
            .get_result::<i64>(conn)
    })?;

    let items = with_connection(pool, |conn| -> Result<Vec<TaskRecord>, ApiError> {
        let mut query = build_task_query(submitted_by_filter, kind_filter, status_filter);
        apply_query_options!(query, query_options, TaskResponse);
        Ok(query.load::<TaskRecord>(conn)?)
    })?;

    Ok((items, total_count))
}

pub async fn list_report_task_output_summaries(
    pool: &DbPool,
    task_ids: &[i32],
) -> Result<Vec<ReportTaskOutputSummaryRecord>, ApiError> {
    use crate::schema::report_task_outputs::dsl::{report_task_outputs, task_id};

    if task_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Return expired-but-present rows too; the caller classifies each against `now` so the
    // `output_expired` flag is consistent with the single-task lookups rather than silently
    // collapsing expired rows into "no output" on the task-list endpoint.
    with_connection(pool, |conn| {
        report_task_outputs
            .filter(task_id.eq_any(task_ids))
            .select(ReportTaskOutputSummaryRecord::as_select())
            .load(conn)
    })
}

pub async fn purge_expired_report_outputs(pool: &DbPool) -> Result<Vec<i32>, ApiError> {
    use crate::schema::report_task_outputs::dsl::{
        output_expires_at, report_task_outputs, task_id,
    };
    use crate::schema::task_events::dsl::task_events;

    let now = Utc::now().naive_utc();
    let expired_task_ids = with_transaction(pool, |conn| {
        let expired_task_ids =
            diesel::delete(report_task_outputs.filter(output_expires_at.le(now)))
                .returning(task_id)
                .get_results::<i32>(conn)?;

        if !expired_task_ids.is_empty() {
            let events = expired_task_ids
                .iter()
                .map(|expired_task_id| NewTaskEventRecord {
                    task_id: *expired_task_id,
                    event_type: "cleanup".to_string(),
                    message: "Stored report output expired and was cleaned up".to_string(),
                    data: Some(serde_json::json!({
                        "cleaned_at": now,
                    })),
                })
                .collect::<Vec<_>>();
            diesel::insert_into(task_events)
                .values(&events)
                .execute(conn)?;
        }

        Ok::<_, diesel::result::Error>(expired_task_ids)
    })?;

    if !expired_task_ids.is_empty() {
        info!(
            message = "Expired report outputs cleaned up",
            cleaned_count = expired_task_ids.len(),
            retention_hours = get_config()
                .map(|config| config.report_output_retention_hours)
                .unwrap_or(168)
        );
    }

    Ok(expired_task_ids)
}

fn decode_history_cursor_id(query_options: &QueryOptions) -> Result<Option<i32>, ApiError> {
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

impl NewTaskEventRecord {
    /// Append this event to its task's history and return the persisted event.
    pub async fn append(self, pool: &DbPool) -> Result<TaskEventRecord, ApiError> {
        use crate::schema::task_events::dsl::task_events;

        with_connection(pool, |conn| {
            diesel::insert_into(task_events)
                .values(&self)
                .get_result::<TaskEventRecord>(conn)
        })
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

    with_connection(pool, |conn| {
        diesel::insert_into(import_task_results)
            .values(entries)
            .execute(conn)
    })
}

pub async fn claim_next_queued_task(pool: &DbPool) -> Result<Option<TaskRecord>, ApiError> {
    use crate::schema::task_events::dsl::task_events;
    use crate::schema::tasks::dsl::{created_at, id, started_at, status, tasks, updated_at};

    let record = with_transaction(pool, |conn| -> Result<Option<TaskRecord>, ApiError> {
        let Some(task_id_value) = tasks
            .filter(status.eq(TaskStatus::Queued.as_str()))
            .order(created_at.asc())
            .for_update()
            .skip_locked()
            .select(id)
            .first::<i32>(conn)
            .optional()?
        else {
            return Ok(None);
        };

        let now = Utc::now().naive_utc();
        let record = diesel::update(tasks.filter(id.eq(task_id_value)))
            .set((
                status.eq(TaskStatus::Validating.as_str()),
                started_at.eq(Some(now)),
                updated_at.eq(now),
            ))
            .get_result::<TaskRecord>(conn)?;

        diesel::insert_into(task_events)
            .values(NewTaskEventRecord {
                task_id: record.id,
                event_type: "validating".to_string(),
                message: "Task claimed for validation".to_string(),
                data: None,
            })
            .execute(conn)?;

        Ok(Some(record))
    })?;

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

impl TaskCreateRequest {
    /// Queue this task (with its initial "queued" event) in a single transaction.
    pub async fn create_generic(self, pool: &DbPool) -> Result<TaskRecord, ApiError> {
        let task = with_transaction(pool, |conn| -> Result<TaskRecord, ApiError> {
            insert_queued_task_with_event(conn, self)
        })?;

        log_task_queued(&task);

        Ok(task)
    }

    /// Queue this report task, rejecting it with `429` if the submitter already has
    /// `max_active_report_tasks` queued/validating/running reports. Capacity is checked under a
    /// per-user advisory lock so concurrent submissions cannot race past the limit.
    pub async fn create_with_active_report_limit(
        self,
        pool: &DbPool,
        max_active_report_tasks: usize,
    ) -> Result<TaskRecord, ApiError> {
        if self.kind != TaskKind::Report {
            return Err(ApiError::BadRequest(
                "create_with_active_report_limit only accepts report tasks".to_string(),
            ));
        }

        let max_active_report_tasks = i64::try_from(max_active_report_tasks).unwrap_or(i64::MAX);
        let submitter_id = self.submitted_by;
        let task = with_transaction(pool, |conn| -> Result<TaskRecord, ApiError> {
            acquire_report_task_capacity_lock(conn, submitter_id)?;
            let active_count =
                count_active_report_tasks_for_user_in_transaction(conn, submitter_id)?;
            if active_count >= max_active_report_tasks {
                return Err(ApiError::TooManyRequests(format!(
                    "Too many active report tasks for user ({active_count} >= {max_active_report_tasks}); wait for queued or running reports to finish"
                )));
            }

            insert_queued_task_with_event(conn, self)
        })?;

        log_task_queued(&task);

        Ok(task)
    }
}

fn insert_queued_task_with_event(
    conn: &mut PgConnection,
    request: TaskCreateRequest,
) -> Result<TaskRecord, ApiError> {
    use crate::schema::task_events::dsl::task_events;
    use crate::schema::tasks::dsl::tasks;

    let task = diesel::insert_into(tasks)
        .values(NewTaskRecord {
            kind: request.kind.as_str().to_string(),
            status: TaskStatus::Queued.as_str().to_string(),
            submitted_by: Some(request.submitted_by),
            idempotency_key: request.idempotency_key,
            request_hash: request.request_hash,
            request_payload: Some(request.request_payload),
            summary: None,
            total_items: request.total_items,
            processed_items: 0,
            success_items: 0,
            failed_items: 0,
            request_redacted_at: None,
            started_at: None,
            finished_at: None,
        })
        .get_result::<TaskRecord>(conn)?;

    diesel::insert_into(task_events)
        .values(NewTaskEventRecord {
            task_id: task.id,
            event_type: "queued".to_string(),
            message: "Task queued".to_string(),
            data: None,
        })
        .execute(conn)?;

    Ok(task)
}

fn acquire_report_task_capacity_lock(
    conn: &mut PgConnection,
    submitted_by: i32,
) -> Result<(), ApiError> {
    let lock_key = report_task_capacity_lock_key(submitted_by);
    let lock = diesel::sql_query("SELECT TRUE AS locked FROM pg_advisory_xact_lock($1)")
        .bind::<BigInt, _>(lock_key)
        .get_result::<AdvisoryLockRow>(conn)?;
    if !lock.locked {
        return Err(ApiError::InternalServerError(
            "Failed to acquire report task capacity lock".to_string(),
        ));
    }

    Ok(())
}

fn report_task_capacity_lock_key(submitted_by: i32) -> i64 {
    4_801_000_000_000_i64 + i64::from(submitted_by)
}

fn count_active_report_tasks_for_user_in_transaction(
    conn: &mut PgConnection,
    submitted_by_value: i32,
) -> Result<i64, ApiError> {
    use crate::schema::tasks::dsl::{deleted_at, kind, status, submitted_by, tasks};

    let active_statuses = [
        TaskStatus::Queued.as_str(),
        TaskStatus::Validating.as_str(),
        TaskStatus::Running.as_str(),
    ];

    tasks
        .filter(kind.eq(TaskKind::Report.as_str()))
        .filter(submitted_by.eq(Some(submitted_by_value)))
        .filter(status.eq_any(active_statuses))
        .filter(deleted_at.is_null())
        .count()
        .get_result::<i64>(conn)
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
    use diesel::prelude::*;
    use futures::executor::block_on;
    use std::sync::mpsc;
    use std::thread;

    use super::{TaskBackend, TaskCreateRequest, claim_next_queued_task};
    use crate::db::traits::user::DeleteUserRecord;
    use crate::db::with_transaction;
    use crate::errors::ApiError;
    use crate::models::search::QueryOptions;
    use crate::models::{NewTaskRecord, TaskID, TaskKind, TaskStatus};
    use crate::tests::{TestContext, create_test_user};

    #[test]
    fn test_claim_next_queued_task_is_safe_under_concurrency() {
        let context = block_on(TestContext::new());
        let mut created_ids = Vec::new();
        let claim_prefix = context.scoped_name("claim");

        for index in 0..3 {
            let task = block_on(
                NewTaskRecord {
                    kind: TaskKind::Import.as_str().to_string(),
                    status: TaskStatus::Queued.as_str().to_string(),
                    submitted_by: Some(context.admin_user.id),
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
                .create(&context.pool),
            )
            .unwrap();
            created_ids.push(task.id);
        }

        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let pool = context.pool.clone();
        let claim_prefix_for_locker = claim_prefix.clone();
        let locker = thread::spawn(move || {
            use crate::schema::tasks::dsl::{created_at, id, idempotency_key, status, tasks};

            with_transaction(&pool, |conn| -> Result<(), crate::errors::ApiError> {
                let locked_id = tasks
                    .filter(status.eq(TaskStatus::Queued.as_str()))
                    .filter(idempotency_key.like(format!("{claim_prefix_for_locker}-%")))
                    .order(created_at.asc())
                    .for_update()
                    .select(id)
                    .first::<i32>(conn)?;
                locked_tx.send(locked_id).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            })
            .unwrap();
        });

        let locked_id = locked_rx.recv().unwrap();
        let claimed = block_on(claim_next_queued_task(&context.pool))
            .unwrap()
            .map(|task| task.id);
        release_tx.send(()).unwrap();
        locker.join().unwrap();

        assert!(claimed.is_some());
        assert_ne!(claimed.unwrap(), locked_id);
        assert!(created_ids.contains(&locked_id));

        let (claimed_events, _) = block_on(
            TaskID::new(claimed.unwrap())
                .unwrap()
                .list_events_with_total_count(
                    &context.pool,
                    &QueryOptions {
                        filters: Vec::new(),
                        sort: Vec::new(),
                        limit: None,
                        cursor: None,
                    },
                ),
        )
        .unwrap();
        assert_eq!(
            claimed_events
                .iter()
                .filter(|event| event.event_type == "validating")
                .count(),
            1
        );
    }

    #[test]
    fn test_task_history_survives_user_deletion() {
        let context = block_on(TestContext::new());
        let task_owner = block_on(create_test_user(&context.pool));
        let task = block_on(
            NewTaskRecord {
                kind: TaskKind::Import.as_str().to_string(),
                status: TaskStatus::Succeeded.as_str().to_string(),
                submitted_by: Some(task_owner.id),
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
            .create(&context.pool),
        )
        .unwrap();

        block_on(task_owner.delete_user_record(&context.pool)).unwrap();

        let stored = block_on(task.find_record(&context.pool)).unwrap();
        assert_eq!(stored.submitted_by, None);
    }

    #[test]
    fn test_report_task_active_limit_blocks_new_work_for_same_user() {
        let context = block_on(TestContext::new());
        let first = block_on(
            TaskCreateRequest {
                kind: TaskKind::Report,
                submitted_by: context.admin_user.id,
                idempotency_key: Some(context.scoped_name("report-cap-first")),
                request_hash: Some(context.scoped_name("report-cap-first-hash")),
                request_payload: serde_json::json!({"report": "first"}),
                total_items: 1,
            }
            .create_with_active_report_limit(&context.pool, 1),
        )
        .unwrap();

        assert_eq!(first.status, TaskStatus::Queued.as_str());

        let error = block_on(
            TaskCreateRequest {
                kind: TaskKind::Report,
                submitted_by: context.admin_user.id,
                idempotency_key: Some(context.scoped_name("report-cap-second")),
                request_hash: Some(context.scoped_name("report-cap-second-hash")),
                request_payload: serde_json::json!({"report": "second"}),
                total_items: 1,
            }
            .create_with_active_report_limit(&context.pool, 1),
        )
        .unwrap_err();

        match error {
            ApiError::TooManyRequests(message) => {
                assert!(message.contains("Too many active report tasks for user"));
            }
            other => panic!("expected TooManyRequests, got {other:?}"),
        }
    }
}
