use chrono::Utc;
use diesel::prelude::*;

use crate::db::{DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::models::{
    ImportTaskResultRecord, NewImportTaskResultRecord, NewTaskEventRecord, NewTaskRecord,
    TaskEventRecord, TaskKind, TaskRecord, TaskStatus,
};

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

pub async fn create_task_record(
    pool: &DbPool,
    new_task: NewTaskRecord,
) -> Result<TaskRecord, ApiError> {
    use crate::schema::tasks::dsl::tasks;

    with_connection(pool, |conn| {
        diesel::insert_into(tasks)
            .values(&new_task)
            .get_result::<TaskRecord>(conn)
    })
}

pub async fn find_task_record(pool: &DbPool, task_id: i32) -> Result<TaskRecord, ApiError> {
    use crate::schema::tasks::dsl::{id, tasks};

    with_connection(pool, |conn| {
        tasks.filter(id.eq(task_id)).first::<TaskRecord>(conn)
    })
}

pub async fn find_task_by_idempotency(
    pool: &DbPool,
    submitter_id: i32,
    key: &str,
) -> Result<Option<TaskRecord>, ApiError> {
    use crate::schema::tasks::dsl::{idempotency_key, submitted_by, tasks};

    with_connection(pool, |conn| {
        tasks
            .filter(submitted_by.eq(submitter_id))
            .filter(idempotency_key.eq(key))
            .first::<TaskRecord>(conn)
            .optional()
    })
}

pub async fn list_task_events(
    pool: &DbPool,
    task_id_value: i32,
) -> Result<Vec<TaskEventRecord>, ApiError> {
    use crate::schema::task_events::dsl::{created_at, task_events, task_id};

    with_connection(pool, |conn| {
        task_events
            .filter(task_id.eq(task_id_value))
            .order((created_at.asc(), crate::schema::task_events::dsl::id.asc()))
            .load::<TaskEventRecord>(conn)
    })
}

pub async fn list_import_results(
    pool: &DbPool,
    task_id_value: i32,
) -> Result<Vec<ImportTaskResultRecord>, ApiError> {
    use crate::schema::import_task_results::dsl::{created_at, import_task_results, task_id};

    with_connection(pool, |conn| {
        import_task_results
            .filter(task_id.eq(task_id_value))
            .order((
                created_at.asc(),
                crate::schema::import_task_results::dsl::id.asc(),
            ))
            .load::<ImportTaskResultRecord>(conn)
    })
}

pub async fn append_task_event(
    pool: &DbPool,
    event: NewTaskEventRecord,
) -> Result<TaskEventRecord, ApiError> {
    use crate::schema::task_events::dsl::task_events;

    with_connection(pool, |conn| {
        diesel::insert_into(task_events)
            .values(&event)
            .get_result::<TaskEventRecord>(conn)
    })
}

pub async fn insert_import_results(
    pool: &DbPool,
    entries: &[NewImportTaskResultRecord],
) -> Result<Vec<ImportTaskResultRecord>, ApiError> {
    use crate::schema::import_task_results::dsl::import_task_results;

    if entries.is_empty() {
        return Ok(Vec::new());
    }

    with_connection(pool, |conn| {
        diesel::insert_into(import_task_results)
            .values(entries)
            .get_results::<ImportTaskResultRecord>(conn)
    })
}

pub async fn update_task_state(
    pool: &DbPool,
    task_id_value: i32,
    update: TaskStateUpdate,
) -> Result<TaskRecord, ApiError> {
    use crate::schema::tasks::dsl::{
        failed_items, finished_at, id, processed_items, started_at, status, success_items, summary,
        tasks, updated_at,
    };

    let now = Utc::now().naive_utc();
    with_connection(pool, |conn| {
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
    })
}

pub async fn redact_task_payload(
    pool: &DbPool,
    task_id_value: i32,
) -> Result<TaskRecord, ApiError> {
    use crate::schema::tasks::dsl::{id, request_payload, request_redacted_at, tasks, updated_at};

    let now = Utc::now().naive_utc();
    with_connection(pool, |conn| {
        diesel::update(tasks.filter(id.eq(task_id_value)))
            .set((
                request_payload.eq::<Option<serde_json::Value>>(None),
                request_redacted_at.eq(now),
                updated_at.eq(now),
            ))
            .get_result::<TaskRecord>(conn)
    })
}

pub async fn claim_next_queued_task(pool: &DbPool) -> Result<Option<TaskRecord>, ApiError> {
    use crate::schema::tasks::dsl::{created_at, id, started_at, status, tasks, updated_at};

    with_transaction(pool, |conn| -> Result<Option<TaskRecord>, ApiError> {
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

        Ok(Some(record))
    })
}

pub async fn create_generic_task(
    pool: &DbPool,
    request: TaskCreateRequest,
) -> Result<TaskRecord, ApiError> {
    let task = create_task_record(
        pool,
        NewTaskRecord {
            kind: request.kind.as_str().to_string(),
            status: TaskStatus::Queued.as_str().to_string(),
            submitted_by: request.submitted_by,
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
        },
    )
    .await?;

    append_task_event(
        pool,
        NewTaskEventRecord {
            task_id: task.id,
            event_type: "queued".to_string(),
            message: "Task queued".to_string(),
            data: None,
        },
    )
    .await?;

    Ok(task)
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use diesel::prelude::*;
    use futures::executor::block_on;
    use std::sync::mpsc;
    use std::thread;

    use super::create_task_record;
    use crate::db::with_transaction;
    use crate::models::{NewTaskRecord, TaskKind, TaskStatus};
    use crate::tests::TestContext;

    #[test]
    fn test_claim_next_queued_task_is_safe_under_concurrency() {
        let context = block_on(TestContext::new());
        let mut created_ids = Vec::new();
        let claim_prefix = context.scoped_name("claim");

        for index in 0..3 {
            let task = block_on(create_task_record(
                &context.pool,
                NewTaskRecord {
                    kind: TaskKind::Import.as_str().to_string(),
                    status: TaskStatus::Queued.as_str().to_string(),
                    submitted_by: context.admin_user.id,
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
                },
            ))
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
        let claimed = with_transaction(
            &context.pool,
            |conn| -> Result<Option<i32>, crate::errors::ApiError> {
                use crate::schema::tasks::dsl::{
                    created_at, id, idempotency_key, started_at, status, tasks, updated_at,
                };

                let Some(task_id_value) = tasks
                    .filter(status.eq(TaskStatus::Queued.as_str()))
                    .filter(idempotency_key.like(format!("{claim_prefix}-%")))
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
                    .get_result::<crate::models::TaskRecord>(conn)?;

                Ok(Some(record.id))
            },
        )
        .unwrap();
        release_tx.send(()).unwrap();
        locker.join().unwrap();

        assert!(claimed.is_some());
        assert_ne!(claimed.unwrap(), locked_id);
        assert!(created_ids.contains(&locked_id));
    }
}
