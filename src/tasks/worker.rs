use std::sync::{Once, OnceLock};
use std::thread;
use std::time::Duration;

use actix_rt::time::sleep;
use tokio::sync::Notify;
use tracing::error;

use crate::config::{DEFAULT_TASK_POLL_INTERVAL_MS, get_config};
use crate::db::DbPool;
use crate::db::traits::task::{
    TaskStateUpdate, claim_next_queued_task, count_import_results_summary,
    finalize_task_terminal_state,
};
use crate::errors::ApiError;
use crate::models::{
    NewTaskEventRecord, TaskKind, TaskRecord, TaskResultCounts, TaskStatus, UserID,
};

use super::execution::execute_import_task;
use super::helpers::sanitize_error_for_storage;
use super::types::WorkerLoopAction;

static TASK_WORKER: Once = Once::new();
static TASK_WORKER_NOTIFY: OnceLock<Notify> = OnceLock::new();

fn get_task_worker_notify() -> &'static Notify {
    TASK_WORKER_NOTIFY.get_or_init(Notify::new)
}

fn configured_task_worker_count() -> usize {
    get_config().map(|config| config.task_workers).unwrap_or(1)
}

fn configured_task_poll_interval() -> Duration {
    let interval_ms = get_config()
        .map(|config| config.task_poll_interval_ms)
        .unwrap_or(DEFAULT_TASK_POLL_INTERVAL_MS);
    Duration::from_millis(interval_ms)
}

pub(super) fn background_worker_action(result: &Result<bool, ApiError>) -> WorkerLoopAction {
    match result {
        Ok(true) => WorkerLoopAction::Continue,
        Ok(false) => WorkerLoopAction::Sleep,
        Err(err) => {
            error!(message = "Task worker iteration failed", error = %err);
            WorkerLoopAction::Sleep
        }
    }
}

async fn wait_for_task_worker_wakeup(poll_interval: Duration) {
    tokio::select! {
        _ = sleep(poll_interval) => {}
        _ = get_task_worker_notify().notified() => {}
    }
}

async fn task_worker_loop(pool: DbPool, poll_interval: Duration) {
    loop {
        let result = process_one_task(&pool).await;
        match background_worker_action(&result) {
            WorkerLoopAction::Continue => continue,
            WorkerLoopAction::Sleep => wait_for_task_worker_wakeup(poll_interval).await,
        }
    }
}

fn spawn_task_worker_loop(pool: DbPool, poll_interval: Duration, worker_index: usize) {
    thread::Builder::new()
        .name(format!("task-worker-{worker_index}"))
        .spawn(move || {
            let system = actix_rt::System::new();
            system.block_on(task_worker_loop(pool, poll_interval));
        })
        .expect("failed to spawn task worker thread");
}

pub fn ensure_task_worker_running(pool: DbPool) {
    let worker_count = configured_task_worker_count();
    let poll_interval = configured_task_poll_interval();
    TASK_WORKER.call_once(move || {
        for worker_index in 0..worker_count {
            spawn_task_worker_loop(pool.clone(), poll_interval, worker_index);
        }
    });
}

pub fn kick_task_worker(pool: DbPool) {
    ensure_task_worker_running(pool);
    get_task_worker_notify().notify_one();
}

pub(super) async fn process_one_task(pool: &DbPool) -> Result<bool, ApiError> {
    let Some(task) = claim_next_queued_task(pool).await? else {
        return Ok(false);
    };

    if let Err(err) = process_claimed_task(pool, &task).await {
        mark_claimed_task_failed(pool, &task, &err).await?;
    }

    Ok(true)
}

async fn process_claimed_task(pool: &DbPool, task: &TaskRecord) -> Result<(), ApiError> {
    let submitted_by = task.submitted_by.ok_or_else(|| {
        ApiError::BadRequest("Submitting user is no longer available for this task".to_string())
    })?;
    let submitted_by = UserID(submitted_by).user(pool).await?;

    match TaskKind::from_db(&task.kind)? {
        TaskKind::Import => execute_import_task(pool, task, &submitted_by).await,
        other => Err(ApiError::BadRequest(format!(
            "Task kind '{}' is not implemented",
            other.as_str()
        ))),
    }
}

pub(super) async fn mark_claimed_task_failed(
    pool: &DbPool,
    task: &TaskRecord,
    err: &ApiError,
) -> Result<(), ApiError> {
    let summary = sanitize_error_for_storage(err);
    let counts = match TaskKind::from_db(&task.kind)? {
        TaskKind::Import => count_import_results_summary(pool, task.id).await?,
        _ => TaskResultCounts::default(),
    };
    finalize_task_terminal_state(
        pool,
        task.id,
        TaskStateUpdate {
            status: TaskStatus::Failed,
            summary: Some(summary.clone()),
            processed_items: counts.processed,
            success_items: counts.success,
            failed_items: counts.failed,
            started_at: task.started_at,
            finished_at: None,
        },
        NewTaskEventRecord {
            task_id: task.id,
            event_type: "failed".to_string(),
            message: "Task failed".to_string(),
            data: Some(serde_json::json!({ "error": summary })),
        },
    )
    .await?;
    Ok(())
}
