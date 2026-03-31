use std::sync::{Mutex, Once, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use actix_rt::time::sleep;
use tokio::sync::Notify;
use tracing::{error, info, warn};

use crate::api::v1::handlers::reports::execute_report_task;
use crate::config::{DEFAULT_TASK_POLL_INTERVAL_MS, get_config};
use crate::db::DbPool;
use crate::db::traits::task::{
    TaskStateUpdate, claim_next_queued_task, count_import_results_summary,
    finalize_task_terminal_state, purge_expired_report_outputs,
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
static REPORT_OUTPUT_CLEANUP_STATE: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();

fn get_task_worker_notify() -> &'static Notify {
    TASK_WORKER_NOTIFY.get_or_init(Notify::new)
}

fn cleanup_state() -> &'static Mutex<Option<Instant>> {
    REPORT_OUTPUT_CLEANUP_STATE.get_or_init(|| Mutex::new(None))
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
            info!(
                message = "Starting task worker loop",
                worker_index = worker_index,
                poll_interval = ?poll_interval
            );
            let system = actix_rt::System::new();
            system.block_on(task_worker_loop(pool, poll_interval));
        })
        .expect("failed to spawn task worker thread");
}

pub fn ensure_task_worker_running(pool: DbPool) {
    let worker_count = configured_task_worker_count();
    let poll_interval = configured_task_poll_interval();
    TASK_WORKER.call_once(move || {
        info!(
            message = "Initializing task workers",
            worker_count = worker_count,
            poll_interval = ?poll_interval
        );
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
    maybe_cleanup_expired_report_outputs(pool).await?;

    let Some(task) = claim_next_queued_task(pool).await? else {
        return Ok(false);
    };

    info!(
        message = "Task picked up by worker",
        task_id = task.id,
        task_kind = task.kind.as_str(),
        status = task.status.as_str(),
        worker = std::thread::current().name().unwrap_or("task-worker")
    );

    if let Err(err) = process_claimed_task(pool, &task).await {
        mark_claimed_task_failed(pool, &task, &err).await?;
    }

    Ok(true)
}

async fn maybe_cleanup_expired_report_outputs(pool: &DbPool) -> Result<(), ApiError> {
    let cleanup_interval = get_config()
        .map(|config| config.report_output_cleanup_interval_seconds)
        .unwrap_or(300);
    let should_run = {
        let mut state = cleanup_state().lock().map_err(|_| {
            ApiError::InternalServerError("Cleanup state lock poisoned".to_string())
        })?;
        match *state {
            Some(last_run) if last_run.elapsed() < Duration::from_secs(cleanup_interval) => false,
            _ => {
                *state = Some(Instant::now());
                true
            }
        }
    };

    if should_run {
        purge_expired_report_outputs(pool).await?;
    }

    Ok(())
}

async fn process_claimed_task(pool: &DbPool, task: &TaskRecord) -> Result<(), ApiError> {
    let submitted_by = task.submitted_by.ok_or_else(|| {
        ApiError::BadRequest("Submitting user is no longer available for this task".to_string())
    })?;
    let submitted_by = UserID(submitted_by).user(pool).await?;

    info!(
        message = "Dispatching task execution",
        task_id = task.id,
        task_kind = task.kind.as_str(),
        status = task.status.as_str(),
        submitted_by = submitted_by.id
    );

    match TaskKind::from_db(&task.kind)? {
        TaskKind::Import => execute_import_task(pool, task, &submitted_by).await,
        TaskKind::Report => execute_report_task(pool, task, &submitted_by).await,
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
        TaskKind::Report => TaskResultCounts::new(1, 0, 1)?,
        _ => TaskResultCounts::default(),
    };

    warn!(
        message = "Claimed task failed",
        task_id = task.id,
        task_kind = task.kind.as_str(),
        status = task.status.as_str(),
        processed_items = counts.processed,
        success_items = counts.success,
        failed_items = counts.failed,
        error = %err
    );

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
