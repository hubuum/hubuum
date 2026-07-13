use std::sync::{Mutex, Once, OnceLock};
use std::time::{Duration, Instant};

use actix_rt::time::sleep;
use chrono::Utc;
use tokio::sync::{Notify, oneshot};
use tracing::{error, info, warn};

#[cfg(test)]
use crate::config::get_config;
use crate::config::{
    DEFAULT_EXPORT_OUTPUT_CLEANUP_INTERVAL_SECONDS, DEFAULT_TASK_HEARTBEAT_SECONDS,
    DEFAULT_TASK_LEASE_SECONDS, DEFAULT_TASK_POLL_INTERVAL_MS,
    DEFAULT_TASK_RECOVERY_INTERVAL_SECONDS,
};
use crate::db::DbPool;
use crate::db::traits::task::{
    TaskBackend, TaskStateUpdate, claim_next_queued_task, purge_expired_export_outputs,
    recover_expired_task_leases, renew_task_lease,
};
use crate::errors::ApiError;
use crate::exports::execute_export_task;
use crate::lifecycle::{ShutdownSignal, spawn_background_worker};
use crate::models::{NewTaskEventRecord, TaskKind, TaskRecord, TaskResultCounts, TaskStatus};
use crate::observability::metrics;

use super::execution::execute_import_task;
use super::helpers::sanitize_error_for_storage;
use super::remote_call::execute_remote_call_task;
use super::types::WorkerLoopAction;

static TASK_WORKER: Once = Once::new();
static TASK_WORKER_NOTIFY: OnceLock<Notify> = OnceLock::new();
static EXPORT_OUTPUT_CLEANUP_STATE: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();
static TASK_RECOVERY_STATE: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();
static TASK_WORKER_SETTINGS: OnceLock<TaskWorkerSettings> = OnceLock::new();

#[derive(Clone, Copy, Debug)]
pub struct TaskWorkerSettings {
    worker_count: usize,
    poll_interval: Duration,
    lease_duration: Duration,
    heartbeat_interval: Duration,
    recovery_interval: Duration,
    export_output_cleanup_interval: Duration,
}

impl TaskWorkerSettings {
    pub fn new(
        worker_count: usize,
        poll_interval: Duration,
        lease_duration: Duration,
        heartbeat_interval: Duration,
        recovery_interval: Duration,
        export_output_cleanup_interval: Duration,
    ) -> Result<Self, String> {
        if poll_interval.is_zero() {
            return Err("task worker poll interval must be greater than zero".to_string());
        }
        if lease_duration.is_zero() {
            return Err("task worker lease duration must be greater than zero".to_string());
        }
        if heartbeat_interval.is_zero() || heartbeat_interval >= lease_duration {
            return Err(
                "task worker heartbeat interval must be greater than zero and shorter than the lease"
                    .to_string(),
            );
        }
        if recovery_interval.is_zero() {
            return Err("task recovery interval must be greater than zero".to_string());
        }
        if export_output_cleanup_interval.is_zero() {
            return Err("export output cleanup interval must be greater than zero".to_string());
        }
        Ok(Self {
            worker_count,
            poll_interval,
            lease_duration,
            heartbeat_interval,
            recovery_interval,
            export_output_cleanup_interval,
        })
    }
}

pub fn initialize_task_worker_settings(settings: TaskWorkerSettings) -> Result<(), String> {
    TASK_WORKER_SETTINGS
        .set(settings)
        .map_err(|_| "task worker settings were already initialized".to_string())
}

fn get_task_worker_notify() -> &'static Notify {
    TASK_WORKER_NOTIFY.get_or_init(Notify::new)
}

fn cleanup_state() -> &'static Mutex<Option<Instant>> {
    EXPORT_OUTPUT_CLEANUP_STATE.get_or_init(|| Mutex::new(None))
}

fn recovery_state() -> &'static Mutex<Option<Instant>> {
    TASK_RECOVERY_STATE.get_or_init(|| Mutex::new(None))
}

fn task_worker_settings() -> TaskWorkerSettings {
    TASK_WORKER_SETTINGS
        .get()
        .copied()
        .unwrap_or(TaskWorkerSettings {
            worker_count: 1,
            poll_interval: Duration::from_millis(DEFAULT_TASK_POLL_INTERVAL_MS),
            lease_duration: Duration::from_secs(DEFAULT_TASK_LEASE_SECONDS),
            heartbeat_interval: Duration::from_secs(DEFAULT_TASK_HEARTBEAT_SECONDS),
            recovery_interval: Duration::from_secs(DEFAULT_TASK_RECOVERY_INTERVAL_SECONDS),
            export_output_cleanup_interval: Duration::from_secs(
                DEFAULT_EXPORT_OUTPUT_CLEANUP_INTERVAL_SECONDS,
            ),
        })
}

fn configured_task_worker_count() -> usize {
    task_worker_settings().worker_count
}

fn configured_task_poll_interval() -> Duration {
    task_worker_settings().poll_interval
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

async fn wait_for_task_worker_wakeup(poll_interval: Duration, shutdown: &ShutdownSignal) -> bool {
    tokio::select! {
        biased;
        _ = shutdown.requested() => false,
        _ = sleep(poll_interval) => true,
        _ = get_task_worker_notify().notified() => true,
    }
}

async fn task_worker_loop(pool: DbPool, poll_interval: Duration, shutdown: ShutdownSignal) {
    loop {
        if shutdown.is_requested() {
            break;
        }
        let result = process_one_task(&pool, Some(&shutdown)).await;
        if shutdown.is_requested() {
            break;
        }
        match background_worker_action(&result) {
            WorkerLoopAction::Continue => continue,
            WorkerLoopAction::Sleep => {
                if !wait_for_task_worker_wakeup(poll_interval, &shutdown).await {
                    break;
                }
            }
        }
    }
}

fn spawn_task_worker_loop(pool: DbPool, poll_interval: Duration, worker_index: usize) {
    spawn_background_worker(format!("task-worker-{worker_index}"), move |shutdown| {
        info!(
            message = "Starting task worker loop",
            worker_index = worker_index,
            poll_interval = ?poll_interval
        );
        let system = actix_rt::System::new();
        system.block_on(async move {
            let pool = task_worker_pool(pool);
            task_worker_loop(pool, poll_interval, shutdown).await;
        });
    });
}

#[cfg(not(test))]
fn task_worker_pool(pool: DbPool) -> DbPool {
    pool
}

/// Async Postgres connections are tied to the runtime that established them.
/// Test cases each own a short-lived Actix runtime, while the background worker
/// thread is process-global. Build the test worker's pool on its own long-lived
/// runtime so it never inherits connections driven by a completed test runtime.
#[cfg(test)]
fn task_worker_pool(pool: DbPool) -> DbPool {
    drop(pool);
    let config = get_config().expect("test task worker requires database configuration");
    crate::db::init_pool(&config.database_url, config.db_pool_size)
}

pub fn ensure_task_worker_running(pool: DbPool) {
    let worker_count = configured_task_worker_count();
    if worker_count == 0 {
        return;
    }
    let poll_interval = configured_task_poll_interval();
    TASK_WORKER.call_once(move || {
        info!(
            message = "Initializing task workers",
            worker_count = worker_count,
            poll_interval = ?poll_interval
        );
        metrics::task_worker_config(worker_count, poll_interval);
        for worker_index in 0..worker_count {
            spawn_task_worker_loop(pool.clone(), poll_interval, worker_index);
        }
    });
}

pub fn kick_task_worker(pool: DbPool) {
    ensure_task_worker_running(pool);
    get_task_worker_notify().notify_one();
}

pub(super) async fn process_one_task(
    pool: &DbPool,
    shutdown: Option<&ShutdownSignal>,
) -> Result<bool, ApiError> {
    maybe_recover_expired_task_leases(pool).await?;

    if let Err(error) = maybe_cleanup_expired_export_outputs(pool).await {
        metrics::task_worker_iteration("error");
        return Err(error);
    }

    let task = match claim_next_queued_task(pool, task_worker_settings().lease_duration).await {
        Ok(task) => task,
        Err(error) => {
            metrics::task_worker_iteration("error");
            return Err(error);
        }
    };

    let Some(task) = task else {
        metrics::task_worker_iteration("idle");
        return Ok(false);
    };
    metrics::task_worker_iteration("claimed");
    metrics::task_claimed(&task.kind, duration_since(task.created_at));

    info!(
        message = "Task picked up by worker",
        task_id = task.id,
        task_kind = task.kind.as_str(),
        status = task.status.as_str(),
        worker = std::thread::current().name().unwrap_or("task-worker")
    );

    let mut heartbeat = start_task_lease_heartbeat(pool.clone(), &task);
    let execution = async {
        match shutdown {
            Some(shutdown) => {
                tokio::select! {
                    biased;
                    _ = shutdown.requested() => Err(ApiError::ServiceUnavailable(
                        "Task interrupted by graceful server shutdown".to_string(),
                    )),
                    result = process_claimed_task(pool, &task) => result,
                }
            }
            None => process_claimed_task(pool, &task).await,
        }
    };
    let mut ownership_lost = false;
    let result = tokio::select! {
        result = execution => result,
        _ = wait_for_lost_task_lease(&mut heartbeat) => {
            ownership_lost = true;
            Err(ApiError::ServiceUnavailable(
                "Task execution stopped because its worker lease was lost".to_string(),
            ))
        }
    };
    if let Some(heartbeat) = heartbeat {
        heartbeat.stop().await;
    }
    if let Err(err) = result
        && !ownership_lost
    {
        mark_claimed_task_failed(pool, &task, &err).await?;
    }

    Ok(true)
}

struct TaskLeaseHeartbeat {
    stop: oneshot::Sender<()>,
    handle: tokio::task::JoinHandle<()>,
    lost: oneshot::Receiver<()>,
}

impl TaskLeaseHeartbeat {
    async fn stop(self) {
        let _ = self.stop.send(());
        let _ = self.handle.await;
    }
}

fn start_task_lease_heartbeat(pool: DbPool, task: &TaskRecord) -> Option<TaskLeaseHeartbeat> {
    let claim_token = task.lease_token?;
    let settings = task_worker_settings();
    let task_id = task.id;
    let (stop_tx, mut stop_rx) = oneshot::channel();
    let (lost_tx, lost_rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        let mut lost_tx = Some(lost_tx);
        loop {
            tokio::select! {
                _ = sleep(settings.heartbeat_interval) => {
                    match renew_task_lease(&pool, task_id, claim_token, settings.lease_duration).await {
                        Ok(true) => {}
                        Ok(false) => {
                            warn!(
                                message = "Task lease is no longer owned by this worker",
                                task_id,
                                claim_token = %claim_token,
                            );
                            if let Some(lost_tx) = lost_tx.take() {
                                let _ = lost_tx.send(());
                            }
                            break;
                        }
                        Err(error) => {
                            warn!(
                                message = "Failed to renew task worker lease",
                                task_id,
                                claim_token = %claim_token,
                                error = %error,
                            );
                        }
                    }
                }
                _ = &mut stop_rx => break,
            }
        }
    });
    Some(TaskLeaseHeartbeat {
        stop: stop_tx,
        handle,
        lost: lost_rx,
    })
}

async fn wait_for_lost_task_lease(heartbeat: &mut Option<TaskLeaseHeartbeat>) {
    match heartbeat {
        Some(heartbeat) => {
            let _ = (&mut heartbeat.lost).await;
        }
        None => std::future::pending().await,
    }
}

async fn maybe_recover_expired_task_leases(pool: &DbPool) -> Result<(), ApiError> {
    let recovery_interval = task_worker_settings().recovery_interval;
    let previous_last_run = {
        let mut state = recovery_state().lock().map_err(|_| {
            ApiError::InternalServerError("Task recovery state lock poisoned".to_string())
        })?;
        match *state {
            Some(last_run) if last_run.elapsed() < recovery_interval => return Ok(()),
            previous_last_run => {
                *state = Some(Instant::now());
                previous_last_run
            }
        }
    };

    match recover_expired_task_leases(pool, 100).await {
        Ok(recovered) => {
            for task in recovered {
                metrics::task_lease_recovered(&task.kind);
                warn!(
                    message = "Recovered task after worker lease expiry",
                    task_id = task.id,
                    task_kind = task.kind,
                    attempt_count = task.attempt_count,
                    recovery_status = task.status,
                );
            }
            Ok(())
        }
        Err(error) => {
            let mut state = recovery_state().lock().map_err(|_| {
                ApiError::InternalServerError("Task recovery state lock poisoned".to_string())
            })?;
            *state = previous_last_run;
            Err(error)
        }
    }
}

async fn maybe_cleanup_expired_export_outputs(pool: &DbPool) -> Result<(), ApiError> {
    let cleanup_interval = task_worker_settings().export_output_cleanup_interval;
    let previous_last_run = {
        let mut state = cleanup_state().lock().map_err(|_| {
            ApiError::InternalServerError("Cleanup state lock poisoned".to_string())
        })?;
        match *state {
            Some(last_run) if last_run.elapsed() < cleanup_interval => {
                return Ok(());
            }
            previous_last_run => {
                *state = Some(Instant::now());
                previous_last_run
            }
        }
    };

    metrics::export_output_cleanup_run();
    match purge_expired_export_outputs(pool).await {
        Ok(deleted) => metrics::export_output_cleanup_deleted(deleted.len()),
        Err(error) => {
            metrics::export_output_cleanup_failed();
            let mut state = cleanup_state().lock().map_err(|_| {
                ApiError::InternalServerError("Cleanup state lock poisoned".to_string())
            })?;
            *state = previous_last_run;
            return Err(error);
        }
    }

    Ok(())
}

fn duration_since(timestamp: chrono::NaiveDateTime) -> Option<Duration> {
    let elapsed = Utc::now()
        .naive_utc()
        .signed_duration_since(timestamp)
        .num_milliseconds();
    (elapsed >= 0).then(|| Duration::from_millis(elapsed as u64))
}

async fn process_claimed_task(pool: &DbPool, task: &TaskRecord) -> Result<(), ApiError> {
    let submitted_by = task.submitted_by.ok_or_else(|| {
        ApiError::BadRequest(
            "Submitting principal is no longer available for this task".to_string(),
        )
    })?;
    let principal = crate::models::principal::load_principal_by_id(pool, submitted_by).await?;

    // Disabled-SA gate: queued service-account tasks must not run once the SA is
    // disabled (mirrors the immediate token-validation rejection).
    if crate::db::traits::service_account::principal_is_disabled(pool, &principal).await? {
        return Err(ApiError::BadRequest(
            "Submitting service account is disabled; task will not run".to_string(),
        ));
    }

    // Reconstruct the submitting token's scope boundary from the snapshot,
    // failing closed on any unknown permission string.
    let snapshot_scopes: Option<Vec<crate::models::Permissions>> = if task.submitted_token_scoped {
        let entries = task.submitted_token_scopes.as_array().ok_or_else(|| {
            ApiError::InternalServerError("Task scope snapshot is not an array".to_string())
        })?;
        let mut parsed = Vec::with_capacity(entries.len());
        for entry in entries {
            let raw = entry.as_str().ok_or_else(|| {
                ApiError::InternalServerError(
                    "Task scope snapshot entry is not a string".to_string(),
                )
            })?;
            parsed.push(crate::models::Permissions::from_string(raw)?);
        }
        Some(parsed)
    } else {
        None
    };
    let scopes = snapshot_scopes.as_deref();

    info!(
        message = "Dispatching task execution",
        task_id = task.id,
        task_kind = task.kind.as_str(),
        status = task.status.as_str(),
        submitted_by = principal.id,
        scoped = task.submitted_token_scoped
    );

    match TaskKind::from_db(&task.kind)? {
        TaskKind::Import => execute_import_task(pool, task, &principal, scopes).await,
        TaskKind::Export => execute_export_task(pool, task, &principal, scopes).await,
        TaskKind::RemoteCall => execute_remote_call_task(pool, task, &principal, scopes).await,
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
        TaskKind::Import => task.count_import_results(pool).await?,
        TaskKind::Export => TaskResultCounts::new(1, 0, 1)?,
        TaskKind::RemoteCall => TaskResultCounts::new(1, 0, 1)?,
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

    task.finalize_terminal(
        pool,
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
