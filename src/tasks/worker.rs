use std::future::Future;
use std::sync::{LazyLock, Mutex, Once, OnceLock};
use std::time::{Duration, Instant};

use actix_rt::time::{Instant as TokioInstant, sleep, sleep_until};
use chrono::Utc;
use tokio::sync::{Notify, oneshot};
use tracing::{error, info, warn};

use crate::backups::{BackupSettings, execute_backup_task};
use crate::config::{
    DEFAULT_BACKUP_MAX_ACTIVE_TASKS_PER_USER, DEFAULT_BACKUP_MAX_OUTPUT_BYTES,
    DEFAULT_BACKUP_OUTPUT_RETENTION_HOURS, DEFAULT_EXPORT_OUTPUT_CLEANUP_INTERVAL_SECONDS,
    DEFAULT_TASK_HEARTBEAT_SECONDS, DEFAULT_TASK_LEASE_SECONDS, DEFAULT_TASK_POLL_INTERVAL_MS,
    DEFAULT_TASK_RECOVERY_INTERVAL_SECONDS, get_config,
};
use crate::db::traits::service_account::principal_is_disabled;
use crate::db::traits::task::{
    TaskBackend, TaskStateUpdate, claim_next_queued_task, purge_expired_backup_outputs,
    purge_expired_export_outputs, recover_expired_task_leases, renew_task_lease,
};
use crate::db::{DatabasePoolSettings, DbPool, init_pool_with_settings};
use crate::errors::ApiError;
use crate::exports::execute_export_task;
use crate::lifecycle::{ShutdownSignal, spawn_background_worker};
use crate::models::principal::load_principal_by_id;
use crate::models::{
    NewTaskEventRecord, Permissions, TaskKind, TaskRecord, TaskResultCounts, TaskStatus,
};
use crate::observability::metrics;
#[cfg(test)]
use crate::permissions::LocalPermissionBackend;
use crate::permissions::{AppContext, require_unscoped_runtime_admin};
use crate::restores::{MaintenanceActivityGuard, maintenance_state};

use super::execution::execute_import_task;
use super::helpers::sanitize_error_for_storage;
use super::remote_call::execute_remote_call_task;
use super::types::WorkerLoopAction;

static TASK_WORKER: Once = Once::new();
static TASK_WORKER_NOTIFY: OnceLock<Notify> = OnceLock::new();
static TASK_OUTPUT_CLEANUP_STATE: OnceLock<Mutex<CleanupSchedule>> = OnceLock::new();
static TASK_RECOVERY_STATE: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();
static TASK_WORKER_SETTINGS: OnceLock<TaskWorkerSettings> = OnceLock::new();
#[cfg(not(test))]
static TASK_LEASE_POOL: OnceLock<DbPool> = OnceLock::new();
static TASK_LEASE_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("task-lease-heartbeat")
        .enable_all()
        .build()
        .expect("task lease heartbeat runtime must start")
});

const TASK_LEASE_POOL_SIZE: u32 = 1;

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

fn cleanup_state() -> &'static Mutex<CleanupSchedule> {
    TASK_OUTPUT_CLEANUP_STATE.get_or_init(|| Mutex::new(CleanupSchedule::default()))
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

fn new_task_lease_pool() -> DbPool {
    let config = get_config().expect("task lease renewal requires database configuration");
    let settings = DatabasePoolSettings::builder(config.database_url.clone())
        .max_size(TASK_LEASE_POOL_SIZE)
        .statement_timeout_ms(config.db_statement_timeout_ms)
        .acquire_timeout_ms(config.db_pool_acquire_timeout_ms)
        .build()
        .expect("task lease pool settings must be valid");
    init_pool_with_settings(&settings)
}

#[cfg(not(test))]
fn task_lease_pool() -> DbPool {
    TASK_LEASE_POOL.get_or_init(new_task_lease_pool).clone()
}

// Test runtimes are short-lived, so do not retain async Postgres connections in
// a process-global pool after the runtime that established them has stopped.
#[cfg(test)]
fn task_lease_pool() -> DbPool {
    new_task_lease_pool()
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

async fn task_worker_loop(
    context: AppContext,
    poll_interval: Duration,
    backup_settings: BackupSettings,
    shutdown: ShutdownSignal,
) {
    loop {
        if shutdown.is_requested() {
            break;
        }
        let result =
            process_one_task_with_settings(&context, Some(&shutdown), &backup_settings).await;
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

fn spawn_task_worker_loop(
    context: AppContext,
    poll_interval: Duration,
    worker_index: usize,
    backup_settings: BackupSettings,
) {
    spawn_background_worker(format!("task-worker-{worker_index}"), move |shutdown| {
        info!(
            message = "Starting task worker loop",
            worker_index = worker_index,
            poll_interval = ?poll_interval
        );
        let system = actix_rt::System::new();
        system.block_on(async move {
            let context = task_worker_context(context);
            task_worker_loop(context, poll_interval, backup_settings, shutdown).await;
        });
    });
}

#[cfg(not(test))]
fn task_worker_context(context: AppContext) -> AppContext {
    context
}

/// Async Postgres connections are tied to the runtime that established them.
/// Test cases each own a short-lived Actix runtime, while the background worker
/// thread is process-global. Build the test worker's pool on its own long-lived
/// runtime so it never inherits connections driven by a completed test runtime.
#[cfg(test)]
fn task_worker_context(context: AppContext) -> AppContext {
    drop(context);
    let config = get_config().expect("test task worker requires database configuration");
    let pool = crate::db::init_pool(&config.database_url, config.db_pool_size);
    let permissions = std::sync::Arc::new(LocalPermissionBackend::new(
        pool.clone(),
        config.admin_groupname.clone(),
    ));
    AppContext::new(pool, permissions)
}

fn configured_backup_settings() -> BackupSettings {
    let config = get_config().ok();
    BackupSettings::new(
        config
            .as_ref()
            .map(|value| value.backup_output_retention_hours)
            .unwrap_or(DEFAULT_BACKUP_OUTPUT_RETENTION_HOURS),
        config
            .as_ref()
            .map(|value| value.backup_max_active_tasks_per_user)
            .unwrap_or(DEFAULT_BACKUP_MAX_ACTIVE_TASKS_PER_USER),
        config
            .as_ref()
            .map(|value| value.backup_max_output_bytes)
            .unwrap_or(DEFAULT_BACKUP_MAX_OUTPUT_BYTES),
    )
    .expect("default backup settings are valid")
}

pub fn ensure_task_worker_running_with_settings(
    context: AppContext,
    backup_settings: BackupSettings,
) {
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
            spawn_task_worker_loop(
                context.clone(),
                poll_interval,
                worker_index,
                backup_settings.clone(),
            );
        }
    });
}

pub fn ensure_task_worker_running(context: AppContext) {
    ensure_task_worker_running_with_settings(context, configured_backup_settings());
}

pub fn kick_task_worker(context: AppContext) {
    ensure_task_worker_running(context);
    get_task_worker_notify().notify_one();
}

#[cfg(test)]
pub(super) async fn process_one_task(
    pool: &DbPool,
    shutdown: Option<&ShutdownSignal>,
) -> Result<bool, ApiError> {
    let admin_groupname = get_config()
        .map(|config| config.admin_groupname.clone())
        .unwrap_or_else(|_| "admin".to_string());
    let context = AppContext::new(
        pool.clone(),
        std::sync::Arc::new(LocalPermissionBackend::new(pool.clone(), admin_groupname)),
    );
    process_one_task_with_settings(&context, shutdown, &configured_backup_settings()).await
}

async fn process_one_task_with_settings(
    context: &AppContext,
    shutdown: Option<&ShutdownSignal>,
    backup_settings: &BackupSettings,
) -> Result<bool, ApiError> {
    let _activity = MaintenanceActivityGuard::begin();
    if maintenance_state(context).await? != "normal" {
        metrics::task_worker_iteration("idle");
        return Ok(false);
    }
    maybe_recover_expired_task_leases(context).await?;

    if let Err(error) = maybe_cleanup_expired_task_outputs(context).await {
        metrics::task_worker_iteration("error");
        return Err(error);
    }

    let settings = task_worker_settings();
    let claim_started_at = TokioInstant::now();
    let task = match claim_next_queued_task(context, settings.lease_duration).await {
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

    let mut heartbeat = start_task_lease_heartbeat(
        task_lease_pool(),
        &task,
        claim_started_at + settings.lease_duration,
    );
    let execution = async {
        match shutdown {
            Some(shutdown) => {
                tokio::select! {
                    biased;
                    _ = shutdown.requested() => Err(ApiError::ServiceUnavailable(
                        "Task interrupted by graceful server shutdown".to_string(),
                    )),
                    result = process_claimed_task(context, &task, backup_settings) => result,
                }
            }
            None => process_claimed_task(context, &task, backup_settings).await,
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
    if let Err(err) = &result
        && !ownership_lost
    {
        let finalized = finalize_failure_while_lease_owned(
            &mut heartbeat,
            mark_claimed_task_failed(context, &task, err),
        )
        .await?;
        if !finalized {
            warn!(
                message = "Task failure finalization stopped because its worker lease was lost",
                task_id = task.id,
                claim_token = ?task.lease_token,
            );
        }
    }
    if let Some(heartbeat) = heartbeat {
        heartbeat.stop().await;
    }

    Ok(true)
}

#[derive(Debug, Default)]
struct CleanupSchedule {
    last_completed_at: Option<Instant>,
    in_progress: bool,
}

struct CleanupReservation<'a> {
    state: &'a Mutex<CleanupSchedule>,
    finished: bool,
}

impl<'a> CleanupReservation<'a> {
    fn reserve(
        state: &'a Mutex<CleanupSchedule>,
        interval: Duration,
    ) -> Result<Option<Self>, ApiError> {
        let mut schedule = state.lock().map_err(|_| {
            ApiError::InternalServerError("Cleanup state lock poisoned".to_string())
        })?;
        if schedule.in_progress
            || schedule
                .last_completed_at
                .is_some_and(|last_run| last_run.elapsed() < interval)
        {
            return Ok(None);
        }
        schedule.in_progress = true;
        Ok(Some(Self {
            state,
            finished: false,
        }))
    }

    fn commit(mut self) -> Result<(), ApiError> {
        let mut schedule = self.state.lock().map_err(|_| {
            ApiError::InternalServerError("Cleanup state lock poisoned".to_string())
        })?;
        schedule.last_completed_at = Some(Instant::now());
        schedule.in_progress = false;
        self.finished = true;
        Ok(())
    }
}

impl Drop for CleanupReservation<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        match self.state.lock() {
            Ok(mut schedule) => schedule.in_progress = false,
            Err(_) => error!(message = "Failed to release poisoned cleanup schedule"),
        }
    }
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

fn start_task_lease_heartbeat(
    pool: DbPool,
    task: &TaskRecord,
    initial_confirmed_expiry: TokioInstant,
) -> Option<TaskLeaseHeartbeat> {
    let claim_token = task.lease_token?;
    let settings = task_worker_settings();
    let task_id = task.id;
    Some(spawn_task_lease_monitor(
        task_id,
        claim_token,
        settings,
        initial_confirmed_expiry,
        move || {
            let pool = pool.clone();
            async move { renew_task_lease(&pool, task_id, claim_token, settings.lease_duration).await }
        },
    ))
}

fn spawn_task_lease_monitor<F, Fut>(
    task_id: i32,
    claim_token: uuid::Uuid,
    settings: TaskWorkerSettings,
    initial_confirmed_expiry: TokioInstant,
    renew: F,
) -> TaskLeaseHeartbeat
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<bool, ApiError>> + Send + 'static,
{
    let (stop_tx, mut stop_rx) = oneshot::channel();
    let (lost_tx, lost_rx) = oneshot::channel();
    // Task execution runs on a current-thread Actix runtime and may spend long
    // stretches in synchronous validation or rendering. Drive lease renewal on
    // a dedicated runtime thread so that work cannot starve its own heartbeat.
    let handle = TASK_LEASE_RUNTIME.spawn(async move {
        let lost = monitor_task_lease(
            task_id,
            claim_token,
            settings,
            initial_confirmed_expiry,
            renew,
            &mut stop_rx,
        )
        .await;
        if lost {
            let _ = lost_tx.send(());
        }
    });
    TaskLeaseHeartbeat {
        stop: stop_tx,
        handle,
        lost: lost_rx,
    }
}

async fn monitor_task_lease<F, Fut>(
    task_id: i32,
    claim_token: uuid::Uuid,
    settings: TaskWorkerSettings,
    mut confirmed_expiry: TokioInstant,
    mut renew: F,
    stop_rx: &mut oneshot::Receiver<()>,
) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool, ApiError>>,
{
    let mut next_heartbeat = TokioInstant::now() + settings.heartbeat_interval;
    loop {
        tokio::select! {
            biased;
            _ = &mut *stop_rx => return false,
            _ = sleep_until(confirmed_expiry) => {
                warn!(
                    message = "Task lease renewal deadline expired",
                    task_id,
                    claim_token = %claim_token,
                );
                return true;
            }
            _ = sleep_until(next_heartbeat) => {
                let renewal_started_at = TokioInstant::now();
                let renewal = renew();
                let result = tokio::select! {
                    biased;
                    _ = &mut *stop_rx => return false,
                    _ = sleep_until(confirmed_expiry) => {
                        warn!(
                            message = "Task lease renewal did not complete before the lease expired",
                            task_id,
                            claim_token = %claim_token,
                        );
                        return true;
                    }
                    result = renewal => result,
                };
                match result {
                    Ok(true) => {
                        // PostgreSQL extends the lease after this request starts, so anchoring the
                        // new deadline at request start is conservative even if the response is
                        // delayed in transit.
                        confirmed_expiry = renewal_started_at + settings.lease_duration;
                    }
                    Ok(false) => {
                        warn!(
                            message = "Task lease is no longer owned by this worker",
                            task_id,
                            claim_token = %claim_token,
                        );
                        return true;
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
                next_heartbeat = TokioInstant::now() + settings.heartbeat_interval;
            }
        }
    }
}

async fn wait_for_lost_task_lease(heartbeat: &mut Option<TaskLeaseHeartbeat>) {
    match heartbeat {
        Some(heartbeat) => {
            let _ = (&mut heartbeat.lost).await;
        }
        None => std::future::pending().await,
    }
}

async fn finalize_failure_while_lease_owned<Fut>(
    heartbeat: &mut Option<TaskLeaseHeartbeat>,
    finalization: Fut,
) -> Result<bool, ApiError>
where
    Fut: Future<Output = Result<(), ApiError>>,
{
    tokio::select! {
        biased;
        result = finalization => {
            result?;
            Ok(true)
        }
        _ = wait_for_lost_task_lease(heartbeat) => Ok(false),
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

async fn maybe_cleanup_expired_task_outputs(pool: &DbPool) -> Result<(), ApiError> {
    let cleanup_interval = task_worker_settings().export_output_cleanup_interval;
    let Some(reservation) = CleanupReservation::reserve(cleanup_state(), cleanup_interval)? else {
        return Ok(());
    };

    metrics::export_output_cleanup_run();
    match purge_expired_export_outputs(pool).await {
        Ok(deleted) => metrics::export_output_cleanup_deleted(deleted.len()),
        Err(error) => {
            metrics::export_output_cleanup_failed();
            return Err(error);
        }
    }
    match purge_expired_backup_outputs(pool).await {
        Ok(deleted) => metrics::export_output_cleanup_deleted(deleted.len()),
        Err(error) => {
            metrics::export_output_cleanup_failed();
            return Err(error);
        }
    }
    reservation.commit()?;

    Ok(())
}

fn duration_since(timestamp: chrono::NaiveDateTime) -> Option<Duration> {
    let elapsed = Utc::now()
        .naive_utc()
        .signed_duration_since(timestamp)
        .num_milliseconds();
    (elapsed >= 0).then(|| Duration::from_millis(elapsed as u64))
}

async fn process_claimed_task(
    context: &AppContext,
    task: &TaskRecord,
    backup_settings: &BackupSettings,
) -> Result<(), ApiError> {
    let pool = &context.db_pool;
    let task_kind = TaskKind::from_db(&task.kind)?;
    if task_kind == TaskKind::Reindex {
        return crate::db::traits::computed_field::execute_computed_reindex_task(pool, task).await;
    }
    let submitted_by = task.submitted_by.ok_or_else(|| {
        ApiError::BadRequest(
            "Submitting principal is no longer available for this task".to_string(),
        )
    })?;
    let principal = load_principal_by_id(context, submitted_by).await?;

    // Disabled-SA gate: queued service-account tasks must not run once the SA is
    // disabled (mirrors the immediate token-validation rejection).
    if principal_is_disabled(context, &principal).await? {
        return Err(ApiError::BadRequest(
            "Submitting service account is disabled; task will not run".to_string(),
        ));
    }

    if matches!(
        task_kind,
        TaskKind::Import | TaskKind::Export | TaskKind::Backup
    ) {
        require_unscoped_runtime_admin(context, &principal, task.submitted_token_scoped).await?;
    }

    // Reconstruct the submitting token's scope boundary from the snapshot,
    // failing closed on any unknown permission string. Import, export, and
    // backup are runtime-admin operations, so only remote calls consume scoped
    // snapshots.
    let snapshot_scopes: Option<Vec<Permissions>> =
        if task_kind == TaskKind::RemoteCall && task.submitted_token_scoped {
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
                parsed.push(Permissions::from_string(raw)?);
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

    match task_kind {
        TaskKind::Import => execute_import_task(context, task, &principal).await,
        TaskKind::Export => execute_export_task(pool, task, &principal).await,
        TaskKind::Backup => {
            execute_backup_task(context, task, &principal, scopes, backup_settings).await
        }
        TaskKind::RemoteCall => execute_remote_call_task(context, task, &principal, scopes).await,
        TaskKind::Reindex => unreachable!("reindex tasks are dispatched before principal loading"),
    }
}

pub(super) async fn mark_claimed_task_failed(
    pool: &DbPool,
    task: &TaskRecord,
    err: &ApiError,
) -> Result<(), ApiError> {
    let task_kind = TaskKind::from_db(&task.kind)?;
    let task = if task_kind == TaskKind::Reindex {
        task.find_record(pool).await?
    } else {
        task.clone()
    };
    let summary = sanitize_error_for_storage(err);
    if task_kind == TaskKind::Reindex {
        crate::db::traits::computed_field::mark_computed_reindex_failed(pool, &task, &summary)
            .await?;
    }
    let counts = match task_kind {
        TaskKind::Import => task.count_import_results(pool).await?,
        TaskKind::Export => TaskResultCounts::new(1, 0, 1)?,
        TaskKind::RemoteCall => TaskResultCounts::new(1, 0, 1)?,
        TaskKind::Backup => TaskResultCounts::new(1, 0, 1)?,
        TaskKind::Reindex => TaskResultCounts::new(task.processed_items, task.success_items, 1)?,
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

#[cfg(test)]
mod cleanup_tests {
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    use rstest::rstest;

    use super::{CleanupReservation, CleanupSchedule};

    #[rstest]
    #[case::failure(false, false)]
    #[case::success(true, true)]
    fn cleanup_reservation_updates_schedule_only_after_commit(
        #[case] commit: bool,
        #[case] expected_scheduled: bool,
    ) {
        let state = Mutex::new(CleanupSchedule::default());
        let reservation = CleanupReservation::reserve(&state, Duration::from_secs(300))
            .expect("cleanup reservation")
            .expect("cleanup is due");
        if commit {
            reservation.commit().expect("commit cleanup reservation");
        } else {
            drop(reservation);
        }

        let schedule = state.lock().expect("cleanup schedule");
        assert_eq!(
            (schedule.last_completed_at.is_some(), schedule.in_progress),
            (expected_scheduled, false)
        );
    }

    #[rstest]
    #[case::recent(false, Some(Instant::now()))]
    #[case::in_progress(true, None)]
    fn unavailable_cleanup_schedule_is_not_reserved_again(
        #[case] in_progress: bool,
        #[case] last_completed_at: Option<Instant>,
    ) {
        let state = Mutex::new(CleanupSchedule {
            last_completed_at,
            in_progress,
        });

        assert!(
            CleanupReservation::reserve(&state, Duration::from_secs(300))
                .expect("cleanup reservation")
                .is_none()
        );
    }
}

#[cfg(test)]
mod lease_heartbeat_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use actix_rt::time::timeout;

    use super::*;

    #[test]
    fn heartbeat_progresses_while_task_runtime_thread_is_blocked() {
        let renewal_attempts = Arc::new(AtomicUsize::new(0));
        let attempts = renewal_attempts.clone();
        let settings = TaskWorkerSettings::new(
            1,
            Duration::from_millis(10),
            Duration::from_millis(500),
            Duration::from_millis(10),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap();

        actix_rt::System::new().block_on(async move {
            let heartbeat = spawn_task_lease_monitor(
                1,
                uuid::Uuid::new_v4(),
                settings,
                TokioInstant::now() + settings.lease_duration,
                move || {
                    attempts.fetch_add(1, Ordering::Relaxed);
                    async { Ok(true) }
                },
            );

            // This blocks the same current-thread runtime used by task
            // execution. A heartbeat spawned with `tokio::spawn` here cannot
            // make progress until the sleep finishes.
            std::thread::sleep(Duration::from_millis(100));

            assert!(renewal_attempts.load(Ordering::Relaxed) > 0);
            heartbeat.stop().await;
        });
    }

    #[test]
    fn lease_loss_is_detected_while_task_runtime_thread_is_blocked() {
        let settings = TaskWorkerSettings::new(
            1,
            Duration::from_millis(10),
            Duration::from_millis(75),
            Duration::from_millis(10),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap();

        actix_rt::System::new().block_on(async move {
            let mut heartbeat = spawn_task_lease_monitor(
                1,
                uuid::Uuid::new_v4(),
                settings,
                TokioInstant::now() + settings.lease_duration,
                || async {
                    Err(ApiError::DbConnectionError(
                        "database unavailable".to_string(),
                    ))
                },
            );

            std::thread::sleep(Duration::from_millis(150));

            heartbeat
                .lost
                .try_recv()
                .expect("lease loss should be signalled by the dedicated runtime");
            heartbeat.stop().await;
        });
    }

    #[tokio::test]
    async fn heartbeat_stays_running_until_failure_finalization_completes() {
        let stopped = Arc::new(AtomicBool::new(false));
        let stopped_by_handle = stopped.clone();
        let (stop_tx, stop_rx) = oneshot::channel();
        let (_lost_tx, lost_rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            let _ = stop_rx.await;
            stopped_by_handle.store(true, Ordering::Release);
        });
        let mut heartbeat = Some(TaskLeaseHeartbeat {
            stop: stop_tx,
            handle,
            lost: lost_rx,
        });

        let finalized = finalize_failure_while_lease_owned(&mut heartbeat, async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(!stopped.load(Ordering::Acquire));
            Ok(())
        })
        .await
        .unwrap();

        assert!(finalized);
        heartbeat.unwrap().stop().await;
        assert!(stopped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn lease_pool_remains_available_when_execution_pool_is_exhausted() {
        let config = get_config().expect("test requires database configuration");
        let execution_settings = DatabasePoolSettings::builder(config.database_url.clone())
            .max_size(1)
            .statement_timeout_ms(config.db_statement_timeout_ms)
            .acquire_timeout_ms(config.db_pool_acquire_timeout_ms)
            .build()
            .unwrap();
        let execution_pool = init_pool_with_settings(&execution_settings);
        let _execution_connection = execution_pool.get().await.unwrap();

        let lease_pool = new_task_lease_pool();
        timeout(Duration::from_secs(5), lease_pool.get())
            .await
            .expect("lease checkout must not wait for the execution pool")
            .expect("lease pool should connect to the test database");
    }

    #[tokio::test]
    async fn renewal_errors_signal_loss_at_the_confirmed_expiry() {
        let settings = TaskWorkerSettings::new(
            1,
            Duration::from_millis(10),
            Duration::from_millis(60),
            Duration::from_millis(10),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap();
        let renewal_attempts = AtomicUsize::new(0);
        let (_stop_tx, mut stop_rx) = oneshot::channel();
        let confirmed_expiry = TokioInstant::now() + settings.lease_duration;

        let lost = timeout(
            Duration::from_millis(250),
            monitor_task_lease(
                1,
                uuid::Uuid::new_v4(),
                settings,
                confirmed_expiry,
                || {
                    renewal_attempts.fetch_add(1, Ordering::Relaxed);
                    async {
                        Err(ApiError::DbConnectionError(
                            "database unavailable".to_string(),
                        ))
                    }
                },
                &mut stop_rx,
            ),
        )
        .await
        .expect("heartbeat must stop no later than the confirmed lease expiry");

        assert!(lost);
        assert!(renewal_attempts.load(Ordering::Relaxed) > 0);
    }
}
