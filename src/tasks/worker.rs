use std::future::Future;
use std::sync::{LazyLock, Mutex, Once, OnceLock};
use std::time::{Duration, Instant};

use actix_rt::time::{Instant as TokioInstant, sleep, sleep_until};
use chrono::Utc;
use tokio::sync::{Notify, oneshot};
use tracing::{Instrument, error, field, info, info_span, warn};

use crate::backups::{BackupSettings, execute_backup_task};
use crate::config::{
    DEFAULT_BACKUP_MAX_ACTIVE_TASKS_PER_USER, DEFAULT_BACKUP_MAX_OUTPUT_BYTES,
    DEFAULT_BACKUP_OUTPUT_RETENTION_HOURS, DEFAULT_EXPORT_OUTPUT_CLEANUP_INTERVAL_SECONDS,
    DEFAULT_TASK_HEARTBEAT_SECONDS, DEFAULT_TASK_LEASE_SECONDS, DEFAULT_TASK_POLL_INTERVAL_MS,
    DEFAULT_TASK_RECOVERY_INTERVAL_SECONDS, get_config,
};
use crate::errors::ApiError;
use crate::exports::execute_export_task;
use crate::lifecycle::{ShutdownSignal, spawn_background_worker};
use crate::models::principal::load_principal_by_id;
use crate::models::{NewTaskEventRecord, TaskKind, TaskStatus};
use crate::observability::{metrics, tracing as telemetry};
#[cfg(test)]
use crate::permissions::LocalPermissionBackend;
use crate::permissions::{AppContext, require_unscoped_runtime_admin};
use crate::restores::{MaintenanceActivityGuard, current_maintenance_state};
use crate::services::identity::is_service_account_disabled;
use crate::services::tasks::{
    ClaimedTask, claim_next_task, fail_task, purge_expired_backup_outputs,
    purge_expired_export_outputs, recover_expired_task_leases, renew_task_lease,
};
use crate::storage::{
    StorageCallSite, StorageNotification, spawn_storage_notification_listener,
    with_mutation_provenance, with_storage_call_site, with_storage_call_site_send,
};

use super::TaskWorkerSettings;
use super::execution::execute_import_task;
use super::helpers::sanitize_error_for_storage;
use super::remote_call::execute_remote_call_task;
use super::types::WorkerLoopAction;

static TASK_WORKER: Once = Once::new();
static TASK_WORKER_LISTENER: Once = Once::new();
static TASK_WORKER_NOTIFY: OnceLock<Notify> = OnceLock::new();
static TASK_OUTPUT_CLEANUP_STATE: OnceLock<Mutex<CleanupSchedule>> = OnceLock::new();
static TASK_RECOVERY_STATE: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();
static TASK_WORKER_SETTINGS: OnceLock<TaskWorkerSettings> = OnceLock::new();
static TASK_LEASE_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("task-lease-heartbeat")
        .enable_all()
        .build()
        .expect("task lease heartbeat runtime must start")
});

pub fn initialize_task_worker_settings(settings: TaskWorkerSettings) -> Result<(), String> {
    TASK_WORKER_SETTINGS
        .set(settings)
        .map_err(|_| "task worker settings were already initialized".to_string())?;
    metrics::task_worker_config(settings.worker_count(), settings.poll_interval());
    Ok(())
}

fn get_task_worker_notify() -> &'static Notify {
    TASK_WORKER_NOTIFY.get_or_init(Notify::new)
}

fn wake_task_worker_from_storage() {
    get_task_worker_notify().notify_one();
}

fn cleanup_state() -> &'static Mutex<CleanupSchedule> {
    TASK_OUTPUT_CLEANUP_STATE.get_or_init(|| Mutex::new(CleanupSchedule::default()))
}

fn recovery_state() -> &'static Mutex<Option<Instant>> {
    TASK_RECOVERY_STATE.get_or_init(|| Mutex::new(None))
}

fn task_worker_settings() -> TaskWorkerSettings {
    TASK_WORKER_SETTINGS.get().copied().unwrap_or_else(|| {
        TaskWorkerSettings::builder()
            .worker_count(1)
            .poll_interval(Duration::from_millis(DEFAULT_TASK_POLL_INTERVAL_MS))
            .lease_duration(Duration::from_secs(DEFAULT_TASK_LEASE_SECONDS))
            .heartbeat_interval(Duration::from_secs(DEFAULT_TASK_HEARTBEAT_SECONDS))
            .recovery_interval(Duration::from_secs(DEFAULT_TASK_RECOVERY_INTERVAL_SECONDS))
            .export_output_cleanup_interval(Duration::from_secs(
                DEFAULT_EXPORT_OUTPUT_CLEANUP_INTERVAL_SECONDS,
            ))
            .build()
            .expect("default task worker settings must be valid")
    })
}

fn configured_task_worker_count() -> usize {
    task_worker_settings().worker_count()
}

fn configured_task_poll_interval() -> Duration {
    task_worker_settings().poll_interval()
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
        // Task execution dispatches several large async implementations. Keep
        // that future behind a pointer so the task-local attribution wrapper
        // does not push the worker thread's stack frame over its default size.
        let iteration = Box::pin(process_one_task_with_settings(
            &context,
            Some(&shutdown),
            &backup_settings,
        ));
        let result = with_storage_call_site(&context, StorageCallSite::TaskWorker, iteration).await;
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

#[cfg(not(any(test, feature = "integration-test-support")))]
fn task_worker_context(context: AppContext) -> AppContext {
    context
}

/// Async Postgres connections are tied to the runtime that established them.
/// Test cases each own a short-lived Actix runtime, while the background worker
/// thread is process-global. Build the test worker's pool on its own long-lived
/// runtime so it never inherits connections driven by a completed test runtime.
#[cfg(any(test, feature = "integration-test-support"))]
fn task_worker_context(context: AppContext) -> AppContext {
    drop(context);
    crate::tests::background_worker_app_context()
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
    TASK_WORKER_LISTENER.call_once(|| {
        spawn_storage_notification_listener(
            context.backend().clone(),
            StorageNotification::TaskQueue,
            "task-worker-storage-listener",
            wake_task_worker_from_storage,
        );
    });
    TASK_WORKER.call_once(move || {
        info!(
            message = "Initializing task workers",
            worker_count = worker_count,
            poll_interval = ?poll_interval
        );
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
pub(super) async fn process_claimed_task_for_test(
    storage: &impl crate::storage::StorageContext,
    task: &ClaimedTask,
) -> Result<(), ApiError> {
    let admin_groupname = get_config()
        .map(|config| config.admin_groupname.clone())
        .unwrap_or_else(|_| "admin".to_string());
    let storage = crate::storage::storage_handle(storage);
    let permissions = std::sync::Arc::new(LocalPermissionBackend::new(
        storage.clone(),
        admin_groupname,
    ));
    let context = AppContext::new(storage, permissions);

    if let Err(error) = process_claimed_task(&context, task, &configured_backup_settings()).await {
        mark_claimed_task_failed(&context, task, &error).await?;
    }
    Ok(())
}

async fn process_one_task_with_settings(
    context: &AppContext,
    shutdown: Option<&ShutdownSignal>,
    backup_settings: &BackupSettings,
) -> Result<bool, ApiError> {
    let _activity = MaintenanceActivityGuard::begin();
    if !current_maintenance_state(context.backend())
        .await?
        .is_normal()
    {
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
    let task = match claim_next_task(context, settings.lease_duration()).await {
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

    let execution_span = info_span!(
        "task.execute",
        otel.kind = "consumer",
        task.kind = task.kind.as_str(),
        task.outcome = field::Empty,
        task.attempt = task.attempt_count,
    );
    telemetry::add_link(&execution_span, task.trace_link.as_ref());
    async {
        info!(
            message = "Task picked up by worker",
            task_id = task.id,
            task_kind = task.kind.as_str(),
            status = task.status.as_str(),
            worker = std::thread::current().name().unwrap_or("task-worker")
        );

        let provenance = task
            .worker_provenance()
            .with_trace_link(telemetry::current_trace_link());
        with_mutation_provenance(context, Some(provenance), async {
        let mut heartbeat = start_task_lease_heartbeat(
            context.backend().clone(),
            &task,
            claim_started_at + settings.lease_duration(),
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
        let mut terminal_status = result.as_ref().ok().copied();
        if let Err(err) = &result
            && !ownership_lost
        {
            let finalized = finalize_failure_while_lease_owned(
                &mut heartbeat,
                mark_claimed_task_failed(context, &task, err),
            )
            .await?;
            if finalized {
                terminal_status = Some(TaskStatus::Failed);
            } else {
                warn!(
                    message = "Task failure finalization stopped because its worker lease was lost",
                    task_id = task.id,
                );
            }
        }
        if let Some(heartbeat) = heartbeat {
            heartbeat.stop().await;
        }

        tracing::Span::current().record(
            "task.outcome",
            task_span_outcome(terminal_status),
        );

        Ok(true)
        })
        .await
    }
    .instrument(execution_span)
    .await
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
    storage: crate::storage::StorageHandle,
    task: &ClaimedTask,
    initial_confirmed_expiry: TokioInstant,
) -> Option<TaskLeaseHeartbeat> {
    let settings = task_worker_settings();
    let task_id = task.id;
    let lease = task.lease().clone();
    Some(spawn_task_lease_monitor(
        task_id,
        settings,
        initial_confirmed_expiry,
        move || {
            let storage = storage.clone();
            let lease = lease.clone();
            async move {
                with_storage_call_site_send(
                    &storage,
                    StorageCallSite::TaskLease,
                    renew_task_lease(&storage, lease, settings.lease_duration()),
                )
                .await
            }
        },
    ))
}

fn spawn_task_lease_monitor<F, Fut>(
    task_id: i32,
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
    settings: TaskWorkerSettings,
    mut confirmed_expiry: TokioInstant,
    mut renew: F,
    stop_rx: &mut oneshot::Receiver<()>,
) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool, ApiError>>,
{
    let mut next_heartbeat = TokioInstant::now() + settings.heartbeat_interval();
    loop {
        tokio::select! {
            biased;
            _ = &mut *stop_rx => return false,
            _ = sleep_until(confirmed_expiry) => {
                warn!(
                    message = "Task lease renewal deadline expired",
                    task_id,
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
                        confirmed_expiry = renewal_started_at + settings.lease_duration();
                    }
                    Ok(false) => {
                        warn!(
                            message = "Task lease is no longer owned by this worker",
                            task_id,
                        );
                        return true;
                    }
                    Err(error) => {
                        warn!(
                            message = "Failed to renew task worker lease",
                            task_id,
                            error = %error,
                        );
                    }
                }
                next_heartbeat = TokioInstant::now() + settings.heartbeat_interval();
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

async fn maybe_recover_expired_task_leases(
    backend: &impl crate::storage::StorageContext,
) -> Result<(), ApiError> {
    let recovery_interval = task_worker_settings().recovery_interval();
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

    match recover_expired_task_leases(backend, 100).await {
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

async fn maybe_cleanup_expired_task_outputs(
    backend: &impl crate::storage::StorageContext,
) -> Result<(), ApiError> {
    let cleanup_interval = task_worker_settings().export_output_cleanup_interval();
    let Some(reservation) = CleanupReservation::reserve(cleanup_state(), cleanup_interval)? else {
        return Ok(());
    };

    metrics::task_output_cleanup_run(metrics::TaskOutputKind::Export);
    let deleted_exports = match purge_expired_export_outputs(backend).await {
        Ok(deleted) => deleted,
        Err(error) => {
            metrics::task_output_cleanup_failed(metrics::TaskOutputKind::Export);
            return Err(error);
        }
    };
    metrics::task_output_cleanup_deleted(metrics::TaskOutputKind::Export, deleted_exports);

    metrics::task_output_cleanup_run(metrics::TaskOutputKind::Backup);
    let deleted_backups = match purge_expired_backup_outputs(backend).await {
        Ok(deleted) => deleted,
        Err(error) => {
            metrics::task_output_cleanup_failed(metrics::TaskOutputKind::Backup);
            return Err(error);
        }
    };
    metrics::task_output_cleanup_deleted(metrics::TaskOutputKind::Backup, deleted_backups);
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
    task: &ClaimedTask,
    backup_settings: &BackupSettings,
) -> Result<TaskStatus, ApiError> {
    let task_kind = TaskKind::from_db(&task.kind)?;
    if task_kind == TaskKind::Reindex {
        let completed =
            crate::services::tasks::execute_computed_field_rebuild(context, task).await?;
        return TaskStatus::from_db(&completed.status);
    }
    let submitted_by = task.submitted_by.ok_or_else(|| {
        ApiError::BadRequest(
            "Submitting principal is no longer available for this task".to_string(),
        )
    })?;
    let principal = load_principal_by_id(context, submitted_by).await?;

    // Disabled-SA gate: queued service-account tasks must not run once the SA is
    // disabled (mirrors the immediate token-validation rejection).
    if is_service_account_disabled(context, principal.id).await? {
        return Err(ApiError::BadRequest(
            "Submitting service account is disabled; task will not run".to_string(),
        ));
    }

    if task_kind == TaskKind::Backup {
        require_unscoped_runtime_admin(context, &principal, task.submitted_token_scoped).await?;
    }

    // Reconstruct the submitting token's scope boundary from the snapshot,
    // failing closed on any unknown permission or resource entry. Import,
    // export, and remote-call workers enforce the persisted boundary.
    let snapshot_scope = if task.submitted_token_scoped {
        Some(crate::models::TokenScope::from_snapshot_json(
            &task.submitted_token_scopes,
        )?)
    } else {
        None
    };
    let scopes = snapshot_scope.as_ref();

    info!(
        message = "Dispatching task execution",
        task_id = task.id,
        task_kind = task.kind.as_str(),
        status = task.status.as_str(),
        submitted_by = principal.id,
        scoped = task.submitted_token_scoped
    );

    match task_kind {
        TaskKind::Import => execute_import_task(context, task, &principal, scopes).await,
        TaskKind::Export => execute_export_task(context, task, &principal, scopes).await,
        TaskKind::Backup => {
            execute_backup_task(context, task, &principal, scopes, backup_settings).await
        }
        TaskKind::RemoteCall => execute_remote_call_task(context, task, &principal, scopes).await,
        TaskKind::Reindex => unreachable!("reindex tasks are dispatched before principal loading"),
    }
}

fn task_span_outcome(terminal_status: Option<TaskStatus>) -> &'static str {
    terminal_status.map(TaskStatus::as_str).unwrap_or("error")
}

#[cfg(test)]
mod task_span_outcome_tests {
    use rstest::rstest;

    use super::{TaskStatus, task_span_outcome};

    #[rstest]
    #[case::succeeded(Some(TaskStatus::Succeeded), "succeeded")]
    #[case::failed(Some(TaskStatus::Failed), "failed")]
    #[case::partially_succeeded(Some(TaskStatus::PartiallySucceeded), "partially_succeeded")]
    #[case::not_persisted(None, "error")]
    fn span_outcome_reports_the_persisted_terminal_status(
        #[case] status: Option<TaskStatus>,
        #[case] expected: &str,
    ) {
        assert_eq!(task_span_outcome(status), expected);
    }
}

pub(super) async fn mark_claimed_task_failed(
    backend: &impl crate::storage::StorageContext,
    task: &ClaimedTask,
    err: &ApiError,
) -> Result<(), ApiError> {
    let summary = sanitize_error_for_storage(err);

    warn!(
        message = "Claimed task failed",
        task_id = task.id,
        task_kind = task.kind.as_str(),
        status = task.status.as_str(),
        error = %err
    );

    fail_task(
        backend,
        task,
        summary.clone(),
        NewTaskEventRecord {
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
    fn lease_diagnostics_do_not_log_claim_tokens() {
        let source = include_str!("worker.rs");
        let direct_token_field = ["claim_", "token = %"].concat();
        let optional_token_field = ["claim_", "token = ?task.lease_token"].concat();

        assert!(!source.contains(&direct_token_field));
        assert!(!source.contains(&optional_token_field));
    }

    fn lease_test_settings(
        lease_duration: Duration,
        heartbeat_interval: Duration,
    ) -> TaskWorkerSettings {
        TaskWorkerSettings::builder()
            .worker_count(1)
            .poll_interval(Duration::from_millis(10))
            .lease_duration(lease_duration)
            .heartbeat_interval(heartbeat_interval)
            .recovery_interval(Duration::from_secs(1))
            .export_output_cleanup_interval(Duration::from_secs(1))
            .build()
            .unwrap()
    }

    #[test]
    fn heartbeat_progresses_while_task_runtime_thread_is_blocked() {
        let renewal_attempts = Arc::new(AtomicUsize::new(0));
        let attempts = renewal_attempts.clone();
        let settings = lease_test_settings(Duration::from_millis(500), Duration::from_millis(10));

        actix_rt::System::new().block_on(async move {
            let heartbeat = spawn_task_lease_monitor(
                1,
                settings,
                TokioInstant::now() + settings.lease_duration(),
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
        let settings = lease_test_settings(Duration::from_millis(75), Duration::from_millis(10));

        actix_rt::System::new().block_on(async move {
            let mut heartbeat = spawn_task_lease_monitor(
                1,
                settings,
                TokioInstant::now() + settings.lease_duration(),
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
    async fn renewal_errors_signal_loss_at_the_confirmed_expiry() {
        let settings = lease_test_settings(Duration::from_millis(60), Duration::from_millis(10));
        let renewal_attempts = AtomicUsize::new(0);
        let (_stop_tx, mut stop_rx) = oneshot::channel();
        let confirmed_expiry = TokioInstant::now() + settings.lease_duration();

        let lost = timeout(
            Duration::from_millis(250),
            monitor_task_lease(
                1,
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
