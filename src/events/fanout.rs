use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Once, OnceLock};
use std::time::Duration;

use actix_rt::time::sleep;
use tokio::sync::Notify;
use tracing::{error, info};

use crate::config::{
    DEFAULT_EVENT_FANOUT_BATCH_SIZE, DEFAULT_EVENT_FANOUT_LOCK_TIMEOUT_MS,
    DEFAULT_EVENT_FANOUT_POLL_INTERVAL_MS, DEFAULT_EVENT_FANOUT_WORKERS, get_config,
};
use crate::errors::ApiError;
use crate::events::EventFanoutSettings;
use crate::lifecycle::{ShutdownSignal, spawn_background_worker};
use crate::models::{EventWorkerHealth, EventWorkerWakeupStats};
use crate::observability::metrics;
use crate::restores::MaintenanceActivityGuard;
use crate::storage::StorageContext;
use crate::storage::{EventFanoutStorage, StorageError, StorageHandle, storage_handle};
use crate::storage::{StorageCallSite, with_storage_call_site};

static EVENT_FANOUT_WORKER: Once = Once::new();
static EVENT_FANOUT_LISTENER: Once = Once::new();
static EVENT_FANOUT_NOTIFY: OnceLock<Notify> = OnceLock::new();
static EVENT_FANOUT_NOTIFICATIONS_SENT: AtomicU64 = AtomicU64::new(0);
static EVENT_FANOUT_NOTIFICATION_WAKEUPS: AtomicU64 = AtomicU64::new(0);
static EVENT_FANOUT_POLL_WAKEUPS: AtomicU64 = AtomicU64::new(0);

fn get_event_fanout_notify() -> &'static Notify {
    EVENT_FANOUT_NOTIFY.get_or_init(Notify::new)
}

fn wake_event_fanout_worker_from_postgres() {
    get_event_fanout_notify().notify_one();
}

fn configured_event_fanout_worker_count() -> usize {
    get_config()
        .map(|config| {
            config
                .runtime_role
                .effective_worker_count(config.event_fanout_workers)
        })
        .unwrap_or(DEFAULT_EVENT_FANOUT_WORKERS)
}

fn configured_event_fanout_poll_interval() -> Duration {
    let interval_ms = get_config()
        .map(|config| config.event_fanout_poll_interval_ms)
        .unwrap_or(DEFAULT_EVENT_FANOUT_POLL_INTERVAL_MS);
    Duration::from_millis(interval_ms)
}

fn configured_event_fanout_settings() -> Result<EventFanoutSettings, ApiError> {
    match get_config() {
        Ok(config) => config.event_fanout_settings(),
        Err(_) => EventFanoutSettings::new(
            DEFAULT_EVENT_FANOUT_BATCH_SIZE,
            DEFAULT_EVENT_FANOUT_LOCK_TIMEOUT_MS,
        )
        .map_err(Into::into),
    }
}

pub(super) fn fanout_worker_should_continue(result: &Result<usize, StorageError>) -> bool {
    match result {
        Ok(processed) => *processed > 0,
        Err(error) => {
            error!(message = "Event fan-out worker iteration failed", error = %error);
            false
        }
    }
}

async fn wait_for_event_fanout_wakeup(poll_interval: Duration, shutdown: &ShutdownSignal) -> bool {
    tokio::select! {
        biased;
        _ = shutdown.requested() => false,
        _ = sleep(poll_interval) => {
            EVENT_FANOUT_POLL_WAKEUPS.fetch_add(1, Ordering::Relaxed);
            metrics::event_worker_wakeup("fanout", "poll");
            true
        }
        _ = get_event_fanout_notify().notified() => {
            EVENT_FANOUT_NOTIFICATION_WAKEUPS.fetch_add(1, Ordering::Relaxed);
            metrics::event_worker_wakeup("fanout", "notification");
            true
        }
    }
}

async fn event_fanout_worker_loop(
    pool: StorageHandle,
    settings: EventFanoutSettings,
    poll_interval: Duration,
    shutdown: ShutdownSignal,
) {
    loop {
        let activity = MaintenanceActivityGuard::begin();
        let result = tokio::select! {
            biased;
            _ = shutdown.requested() => break,
            result = with_storage_call_site(
                &pool,
                StorageCallSite::EventFanout,
                pool.process_event_fanout_batch(settings),
            ) => result,
        };
        drop(activity);
        if fanout_worker_should_continue(&result) {
            continue;
        }
        if !wait_for_event_fanout_wakeup(poll_interval, &shutdown).await {
            break;
        }
    }
}

fn spawn_event_fanout_worker_loop(
    pool: StorageHandle,
    settings: EventFanoutSettings,
    poll_interval: Duration,
    worker_index: usize,
) {
    spawn_background_worker(
        format!("event-fanout-worker-{worker_index}"),
        move |shutdown| {
            info!(
                message = "Starting event fan-out worker loop",
                worker_index = worker_index,
                batch_size = settings.batch_size(),
                lock_timeout_ms = settings.lock_timeout_ms(),
                poll_interval = ?poll_interval
            );
            let system = actix_rt::System::new();
            system.block_on(event_fanout_worker_loop(
                pool,
                settings,
                poll_interval,
                shutdown,
            ));
        },
    );
}

pub fn ensure_event_fanout_worker_running<C>(backend: C)
where
    C: StorageContext,
{
    let pool = storage_handle(&backend);
    let worker_count = configured_event_fanout_worker_count();
    if worker_count == 0 {
        return;
    }

    let poll_interval = configured_event_fanout_poll_interval();
    let settings = match configured_event_fanout_settings() {
        Ok(settings) => settings,
        Err(error) => {
            error!(message = "Event fan-out settings are invalid", error = %error);
            return;
        }
    };

    EVENT_FANOUT_LISTENER.call_once(|| {
        super::pg_notify::spawn_postgres_notification_listener(
            super::pg_notify::EVENT_FANOUT_CHANNEL,
            "event-fanout-pg-listener",
            wake_event_fanout_worker_from_postgres,
        );
    });

    EVENT_FANOUT_WORKER.call_once(move || {
        info!(
            message = "Initializing event fan-out workers",
            worker_count = worker_count,
            batch_size = settings.batch_size(),
            lock_timeout_ms = settings.lock_timeout_ms(),
            poll_interval = ?poll_interval
        );
        for worker_index in 0..worker_count {
            spawn_event_fanout_worker_loop(pool.clone(), settings, poll_interval, worker_index);
        }
    });
}

pub fn kick_event_fanout_worker<C>(backend: C)
where
    C: StorageContext,
{
    ensure_event_fanout_worker_running(backend);
    EVENT_FANOUT_NOTIFICATIONS_SENT.fetch_add(1, Ordering::Relaxed);
    metrics::event_worker_wakeup("fanout", "notifications_sent");
    get_event_fanout_notify().notify_one();
}

pub fn event_fanout_wakeup_stats() -> EventWorkerWakeupStats {
    EventWorkerWakeupStats {
        notifications_sent: EVENT_FANOUT_NOTIFICATIONS_SENT.load(Ordering::Relaxed),
        notification_wakeups: EVENT_FANOUT_NOTIFICATION_WAKEUPS.load(Ordering::Relaxed),
        poll_wakeups: EVENT_FANOUT_POLL_WAKEUPS.load(Ordering::Relaxed),
    }
}

pub(crate) fn event_fanout_worker_health() -> EventWorkerHealth {
    let config = get_config().ok();
    EventWorkerHealth {
        workers_configured: configured_event_fanout_worker_count(),
        batch_size: config
            .as_ref()
            .map(|config| config.event_fanout_batch_size)
            .unwrap_or(DEFAULT_EVENT_FANOUT_BATCH_SIZE),
        poll_interval_ms: config
            .as_ref()
            .map(|config| config.event_fanout_poll_interval_ms)
            .unwrap_or(DEFAULT_EVENT_FANOUT_POLL_INTERVAL_MS),
        lock_timeout_ms: config
            .as_ref()
            .map(|config| config.event_fanout_lock_timeout_ms)
            .unwrap_or(DEFAULT_EVENT_FANOUT_LOCK_TIMEOUT_MS),
        wakeups: event_fanout_wakeup_stats(),
    }
}
