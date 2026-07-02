use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Once, OnceLock};
use std::thread;
use std::time::Duration;

use actix_rt::time::sleep;
use tokio::sync::Notify;
use tracing::{error, info};

use crate::config::{
    DEFAULT_EVENT_FANOUT_BATCH_SIZE, DEFAULT_EVENT_FANOUT_LOCK_TIMEOUT_MS,
    DEFAULT_EVENT_FANOUT_POLL_INTERVAL_MS, DEFAULT_EVENT_FANOUT_WORKERS, get_config,
};
use crate::db::DbPool;
use crate::db::traits::event_fanout::{EventFanoutSettings, process_event_fanout_batch};
use crate::errors::ApiError;
use crate::models::EventWorkerWakeupStats;

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
        .map(|config| config.event_fanout_workers)
        .unwrap_or(DEFAULT_EVENT_FANOUT_WORKERS)
}

fn configured_event_fanout_poll_interval() -> Duration {
    let interval_ms = get_config()
        .map(|config| config.event_fanout_poll_interval_ms)
        .unwrap_or(DEFAULT_EVENT_FANOUT_POLL_INTERVAL_MS);
    Duration::from_millis(interval_ms)
}

fn configured_event_fanout_settings() -> EventFanoutSettings {
    get_config()
        .map(|config| EventFanoutSettings {
            batch_size: config.event_fanout_batch_size,
            lock_timeout_ms: config.event_fanout_lock_timeout_ms,
        })
        .unwrap_or(EventFanoutSettings {
            batch_size: DEFAULT_EVENT_FANOUT_BATCH_SIZE,
            lock_timeout_ms: DEFAULT_EVENT_FANOUT_LOCK_TIMEOUT_MS,
        })
}

pub(super) fn fanout_worker_should_continue(result: &Result<usize, ApiError>) -> bool {
    match result {
        Ok(processed) => *processed > 0,
        Err(error) => {
            error!(message = "Event fan-out worker iteration failed", error = %error);
            false
        }
    }
}

async fn wait_for_event_fanout_wakeup(poll_interval: Duration) {
    tokio::select! {
        _ = sleep(poll_interval) => {
            EVENT_FANOUT_POLL_WAKEUPS.fetch_add(1, Ordering::Relaxed);
        }
        _ = get_event_fanout_notify().notified() => {
            EVENT_FANOUT_NOTIFICATION_WAKEUPS.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn event_fanout_worker_loop(
    pool: DbPool,
    settings: EventFanoutSettings,
    poll_interval: Duration,
) {
    loop {
        let result = process_event_fanout_batch(&pool, settings).await;
        if fanout_worker_should_continue(&result) {
            continue;
        }
        wait_for_event_fanout_wakeup(poll_interval).await;
    }
}

fn spawn_event_fanout_worker_loop(
    pool: DbPool,
    settings: EventFanoutSettings,
    poll_interval: Duration,
    worker_index: usize,
) {
    thread::Builder::new()
        .name(format!("event-fanout-worker-{worker_index}"))
        .spawn(move || {
            info!(
                message = "Starting event fan-out worker loop",
                worker_index = worker_index,
                batch_size = settings.batch_size,
                lock_timeout_ms = settings.lock_timeout_ms,
                poll_interval = ?poll_interval
            );
            let system = actix_rt::System::new();
            system.block_on(event_fanout_worker_loop(pool, settings, poll_interval));
        })
        .expect("failed to spawn event fan-out worker thread");
}

pub fn ensure_event_fanout_worker_running(pool: DbPool) {
    let worker_count = configured_event_fanout_worker_count();
    if worker_count == 0 {
        return;
    }

    let poll_interval = configured_event_fanout_poll_interval();
    let settings = configured_event_fanout_settings();

    EVENT_FANOUT_LISTENER.call_once({
        let pool = pool.clone();
        move || {
            super::pg_notify::spawn_postgres_notification_listener(
                pool,
                super::pg_notify::EVENT_FANOUT_CHANNEL,
                "event-fanout-pg-listener",
                wake_event_fanout_worker_from_postgres,
            );
        }
    });

    EVENT_FANOUT_WORKER.call_once(move || {
        info!(
            message = "Initializing event fan-out workers",
            worker_count = worker_count,
            batch_size = settings.batch_size,
            lock_timeout_ms = settings.lock_timeout_ms,
            poll_interval = ?poll_interval
        );
        for worker_index in 0..worker_count {
            spawn_event_fanout_worker_loop(pool.clone(), settings, poll_interval, worker_index);
        }
    });
}

pub fn kick_event_fanout_worker(pool: DbPool) {
    ensure_event_fanout_worker_running(pool);
    EVENT_FANOUT_NOTIFICATIONS_SENT.fetch_add(1, Ordering::Relaxed);
    get_event_fanout_notify().notify_one();
}

pub fn event_fanout_wakeup_stats() -> EventWorkerWakeupStats {
    EventWorkerWakeupStats {
        notifications_sent: EVENT_FANOUT_NOTIFICATIONS_SENT.load(Ordering::Relaxed),
        notification_wakeups: EVENT_FANOUT_NOTIFICATION_WAKEUPS.load(Ordering::Relaxed),
        poll_wakeups: EVENT_FANOUT_POLL_WAKEUPS.load(Ordering::Relaxed),
    }
}
