use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use tokio::sync::Notify;
use tracing::{error, info, warn};

const JOIN_POLL_INTERVAL: Duration = Duration::from_millis(10);
const SUPERVISION_POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Clone, Debug)]
pub struct ShutdownSignal {
    state: Arc<ShutdownState>,
}

#[derive(Debug)]
struct ShutdownState {
    requested: AtomicBool,
    notify: Notify,
}

impl ShutdownSignal {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(ShutdownState {
                requested: AtomicBool::new(false),
                notify: Notify::new(),
            }),
        }
    }

    pub(crate) fn is_requested(&self) -> bool {
        self.state.requested.load(Ordering::Acquire)
    }

    pub(crate) fn request(&self) {
        if !self.state.requested.swap(true, Ordering::AcqRel) {
            self.state.notify.notify_waiters();
        }
    }

    pub(crate) async fn requested(&self) {
        loop {
            let notified = self.state.notify.notified();
            if self.is_requested() {
                return;
            }
            notified.await;
        }
    }
}

struct NamedWorkerHandle {
    name: String,
    handle: JoinHandle<()>,
}

struct BackgroundWorkers {
    shutdown: ShutdownSignal,
    handles: Mutex<Vec<NamedWorkerHandle>>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ShutdownReport {
    pub joined: usize,
    pub panicked: Vec<String>,
    pub timed_out: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BackgroundWorkerExit {
    NoWorkers,
    Stopped { name: String },
}

impl std::fmt::Display for BackgroundWorkerExit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoWorkers => formatter.write_str("no background workers were started"),
            Self::Stopped { name } => write!(formatter, "background worker '{name}' stopped"),
        }
    }
}

impl BackgroundWorkers {
    fn new() -> Self {
        Self {
            shutdown: ShutdownSignal::new(),
            handles: Mutex::new(Vec::new()),
        }
    }

    fn spawn<F>(&self, name: impl Into<String>, run: F)
    where
        F: FnOnce(ShutdownSignal) + Send + 'static,
    {
        let name = name.into();
        let thread_name = name.clone();
        let shutdown = self.shutdown.clone();
        let handle = thread::Builder::new()
            .name(thread_name.clone())
            .spawn(move || {
                run(shutdown);
                info!(message = "Background worker stopped", worker = thread_name);
            })
            .unwrap_or_else(|error| panic!("failed to spawn {name}: {error}"));

        self.handles
            .lock()
            .expect("background worker handle lock poisoned")
            .push(NamedWorkerHandle { name, handle });
    }

    async fn shutdown(&self, timeout: Duration) -> ShutdownReport {
        let started_at = Instant::now();
        self.shutdown.request();
        let mut handles = {
            let mut handles = self
                .handles
                .lock()
                .expect("background worker handle lock poisoned");
            std::mem::take(&mut *handles)
        };
        let worker_count = handles.len();
        info!(
            message = "Background worker shutdown started",
            worker_count,
            timeout_ms = timeout.as_millis()
        );

        while handles.iter().any(|worker| !worker.handle.is_finished())
            && started_at.elapsed() < timeout
        {
            tokio::time::sleep(
                JOIN_POLL_INTERVAL.min(timeout.saturating_sub(started_at.elapsed())),
            )
            .await;
        }

        let mut report = ShutdownReport::default();
        for worker in handles.drain(..) {
            if !worker.handle.is_finished() {
                report.timed_out.push(worker.name);
                continue;
            }
            match worker.handle.join() {
                Ok(()) => report.joined += 1,
                Err(_) => report.panicked.push(worker.name),
            }
        }

        if report.panicked.is_empty() && report.timed_out.is_empty() {
            info!(
                message = "Background worker shutdown completed",
                joined = report.joined,
                elapsed_ms = started_at.elapsed().as_millis()
            );
        } else {
            if !report.panicked.is_empty() {
                error!(
                    message = "Background workers panicked during shutdown",
                    workers = ?report.panicked
                );
            }
            if !report.timed_out.is_empty() {
                warn!(
                    message = "Background worker shutdown timed out",
                    workers = ?report.timed_out,
                    timeout_ms = timeout.as_millis()
                );
            }
        }

        report
    }

    fn worker_count(&self) -> usize {
        self.handles
            .lock()
            .expect("background worker handle lock poisoned")
            .len()
    }

    async fn wait_for_unexpected_exit(&self) -> BackgroundWorkerExit {
        loop {
            let exit = {
                let handles = self
                    .handles
                    .lock()
                    .expect("background worker handle lock poisoned");
                if handles.is_empty() {
                    Some(BackgroundWorkerExit::NoWorkers)
                } else {
                    handles
                        .iter()
                        .find(|worker| worker.handle.is_finished())
                        .map(|worker| BackgroundWorkerExit::Stopped {
                            name: worker.name.clone(),
                        })
                }
            };
            if let Some(exit) = exit {
                return exit;
            }
            tokio::time::sleep(SUPERVISION_POLL_INTERVAL).await;
        }
    }
}

static BACKGROUND_WORKERS: LazyLock<BackgroundWorkers> = LazyLock::new(BackgroundWorkers::new);

pub fn spawn_background_worker<F>(name: impl Into<String>, run: F)
where
    F: FnOnce(ShutdownSignal) + Send + 'static,
{
    BACKGROUND_WORKERS.spawn(name, run);
}

pub async fn shutdown_background_workers(timeout: Duration) -> ShutdownReport {
    BACKGROUND_WORKERS.shutdown(timeout).await
}

pub fn background_worker_count() -> usize {
    BACKGROUND_WORKERS.worker_count()
}

pub async fn wait_for_background_worker_exit() -> BackgroundWorkerExit {
    BACKGROUND_WORKERS.wait_for_unexpected_exit().await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    #[actix_rt::test]
    async fn shutdown_joins_an_idle_worker() {
        let workers = BackgroundWorkers::new();
        workers.spawn("idle-worker", |shutdown| {
            actix_rt::System::new().block_on(shutdown.requested());
        });

        let report = workers.shutdown(Duration::from_secs(1)).await;

        assert_eq!(report.joined, 1);
        assert!(report.panicked.is_empty());
        assert!(report.timed_out.is_empty());
    }

    #[actix_rt::test]
    async fn shutdown_interrupts_a_worker_in_polling_sleep() {
        let workers = BackgroundWorkers::new();
        workers.spawn("sleeping-worker", |shutdown| {
            actix_rt::System::new().block_on(async move {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(60)) => {}
                    _ = shutdown.requested() => {}
                }
            });
        });

        let report = workers.shutdown(Duration::from_secs(1)).await;

        assert_eq!(report.joined, 1);
        assert!(report.timed_out.is_empty());
    }

    #[actix_rt::test]
    async fn shutdown_cancels_an_in_progress_iteration() {
        let workers = BackgroundWorkers::new();
        let iteration_dropped = Arc::new(AtomicBool::new(false));
        let dropped = iteration_dropped.clone();
        workers.spawn("busy-worker", move |shutdown| {
            struct DropMarker(Arc<AtomicBool>);
            impl Drop for DropMarker {
                fn drop(&mut self) {
                    self.0.store(true, Ordering::Release);
                }
            }

            actix_rt::System::new().block_on(async move {
                let marker = DropMarker(dropped);
                tokio::select! {
                    _ = std::future::pending::<()>() => {}
                    _ = shutdown.requested() => {}
                }
                drop(marker);
            });
        });

        let report = workers.shutdown(Duration::from_secs(1)).await;

        assert_eq!(report.joined, 1);
        assert!(iteration_dropped.load(Ordering::Acquire));
    }

    #[actix_rt::test]
    async fn supervision_rejects_an_empty_worker_set() {
        let workers = BackgroundWorkers::new();

        let exit = workers.wait_for_unexpected_exit().await;

        assert_eq!(exit, BackgroundWorkerExit::NoWorkers);
    }

    #[actix_rt::test]
    async fn supervision_reports_a_worker_that_stops() {
        let workers = BackgroundWorkers::new();
        workers.spawn("stopping-worker", |_| {});

        let exit = tokio::time::timeout(Duration::from_secs(1), workers.wait_for_unexpected_exit())
            .await
            .expect("supervision should notice the stopped worker");

        assert_eq!(
            exit,
            BackgroundWorkerExit::Stopped {
                name: "stopping-worker".to_string(),
            }
        );
        let report = workers.shutdown(Duration::from_secs(1)).await;
        assert_eq!(report.joined, 1);
    }
}
