use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};

use crate::storage::{ExecutionStorage, StorageError, StorageHandle};

static ACTIVE_MAINTENANCE_WORK: LazyLock<Arc<AtomicUsize>> =
    LazyLock::new(|| Arc::new(AtomicUsize::new(0)));
static ABANDONED_APPLICATION_WORK: LazyLock<Arc<AtomicBool>> =
    LazyLock::new(|| Arc::new(AtomicBool::new(false)));

/// Counts local request/worker activity across the maintenance state check and
/// the work it protects. The restore coordinator reports this instance drained
/// only after every such guard has dropped.
/// The activity counter can only be acquired through `begin`.
///
/// ```compile_fail
/// use hubuum::restores::MaintenanceActivityGuard;
/// let guard = MaintenanceActivityGuard;
/// ```
pub struct MaintenanceActivityGuard {
    counter: Arc<AtomicUsize>,
}

impl MaintenanceActivityGuard {
    pub fn begin() -> Self {
        Self::begin_with_counter(Arc::clone(&ACTIVE_MAINTENANCE_WORK))
    }

    fn begin_with_counter(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        Self { counter }
    }

    #[cfg(test)]
    pub(crate) fn isolated() -> (Arc<AtomicUsize>, Self) {
        let counter = Arc::new(AtomicUsize::new(0));
        let guard = Self::begin_with_counter(Arc::clone(&counter));
        (counter, guard)
    }
}

impl Drop for MaintenanceActivityGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(crate) fn active_maintenance_work() -> usize {
    ACTIVE_MAINTENANCE_WORK.load(Ordering::Acquire)
}

pub(crate) fn local_work_is_settled() -> bool {
    active_maintenance_work() == 0
        && crate::middlewares::maintenance::http_work_is_settled()
        && application_work_is_settled()
        && hubuum_templates::template_workers_idle()
}

pub(crate) fn application_work_is_settled() -> bool {
    !ABANDONED_APPLICATION_WORK.load(Ordering::Acquire)
}

struct ApplicationWorkCompletion {
    abandoned: Arc<AtomicBool>,
    complete: bool,
}

impl Drop for ApplicationWorkCompletion {
    fn drop(&mut self) {
        if !self.complete {
            self.abandoned.store(true, Ordering::Release);
        }
    }
}

/// Cancellation cannot establish that external sends or detached work settled.
/// Retain that uncertainty until an independent process-exclusion recovery.
pub(crate) async fn track_application_completion<F: Future>(future: F) -> F::Output {
    track_completion(future, Arc::clone(&ABANDONED_APPLICATION_WORK)).await
}

async fn track_completion<F: Future>(future: F, abandoned: Arc<AtomicBool>) -> F::Output {
    let mut completion = ApplicationWorkCompletion {
        abandoned,
        complete: false,
    };
    let result = future.await;
    completion.complete = true;
    result
}

/// Admit one ordinary worker batch before starting any of its storage work.
/// Admission inherits an existing permit, so nested work cannot refresh an old
/// producer after a deletion barrier or application replacement.
pub(crate) async fn with_application_work<F, T, E>(
    storage: &StorageHandle,
    future: F,
) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
    E: From<StorageError>,
{
    let _activity = MaintenanceActivityGuard::begin();
    let permit = storage.admit_application_work().await?;
    let scope = storage
        .current_execution_scope()
        .with_application_access(permit);
    storage
        .run_in_scope(scope, track_application_completion(future))
        .await
}

#[cfg(test)]
mod tests;
