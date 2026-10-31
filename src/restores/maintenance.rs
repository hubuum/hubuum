use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};

static ACTIVE_MAINTENANCE_WORK: LazyLock<Arc<AtomicUsize>> =
    LazyLock::new(|| Arc::new(AtomicUsize::new(0)));

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
