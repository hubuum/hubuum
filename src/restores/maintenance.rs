use std::sync::atomic::{AtomicUsize, Ordering};

static ACTIVE_MAINTENANCE_WORK: AtomicUsize = AtomicUsize::new(0);

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
    counter: &'static AtomicUsize,
}

impl MaintenanceActivityGuard {
    pub fn begin() -> Self {
        ACTIVE_MAINTENANCE_WORK.fetch_add(1, Ordering::AcqRel);
        Self {
            counter: &ACTIVE_MAINTENANCE_WORK,
        }
    }
}

impl Drop for MaintenanceActivityGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(super) fn active_maintenance_work() -> usize {
    ACTIVE_MAINTENANCE_WORK.load(Ordering::Acquire)
}
