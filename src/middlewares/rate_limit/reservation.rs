use std::fmt;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use uuid::Uuid;

/// Identifies one attempt across all scopes and both limiter backends.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub(super) struct ReservationId(Uuid);

impl fmt::Display for ReservationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

struct ReservationState {
    id: ReservationId,
    expires_at: Instant,
}

/// The permit is the sole owner. Memory scopes keep weak leases, so cancellation
/// invalidates their capacity synchronously, without locking or spawning cleanup.
pub(super) struct Reservation(Arc<ReservationState>);

impl Reservation {
    pub(super) fn new(window: Duration) -> Self {
        Self(Arc::new(ReservationState {
            id: ReservationId(Uuid::new_v4()),
            expires_at: Instant::now() + Self::lifetime(window),
        }))
    }

    /// Preserve the shared backend's existing bounded reservation lifetime.
    pub(super) fn lifetime(window: Duration) -> Duration {
        window.clamp(Duration::from_secs(5), Duration::from_secs(60))
    }

    pub(super) fn id(&self) -> ReservationId {
        self.0.id
    }

    pub(super) fn lease(&self) -> ReservationLease {
        ReservationLease(Arc::downgrade(&self.0))
    }
}

pub(super) struct ReservationLease(Weak<ReservationState>);

impl ReservationLease {
    pub(super) fn is_active(&self, now: Instant) -> bool {
        self.0
            .upgrade()
            .is_some_and(|reservation| now < reservation.expires_at)
    }
}
