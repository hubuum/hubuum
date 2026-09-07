#[cfg(feature = "integration-test-support")]
use std::future::Future;
#[cfg(feature = "integration-test-support")]
use std::sync::Arc;
#[cfg(feature = "integration-test-support")]
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

#[cfg(feature = "integration-test-support")]
use diesel::QueryableByName;
#[cfg(feature = "integration-test-support")]
use diesel_async::RunQueryDsl;
#[cfg(feature = "integration-test-support")]
use tokio::sync::Notify;
#[cfg(feature = "integration-test-support")]
use tracing::error;

use crate::{PostgresConnection, PostgresStorageError};

/// Adapter-private deterministic failure seams used by native verification.
///
/// The enum is exposed only through the `integration-test-support` feature.
/// Production callers cannot select or activate a fault.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PostgresFaultPoint {
    CollectionCreateAfterRecords,
    EventDeliveryAfterClaim,
    EventDeliveryBeforeAcknowledge,
    PageAfterCount,
    RestoreAfterDrainTransition,
    RestoreCoordinatorAfterHeartbeat,
    TaskFinalizeAfterEvent,
    ImportAfterCommit,
    TaskLeaseBeforeRenewal,
    TransactionBeforeCommit,
}

#[cfg(feature = "integration-test-support")]
impl PostgresFaultPoint {
    const fn as_str(self) -> &'static str {
        match self {
            Self::CollectionCreateAfterRecords => "collection_create_after_records",
            Self::EventDeliveryAfterClaim => "event_delivery_after_claim",
            Self::EventDeliveryBeforeAcknowledge => "event_delivery_before_acknowledge",
            Self::PageAfterCount => "page_after_count",
            Self::RestoreAfterDrainTransition => "restore_after_drain_transition",
            Self::RestoreCoordinatorAfterHeartbeat => "restore_coordinator_after_heartbeat",
            Self::ImportAfterCommit => "import_after_commit",
            Self::TaskFinalizeAfterEvent => "task_finalize_after_event",
            Self::TaskLeaseBeforeRenewal => "task_lease_before_renewal",
            Self::TransactionBeforeCommit => "transaction_before_commit",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg(feature = "integration-test-support")]
enum FaultMode {
    Fail,
    Pause,
}

#[cfg(feature = "integration-test-support")]
struct FaultState {
    point: PostgresFaultPoint,
    mode: FaultMode,
    reached: AtomicBool,
    backend_pid: AtomicI32,
    reached_notification: Notify,
    resume_notification: Notify,
}

/// Evidence emitted when a paused fault seam is reached.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg(feature = "integration-test-support")]
pub struct PostgresFaultReached {
    backend_pid: Option<i32>,
}

#[cfg(feature = "integration-test-support")]
impl PostgresFaultReached {
    /// PostgreSQL backend PID for a seam reached while holding a connection.
    #[must_use]
    pub const fn backend_pid(self) -> Option<i32> {
        self.backend_pid
    }
}

/// Task-local deterministic fault activation.
///
/// A controller affects only the future passed to [`Self::run`]. This prevents
/// parallel tests from injecting failures into unrelated storage work.
#[doc(hidden)]
#[derive(Clone)]
#[cfg(feature = "integration-test-support")]
pub struct PostgresFaultController {
    state: Arc<FaultState>,
}

#[cfg(feature = "integration-test-support")]
impl PostgresFaultController {
    #[must_use]
    pub fn failing(point: PostgresFaultPoint) -> Self {
        Self::new(point, FaultMode::Fail)
    }

    #[must_use]
    pub fn pausing(point: PostgresFaultPoint) -> Self {
        Self::new(point, FaultMode::Pause)
    }

    fn new(point: PostgresFaultPoint, mode: FaultMode) -> Self {
        Self {
            state: Arc::new(FaultState {
                point,
                mode,
                reached: AtomicBool::new(false),
                backend_pid: AtomicI32::new(0),
                reached_notification: Notify::new(),
                resume_notification: Notify::new(),
            }),
        }
    }

    pub async fn run<T>(&self, future: impl Future<Output = T>) -> T {
        ACTIVE_FAULT.scope(self.state.clone(), future).await
    }

    /// Wait until the selected seam has been reached.
    pub async fn wait_until_reached(&self) -> PostgresFaultReached {
        while !AtomicBool::load(&self.state.reached, Ordering::Acquire) {
            self.state.reached_notification.notified().await;
        }
        let backend_pid = AtomicI32::load(&self.state.backend_pid, Ordering::Acquire);
        PostgresFaultReached {
            backend_pid: (backend_pid > 0).then_some(backend_pid),
        }
    }

    /// Release a future paused at the selected seam.
    pub fn resume(&self) {
        self.state.resume_notification.notify_one();
    }
}

#[cfg(feature = "integration-test-support")]
tokio::task_local! {
    static ACTIVE_FAULT: Arc<FaultState>;
}

#[derive(QueryableByName)]
#[cfg(feature = "integration-test-support")]
struct BackendPid {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    backend_pid: i32,
}

/// Reach one deterministic adapter failure seam.
#[doc(hidden)]
#[cfg(feature = "integration-test-support")]
pub async fn reach_fault_point(
    point: PostgresFaultPoint,
    mut connection: Option<&mut PostgresConnection>,
) -> Result<(), PostgresStorageError> {
    let Some(state) = ACTIVE_FAULT
        .try_with(Arc::clone)
        .ok()
        .filter(|state| state.point == point)
    else {
        return Ok(());
    };

    let backend_pid = match connection.as_mut() {
        Some(connection) => {
            diesel::sql_query("SELECT pg_backend_pid() AS backend_pid")
                .get_result::<BackendPid>(*connection)
                .await?
                .backend_pid
        }
        None => 0,
    };
    state.backend_pid.store(backend_pid, Ordering::Release);
    state.reached.store(true, Ordering::Release);
    state.reached_notification.notify_one();

    match state.mode {
        FaultMode::Fail => {
            error!(
                message = "Injected PostgreSQL adapter failure",
                backend = "postgresql",
                fault_point = point.as_str(),
            );
            Err(PostgresStorageError::database(format!(
                "injected PostgreSQL failure at {}",
                point.as_str()
            )))
        }
        FaultMode::Pause => {
            state.resume_notification.notified().await;
            Ok(())
        }
    }
}

#[cfg(not(feature = "integration-test-support"))]
pub(crate) async fn reach_fault_point(
    _point: PostgresFaultPoint,
    _connection: Option<&mut PostgresConnection>,
) -> Result<(), PostgresStorageError> {
    Ok(())
}
