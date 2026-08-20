use std::time::Duration;

/// One completed logical storage operation observed at the application edge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageObservation {
    backend: &'static str,
    capability: &'static str,
    operation: &'static str,
    result: &'static str,
    duration: Duration,
}

impl StorageObservation {
    #[must_use]
    pub const fn new(
        backend: &'static str,
        capability: &'static str,
        operation: &'static str,
        result: &'static str,
        duration: Duration,
    ) -> Self {
        Self {
            backend,
            capability,
            operation,
            result,
            duration,
        }
    }

    #[must_use]
    pub const fn backend(&self) -> &'static str {
        self.backend
    }

    #[must_use]
    pub const fn capability(&self) -> &'static str {
        self.capability
    }

    #[must_use]
    pub const fn operation(&self) -> &'static str {
        self.operation
    }

    #[must_use]
    pub const fn result(&self) -> &'static str {
        self.result
    }

    #[must_use]
    pub const fn duration(&self) -> Duration {
        self.duration
    }
}

/// Application-owned observer for backend-neutral storage operations.
///
/// Storage adapters and wrappers report observations through this trait. They
/// do not select a metrics registry, exporter, or global telemetry provider.
pub trait StorageObserver: Send + Sync {
    fn operation_finished(&self, observation: &StorageObservation);
}
