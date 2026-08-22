use std::future::Future;
use std::pin::Pin;

/// Backend-neutral wake-up topics used by Hubuum's durable workers.
///
/// Notifications are only an optimization: workers retain their polling path
/// and storage remains the source of truth.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageNotification {
    EventFanout,
    EventDelivery,
    TaskQueue,
}

impl StorageNotification {
    /// Every topic an attached notification provider must support.
    pub const ALL: [Self; 3] = [Self::EventFanout, Self::EventDelivery, Self::TaskQueue];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EventFanout => "event_fanout",
            Self::EventDelivery => "event_delivery",
            Self::TaskQueue => "task_queue",
        }
    }
}

/// Boxed application shutdown signal consumed by a backend listener.
pub type StorageNotificationShutdown = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Boxed backend listener that runs until its shutdown signal resolves.
pub type StorageNotificationListener = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Optional provider of backend-native listeners for durable-worker wake-ups.
///
/// A composed application may attach this provider when an adapter offers a
/// lower-latency wake-up mechanism. Polling remains the correctness path, so
/// implementing the storage aggregate never requires notifications. Process
/// and thread supervision remain application responsibilities.
pub trait WorkerNotificationProvider: Send + Sync {
    fn listen_for_worker_notifications(
        &self,
        topic: StorageNotification,
        on_notification: fn(),
        shutdown: StorageNotificationShutdown,
    ) -> StorageNotificationListener;
}

#[cfg(test)]
mod tests {
    use super::StorageNotification;

    #[test]
    fn notification_topics_have_stable_bounded_names() {
        assert_eq!(
            StorageNotification::ALL,
            [
                StorageNotification::EventFanout,
                StorageNotification::EventDelivery,
                StorageNotification::TaskQueue,
            ]
        );
        assert_eq!(StorageNotification::EventFanout.as_str(), "event_fanout");
        assert_eq!(
            StorageNotification::EventDelivery.as_str(),
            "event_delivery"
        );
        assert_eq!(StorageNotification::TaskQueue.as_str(), "task_queue");
    }
}
