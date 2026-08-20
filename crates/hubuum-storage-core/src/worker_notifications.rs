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
    /// Every notification topic a complete backend must support.
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

/// Provides backend-native listeners for durable-worker wake-ups.
///
/// A selectable backend must implement every topic. Application workers do
/// not need to know whether the adapter uses PostgreSQL LISTEN/NOTIFY or a
/// different backend-native mechanism. Process and thread supervision remain
/// application responsibilities.
pub trait WorkerNotificationStorage: Send + Sync {
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
