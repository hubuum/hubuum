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
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EventFanout => "event_fanout",
            Self::EventDelivery => "event_delivery",
            Self::TaskQueue => "task_queue",
        }
    }
}

/// Starts backend-native listeners for durable-worker wake-ups.
///
/// A selectable backend must implement every topic. Application workers do
/// not need to know whether the adapter uses PostgreSQL LISTEN/NOTIFY or a
/// different backend-native mechanism.
pub trait WorkerNotificationStorage: Send + Sync {
    fn spawn_worker_notification_listener(
        &self,
        topic: StorageNotification,
        worker_name: &'static str,
        on_notification: fn(),
    );
}

#[cfg(test)]
mod tests {
    use super::StorageNotification;

    #[test]
    fn notification_topics_have_stable_bounded_names() {
        assert_eq!(StorageNotification::EventFanout.as_str(), "event_fanout");
        assert_eq!(
            StorageNotification::EventDelivery.as_str(),
            "event_delivery"
        );
        assert_eq!(StorageNotification::TaskQueue.as_str(), "task_queue");
    }
}
