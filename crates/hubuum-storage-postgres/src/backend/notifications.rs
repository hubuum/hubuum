use super::*;

impl WorkerNotificationStorage for PostgresStorage {
    fn worker_notification_listener(
        &self,
        topic: StorageNotification,
        on_notification: fn(),
        shutdown: StorageNotificationShutdown,
    ) -> StorageNotificationListener {
        let pool = self.notification_listener_pool();
        Box::pin(crate::worker_notifications::listen(
            pool,
            topic,
            on_notification,
            || {},
            shutdown,
        ))
    }
}
