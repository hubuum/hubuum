use crate::lifecycle::spawn_background_worker;

use super::{StorageHandle, StorageNotification, WorkerNotificationStorage};

/// Start one application-supervised backend notification listener.
///
/// Notifications are wake-up hints only. The durable worker polling path
/// remains the source of correctness if a listener exits or reconnects.
pub(crate) fn spawn_storage_notification_listener(
    storage: StorageHandle,
    topic: StorageNotification,
    worker_name: &'static str,
    on_notification: fn(),
) {
    spawn_background_worker(worker_name, move |shutdown| {
        let system = actix_rt::System::new();
        system.block_on(storage.worker_notification_listener(
            topic,
            on_notification,
            Box::pin(async move { shutdown.requested().await }),
        ));
    });
}
