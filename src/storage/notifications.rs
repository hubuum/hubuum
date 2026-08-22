use crate::lifecycle::spawn_background_worker;
use tracing::debug;

use super::{StorageHandle, StorageNotification};

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
    if !storage.has_worker_notification_provider() {
        debug!(
            message = "storage backend has no worker notification provider; polling remains active",
            topic = topic.as_str(),
        );
        return;
    }
    spawn_background_worker(worker_name, move |shutdown| {
        let listener = storage
            .create_worker_notification_listener(
                topic,
                on_notification,
                Box::pin(async move { shutdown.requested().await }),
            )
            .expect("notification provider remains attached for the handle lifetime");
        let system = actix_rt::System::new();
        system.block_on(listener);
    });
}
