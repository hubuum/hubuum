use std::{sync::Arc, time::Duration};

use tokio::sync::Notify;

use super::StorageHandle;
use crate::lifecycle::{ShutdownSignal, spawn_background_worker};
use crate::storage::StorageNotification;
use crate::storage::query_observations::CONTROL_REFRESH_INTERVAL;

impl StorageHandle {
    /// Each process participates, including API-only replicas.
    pub(crate) fn start_query_observation_control(&self) {
        let storage = self.clone();
        spawn_background_worker("query-observation-control", move |shutdown| {
            actix_rt::System::new().block_on(
                storage.run_query_observation_control(shutdown, CONTROL_REFRESH_INTERVAL),
            );
        });
    }

    async fn run_query_observation_control(
        &self,
        shutdown: ShutdownSignal,
        refresh_interval: Duration,
    ) {
        let wakeup = Arc::new(Notify::new());
        let notified = wakeup.clone();
        let listener_shutdown = shutdown.clone();
        let listener = self.create_worker_notification_listener(
            StorageNotification::QueryObservationControl,
            Box::new(move || notified.notify_one()),
            Box::pin(async move { listener_shutdown.requested().await }),
        );
        let refresh = async {
            let mut interval = tokio::time::interval_at(
                tokio::time::Instant::now() + refresh_interval,
                refresh_interval,
            );
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    () = shutdown.requested() => break,
                    () = wakeup.notified() => {},
                    _ = interval.tick() => {},
                }
                // The collector pauses and logs on a failed read. Polling and
                // a wake-up after LISTEN registration recover missed changes.
                let _ = self.query_observation_configuration().await;
            }
        };
        if let Some(listener) = listener {
            tokio::join!(listener, refresh);
        } else {
            refresh.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hubuum_storage_core::{
        QueryUsageStorage, StorageNotificationListener, StorageNotificationShutdown,
        StorageQueryObservationControl, WorkerNotificationProvider,
    };
    use hubuum_storage_memory::MemoryStorage;
    use tokio::sync::watch;

    struct Notifications {
        changes: watch::Sender<()>,
        listening: Arc<Notify>,
    }

    impl WorkerNotificationProvider for Notifications {
        fn listen_for_worker_notifications(
            &self,
            topic: StorageNotification,
            on_notification: Box<dyn Fn() + Send + Sync>,
            shutdown: StorageNotificationShutdown,
        ) -> StorageNotificationListener {
            assert_eq!(topic, StorageNotification::QueryObservationControl);
            let mut changes = self.changes.subscribe();
            let listening = self.listening.clone();
            Box::pin(async move {
                tokio::pin!(shutdown);
                on_notification();
                listening.notify_one();
                loop {
                    tokio::select! {
                        () = &mut shutdown => break,
                        result = changes.changed() => {
                            if result.is_err() { break; }
                            on_notification();
                        }
                    }
                }
            })
        }
    }

    #[rstest::rstest]
    #[case::enable_notification(true, true)]
    #[case::disable_notification(false, true)]
    #[case::enable_missed_notification(true, false)]
    #[case::disable_missed_notification(false, false)]
    #[tokio::test]
    async fn replicas_apply_changes_without_waiting_for_a_query(
        #[case] enabled: bool,
        #[case] notify: bool,
    ) {
        let backend = MemoryStorage::new();
        let writer = StorageHandle::from_registered_backend(backend.clone());
        writer
            .set_query_observation_control(StorageQueryObservationControl::from_override(Some(
                !enabled,
            )))
            .await
            .unwrap();
        let (changes, _) = watch::channel(());
        let listening = Arc::new(Notify::new());
        let reader = StorageHandle::from_registered_backend(backend)
            .with_worker_notification_provider(Arc::new(Notifications {
                changes: changes.clone(),
                listening: listening.clone(),
            }));
        reader.query_observation_configuration().await.unwrap();
        let shutdown = ShutdownSignal::new();
        let worker = reader.clone();
        let worker_shutdown = shutdown.clone();
        // Notification cases cannot be rescued by the periodic refresh.
        let interval = if notify {
            Duration::from_secs(60)
        } else {
            Duration::from_millis(20)
        };
        let task = tokio::spawn(async move {
            worker
                .run_query_observation_control(worker_shutdown, interval)
                .await;
        });
        listening.notified().await;
        tokio::task::yield_now().await;
        writer
            .set_query_observation_control(StorageQueryObservationControl::from_override(Some(
                enabled,
            )))
            .await
            .unwrap();
        if notify {
            changes.send(()).unwrap();
        }
        tokio::time::timeout(Duration::from_millis(750), async {
            loop {
                let active = reader
                    .inner
                    .query_observations
                    .collection_enabled(async {
                        panic!("the background worker must apply the change before cache expiry")
                    })
                    .await;
                if active == enabled {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("replica must apply the shared value promptly");
        shutdown.request();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
    }
}
