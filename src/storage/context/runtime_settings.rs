use std::{sync::Arc, time::Duration};

use futures_util::future::BoxFuture;
use hubuum_storage_core::{
    RuntimeSettingsStorage, StorageQueryObservationSettings, StorageRuntimeSettingsChange,
    StorageRuntimeSettingsHistoryQuery, StorageRuntimeSettingsSnapshot,
    StorageRuntimeSettingsUpdate, StorageRuntimeSettingsUpdateOutcome,
};
use tokio::sync::Notify;

use super::*;
use crate::lifecycle::{ShutdownSignal, spawn_background_worker};
use crate::storage::runtime_settings::{
    AppliedRuntimeSettings, RUNTIME_SETTINGS_RECONCILE_INTERVAL,
};

#[async_trait]
impl RuntimeSettingsStorage for StorageHandle {
    async fn load_runtime_settings(&self) -> Result<StorageRuntimeSettingsSnapshot, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::RuntimeSettings,
            "load_runtime_settings",
            async { dispatch_backend!(self, |backend| backend.load_runtime_settings().await) },
        )
        .await
    }

    async fn update_runtime_settings(
        &self,
        request: StorageRuntimeSettingsUpdate,
    ) -> Result<StorageRuntimeSettingsUpdateOutcome, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::RuntimeSettings,
            "update_runtime_settings",
            async {
                dispatch_backend!(self, |backend| {
                    backend.update_runtime_settings(request).await
                })
            },
        )
        .await
    }

    async fn list_runtime_settings_changes(
        &self,
        query: StorageRuntimeSettingsHistoryQuery,
    ) -> Result<Vec<StorageRuntimeSettingsChange>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::RuntimeSettings,
            "list_runtime_settings_changes",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_runtime_settings_changes(query).await
                })
            },
        )
        .await
    }
}

impl StorageHandle {
    pub(crate) fn subscribe_runtime_settings(&self) -> tokio::sync::watch::Receiver<()> {
        self.inner.runtime_settings.subscribe()
    }

    /// Settings this process currently applies, without a storage read.
    pub(crate) fn applied_runtime_settings(&self) -> AppliedRuntimeSettings {
        self.inner.runtime_settings.current()
    }

    /// Reload the authoritative snapshot and apply it to this process.
    pub(crate) async fn refresh_runtime_settings(
        &self,
    ) -> Result<AppliedRuntimeSettings, StorageError> {
        self.inner
            .runtime_settings
            .refresh(async {
                let snapshot = self.load_runtime_settings().await?;
                let deployment = crate::config::get_config()
                    .map_err(|error| StorageError::invalid_input(error.to_string()))?;
                crate::config::runtime::resolve(deployment.clone(), Some(&snapshot))
                    .map_err(|error| StorageError::invalid_input(error.to_string()))?;
                Ok(snapshot)
            })
            .await
    }

    /// Query observation settings from this process's startup configuration.
    pub(crate) fn deployment_query_observation_settings(&self) -> StorageQueryObservationSettings {
        self.inner.query_observations.deployment_settings()
    }

    /// Effective query observation settings applied by this process.
    pub(crate) fn query_observation_settings(
        &self,
        applied: &AppliedRuntimeSettings,
    ) -> StorageQueryObservationSettings {
        self.inner.query_observations.settings(applied)
    }

    /// Follow committed changes. Every process participates, including
    /// API-only replicas, because each applies shared settings locally.
    pub(crate) fn start_runtime_settings_reconciler_with(
        &self,
        on_applied: impl Fn(AppliedRuntimeSettings) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) {
        let storage = self.clone();
        let on_applied = Arc::new(on_applied);
        spawn_background_worker("runtime-settings", move |shutdown| {
            let on_applied = on_applied.clone();
            actix_rt::System::new().block_on(storage.run_runtime_settings_reconciler_with(
                shutdown,
                RUNTIME_SETTINGS_RECONCILE_INTERVAL,
                on_applied,
            ));
        });
    }

    #[cfg(test)]
    async fn run_runtime_settings_reconciler(
        &self,
        shutdown: ShutdownSignal,
        refresh_interval: Duration,
    ) {
        self.run_runtime_settings_reconciler_with(
            shutdown,
            refresh_interval,
            Arc::new(|_| Box::pin(async {})),
        )
        .await;
    }

    async fn run_runtime_settings_reconciler_with(
        &self,
        shutdown: ShutdownSignal,
        refresh_interval: Duration,
        on_applied: Arc<dyn Fn(AppliedRuntimeSettings) -> BoxFuture<'static, ()> + Send + Sync>,
    ) {
        let wakeup = Arc::new(Notify::new());
        let notified = wakeup.clone();
        let listener_shutdown = shutdown.clone();
        // Notifications are wake-ups only. Listeners invoke the callback after
        // (re)registering, so each wake-up reloads authoritative state after
        // LISTEN is active and changes missed while disconnected are applied.
        let listener = self.create_worker_notification_listener(
            StorageNotification::RuntimeSettings,
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
                // Failures are logged and reported as staleness; the next
                // notification or poll retries.
                if let Ok(applied) = self.refresh_runtime_settings().await {
                    on_applied(applied).await;
                }
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
        StorageNotificationListener, StorageNotificationShutdown, StoragePaginationPolicy,
        StorageRuntimeSettingsAssignment, StorageRuntimeSettingsGroup, StorageRuntimeSettingsValue,
        WorkerNotificationProvider,
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
            assert_eq!(topic, StorageNotification::RuntimeSettings);
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

    async fn set_page_limit(storage: &StorageHandle, limit: u64) {
        let current = storage
            .load_runtime_settings()
            .await
            .unwrap()
            .record(StorageRuntimeSettingsGroup::Pagination)
            .revision();
        storage
            .update_runtime_settings(StorageRuntimeSettingsUpdate::new(
                StorageRuntimeSettingsAssignment::Managed(StorageRuntimeSettingsValue::Pagination(
                    StoragePaginationPolicy::try_new(limit).unwrap(),
                )),
                current,
                EventContext::system(),
            ))
            .await
            .unwrap();
    }

    fn applied_page_limit(storage: &StorageHandle) -> Option<u32> {
        storage
            .applied_runtime_settings()
            .snapshot()
            .and_then(|snapshot| snapshot.pagination())
            .map(StoragePaginationPolicy::default_page_limit)
    }

    #[rstest::rstest]
    #[case::notification(true)]
    #[case::missed_notification(false)]
    #[tokio::test]
    async fn replicas_apply_committed_changes_without_a_request(#[case] notify: bool) {
        let backend = MemoryStorage::new();
        let writer = StorageHandle::from_registered_backend(backend.clone());
        set_page_limit(&writer, 11).await;
        let (changes, _) = watch::channel(());
        let listening = Arc::new(Notify::new());
        let reader = StorageHandle::from_registered_backend(backend)
            .with_worker_notification_provider(Arc::new(Notifications {
                changes: changes.clone(),
                listening: listening.clone(),
            }));
        let shutdown = ShutdownSignal::new();
        let worker = reader.clone();
        let worker_shutdown = shutdown.clone();
        // Notification cases cannot be rescued by the periodic reconcile.
        let interval = if notify {
            Duration::from_secs(60)
        } else {
            Duration::from_millis(20)
        };
        let task = tokio::spawn(async move {
            worker
                .run_runtime_settings_reconciler(worker_shutdown, interval)
                .await;
        });
        listening.notified().await;
        // Registration triggers a reconcile, applying state committed earlier.
        tokio::time::timeout(Duration::from_millis(750), async {
            while applied_page_limit(&reader) != Some(11) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("registration must reconcile previously committed state");
        set_page_limit(&writer, 17).await;
        if notify {
            changes.send(()).unwrap();
        }
        tokio::time::timeout(Duration::from_millis(750), async {
            while applied_page_limit(&reader) != Some(17) {
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
