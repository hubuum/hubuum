use super::*;
use hubuum_storage_core::StorageCallSite;

tokio::task_local! { static CALL_SITE: StorageCallSite; }

async fn track_call_site<F: Future>(site: Option<StorageCallSite>, work: F) -> F::Output {
    match site {
        Some(site) => CALL_SITE.scope(site, work).await,
        None => work.await,
    }
}

impl ExecutionStorage for StorageHandle {
    fn run_in_scope<'a, F, R>(
        &'a self,
        scope: StorageExecutionScope,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a,
    {
        let future = track_call_site(scope.call_site_override(), future);
        dispatch_backend!(self, |backend| backend.run_in_scope(scope, future))
    }

    fn run_in_scope_send<'a, F, R>(
        &'a self,
        scope: StorageExecutionScope,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + Send + 'a>>
    where
        F: Future<Output = R> + Send + 'a,
        R: Send + 'a,
    {
        let future = track_call_site(scope.call_site_override(), future);
        dispatch_backend!(self, |backend| backend.run_in_scope_send(scope, future))
    }
}

impl StorageHandle {
    pub(crate) fn current_call_site() -> StorageCallSite {
        CALL_SITE.try_with(|site| *site).unwrap_or_default()
    }

    pub(crate) fn has_worker_notification_provider(&self) -> bool {
        self.inner.worker_notification_provider.is_some()
    }

    pub(crate) fn create_worker_notification_listener(
        &self,
        topic: StorageNotification,
        on_notification: Box<dyn Fn() + Send + Sync>,
        shutdown: StorageNotificationShutdown,
    ) -> Option<StorageNotificationListener> {
        let provider = self.inner.worker_notification_provider.clone()?;
        debug!(
            message = "storage worker notification listener created",
            topic = topic.as_str(),
        );
        Some(provider.listen_for_worker_notifications(topic, on_notification, shutdown))
    }
}
