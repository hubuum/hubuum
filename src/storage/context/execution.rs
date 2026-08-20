use super::*;

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
        dispatch_backend!(self, |backend| backend.run_in_scope_send(scope, future))
    }
}

impl WorkerNotificationStorage for StorageHandle {
    fn listen_for_worker_notifications(
        &self,
        topic: StorageNotification,
        on_notification: fn(),
        shutdown: StorageNotificationShutdown,
    ) -> StorageNotificationListener {
        let backend_name = self.backend_name();
        self.observe_infallible_storage_call(
            backend_name,
            "worker_notifications",
            "create_listener",
            || {
                debug!(
                    message = "storage worker notification listener created",
                    topic = topic.as_str(),
                );
                dispatch_backend!(self, |backend| backend.listen_for_worker_notifications(
                    topic,
                    on_notification,
                    shutdown,
                ))
            },
        )
    }
}
