use super::*;

impl ExportQueryStorage for StorageHandle {
    fn run_export_queries<'a, F, R>(
        &'a self,
        budget: Option<StorageQueryBudget>,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a,
    {
        dispatch_backend!(self, |backend| {
            backend.run_export_queries(budget, future)
        })
    }
}

impl StorageExecution for StorageHandle {
    fn run_with_call_site<'a, F, R>(
        &'a self,
        call_site: StorageCallSite,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a,
    {
        dispatch_backend!(self, |backend| {
            backend.run_with_call_site(call_site, future)
        })
    }

    fn run_with_call_site_send<'a, F, R>(
        &'a self,
        call_site: StorageCallSite,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + Send + 'a>>
    where
        F: Future<Output = R> + Send + 'a,
        R: Send + 'a,
    {
        dispatch_backend!(self, |backend| {
            backend.run_with_call_site_send(call_site, future)
        })
    }

    fn run_with_mutation_provenance<'a, F, R>(
        &'a self,
        provenance: Option<MutationProvenance>,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a,
    {
        dispatch_backend!(self, |backend| {
            backend.run_with_mutation_provenance(provenance, future)
        })
    }

    fn run_with_revision_precondition<'a, F, R>(
        &'a self,
        precondition: Option<StorageRevisionPrecondition>,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a,
    {
        dispatch_backend!(self, |backend| {
            backend.run_with_revision_precondition(precondition, future)
        })
    }
}

impl WorkerNotificationStorage for StorageHandle {
    fn spawn_worker_notification_listener(
        &self,
        topic: StorageNotification,
        worker_name: &'static str,
        on_notification: fn(),
    ) {
        let backend_name = self.backend_name();
        observe_infallible_storage_call(
            backend_name,
            "worker_notifications",
            "spawn_listener",
            || {
                debug!(
                    message = "registering storage worker notification listener",
                    topic = topic.as_str(),
                    worker_name,
                );
                dispatch_backend!(self, |backend| backend.spawn_worker_notification_listener(
                    topic,
                    worker_name,
                    on_notification
                ))
            },
        )
    }
}
