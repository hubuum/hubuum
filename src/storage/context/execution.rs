use super::*;
use hubuum_storage_core::ApplicationWorkAdmission;

impl ExecutionStorage for StorageHandle {
    fn current_execution_scope(&self) -> StorageExecutionScope {
        dispatch_backend!(self, |backend| backend.current_execution_scope())
    }

    fn run_in_scope_sync<F, R>(&self, scope: StorageExecutionScope, operation: F) -> R
    where
        F: FnOnce() -> R,
    {
        dispatch_backend!(self, |backend| backend.run_in_scope_sync(scope, operation))
    }

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

impl StorageHandle {
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

/// A resource port shares the bound adapter, without retaining a cyclic handle.
#[derive(Clone)]
pub(crate) struct ApplicationAdmission {
    backend: BackendImplementation,
    owner: RuntimeOwner,
}

impl ApplicationAdmission {
    pub(crate) async fn run<T: Send>(
        &self,
        future: impl Future<Output = Result<T, StorageError>> + Send,
    ) -> Result<T, StorageError> {
        match &self.backend {
            BackendImplementation::Postgres(backend) => {
                run_admitted(backend, &self.owner, future).await
            }
            BackendImplementation::Memory(backend) => {
                run_admitted(backend, &self.owner, future).await
            }
        }
    }
}

async fn run_admitted<S: ExecutionStorage + DeletionControlStorage, T: Send>(
    backend: &S,
    owner: &RuntimeOwner,
    future: impl Future<Output = Result<T, StorageError>> + Send,
) -> Result<T, StorageError> {
    let scope = backend.current_execution_scope();
    let permit = match scope.application_access_override() {
        Some(permit) => permit.clone(),
        None => {
            backend
                .capture_application_execution(Some(owner.clone()))
                .await?
        }
    };
    backend
        .run_in_scope_send(scope.with_application_access(permit), future)
        .await
}

impl StorageHandle {
    /// Bind all capability ports and background providers before composition.
    pub(crate) fn with_runtime_owner(self, owner: RuntimeOwner) -> Self {
        let mut bound = match &self.inner.implementation {
            BackendImplementation::Postgres(backend) => {
                Self::from_postgres_backend(backend.clone().with_runtime_owner(owner.clone()))
            }
            BackendImplementation::Memory(backend) => {
                Self::from_registered_backend(backend.clone().with_runtime_owner(owner.clone()))
            }
        };
        let inner = Arc::get_mut(&mut bound.inner).expect("new bound handle has one owner");
        inner.runtime_owner = Some(owner.clone());
        inner.observer = self.inner.observer.clone();
        inner.query_observations = self.inner.query_observations.clone();
        let admission = Some(ApplicationAdmission {
            backend: inner.implementation.clone(),
            owner,
        });
        inner.resource_ports = match &inner.implementation {
            BackendImplementation::Postgres(backend) => ResourceStoragePorts::observed(
                backend.clone(),
                inner.descriptor.kind().as_str(),
                inner.observer.clone(),
                admission,
            ),
            BackendImplementation::Memory(backend) => ResourceStoragePorts::observed(
                backend.clone(),
                inner.descriptor.kind().as_str(),
                inner.observer.clone(),
                admission,
            ),
        };
        bound
    }

    pub(crate) async fn admit_application_work_with_maintenance(
        &self,
    ) -> Result<ApplicationWorkAdmission, StorageError> {
        self.capture_application_execution_with_maintenance(self.inner.runtime_owner.clone())
            .await
    }

    /// Inherit the producer's original admission, including across nested calls.
    /// Never acquire a newer generation on behalf of already admitted work.
    pub(crate) async fn admit_application_work(
        &self,
    ) -> Result<StorageExecutionPermit, StorageError> {
        if let Some(permit) = self.current_execution_scope().application_access_override() {
            return Ok(permit.clone());
        }
        self.capture_application_execution(self.inner.runtime_owner.clone())
            .await
    }
}
