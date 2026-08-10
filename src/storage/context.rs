use actix_web::web::Data;

use crate::models::{MaintenanceState, TokenRetentionSettings};
use crate::permissions::{AppContext, PermissionBackend};
use crate::storage::observed::observe_storage_call;
use crate::storage::postgres::PostgresPool;
use crate::storage::{
    DynLifecycleStorage, EventDeliveryHealthSnapshot, EventHealthStorage, EventMetricsSnapshot,
    InventoryGaugeSnapshot, MetricsStorage, OperationalStateStorage, PostgresStorage,
    ReadinessSnapshot, StorageBackend, StorageBackendDescriptor, StorageError, StoragePoolState,
    TaskGaugeSnapshot, TokenRetentionStorage,
};
use async_trait::async_trait;

mod private {
    use crate::storage::postgres::PostgresPool;

    pub trait BackendAccess {
        fn db_pool(&self) -> &PostgresPool;
    }
}

/// An opaque handle to Hubuum's configured persistence backend.
///
/// Application code passes this handle to domain operations without selecting
/// a database implementation or handling a connection pool directly.
#[derive(Clone)]
pub(crate) struct StorageHandle {
    implementation: BackendImplementation,
}

#[derive(Clone)]
enum BackendImplementation {
    Postgresql(PostgresStorage),
}

impl StorageHandle {
    pub(crate) fn postgres(pool: PostgresPool) -> Self {
        let backend = PostgresStorage::new(pool);
        assert_complete_storage_backend(&backend);
        Self {
            implementation: BackendImplementation::Postgresql(backend),
        }
    }

    pub(crate) fn descriptor(&self) -> StorageBackendDescriptor {
        match &self.implementation {
            BackendImplementation::Postgresql(backend) => backend.descriptor(),
        }
    }

    fn backend_name(&self) -> &'static str {
        self.descriptor().kind().as_str()
    }

    pub(crate) fn lifecycle_storage(&self) -> DynLifecycleStorage {
        match &self.implementation {
            BackendImplementation::Postgresql(backend) => {
                DynLifecycleStorage::from_backend(backend.clone())
            }
        }
    }

    fn postgres_pool(&self) -> &PostgresPool {
        match &self.implementation {
            BackendImplementation::Postgresql(backend) => backend.pool(),
        }
    }
}

#[async_trait]
impl MetricsStorage for StorageHandle {
    fn metrics_pool_state(&self) -> StoragePoolState {
        match &self.implementation {
            BackendImplementation::Postgresql(backend) => backend.metrics_pool_state(),
        }
    }

    async fn metrics_inventory_snapshot(&self) -> Result<InventoryGaugeSnapshot, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "metrics",
            "inventory_snapshot",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.metrics_inventory_snapshot().await
                    }
                }
            },
        )
        .await
    }

    async fn metrics_task_snapshot(&self) -> Result<TaskGaugeSnapshot, StorageError> {
        observe_storage_call(self.backend_name(), "metrics", "task_snapshot", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => backend.metrics_task_snapshot().await,
            }
        })
        .await
    }

    async fn metrics_event_snapshot(&self) -> Result<EventMetricsSnapshot, StorageError> {
        observe_storage_call(self.backend_name(), "metrics", "event_snapshot", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.metrics_event_snapshot().await
                }
            }
        })
        .await
    }
}

#[async_trait]
impl OperationalStateStorage for StorageHandle {
    async fn readiness_snapshot(&self) -> Result<ReadinessSnapshot, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "operational_state",
            "readiness_snapshot",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.readiness_snapshot().await
                    }
                }
            },
        )
        .await
    }

    async fn maintenance_state(&self) -> Result<MaintenanceState, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "operational_state",
            "maintenance_state",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => backend.maintenance_state().await,
                }
            },
        )
        .await
    }
}

#[async_trait]
impl EventHealthStorage for StorageHandle {
    async fn event_delivery_health(&self) -> Result<EventDeliveryHealthSnapshot, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_health",
            "delivery_health",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.event_delivery_health().await
                    }
                }
            },
        )
        .await
    }
}

#[async_trait]
impl TokenRetentionStorage for StorageHandle {
    async fn purge_expired_tokens(
        &self,
        settings: TokenRetentionSettings,
    ) -> Result<usize, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "token_retention",
            "purge_expired",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.purge_expired_tokens(settings).await
                    }
                }
            },
        )
        .await
    }
}

fn assert_complete_storage_backend(backend: &impl StorageBackend) {
    let _ = backend.descriptor();
}

/// A persistence capability accepted by Hubuum's domain and workflow APIs.
///
/// The trait is sealed so consumers cannot depend on backend implementation
/// details. The current PostgreSQL adapter is selected at application
/// composition time and remains hidden behind this capability.
pub trait StorageContext: private::BackendAccess + Send + Sync {
    fn permission_backend(&self) -> Option<&dyn PermissionBackend> {
        None
    }
}

pub(in crate::storage) fn postgres_pool<C>(backend: &C) -> &PostgresPool
where
    C: StorageContext + ?Sized,
{
    private::BackendAccess::db_pool(backend)
}

/// Normalize any accepted storage context into the opaque application handle.
pub(crate) fn storage_handle<C>(backend: &C) -> StorageHandle
where
    C: StorageContext + ?Sized,
{
    StorageHandle::postgres(postgres_pool(backend).clone())
}

impl private::BackendAccess for StorageHandle {
    fn db_pool(&self) -> &PostgresPool {
        self.postgres_pool()
    }
}

impl StorageContext for StorageHandle {}

impl private::BackendAccess for PostgresPool {
    fn db_pool(&self) -> &PostgresPool {
        self
    }
}

impl StorageContext for PostgresPool {}

impl private::BackendAccess for AppContext {
    fn db_pool(&self) -> &PostgresPool {
        private::BackendAccess::db_pool(self.backend())
    }
}

impl<T> private::BackendAccess for &T
where
    T: StorageContext + ?Sized,
{
    fn db_pool(&self) -> &PostgresPool {
        private::BackendAccess::db_pool(*self)
    }
}

impl<T> StorageContext for &T
where
    T: StorageContext + ?Sized,
{
    fn permission_backend(&self) -> Option<&dyn PermissionBackend> {
        (*self).permission_backend()
    }
}

impl<T> private::BackendAccess for Data<T>
where
    T: StorageContext + ?Sized + 'static,
{
    fn db_pool(&self) -> &PostgresPool {
        private::BackendAccess::db_pool(self.as_ref())
    }
}

impl<T> StorageContext for Data<T>
where
    T: StorageContext + ?Sized + 'static,
{
    fn permission_backend(&self) -> Option<&dyn PermissionBackend> {
        self.as_ref().permission_backend()
    }
}
