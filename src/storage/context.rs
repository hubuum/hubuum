use actix_web::web::Data;

use crate::permissions::{AppContext, PermissionBackend};
use crate::storage::postgres::PostgresPool;
use crate::storage::{
    DynLifecycleStorage, PostgresStorage, StorageBackend, StorageBackendDescriptor,
};

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
