use actix_web::web::Data;

use crate::db::DbPool;
use crate::permissions::{AppContext, PermissionBackend};
use crate::storage::{
    DynLifecycleStorage, PostgresStorage, StorageBackend, StorageBackendDescriptor,
};

mod private {
    use crate::db::DbPool;

    pub trait BackendAccess {
        fn db_pool(&self) -> &DbPool;
    }
}

/// An opaque handle to Hubuum's configured persistence backend.
///
/// Application code passes this handle to domain operations without selecting
/// a database implementation or handling a connection pool directly.
#[derive(Clone)]
pub(crate) struct BackendHandle {
    implementation: BackendImplementation,
}

#[derive(Clone)]
enum BackendImplementation {
    Postgresql(PostgresStorage),
}

impl BackendHandle {
    pub(crate) fn postgres(pool: DbPool) -> Self {
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

    fn postgres_pool(&self) -> &DbPool {
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
pub trait BackendContext: private::BackendAccess + Send + Sync {
    fn permission_backend(&self) -> Option<&dyn PermissionBackend> {
        None
    }
}

pub(crate) fn backend_pool<C>(backend: &C) -> &DbPool
where
    C: BackendContext + ?Sized,
{
    private::BackendAccess::db_pool(backend)
}

impl private::BackendAccess for BackendHandle {
    fn db_pool(&self) -> &DbPool {
        self.postgres_pool()
    }
}

impl BackendContext for BackendHandle {}

impl private::BackendAccess for DbPool {
    fn db_pool(&self) -> &DbPool {
        self
    }
}

impl BackendContext for DbPool {}

impl private::BackendAccess for AppContext {
    fn db_pool(&self) -> &DbPool {
        private::BackendAccess::db_pool(self.backend())
    }
}

impl<T> private::BackendAccess for &T
where
    T: BackendContext + ?Sized,
{
    fn db_pool(&self) -> &DbPool {
        private::BackendAccess::db_pool(*self)
    }
}

impl<T> BackendContext for &T
where
    T: BackendContext + ?Sized,
{
    fn permission_backend(&self) -> Option<&dyn PermissionBackend> {
        (*self).permission_backend()
    }
}

impl<T> private::BackendAccess for Data<T>
where
    T: BackendContext + ?Sized + 'static,
{
    fn db_pool(&self) -> &DbPool {
        private::BackendAccess::db_pool(self.as_ref())
    }
}

impl<T> BackendContext for Data<T>
where
    T: BackendContext + ?Sized + 'static,
{
    fn permission_backend(&self) -> Option<&dyn PermissionBackend> {
        self.as_ref().permission_backend()
    }
}
