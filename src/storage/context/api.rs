use super::*;

pub(super) fn assert_complete_storage_backend(
    backend: &impl CertifiedStorageBackend,
    expected_kind: StorageBackendKind,
) {
    assert_eq!(
        backend.storage_name(),
        expected_kind.as_str(),
        "storage adapter identity must match its selectable backend kind"
    );
}

/// A persistence capability accepted by Hubuum's domain and workflow APIs.
///
/// The trait is sealed so consumers cannot depend on backend implementation
/// details. The current PostgreSQL adapter is selected at application
/// composition time and remains hidden behind this capability.
pub trait StorageContext: private::BackendAccess + Send + Sync {}

/// Normalize any accepted storage context into the opaque application handle.
pub(crate) fn storage_handle<C>(backend: &C) -> StorageHandle
where
    C: StorageContext + ?Sized,
{
    private::BackendAccess::storage_handle(backend)
}

impl private::BackendAccess for StorageHandle {
    fn storage_handle(&self) -> StorageHandle {
        self.clone()
    }
}

impl StorageContext for StorageHandle {}

// Adapter-focused tests can opt into the historical pool-shaped context. The
// production adapter and application composition both use their explicit
// boundaries instead of making a concrete pool an application capability.
#[cfg(any(test, feature = "integration-test-support"))]
impl private::BackendAccess for PostgresPool {
    fn storage_handle(&self) -> StorageHandle {
        StorageHandle::postgres(self.clone())
    }
}

#[cfg(any(test, feature = "integration-test-support"))]
impl StorageContext for PostgresPool {}

impl private::BackendAccess for AppContext {
    fn storage_handle(&self) -> StorageHandle {
        self.clone_backend()
    }
}

impl<T> private::BackendAccess for &T
where
    T: StorageContext + ?Sized,
{
    fn storage_handle(&self) -> StorageHandle {
        private::BackendAccess::storage_handle(*self)
    }
}

impl<T> StorageContext for &T where T: StorageContext + ?Sized {}

impl<T> private::BackendAccess for Data<T>
where
    T: StorageContext + ?Sized + 'static,
{
    fn storage_handle(&self) -> StorageHandle {
        private::BackendAccess::storage_handle(self.as_ref())
    }
}

impl<T> StorageContext for Data<T> where T: StorageContext + ?Sized + 'static {}
