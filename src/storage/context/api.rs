use super::*;

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
