use std::sync::Arc;

use super::{
    ClassRelationStore, ClassStore, CollectionStore, EventDeliveryStorage, EventFanoutStorage,
    EventHealthStorage, EventRetentionStorage, MetricsStorage, ObjectRelationStore, ObjectStore,
    OperationalStateStorage, PostgresStorage, TokenRetentionStorage,
    observed::ObservedLifecycleStorage,
};

#[cfg(test)]
pub(crate) use hubuum_storage_core::{STORAGE_CONTRACT_VERSION, StorageCapability};
pub(crate) use hubuum_storage_core::{StorageBackendDescriptor, StorageBackendKind};

/// Identifies a lifecycle implementation for diagnostics and contract tests.
///
/// Focused models may implement this and [`LifecycleStorage`] without becoming
/// selectable application backends.
pub(crate) trait StorageIdentity: Send + Sync {
    fn storage_name(&self) -> &'static str;
}

/// Complete lifecycle contract shared by services and focused test models.
pub(crate) trait LifecycleStorage:
    StorageIdentity
    + CollectionStore
    + ClassStore
    + ObjectStore
    + ClassRelationStore
    + ObjectRelationStore
{
}

impl<T> LifecycleStorage for T where
    T: StorageIdentity
        + CollectionStore
        + ClassStore
        + ObjectStore
        + ClassRelationStore
        + ObjectRelationStore
{
}

/// Certification gates for capability families that are still implemented by
/// operation-shaped adapters outside `src/storage`.
///
/// These traits are intentionally sealed and implemented in this central
/// contract module. They make the composition checklist compile-time visible:
/// adding a selectable backend requires an explicit implementation for every
/// family here, followed by the shared compatibility suite.
pub(crate) trait IdentityAndAuthorizationStorage: Send + Sync {}
pub(crate) trait QueryAndHistoryStorage: Send + Sync {}
pub(crate) trait WorkflowStorage: Send + Sync {}
pub(crate) trait OperationalStorage: Send + Sync {}

mod sealed {
    pub trait CertifiedStorageBackend {}
}

/// All-or-nothing storage backend accepted by the application composition root.
///
/// Partial lifecycle models cannot implement the private certification trait
/// and therefore cannot be selected by `AppContext`.
pub(crate) trait StorageBackend:
    LifecycleStorage
    + EventDeliveryStorage
    + EventFanoutStorage
    + EventHealthStorage
    + EventRetentionStorage
    + MetricsStorage
    + OperationalStateStorage
    + TokenRetentionStorage
    + IdentityAndAuthorizationStorage
    + QueryAndHistoryStorage
    + WorkflowStorage
    + OperationalStorage
    + sealed::CertifiedStorageBackend
{
    fn descriptor(&self) -> StorageBackendDescriptor;
}

impl IdentityAndAuthorizationStorage for PostgresStorage {}
impl QueryAndHistoryStorage for PostgresStorage {}
impl WorkflowStorage for PostgresStorage {}
impl OperationalStorage for PostgresStorage {}
impl sealed::CertifiedStorageBackend for PostgresStorage {}

impl StorageBackend for PostgresStorage {
    fn descriptor(&self) -> StorageBackendDescriptor {
        StorageBackendDescriptor::new(StorageBackendKind::Postgresql)
    }
}

/// Type-erased lifecycle capability used by application services.
#[derive(Clone)]
pub(crate) struct DynLifecycleStorage {
    inner: Arc<dyn LifecycleStorage>,
}

impl DynLifecycleStorage {
    pub(crate) fn from_backend(storage: impl StorageBackend + 'static) -> Self {
        Self::new(storage)
    }

    /// Construct a focused lifecycle contract harness.
    ///
    /// Production composition must use [`Self::from_backend`], whose stronger
    /// bound rejects partial implementations.
    pub(crate) fn new(storage: impl LifecycleStorage + 'static) -> Self {
        let inner: Arc<dyn LifecycleStorage> = Arc::new(storage);
        Self {
            inner: Arc::new(ObservedLifecycleStorage::new(inner)),
        }
    }

    pub(crate) fn inner(&self) -> &dyn LifecycleStorage {
        self.inner.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_complete_backend<T: StorageBackend>() {}

    #[test]
    fn postgres_satisfies_the_complete_storage_backend_contract() {
        assert_complete_backend::<PostgresStorage>();
    }

    #[test]
    fn required_capabilities_are_stable_and_complete() {
        assert_eq!(
            StorageCapability::ALL.map(StorageCapability::as_str),
            [
                "domain_lifecycle",
                "identity_and_authorization_data",
                "queries_and_history",
                "workflows",
                "operations",
            ]
        );
    }
}
