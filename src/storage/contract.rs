use std::sync::Arc;

use super::{
    AuthenticationStorage, AuthorizationStorage, CatalogStorage, ClassRelationStore, ClassStore,
    CollectionStore, ComputedFieldLifecycleStorage, ComputedObjectStorage, EventDeliveryStorage,
    EventFanoutStorage, EventHealthStorage, EventRetentionStorage, HistoryStorage, MetricsStorage,
    ObjectAggregateStorage, ObjectRelationStore, ObjectStore, OperationalStateStorage,
    PostgresStorage, RelationQueryStorage, TaskQueueStorage, TokenRetentionStorage,
    UnifiedSearchStorage, observed::ObservedLifecycleStorage,
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

/// Temporary migration gates for capability families whose operation-shaped
/// traits have not yet been extracted.
///
/// These gates prevent another implementation from becoming selectable during
/// the refactor, but they do not certify behavior. Each one must be replaced by
/// mandatory operation-shaped traits and shared compatibility tests, as the
/// authentication gate has been in this layer.
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
    + AuthenticationStorage
    + AuthorizationStorage
    + CatalogStorage
    + ComputedFieldLifecycleStorage
    + ComputedObjectStorage
    + ObjectAggregateStorage
    + RelationQueryStorage
    + EventDeliveryStorage
    + EventFanoutStorage
    + EventHealthStorage
    + EventRetentionStorage
    + HistoryStorage
    + MetricsStorage
    + OperationalStateStorage
    + TokenRetentionStorage
    + UnifiedSearchStorage
    + TaskQueueStorage
    + WorkflowStorage
    + OperationalStorage
    + sealed::CertifiedStorageBackend
{
    fn descriptor(&self) -> StorageBackendDescriptor;
}

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
                "catalog_queries",
                "computed_object_queries",
                "computed_field_lifecycle",
                "object_aggregates",
                "relation_queries",
                "identity_and_authorization_data",
                "temporal_history",
                "unified_search",
                "task_queue",
                "workflows",
                "operations",
            ]
        );
    }
}
