use std::sync::Arc;

use super::{
    AuditEventStorage, AuthenticationStorage, AuthorizationStorage, BackupSnapshotStorage,
    CatalogStorage, ClassRelationStore, ClassStore, CollectionStore, ComputedFieldLifecycleStorage,
    ComputedObjectStorage, EventDeliveryAdministrationStorage, EventDeliveryStorage,
    EventFanoutStorage, EventHealthStorage, EventRetentionStorage, EventSubscriptionStorage,
    ExportQueryStorage, HistoryStorage, IdentityStorage, ImportStorage, MetricsStorage,
    ObjectAggregateStorage, ObjectRelationStore, ObjectStore, OperationalStateStorage,
    PostgresStorage, RelationQueryStorage, RemoteTargetStorage, RestoreStorage, StorageExecution,
    TaskExecutionStorage, TaskQueueStorage, TokenRetentionStorage, UnifiedSearchStorage,
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
    + IdentityStorage
    + AuthorizationStorage
    + CatalogStorage
    + ComputedFieldLifecycleStorage
    + ComputedObjectStorage
    + ObjectAggregateStorage
    + RelationQueryStorage
    + AuditEventStorage
    + EventSubscriptionStorage
    + EventDeliveryAdministrationStorage
    + EventDeliveryStorage
    + EventFanoutStorage
    + EventHealthStorage
    + EventRetentionStorage
    + HistoryStorage
    + MetricsStorage
    + OperationalStateStorage
    + TokenRetentionStorage
    + UnifiedSearchStorage
    + RemoteTargetStorage
    + TaskQueueStorage
    + TaskExecutionStorage
    + BackupSnapshotStorage
    + RestoreStorage
    + ImportStorage
    + ExportQueryStorage
    + StorageExecution
    + sealed::CertifiedStorageBackend
{
    fn descriptor(&self) -> StorageBackendDescriptor;
}

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
                "remote_targets",
                "task_queue",
                "task_execution",
                "backup_snapshots",
                "restores",
                "imports",
                "export_queries",
                "event_administration",
                "operations",
            ]
        );
    }
}
