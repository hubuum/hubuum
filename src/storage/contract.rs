use super::{
    AuditEventStorage, AuthenticationStorage, AuthorizationStorage, BackupSnapshotStorage,
    CatalogStorage, ClassRecordStorage, ClassRelationStore, ClassStore,
    CollectionPermissionStorage, CollectionRecordStorage, CollectionStore,
    ComputedFieldLifecycleStorage, ComputedObjectStorage, EventDeliveryAdministrationStorage,
    EventDeliveryStorage, EventFanoutStorage, EventHealthStorage, EventRetentionStorage,
    EventSubscriptionStorage, ExportQueryStorage, ExportTemplateStorage, GroupStorage,
    HistoryStorage, IdentityStorage, ImportStorage, InventoryStorage, MetricsStorage,
    ObjectAggregateStorage, ObjectRecordStorage, ObjectRelationStore, ObjectStore,
    OperationalStateStorage, PostgresStorage, PrincipalStorage, RelationQueryStorage,
    RemoteTargetStorage, RestoreStorage, StorageExecution, TaskExecutionStorage, TaskQueueStorage,
    TokenRetentionStorage, TokenStorage, UnifiedSearchStorage, UserStorage,
    WorkerNotificationStorage,
};

pub(crate) use hubuum_storage_core::{StorageBackendDescriptor, StorageBackendKind};

/// Identifies a storage implementation for diagnostics and contract tests.
pub(crate) trait StorageIdentity: Send + Sync {
    fn storage_name(&self) -> &'static str;
}

/// All-or-nothing storage backend accepted by the application composition root.
///
/// Focused adapters implement only the operation-family traits they support.
/// A selectable backend opts into this aggregate only after implementing every
/// required family; the supertrait bounds make omissions a compile error.
pub(crate) trait StorageBackend:
    StorageIdentity
    + CollectionStore
    + ClassStore
    + ObjectStore
    + ClassRelationStore
    + ObjectRelationStore
    + AuthenticationStorage
    + IdentityStorage
    + UserStorage
    + TokenStorage
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
    + InventoryStorage
    + MetricsStorage
    + OperationalStateStorage
    + TokenRetentionStorage
    + UnifiedSearchStorage
    + GroupStorage
    + PrincipalStorage
    + CollectionPermissionStorage
    + CollectionRecordStorage
    + ClassRecordStorage
    + ObjectRecordStorage
    + RemoteTargetStorage
    + TaskQueueStorage
    + TaskExecutionStorage
    + BackupSnapshotStorage
    + RestoreStorage
    + ImportStorage
    + ExportQueryStorage
    + ExportTemplateStorage
    + WorkerNotificationStorage
    + StorageExecution
{
}

impl StorageBackend for PostgresStorage {}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_complete_backend<T: StorageBackend>() {}

    #[test]
    fn postgres_satisfies_the_complete_storage_backend_contract() {
        assert_complete_backend::<PostgresStorage>();
    }
}
