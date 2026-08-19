use crate::{
    AuditEventStorage, AuthenticationStorage, AuthorizationStorage, BackupSnapshotStorage,
    CatalogStorage, ClassRelationStorage, ClassStorage, CollectionAuthorizationStorage,
    CollectionStorage, ComputedFieldLifecycleStorage, ComputedObjectStorage,
    EventDeliveryAdministrationStorage, EventDeliveryStorage, EventFanoutStorage,
    EventHealthStorage, EventRetentionStorage, EventSubscriptionStorage, ExportTemplateStorage,
    GroupStorage, HistoryStorage, IdentityStorage, InventoryStorage, MaintenanceStorage,
    MetricsStorage, ObjectAggregateStorage, ObjectRelationStorage, ObjectStorage,
    OperationalStateStorage, PrincipalStorage, RelationQueryStorage, RemoteTargetStorage,
    StorageBackendIdentity, StorageExecution, TaskExecutionStorage, TaskQueueStorage,
    TokenRetentionStorage, TokenStorage, TransactionalStorage, UnifiedSearchStorage, UserStorage,
    WorkerNotificationStorage,
};

/// Complete storage contract accepted by an application composition root.
///
/// Capability traits remain independently useful for focused services and
/// tests. A selectable backend implements this aggregate only after it
/// implements every required family. Missing behavior is therefore a compile
/// error instead of a runtime `unsupported` path.
///
/// This trait describes static Rust composition. It is not a dynamic plugin
/// interface and does not define runtime discovery or contract versioning.
pub trait StorageBackend:
    StorageBackendIdentity
    + CollectionStorage
    + ClassStorage
    + ObjectStorage
    + ClassRelationStorage
    + ObjectRelationStorage
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
    + CollectionAuthorizationStorage
    + RemoteTargetStorage
    + TaskQueueStorage
    + TaskExecutionStorage
    + BackupSnapshotStorage
    + MaintenanceStorage
    + ExportTemplateStorage
    + WorkerNotificationStorage
    + StorageExecution
    + TransactionalStorage
{
}
