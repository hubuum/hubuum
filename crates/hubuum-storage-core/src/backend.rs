use crate::{
    AuditEventStorage, AuthenticationStorage, AuthorizationStorage, BackupSnapshotStorage,
    BootstrapStorage, CatalogStorage, ClassRelationStorage, ClassStorage,
    CollectionAuthorizationStorage, CollectionStorage, ComputedFieldLifecycleStorage,
    ComputedObjectStorage, EventDeliveryAdministrationStorage, EventDeliveryStorage,
    EventFanoutStorage, EventHealthStorage, EventRetentionStorage, EventSubscriptionStorage,
    ExecutionStorage, ExportTemplateStorage, ExternalIdentityStorage, GroupStorage, HistoryStorage,
    IdentityMembershipStorage, IdentityScopeStorage, ImportStorage, InventoryStorage,
    MetricsStorage, ObjectAggregateStorage, ObjectRelationStorage, ObjectStorage,
    OperationalStateStorage, PrincipalStorage, RelationQueryStorage, RemoteTargetStorage,
    RestoreStorage, ServiceAccountStorage, TaskExecutionStorage, TaskQueueStorage,
    TokenRetentionStorage, TokenStorage, TransactionStorage, UnifiedSearchStorage, UserStorage,
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
    CollectionStorage
    + ClassStorage
    + ObjectStorage
    + ClassRelationStorage
    + ObjectRelationStorage
    + AuthenticationStorage
    + BootstrapStorage
    + IdentityScopeStorage
    + IdentityMembershipStorage
    + ServiceAccountStorage
    + ExternalIdentityStorage
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
    + ImportStorage
    + RestoreStorage
    + ExportTemplateStorage
    + ExecutionStorage
    + TransactionStorage
{
}
