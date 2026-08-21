use crate::{
    AuditEventStorage, AuthenticationStorage, AuthorizationDataStorage, BackupSnapshotStorage,
    CatalogStorage, ClassRelationStorage, ClassStorage, CollectionAuthorizationQueryStorage,
    CollectionStorage, ComputedFieldStorage, ComputedObjectStorage, EventConfigurationStorage,
    EventDeliveryAdministrationStorage, EventDeliveryWorkerStorage, EventFanoutStorage,
    EventHealthStorage, EventRetentionStorage, ExecutionStorage, ExportTemplateStorage,
    ExternalIdentityStorage, GroupStorage, HistoryStorage, IdentityMembershipStorage,
    IdentityScopeStorage, ImportStorage, InventoryStorage, LocalIdentityCredentialStorage,
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
    + LocalIdentityCredentialStorage
    + IdentityScopeStorage
    + IdentityMembershipStorage
    + ServiceAccountStorage
    + ExternalIdentityStorage
    + UserStorage
    + TokenStorage
    + AuthorizationDataStorage
    + CatalogStorage
    + ComputedFieldStorage
    + ComputedObjectStorage
    + ObjectAggregateStorage
    + RelationQueryStorage
    + AuditEventStorage
    + EventConfigurationStorage
    + EventDeliveryAdministrationStorage
    + EventDeliveryWorkerStorage
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
    + CollectionAuthorizationQueryStorage
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
