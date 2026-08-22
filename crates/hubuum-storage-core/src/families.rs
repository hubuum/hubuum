//! Coarse, method-free views over the complete storage contract.
//!
//! The operation traits remain the implementation units. These family traits
//! give adapter authors and focused application services stable bounds without
//! duplicating or wrapping any operation.

use crate::{
    AuditEventStorage, AuthenticationStorage, AuthorizationDataStorage, BackupSnapshotStorage,
    CatalogStorage, ClassRelationStorage, ClassStorage, CollectionAuthorizationQueryStorage,
    CollectionStorage, ComputedFieldStorage, ComputedObjectStorage, EventConfigurationStorage,
    EventDeliveryAdministrationStorage, EventDeliveryWorkerStorage, EventFanoutStorage,
    EventHealthStorage, EventRetentionStorage, ExecutionStorage, ExportTemplateStorage,
    ExternalIdentityStorage, GroupMembershipStorage, GroupStorage, HistoryStorage,
    IdentityScopeStorage, ImportStorage, InventoryStorage, LocalIdentityCredentialStorage,
    MetricsStorage, ObjectAggregateStorage, ObjectRelationStorage, ObjectStorage,
    OperationalStateStorage, PrincipalStorage, RelationQueryStorage, RemoteTargetStorage,
    RestoreStorage, ServiceAccountStorage, TaskExecutionStorage, TaskQueueStorage,
    TokenRetentionStorage, TokenStorage, TransactionStorage, UnifiedSearchStorage, UserStorage,
};

/// Atomic domain-resource lifecycle and transaction behavior.
pub trait ResourceStorage:
    CollectionStorage
    + ClassStorage
    + ObjectStorage
    + ClassRelationStorage
    + ObjectRelationStorage
    + TransactionStorage
{
}

impl<T> ResourceStorage for T where
    T: CollectionStorage
        + ClassStorage
        + ObjectStorage
        + ClassRelationStorage
        + ObjectRelationStorage
        + TransactionStorage
        + ?Sized
{
}

/// Authentication, identity resources, credentials, and authorization data.
pub trait IdentityStorage:
    AuthenticationStorage
    + LocalIdentityCredentialStorage
    + IdentityScopeStorage
    + GroupMembershipStorage
    + ServiceAccountStorage
    + ExternalIdentityStorage
    + UserStorage
    + TokenStorage
    + GroupStorage
    + PrincipalStorage
    + AuthorizationDataStorage
    + CollectionAuthorizationQueryStorage
{
}

impl<T> IdentityStorage for T where
    T: AuthenticationStorage
        + LocalIdentityCredentialStorage
        + IdentityScopeStorage
        + GroupMembershipStorage
        + ServiceAccountStorage
        + ExternalIdentityStorage
        + UserStorage
        + TokenStorage
        + GroupStorage
        + PrincipalStorage
        + AuthorizationDataStorage
        + CollectionAuthorizationQueryStorage
        + ?Sized
{
}

/// Backend-neutral read models and computed projections.
pub trait QueryStorage:
    CatalogStorage
    + ComputedObjectStorage
    + ObjectAggregateStorage
    + RelationQueryStorage
    + HistoryStorage
    + InventoryStorage
    + UnifiedSearchStorage
{
}

impl<T> QueryStorage for T where
    T: CatalogStorage
        + ComputedObjectStorage
        + ObjectAggregateStorage
        + RelationQueryStorage
        + HistoryStorage
        + InventoryStorage
        + UnifiedSearchStorage
        + ?Sized
{
}

/// Long-running tasks and import, export, backup, restore, and remote workflows.
pub trait WorkflowStorage:
    RemoteTargetStorage
    + ComputedFieldStorage
    + TaskQueueStorage
    + TaskExecutionStorage
    + BackupSnapshotStorage
    + ImportStorage
    + RestoreStorage
    + ExportTemplateStorage
{
}

impl<T> WorkflowStorage for T where
    T: RemoteTargetStorage
        + ComputedFieldStorage
        + TaskQueueStorage
        + TaskExecutionStorage
        + BackupSnapshotStorage
        + ImportStorage
        + RestoreStorage
        + ExportTemplateStorage
        + ?Sized
{
}

/// Audit, configuration, fan-out, delivery, health, and retention behavior.
pub trait EventStorage:
    AuditEventStorage
    + EventConfigurationStorage
    + EventDeliveryAdministrationStorage
    + EventDeliveryWorkerStorage
    + EventFanoutStorage
    + EventHealthStorage
    + EventRetentionStorage
{
}

impl<T> EventStorage for T where
    T: AuditEventStorage
        + EventConfigurationStorage
        + EventDeliveryAdministrationStorage
        + EventDeliveryWorkerStorage
        + EventFanoutStorage
        + EventHealthStorage
        + EventRetentionStorage
        + ?Sized
{
}

/// Cross-cutting execution, metrics, operational state, and token retention.
pub trait OperationalStorage:
    MetricsStorage + OperationalStateStorage + TokenRetentionStorage + ExecutionStorage
{
}

impl<T> OperationalStorage for T where
    T: MetricsStorage + OperationalStateStorage + TokenRetentionStorage + ExecutionStorage + ?Sized
{
}
