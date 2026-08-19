//! Discoverable groupings of the storage capability traits.
//!
//! The crate-root exports remain available for concise imports. These modules
//! are the preferred discovery surface for adapter authors and make the
//! intended future crate boundaries explicit without publishing or splitting
//! any package today.

/// Complete-backend composition and diagnostic identity.
pub mod backend {
    pub use crate::{StorageBackend, StorageBackendIdentity};
}

/// Atomic resource lifecycle and transaction capabilities.
pub mod resources {
    pub use crate::{
        ClassRelationStorage, ClassStorage, CollectionStorage, ObjectRelationStorage,
        ObjectStorage, StorageTransaction, TransactionalStorage,
    };
}

/// Authentication, identity, group, principal, and authorization capabilities.
pub mod identity {
    pub use crate::{
        AuthenticationStorage, AuthorizationStorage, CollectionAuthorizationStorage, GroupStorage,
        IdentityStorage, PrincipalStorage, TokenStorage, UserStorage,
    };
}

/// Backend-neutral read-model capabilities.
pub mod queries {
    pub use crate::{
        CatalogStorage, ComputedObjectStorage, HistoryStorage, InventoryStorage,
        ObjectAggregateStorage, RelationQueryStorage, UnifiedSearchStorage,
    };
}

/// Long-running and application workflow capabilities.
pub mod workflows {
    pub use crate::{
        BackupSnapshotStorage, ComputedFieldLifecycleStorage, ExportTemplateStorage, ImportStorage,
        MaintenanceStorage, RemoteTargetStorage, RestoreStorage, TaskExecutionStorage,
        TaskQueueStorage,
    };
}

/// Audit, fan-out, delivery, administration, and retention capabilities.
pub mod events {
    pub use crate::{
        AuditEventStorage, EventArchive, EventDeliveryAdministrationStorage, EventDeliveryStorage,
        EventFanoutStorage, EventHealthStorage, EventRetentionStorage, EventSubscriptionStorage,
    };
}

/// Execution context, observability, maintenance, and process integration.
pub mod operations {
    pub use crate::{
        MetricsStorage, OperationalStateStorage, StorageExecution, StorageTelemetry,
        TokenRetentionStorage, WorkerNotificationStorage,
    };
}
