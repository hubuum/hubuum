use std::time::Duration;

/// Stable storage capability names used by observations and metrics labels.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum StorageCapability {
    AuditEvents,
    Authentication,
    AuthorizationData,
    BackupSnapshots,
    Catalog,
    Classes,
    ClassRelations,
    CollectionAuthorization,
    Collections,
    ComputedFields,
    ComputedObjects,
    EventConfiguration,
    EventDeliveryAdministration,
    EventDeliveryWorker,
    EventFanout,
    EventHealth,
    EventRetention,
    ExportTemplates,
    ExternalIdentity,
    Groups,
    History,
    IdentityMembership,
    IdentityScopes,
    Imports,
    Inventory,
    LocalIdentityCredentials,
    Metrics,
    ObjectAggregates,
    ObjectRelations,
    Objects,
    OperationalState,
    Principals,
    RelationQueries,
    RemoteTargets,
    Restores,
    ServiceAccounts,
    TaskExecution,
    Tasks,
    TokenRetention,
    Tokens,
    Transactions,
    UnifiedSearch,
    Users,
}

impl StorageCapability {
    pub const ALL: &'static [Self] = &[
        Self::AuditEvents,
        Self::Authentication,
        Self::AuthorizationData,
        Self::BackupSnapshots,
        Self::Catalog,
        Self::Classes,
        Self::ClassRelations,
        Self::CollectionAuthorization,
        Self::Collections,
        Self::ComputedFields,
        Self::ComputedObjects,
        Self::EventConfiguration,
        Self::EventDeliveryAdministration,
        Self::EventDeliveryWorker,
        Self::EventFanout,
        Self::EventHealth,
        Self::EventRetention,
        Self::ExportTemplates,
        Self::ExternalIdentity,
        Self::Groups,
        Self::History,
        Self::IdentityMembership,
        Self::IdentityScopes,
        Self::Imports,
        Self::Inventory,
        Self::LocalIdentityCredentials,
        Self::Metrics,
        Self::ObjectAggregates,
        Self::ObjectRelations,
        Self::Objects,
        Self::OperationalState,
        Self::Principals,
        Self::RelationQueries,
        Self::RemoteTargets,
        Self::Restores,
        Self::ServiceAccounts,
        Self::TaskExecution,
        Self::Tasks,
        Self::TokenRetention,
        Self::Tokens,
        Self::Transactions,
        Self::UnifiedSearch,
        Self::Users,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AuditEvents => "audit_events",
            Self::Authentication => "authentication",
            Self::AuthorizationData => "authorization_data",
            Self::BackupSnapshots => "backup_snapshots",
            Self::Catalog => "catalog",
            Self::Classes => "classes",
            Self::ClassRelations => "class_relations",
            Self::CollectionAuthorization => "collection_authorization",
            Self::Collections => "collections",
            Self::ComputedFields => "computed_fields",
            Self::ComputedObjects => "computed_objects",
            Self::EventConfiguration => "event_configuration",
            Self::EventDeliveryAdministration => "event_delivery_administration",
            Self::EventDeliveryWorker => "event_delivery_worker",
            Self::EventFanout => "event_fanout",
            Self::EventHealth => "event_health",
            Self::EventRetention => "event_retention",
            Self::ExportTemplates => "export_templates",
            Self::ExternalIdentity => "external_identity",
            Self::Groups => "groups",
            Self::History => "history",
            Self::IdentityMembership => "identity_membership",
            Self::IdentityScopes => "identity_scopes",
            Self::Imports => "imports",
            Self::Inventory => "inventory",
            Self::LocalIdentityCredentials => "local_identity_credentials",
            Self::Metrics => "metrics",
            Self::ObjectAggregates => "object_aggregates",
            Self::ObjectRelations => "object_relations",
            Self::Objects => "objects",
            Self::OperationalState => "operational_state",
            Self::Principals => "principals",
            Self::RelationQueries => "relation_queries",
            Self::RemoteTargets => "remote_targets",
            Self::Restores => "restores",
            Self::ServiceAccounts => "service_accounts",
            Self::TaskExecution => "task_execution",
            Self::Tasks => "tasks",
            Self::TokenRetention => "token_retention",
            Self::Tokens => "tokens",
            Self::Transactions => "transactions",
            Self::UnifiedSearch => "unified_search",
            Self::Users => "users",
        }
    }
}

/// One completed logical storage operation observed at the application edge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageObservation {
    backend: &'static str,
    capability: StorageCapability,
    operation: &'static str,
    result: &'static str,
    duration: Duration,
}

impl StorageObservation {
    #[must_use]
    pub const fn new(
        backend: &'static str,
        capability: StorageCapability,
        operation: &'static str,
        result: &'static str,
        duration: Duration,
    ) -> Self {
        Self {
            backend,
            capability,
            operation,
            result,
            duration,
        }
    }

    #[must_use]
    pub const fn backend(&self) -> &'static str {
        self.backend
    }

    #[must_use]
    pub const fn capability(&self) -> &'static str {
        self.capability.as_str()
    }

    #[must_use]
    pub const fn operation(&self) -> &'static str {
        self.operation
    }

    #[must_use]
    pub const fn result(&self) -> &'static str {
        self.result
    }

    #[must_use]
    pub const fn duration(&self) -> Duration {
        self.duration
    }
}

/// Application-owned observer for backend-neutral storage operations.
///
/// Storage adapters and wrappers report observations through this trait. They
/// do not select a metrics registry, exporter, or global telemetry provider.
pub trait StorageObserver: Send + Sync {
    fn operation_finished(&self, observation: &StorageObservation);
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::StorageCapability;

    #[test]
    fn storage_capability_labels_are_unique() {
        let labels = StorageCapability::ALL
            .iter()
            .copied()
            .map(StorageCapability::as_str)
            .collect::<HashSet<_>>();

        assert_eq!(labels.len(), StorageCapability::ALL.len());
    }
}
