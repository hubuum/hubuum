use std::time::Duration;

/// Stable storage capability names used by observations and metrics labels.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum StorageCapability {
    AuditEvent,
    Authentication,
    AuthorizationData,
    BackupSnapshot,
    Catalog,
    Class,
    ClassRelation,
    CollectionAuthorizationQuery,
    Collection,
    ComputedField,
    ComputedObject,
    EventConfiguration,
    EventDeliveryAdministration,
    EventDeliveryWorker,
    EventFanout,
    EventHealth,
    EventRetention,
    ExportTemplate,
    ExternalIdentity,
    Group,
    History,
    GroupMembership,
    IdentityScope,
    Import,
    Inventory,
    LocalIdentityCredential,
    Metrics,
    ObjectAggregate,
    ObjectRelation,
    Object,
    OperationalState,
    Principal,
    RelationQuery,
    RemoteTarget,
    Restore,
    ServiceAccount,
    TaskExecution,
    TaskQueue,
    TokenRetention,
    Token,
    Transaction,
    UnifiedSearch,
    User,
}

impl StorageCapability {
    pub const ALL: &'static [Self] = &[
        Self::AuditEvent,
        Self::Authentication,
        Self::AuthorizationData,
        Self::BackupSnapshot,
        Self::Catalog,
        Self::Class,
        Self::ClassRelation,
        Self::CollectionAuthorizationQuery,
        Self::Collection,
        Self::ComputedField,
        Self::ComputedObject,
        Self::EventConfiguration,
        Self::EventDeliveryAdministration,
        Self::EventDeliveryWorker,
        Self::EventFanout,
        Self::EventHealth,
        Self::EventRetention,
        Self::ExportTemplate,
        Self::ExternalIdentity,
        Self::Group,
        Self::History,
        Self::GroupMembership,
        Self::IdentityScope,
        Self::Import,
        Self::Inventory,
        Self::LocalIdentityCredential,
        Self::Metrics,
        Self::ObjectAggregate,
        Self::ObjectRelation,
        Self::Object,
        Self::OperationalState,
        Self::Principal,
        Self::RelationQuery,
        Self::RemoteTarget,
        Self::Restore,
        Self::ServiceAccount,
        Self::TaskExecution,
        Self::TaskQueue,
        Self::TokenRetention,
        Self::Token,
        Self::Transaction,
        Self::UnifiedSearch,
        Self::User,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AuditEvent => "audit_event",
            Self::Authentication => "authentication",
            Self::AuthorizationData => "authorization_data",
            Self::BackupSnapshot => "backup_snapshot",
            Self::Catalog => "catalog",
            Self::Class => "class",
            Self::ClassRelation => "class_relation",
            Self::CollectionAuthorizationQuery => "collection_authorization_query",
            Self::Collection => "collection",
            Self::ComputedField => "computed_field",
            Self::ComputedObject => "computed_object",
            Self::EventConfiguration => "event_configuration",
            Self::EventDeliveryAdministration => "event_delivery_administration",
            Self::EventDeliveryWorker => "event_delivery_worker",
            Self::EventFanout => "event_fanout",
            Self::EventHealth => "event_health",
            Self::EventRetention => "event_retention",
            Self::ExportTemplate => "export_template",
            Self::ExternalIdentity => "external_identity",
            Self::Group => "group",
            Self::History => "history",
            Self::GroupMembership => "group_membership",
            Self::IdentityScope => "identity_scope",
            Self::Import => "import",
            Self::Inventory => "inventory",
            Self::LocalIdentityCredential => "local_identity_credential",
            Self::Metrics => "metrics",
            Self::ObjectAggregate => "object_aggregate",
            Self::ObjectRelation => "object_relation",
            Self::Object => "object",
            Self::OperationalState => "operational_state",
            Self::Principal => "principal",
            Self::RelationQuery => "relation_query",
            Self::RemoteTarget => "remote_target",
            Self::Restore => "restore",
            Self::ServiceAccount => "service_account",
            Self::TaskExecution => "task_execution",
            Self::TaskQueue => "task_queue",
            Self::TokenRetention => "token_retention",
            Self::Token => "token",
            Self::Transaction => "transaction",
            Self::UnifiedSearch => "unified_search",
            Self::User => "user",
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
