use std::fmt;
use std::future::Future;
use std::pin::Pin;

use hubuum_domain::{
    ClassId, ClassRelationId, CollectionId, ComputedFieldDefinitionId, EventSinkId,
    EventSubscriptionId, ExportTemplateId, GroupId, IdentityScopeId, ObjectId, ObjectRelationId,
    PrincipalId, RemoteTargetId, ResourceRevision, TokenId,
};
use hubuum_events_core::MutationProvenance;

/// Bounded attribution for storage work initiated by one application
/// subsystem.
///
/// These labels are part of the complete storage contract so every selectable
/// backend receives the same low-cardinality diagnostic context.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StorageCallSite {
    EventDelivery,
    EventFanout,
    EventRetention,
    HttpRequest,
    MetricsRefresh,
    Readiness,
    RequestMaintenance,
    RestoreCoordinator,
    TaskLease,
    TaskWorker,
    TokenRetention,
    #[default]
    Unattributed,
}

impl StorageCallSite {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EventDelivery => "event_delivery",
            Self::EventFanout => "event_fanout",
            Self::EventRetention => "event_retention",
            Self::HttpRequest => "http_request",
            Self::MetricsRefresh => "metrics_refresh",
            Self::Readiness => "readiness",
            Self::RequestMaintenance => "request_maintenance",
            Self::RestoreCoordinator => "restore_coordinator",
            Self::TaskLease => "task_lease",
            Self::TaskWorker => "task_worker",
            Self::TokenRetention => "token_retention",
            Self::Unattributed => "unattributed",
        }
    }
}

/// Backend-neutral optimistic-concurrency assertion.
///
/// An empty revision list represents an existence-only wildcard assertion.
/// Non-empty lists contain the positive revisions accepted by the caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StorageRevisionTarget {
    IdentityScope(IdentityScopeId),
    Group(GroupId),
    Principal(PrincipalId),
    Membership {
        principal_id: PrincipalId,
        group_id: GroupId,
    },
    Collection(CollectionId),
    CollectionPermissions(CollectionId),
    Class(ClassId),
    Object(ObjectId),
    ClassRelation(ClassRelationId),
    ObjectRelation(ObjectRelationId),
    ExportTemplate(ExportTemplateId),
    RemoteTarget(RemoteTargetId),
    EventSink(EventSinkId),
    EventSubscription(EventSubscriptionId),
    ComputedField(ComputedFieldDefinitionId),
    Token(TokenId),
}

impl StorageRevisionTarget {
    const fn kind(self) -> &'static str {
        match self {
            Self::IdentityScope(_) => "identity_scope",
            Self::Group(_) => "group",
            Self::Principal(_) => "principal",
            Self::Membership { .. } => "membership",
            Self::Collection(_) => "collection",
            Self::CollectionPermissions(_) => "collection_permissions",
            Self::Class(_) => "class",
            Self::Object(_) => "object",
            Self::ClassRelation(_) => "class_relation",
            Self::ObjectRelation(_) => "object_relation",
            Self::ExportTemplate(_) => "export_template",
            Self::RemoteTarget(_) => "remote_target",
            Self::EventSink(_) => "event_sink",
            Self::EventSubscription(_) => "event_subscription",
            Self::ComputedField(_) => "computed_field",
            Self::Token(_) => "token",
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageRevisionPrecondition {
    target: StorageRevisionTarget,
    revisions: Vec<ResourceRevision>,
}

impl fmt::Debug for StorageRevisionPrecondition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRevisionPrecondition")
            .field("target_kind", &self.target.kind())
            .field("revision_count", &self.revisions.len())
            .finish()
    }
}

impl StorageRevisionPrecondition {
    #[must_use]
    pub const fn new(target: StorageRevisionTarget, revisions: Vec<ResourceRevision>) -> Self {
        Self { target, revisions }
    }

    #[must_use]
    pub const fn target(&self) -> StorageRevisionTarget {
        self.target
    }

    #[must_use]
    pub fn revisions(&self) -> &[ResourceRevision] {
        &self.revisions
    }
}

/// Execution context every selectable backend must honor.
///
/// The contract carries diagnostic attribution, durable mutation provenance,
/// and optimistic-concurrency assertions without exposing task locals,
/// transaction settings, connections, or database-specific session state.
pub trait StorageExecution: Send + Sync {
    fn run_with_call_site<'a, F, R>(
        &'a self,
        call_site: StorageCallSite,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a;

    /// Send-capable form used by work that crosses a task or thread boundary.
    fn run_with_call_site_send<'a, F, R>(
        &'a self,
        call_site: StorageCallSite,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + Send + 'a>>
    where
        F: Future<Output = R> + Send + 'a,
        R: Send + 'a;

    fn run_with_mutation_provenance<'a, F, R>(
        &'a self,
        provenance: Option<MutationProvenance>,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a;

    fn run_with_revision_precondition<'a, F, R>(
        &'a self,
        precondition: Option<StorageRevisionPrecondition>,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn call_site_labels_are_stable_and_bounded() {
        assert_eq!(
            [
                StorageCallSite::EventDelivery,
                StorageCallSite::EventFanout,
                StorageCallSite::EventRetention,
                StorageCallSite::HttpRequest,
                StorageCallSite::MetricsRefresh,
                StorageCallSite::Readiness,
                StorageCallSite::RequestMaintenance,
                StorageCallSite::RestoreCoordinator,
                StorageCallSite::TaskLease,
                StorageCallSite::TaskWorker,
                StorageCallSite::TokenRetention,
                StorageCallSite::Unattributed,
            ]
            .map(StorageCallSite::as_str),
            [
                "event_delivery",
                "event_fanout",
                "event_retention",
                "http_request",
                "metrics_refresh",
                "readiness",
                "request_maintenance",
                "restore_coordinator",
                "task_lease",
                "task_worker",
                "token_retention",
                "unattributed",
            ]
        );
    }

    #[test]
    fn wildcard_revision_preconditions_are_valid() {
        let precondition = StorageRevisionPrecondition::new(
            StorageRevisionTarget::Collection(CollectionId::new(7).unwrap()),
            Vec::new(),
        );

        assert!(precondition.revisions().is_empty());
    }

    #[test]
    fn revision_preconditions_retain_typed_parts() {
        let target = StorageRevisionTarget::Collection(CollectionId::new(7).unwrap());
        let revision = ResourceRevision::new(3).unwrap();
        let precondition = StorageRevisionPrecondition::new(target, vec![revision]);

        assert_eq!(precondition.target(), target);
        assert_eq!(precondition.revisions(), &[revision]);
    }
}
