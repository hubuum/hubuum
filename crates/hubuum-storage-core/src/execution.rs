use std::fmt;
use std::future::Future;
use std::pin::Pin;

use hubuum_domain::{
    ClassId, ClassRelationId, CollectionId, ComputedFieldDefinitionId, EventSinkId,
    EventSubscriptionId, ExportTemplateId, GroupId, IdentityScopeId, ObjectId, ObjectRelationId,
    PrincipalId, RemoteTargetId, ResourceRevision, TokenId,
};
use hubuum_events_core::MutationProvenance;

use crate::StorageQueryBudget;

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

/// A composable override applied while one unit of application work runs.
///
/// An absent field inherits the surrounding scope. A present optional value
/// explicitly replaces that field, including clearing an inherited value with
/// `None`. This distinction makes independently supplied scope layers compose
/// without accidentally resetting each other.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StorageExecutionScope {
    call_site: Option<StorageCallSite>,
    mutation_provenance: Option<Option<MutationProvenance>>,
    revision_precondition: Option<Option<StorageRevisionPrecondition>>,
    query_budget: Option<Option<StorageQueryBudget>>,
}

impl StorageExecutionScope {
    #[must_use]
    pub const fn with_call_site(mut self, call_site: StorageCallSite) -> Self {
        self.call_site = Some(call_site);
        self
    }

    #[must_use]
    pub fn with_mutation_provenance(mut self, provenance: Option<MutationProvenance>) -> Self {
        self.mutation_provenance = Some(provenance);
        self
    }

    #[must_use]
    pub fn with_revision_precondition(
        mut self,
        precondition: Option<StorageRevisionPrecondition>,
    ) -> Self {
        self.revision_precondition = Some(precondition);
        self
    }

    #[must_use]
    pub const fn with_query_budget(mut self, budget: Option<StorageQueryBudget>) -> Self {
        self.query_budget = Some(budget);
        self
    }

    /// Return the call-site override, or `None` when the surrounding scope is
    /// inherited.
    #[must_use]
    pub const fn call_site_override(&self) -> Option<StorageCallSite> {
        self.call_site
    }

    /// Return the provenance override. The outer option distinguishes
    /// inheritance from an explicit clear.
    #[must_use]
    pub fn mutation_provenance_override(&self) -> Option<&Option<MutationProvenance>> {
        self.mutation_provenance.as_ref()
    }

    /// Return the revision-precondition override. The outer option
    /// distinguishes inheritance from an explicit clear.
    #[must_use]
    pub fn revision_precondition_override(&self) -> Option<&Option<StorageRevisionPrecondition>> {
        self.revision_precondition.as_ref()
    }

    /// Return the query-budget override. The outer option distinguishes
    /// inheritance from an explicit clear.
    #[must_use]
    pub const fn query_budget_override(&self) -> Option<Option<StorageQueryBudget>> {
        self.query_budget
    }
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
pub trait ExecutionStorage: Send + Sync {
    /// Run task-local work under the supplied composable scope overrides.
    fn run_in_scope<'a, F, R>(
        &'a self,
        scope: StorageExecutionScope,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a;

    /// Send-capable form used by work that crosses a task or thread boundary.
    fn run_in_scope_send<'a, F, R>(
        &'a self,
        scope: StorageExecutionScope,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + Send + 'a>>
    where
        F: Future<Output = R> + Send + 'a,
        R: Send + 'a;
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

    #[test]
    fn execution_scope_distinguishes_inheritance_from_explicit_clear() {
        let inherited = StorageExecutionScope::default();
        let cleared = StorageExecutionScope::default().with_revision_precondition(None);

        assert_eq!(inherited.revision_precondition_override(), None);
        assert_eq!(cleared.revision_precondition_override(), Some(&None));
    }
}
