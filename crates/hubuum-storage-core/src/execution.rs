use std::fmt;
use std::future::Future;
use std::pin::Pin;

use hubuum_domain::{
    ClassId, ClassRelationId, CollectionId, ComputedFieldDefinitionId, EventSinkId,
    EventSubscriptionId, ExportTemplateId, GroupId, IdentityScopeId, ObjectId, ObjectRelationId,
    PrincipalId, RemoteTargetId, ResourceRevision, TokenId,
};
use hubuum_events_core::MutationProvenance;
use hubuum_task_core::TaskExecutionContext;

use crate::{
    HistoryReadContext, StorageApiRequestPermit, StorageExecutionPermit, StorageQueryBudget,
};

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
    pub const ALL: [Self; 12] = [
        Self::EventDelivery,
        Self::EventFanout,
        Self::EventRetention,
        Self::HttpRequest,
        Self::MetricsRefresh,
        Self::Readiness,
        Self::RequestMaintenance,
        Self::RestoreCoordinator,
        Self::TaskLease,
        Self::TaskWorker,
        Self::TokenRetention,
        Self::Unattributed,
    ];

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
    storage_callback: bool,
    history_read: Option<HistoryReadContext>,
    api_request: Option<StorageApiRequestPermit>,
    application_access: Option<StorageExecutionPermit>,
    call_site: Option<StorageCallSite>,
    mutation_provenance: Option<Option<MutationProvenance>>,
    revision_precondition: Option<Option<StorageRevisionPrecondition>>,
    query_budget: Option<Option<StorageQueryBudget>>,
    task_execution: Option<Option<TaskExecutionContext>>,
}

impl StorageExecutionScope {
    /// Compose nested work without refreshing inherited ownership. A history
    /// context may acquire additional local restrictions, never weaker ones.
    #[must_use]
    pub fn nested_within(mut self, parent: &Self) -> Self {
        self.storage_callback |= parent.storage_callback;
        self.history_read = match (&parent.history_read, &self.history_read) {
            (Some(parent), Some(child)) if child.is_restriction_of(parent) => Some(child.clone()),
            (Some(parent), _) => Some(parent.clone()),
            (None, _) => self.history_read,
        };
        self.application_access = parent
            .application_access
            .clone()
            .or(self.application_access);
        self.api_request = parent.api_request.clone().or(self.api_request);
        self.task_execution = self
            .task_execution
            .or_else(|| parent.task_execution.clone());
        self.call_site = self.call_site.or(parent.call_site);
        self.mutation_provenance = self
            .mutation_provenance
            .or_else(|| parent.mutation_provenance.clone());
        self.revision_precondition = self
            .revision_precondition
            .or_else(|| parent.revision_precondition.clone());
        self.query_budget = self.query_budget.or(parent.query_budget);
        if let Some(context) = &self.history_read
            && let Some(binding) = self.history_binding().restriction_of(context.binding())
        {
            self.history_read = Some(context.with_binding(binding));
        }
        // Invalid changes retain the old proof and fail its exact validation
        // on productive entry; scope installation itself has a generic result.
        self
    }

    pub(crate) fn restriction_of(&self, parent: &Self) -> Option<Self> {
        let mut expected = parent.clone();
        if expected
            .revision_precondition
            .as_ref()
            .is_none_or(Option::is_none)
        {
            expected.revision_precondition = self.revision_precondition.clone();
        }
        if expected.query_budget.flatten().is_none_or(|original| {
            self.query_budget
                .flatten()
                .is_some_and(|budget| budget.as_millis() <= original.as_millis())
        }) {
            expected.query_budget = self.query_budget;
        }
        (self == &expected).then(|| self.clone())
    }

    /// Restrictive marker for a DB callback or its captured descendants. Scope
    /// layering must OR this bit; callers cannot clear an inherited marker.
    #[must_use]
    pub fn with_storage_callback(mut self) -> Self {
        self.storage_callback = true;
        self
    }
    pub const fn storage_callback_active(&self) -> bool {
        self.storage_callback
    }

    pub(crate) fn resume_history_binding(&self, original: &Self) -> Self {
        let mut value = self.clone();
        if value.application_access.is_none() {
            value.application_access = original.application_access.clone();
        }
        if value.api_request.is_none() {
            value.api_request = original.api_request.clone();
        }
        if value.task_execution.is_none() {
            value.task_execution = original.task_execution.clone();
        }
        if value.query_budget.is_none() {
            value.query_budget = original.query_budget;
        }
        if value.mutation_provenance.is_none() {
            value.mutation_provenance = original.mutation_provenance.clone();
        }
        if value.revision_precondition.is_none() {
            value.revision_precondition = original.revision_precondition.clone();
        }
        value
    }

    /// Effective authority binding; diagnostic attribution and the inherited
    /// history context do not mint productive authority. Explicitly absent
    /// optional overrides normalize to the same effective empty value.
    pub(crate) fn history_binding(&self) -> Self {
        let mut value = self.clone();
        value.storage_callback = false;
        value.history_read = None;
        value.call_site = None;
        value.task_execution = Some(value.task_execution.flatten());
        value.query_budget = Some(value.query_budget.flatten());
        value.mutation_provenance = Some(value.mutation_provenance.flatten());
        value.revision_precondition = Some(value.revision_precondition.flatten());
        value
    }

    /// Retain the original publication epoch across nested and blocking work.
    #[must_use]
    pub fn with_history_read(mut self, context: HistoryReadContext) -> Self {
        self.history_read = Some(context);
        self
    }

    #[must_use]
    pub const fn history_read_override(&self) -> Option<&HistoryReadContext> {
        self.history_read.as_ref()
    }

    #[must_use]
    pub fn with_api_request(mut self, permit: StorageApiRequestPermit) -> Self {
        self.api_request = Some(permit);
        self
    }

    #[must_use]
    pub const fn api_request_override(&self) -> Option<&StorageApiRequestPermit> {
        self.api_request.as_ref()
    }

    #[must_use]
    pub fn with_application_access(mut self, permit: StorageExecutionPermit) -> Self {
        self.application_access = Some(permit);
        self
    }

    #[must_use]
    pub const fn application_access_override(&self) -> Option<&StorageExecutionPermit> {
        self.application_access.as_ref()
    }

    #[must_use]
    pub fn with_task_execution(mut self, execution: Option<TaskExecutionContext>) -> Self {
        self.task_execution = Some(execution);
        self
    }

    #[must_use]
    pub fn task_execution_override(&self) -> Option<&Option<TaskExecutionContext>> {
        self.task_execution.as_ref()
    }

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
    fn current_execution_scope(&self) -> StorageExecutionScope;
    fn run_in_scope_sync<F, R>(&self, scope: StorageExecutionScope, operation: F) -> R
    where
        F: FnOnce() -> R;

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
