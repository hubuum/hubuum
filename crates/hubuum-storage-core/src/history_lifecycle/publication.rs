//! Publication ownership is independent of API occupancy and task leases.
//! Owners have no TTL; disappearance of a caller never proves quiescence.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;

use uuid::Uuid;

use super::producer::ProducerRecord;
use super::*;
use crate::{RuntimeIsolationDomain, StorageError, StorageExecutionPermit, StorageExecutionScope};

/// The complete producer classes supported by the online history boundary.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryPublicationKind {
    HttpResponse,
    TaskOutput,
    Backup,
    Export,
    ArchiveRead,
}
impl HistoryPublicationKind {
    /// Use the original admitting runtime's coverage. A recovery caller cannot
    /// upgrade an old parent-only owner by advertising stronger exclusion now.
    pub const fn isolated_retirement_supported(self, original: &RuntimeIsolationDomain) -> bool {
        match self {
            Self::HttpResponse | Self::Export => original.excludes_renderers(),
            Self::TaskOutput | Self::Backup | Self::ArchiveRead => true,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HttpResponse => "http_response",
            Self::TaskOutput => "task_output",
            Self::Backup => "backup",
            Self::Export => "export",
            Self::ArchiveRead => "archive_read",
        }
    }
}

/// Correlated adapter-validated facts; no raw request can manufacture this epoch.
#[derive(Clone, Eq, PartialEq)]
pub struct HistoryReadEpoch {
    application: StorageExecutionPermit,
    history: HistoryReviewFence,
    read_revision: Option<HistoryReadRevision>,
    generation: HistoryPublicationGeneration,
    scope: Box<StorageExecutionScope>,
}

impl fmt::Debug for HistoryReadEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HistoryReadEpoch")
            .field("history", &self.history)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

impl HistoryReadEpoch {
    pub fn new(
        application: StorageExecutionPermit,
        history: HistoryReviewFence,
        generation: HistoryPublicationGeneration,
    ) -> Result<Self, StorageValidationError> {
        if application
            .owner()
            .is_none_or(|owner| !owner.history_protocol().supports_online())
            || application.fence().incarnation() != history.incarnation()
            || application.fence().revision() as u64 != history.deletion_revision().value()
        {
            return Err(invalid(
                "Publication requires one registered application/history epoch",
            ));
        }
        let scope = Box::new(
            StorageExecutionScope::default()
                .with_application_access(application.clone())
                .history_binding(),
        );
        Ok(Self {
            application,
            history,
            read_revision: None,
            generation,
            scope,
        })
    }
    /// Archive ownership is available to the still-Legacy reader only while
    /// no expiry decision is retained. It does not advertise OnlineV1 support.
    pub fn for_publication(
        kind: HistoryPublicationKind,
        application: StorageExecutionPermit,
        snapshot: &HistoryLifecycleSnapshot,
        generation: HistoryPublicationGeneration,
    ) -> Result<Self, StorageValidationError> {
        if kind != HistoryPublicationKind::ArchiveRead
            || application
                .owner()
                .is_some_and(|owner| owner.history_protocol().supports_online())
        {
            let mut epoch = Self::new(application, snapshot.fence(), generation)?;
            epoch.read_revision = Some(snapshot.control().authority().read_revision());
            return Ok(epoch);
        }
        let history = snapshot.fence();
        if application.owner().is_none()
            || !snapshot.control().authority().expiry().is_empty()
            || application.fence().incarnation() != history.incarnation()
            || application.fence().revision() as u64 != history.deletion_revision().value()
        {
            return Err(invalid(
                "Legacy archive publication requires its unchanged pre-expiry epoch",
            ));
        }
        let scope = Box::new(
            StorageExecutionScope::default()
                .with_application_access(application.clone())
                .history_binding(),
        );
        Ok(Self {
            application,
            history,
            read_revision: None,
            generation,
            scope,
        })
    }
    /// Adapter admission binds the fully effective original productive scope.
    /// Rebinding a clone cannot change any already-issued private context.
    pub fn bind_scope(
        mut self,
        scope: &StorageExecutionScope,
    ) -> Result<Self, StorageValidationError> {
        if scope.application_access_override() != Some(&self.application) {
            return Err(invalid(
                "History epoch requires its original application scope",
            ));
        }
        self.scope = Box::new(scope.history_binding());
        Ok(self)
    }
    pub fn validate_scope(&self, scope: &StorageExecutionScope) -> Result<(), StorageError> {
        if self.scope.as_ref() != &scope.history_binding() {
            return Err(StorageError::unavailable(
                "History scope differs from its original productive binding",
            ));
        }
        Ok(())
    }

    pub const fn application(&self) -> &StorageExecutionPermit {
        &self.application
    }
    pub const fn history(&self) -> HistoryReviewFence {
        self.history
    }
    pub const fn read_revision(&self) -> Option<HistoryReadRevision> {
        self.read_revision
    }
    pub const fn generation(&self) -> HistoryPublicationGeneration {
        self.generation
    }
}

/// Cloneable producer context; it can be inherited but cannot retire its owner.
/// Neither this context nor its opaque ownership material is serializable.
#[derive(Clone, Eq, PartialEq)]
pub struct HistoryReadContext {
    id: Uuid,
    ownership: Uuid,
    kind: HistoryPublicationKind,
    epoch: HistoryReadEpoch,
    binding: Box<StorageExecutionScope>,
}

impl HistoryReadContext {
    pub(crate) fn binding(&self) -> &StorageExecutionScope {
        &self.binding
    }
    pub(crate) fn with_binding(&self, binding: StorageExecutionScope) -> Self {
        let mut context = self.clone();
        context.binding = Box::new(binding);
        context
    }
    /// A narrowed local context retains the original native owner and epoch.
    pub fn is_restriction_of(&self, parent: &Self) -> bool {
        self.same_publication(parent) && self.binding.restriction_of(&parent.binding).is_some()
    }
    fn same_publication(&self, other: &Self) -> bool {
        self.id == other.id
            && self.ownership == other.ownership
            && self.kind == other.kind
            && self.epoch == other.epoch
    }
    pub fn validate_scope(&self, scope: &StorageExecutionScope) -> Result<(), StorageError> {
        if self.binding.as_ref() != &scope.history_binding() {
            return Err(StorageError::unavailable(
                "History scope differs from its bound productive restrictions",
            ));
        }
        Ok(())
    }
    pub const fn id(&self) -> Uuid {
        self.id
    }
    /// Adapter comparison material. Never log or expose this nonce.
    pub const fn ownership_token(&self) -> Uuid {
        self.ownership
    }
    pub const fn kind(&self) -> HistoryPublicationKind {
        self.kind
    }
    pub const fn epoch(&self) -> &HistoryReadEpoch {
        &self.epoch
    }
}
impl fmt::Debug for HistoryReadContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HistoryReadContext")
            .field("id", &self.id)
            .field("kind", &self.kind)
            .field("epoch", &self.epoch)
            .finish_non_exhaustive()
    }
}

/// Single-use settlement capability, created only by an admitting adapter.
/// Dropping this value does not retire the persisted owner.
#[derive(Debug)]
pub struct HistoryPublicationOwner(HistoryReadContext);
pub(crate) struct HistoryDriverRelease(HistoryReadContext);

impl HistoryPublicationOwner {
    /// Adapter boundary after atomic capacity, registered-protocol and epoch checks.
    pub fn generate(kind: HistoryPublicationKind, epoch: HistoryReadEpoch) -> Self {
        Self(HistoryReadContext {
            id: Uuid::new_v4(),
            ownership: Uuid::new_v4(),
            kind,
            binding: epoch.scope.clone(),
            epoch,
        })
    }
    pub fn context(&self) -> HistoryReadContext {
        self.0.clone()
    }

    pub(crate) async fn archive_driver_released<D>(
        &self,
        driver: D,
    ) -> Result<HistoryDriverRelease, StorageError>
    where
        D: Future<Output = Result<(), StorageError>>,
    {
        driver.await?;
        Ok(HistoryDriverRelease(self.0.clone()))
    }
    pub(crate) fn after_archive_drained(
        self,
        driver: HistoryDriverRelease,
        child: &HistoryProducerSettlement,
    ) -> Result<HistoryPublicationSettlement, StorageError> {
        if driver.0 != self.0 || child.context().parent() != &self.0 {
            return Err(StorageError::conflict(
                "Archive completion belongs to different owners",
            ));
        }
        Ok(HistoryPublicationSettlement(self.0))
    }

    /// HTTP callers supply a positive same-worker driver-release future and a
    /// separately supervised producer-drain future. Other producer classes use
    /// their actual final publication driver. EOF, channel closure, a timer or
    /// a shutdown request must resolve as errors unless completion is proved.
    ///
    /// Poll the driver first so its bounded abort/stop supervision can run even
    /// while a producer is blocked. A failed drain retains the native owner.
    pub async fn after_drained<D, P>(
        self,
        driver: D,
        producer: P,
    ) -> Result<HistoryPublicationSettlement, StorageError>
    where
        D: Future<Output = Result<(), StorageError>>,
        P: Future<Output = Result<(), StorageError>>,
    {
        driver.await?;
        producer.await?;
        Ok(HistoryPublicationSettlement(self.0))
    }
}

/// Consumes positive producer and publication completion; cannot be deserialized
/// or constructed from a clock, absent owner row or retired API permit.
#[derive(Debug)]
pub struct HistoryPublicationSettlement(HistoryReadContext);
impl HistoryPublicationSettlement {
    pub const fn context(&self) -> &HistoryReadContext {
        &self.0
    }
}

/// Cutoff captured while advancing admission under history authority. New
/// producers belong to a later generation and must obey retained exclusions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HistoryPublicationBarrier {
    incarnation: Uuid,
    cutoff: HistoryPublicationGeneration,
}
impl HistoryPublicationBarrier {
    pub const fn incarnation(self) -> Uuid {
        self.incarnation
    }
    pub const fn cutoff(self) -> HistoryPublicationGeneration {
        self.cutoff
    }
}

/// Logical contract used by the complete memory adapter. Native adapters enforce
/// the same limits and transitions in transactions, not with process-local state.
#[derive(Clone, Debug)]
pub struct HistoryPublicationRegistry {
    generation: HistoryPublicationGeneration,
    owners: BTreeMap<Uuid, HistoryReadContext>,
    producers: BTreeMap<Uuid, ProducerRecord>,
    isolation: BTreeMap<Uuid, Option<RuntimeIsolationDomain>>,
}
impl Default for HistoryPublicationRegistry {
    fn default() -> Self {
        Self {
            generation: HistoryPublicationGeneration::new(0).expect("zero publication generation"),
            owners: BTreeMap::new(),
            producers: BTreeMap::new(),
            isolation: BTreeMap::new(),
        }
    }
}
impl HistoryPublicationRegistry {
    pub const MAX_OWNERS: usize = 256;
    pub const MAX_RUNTIME_OWNERS: usize = 64;

    pub const fn generation(&self) -> HistoryPublicationGeneration {
        self.generation
    }
    pub fn active(&self) -> usize {
        self.owners.len()
    }
    pub fn admit(
        &mut self,
        kind: HistoryPublicationKind,
        epoch: HistoryReadEpoch,
    ) -> Result<HistoryPublicationOwner, StorageError> {
        self.admit_isolated(kind, epoch, None)
    }

    /// Retain the admitting process domain even after registration removal.
    pub fn admit_isolated(
        &mut self,
        kind: HistoryPublicationKind,
        epoch: HistoryReadEpoch,
        isolation: Option<RuntimeIsolationDomain>,
    ) -> Result<HistoryPublicationOwner, StorageError> {
        if kind != HistoryPublicationKind::ArchiveRead
            && epoch
                .application()
                .owner()
                .is_none_or(|owner| !owner.history_protocol().supports_online())
        {
            return Err(StorageError::unavailable(
                "Legacy epoch is restricted to archive reads",
            ));
        }
        if epoch.generation() != self.generation {
            return Err(StorageError::conflict(
                "Publication admission generation changed",
            ));
        }
        if self.owners.len() >= Self::MAX_OWNERS {
            return Err(StorageError::unavailable(
                "Publication owner capacity exhausted",
            ));
        }
        let runtime = epoch
            .application()
            .owner()
            .expect("validated registered epoch")
            .id();
        if self
            .owners
            .values()
            .filter(|owner| {
                owner
                    .epoch
                    .application()
                    .owner()
                    .is_some_and(|owner| owner.id() == runtime)
            })
            .count()
            >= Self::MAX_RUNTIME_OWNERS
        {
            return Err(StorageError::unavailable(
                "Runtime publication owner capacity exhausted",
            ));
        }
        let owner = HistoryPublicationOwner::generate(kind, epoch);
        self.isolation.insert(owner.0.id, isolation);
        self.owners.insert(owner.0.id, owner.context());
        Ok(owner)
    }
    pub fn check(
        &self,
        context: &HistoryReadContext,
        current: &HistoryReadEpoch,
    ) -> Result<(), StorageError> {
        if self
            .owners
            .get(&context.id)
            .is_none_or(|owner| !context.same_publication(owner))
            || context.epoch.application != current.application
            || (context.epoch.history != current.history
                && !(context.kind == HistoryPublicationKind::TaskOutput
                    && !self.producers.contains_key(&context.id)
                    && context.epoch.read_revision.is_some()
                    && context.epoch.read_revision == current.read_revision))
            || context.epoch.generation != current.generation
            || context.binding != current.scope
            || current.generation() != self.generation
        {
            return Err(StorageError::unavailable(
                "Publication context is stale, retired or mismatched",
            ));
        }
        Ok(())
    }
    pub fn check_producer(
        &self,
        context: &HistoryProducerContext,
        current: &HistoryReadEpoch,
    ) -> Result<(), StorageError> {
        if self
            .producers
            .get(&context.parent().id())
            .is_none_or(|record| record.retired || record.context != *context)
        {
            return Err(StorageError::unavailable(
                "Archive producer is missing, retired or mismatched",
            ));
        }
        self.check(context.parent(), current)
    }
    /// Reserve before issuing work. A retired slot is not reusable: each
    /// publication owns one bounded producer obligation for its whole lifetime.
    pub fn admit_producer(
        &mut self,
        context: &HistoryReadContext,
        current: &HistoryReadEpoch,
    ) -> Result<HistoryProducerOwner, StorageError> {
        self.check(context, current)?;
        if context.epoch.history != current.history {
            return Err(StorageError::unavailable(
                "Producer requires its original full history epoch",
            ));
        }
        if self.producers.contains_key(&context.id()) {
            return Err(StorageError::unavailable(
                "Publication producer slot is already assigned",
            ));
        }
        let producer = HistoryProducerOwner::generate(context.clone());
        self.producers.insert(
            context.id(),
            ProducerRecord {
                context: producer.context().clone(),
                retired: false,
                publication_claimed: false,
            },
        );
        Ok(producer)
    }
    /// Record unfinished physical publication alongside its exact child. The
    /// complete adapter commits the fixed manifest/pin in the same transition.
    /// No completion API exists until a real physical outcome can be proven.
    pub fn claim_archive_producer(
        &mut self,
        context: &HistoryProducerContext,
        current: &HistoryReadEpoch,
    ) -> Result<(), StorageError> {
        self.check_producer(context, current)?;
        if context.parent().kind() != HistoryPublicationKind::ArchiveRead {
            return Err(StorageError::unavailable(
                "Artifact publication requires its original archive producer",
            ));
        }
        let record = self
            .producers
            .get_mut(&context.parent().id())
            .expect("producer validated under exclusive registry access");
        if record.publication_claimed {
            return Err(StorageError::conflict(
                "Publication claim is already assigned",
            ));
        }
        record.publication_claimed = true;
        Ok(())
    }

    /// Exact, idempotent settlement; no current API/application/history scope is
    /// reacquired, so known old producers can finish after fencing or restore.
    pub fn settle_producer(
        &mut self,
        settlement: &HistoryProducerSettlement,
    ) -> Result<(), StorageError> {
        let context = settlement.context();
        let record = self
            .producers
            .get_mut(&context.parent().id())
            .filter(|record| record.context == *context)
            .ok_or_else(|| {
                StorageError::conflict("Producer settlement is missing or mismatched")
            })?;
        if record.publication_claimed {
            return Err(StorageError::unavailable(
                "Producer still owns an unfinished artifact claim",
            ));
        }
        record.retired = true;
        Ok(())
    }
    pub fn active_producers(&self) -> usize {
        self.producers
            .values()
            .filter(|record| !record.retired)
            .count()
    }
    pub fn advance(
        &mut self,
        incarnation: Uuid,
    ) -> Result<HistoryPublicationBarrier, StorageValidationError> {
        if incarnation.is_nil() {
            return Err(invalid(
                "Publication barrier requires an application incarnation",
            ));
        }
        let cutoff = self.generation;
        self.generation = HistoryPublicationGeneration::new(
            cutoff
                .value()
                .checked_add(1)
                .ok_or_else(|| invalid("Publication generation exhausted"))?,
        )?;
        Ok(HistoryPublicationBarrier {
            incarnation,
            cutoff,
        })
    }
    pub fn blocked(&self, barrier: HistoryPublicationBarrier) -> bool {
        self.owners.values().any(|owner| {
            owner.epoch.history().incarnation() != barrier.incarnation
                || owner.epoch.generation() <= barrier.cutoff
        })
    }

    /// Owners admitted before the exact exclusion can still hold materialized
    /// bytes. Only their positive settlement removes this obligation.
    pub fn expiry_blocked(&self, fence: HistoryReviewFence) -> bool {
        self.owners.values().any(|owner| {
            owner.epoch.history().incarnation() != fence.incarnation()
                || owner.epoch.history().authority_revision() <= fence.authority_revision()
        })
    }

    /// Trusted adapter boundary while holding actual exclusive process isolation
    /// and the application fence. External publication claims retain their owner:
    /// process exit does not reconcile an independently durable provider result.
    pub fn retire_isolated(
        &mut self,
        domain: &RuntimeIsolationDomain,
    ) -> Result<usize, StorageError> {
        if self.owners.keys().any(|id| {
            self.isolation
                .get(id)
                .and_then(Option::as_ref)
                .is_none_or(|original| !original.same_lock(domain))
        }) || self
            .producers
            .keys()
            .any(|id| !self.owners.contains_key(id))
        {
            return Err(StorageError::unavailable(
                "Unknown or different publication process isolation prevents recovery",
            ));
        }
        let ids: Vec<_> = self
            .owners
            .keys()
            .copied()
            .filter(|id| {
                self.owners[id].kind.isolated_retirement_supported(
                    self.isolation[id]
                        .as_ref()
                        .expect("validated original isolation"),
                ) && self
                    .producers
                    .get(id)
                    .is_none_or(|producer| !producer.publication_claimed)
            })
            .collect();
        for id in &ids {
            self.producers.remove(id);
            self.owners.remove(id);
            self.isolation.remove(id);
        }
        Ok(ids.len())
    }
    /// Exact-owner-only settlement. No current application/history/API owner is
    /// reacquired: old producers still need to settle after fencing or restore.
    pub fn settle(&mut self, settlement: HistoryPublicationSettlement) -> Result<(), StorageError> {
        let context = settlement.0;
        if self.owners.get(&context.id) != Some(&context) {
            return Err(StorageError::conflict(
                "Publication settlement owner is missing or mismatched",
            ));
        }
        if self
            .producers
            .get(&context.id)
            .is_some_and(|producer| !producer.retired || producer.publication_claimed)
        {
            return Err(StorageError::unavailable(
                "Publication still owns an unsettled producer",
            ));
        }
        self.producers.remove(&context.id);
        self.owners.remove(&context.id);
        self.isolation.remove(&context.id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Future, ready};
    use std::task::{Context, Poll, Waker};

    use rstest::rstest;

    use super::*;
    use crate::{ApplicationFence, RuntimeRegistration};

    fn epoch(runtime: &RuntimeRegistration, incarnation: Uuid) -> HistoryReadEpoch {
        HistoryReadEpoch::new(
            StorageExecutionPermit::new(
                Some(runtime.owner()),
                ApplicationFence::new(incarnation, 0).unwrap(),
            ),
            HistoryReviewFence::new(
                incarnation,
                HistoryAuthorityRevision::new(0).unwrap(),
                HistoryDeletionRevision::new(0).unwrap(),
            )
            .unwrap(),
            HistoryPublicationGeneration::new(0).unwrap(),
        )
        .unwrap()
    }
    fn immediate<T>(future: impl Future<Output = T>) -> T {
        match std::pin::pin!(future)
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("ready drain fixture unexpectedly blocked"),
        }
    }

    #[rstest]
    #[case::known(true, false, true)]
    #[case::unknown(false, false, false)]
    #[case::foreign(true, true, false)]
    fn isolated_retirement_requires_every_original_domain(
        #[case] known: bool,
        #[case] foreign: bool,
        #[case] retired: bool,
    ) {
        let domain = RuntimeIsolationDomain::new(Uuid::new_v4(), 1, 2).unwrap();
        let other = RuntimeIsolationDomain::new(Uuid::new_v4(), 1, 2).unwrap();
        let runtime = RuntimeRegistration::generate_history_aware();
        let incarnation = Uuid::new_v4();
        let mut registry = HistoryPublicationRegistry::default();
        // A valid owner must not be partially retired if another is unknown.
        registry
            .admit_isolated(
                HistoryPublicationKind::Backup,
                epoch(&runtime, incarnation),
                Some(domain.clone()),
            )
            .unwrap();
        let owner = registry
            .admit_isolated(
                HistoryPublicationKind::TaskOutput,
                epoch(&runtime, incarnation),
                known.then_some(if foreign { other } else { domain.clone() }),
            )
            .unwrap();
        let context = owner.context();
        registry.admit_producer(&context, context.epoch()).unwrap();
        let result = registry.retire_isolated(&domain);
        assert_eq!(result.is_ok(), retired);
        assert_eq!(registry.active(), if retired { 0 } else { 2 });
        assert_eq!(registry.active_producers(), usize::from(!retired));
    }

    #[rstest]
    #[case::http(HistoryPublicationKind::HttpResponse)]
    #[case::export(HistoryPublicationKind::Export)]
    fn parent_process_isolation_cannot_retire_renderer_capable_owners(
        #[case] kind: HistoryPublicationKind,
        #[values(false, true)] recovery_advertises_renderers: bool,
    ) {
        let domain = RuntimeIsolationDomain::new(Uuid::new_v4(), 1, 2).unwrap();
        let runtime = RuntimeRegistration::generate_history_aware();
        let mut registry = HistoryPublicationRegistry::default();
        let owner = registry
            .admit_isolated(kind, epoch(&runtime, Uuid::new_v4()), Some(domain.clone()))
            .unwrap();
        let context = owner.context();
        registry.admit_producer(&context, context.epoch()).unwrap();
        let recovery = if recovery_advertises_renderers {
            domain.with_renderer_isolation()
        } else {
            domain
        };
        assert_eq!(registry.retire_isolated(&recovery).unwrap(), 0);
        assert_eq!(registry.active(), 1);
        assert_eq!(registry.active_producers(), 1);
    }

    #[rstest]
    #[case::http(HistoryPublicationKind::HttpResponse)]
    #[case::export(HistoryPublicationKind::Export)]
    fn original_inherited_renderer_lock_allows_positive_isolated_retirement(
        #[case] kind: HistoryPublicationKind,
    ) {
        let parent = RuntimeIsolationDomain::new(Uuid::new_v4(), 1, 2).unwrap();
        let runtime = RuntimeRegistration::generate_history_aware();
        let mut registry = HistoryPublicationRegistry::default();
        let owner = registry
            .admit_isolated(
                kind,
                epoch(&runtime, Uuid::new_v4()),
                Some(parent.clone().with_renderer_isolation()),
            )
            .unwrap();
        let context = owner.context();
        registry.admit_producer(&context, context.epoch()).unwrap();
        assert_eq!(registry.retire_isolated(&parent).unwrap(), 1);
        assert_eq!(registry.active(), 0);
        assert_eq!(registry.active_producers(), 0);
    }

    #[test]
    fn isolated_exit_cannot_reconcile_a_claimed_provider_publication() {
        let domain = RuntimeIsolationDomain::new(Uuid::new_v4(), 1, 2).unwrap();
        let runtime = RuntimeRegistration::generate_history_aware();
        let mut registry = HistoryPublicationRegistry::default();
        let owner = registry
            .admit_isolated(
                HistoryPublicationKind::ArchiveRead,
                epoch(&runtime, Uuid::new_v4()),
                Some(domain.clone()),
            )
            .unwrap();
        let context = owner.context();
        let producer = registry.admit_producer(&context, context.epoch()).unwrap();
        registry
            .claim_archive_producer(producer.context(), context.epoch())
            .unwrap();
        assert_eq!(registry.retire_isolated(&domain).unwrap(), 0);
        assert_eq!(registry.active(), 1);
        assert_eq!(registry.active_producers(), 1);
    }

    #[rstest]
    #[case::archive(HistoryPublicationKind::ArchiveRead, true)]
    #[case::http(HistoryPublicationKind::HttpResponse, false)]
    #[case::backup(HistoryPublicationKind::Backup, false)]
    #[case::task(HistoryPublicationKind::TaskOutput, false)]
    #[case::export(HistoryPublicationKind::Export, false)]
    fn legacy_archive_epoch_cannot_admit_another_producer_class(
        #[case] kind: HistoryPublicationKind,
        #[case] allowed: bool,
    ) {
        let runtime = RuntimeRegistration::generate();
        let incarnation = Uuid::new_v4();
        let snapshot = HistoryLifecycleSnapshot::new(
            HistoryLifecycleControl::empty(),
            incarnation,
            HistoryDeletionRevision::new(0).unwrap(),
            chrono::Utc::now(),
        )
        .unwrap();
        let application = StorageExecutionPermit::new(
            Some(runtime.owner()),
            ApplicationFence::new(incarnation, 0).unwrap(),
        );
        assert!(
            HistoryReadEpoch::new(
                application.clone(),
                snapshot.fence(),
                HistoryPublicationGeneration::new(0).unwrap()
            )
            .is_err()
        );
        let epoch = HistoryReadEpoch::for_publication(
            HistoryPublicationKind::ArchiveRead,
            application,
            &snapshot,
            HistoryPublicationGeneration::new(0).unwrap(),
        )
        .unwrap();
        let mut registry = HistoryPublicationRegistry::default();
        assert_eq!(registry.admit(kind, epoch).is_ok(), allowed);
        assert_eq!(registry.active(), usize::from(allowed));
    }

    #[rstest]
    #[case::driver_unproved(false, true, false)]
    #[case::producer_unproved(true, false, false)]
    #[case::both_drained(true, true, true)]
    fn publication_retirement_requires_both_drains(
        #[case] driver: bool,
        #[case] producer: bool,
        #[case] retired: bool,
    ) {
        let runtime = RuntimeRegistration::generate_history_aware();
        let incarnation = Uuid::new_v4();
        let mut registry = HistoryPublicationRegistry::default();
        let owner = registry
            .admit(
                HistoryPublicationKind::HttpResponse,
                epoch(&runtime, incarnation),
            )
            .unwrap();
        let barrier = registry.advance(incarnation).unwrap();
        assert!(registry.blocked(barrier));
        let drain = |proved| {
            if proved {
                Ok(())
            } else {
                Err(StorageError::unavailable("unproved drain"))
            }
        };
        if let Ok(settlement) =
            immediate(owner.after_drained(ready(drain(driver)), ready(drain(producer))))
        {
            registry.settle(settlement).unwrap();
        }
        assert_eq!(!registry.blocked(barrier), retired);
    }

    #[rstest]
    #[case::dropped(0, false)]
    #[case::unproved_completion(1, false)]
    #[case::completed(2, true)]
    fn producer_slot_blocks_parent_until_positive_completion(
        #[case] outcome: u8,
        #[case] retired: bool,
    ) {
        let runtime = RuntimeRegistration::generate_history_aware();
        let incarnation = Uuid::new_v4();
        let mut registry = HistoryPublicationRegistry::default();
        let owner = registry
            .admit(
                HistoryPublicationKind::TaskOutput,
                epoch(&runtime, incarnation),
            )
            .unwrap();
        let context = owner.context();
        let producer = registry.admit_producer(&context, context.epoch()).unwrap();
        let barrier = registry.advance(incarnation).unwrap();
        match outcome {
            0 => {
                let _unacknowledged = producer;
            }
            1 => {
                let completion = immediate(
                    producer.after_completed(ready(Err(StorageError::unavailable(
                        "unproved producer",
                    )))),
                );
                assert!(completion.is_err());
            }
            2 => {
                let completion = immediate(producer.after_completed(ready(Ok(())))).unwrap();
                registry.settle_producer(&completion).unwrap();
            }
            _ => unreachable!(),
        }
        let parent = immediate(owner.after_drained(ready(Ok(())), ready(Ok(())))).unwrap();
        assert_eq!(registry.settle(parent).is_ok(), retired);
        assert_eq!(registry.blocked(barrier), !retired);
        assert_eq!(registry.active_producers(), usize::from(!retired));
    }

    #[test]
    fn producer_completion_is_idempotent_without_reopening_its_slot() {
        let runtime = RuntimeRegistration::generate_history_aware();
        let mut registry = HistoryPublicationRegistry::default();
        let owner = registry
            .admit(
                HistoryPublicationKind::TaskOutput,
                epoch(&runtime, Uuid::new_v4()),
            )
            .unwrap();
        let context = owner.context();
        let producer = registry.admit_producer(&context, context.epoch()).unwrap();
        let completion = immediate(producer.after_completed(ready(Ok(())))).unwrap();
        registry.settle_producer(&completion).unwrap();
        registry.settle_producer(&completion).unwrap();
        assert_eq!(registry.active_producers(), 0);
        assert!(registry.admit_producer(&context, context.epoch()).is_err());
        registry
            .settle(immediate(owner.after_drained(ready(Ok(())), ready(Ok(())))).unwrap())
            .unwrap();
        assert_eq!(registry.active(), 0);
    }

    #[test]
    fn dropping_owners_never_evicts_them_at_runtime_capacity() {
        let runtime = RuntimeRegistration::generate_history_aware();
        let incarnation = Uuid::new_v4();
        let mut registry = HistoryPublicationRegistry::default();
        for _ in 0..HistoryPublicationRegistry::MAX_RUNTIME_OWNERS {
            let _owner = registry
                .admit(
                    HistoryPublicationKind::HttpResponse,
                    epoch(&runtime, incarnation),
                )
                .unwrap();
        }
        assert!(
            registry
                .admit(
                    HistoryPublicationKind::HttpResponse,
                    epoch(&runtime, incarnation)
                )
                .is_err()
        );
        assert_eq!(
            registry.active(),
            HistoryPublicationRegistry::MAX_RUNTIME_OWNERS
        );
    }

    #[test]
    fn advanced_generation_cannot_refresh_an_existing_context() {
        let runtime = RuntimeRegistration::generate_history_aware();
        let incarnation = Uuid::new_v4();
        let mut registry = HistoryPublicationRegistry::default();
        let owner = registry
            .admit(HistoryPublicationKind::Backup, epoch(&runtime, incarnation))
            .unwrap();
        let context = owner.context();
        registry.advance(incarnation).unwrap();
        let fresh = HistoryReadEpoch::new(
            context.epoch().application().clone(),
            context.epoch().history(),
            registry.generation(),
        )
        .unwrap();
        assert!(registry.check(&context, &fresh).is_err());
    }
    #[rstest]
    #[case::original_epoch(false)]
    #[case::advanced_epoch(true)]
    fn claimed_producer_remains_charged_after_join_or_epoch_change(#[case] advance: bool) {
        let runtime = RuntimeRegistration::generate_history_aware();
        let incarnation = Uuid::new_v4();
        let mut registry = HistoryPublicationRegistry::default();
        let parent = registry
            .admit(
                HistoryPublicationKind::ArchiveRead,
                epoch(&runtime, incarnation),
            )
            .unwrap();
        let context = parent.context();
        let producer = registry.admit_producer(&context, context.epoch()).unwrap();
        registry
            .claim_archive_producer(producer.context(), context.epoch())
            .unwrap();
        assert!(
            registry
                .claim_archive_producer(producer.context(), context.epoch())
                .is_err()
        );
        if advance {
            registry.advance(incarnation).unwrap();
        }
        // Cloning retained state cannot lose the obligation. A joined task is
        // deliberately not a provider publication/cleanup outcome.
        let mut retained = registry.clone();
        let completed = immediate(producer.after_completed(ready(Ok(())))).unwrap();
        assert!(retained.settle_producer(&completed).is_err());
        let completed = immediate(parent.after_drained(ready(Ok(())), ready(Ok(())))).unwrap();
        assert!(retained.settle(completed).is_err());
        assert_eq!(retained.active(), 1);
        assert_eq!(retained.active_producers(), 1);
    }
}
