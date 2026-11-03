//! Publication ownership is independent of API occupancy and task leases.
//! Owners have no TTL; disappearance of a caller never proves quiescence.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;

use uuid::Uuid;

use super::*;
use crate::{StorageError, StorageExecutionPermit};

/// The complete producer classes supported by the online history boundary.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryPublicationKind {
    HttpResponse,
    TaskOutput,
    Backup,
    Export,
}
impl HistoryPublicationKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HttpResponse => "http_response",
            Self::TaskOutput => "task_output",
            Self::Backup => "backup",
            Self::Export => "export",
        }
    }
}

/// Correlated adapter-validated facts; no raw request can manufacture this epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryReadEpoch {
    application: StorageExecutionPermit,
    history: HistoryReviewFence,
    generation: HistoryPublicationGeneration,
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
        Ok(Self {
            application,
            history,
            generation,
        })
    }
    pub const fn application(&self) -> &StorageExecutionPermit {
        &self.application
    }
    pub const fn history(&self) -> HistoryReviewFence {
        self.history
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
}

impl HistoryReadContext {
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
impl HistoryPublicationOwner {
    /// Adapter boundary after atomic capacity, registered-protocol and epoch checks.
    pub fn generate(kind: HistoryPublicationKind, epoch: HistoryReadEpoch) -> Self {
        Self(HistoryReadContext {
            id: Uuid::new_v4(),
            ownership: Uuid::new_v4(),
            kind,
            epoch,
        })
    }
    pub fn context(&self) -> HistoryReadContext {
        self.0.clone()
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
}
impl Default for HistoryPublicationRegistry {
    fn default() -> Self {
        Self {
            generation: HistoryPublicationGeneration::new(0).expect("zero publication generation"),
            owners: BTreeMap::new(),
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
        self.owners.insert(owner.0.id, owner.context());
        Ok(owner)
    }
    pub fn check(
        &self,
        context: &HistoryReadContext,
        current: &HistoryReadEpoch,
    ) -> Result<(), StorageError> {
        if self.owners.get(&context.id) != Some(context)
            || context.epoch() != current
            || current.generation() != self.generation
        {
            return Err(StorageError::unavailable(
                "Publication context is stale, retired or mismatched",
            ));
        }
        Ok(())
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
    /// Exact-owner-only settlement. No current application/history/API owner is
    /// reacquired: old producers still need to settle after fencing or restore.
    pub fn settle(&mut self, settlement: HistoryPublicationSettlement) -> Result<(), StorageError> {
        let context = settlement.0;
        if self.owners.get(&context.id) != Some(&context) {
            return Err(StorageError::conflict(
                "Publication settlement owner is missing or mismatched",
            ));
        }
        self.owners.remove(&context.id);
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
}
