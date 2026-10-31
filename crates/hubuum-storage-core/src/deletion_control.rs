//! Restore-independent deletion authority. These records contain identities,
//! never deleted payloads. Cleanup execution belongs to the owning lifecycle.

use std::collections::BTreeMap;

use async_trait::async_trait;
use hubuum_domain::{ResourceIdentity, VersionIdentity};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{StorageError, StorageIdentityResourceKind, StorageValidationError};

mod application;
pub use application::*;
mod cleanup;
pub use cleanup::*;
#[cfg(test)]
mod cleanup_tests;

/// Initial bounded control format. Reaching a bound blocks admission; records
/// must not be discarded to make space for new deletion decisions.
pub const DELETION_CONTROL_MAX_INTENTS: usize = 1024;
pub const DELETION_CONTROL_MAX_COPIES: usize = 64;
pub const DELETION_CONTROL_MAX_BYTES: usize = 4 * 1024 * 1024;

/// Exact identity selector; integer compatibility IDs are deliberately absent.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DeletionSubject {
    Resource {
        family: StorageIdentityResourceKind,
        identity: ResourceIdentity,
    },
    Version {
        identity: VersionIdentity,
    },
}

/// A physical-copy outcome. A blocked copy is still an outstanding obligation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeletionCopyState {
    Pending,
    Blocked,
    Removed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "IntentInput")]
pub struct DeletionIntent {
    subject: DeletionSubject,
    generation: i64,
    copies: BTreeMap<Uuid, DeletionCopyState>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    bindings: BTreeMap<Uuid, DeletionCopyBinding>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    receipts: BTreeMap<Uuid, DeletionCopyReceipt>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IntentInput {
    subject: DeletionSubject,
    generation: i64,
    copies: BTreeMap<Uuid, DeletionCopyState>,
    #[serde(default)]
    bindings: BTreeMap<Uuid, DeletionCopyBinding>,
    #[serde(default)]
    receipts: BTreeMap<Uuid, DeletionCopyReceipt>,
}

impl TryFrom<IntentInput> for DeletionIntent {
    type Error = StorageValidationError;

    fn try_from(value: IntentInput) -> Result<Self, Self::Error> {
        if value.generation <= 0
            || value.copies.is_empty()
            || value.copies.len() > DELETION_CONTROL_MAX_COPIES
            || value.copies.keys().any(Uuid::is_nil)
        {
            return Err(invalid(
                "Invalid deletion intent generation or copy obligations",
            ));
        }
        for (copy, binding) in &value.bindings {
            if !value.copies.contains_key(copy)
                || value.subject
                    != (DeletionSubject::Version {
                        identity: binding.target().version(),
                    })
            {
                return Err(invalid("Deletion copy binding differs from its intent"));
            }
            if (value.copies[copy] == DeletionCopyState::Removed)
                != value.receipts.contains_key(copy)
            {
                return Err(invalid(
                    "Managed deletion outcome requires its cleanup receipt",
                ));
            }
        }
        if value
            .receipts
            .keys()
            .any(|copy| !value.bindings.contains_key(copy))
        {
            return Err(invalid("Deletion receipt has no managed copy binding"));
        }
        Ok(Self {
            subject: value.subject,
            generation: value.generation,
            copies: value.copies,
            bindings: value.bindings,
            receipts: value.receipts,
        })
    }
}

impl DeletionIntent {
    pub const fn subject(&self) -> &DeletionSubject {
        &self.subject
    }
    pub const fn generation(&self) -> i64 {
        self.generation
    }
    pub fn copies(&self) -> &BTreeMap<Uuid, DeletionCopyState> {
        &self.copies
    }
    pub fn bindings(&self) -> &BTreeMap<Uuid, DeletionCopyBinding> {
        &self.bindings
    }
    pub fn receipts(&self) -> &BTreeMap<Uuid, DeletionCopyReceipt> {
        &self.receipts
    }
    pub fn is_physically_complete(&self) -> bool {
        self.copies
            .values()
            .all(|state| *state == DeletionCopyState::Removed)
    }
}

/// Validated, bounded recovery material. Every revision is a complete snapshot;
/// receipts currently have indefinite retention and cannot be removed.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "ControlInput")]
pub struct DeletionControlState {
    format: u32,
    lineage: Uuid,
    epoch: Uuid,
    revision: i64,
    intents: BTreeMap<Uuid, DeletionIntent>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlInput {
    format: u32,
    lineage: Uuid,
    epoch: Uuid,
    revision: i64,
    intents: BTreeMap<Uuid, DeletionIntent>,
}

impl TryFrom<ControlInput> for DeletionControlState {
    type Error = StorageValidationError;

    fn try_from(value: ControlInput) -> Result<Self, Self::Error> {
        if !matches!(value.format, 1 | 2)
            || value.lineage.is_nil()
            || value.epoch.is_nil()
            || value.revision < 0
            || value.intents.len() > DELETION_CONTROL_MAX_INTENTS
        {
            return Err(invalid("Invalid deletion control header or intent bound"));
        }
        for (id, intent) in &value.intents {
            if id.is_nil()
                || intent.generation > value.revision
                || (value.format == 1
                    && (!intent.bindings.is_empty() || !intent.receipts.is_empty()))
            {
                return Err(invalid(
                    "Invalid deletion intent generation or copy obligations",
                ));
            }
        }
        Ok(Self {
            format: value.format,
            lineage: value.lineage,
            epoch: value.epoch,
            revision: value.revision,
            intents: value.intents,
        })
    }
}

fn invalid(message: &str) -> StorageValidationError {
    StorageValidationError::invalid(message)
}

impl DeletionControlState {
    pub fn empty(lineage: Uuid, epoch: Uuid) -> Result<Self, StorageValidationError> {
        ControlInput {
            format: 1,
            lineage,
            epoch,
            revision: 0,
            intents: BTreeMap::new(),
        }
        .try_into()
    }

    pub const fn lineage(&self) -> Uuid {
        self.lineage
    }
    pub const fn epoch(&self) -> Uuid {
        self.epoch
    }
    pub const fn revision(&self) -> i64 {
        self.revision
    }
    pub fn intents(&self) -> &BTreeMap<Uuid, DeletionIntent> {
        &self.intents
    }

    /// Conservative exposure restriction until individual lifecycle consumers
    /// can reapply every retained decision, including completed deletions.
    pub fn permits_exposure(&self) -> bool {
        self.intents.is_empty()
    }

    /// Restore authenticated recovery material without discarding local work.
    /// The caller must authenticate the latest independent checkpoint and keep
    /// its publication lock through the compare-and-swap and read-back.
    pub fn recover_from(
        &self,
        required: &Self,
    ) -> Result<DeletionControlChange, StorageValidationError> {
        if self.revision == required.revision && self != required {
            return Err(invalid(
                "Conflicting deletion authority at the same revision",
            ));
        }
        let next = if self.covers(required) {
            self
        } else if required.covers(self) {
            required
        } else {
            return Err(invalid(
                "Recovery authority has a different lineage, epoch, or conflicting decisions; keep recovery quarantined",
            ));
        };
        Ok(DeletionControlChange {
            previous: self.clone(),
            next: next.clone(),
        })
    }

    /// Decode untrusted persisted/recovery bytes once at the boundary.
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageValidationError> {
        if bytes.len() > DELETION_CONTROL_MAX_BYTES {
            return Err(invalid("Deletion recovery material exceeds its byte limit"));
        }
        serde_json::from_slice(bytes).map_err(|_| invalid("Invalid deletion recovery material"))
    }

    pub fn encode(&self) -> Result<Vec<u8>, StorageValidationError> {
        let bytes =
            serde_json::to_vec(self).map_err(|_| invalid("Cannot encode deletion control"))?;
        if bytes.len() > DELETION_CONTROL_MAX_BYTES {
            return Err(invalid("Deletion recovery material exceeds its byte limit"));
        }
        Ok(bytes)
    }

    /// Construct an idempotent intent transition. Persist this before attempting
    /// checkpoint publication; failed publication must not discard the intent.
    pub fn record_intent(
        &self,
        id: Uuid,
        subject: DeletionSubject,
        copies: Vec<Uuid>,
    ) -> Result<DeletionControlChange, StorageValidationError> {
        if id.is_nil()
            || copies.is_empty()
            || copies.len() > DELETION_CONTROL_MAX_COPIES
            || copies.iter().any(Uuid::is_nil)
        {
            return Err(invalid(
                "Deletion intent requires nonempty bounded copy identities",
            ));
        }
        let count = copies.len();
        let copies: BTreeMap<_, _> = copies
            .into_iter()
            .map(|id| (id, DeletionCopyState::Pending))
            .collect();
        if copies.len() != count {
            return Err(invalid("Duplicate deletion copy identity"));
        }
        if let Some(existing) = self.intents.get(&id) {
            if existing.subject != subject || !existing.copies.keys().eq(copies.keys()) {
                return Err(invalid(
                    "Deletion intent identity already has different scope",
                ));
            }
            return Ok(DeletionControlChange {
                previous: self.clone(),
                next: self.clone(),
            });
        }
        if self.intents.len() == DELETION_CONTROL_MAX_INTENTS {
            return Err(invalid(
                "Deletion intent capacity exhausted; retained receipts cannot be evicted",
            ));
        }
        let mut next = self.successor()?;
        next.intents.insert(
            id,
            DeletionIntent {
                subject,
                generation: next.revision,
                copies,
                bindings: BTreeMap::new(),
                receipts: BTreeMap::new(),
            },
        );
        Ok(DeletionControlChange {
            previous: self.clone(),
            next,
        })
    }

    /// Record physical evidence only after the caller has independently protected
    /// the intent. This transition cannot remove an obligation or undo removal.
    pub fn record_copy(
        &self,
        intent: Uuid,
        copy: Uuid,
        outcome: DeletionCopyState,
    ) -> Result<DeletionControlChange, StorageValidationError> {
        if self
            .intents
            .get(&intent)
            .is_some_and(|intent| intent.bindings.contains_key(&copy))
        {
            return Err(invalid(
                "Managed copy outcomes require verified adapter cleanup",
            ));
        }
        let old = self
            .intents
            .get(&intent)
            .and_then(|intent| intent.copies.get(&copy))
            .ok_or_else(|| invalid("Unknown deletion copy obligation"))?;
        if *old == outcome {
            return Ok(DeletionControlChange {
                previous: self.clone(),
                next: self.clone(),
            });
        }
        if *old == DeletionCopyState::Removed || outcome == DeletionCopyState::Pending {
            return Err(invalid("Deletion copy outcome cannot move backwards"));
        }
        let mut next = self.successor()?;
        *next
            .intents
            .get_mut(&intent)
            .expect("validated intent")
            .copies
            .get_mut(&copy)
            .expect("validated copy") = outcome;
        Ok(DeletionControlChange {
            previous: self.clone(),
            next,
        })
    }

    /// Adapter-only final transition after all retained decisions have been
    /// reapplied to the current application. Even unchanged Removed outcomes
    /// need a new revision so their reapplication gets a distinct checkpoint.
    pub fn record_application_reapplication(
        &self,
    ) -> Result<DeletionControlChange, StorageValidationError> {
        if self.intents.values().any(|intent| {
            intent.bindings.len() != intent.copies.len() || !intent.is_physically_complete()
        }) {
            return Err(invalid(
                "Unbound or incomplete deletion obligations prevent application reapplication",
            ));
        }
        Ok(DeletionControlChange {
            previous: self.clone(),
            next: self.successor()?,
        })
    }

    fn successor(&self) -> Result<Self, StorageValidationError> {
        let mut next = self.clone();
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or_else(|| invalid("Deletion revision exhausted"))?;
        Ok(next)
    }

    /// Recovery may accept a newer local ledger only when it contains every
    /// acknowledged decision and outcome. A revision number alone is not proof.
    pub fn covers(&self, required: &Self) -> bool {
        self.lineage == required.lineage
            && self.epoch == required.epoch
            && self.revision >= required.revision
            && required.intents.iter().all(|(id, old)| {
                self.intents.get(id).is_some_and(|current| {
                    current.subject == old.subject
                        && current.generation == old.generation
                        && current.copies.keys().eq(old.copies.keys())
                        && current.bindings == old.bindings
                        && old
                            .receipts
                            .iter()
                            .all(|(copy, receipt)| current.receipts.get(copy) == Some(receipt))
                        && old.copies.iter().all(|(copy, state)| match state {
                            DeletionCopyState::Pending => true,
                            DeletionCopyState::Blocked => {
                                current.copies[copy] != DeletionCopyState::Pending
                            }
                            DeletionCopyState::Removed => {
                                current.copies[copy] == DeletionCopyState::Removed
                            }
                        })
                })
            })
    }
}

/// A validated transition with an exact compare-and-swap predecessor.
#[derive(Clone, Debug)]
pub struct DeletionControlChange {
    previous: DeletionControlState,
    next: DeletionControlState,
}

impl DeletionControlChange {
    pub const fn previous(&self) -> &DeletionControlState {
        &self.previous
    }
    pub const fn next(&self) -> &DeletionControlState {
        &self.next
    }
}

/// Adapter-owned recovery coordinates, bound into independent evidence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "RecoveryPositionInput", into = "RecoveryPositionInput")]
pub struct DeletionRecoveryPosition(RecoveryPositionInput);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "backend", rename_all = "snake_case", deny_unknown_fields)]
enum RecoveryPositionInput {
    PostgreSql {
        system_identifier: String,
        wal_file: String,
        lsn: String,
    },
    Memory {
        instance: Uuid,
    },
}

impl TryFrom<RecoveryPositionInput> for DeletionRecoveryPosition {
    type Error = StorageValidationError;

    fn try_from(value: RecoveryPositionInput) -> Result<Self, Self::Error> {
        let valid = match &value {
            RecoveryPositionInput::Memory { instance } => !instance.is_nil(),
            RecoveryPositionInput::PostgreSql {
                system_identifier,
                wal_file,
                lsn,
            } => {
                let hexadecimal = |part: &str| {
                    !part.is_empty()
                        && part
                            .bytes()
                            .all(|b| b.is_ascii_digit() || (b'A'..=b'F').contains(&b))
                };
                system_identifier.len() <= 20
                    && system_identifier.parse::<u64>().is_ok_and(|id| id != 0)
                    && wal_file.len() == 24
                    && hexadecimal(wal_file)
                    && lsn.split_once('/').is_some_and(|(high, low)| {
                        high.len() <= 8 && low.len() <= 8 && hexadecimal(high) && hexadecimal(low)
                    })
            }
        };
        if !valid {
            return Err(invalid("Invalid deletion recovery coordinates"));
        }
        Ok(Self(value))
    }
}

impl From<DeletionRecoveryPosition> for RecoveryPositionInput {
    fn from(value: DeletionRecoveryPosition) -> Self {
        value.0
    }
}

impl DeletionRecoveryPosition {
    pub fn postgresql(
        system_identifier: String,
        wal_file: String,
        lsn: String,
    ) -> Result<Self, StorageValidationError> {
        RecoveryPositionInput::PostgreSql {
            system_identifier,
            wal_file,
            lsn,
        }
        .try_into()
    }

    pub fn memory(instance: Uuid) -> Result<Self, StorageValidationError> {
        RecoveryPositionInput::Memory { instance }.try_into()
    }
}

#[derive(Clone, Debug)]
pub struct DeletionControlCapture {
    state: DeletionControlState,
    position: DeletionRecoveryPosition,
    reapplication: Option<ApplicationReapplication>,
}

impl DeletionControlCapture {
    pub fn new(state: DeletionControlState, position: DeletionRecoveryPosition) -> Self {
        Self {
            state,
            position,
            reapplication: None,
        }
    }
    pub fn with_reapplication(
        mut self,
        receipt: ApplicationReapplication,
    ) -> Result<Self, StorageValidationError> {
        if receipt.revision() != self.state.revision() {
            return Err(invalid(
                "Reapplication does not cover the captured authority revision",
            ));
        }
        self.reapplication = Some(receipt);
        Ok(self)
    }
    pub const fn reapplication(&self) -> Option<&ApplicationReapplication> {
        self.reapplication.as_ref()
    }
    pub const fn state(&self) -> &DeletionControlState {
        &self.state
    }
    pub const fn position(&self) -> &DeletionRecoveryPosition {
        &self.position
    }
}

/// Required control capability. Mutations require privileged control authority;
/// application logical replacement must preserve these records independently.
#[async_trait]
pub trait DeletionControlStorage: Send + Sync {
    /// Read minimal authority without physical backup coordinates. Adapters may
    /// override this to provide a read-only runtime capability while reserving
    /// capture and mutations for the privileged checkpoint operator.
    async fn read_deletion_control(&self) -> Result<DeletionControlState, StorageError> {
        Ok(self.capture_deletion_control().await?.state().clone())
    }
    async fn capture_deletion_control(&self) -> Result<DeletionControlCapture, StorageError>;
    /// Register before opening listeners or starting workers. A crashed or
    /// uncertain runtime remains registered; registrations never expire.
    async fn register_runtime(&self) -> Result<RuntimeRegistration, StorageError> {
        self.register_runtime_isolated(None).await
    }
    async fn register_runtime_isolated(
        &self,
        isolation: Option<RuntimeIsolationDomain>,
    ) -> Result<RuntimeRegistration, StorageError>;
    async fn capture_application_execution(
        &self,
        owner: Option<RuntimeOwner>,
    ) -> Result<StorageExecutionPermit, StorageError>;
    /// Admit a worker batch and observe maintenance using one consistent acquisition.
    /// An ambient application permit must be validated and inherited, never refreshed.
    async fn capture_application_execution_with_maintenance(
        &self,
        owner: Option<RuntimeOwner>,
    ) -> Result<ApplicationWorkAdmission, StorageError>;
    async fn read_application_control(&self) -> Result<ApplicationControlState, StorageError>;
    async fn acknowledge_runtime_drain(
        &self,
        owner: RuntimeOwner,
        fence: ApplicationFence,
    ) -> Result<(), StorageError>;
    /// Root must hold a durable independent checkpoint session covering expected.
    async fn reapply_deletion_decisions(
        &self,
        expected: DeletionControlState,
        fence: ApplicationFence,
        checkpoint: DeletionCheckpointIdentity,
    ) -> Result<ApplicationReapplication, StorageError>;
    /// Root must have independently published and read back this exact receipt.
    async fn release_application_exposure(
        &self,
        receipt: ApplicationReapplication,
        checkpoint: DeletionCheckpointIdentity,
    ) -> Result<(), StorageError>;
    /// Root must hold actual exclusive process isolation for this exact domain.
    /// Unknown or mismatched registered domains block retirement.
    async fn recover_application_control(
        &self,
        change: DeletionControlChange,
        isolation: RuntimeIsolationDomain,
    ) -> Result<ApplicationFence, StorageError>;
    /// Consume ownership only after every listener, request and worker has stopped.
    async fn unregister_runtime(
        &self,
        registration: RuntimeRegistration,
    ) -> Result<(), StorageError>;
    /// Privileged, offline admission. Lock authority, require no registered
    /// runtimes and the exact expected state, then validate the closed copy.
    async fn prepare_hot_object_history_deletion(
        &self,
        expected: DeletionControlState,
        request: HotObjectHistoryDeletion,
    ) -> Result<DeletionControlState, StorageError>;
    /// Privileged caller must retain the independent repository's locked,
    /// durable latest-evidence session. The request validates scope and CAS;
    /// its checkpoint reference alone does not assert independent durability.
    /// Delete and record the adapter-verified outcome in one transaction.
    async fn cleanup_hot_object_history(
        &self,
        request: HotObjectHistoryCleanup,
    ) -> Result<DeletionControlState, StorageError>;
    async fn commit_deletion_control(
        &self,
        change: DeletionControlChange,
    ) -> Result<(), StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn empty() -> DeletionControlState {
        DeletionControlState::empty(Uuid::new_v4(), Uuid::new_v4()).unwrap()
    }

    fn subject() -> DeletionSubject {
        DeletionSubject::Version {
            identity: serde_json::from_value(serde_json::json!(Uuid::new_v4())).unwrap(),
        }
    }

    #[rstest]
    #[case::older_primary(false)]
    #[case::newer_primary(true)]
    fn recovery_preserves_the_complete_newer_authority(#[case] newer_primary: bool) {
        let initial = empty();
        let next = initial
            .record_intent(Uuid::new_v4(), subject(), vec![Uuid::new_v4()])
            .unwrap();
        let (current, required) = if newer_primary {
            (next.next(), &initial)
        } else {
            (&initial, next.next())
        };
        assert_eq!(current.recover_from(required).unwrap().next(), next.next());
    }

    #[rstest]
    #[case::lineage("lineage")]
    #[case::epoch("epoch")]
    fn recovery_rejects_a_different_identity(#[case] field: &str) {
        let initial = empty();
        let mut raw = serde_json::to_value(&initial).unwrap();
        raw[field] = serde_json::json!(Uuid::new_v4());
        let other: DeletionControlState = serde_json::from_value(raw).unwrap();
        assert!(initial.recover_from(&other).is_err());
    }

    #[test]
    fn recovery_rejects_divergent_decisions() {
        let initial = empty();
        let first = initial
            .record_intent(Uuid::new_v4(), subject(), vec![Uuid::new_v4()])
            .unwrap();
        let second = initial
            .record_intent(Uuid::new_v4(), subject(), vec![Uuid::new_v4()])
            .unwrap();
        let later = second
            .next()
            .record_intent(Uuid::new_v4(), subject(), vec![Uuid::new_v4()])
            .unwrap();
        assert!(first.next().recover_from(later.next()).is_err());
    }

    #[rstest]
    #[case::pending(DeletionCopyState::Pending)]
    #[case::blocked(DeletionCopyState::Blocked)]
    #[case::removed(DeletionCopyState::Removed)]
    fn retained_decisions_always_restrict_exposure(#[case] outcome: DeletionCopyState) {
        let id = Uuid::new_v4();
        let copy = Uuid::new_v4();
        let intent = empty().record_intent(id, subject(), vec![copy]).unwrap();
        let state = intent.next().record_copy(id, copy, outcome).unwrap();
        assert!(!state.next().permits_exposure());
    }

    #[test]
    fn intent_retry_preserves_generation() {
        let state = empty();
        let id = Uuid::new_v4();
        let copy = Uuid::new_v4();
        let target = subject();
        let first = state.record_intent(id, target.clone(), vec![copy]).unwrap();
        let retry = first.next().record_intent(id, target, vec![copy]).unwrap();
        assert_eq!(retry.next(), first.next());
    }

    #[test]
    fn reused_intent_cannot_change_target_identity() {
        let id = Uuid::new_v4();
        let copy = Uuid::new_v4();
        let first = empty().record_intent(id, subject(), vec![copy]).unwrap();
        assert!(
            first
                .next()
                .record_intent(id, subject(), vec![copy])
                .is_err()
        );
    }

    #[rstest]
    #[case("format", serde_json::json!(3))]
    #[case("lineage", serde_json::json!(Uuid::nil()))]
    #[case("epoch", serde_json::json!(Uuid::nil()))]
    #[case("revision", serde_json::json!(-1))]
    #[case("extra", serde_json::json!(true))]
    fn malformed_control_is_rejected(#[case] field: &str, #[case] value: serde_json::Value) {
        let mut document = serde_json::to_value(empty()).unwrap();
        document[field] = value;
        assert!(DeletionControlState::decode(&serde_json::to_vec(&document).unwrap()).is_err());
    }

    #[rstest]
    #[case(vec![])]
    #[case(vec![Uuid::nil()])]
    #[case(vec![Uuid::from_u128(1); 2])]
    #[case((1..=65).map(Uuid::from_u128).collect())]
    fn invalid_copy_scope_is_rejected(#[case] copies: Vec<Uuid>) {
        assert!(
            empty()
                .record_intent(Uuid::new_v4(), subject(), copies)
                .is_err()
        );
    }

    #[test]
    fn older_snapshot_cannot_cover_later_decision() {
        let old = empty();
        let new = old
            .record_intent(Uuid::new_v4(), subject(), vec![Uuid::new_v4()])
            .unwrap();
        assert!(!old.covers(new.next()));
    }

    #[rstest]
    #[case("lineage")]
    #[case("epoch")]
    fn unrelated_control_cannot_cover_evidence(#[case] field: &str) {
        let state = empty();
        let mut document = serde_json::to_value(&state).unwrap();
        document[field] = serde_json::json!(Uuid::new_v4());
        let other: DeletionControlState = serde_json::from_value(document).unwrap();
        assert!(!other.covers(&state));
    }

    #[test]
    fn revision_alone_cannot_prove_coverage() {
        let old = empty();
        let intent = old
            .record_intent(Uuid::new_v4(), subject(), vec![Uuid::new_v4()])
            .unwrap();
        let mut document = serde_json::to_value(&old).unwrap();
        document["revision"] = serde_json::json!(100);
        let forged: DeletionControlState = serde_json::from_value(document).unwrap();
        assert!(!forged.covers(intent.next()));
    }

    #[test]
    fn completion_requires_every_copy() {
        let id = Uuid::new_v4();
        let copies = [Uuid::new_v4(), Uuid::new_v4()];
        let intent = empty()
            .record_intent(id, subject(), copies.to_vec())
            .unwrap();
        let partial = intent
            .next()
            .record_copy(id, copies[0], DeletionCopyState::Removed)
            .unwrap();
        assert!(!partial.next().intents()[&id].is_physically_complete());
    }

    #[test]
    fn pending_evidence_cannot_acknowledge_completion() {
        let id = Uuid::new_v4();
        let copy = Uuid::new_v4();
        let intent = empty().record_intent(id, subject(), vec![copy]).unwrap();
        let complete = intent
            .next()
            .record_copy(id, copy, DeletionCopyState::Removed)
            .unwrap();
        assert!(!intent.next().covers(complete.next()));
    }

    #[rstest]
    #[case(DeletionCopyState::Pending)]
    #[case(DeletionCopyState::Blocked)]
    fn removed_copy_cannot_regress(#[case] outcome: DeletionCopyState) {
        let id = Uuid::new_v4();
        let copy = Uuid::new_v4();
        let intent = empty().record_intent(id, subject(), vec![copy]).unwrap();
        let complete = intent
            .next()
            .record_copy(id, copy, DeletionCopyState::Removed)
            .unwrap();
        assert!(complete.next().record_copy(id, copy, outcome).is_err());
    }

    #[test]
    fn recovery_encoding_preserves_exact_authority() {
        let intent = empty()
            .record_intent(Uuid::new_v4(), subject(), vec![Uuid::new_v4()])
            .unwrap();
        assert_eq!(
            &DeletionControlState::decode(&intent.next().encode().unwrap()).unwrap(),
            intent.next()
        );
    }

    #[rstest]
    #[case("", "000000010000000000000001", "0/1000000")]
    #[case("000000000000000000001", "000000010000000000000001", "0/1000000")]
    #[case("1", "../elsewhere", "0/1000000")]
    #[case("1", "000000010000000000000001", "not-an-lsn")]
    fn invalid_physical_coordinates_are_rejected(
        #[case] system: &str,
        #[case] file: &str,
        #[case] lsn: &str,
    ) {
        assert!(
            DeletionRecoveryPosition::postgresql(system.into(), file.into(), lsn.into()).is_err()
        );
    }
}
