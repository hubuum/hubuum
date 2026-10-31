use chrono::{DateTime, Utc};
use std::fmt;
use uuid::Variant;

use super::*;
use crate::StorageRecordIdentity;

/// Registrations are retained until their owner proves a clean shutdown.
pub const DELETION_CONTROL_MAX_RUNTIMES: usize = 1024;

/// An ownership capability, deliberately neither cloneable nor serializable.
/// Dropping it does not unregister a potentially still-running process.
pub struct RuntimeRegistration {
    id: Uuid,
    ownership_token: Uuid,
}

impl RuntimeRegistration {
    /// Adapters generate ownership before atomically registering it.
    pub fn generate() -> Self {
        Self {
            id: Uuid::new_v4(),
            ownership_token: Uuid::new_v4(),
        }
    }

    pub fn owner(&self) -> RuntimeOwner {
        RuntimeOwner::new(self.id, self.ownership_token)
    }

    pub const fn id(&self) -> Uuid {
        self.id
    }

    /// Adapter-only persistence/comparison material. Never log or expose it.
    pub const fn ownership_token(&self) -> Uuid {
        self.ownership_token
    }
}

impl fmt::Debug for RuntimeRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeRegistration")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// Content address of authenticated, independently published checkpoint bytes.
/// This validates an identity; construction does not prove publication/durability.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct DeletionCheckpointIdentity(String);

impl DeletionCheckpointIdentity {
    pub fn new(value: String) -> Result<Self, StorageValidationError> {
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(invalid("Invalid deletion checkpoint identity"));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for DeletionCheckpointIdentity {
    type Error = StorageValidationError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<DeletionCheckpointIdentity> for String {
    fn from(value: DeletionCheckpointIdentity) -> Self {
        value.0
    }
}

/// Managed locations with an implemented, identity-fenced cleanup operation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DeletionCopyBinding {
    HotObjectHistory { target: StorageRecordIdentity },
}

impl DeletionCopyBinding {
    pub const fn target(&self) -> StorageRecordIdentity {
        match self {
            Self::HotObjectHistory { target } => *target,
        }
    }
}

/// The intent checkpoint used by the adapter's first committed copy removal.
/// Completion acknowledgement additionally requires a checkpoint of this receipt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeletionCopyReceipt {
    checkpoint: DeletionCheckpointIdentity,
}

impl DeletionCopyReceipt {
    pub const fn checkpoint(&self) -> &DeletionCheckpointIdentity {
        &self.checkpoint
    }
}

/// One exact hot object-history copy. Integer compatibility IDs are absent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HotObjectHistoryDeletion {
    intent: Uuid,
    copy: Uuid,
    target: StorageRecordIdentity,
}

impl HotObjectHistoryDeletion {
    pub fn new(
        intent: Uuid,
        copy: Uuid,
        target: StorageRecordIdentity,
    ) -> Result<Self, StorageValidationError> {
        if [intent, copy].iter().any(|identity| {
            identity.get_variant() != Variant::RFC4122 || identity.get_version().is_none()
        }) {
            return Err(invalid(
                "Deletion intent and copy identities must be non-nil RFC UUIDs",
            ));
        }
        Ok(Self {
            intent,
            copy,
            target,
        })
    }
    pub const fn intent(&self) -> Uuid {
        self.intent
    }
    pub const fn copy(&self) -> Uuid {
        self.copy
    }
    pub const fn target(&self) -> StorageRecordIdentity {
        self.target
    }

    fn binding(&self) -> DeletionCopyBinding {
        DeletionCopyBinding::HotObjectHistory {
            target: self.target,
        }
    }
    pub(super) fn matches(&self, intent: &DeletionIntent) -> bool {
        intent.subject
            == (DeletionSubject::Version {
                identity: self.target.version(),
            })
            && intent.copies.len() == 1
            && intent.bindings.len() == 1
            && intent.bindings.get(&self.copy) == Some(&self.binding())
    }
}

/// Exact control-state/scope fence and checkpoint provenance for an offline
/// cleanup call. Only the service's independent repository session establishes
/// durability; neither a raw checksum nor this constructor proves it.
#[derive(Clone, Debug)]
pub struct HotObjectHistoryCleanup {
    expected: DeletionControlState,
    request: HotObjectHistoryDeletion,
    checkpoint: DeletionCheckpointIdentity,
}

impl HotObjectHistoryCleanup {
    pub fn new(
        expected: DeletionControlState,
        request: HotObjectHistoryDeletion,
        checkpoint: DeletionCheckpointIdentity,
    ) -> Result<Self, StorageValidationError> {
        if !expected
            .intents
            .get(&request.intent)
            .is_some_and(|intent| request.matches(intent))
        {
            return Err(invalid("Cleanup requires an exact managed copy binding"));
        }
        Ok(Self {
            expected,
            request,
            checkpoint,
        })
    }
    pub const fn expected(&self) -> &DeletionControlState {
        &self.expected
    }
    pub const fn request(&self) -> &HotObjectHistoryDeletion {
        &self.request
    }
    pub const fn checkpoint(&self) -> &DeletionCheckpointIdentity {
        &self.checkpoint
    }
}

impl DeletionControlState {
    /// Adapters persist this transition only after verifying the exact closed
    /// history copy and absence of runtime registrations under the authority lock.
    pub fn record_hot_object_history_intent(
        &self,
        request: &HotObjectHistoryDeletion,
    ) -> Result<DeletionControlChange, StorageValidationError> {
        if let Some(existing) = self.intents.get(&request.intent) {
            if !request.matches(existing) {
                return Err(invalid(
                    "Deletion intent already has different or unbound scope",
                ));
            }
            return Ok(DeletionControlChange {
                previous: self.clone(),
                next: self.clone(),
            });
        }
        let mut change = self.record_intent(
            request.intent,
            DeletionSubject::Version {
                identity: request.target.version(),
            },
            vec![request.copy],
        )?;
        change.next.format = 2;
        change
            .next
            .intents
            .get_mut(&request.intent)
            .expect("new intent")
            .bindings
            .insert(request.copy, request.binding());
        Ok(change)
    }

    /// Adapter transition, committed atomically with verified exact-copy removal.
    /// This is not an acknowledgement API. The privileged service must protect
    /// the resulting state independently before acknowledging completion.
    pub fn record_hot_object_history_removed(
        &self,
        cleanup: &HotObjectHistoryCleanup,
        now: DateTime<Utc>,
    ) -> Result<DeletionControlChange, StorageValidationError> {
        if self != cleanup.expected() {
            return Err(invalid(
                "Deletion authority changed; recapture before retrying",
            ));
        }
        let request = cleanup.request();
        let intent = self
            .intents
            .get(&request.intent)
            .ok_or_else(|| invalid("Missing managed deletion intent"))?;
        if !request.matches(intent) {
            return Err(invalid("Managed deletion scope changed"));
        }
        // Even an unchanged receipt requires the adapter to recheck physical
        // absence: recovery may have restored this version in application data.
        if intent.copies[&request.copy] == DeletionCopyState::Removed {
            return Ok(DeletionControlChange {
                previous: self.clone(),
                next: self.clone(),
            });
        }
        if intent.audit.as_ref().is_some_and(|audit| {
            !audit
                .events()
                .iter()
                .any(|event| event.transition() == DeletionAuditTransition::RecoveryProtected)
        }) {
            return Err(invalid(
                "Required audit recovery protection must commit before copy removal",
            ));
        }
        let mut next = self.successor()?;
        let intent = next
            .intents
            .get_mut(&request.intent)
            .expect("validated intent");
        intent
            .copies
            .insert(request.copy, DeletionCopyState::Removed);
        intent.receipts.insert(
            request.copy,
            DeletionCopyReceipt {
                checkpoint: cleanup.checkpoint.clone(),
            },
        );
        if let Some(audit) = &mut intent.audit {
            audit.record(
                DeletionAuditTransition::CopyRemoved,
                next.revision,
                now,
                Some(cleanup.checkpoint.clone()),
            );
        }
        Ok(DeletionControlChange {
            previous: self.clone(),
            next,
        })
    }
}
