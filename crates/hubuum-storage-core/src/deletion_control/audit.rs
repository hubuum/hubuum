use chrono::{DateTime, Utc};
use hubuum_events_core::{Action, ActorKind, AuditDocument, EntityType, EventId, NewEvent};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use uuid::Variant;

use super::*;

/// Minimum evidence retained by the existing protected deletion authority.
/// It contains no resource snapshots, names, provider locations or credentials.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "AuditInput")]
pub struct DeletionAudit {
    collection: ResourceIdentity,
    membership: Uuid,
    events: Vec<DeletionAuditFact>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AuditInput {
    collection: ResourceIdentity,
    membership: Uuid,
    events: Vec<DeletionAuditFact>,
}

impl TryFrom<AuditInput> for DeletionAudit {
    type Error = StorageValidationError;

    fn try_from(value: AuditInput) -> Result<Self, Self::Error> {
        if value.membership.get_variant() != Variant::RFC4122
            || value.membership.get_version().is_none()
            || !(2..=5).contains(&value.events.len())
        {
            return Err(invalid("Invalid protected deletion audit bound"));
        }
        let mut ids = BTreeSet::new();
        let order = [
            DeletionAuditTransition::Requested,
            DeletionAuditTransition::AccessRestricted,
            DeletionAuditTransition::RecoveryProtected,
            DeletionAuditTransition::CopyRemoved,
            DeletionAuditTransition::Completed,
        ];
        for (index, (event, transition)) in value.events.iter().zip(order).enumerate() {
            if event.identity.get_variant() != Variant::RFC4122
                || event.identity.get_version().is_none()
                || event.revision.0 <= 0
                || event.operation_revision.0 != index as i64 + 1
                || !ids.insert(event.identity)
                || event.transition != transition
                || event.checkpoint.is_some()
                    != matches!(
                        transition,
                        DeletionAuditTransition::RecoveryProtected
                            | DeletionAuditTransition::CopyRemoved
                            | DeletionAuditTransition::Completed
                    )
            {
                return Err(invalid("Invalid protected deletion audit fact"));
            }
        }
        if value
            .events
            .windows(2)
            .any(|events| events[0].revision.0 > events[1].revision.0)
        {
            return Err(invalid("Protected audit revisions cannot move backwards"));
        }
        Ok(Self {
            collection: value.collection,
            membership: value.membership,
            events: value.events,
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeletionAuditTransition {
    Requested,
    AccessRestricted,
    RecoveryProtected,
    CopyRemoved,
    Completed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeletionAuditProtection {
    Intent,
    Completion,
}

/// Proof that the transition changes audit evidence only. Deletion decisions,
/// copy outcomes and other protected authority are inherited unchanged.
#[derive(Clone, Debug)]
pub struct DeletionAuditChange {
    change: DeletionControlChange,
    protection: DeletionAuditProtection,
}

impl DeletionAuditChange {
    pub const fn previous(&self) -> &DeletionControlState {
        self.change.previous()
    }
    pub const fn next(&self) -> &DeletionControlState {
        self.change.next()
    }
    pub const fn protection(&self) -> DeletionAuditProtection {
        self.protection
    }
}

/// A repository observation admitted by the trusted service while it retains
/// independent publication exclusion. The checkpoint identifier is not itself
/// a durability capability; only the service's owned session supplies that.
#[derive(Clone, Debug)]
pub struct DeletionAuditObservation {
    expected: DeletionControlState,
    checkpoint: DeletionCheckpointIdentity,
    protection: DeletionAuditProtection,
}

impl DeletionAuditObservation {
    pub fn new(
        expected: DeletionControlState,
        checkpoint: DeletionCheckpointIdentity,
        protection: DeletionAuditProtection,
    ) -> Self {
        Self {
            expected,
            checkpoint,
            protection,
        }
    }

    pub const fn expected(&self) -> &DeletionControlState {
        &self.expected
    }
    pub const fn checkpoint(&self) -> &DeletionCheckpointIdentity {
        &self.checkpoint
    }
    pub const fn protection(&self) -> DeletionAuditProtection {
        self.protection
    }
}

impl DeletionAuditTransition {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Requested => "lifecycle.erasure.requested",
            Self::AccessRestricted => "lifecycle.erasure.access_restricted",
            Self::RecoveryProtected => "lifecycle.erasure.recovery_protected",
            Self::CopyRemoved => "lifecycle.erasure.copy_removed",
            Self::Completed => "lifecycle.erasure.completed",
        }
    }

    const fn action(self) -> Action {
        match self {
            Self::Requested => Action::Queued,
            Self::Completed => Action::Succeeded,
            _ => Action::Updated,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeletionAuditFact {
    #[serde(rename = "event_id")]
    identity: Uuid,
    transition: DeletionAuditTransition,
    occurred_at: DateTime<Utc>,
    #[serde(rename = "control_revision")]
    revision: AuditRevision,
    operation_revision: AuditRevision,
    checkpoint: Option<DeletionCheckpointIdentity>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "String", into = "String")]
struct AuditRevision(i64);

impl TryFrom<String> for AuditRevision {
    type Error = StorageValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let revision = value
            .parse::<i64>()
            .map_err(|_| invalid("Invalid protected audit revision"))?;
        if revision <= 0 || revision.to_string() != value {
            return Err(invalid(
                "Protected audit revision must be a positive canonical decimal string",
            ));
        }
        Ok(Self(revision))
    }
}

impl From<AuditRevision> for String {
    fn from(revision: AuditRevision) -> Self {
        revision.0.to_string()
    }
}

impl DeletionAuditFact {
    pub const fn identity(&self) -> Uuid {
        self.identity
    }

    pub const fn transition(&self) -> DeletionAuditTransition {
        self.transition
    }

    pub const fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }

    pub const fn revision(&self) -> i64 {
        self.revision.0
    }
}

impl DeletionAudit {
    pub fn events(&self) -> &[DeletionAuditFact] {
        &self.events
    }

    pub const fn collection(&self) -> ResourceIdentity {
        self.collection
    }

    pub const fn membership(&self) -> Uuid {
        self.membership
    }

    pub(super) fn covers(&self, required: &Self) -> bool {
        self.collection == required.collection
            && self.membership == required.membership
            && self.events.starts_with(&required.events)
    }

    pub(super) fn record(
        &mut self,
        transition: DeletionAuditTransition,
        revision: i64,
        occurred_at: DateTime<Utc>,
        checkpoint: Option<DeletionCheckpointIdentity>,
    ) {
        if !self
            .events
            .iter()
            .any(|event| event.transition == transition)
        {
            self.events.push(DeletionAuditFact {
                identity: Uuid::new_v4(),
                transition,
                occurred_at,
                revision: AuditRevision(revision),
                operation_revision: AuditRevision(self.events.len() as i64 + 1),
                checkpoint,
            });
        }
    }

    /// An offline administrator operation remains system-only. Its membership
    /// retains the actual historical collection UUID without granting visibility
    /// through a subsequently reused collection integer.
    pub(super) fn event(
        &self,
        operation: Uuid,
        intent: &DeletionIntent,
        fact: &DeletionAuditFact,
    ) -> Result<NewEvent, StorageValidationError> {
        let binding = intent
            .bindings
            .values()
            .next()
            .ok_or_else(|| invalid("Missing audited copy"))?;
        let target = binding.target();
        let membership_bytes = format!(
            "hubuum-lifecycle-membership-v1\n{}\n{}\n{}\n{}\t{}\n",
            self.membership,
            self.collection,
            operation,
            target.resource(),
            target.version()
        );
        let document = AuditDocument::builder("Lifecycle erasure state changed")
            .metadata(json!({"lifecycle": {
                "contract_version": 1,
                "name": fact.transition.name(),
                "projection": "audit",
                "operation_id": operation,
                "operation_revision": fact.operation_revision.0.to_string(),
                "scope": {"kind": "system"},
                "state": {"outcome": "committed", "reasons": []},
                "counts": {"versions": 1, "logical_bytes": null, "physical_bytes": null, "transferred_bytes": null},
                "availability": null,
                "run_id": null, "batch_id": null, "attempt_id": null, "caused_by_event_id": null,
                "observed_at": null,
                "attribution": {"actor_principal_uuid": null, "initiator_principal_uuid": null, "trigger": "manual"},
                "authority": {"policy_id": null, "policy_revision": null, "review_id": null, "hold_generation": null, "deletion_generation": intent.generation.to_string()},
                "copies": {"source_tier": "hot", "destination_tier": null, "destination_id": null, "artifact_id": null, "source_destination_id": null, "source_artifact_id": null},
                "evidence": {"membership": {"membership_id": self.membership, "format_version": 1,
                    "sha256": Sha256::digest(membership_bytes.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect::<String>(), "count": 1,
                    "encoded_bytes": membership_bytes.len(), "expires_at": null, "retention": "control_lineage"},
                    "verification_id": null, "receipt_id": operation},
                "delivery": null
            }}))
            .try_build()
            .map_err(|error| invalid(&error.to_string()))?;
        NewEvent::from_document(
            EntityType::Lifecycle,
            fact.transition.action(),
            ActorKind::System,
            document,
        )
        .map(|event| event.with_recorded_identity(EventId::from(fact.identity), fact.occurred_at))
        .map_err(|error| invalid(&error.to_string()))
    }
}

impl DeletionControlChange {
    /// Adapters resolve this historical reference under the same lock/snapshot
    /// as the exact closed-copy check; callers cannot supply a current parent.
    pub fn with_hot_history_audit(
        mut self,
        request: &HotObjectHistoryDeletion,
        collection: ResourceIdentity,
        now: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        let intent = self
            .next
            .intents
            .get_mut(&request.intent())
            .ok_or_else(|| invalid("Missing deletion intent"))?;
        if !request.matches(intent) {
            return Err(invalid("Audit membership differs from deletion scope"));
        }
        if intent.audit.is_none() && !self.previous.intents.contains_key(&request.intent()) {
            let mut audit = DeletionAudit {
                collection,
                membership: Uuid::new_v4(),
                events: Vec::new(),
            };
            audit.record(
                DeletionAuditTransition::Requested,
                self.next.revision,
                now,
                None,
            );
            audit.record(
                DeletionAuditTransition::AccessRestricted,
                self.next.revision,
                now,
                None,
            );
            intent.audit = Some(audit);
            self.next.format = 3;
        }
        Ok(self)
    }
}

impl DeletionControlState {
    pub fn has_required_audit_completion(&self) -> bool {
        self.intents.values().all(|intent| {
            intent.audit.as_ref().is_none_or(|audit| {
                audit
                    .events
                    .iter()
                    .any(|event| event.transition == DeletionAuditTransition::Completed)
            })
        })
    }
    pub fn new_audit_events_since(
        &self,
        previous: &Self,
    ) -> Result<Vec<NewEvent>, StorageValidationError> {
        let known: BTreeSet<_> = previous
            .intents
            .values()
            .filter_map(|intent| intent.audit.as_ref())
            .flat_map(|audit| audit.events.iter().map(|event| event.identity))
            .collect();
        let mut events = Vec::new();
        for (operation, intent) in &self.intents {
            if let Some(audit) = &intent.audit {
                for fact in &audit.events {
                    if !known.contains(&fact.identity) {
                        events.push(audit.event(*operation, intent, fact)?);
                    }
                }
            }
        }
        Ok(events)
    }

    /// The trusted repository service supplies this observation while retaining
    /// its authenticated publication session. A checksum alone is not proof.
    pub fn record_audit_protection(
        &self,
        checkpoint: DeletionCheckpointIdentity,
        now: DateTime<Utc>,
        protection: DeletionAuditProtection,
    ) -> Result<DeletionAuditChange, StorageValidationError> {
        let completed = protection == DeletionAuditProtection::Completion;
        let transition = if completed {
            DeletionAuditTransition::Completed
        } else {
            DeletionAuditTransition::RecoveryProtected
        };
        let mut next = self.clone();
        let revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| invalid("Deletion revision exhausted"))?;
        for intent in next.intents.values_mut() {
            if (!completed || intent.is_physically_complete())
                && let Some(audit) = &mut intent.audit
            {
                if completed
                    && !audit
                        .events
                        .iter()
                        .any(|event| event.transition == DeletionAuditTransition::CopyRemoved)
                {
                    return Err(invalid(
                        "Completion requires recorded copy removal and recovery protection",
                    ));
                }
                audit.record(transition, revision, now, Some(checkpoint.clone()));
            }
        }
        if next != *self {
            next.revision = revision;
        }
        Ok(DeletionAuditChange {
            change: DeletionControlChange {
                previous: self.clone(),
                next,
            },
            protection,
        })
    }
}
