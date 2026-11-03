use hubuum_events_core::{Action, ActorKind, AuditDocument, EntityType, EventId, NewEvent};
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::*;

fn event_identity(operation: Uuid, name: &str) -> EventId {
    let mut digest = Sha256::new();
    digest.update(b"hubuum.history-policy.audit.v1\0");
    digest.update(operation.as_bytes());
    digest.update(name.as_bytes());
    let digest = digest.finalize();
    let mut identity = [0; 16];
    identity.copy_from_slice(&digest[..16]);
    identity[6] = (identity[6] & 0x0f) | 0x80;
    identity[8] = (identity[8] & 0x3f) | 0x80;
    EventId::from(Uuid::from_bytes(identity))
}
struct HistoryAuditRecord {
    operation: Uuid,
    name: &'static str,
    action: Action,
    attribution: HistoryAuditAttribution,
    at: DateTime<Utc>,
    caused_by: Option<EventId>,
    observed_at: Option<DateTime<Utc>>,
}
struct HistoryAuditMembership {
    collection: ResourceIdentity,
    run: Uuid,
    batch: Uuid,
    receipt: Uuid,
    evidence: serde_json::Value,
    versions: usize,
    logical_bytes: Option<u64>,
    reasons: Vec<&'static str>,
}
impl HistoryAuditRecord {
    fn into_event(self, authority: serde_json::Value) -> Result<NewEvent, StorageValidationError> {
        self.into_scoped_event(authority, None)
    }
    fn into_scoped_event(
        self,
        authority: serde_json::Value,
        membership: Option<HistoryAuditMembership>,
    ) -> Result<NewEvent, StorageValidationError> {
        let actor_kind = if self.attribution.actor.is_some() {
            ActorKind::User
        } else if membership.is_some() {
            ActorKind::Worker
        } else {
            ActorKind::System
        };
        let scope = membership.as_ref().map_or_else(
            || json!({"kind":"system"}),
            |evidence| json!({"kind":"collection","collection_uuid":evidence.collection}),
        );
        let document = AuditDocument::builder("History lifecycle authority changed")
            .metadata(json!({"lifecycle": {
                "contract_version": 1, "name": self.name, "projection": "audit",
                "operation_id": self.operation, "operation_revision": "1",
                "scope": scope,
                "attribution": {"actor_principal_uuid": self.attribution.actor, "initiator_principal_uuid": self.attribution.initiator, "trigger": self.attribution.trigger},
                "authority": authority, "state": {"outcome": "committed", "reasons": membership.as_ref().map(|evidence| evidence.reasons.as_slice()).unwrap_or(&[])},
                "counts": {"versions": membership.as_ref().map_or(0, |evidence| evidence.versions), "logical_bytes": membership.as_ref().map(|evidence| evidence.logical_bytes), "physical_bytes": null, "transferred_bytes": null},
                "run_id": membership.as_ref().map(|evidence| evidence.run), "batch_id": membership.as_ref().map(|evidence| evidence.batch), "attempt_id": null,
                "caused_by_event_id": self.caused_by, "observed_at": self.observed_at, "availability": null,
                "copies": {"source_tier": null, "destination_tier": null, "destination_id": null, "artifact_id": null, "source_destination_id": null, "source_artifact_id": null},
                "evidence": {"membership": membership.as_ref().map(|evidence| &evidence.evidence), "verification_id": null, "receipt_id": membership.as_ref().map_or(self.operation, |evidence| evidence.receipt)}, "delivery": null
            }})).try_build().map_err(|error| invalid(&error.to_string()))?;
        NewEvent::from_document(EntityType::Lifecycle, self.action, actor_kind, document)
            .map(|event| {
                event.with_recorded_identity(event_identity(self.operation, self.name), self.at)
            })
            .map_err(|error| invalid(&error.to_string()))
    }
}
impl HistoryLifecycleAuthority {
    /// Canonical facts retain stable identities and original clocks through
    /// recovery. Exact policy/hold/review details live in the referenced control
    /// receipt; the closed audit envelope follows the committed #550 proposal.
    pub fn audit_events_since(
        &self,
        previous: &Self,
    ) -> Result<Vec<NewEvent>, StorageValidationError> {
        if !self.covers(previous) {
            return Err(invalid(
                "History audit predecessor differs from retained authority",
            ));
        }
        let mut events = Vec::new();
        for record in &self.policies().records()[previous.policies().records().len()..] {
            if let Some(review) = record.review() {
                events.push(self.review_event(record.proposal(), review)?);
            }
            let created = record.proposal().expected_policy_revision().value() == 0;
            let policy_id = self
                .policies()
                .records()
                .iter()
                .find(|source| source.proposal().key() == record.proposal().key())
                .expect("current policy has an initial revision")
                .operation();
            events.push(HistoryAuditRecord {
                operation: record.operation(), name: if created { "lifecycle.policy.created" } else { "lifecycle.policy.changed" }, action: if created { Action::Created } else { Action::Updated },
                attribution: HistoryAuditAttribution::manual(record.principal()), at: record.applied_at(),
                caused_by: record.review().map(|review| event_identity(review.reference().preview_id(), "lifecycle.policy.reviewed")), observed_at: None,
            }.into_event(json!({"policy_id": policy_id, "policy_revision": record.revision(), "review_id": record.review().map(|review| review.reference().preview_id()), "hold_generation": null, "deletion_generation": null}))?);
        }
        for entry in &self.hold_revisions()[previous.hold_revisions().len()..] {
            let (name, action, at, attribution, observed_at) = match entry.transition() {
                HistoryHoldTransition::Created => (
                    "lifecycle.hold.created",
                    Action::Created,
                    entry.recorded_at(),
                    HistoryAuditAttribution::manual(
                        entry.actor().expect("validated creation actor"),
                    ),
                    None,
                ),
                HistoryHoldTransition::Released => (
                    "lifecycle.hold.released",
                    Action::Updated,
                    entry.recorded_at(),
                    HistoryAuditAttribution::manual(
                        entry.actor().expect("validated release actor"),
                    ),
                    None,
                ),
                HistoryHoldTransition::Expired => (
                    "lifecycle.hold.expired",
                    Action::Updated,
                    entry.hold().expires_at().expect("validated expiry"),
                    HistoryAuditAttribution {
                        actor: None,
                        initiator: Some(entry.hold().principal()),
                        trigger: "scheduled",
                    },
                    Some(entry.recorded_at()),
                ),
            };
            events.push(HistoryAuditRecord { operation: entry.operation(), name, action, attribution, at, caused_by: None, observed_at }.into_event(json!({
                "policy_id": null, "policy_revision": null, "review_id": null, "hold_generation": entry.revision(), "deletion_generation": null
            }))?);
        }
        events.extend(self.expiry().audit_events_since(previous.expiry())?);
        Ok(events)
    }
}
impl HistoryLifecycleAuthority {
    fn review_event(
        &self,
        proposal: &HistoryPolicyChange,
        review: &HistoryPolicyReviewEvidence,
    ) -> Result<NewEvent, StorageValidationError> {
        let policy_id = (proposal.expected_policy_revision().value() != 0)
            .then(|| {
                self.policies()
                    .records()
                    .iter()
                    .find(|record| record.proposal().key() == proposal.key())
                    .map(HistoryPolicyRecord::operation)
            })
            .flatten();
        let revision = (proposal.expected_policy_revision().value() != 0)
            .then_some(proposal.expected_policy_revision());
        let hold_generation = self
            .hold_revisions()
            .iter()
            .rev()
            .find(|entry| entry.revision() <= review.fence().authority_revision())
            .map(HistoryHoldRevision::revision);
        HistoryAuditRecord { operation: review.reference().preview_id(), name: "lifecycle.policy.reviewed", action: Action::Updated, attribution: HistoryAuditAttribution::manual(review.principal()), at: review.issued_at(), caused_by: None, observed_at: None }.into_event(json!({
            "policy_id": policy_id, "policy_revision": revision, "review_id": review.reference().preview_id(), "hold_generation": hold_generation, "deletion_generation": null
        }))
    }
}
impl HistoryLifecycleControl {
    pub fn audit_events_since(
        &self,
        previous: &Self,
    ) -> Result<Vec<NewEvent>, StorageValidationError> {
        let mut events = self.authority().audit_events_since(previous.authority())?;
        for preview in self.previews().iter().filter(|preview| {
            !previous
                .previews()
                .iter()
                .any(|old| old.reference() == preview.reference())
        }) {
            let event = self
                .authority()
                .review_event(preview.proposal(), &preview.evidence())?;
            if !events
                .iter()
                .any(|existing| existing.event_id() == event.event_id())
            {
                events.push(event);
            }
        }
        Ok(events)
    }
}
struct HistoryAuditAttribution {
    actor: Option<PrincipalIdentity>,
    initiator: Option<PrincipalIdentity>,
    trigger: &'static str,
}
impl HistoryAuditAttribution {
    fn manual(principal: PrincipalIdentity) -> Self {
        Self {
            actor: Some(principal),
            initiator: Some(principal),
            trigger: "manual",
        }
    }
}

impl HistoryExpiryJournal {
    /// Rebuild exact collection-scoped facts without task/principal/resource
    /// rows. Stable receipts retain original clocks even after recovery.
    pub fn audit_events_since(
        &self,
        previous: &Self,
    ) -> Result<Vec<NewEvent>, StorageValidationError> {
        if !self.covers(previous) {
            return Err(invalid(
                "Online expiry audit predecessor lost retained evidence",
            ));
        }
        let mut events = Vec::new();
        for intent in self.intents() {
            let old = previous.intent(intent.identity());
            let mut collections = std::collections::BTreeMap::new();
            for member in intent.manifest().members() {
                for attribution in member.attributions() {
                    collections.insert(
                        *attribution.collection().as_bytes(),
                        attribution.collection(),
                    );
                }
            }
            for collection in collections.into_values() {
                if old.is_none() {
                    for transition in [
                        ExpiryAuditTransition::Requested,
                        ExpiryAuditTransition::Restricted,
                    ] {
                        events.push(expiry_event(intent, collection, transition)?);
                    }
                }
                if let Some(cancellation) = intent
                    .cancellation()
                    .filter(|_| old.is_none_or(|old| old.cancellation().is_none()))
                {
                    events.push(expiry_event(
                        intent,
                        collection,
                        ExpiryAuditTransition::Cancelled(cancellation),
                    )?);
                }
                for receipt in intent
                    .removals()
                    .iter()
                    .filter(|receipt| old.is_none_or(|old| !old.removals().contains(receipt)))
                {
                    if intent.manifest().members().iter().any(|member| {
                        member.target() == receipt.target()
                            && member
                                .attributions()
                                .iter()
                                .any(|attribution| attribution.collection() == collection)
                    }) {
                        events.push(expiry_event(
                            intent,
                            collection,
                            ExpiryAuditTransition::Removed(receipt),
                        )?);
                    }
                }
            }
        }
        Ok(events)
    }
}
enum ExpiryAuditTransition<'a> {
    Requested,
    Restricted,
    Cancelled(&'a HistoryExpiryCancellation),
    Removed(&'a HistoryExpiryRemovalReceipt),
}
fn expiry_event(
    intent: &HistoryExpiryIntent,
    collection: ResourceIdentity,
    transition: ExpiryAuditTransition<'_>,
) -> Result<NewEvent, StorageValidationError> {
    let (name, action, at, actor, removal, caused_by_name) = match transition {
        ExpiryAuditTransition::Requested => (
            "lifecycle.erasure.requested",
            Action::Queued,
            intent.manifest().selected_at(),
            None,
            None,
            None,
        ),
        ExpiryAuditTransition::Restricted => (
            "lifecycle.erasure.access_restricted",
            Action::Updated,
            intent.manifest().selected_at(),
            None,
            None,
            Some("lifecycle.erasure.requested"),
        ),
        ExpiryAuditTransition::Cancelled(cancellation) => (
            "lifecycle.erasure.blocked",
            Action::Updated,
            cancellation.requested_at(),
            Some(cancellation.principal()),
            None,
            Some("lifecycle.erasure.requested"),
        ),
        ExpiryAuditTransition::Removed(receipt) => (
            "lifecycle.erasure.copy_removed",
            Action::Updated,
            receipt.removed_at(),
            None,
            Some(receipt),
            Some("lifecycle.erasure.access_restricted"),
        ),
    };
    let manifest = intent.manifest();
    let mut members = manifest
        .members()
        .iter()
        .filter(|member| {
            member
                .attributions()
                .iter()
                .any(|attribution| attribution.collection() == collection)
        })
        .collect::<Vec<_>>();
    members.sort_by_key(|member| {
        (
            *member.target().resource().as_bytes(),
            *member.target().version().as_bytes(),
        )
    });
    let pairs = members
        .iter()
        .map(|member| json!({"resource_uuid": member.target().resource(), "version_uuid": member.target().version()}))
        .collect::<Vec<_>>();
    let bytes = serde_json::to_vec(&pairs)
        .map_err(|_| invalid("Cannot encode scoped expiry membership"))?;
    let scope_operation = event_identity(
        intent.identity(),
        &format!("operation:{}", Uuid::from_bytes(*collection.as_bytes())),
    )
    .as_uuid();
    let membership_id = event_identity(
        intent.identity(),
        &format!("membership:{}", Uuid::from_bytes(*collection.as_bytes())),
    )
    .as_uuid();
    let operation = removal.map_or(scope_operation, |receipt| {
        event_identity(
            scope_operation,
            &format!(
                "removed:{}:{}",
                Uuid::from_bytes(*receipt.target().resource().as_bytes()),
                Uuid::from_bytes(*receipt.target().version().as_bytes())
            ),
        )
        .as_uuid()
    });
    let (initiator, trigger) = match manifest.origin() {
        HistoryExpiryOrigin::Manual { principal } => (Some(*principal), "manual"),
        HistoryExpiryOrigin::Scheduled { initiator } => (*initiator, "scheduled"),
    };
    let versions = if removal.is_some() { 1 } else { pairs.len() };
    HistoryAuditRecord {
        operation, name, action,
        attribution: HistoryAuditAttribution { actor, initiator, trigger },
        at, caused_by: caused_by_name.map(|name| event_identity(scope_operation, name)), observed_at: None,
    }.into_scoped_event(json!({"policy_id":null,"policy_revision":null,"review_id":null,"hold_generation":null,"deletion_generation":null}), Some(HistoryAuditMembership {
        collection, run: manifest.run(), batch: manifest.identity(), receipt: intent.identity(),
        evidence: json!({"membership_id":membership_id,"format_version":1,"sha256":Sha256::digest(&bytes).iter().map(|byte| format!("{byte:02x}")).collect::<String>(),"count":pairs.len(),"encoded_bytes":bytes.len(),"expires_at":null,"retention":"control_lineage"}),
        versions, logical_bytes: None, reasons: if actor.is_some() { vec!["cancel_requested"] } else { Vec::new() },
    }))
}
