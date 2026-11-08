use std::io::{self, Write};

use async_trait::async_trait;
use hubuum_domain::PrincipalIdentity;
use uuid::Uuid;

use super::*;
use crate::{
    ApplicationFence, DeletionCheckpointIdentity, HistoryArchiveExecutionOwner,
    RuntimeIsolationDomain, RuntimeRegistration, StorageError, StorageIdentityEvidence,
};

#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryPolicyApplyRequest {
    proposal: HistoryPolicyChange,
    #[serde(
        default,
        deserialize_with = "deserialize_review",
        skip_serializing_if = "Option::is_none"
    )]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    review: Option<HistoryPolicyReviewReference>,
}
impl HistoryPolicyApplyRequest {
    pub const fn new(
        proposal: HistoryPolicyChange,
        review: Option<HistoryPolicyReviewReference>,
    ) -> Self {
        Self { proposal, review }
    }
    pub const fn proposal(&self) -> &HistoryPolicyChange {
        &self.proposal
    }
}

/// Preview issuance has a separate bounded capacity and never advances policy
/// decision revisions or reserves execution work.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "ControlInput")]
pub struct HistoryLifecycleControl {
    authority: HistoryLifecycleAuthority,
    previews: Vec<HistoryPolicyPreview>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlInput {
    authority: HistoryLifecycleAuthority,
    previews: Vec<HistoryPolicyPreview>,
}
impl HistoryLifecycleControl {
    pub const MAX_PREVIEWS: usize = 256;
    pub const MAX_BYTES: usize = 4 * 1024 * 1024;
    pub const HOLD_COMPLETION_RESERVE_BYTES: usize = 1024;
    pub fn empty() -> Self {
        Self {
            authority: HistoryLifecycleAuthority::empty(),
            previews: Vec::new(),
        }
    }
    pub const fn authority(&self) -> &HistoryLifecycleAuthority {
        &self.authority
    }
    pub fn previews(&self) -> &[HistoryPolicyPreview] {
        &self.previews
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageValidationError> {
        if bytes.len() > Self::MAX_BYTES {
            return Err(invalid("History lifecycle control exceeds its byte limit"));
        }
        serde_json::from_slice(bytes).map_err(|_| invalid("Invalid history lifecycle control"))
    }
    pub fn encode(&self) -> Result<Vec<u8>, StorageValidationError> {
        let bytes = serde_json::to_vec(self)
            .map_err(|_| invalid("Cannot encode history lifecycle control"))?;
        let completion_reserve = self.authority.pending_hold_completions()
            * Self::HOLD_COMPLETION_RESERVE_BYTES
            + self.authority.expiry().completion_reserve_bytes();
        if bytes.len() + completion_reserve > Self::MAX_BYTES {
            return Err(invalid(
                "History control capacity must preserve reserved hold completion space",
            ));
        }
        Ok(bytes)
    }
    pub fn admit_expiry(
        &self,
        manifest: HistoryExpiryManifest,
        fence: HistoryReviewFence,
    ) -> Result<HistoryLifecycleControlChange, StorageValidationError> {
        if self
            .previews
            .iter()
            .any(|preview| preview.reference().preview_id() == manifest.identity())
        {
            return Err(invalid(
                "Expiry intent identity collides with issued review evidence",
            ));
        }
        if manifest.fence() != fence {
            return Err(invalid("Expiry selection fence changed"));
        }
        let mut next = self.clone();
        next.authority = self.authority.admit_expiry(manifest)?;
        self.change(next, fence)
    }
    pub fn cancel_expiry(
        &self,
        identity: Uuid,
        principal: PrincipalIdentity,
        fence: HistoryReviewFence,
        at: DateTime<Utc>,
    ) -> Result<HistoryLifecycleControlChange, StorageValidationError> {
        let mut next = self.clone();
        next.authority = self.authority.cancel_expiry(identity, principal, at)?;
        self.change(next, fence)
    }
    /// Retain an exact native deletion outcome in the same transaction as the
    /// deletion. The adapter must already have checked independent protection,
    /// publication retirement and current holds/dependencies; this transition
    /// neither authorizes removal nor makes generic policy CAS an executor.
    /// A repeated physical removal preserves the first receipt and audit clock.
    pub fn record_expiry_removal(
        &self,
        identity: Uuid,
        deletion: HistoryExpiryExactDeletion,
        fence: HistoryReviewFence,
    ) -> Result<HistoryExpiryRemovalChange, StorageValidationError> {
        let mut next = self.clone();
        next.authority = self.authority.record_expiry_removal(identity, deletion)?;
        self.change(next, fence).map(HistoryExpiryRemovalChange)
    }
    pub fn issue_preview(
        &self,
        preview: HistoryPolicyPreview,
        fence: HistoryReviewFence,
    ) -> Result<HistoryLifecycleControlChange, StorageValidationError> {
        if preview.fence() != fence
            || fence.authority_revision() != self.authority.revision()
            || preview.proposal().expected_policy_revision()
                != self
                    .authority
                    .policies()
                    .current_revision(preview.proposal().key())
        {
            return Err(invalid("Policy authority changed before preview issuance"));
        }
        let mut next = self.clone();
        next.previews
            .retain(|old| old.expires_at() > preview.issued_at());
        if next.previews.len() == Self::MAX_PREVIEWS {
            return Err(invalid("Policy preview capacity exhausted"));
        }
        if next
            .previews
            .iter()
            .any(|old| old.reference().preview_id() == preview.reference().preview_id())
        {
            return Err(invalid("Preview identity is already issued"));
        }
        next.previews.push(preview);
        self.change(next, fence)
    }
    pub fn apply(
        &self,
        request: HistoryPolicyApplyRequest,
        principal: PrincipalIdentity,
        fence: HistoryReviewFence,
        now: DateTime<Utc>,
    ) -> Result<HistoryLifecycleControlChange, HistoryPolicyApplyError> {
        if self.authority.revision() != fence.authority_revision() {
            return Err(HistoryPolicyApplyError::Conflict(
                HistoryPolicyReviewConflict::StalePreview,
            ));
        }
        if request.proposal.expected_policy_revision()
            != self
                .authority
                .policies()
                .current_revision(request.proposal.key())
        {
            return Err(HistoryPolicyApplyError::Conflict(
                HistoryPolicyReviewConflict::StalePreview,
            ));
        }
        match &request.review {
            Some(reference) => {
                let issued = self
                    .previews
                    .iter()
                    .find(|preview| preview.reference().preview_id() == reference.preview_id())
                    .ok_or(HistoryPolicyApplyError::Conflict(
                        HistoryPolicyReviewConflict::InaccessiblePreview,
                    ))?;
                issued
                    .validate_apply(principal, &request.proposal, reference, fence, now)
                    .map_err(HistoryPolicyApplyError::Conflict)?;
                if self
                    .authority
                    .current_holds()
                    .any(|hold| hold.is_active(issued.issued_at()) && !hold.is_active(now))
                {
                    return Err(HistoryPolicyApplyError::Conflict(
                        HistoryPolicyReviewConflict::StalePreview,
                    ));
                }
            }
            None if matches!(
                request.proposal.application(),
                HistoryPolicyApplication::Retrospective { .. }
            ) =>
            {
                return Err(HistoryPolicyApplyError::Invalid(invalid(
                    "Retrospective application requires an issued review",
                )));
            }
            None => {}
        }
        let review = request
            .review
            .as_ref()
            .and_then(|reference| {
                self.previews
                    .iter()
                    .find(|preview| preview.reference() == reference)
            })
            .map(HistoryPolicyPreview::evidence);
        let mut next = self.clone();
        next.authority = self
            .authority
            .apply_reviewed(request.proposal, principal, now, review)
            .map_err(HistoryPolicyApplyError::Invalid)?;
        self.change(next, fence)
            .map_err(HistoryPolicyApplyError::Invalid)
    }
    pub fn create_hold(
        &self,
        hold: HistoryRetentionHold,
        fence: HistoryReviewFence,
    ) -> Result<HistoryLifecycleControlChange, StorageValidationError> {
        let mut next = self.clone();
        next.authority = self.authority.create_hold(hold)?;
        self.change(next, fence)
    }
    pub fn settle_expired_hold(
        &self,
        identity: Uuid,
        fence: HistoryReviewFence,
        now: DateTime<Utc>,
    ) -> Result<HistoryLifecycleControlChange, StorageValidationError> {
        let mut next = self.clone();
        next.authority = self.authority.settle_expired_hold(identity, now)?;
        self.change(next, fence)
    }
    pub fn release_hold(
        &self,
        identity: Uuid,
        actor: PrincipalIdentity,
        fence: HistoryReviewFence,
        now: DateTime<Utc>,
    ) -> Result<HistoryLifecycleControlChange, StorageValidationError> {
        let mut next = self.clone();
        next.authority = self.authority.release_hold(identity, actor, now)?;
        self.change(next, fence)
    }
    fn change(
        &self,
        next: Self,
        fence: HistoryReviewFence,
    ) -> Result<HistoryLifecycleControlChange, StorageValidationError> {
        if self.authority.revision() != fence.authority_revision() {
            return Err(invalid("History authority changed"));
        }
        next.encode()?;
        Ok(HistoryLifecycleControlChange {
            expected: self.clone(),
            next,
            fence,
        })
    }
}
impl Default for HistoryLifecycleControl {
    fn default() -> Self {
        Self::empty()
    }
}
impl TryFrom<ControlInput> for HistoryLifecycleControl {
    type Error = StorageValidationError;
    fn try_from(value: ControlInput) -> Result<Self, Self::Error> {
        if value.previews.len() > Self::MAX_PREVIEWS {
            return Err(invalid("Policy preview capacity exceeded"));
        }
        let mut ids = std::collections::BTreeSet::new();
        if value
            .previews
            .iter()
            .any(|preview| !ids.insert(preview.reference().preview_id()))
        {
            return Err(invalid("Duplicate issued preview identity"));
        }
        let control = Self {
            authority: value.authority,
            previews: value.previews,
        };
        control.encode()?;
        Ok(control)
    }
}
#[derive(Debug)]
pub enum HistoryPolicyApplyError {
    Invalid(StorageValidationError),
    Conflict(HistoryPolicyReviewConflict),
}

/// This transition cannot be deserialized from public requests. Adapters commit
/// it only against the exact predecessor and current application/deletion fence.
#[derive(Clone, Debug)]
pub struct HistoryLifecycleControlChange {
    expected: HistoryLifecycleControl,
    next: HistoryLifecycleControl,
    fence: HistoryReviewFence,
}
impl HistoryLifecycleControlChange {
    pub const fn expected(&self) -> &HistoryLifecycleControl {
        &self.expected
    }
    pub const fn next(&self) -> &HistoryLifecycleControl {
        &self.next
    }
    pub const fn fence(&self) -> HistoryReviewFence {
        self.fence
    }
}

/// Adapter-local outcome of an exact DELETE. It deliberately cannot be passed
/// to `commit_history_lifecycle`: policy/hold CAS cannot record physical removal.
/// The native deletion transaction persists this outcome and its audit facts
/// atomically; the trusted service subsequently protects that completion.
///
/// ```compile_fail
/// use hubuum_storage_core::{HistoryExpiryRemovalChange, HistoryLifecycleStorage};
///
/// async fn commit_through_policy_api(
///     storage: &impl HistoryLifecycleStorage,
///     removal: HistoryExpiryRemovalChange,
/// ) {
///     storage.commit_history_lifecycle(removal).await.unwrap();
/// }
/// ```
#[derive(Debug)]
pub struct HistoryExpiryRemovalChange(HistoryLifecycleControlChange);

impl HistoryExpiryRemovalChange {
    pub const fn expected(&self) -> &HistoryLifecycleControl {
        self.0.expected()
    }
    pub const fn next(&self) -> &HistoryLifecycleControl {
        self.0.next()
    }
    pub const fn fence(&self) -> HistoryReviewFence {
        self.0.fence()
    }
}

#[derive(Clone, Debug)]
pub struct HistoryLifecycleSnapshot {
    observed_at: DateTime<Utc>,
    control: HistoryLifecycleControl,
    fence: HistoryReviewFence,
}
impl HistoryLifecycleSnapshot {
    pub fn new(
        control: HistoryLifecycleControl,
        incarnation: Uuid,
        deletion_revision: HistoryDeletionRevision,
        observed_at: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        let fence = HistoryReviewFence::new(
            incarnation,
            control.authority().revision(),
            deletion_revision,
        )?;
        Ok(Self {
            control,
            fence,
            observed_at,
        })
    }
    pub const fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
    }
    pub const fn control(&self) -> &HistoryLifecycleControl {
        &self.control
    }
    pub const fn fence(&self) -> HistoryReviewFence {
        self.fence
    }
}

/// Raw commit is not independent protection or permission to start deletion.
/// The trusted service checkpoints authoritative changes before acknowledging
/// application; previews alone require no recovery checkpoint.
#[async_trait]
pub trait HistoryLifecycleStorage: Send + Sync {
    /// Atomic parent/child archive admission. No usable permit escapes before
    /// commit; rollback issues no work and preserves capacity exactly.
    async fn admit_history_archive_execution(
        &self,
    ) -> Result<HistoryArchiveExecutionOwner, StorageError>;
    /// Admit only from the original registered application scope. Nested
    /// producers inherit their context and may not refresh an older epoch.
    async fn admit_history_publication(
        &self,
        kind: HistoryPublicationKind,
    ) -> Result<HistoryPublicationOwner, StorageError>;
    /// Exact-owner settlement after positive producer/driver completion, outside
    /// productive authority transactions and without reacquiring API ownership.
    async fn settle_history_publication(
        &self,
        settlement: HistoryPublicationSettlement,
    ) -> Result<(), StorageError>;
    /// Reserve the parent's single producer slot before issuing external work.
    /// The caller must carry the original productive scope and read context.
    async fn admit_history_producer(
        &self,
        context: HistoryReadContext,
    ) -> Result<HistoryProducerOwner, StorageError>;
    /// Exact, idempotent physical-completion acknowledgement, exempt from
    /// productive authority/API locks. Unknown work remains charged.
    async fn settle_history_producer(
        &self,
        settlement: &HistoryProducerSettlement,
    ) -> Result<(), StorageError>;
    /// Declare complete history read/publication fencing before starting any
    /// listeners or producers. Caller still owns positive shutdown/isolation;
    /// this capability is not a heartbeat or permission to activate expiry.
    async fn register_history_runtime(
        &self,
        isolation: Option<RuntimeIsolationDomain>,
    ) -> Result<RuntimeRegistration, StorageError>;
    async fn scan_history_lifecycle(
        &self,
        scan: HistoryLifecycleScan,
    ) -> Result<HistoryLifecycleScanResult, StorageError>;
    /// Select and atomically retain exact native membership and audit facts.
    /// Requires original OnlineV1 application admission and an entirely aware
    /// fleet. None means no batch was selected within this bounded scan, never
    /// proof of an empty source. The service must independently checkpoint the
    /// result before any removal; this operation never removes payloads.
    async fn admit_history_expiry(
        &self,
        request: HistoryExpiryAdmissionRequest,
    ) -> Result<Option<HistoryExpiryManifest>, StorageError>;
    async fn recover_history_lifecycle(
        &self,
        recovery: HistoryLifecycleRecovery,
    ) -> Result<(), StorageError>;
    async fn capture_history_lifecycle(&self) -> Result<HistoryLifecycleSnapshot, StorageError>;
    async fn commit_history_lifecycle(
        &self,
        change: HistoryLifecycleControlChange,
    ) -> Result<HistoryLifecycleSnapshot, StorageError>;
}

/// Correlation carried from the trusted service's authenticated independent
/// checkpoint read-back into the final native exposure CAS. Construction does
/// not authenticate a primary capture or replace repository verification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryExposureProtection {
    authority: HistoryLifecycleAuthority,
    fence: ApplicationFence,
    checkpoint: DeletionCheckpointIdentity,
}
impl HistoryExposureProtection {
    pub fn new(
        authority: HistoryLifecycleAuthority,
        fence: ApplicationFence,
        checkpoint: DeletionCheckpointIdentity,
    ) -> Self {
        Self {
            authority,
            fence,
            checkpoint,
        }
    }
    pub const fn authority(&self) -> &HistoryLifecycleAuthority {
        &self.authority
    }
    pub const fn fence(&self) -> ApplicationFence {
        self.fence
    }
    pub const fn checkpoint(&self) -> &DeletionCheckpointIdentity {
        &self.checkpoint
    }
    pub fn matches(
        &self,
        authority: &HistoryLifecycleAuthority,
        fence: ApplicationFence,
        checkpoint: &DeletionCheckpointIdentity,
    ) -> bool {
        &self.authority == authority && self.fence == fence && &self.checkpoint == checkpoint
    }
}

/// A bounded exposure check must fail closed on incomplete coverage. These
/// limits bound metadata observation; they are not scale/liveness qualification
/// and do not authorize removal or turn a partial scan into a recovery proof.
#[derive(Default)]
pub struct HistoryRecoveryExposureBudget {
    rows: u32,
    bytes: usize,
}
impl HistoryRecoveryExposureBudget {
    pub const MAX_ROWS: u32 = 1000;
    pub const MAX_BYTES: usize = 1024 * 1024;
    pub const MAX_STATEMENT_MILLISECONDS: u32 = 5000;

    pub fn remaining_rows(&self) -> u32 {
        Self::MAX_ROWS - self.rows
    }
    pub fn remaining_bytes(&self) -> usize {
        Self::MAX_BYTES - self.bytes
    }
    pub fn observe(&mut self, bytes: usize) -> Result<(), StorageValidationError> {
        if self.rows == Self::MAX_ROWS || bytes > self.remaining_bytes() {
            return Err(invalid(
                "History recovery exposure coverage exceeds its bounded check; keep quarantine",
            ));
        }
        self.rows += 1;
        self.bytes += bytes;
        Ok(())
    }
    pub fn observe_evidence(
        &mut self,
        evidence: &StorageIdentityEvidence,
    ) -> Result<(), StorageValidationError> {
        let bytes = self.encoded_bytes(evidence)?;
        self.observe(bytes)
    }
    pub fn observe_json(
        &mut self,
        evidence: &serde_json::Value,
    ) -> Result<(), StorageValidationError> {
        let bytes = self.encoded_bytes(evidence)?;
        self.observe(bytes)
    }
    fn encoded_bytes<T: Serialize>(&self, evidence: &T) -> Result<usize, StorageValidationError> {
        if self.remaining_rows() == 0 {
            return Err(invalid(
                "History recovery exposure row bound exhausted; keep quarantine",
            ));
        }
        struct Counter {
            bytes: usize,
            remaining: usize,
        }
        impl Write for Counter {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if bytes.len() > self.remaining {
                    return Err(io::Error::other(
                        "Exposure identity exceeds remaining bytes",
                    ));
                }
                self.bytes += bytes.len();
                self.remaining -= bytes.len();
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let mut counter = Counter {
            bytes: 0,
            remaining: self.remaining_bytes(),
        };
        serde_json::to_writer(&mut counter, evidence).map_err(|_| {
            invalid("History recovery exposure identity exceeds its bounded check; keep quarantine")
        })?;
        Ok(counter.bytes)
    }
}

/// Recovery is privileged and quarantine-only. It may add authenticated retained
/// decisions but can never drop a local policy revision or hold transition.
#[derive(Clone, Debug)]
pub struct HistoryLifecycleRecovery {
    expected: HistoryLifecycleControl,
    recovered: HistoryLifecycleControl,
    fence: HistoryReviewFence,
}
impl HistoryLifecycleRecovery {
    pub fn new(
        snapshot: HistoryLifecycleSnapshot,
        required: HistoryLifecycleAuthority,
    ) -> Result<Self, StorageValidationError> {
        let current = snapshot.control.authority();
        let authority = current.merge_retained(&required)?;
        let recovered = HistoryLifecycleControl {
            authority,
            previews: Vec::new(),
        };
        recovered.encode()?;
        Ok(Self {
            expected: snapshot.control,
            recovered,
            fence: snapshot.fence,
        })
    }
    pub const fn expected(&self) -> &HistoryLifecycleControl {
        &self.expected
    }
    pub const fn recovered(&self) -> &HistoryLifecycleControl {
        &self.recovered
    }
    pub const fn fence(&self) -> HistoryReviewFence {
        self.fence
    }
}

impl HistoryLifecycleControlChange {
    /// New authority and review scopes requiring existence validation inside the
    /// adapter transaction. Existing bindings remain governable after deletion.
    pub fn scopes_to_validate(&self) -> Vec<HistoryPolicyScope> {
        let mut scopes = std::collections::BTreeSet::new();
        for record in &self.next.authority.policies().records()
            [self.expected.authority.policies().records().len()..]
        {
            scopes.insert(record.proposal().key().scope());
        }
        for entry in
            &self.next.authority.hold_revisions()[self.expected.authority.hold_revisions().len()..]
        {
            scopes.insert(entry.hold().key().scope());
        }
        for preview in self.next.previews.iter().filter(|preview| {
            !self
                .expected
                .previews
                .iter()
                .any(|old| old.reference() == preview.reference())
        }) {
            scopes.insert(preview.proposal().key().scope());
        }
        scopes.into_iter().collect()
    }
}
impl HistoryLifecycleControl {
    pub fn retains_scope(&self, scope: HistoryPolicyScope) -> bool {
        scope == HistoryPolicyScope::Deployment
            || self
                .authority
                .policies()
                .records()
                .iter()
                .any(|record| record.proposal().key().scope() == scope)
            || self
                .authority
                .hold_revisions()
                .iter()
                .any(|entry| entry.hold().key().scope() == scope)
            || self
                .authority
                .expiry()
                .intents()
                .iter()
                .flat_map(|intent| intent.manifest().members())
                .flat_map(HistoryExpiryMember::attributions)
                .any(|attribution| attribution.matches(scope))
    }
}

/// Bounded administrator evidence for one exact lifecycle operation. Applied
/// review proof remains available through its policy record after preview expiry.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", content = "record", rename_all = "snake_case")]
pub enum HistoryLifecycleOperationEvidence {
    Policy(HistoryPolicyRecord),
    Hold(HistoryHoldRevision),
    ExpiryIntent(HistoryExpiryIntent),
    Review {
        proposal: HistoryPolicyChange,
        review: HistoryPolicyReviewEvidence,
    },
}
impl HistoryLifecycleControl {
    pub fn operation_evidence(&self, identity: Uuid) -> Option<HistoryLifecycleOperationEvidence> {
        if let Some(record) = self
            .authority
            .policies()
            .records()
            .iter()
            .find(|record| record.operation() == identity)
        {
            return Some(HistoryLifecycleOperationEvidence::Policy(record.clone()));
        }
        if let Some(record) = self
            .authority
            .hold_revisions()
            .iter()
            .find(|record| record.operation() == identity)
        {
            return Some(HistoryLifecycleOperationEvidence::Hold(record.clone()));
        }
        if let Some(intent) = self.authority.expiry().intent(identity) {
            return Some(HistoryLifecycleOperationEvidence::ExpiryIntent(
                intent.clone(),
            ));
        }
        if let Some(preview) = self
            .previews
            .iter()
            .find(|preview| preview.reference().preview_id() == identity)
        {
            return Some(HistoryLifecycleOperationEvidence::Review {
                proposal: preview.proposal().clone(),
                review: preview.evidence(),
            });
        }
        self.authority
            .policies()
            .records()
            .iter()
            .find_map(|record| {
                record
                    .review()
                    .filter(|review| review.reference().preview_id() == identity)
                    .map(|review| HistoryLifecycleOperationEvidence::Review {
                        proposal: record.proposal().clone(),
                        review: review.clone(),
                    })
            })
    }
}

fn deserialize_review<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<HistoryPolicyReviewReference>, D::Error> {
    HistoryPolicyReviewReference::deserialize(deserializer).map(Some)
}
