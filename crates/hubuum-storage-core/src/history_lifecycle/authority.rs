use chrono::Duration;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::*;

/// Bounded immutable hold transitions. Expiry is interpreted from its original
/// deadline; recording expiry never starts a new retention window.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "HoldInput")]
pub struct HistoryRetentionHold {
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = "uuid"))]
    identity: Uuid,
    family: StorageIdentityHistoryKind,
    scope: HistoryPolicyScope,
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = DateTime))]
    created_at: DateTime<Utc>,
    #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, format = DateTime))]
    expires_at: Option<DateTime<Utc>>,
    #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, format = DateTime))]
    released_at: Option<DateTime<Utc>>,
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = "uuid"))]
    principal: PrincipalIdentity,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HoldInput {
    identity: Uuid,
    family: StorageIdentityHistoryKind,
    scope: HistoryPolicyScope,
    created_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
    released_at: Option<DateTime<Utc>>,
    principal: PrincipalIdentity,
}
impl HistoryRetentionHold {
    pub fn new(
        identity: Uuid,
        key: HistoryPolicyKey,
        created_at: DateTime<Utc>,
        expires_at: Option<DateTime<Utc>>,
        principal: PrincipalIdentity,
    ) -> Result<Self, StorageValidationError> {
        if identity.get_variant() != uuid::Variant::RFC4122
            || identity.get_version().is_none()
            || expires_at.is_some_and(|at| at <= created_at)
        {
            return Err(invalid("Hold requires an RFC UUID and a future expiry"));
        }
        Ok(Self {
            identity,
            family: key.family(),
            scope: key.scope(),
            created_at,
            expires_at,
            released_at: None,
            principal,
        })
    }
    pub const fn principal(&self) -> PrincipalIdentity {
        self.principal
    }
    pub const fn identity(&self) -> Uuid {
        self.identity
    }
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
    pub const fn expires_at(&self) -> Option<DateTime<Utc>> {
        self.expires_at
    }
    pub const fn released_at(&self) -> Option<DateTime<Utc>> {
        self.released_at
    }
    pub fn key(&self) -> HistoryPolicyKey {
        HistoryPolicyKey {
            family: self.family,
            scope: self.scope,
        }
    }
    pub fn is_active(&self, at: DateTime<Utc>) -> bool {
        at >= self.created_at
            && self.expires_at.is_none_or(|until| at < until)
            && self.released_at.is_none_or(|until| at < until)
    }
    pub fn covers(&self, candidate: &HistoryExpiryCandidate, at: DateTime<Utc>) -> bool {
        self.is_active(at)
            && self.family == candidate.family()
            && (self.scope == HistoryPolicyScope::Deployment
                || candidate
                    .attributions()
                    .iter()
                    .any(|attribution| attribution.matches(self.scope)))
    }
    fn release(&self, at: DateTime<Utc>) -> Result<Self, StorageValidationError> {
        if at < self.created_at {
            return Err(invalid("Hold release cannot precede creation"));
        }
        let mut next = self.clone();
        if next.released_at.is_none() {
            next.released_at = Some(at);
        }
        Ok(next)
    }
}
impl TryFrom<HoldInput> for HistoryRetentionHold {
    type Error = StorageValidationError;
    fn try_from(value: HoldInput) -> Result<Self, Self::Error> {
        let mut hold = Self::new(
            value.identity,
            HistoryPolicyKey::new(value.family, value.scope)?,
            value.created_at,
            value.expires_at,
            value.principal,
        )?;
        if let Some(at) = value.released_at {
            hold = hold.release(at)?;
        }
        Ok(hold)
    }
}

#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "AuthorityInput")]
pub struct HistoryLifecycleAuthority {
    pub(super) revision: HistoryAuthorityRevision,
    policies: HistoryPolicyCatalog,
    holds: Vec<HistoryHoldRevision>,
    #[serde(default, skip_serializing_if = "HistoryExpiryJournal::is_empty")]
    expiry: HistoryExpiryJournal,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(super) size_transitions: Vec<HistorySizeTransition>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthorityInput {
    revision: HistoryAuthorityRevision,
    policies: HistoryPolicyCatalog,
    holds: Vec<HistoryHoldRevision>,
    #[serde(default)]
    expiry: HistoryExpiryJournal,
    #[serde(default)]
    size_transitions: Vec<HistorySizeTransition>,
}
/// Holds retain their creation/release evidence as an append-only sequence.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryHoldRevision {
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = "uuid"))]
    operation: Uuid,
    revision: HistoryAuthorityRevision,
    hold: HistoryRetentionHold,
    #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, format = "uuid"))]
    actor: Option<PrincipalIdentity>,
    transition: HistoryHoldTransition,
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = DateTime))]
    recorded_at: DateTime<Utc>,
}
impl HistoryHoldRevision {
    pub const fn operation(&self) -> Uuid {
        self.operation
    }
    pub const fn hold(&self) -> &HistoryRetentionHold {
        &self.hold
    }
    pub const fn actor(&self) -> Option<PrincipalIdentity> {
        self.actor
    }
    pub const fn revision(&self) -> HistoryAuthorityRevision {
        self.revision
    }
    pub const fn transition(&self) -> HistoryHoldTransition {
        self.transition
    }
    pub const fn recorded_at(&self) -> DateTime<Utc> {
        self.recorded_at
    }
}
impl HistoryLifecycleAuthority {
    pub const MAX_HOLD_REVISIONS: usize = 4096;
    pub fn empty() -> Self {
        Self {
            revision: HistoryAuthorityRevision(0),
            policies: HistoryPolicyCatalog::default(),
            holds: Vec::new(),
            expiry: HistoryExpiryJournal::default(),
            size_transitions: Vec::new(),
        }
    }
    /// Exact pristine authority: zero revision, empty policy/review catalog,
    /// no hold transitions and no retained intent/cancellation/removal facts.
    /// Current eligibility or absence of live targets is never an emptiness test.
    pub fn is_pristine(&self) -> bool {
        self == &Self::empty()
    }
    pub const fn revision(&self) -> HistoryAuthorityRevision {
        self.revision
    }
    /// Policy, hold and exclusion decisions fence control reads. First-removal
    /// receipts record progress under an already installed exclusion.
    pub fn read_revision(&self) -> HistoryReadRevision {
        let decisions = self.policies.records().len()
            + self.holds.len()
            + self
                .expiry
                .intents()
                .iter()
                .map(|intent| 1 + usize::from(intent.cancellation().is_some()))
                .sum::<usize>();
        HistoryReadRevision::new(decisions as u64).expect("bounded history read decisions")
    }
    pub const fn policies(&self) -> &HistoryPolicyCatalog {
        &self.policies
    }
    pub const fn expiry(&self) -> &HistoryExpiryJournal {
        &self.expiry
    }
    pub(super) fn admit_expiry(
        &self,
        manifest: HistoryExpiryManifest,
    ) -> Result<Self, StorageValidationError> {
        if self.policies.records().iter().any(|record| {
            record.operation() == manifest.identity()
                || record
                    .review()
                    .is_some_and(|review| review.reference().preview_id() == manifest.identity())
        }) || self
            .holds
            .iter()
            .any(|record| record.operation() == manifest.identity())
        {
            return Err(invalid(
                "Expiry intent identity collides with retained operation evidence",
            ));
        }
        if manifest.fence().authority_revision() != self.revision {
            return Err(invalid("Expiry selection authority changed"));
        }
        for member in manifest.members() {
            let candidate = HistoryExpiryCandidate::new(
                member.family(),
                member.target(),
                Some(member.anchor()),
                member.attributions().to_vec(),
                false,
            )?;
            let decision = self.inspect(&candidate, manifest.selected_at(), &[])?;
            let policies = decision
                .endpoints()
                .iter()
                .map(|endpoint| {
                    endpoint
                        .policy()
                        .source()
                        .cloned()
                        .ok_or_else(|| invalid("Expiry endpoint lacks current policy evidence"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            if !decision.eligible() || policies != member.policies() {
                return Err(invalid(
                    "Expiry membership no longer has current policy/hold eligibility",
                ));
            }
        }
        self.with_expiry(self.expiry.admit(manifest)?)
    }
    pub(super) fn cancel_expiry(
        &self,
        identity: Uuid,
        principal: PrincipalIdentity,
        at: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        let intent = self
            .expiry
            .intent(identity)
            .ok_or_else(|| invalid("Expiry intent was not found"))?;
        self.with_expiry(
            self.expiry
                .retain(intent.request_cancellation(principal, at)?)?,
        )
    }
    pub(super) fn record_expiry_removal(
        &self,
        identity: Uuid,
        deletion: HistoryExpiryExactDeletion,
    ) -> Result<Self, StorageValidationError> {
        let intent = self
            .expiry
            .intent(identity)
            .ok_or_else(|| invalid("Expiry intent was not found"))?;
        self.with_expiry(self.expiry.retain(intent.record_removed(deletion)?)?)
    }
    fn with_expiry(&self, expiry: HistoryExpiryJournal) -> Result<Self, StorageValidationError> {
        if !expiry.covers(&self.expiry) {
            return Err(invalid("Expiry authority would lose retained evidence"));
        }
        Ok(Self {
            revision: HistoryAuthorityRevision::new(
                self.revision.value() + expiry.revision().value() - self.expiry.revision().value(),
            )?,
            policies: self.policies.clone(),
            holds: self.holds.clone(),
            expiry,
            size_transitions: self.size_transitions.clone(),
        })
    }
    pub(super) fn merge_retained(&self, required: &Self) -> Result<Self, StorageValidationError> {
        let mut merged = if self
            .policies
            .records()
            .starts_with(required.policies.records())
            && self.holds.starts_with(&required.holds)
            && self
                .size_transitions
                .starts_with(&required.size_transitions)
        {
            self.clone()
        } else if required
            .policies
            .records()
            .starts_with(self.policies.records())
            && required.holds.starts_with(&self.holds)
            && required
                .size_transitions
                .starts_with(&self.size_transitions)
        {
            required.clone()
        } else {
            return Err(invalid("Recovered history policy/hold lineage conflicts"));
        };
        merged.expiry = self.expiry.merge(&required.expiry)?;
        merged.revision = HistoryAuthorityRevision::new(
            (merged.policies.records().len() + merged.holds.len() + merged.size_transitions.len())
                as u64
                + merged.expiry.revision().value(),
        )?;
        Ok(merged)
    }
    pub fn hold_revisions(&self) -> &[HistoryHoldRevision] {
        &self.holds
    }
    fn current_hold_revisions(&self) -> impl Iterator<Item = &HistoryHoldRevision> {
        let mut seen = std::collections::BTreeSet::new();
        self.holds
            .iter()
            .rev()
            .filter(move |entry| seen.insert(entry.hold.identity()))
    }
    pub fn current_holds(&self) -> impl Iterator<Item = &HistoryRetentionHold> {
        self.current_hold_revisions().map(|entry| &entry.hold)
    }
    pub fn pending_hold_completions(&self) -> usize {
        self.current_hold_revisions()
            .filter(|entry| entry.transition == HistoryHoldTransition::Created)
            .count()
    }
    pub fn holds(&self, candidate: &HistoryExpiryCandidate, now: DateTime<Utc>) -> bool {
        self.current_holds().any(|hold| hold.covers(candidate, now))
    }
    pub fn inspect(
        &self,
        candidate: &HistoryExpiryCandidate,
        now: DateTime<Utc>,
        measurements: &[HistorySizePressure],
    ) -> Result<HistoryExpiryDecision, StorageValidationError> {
        candidate.inspect(
            &self.policies,
            now,
            self.holds(candidate, now),
            measurements,
        )
    }
    pub fn apply(
        &self,
        change: HistoryPolicyChange,
        principal: PrincipalIdentity,
        at: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        let mut next = self.clone();
        next.policies = self.policies.append(change, principal, at)?;
        next.revision = self.successor()?;
        Ok(next)
    }
    pub(super) fn apply_reviewed(
        &self,
        change: HistoryPolicyChange,
        principal: PrincipalIdentity,
        at: DateTime<Utc>,
        review: Option<HistoryPolicyReviewEvidence>,
    ) -> Result<Self, StorageValidationError> {
        let mut next = self.apply(change, principal, at)?;
        if let Some(review) = review {
            next.policies = next.policies.bind_last_review(review)?;
        }
        Ok(next)
    }
    pub fn create_hold(&self, hold: HistoryRetentionHold) -> Result<Self, StorageValidationError> {
        if self
            .holds
            .iter()
            .any(|old| old.hold.identity() == hold.identity())
        {
            // Only an exact retry is a no-op; release cannot be undone by retrying creation.
            if self
                .holds
                .iter()
                .find(|old| old.hold.identity() == hold.identity())
                .is_some_and(|old| old.hold == hold)
            {
                return Ok(self.clone());
            }
            return Err(invalid(
                "Hold identity already has different immutable scope",
            ));
        }
        if hold.released_at().is_some() {
            return Err(invalid("New holds cannot already be released"));
        }
        let at = hold.created_at();
        let actor = hold.principal;
        self.append_hold(hold, Some(actor), at, HistoryHoldTransition::Created)
    }
    pub fn release_hold(
        &self,
        identity: Uuid,
        actor: PrincipalIdentity,
        now: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        let hold = self
            .holds
            .iter()
            .rev()
            .find(|entry| entry.hold.identity() == identity)
            .ok_or_else(|| invalid("Hold was not found"))?;
        if hold.hold.released_at().is_some() || hold.transition == HistoryHoldTransition::Expired {
            return Ok(self.clone());
        }
        if hold.hold.expires_at().is_some_and(|at| at <= now) {
            return self.settle_expired_hold(identity, now);
        }
        self.append_hold(
            hold.hold.release(now)?,
            Some(actor),
            now,
            HistoryHoldTransition::Released,
        )
    }
    pub fn settle_expired_hold(
        &self,
        identity: Uuid,
        now: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        let entry = self
            .holds
            .iter()
            .rev()
            .find(|entry| entry.hold.identity() == identity)
            .ok_or_else(|| invalid("Hold was not found"))?;
        if entry.transition == HistoryHoldTransition::Expired || entry.hold.released_at().is_some()
        {
            return Ok(self.clone());
        }
        if !entry.hold.expires_at().is_some_and(|at| at <= now) {
            return Err(invalid("Hold has not expired"));
        }
        self.append_hold(
            entry.hold.clone(),
            None,
            now,
            HistoryHoldTransition::Expired,
        )
    }
    fn append_hold(
        &self,
        hold: HistoryRetentionHold,
        actor: Option<PrincipalIdentity>,
        at: DateTime<Utc>,
        transition: HistoryHoldTransition,
    ) -> Result<Self, StorageValidationError> {
        let pending_after = match transition {
            HistoryHoldTransition::Created => self.pending_hold_completions() + 1,
            HistoryHoldTransition::Released | HistoryHoldTransition::Expired => {
                self.pending_hold_completions().saturating_sub(1)
            }
        };
        if self.holds.len() + 1 + pending_after > Self::MAX_HOLD_REVISIONS {
            return Err(invalid(
                "History hold evidence capacity exhausted; terminal transitions require reserved space",
            ));
        }
        let mut next = self.clone();
        next.revision = self.successor()?;
        next.holds.push(HistoryHoldRevision {
            operation: Uuid::new_v4(),
            revision: next.revision,
            hold,
            actor,
            transition,
            recorded_at: at,
        });
        Ok(next)
    }
    fn successor(&self) -> Result<HistoryAuthorityRevision, StorageValidationError> {
        HistoryAuthorityRevision::new(
            self.revision
                .value()
                .checked_add(1)
                .ok_or_else(|| invalid("History authority revision exhausted"))?,
        )
    }
    pub fn covers(&self, previous: &Self) -> bool {
        self.revision >= previous.revision
            && self
                .policies
                .records()
                .starts_with(previous.policies.records())
            && self.holds.starts_with(&previous.holds)
            && self
                .size_transitions
                .starts_with(&previous.size_transitions)
            && self.expiry.covers(&previous.expiry)
    }
}
impl Default for HistoryLifecycleAuthority {
    fn default() -> Self {
        Self::empty()
    }
}
impl TryFrom<AuthorityInput> for HistoryLifecycleAuthority {
    type Error = StorageValidationError;
    fn try_from(value: AuthorityInput) -> Result<Self, Self::Error> {
        if value.holds.len() > Self::MAX_HOLD_REVISIONS
            || value.revision.value()
                != (value.policies.records().len()
                    + value.holds.len()
                    + value.size_transitions.len()) as u64
                    + value.expiry.revision().value()
        {
            return Err(invalid("Invalid history authority revision or capacity"));
        }
        let mut operations = value
            .policies
            .records()
            .iter()
            .map(HistoryPolicyRecord::operation)
            .collect::<std::collections::BTreeSet<_>>();
        let mut last = 0;
        let mut known = std::collections::BTreeMap::<Uuid, &HistoryHoldRevision>::new();
        for entry in &value.holds {
            if !operations.insert(entry.operation)
                || entry.operation.get_variant() != uuid::Variant::RFC4122
                || entry.operation.get_version().is_none()
                || entry.revision.value() <= last
                || entry.revision > value.revision
                || entry.recorded_at < entry.hold.created_at()
            {
                return Err(invalid(
                    "Invalid hold transition revision, identity or clock",
                ));
            }
            let previous = known.insert(entry.hold.identity(), entry);
            let valid = match (previous, entry.transition) {
                (None, HistoryHoldTransition::Created) => {
                    entry.hold.released_at().is_none()
                        && entry.actor == Some(entry.hold.principal())
                        && entry.recorded_at == entry.hold.created_at()
                }
                (Some(previous), HistoryHoldTransition::Released) => {
                    previous.transition == HistoryHoldTransition::Created
                        && entry.actor.is_some()
                        && previous.hold.release(entry.recorded_at)? == entry.hold
                        && previous
                            .hold
                            .expires_at()
                            .is_none_or(|at| at > entry.recorded_at)
                }
                (Some(previous), HistoryHoldTransition::Expired) => {
                    previous.transition == HistoryHoldTransition::Created
                        && entry.actor.is_none()
                        && previous.hold == entry.hold
                        && entry
                            .hold
                            .expires_at()
                            .is_some_and(|at| at <= entry.recorded_at)
                }
                _ => false,
            };
            if !valid {
                return Err(invalid(
                    "Hold transition changed immutable scope or original clocks",
                ));
            }
            last = entry.revision.value();
        }
        if value
            .expiry
            .intents()
            .iter()
            .any(|intent| !operations.insert(intent.identity()))
        {
            return Err(invalid("Duplicate retained history operation identity"));
        }
        for intent in value.expiry.intents() {
            let manifest = intent.manifest();
            if manifest.fence().authority_revision() >= value.revision
                || manifest
                    .members()
                    .iter()
                    .flat_map(HistoryExpiryMember::policies)
                    .any(|source| {
                        !value.policies.records().iter().any(|record| {
                            record.revision() == source.revision()
                                && record.proposal().key() == source.key()
                                && record.applied_at() <= manifest.selected_at()
                        })
                    })
            {
                return Err(invalid(
                    "Retained expiry references unknown or future policy authority",
                ));
            }
        }
        let authority = Self {
            revision: value.revision,
            policies: value.policies,
            holds: value.holds,
            expiry: value.expiry,
            size_transitions: value.size_transitions,
        };
        if authority.holds.len() + authority.pending_hold_completions() > Self::MAX_HOLD_REVISIONS {
            return Err(invalid(
                "History authority must reserve terminal hold revisions",
            ));
        }
        authority.validate_size_transitions()?;
        Ok(authority)
    }
}

/// Application incarnation invalidates reviews after logical or physical recovery.
/// Deletion authority is separate from ordinary receipt settlement/task activity.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "FenceInput")]
pub struct HistoryReviewFence {
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = "uuid"))]
    incarnation: Uuid,
    authority_revision: HistoryAuthorityRevision,
    deletion_revision: HistoryDeletionRevision,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FenceInput {
    incarnation: Uuid,
    authority_revision: HistoryAuthorityRevision,
    deletion_revision: HistoryDeletionRevision,
}
impl HistoryReviewFence {
    pub fn new(
        incarnation: Uuid,
        authority_revision: HistoryAuthorityRevision,
        deletion_revision: HistoryDeletionRevision,
    ) -> Result<Self, StorageValidationError> {
        if incarnation.is_nil() {
            return Err(invalid("Review incarnation must not be nil"));
        }
        Ok(Self {
            incarnation,
            authority_revision,
            deletion_revision,
        })
    }
    pub const fn incarnation(self) -> Uuid {
        self.incarnation
    }
    pub const fn deletion_revision(self) -> HistoryDeletionRevision {
        self.deletion_revision
    }
    pub const fn authority_revision(self) -> HistoryAuthorityRevision {
        self.authority_revision
    }
}
impl TryFrom<FenceInput> for HistoryReviewFence {
    type Error = StorageValidationError;
    fn try_from(value: FenceInput) -> Result<Self, Self::Error> {
        Self::new(
            value.incarnation,
            value.authority_revision,
            value.deletion_revision,
        )
    }
}

#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryPreviewCoverage {
    Complete,
    Bounded,
}

#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "CoverageInput")]
pub struct HistoryPreviewImpact {
    coverage: HistoryPreviewCoverage,
    #[serde(serialize_with = "super::serialize_count")]
    #[cfg_attr(feature = "openapi", schema(value_type = String))]
    scanned: u32,
    #[serde(serialize_with = "super::serialize_count")]
    #[cfg_attr(feature = "openapi", schema(value_type = String))]
    eligible_observed: u32,
    samples: Vec<HistoryExpiryDecision>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CoverageInput {
    coverage: HistoryPreviewCoverage,
    #[serde(deserialize_with = "super::deserialize_count")]
    scanned: u32,
    #[serde(deserialize_with = "super::deserialize_count")]
    eligible_observed: u32,
    samples: Vec<HistoryExpiryDecision>,
}
impl HistoryPreviewImpact {
    pub const MAX_SCANNED: u32 = 1000;
    pub const MAX_SAMPLES: usize = 16;
    pub fn new(
        coverage: HistoryPreviewCoverage,
        scanned: u32,
        eligible_observed: u32,
        samples: Vec<HistoryExpiryDecision>,
    ) -> Result<Self, StorageValidationError> {
        if scanned > Self::MAX_SCANNED
            || eligible_observed > scanned
            || samples.len() > Self::MAX_SAMPLES
            || samples.len() > scanned as usize
        {
            return Err(invalid("Preview impact exceeds its scan/sample bounds"));
        }
        Ok(Self {
            coverage,
            scanned,
            eligible_observed,
            samples,
        })
    }
    pub const fn coverage(&self) -> HistoryPreviewCoverage {
        self.coverage
    }
    pub const fn scanned(&self) -> u32 {
        self.scanned
    }
    pub const fn eligible_observed(&self) -> u32 {
        self.eligible_observed
    }
    pub fn samples(&self) -> &[HistoryExpiryDecision] {
        &self.samples
    }
}
impl TryFrom<CoverageInput> for HistoryPreviewImpact {
    type Error = StorageValidationError;
    fn try_from(value: CoverageInput) -> Result<Self, Self::Error> {
        Self::new(
            value.coverage,
            value.scanned,
            value.eligible_observed,
            value.samples,
        )
    }
}

#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "ReviewInput")]
pub struct HistoryPolicyReviewReference {
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = "uuid"))]
    preview_id: Uuid,
    proposal_fingerprint: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewInput {
    preview_id: Uuid,
    proposal_fingerprint: String,
}
impl TryFrom<ReviewInput> for HistoryPolicyReviewReference {
    type Error = StorageValidationError;
    fn try_from(value: ReviewInput) -> Result<Self, Self::Error> {
        Self::new(value.preview_id, value.proposal_fingerprint)
    }
}
impl HistoryPolicyReviewReference {
    pub fn new(
        preview_id: Uuid,
        proposal_fingerprint: String,
    ) -> Result<Self, StorageValidationError> {
        if preview_id.get_variant() != uuid::Variant::RFC4122
            || preview_id.get_version().is_none()
            || proposal_fingerprint.len() != 71
            || !proposal_fingerprint.starts_with("sha256:")
            || !proposal_fingerprint.as_bytes()[7..]
                .iter()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
        {
            return Err(invalid("Invalid policy review identity or fingerprint"));
        }
        Ok(Self {
            preview_id,
            proposal_fingerprint,
        })
    }
    pub const fn preview_id(&self) -> Uuid {
        self.preview_id
    }
    pub fn proposal_fingerprint(&self) -> &str {
        &self.proposal_fingerprint
    }
}

/// Only a stored issuance record is proof. A caller-supplied digest is insufficient.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "PreviewInput")]
pub struct HistoryPolicyPreview {
    reference: HistoryPolicyReviewReference,
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = "uuid"))]
    principal: PrincipalIdentity,
    proposal: HistoryPolicyChange,
    fence: HistoryReviewFence,
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = DateTime))]
    issued_at: DateTime<Utc>,
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = DateTime))]
    expires_at: DateTime<Utc>,
    impact: HistoryPreviewImpact,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PreviewInput {
    reference: HistoryPolicyReviewReference,
    principal: PrincipalIdentity,
    proposal: HistoryPolicyChange,
    fence: HistoryReviewFence,
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    impact: HistoryPreviewImpact,
}
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryPolicyReviewConflict {
    StalePreview,
    PreviewExpired,
    PreviewMismatch,
    InaccessiblePreview,
}
impl HistoryPolicyReviewConflict {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StalePreview => "stale_preview",
            Self::PreviewExpired => "preview_expired",
            Self::PreviewMismatch => "preview_mismatch",
            Self::InaccessiblePreview => "preview_not_found",
        }
    }
}
impl HistoryPolicyPreview {
    pub const LIFETIME_SECONDS: i64 = 300;
    pub fn issue(
        principal: PrincipalIdentity,
        proposal: HistoryPolicyChange,
        fence: HistoryReviewFence,
        issued_at: DateTime<Utc>,
        impact: HistoryPreviewImpact,
    ) -> Result<Self, StorageValidationError> {
        proposal.validate_at(issued_at)?;
        let expires_at = issued_at
            .checked_add_signed(Duration::seconds(Self::LIFETIME_SECONDS))
            .ok_or_else(|| invalid("Preview expiry is outside the supported clock range"))?;
        let fingerprint = fingerprint(&proposal)?;
        Ok(Self {
            reference: HistoryPolicyReviewReference::new(Uuid::new_v4(), fingerprint)?,
            principal,
            proposal,
            fence,
            issued_at,
            expires_at,
            impact,
        })
    }
    pub const fn principal(&self) -> PrincipalIdentity {
        self.principal
    }
    pub const fn reference(&self) -> &HistoryPolicyReviewReference {
        &self.reference
    }
    pub const fn proposal(&self) -> &HistoryPolicyChange {
        &self.proposal
    }
    pub const fn issued_at(&self) -> DateTime<Utc> {
        self.issued_at
    }
    pub const fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }
    pub const fn fence(&self) -> HistoryReviewFence {
        self.fence
    }
    pub const fn impact(&self) -> &HistoryPreviewImpact {
        &self.impact
    }
    pub fn validate_apply(
        &self,
        principal: PrincipalIdentity,
        proposal: &HistoryPolicyChange,
        reference: &HistoryPolicyReviewReference,
        fence: HistoryReviewFence,
        now: DateTime<Utc>,
    ) -> Result<(), HistoryPolicyReviewConflict> {
        if self.principal != principal {
            return Err(HistoryPolicyReviewConflict::InaccessiblePreview);
        }
        if self.reference != *reference || self.proposal != *proposal {
            return Err(HistoryPolicyReviewConflict::PreviewMismatch);
        }
        if now >= self.expires_at {
            return Err(HistoryPolicyReviewConflict::PreviewExpired);
        }
        if now < self.issued_at || self.fence != fence {
            return Err(HistoryPolicyReviewConflict::StalePreview);
        }
        Ok(())
    }
}
impl TryFrom<PreviewInput> for HistoryPolicyPreview {
    type Error = StorageValidationError;
    fn try_from(value: PreviewInput) -> Result<Self, Self::Error> {
        let reference = HistoryPolicyReviewReference::new(
            value.reference.preview_id,
            value.reference.proposal_fingerprint,
        )?;
        value.proposal.validate_at(value.issued_at)?;
        if reference.proposal_fingerprint != fingerprint(&value.proposal)?
            || value.expires_at
                != value
                    .issued_at
                    .checked_add_signed(Duration::seconds(Self::LIFETIME_SECONDS))
                    .ok_or_else(|| invalid("Invalid preview expiry"))?
        {
            return Err(invalid(
                "Issued review fingerprint or original expiry is invalid",
            ));
        }
        Ok(Self {
            reference,
            principal: value.principal,
            proposal: value.proposal,
            fence: value.fence,
            issued_at: value.issued_at,
            expires_at: value.expires_at,
            impact: value.impact,
        })
    }
}
pub(super) fn fingerprint(
    proposal: &HistoryPolicyChange,
) -> Result<String, StorageValidationError> {
    let bytes =
        serde_json::to_vec(proposal).map_err(|_| invalid("Cannot encode policy proposal"))?;
    Ok(format!(
        "sha256:{}",
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    ))
}

/// The proof metadata retained with an applied policy. Samples remain transient;
/// issuance clocks, principal, proposal fingerprint and relevant fence survive recovery.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryPolicyReviewEvidence {
    reference: HistoryPolicyReviewReference,
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = "uuid"))]
    principal: PrincipalIdentity,
    fence: HistoryReviewFence,
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = DateTime))]
    issued_at: DateTime<Utc>,
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = DateTime))]
    expires_at: DateTime<Utc>,
    coverage: HistoryPreviewCoverage,
}
impl HistoryPolicyPreview {
    pub fn evidence(&self) -> HistoryPolicyReviewEvidence {
        HistoryPolicyReviewEvidence {
            reference: self.reference.clone(),
            principal: self.principal,
            fence: self.fence,
            issued_at: self.issued_at,
            expires_at: self.expires_at,
            coverage: self.impact.coverage(),
        }
    }
}
impl HistoryPolicyReviewEvidence {
    pub const fn principal(&self) -> PrincipalIdentity {
        self.principal
    }
    pub const fn fence(&self) -> HistoryReviewFence {
        self.fence
    }
    pub const fn reference(&self) -> &HistoryPolicyReviewReference {
        &self.reference
    }
    pub const fn issued_at(&self) -> DateTime<Utc> {
        self.issued_at
    }
    pub const fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }
    pub(super) fn validate(
        &self,
        proposal: &HistoryPolicyChange,
        principal: PrincipalIdentity,
        applied_at: DateTime<Utc>,
    ) -> Result<(), StorageValidationError> {
        if self.principal != principal
            || self.reference.proposal_fingerprint() != fingerprint(proposal)?
            || applied_at < self.issued_at
            || applied_at >= self.expires_at
            || self
                .issued_at
                .checked_add_signed(Duration::seconds(HistoryPolicyPreview::LIFETIME_SECONDS))
                != Some(self.expires_at)
        {
            return Err(invalid(
                "Applied policy review does not bind its proposal, principal or original clocks",
            ));
        }
        Ok(())
    }
}

#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryHoldTransition {
    Created,
    Released,
    Expired,
}
