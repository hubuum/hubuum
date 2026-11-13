//! Exact online selection material. Selection is neither a protected intent nor
//! physical cleanup authority; adapters must recheck its fence and protections.

use std::collections::BTreeSet;

use uuid::Uuid;

use super::*;
use crate::StorageRecordIdentity;

mod scan;
pub use scan::*;

/// Stable batch identity and captured authority for native selection. This is
/// not caller-supplied membership or independent checkpoint protection.
#[derive(Clone, Debug)]
pub struct HistoryExpiryAdmissionRequest {
    identity: Uuid,
    run: Uuid,
    origin: HistoryExpiryOrigin,
    scan: HistoryLifecycleScan,
    fence: HistoryReviewFence,
    continuation: Option<HistoryExpiryScanCursor>,
}

impl HistoryExpiryAdmissionRequest {
    pub fn new(
        identity: Uuid,
        run: Uuid,
        origin: HistoryExpiryOrigin,
        scan: HistoryLifecycleScan,
        fence: HistoryReviewFence,
    ) -> Result<Self, StorageValidationError> {
        origin.validate_key(scan.key())?;
        if [identity, run]
            .iter()
            .any(|id| id.get_variant() != uuid::Variant::RFC4122 || id.get_version().is_none())
        {
            return Err(invalid("Expiry admission requires RFC batch and run UUIDs"));
        }
        Ok(Self {
            identity,
            run,
            origin,
            scan,
            fence,
            continuation: None,
        })
    }
    pub const fn identity(&self) -> Uuid {
        self.identity
    }
    pub const fn scan(&self) -> &HistoryLifecycleScan {
        &self.scan
    }
    pub const fn fence(&self) -> HistoryReviewFence {
        self.fence
    }

    pub fn continue_from(
        mut self,
        cursor: HistoryExpiryScanCursor,
    ) -> Result<Self, StorageValidationError> {
        cursor.validate_request(&self)?;
        self.continuation = Some(cursor);
        Ok(self)
    }

    /// Capture a finite native pass boundary under the adapter's authority
    /// lock. A continuation preserves its original boundary despite new rows.
    pub fn scan_window(
        &self,
        through: u64,
    ) -> Result<HistoryExpiryScanWindow<'_>, StorageValidationError> {
        HistoryExpiryScanWindow::new(self, through)
    }

    /// Retain the run and initiation while accepting current policy authority.
    /// A finite pass always restarts; application/deletion fencing stops the run.
    pub fn next_page(
        &self,
        identity: Uuid,
        fence: HistoryReviewFence,
        progress: HistoryExpiryScanProgress,
    ) -> Result<Self, StorageValidationError> {
        if identity == self.identity
            || fence.incarnation() != self.fence.incarnation()
            || fence.deletion_revision() != self.fence.deletion_revision()
        {
            return Err(invalid(
                "Expiry page requires a fresh batch in the original application image",
            ));
        }
        let request = Self::new(
            identity,
            self.run,
            self.origin.clone(),
            self.scan.clone(),
            fence,
        )?;
        match progress {
            HistoryExpiryScanProgress::Continue(cursor) => request.continue_from(cursor),
            HistoryExpiryScanProgress::PassFinished => Ok(request),
        }
    }

    /// Adapters call this under their authority lock. An uncertain retry must
    /// inspect the original batch; it cannot silently select new membership.
    pub fn selection<'a>(
        &self,
        snapshot: &'a HistoryLifecycleSnapshot,
    ) -> Result<HistoryExpirySelection<'a>, StorageValidationError> {
        if self.fence != snapshot.fence()
            || snapshot
                .control()
                .authority()
                .expiry()
                .intent(self.identity)
                .is_some()
        {
            return Err(invalid(
                "Expiry admission authority changed or batch already exists",
            ));
        }
        let mut selection = HistoryExpirySelection::new(
            self.run,
            self.origin.clone(),
            self.scan.clone(),
            snapshot,
        )?;
        selection.manifest.identity = self.identity;
        Ok(selection)
    }
}

/// Complete native encoded input size for one selected historical row, including
/// payload and metadata. Preview identity-byte samples cannot establish it.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "u64")]
pub struct HistoryExpiryInputBytes(u32);
impl HistoryExpiryInputBytes {
    pub const MAX_BATCH_BYTES: u32 = 16 * 1024 * 1024;

    pub fn new(bytes: u64) -> Result<Self, StorageValidationError> {
        if bytes == 0 || bytes > u64::from(Self::MAX_BATCH_BYTES) {
            return Err(invalid(
                "History expiry input must fit the 16 MiB batch bound",
            ));
        }
        Ok(Self(bytes as u32))
    }
    pub const fn value(self) -> u32 {
        self.0
    }
}

impl TryFrom<u64> for HistoryExpiryInputBytes {
    type Error = StorageValidationError;
    fn try_from(bytes: u64) -> Result<Self, Self::Error> {
        Self::new(bytes)
    }
}

/// Minimum temporal evidence retained after payload removal. It prevents
/// as-of selection from silently falling back across an expired interval.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "ReadSpanInput")]
pub struct HistoryExpiryReadSpan {
    kind: HistoryExpiryReadSpanKind,
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = DateTime))]
    valid_from: DateTime<Utc>,
    #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, format = DateTime))]
    valid_to: Option<DateTime<Utc>>,
}
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum HistoryExpiryReadSpanKind {
    Closed,
    Schema,
    Deleted,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadSpanInput {
    kind: HistoryExpiryReadSpanKind,
    valid_from: DateTime<Utc>,
    valid_to: Option<DateTime<Utc>>,
}
impl TryFrom<ReadSpanInput> for HistoryExpiryReadSpan {
    type Error = StorageValidationError;
    fn try_from(value: ReadSpanInput) -> Result<Self, Self::Error> {
        match (value.kind, value.valid_to) {
            (HistoryExpiryReadSpanKind::Deleted, until) => Self::tombstone(value.valid_from, until),
            (HistoryExpiryReadSpanKind::Closed, Some(until)) => {
                Self::closed(value.valid_from, until)
            }
            (HistoryExpiryReadSpanKind::Schema, until) => Self::schema(value.valid_from, until),
            _ => Err(invalid(
                "Expiry read span requires its original bounded interval",
            )),
        }
    }
}
impl HistoryExpiryReadSpan {
    pub fn closed(
        from: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        Self::interval(HistoryExpiryReadSpanKind::Closed, from, until)
    }
    pub fn schema(
        from: DateTime<Utc>,
        until: Option<DateTime<Utc>>,
    ) -> Result<Self, StorageValidationError> {
        match until {
            Some(until) => Self::interval(HistoryExpiryReadSpanKind::Schema, from, until),
            None => Ok(Self {
                kind: HistoryExpiryReadSpanKind::Schema,
                valid_from: from,
                valid_to: None,
            }),
        }
    }
    pub const fn deleted(at: DateTime<Utc>) -> Self {
        Self {
            kind: HistoryExpiryReadSpanKind::Deleted,
            valid_from: at,
            valid_to: None,
        }
    }
    /// Native deletion events may store a point interval or an unbounded marker.
    /// Retain that original representation; both protect the gap after deletion.
    pub(super) fn tombstone(
        from: DateTime<Utc>,
        until: Option<DateTime<Utc>>,
    ) -> Result<Self, StorageValidationError> {
        if until.is_some_and(|until| until != from) {
            return Err(invalid(
                "A tombstone must retain its original deletion point",
            ));
        }
        Ok(Self {
            kind: HistoryExpiryReadSpanKind::Deleted,
            valid_from: from,
            valid_to: until,
        })
    }
    fn interval(
        kind: HistoryExpiryReadSpanKind,
        from: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        if until <= from {
            return Err(invalid("Expiry read interval must end after its start"));
        }
        Ok(Self {
            kind,
            valid_from: from,
            valid_to: Some(until),
        })
    }
    pub const fn valid_from(&self) -> DateTime<Utc> {
        self.valid_from
    }
    pub const fn valid_to(&self) -> Option<DateTime<Utc>> {
        self.valid_to
    }
    pub fn contains(&self, at: DateTime<Utc>) -> bool {
        at >= self.valid_from
            && (self.kind == HistoryExpiryReadSpanKind::Deleted
                || self.valid_to.is_none_or(|until| at < until))
    }
    pub(super) fn matches(
        &self,
        family: StorageIdentityHistoryKind,
        anchor: HistoryRetentionAnchor,
    ) -> bool {
        match (self.kind, anchor) {
            (HistoryExpiryReadSpanKind::Closed, HistoryRetentionAnchor::Closed(at)) => {
                family != StorageIdentityHistoryKind::ClassSchema && self.valid_to == Some(at)
            }
            (HistoryExpiryReadSpanKind::Schema, HistoryRetentionAnchor::SchemaEvent(at)) => {
                family == StorageIdentityHistoryKind::ClassSchema && self.valid_from == at
            }
            (HistoryExpiryReadSpanKind::Deleted, HistoryRetentionAnchor::Deleted(at)) => {
                family != StorageIdentityHistoryKind::ClassSchema && self.valid_from == at
            }
            _ => false,
        }
    }
}

#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "MemberInput")]
pub struct HistoryExpiryMember {
    family: StorageIdentityHistoryKind,
    target: StorageRecordIdentity,
    anchor: HistoryRetentionAnchor,
    read_span: HistoryExpiryReadSpan,
    attributions: Vec<HistoryScopeAttribution>,
    policies: Vec<HistoryPolicyReference>,
    input_bytes: HistoryExpiryInputBytes,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    object_authorization: Option<HistoryObjectAuthorizationFacts>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MemberInput {
    family: StorageIdentityHistoryKind,
    target: StorageRecordIdentity,
    anchor: HistoryRetentionAnchor,
    read_span: HistoryExpiryReadSpan,
    attributions: Vec<HistoryScopeAttribution>,
    policies: Vec<HistoryPolicyReference>,
    input_bytes: HistoryExpiryInputBytes,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    object_authorization: Option<HistoryObjectAuthorizationFacts>,
}
impl TryFrom<MemberInput> for HistoryExpiryMember {
    type Error = StorageValidationError;
    fn try_from(value: MemberInput) -> Result<Self, Self::Error> {
        let member = Self {
            family: value.family,
            target: value.target,
            anchor: value.anchor,
            read_span: value.read_span,
            attributions: value.attributions,
            policies: value.policies,
            input_bytes: value.input_bytes,
            object_authorization: value.object_authorization,
        };
        member.validate()?;
        Ok(member)
    }
}
impl HistoryExpiryMember {
    fn validate(&self) -> Result<(), StorageValidationError> {
        let expected = if matches!(
            self.family,
            StorageIdentityHistoryKind::ClassRelation | StorageIdentityHistoryKind::ObjectRelation
        ) {
            2
        } else {
            1
        };
        if !self.read_span.matches(self.family, self.anchor)
            || self.attributions.len() != expected
            || self.policies.len() != expected
        {
            return Err(invalid(
                "Retained expiry membership lacks exact anchor/endpoint evidence",
            ));
        }
        if let Some(facts) = &self.object_authorization
            && (self.family != StorageIdentityHistoryKind::Object
                || self.target != facts.target()
                || &self.read_span != facts.read_span()
                || self.attributions.as_slice() != [facts.attribution()])
        {
            return Err(invalid(
                "Retained object authorization differs from exact membership",
            ));
        }
        for (source, attribution) in self.policies.iter().zip(&self.attributions) {
            if source.revision().value() == 0
                || source.key().family() != self.family
                || (source.key().scope() != HistoryPolicyScope::Deployment
                    && !attribution.matches(source.key().scope()))
            {
                return Err(invalid(
                    "Retained expiry policy differs from captured endpoint scope",
                ));
            }
        }
        Ok(())
    }
    pub const fn family(&self) -> StorageIdentityHistoryKind {
        self.family
    }
    pub const fn target(&self) -> StorageRecordIdentity {
        self.target
    }
    pub const fn anchor(&self) -> HistoryRetentionAnchor {
        self.anchor
    }
    pub const fn read_span(&self) -> &HistoryExpiryReadSpan {
        &self.read_span
    }
    pub fn attributions(&self) -> &[HistoryScopeAttribution] {
        &self.attributions
    }
    pub fn policies(&self) -> &[HistoryPolicyReference] {
        &self.policies
    }
    pub const fn object_authorization(&self) -> Option<&HistoryObjectAuthorizationFacts> {
        self.object_authorization.as_ref()
    }
    pub const fn input_bytes(&self) -> HistoryExpiryInputBytes {
        self.input_bytes
    }

    /// Recheck native facts and current governing policy under the adapter's
    /// removal lock. An exclusion is never evidence of continued eligibility.
    pub fn validate_removal(
        &self,
        candidate: &HistoryExpiryCandidate,
        authority: &HistoryLifecycleAuthority,
        now: DateTime<Utc>,
    ) -> Result<(), StorageValidationError> {
        if candidate.family() != self.family
            || candidate.target() != self.target
            || candidate.anchor() != Some(self.anchor)
            || candidate.read_span() != Some(&self.read_span)
            || candidate.attributions() != self.attributions
        {
            return Err(invalid(
                "Native expiry copy differs from retained membership",
            ));
        }
        let decision = authority.inspect(candidate, now, &[])?;
        if !decision.eligible()
            || decision
                .endpoints()
                .iter()
                .map(|endpoint| endpoint.policy().source())
                .ne(self.policies.iter().map(Some))
        {
            return Err(invalid(
                "Current policy, hold or dependency blocks history removal",
            ));
        }
        Ok(())
    }
}

/// Stable root initiation, retained independently of task and principal rows.
/// Batch execution may be performed by a worker without changing this origin.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HistoryExpiryOrigin {
    Manual {
        #[cfg_attr(feature = "openapi", schema(value_type = String, format = "uuid"))]
        principal: PrincipalIdentity,
    },
    Scheduled {
        #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, format = "uuid"))]
        initiator: Option<PrincipalIdentity>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        policy: Option<HistoryPolicyReference>,
    },
}

impl HistoryExpiryOrigin {
    pub(super) fn validate_key(&self, key: HistoryPolicyKey) -> Result<(), StorageValidationError> {
        if let Self::Scheduled {
            policy: Some(root),
            initiator,
        } = self
            && (root.key() != key || initiator.is_none())
        {
            return Err(invalid(
                "Periodic history expiry requires its exact policy scope and original initiator",
            ));
        }
        Ok(())
    }
}

/// Finite membership, not a timestamp/allocated-ID progress watermark. No new
/// candidate may join this batch after the adapter admits its protected intent.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "ManifestInput")]
pub struct HistoryExpiryManifest {
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = "uuid"))]
    identity: Uuid,
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = "uuid"))]
    run: Uuid,
    origin: HistoryExpiryOrigin,
    key: HistoryPolicyKey,
    fence: HistoryReviewFence,
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = DateTime))]
    selected_at: DateTime<Utc>,
    input_bytes: u32,
    members: Vec<HistoryExpiryMember>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestInput {
    identity: Uuid,
    run: Uuid,
    origin: HistoryExpiryOrigin,
    key: HistoryPolicyKey,
    fence: HistoryReviewFence,
    selected_at: DateTime<Utc>,
    input_bytes: u32,
    members: Vec<HistoryExpiryMember>,
}
impl TryFrom<ManifestInput> for HistoryExpiryManifest {
    type Error = StorageValidationError;
    fn try_from(value: ManifestInput) -> Result<Self, Self::Error> {
        let manifest = Self {
            identity: value.identity,
            run: value.run,
            origin: value.origin,
            key: value.key,
            fence: value.fence,
            selected_at: value.selected_at,
            input_bytes: value.input_bytes,
            members: value.members,
        };
        manifest.validate()?;
        Ok(manifest)
    }
}
impl HistoryExpiryManifest {
    pub const MAX_MEMBERS: usize = 1000;
    pub const MAX_BYTES: usize = 1024 * 1024;
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageValidationError> {
        if bytes.len() > Self::MAX_BYTES {
            return Err(invalid("Expiry manifest exceeds its encoded byte bound"));
        }
        serde_json::from_slice(bytes).map_err(|_| invalid("Invalid retained expiry manifest"))
    }
    fn validate(&self) -> Result<(), StorageValidationError> {
        self.origin.validate_key(self.key)?;
        if [self.identity, self.run]
            .iter()
            .any(|id| id.get_variant() != uuid::Variant::RFC4122 || id.get_version().is_none())
            || self.members.is_empty()
            || self.members.len() > Self::MAX_MEMBERS
        {
            return Err(invalid(
                "Expiry manifest requires bounded nonempty RFC UUID membership",
            ));
        }
        let scan = HistoryLifecycleScan::new(self.key, Self::MAX_MEMBERS as u32)?;
        let mut seen = BTreeSet::new();
        let mut bytes = 0_u64;
        for member in &self.members {
            member.validate()?;
            let candidate = HistoryExpiryCandidate::new(
                member.family(),
                member.target(),
                Some(member.anchor()),
                member.attributions().to_vec(),
                false,
            )?;
            if !scan.includes(&candidate)
                || member.anchor().at() > self.selected_at
                || !seen.insert((
                    *member.target().resource().as_bytes(),
                    *member.target().version().as_bytes(),
                ))
            {
                return Err(invalid(
                    "Expiry membership changed scope, original clock or exact identity",
                ));
            }
            bytes += u64::from(member.input_bytes().value());
        }
        if bytes != u64::from(self.input_bytes)
            || bytes > u64::from(HistoryExpiryInputBytes::MAX_BATCH_BYTES)
        {
            return Err(invalid(
                "Expiry input accounting differs from exact membership",
            ));
        }
        if serde_json::to_vec(self)
            .map_err(|_| invalid("Cannot encode expiry manifest"))?
            .len()
            > Self::MAX_BYTES
        {
            return Err(invalid("Expiry manifest exceeds its encoded byte bound"));
        }
        Ok(())
    }

    pub const fn identity(&self) -> Uuid {
        self.identity
    }
    pub const fn run(&self) -> Uuid {
        self.run
    }
    pub const fn origin(&self) -> &HistoryExpiryOrigin {
        &self.origin
    }
    pub const fn key(&self) -> HistoryPolicyKey {
        self.key
    }
    pub const fn fence(&self) -> HistoryReviewFence {
        self.fence
    }
    pub const fn selected_at(&self) -> DateTime<Utc> {
        self.selected_at
    }
    pub const fn input_bytes(&self) -> u32 {
        self.input_bytes
    }
    pub fn members(&self) -> &[HistoryExpiryMember] {
        &self.members
    }
}

#[derive(Debug)]
pub enum HistoryExpirySelectionOutcome {
    Selected,
    Blocked(HistoryExpiryDecision),
    /// The selected prefix has reached a row, input, metadata or retained
    /// completion budget. Never advance past the unselected next member.
    BudgetReached,
}

/// Bound a new manifest together with all of its future completion evidence.
/// Compute this once from the locked snapshot, before considering source rows.
struct HistoryExpirySelectionBudget(usize);

impl HistoryExpirySelectionBudget {
    fn new(
        snapshot: &HistoryLifecycleSnapshot,
        manifest: &HistoryExpiryManifest,
        manifest_bytes: usize,
    ) -> Result<Self, StorageValidationError> {
        let control = snapshot.control();
        let authority = control.authority();
        let journal = authority.expiry();
        if journal.intents().len() == HistoryExpiryJournal::MAX_INTENTS {
            return Ok(Self(0));
        }
        let intent_bytes = serde_json::to_vec(&HistoryExpiryIntent::new(manifest.clone()))
            .map_err(|_| invalid("Cannot encode expiry intent header"))?
            .len();
        let overhead = intent_bytes - manifest_bytes
            + usize::from(!journal.is_empty())
            + HistoryExpiryJournal::CANCELLATION_RESERVE_BYTES;
        let pending = journal.completion_reserve_bytes();
        let journal_bytes = serde_json::to_vec(journal.intents())
            .map_err(|_| invalid("Cannot encode retained expiry journal"))?
            .len();
        let journal_budget =
            HistoryExpiryJournal::MAX_BYTES.saturating_sub(journal_bytes + pending + overhead);
        let revision = authority.revision().value();
        let next_revision = HistoryAuthorityRevision::new(
            revision
                .checked_add(1)
                .ok_or_else(|| invalid("History authority revision exhausted"))?,
        )?;
        let revision_growth = next_revision.value().to_string().len() - revision.to_string().len();
        let control_budget = HistoryLifecycleControl::MAX_BYTES.saturating_sub(
            control.encode()?.len()
                + authority.pending_hold_completions()
                    * HistoryLifecycleControl::HOLD_COMPLETION_RESERVE_BYTES
                + pending
                + overhead
                + revision_growth,
        );
        Ok(Self(journal_budget.min(control_budget)))
    }

    fn includes(&self, manifest_bytes: usize, members: usize) -> bool {
        manifest_bytes + members * HistoryExpiryJournal::REMOVAL_RESERVE_BYTES <= self.0
    }
}

/// The adapter measures native full-row input and evaluates the current source;
/// this builder preserves those validated facts across the admission boundary.
pub struct HistoryExpirySelection<'a> {
    snapshot: &'a HistoryLifecycleSnapshot,
    scan: HistoryLifecycleScan,
    manifest: HistoryExpiryManifest,
    identities: BTreeSet<([u8; 16], [u8; 16])>,
    retained_bytes: usize,
    completion_budget: HistoryExpirySelectionBudget,
}
impl<'a> HistoryExpirySelection<'a> {
    pub fn is_empty(&self) -> bool {
        self.manifest.members.is_empty()
    }
    pub fn new(
        run: Uuid,
        origin: HistoryExpiryOrigin,
        scan: HistoryLifecycleScan,
        snapshot: &'a HistoryLifecycleSnapshot,
    ) -> Result<Self, StorageValidationError> {
        if run.get_variant() != uuid::Variant::RFC4122 || run.get_version().is_none() {
            return Err(invalid("History expiry run requires an RFC UUID"));
        }
        let manifest = HistoryExpiryManifest {
            identity: Uuid::new_v4(),
            run,
            origin,
            key: scan.key(),
            fence: snapshot.fence(),
            selected_at: snapshot.observed_at(),
            input_bytes: 0,
            members: Vec::new(),
        };
        let retained_bytes = serde_json::to_vec(&manifest)
            .map_err(|_| invalid("Cannot encode expiry manifest"))?
            .len();
        if retained_bytes > HistoryExpiryManifest::MAX_BYTES {
            return Err(invalid(
                "Expiry manifest header exceeds its encoded byte bound",
            ));
        }
        let completion_budget =
            HistoryExpirySelectionBudget::new(snapshot, &manifest, retained_bytes)?;
        Ok(Self {
            snapshot,
            manifest,
            scan,
            identities: BTreeSet::new(),
            retained_bytes,
            completion_budget,
        })
    }

    pub fn consider(
        &mut self,
        candidate: HistoryExpiryCandidate,
        input_bytes: HistoryExpiryInputBytes,
        measurements: &[HistorySizePressure],
    ) -> Result<HistoryExpirySelectionOutcome, StorageValidationError> {
        if !self.scan.includes(&candidate) {
            return Err(invalid(
                "History expiry candidate differs from selected scope",
            ));
        }
        let target = candidate.target();
        let identity = (*target.resource().as_bytes(), *target.version().as_bytes());
        if self.identities.contains(&identity) {
            return Err(invalid("Duplicate exact history expiry membership"));
        }
        let decision = self.snapshot.control().authority().inspect(
            &candidate,
            self.snapshot.observed_at(),
            measurements,
        )?;
        if !decision.eligible() {
            return Ok(HistoryExpirySelectionOutcome::Blocked(decision));
        }
        if self.manifest.members.len() >= self.scan.limit() as usize
            || self.manifest.members.len() >= HistoryExpiryManifest::MAX_MEMBERS
            || self.manifest.input_bytes + input_bytes.value()
                > HistoryExpiryInputBytes::MAX_BATCH_BYTES
        {
            return Ok(HistoryExpirySelectionOutcome::BudgetReached);
        }
        let anchor = candidate
            .anchor()
            .ok_or_else(|| invalid("Expiry membership requires an original closed anchor"))?;
        let read_span = candidate.read_span().cloned().ok_or_else(|| {
            invalid("Executable expiry membership requires native read-span evidence")
        })?;
        let policies = decision
            .endpoints()
            .iter()
            .map(|endpoint| {
                endpoint
                    .policy()
                    .source()
                    .cloned()
                    .ok_or_else(|| invalid("Eligible expiry endpoint lacks governing policy"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let member = HistoryExpiryMember {
            family: candidate.family(),
            target: candidate.target(),
            anchor,
            read_span,
            attributions: candidate.attributions().to_vec(),
            policies,
            input_bytes,
            object_authorization: candidate.object_authorization().cloned(),
        };
        let next_input = self.manifest.input_bytes + input_bytes.value();
        let member_bytes = serde_json::to_vec(&member)
            .map_err(|_| invalid("Cannot encode exact expiry membership"))?
            .len();
        // Fixed manifest fields are immutable; only membership commas and the
        // decimal input-byte total change. Count each encoded member once.
        let next_retained = self.retained_bytes
            + member_bytes
            + usize::from(!self.manifest.members.is_empty())
            + next_input.to_string().len()
            - self.manifest.input_bytes.to_string().len();
        if next_retained > HistoryExpiryManifest::MAX_BYTES {
            return Ok(HistoryExpirySelectionOutcome::BudgetReached);
        }
        if !self
            .completion_budget
            .includes(next_retained, self.manifest.members.len() + 1)
        {
            if self.is_empty() {
                return Err(invalid(
                    "Retained history has no completion capacity for another exact member",
                ));
            }
            return Ok(HistoryExpirySelectionOutcome::BudgetReached);
        }
        self.identities.insert(identity);
        self.manifest.input_bytes = next_input;
        self.retained_bytes = next_retained;
        self.manifest.members.push(member);
        Ok(HistoryExpirySelectionOutcome::Selected)
    }

    pub fn finish(self) -> Result<HistoryExpiryManifest, StorageValidationError> {
        self.manifest.validate()?;
        Ok(self.manifest)
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Future, ready};
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    use hubuum_domain::{IdentityReference, VersionIdentity};
    use rstest::rstest;

    use super::*;
    use crate::StorageRecordIdentity;

    #[rstest]
    #[case::batch(true)]
    #[case::run(false)]
    fn admission_rejects_unidentified_batches(#[case] nil_batch: bool) {
        let before = snapshot();
        assert!(
            HistoryExpiryAdmissionRequest::new(
                if nil_batch {
                    Uuid::nil()
                } else {
                    Uuid::new_v4()
                },
                if nil_batch {
                    Uuid::new_v4()
                } else {
                    Uuid::nil()
                },
                HistoryExpiryOrigin::Scheduled {
                    initiator: None,
                    policy: None
                },
                HistoryLifecycleScan::new(key(), 1).unwrap(),
                before.fence(),
            )
            .is_err()
        );
    }

    #[test]
    fn admission_binds_original_batch_identity_and_fence() {
        let before = snapshot();
        let identity = Uuid::new_v4();
        let request = HistoryExpiryAdmissionRequest::new(
            identity,
            Uuid::new_v4(),
            HistoryExpiryOrigin::Scheduled {
                initiator: None,
                policy: None,
            },
            HistoryLifecycleScan::new(key(), 1).unwrap(),
            before.fence(),
        )
        .unwrap();
        let mut selection = request.selection(&before).unwrap();
        assert!(matches!(
            selection
                .consider(
                    candidate(Some(HistoryRetentionAnchor::Closed(at(110)))),
                    HistoryExpiryInputBytes::new(1).unwrap(),
                    &[],
                )
                .unwrap(),
            HistoryExpirySelectionOutcome::Selected
        ));
        assert_eq!(selection.finish().unwrap().identity(), identity);
        let changed = HistoryLifecycleSnapshot::new(
            before.control().clone(),
            Uuid::new_v4(),
            before.fence().deletion_revision(),
            before.observed_at(),
        )
        .unwrap();
        assert!(request.selection(&changed).is_err());
    }

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(seconds, 0).unwrap()
    }
    fn key() -> HistoryPolicyKey {
        HistoryPolicyKey::new(
            StorageIdentityHistoryKind::Object,
            HistoryPolicyScope::Deployment,
        )
        .unwrap()
    }
    fn snapshot() -> HistoryLifecycleSnapshot {
        let snapshot = HistoryLifecycleSnapshot::new(
            HistoryLifecycleControl::empty(),
            Uuid::new_v4(),
            HistoryDeletionRevision::new(0).unwrap(),
            at(100),
        )
        .unwrap();
        let proposal = HistoryPolicyChange::new(
            key(),
            HistoryPolicyRevision::new(0).unwrap(),
            HistoryPolicyBinding::Expire {
                trigger: HistoryExpiryTrigger::Age {
                    age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
                    deleted_age_seconds: None,
                },
                minimum_age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
            },
            HistoryPolicyApplication::Prospective,
        )
        .unwrap();
        let applied = snapshot
            .control()
            .apply(
                HistoryPolicyApplyRequest::new(proposal, None),
                PrincipalIdentity::generate(),
                snapshot.fence(),
                at(100),
            )
            .unwrap();
        HistoryLifecycleSnapshot::new(
            applied.next().clone(),
            snapshot.fence().incarnation(),
            snapshot.fence().deletion_revision(),
            at(200),
        )
        .unwrap()
    }
    fn candidate(anchor: Option<HistoryRetentionAnchor>) -> HistoryExpiryCandidate {
        let candidate = HistoryExpiryCandidate::new(
            StorageIdentityHistoryKind::Object,
            StorageRecordIdentity::new(ResourceIdentity::generate(), VersionIdentity::generate()),
            anchor,
            vec![HistoryScopeAttribution::new(
                ResourceIdentity::generate(),
                None,
            )],
            false,
        )
        .unwrap();
        match anchor {
            Some(HistoryRetentionAnchor::Closed(until)) => candidate
                .with_read_span(HistoryExpiryReadSpan::closed(at(100), until).unwrap())
                .unwrap(),
            Some(HistoryRetentionAnchor::Deleted(at)) => candidate
                .with_read_span(HistoryExpiryReadSpan::deleted(at))
                .unwrap(),
            Some(HistoryRetentionAnchor::SchemaEvent(at)) => candidate
                .with_read_span(
                    HistoryExpiryReadSpan::schema(at, Some(at + chrono::Duration::seconds(1)))
                        .unwrap(),
                )
                .unwrap(),
            None => candidate,
        }
    }

    fn fill_completion_capacity(holds: usize) -> (HistoryLifecycleSnapshot, usize) {
        let mut current = snapshot();
        for _ in 0..holds {
            let hold = HistoryRetentionHold::new(
                Uuid::new_v4(),
                HistoryPolicyKey::new(
                    StorageIdentityHistoryKind::Class,
                    HistoryPolicyScope::Deployment,
                )
                .unwrap(),
                at(150),
                None,
                PrincipalIdentity::generate(),
            )
            .unwrap();
            let change = current
                .control()
                .create_hold(hold, current.fence())
                .unwrap();
            current = HistoryLifecycleSnapshot::new(
                change.next().clone(),
                current.fence().incarnation(),
                current.fence().deletion_revision(),
                at(200),
            )
            .unwrap();
        }
        for _ in 0..HistoryExpiryJournal::MAX_INTENTS {
            let mut selection = HistoryExpirySelection::new(
                Uuid::new_v4(),
                HistoryExpiryOrigin::Manual {
                    principal: PrincipalIdentity::generate(),
                },
                HistoryLifecycleScan::new(key(), 1000).unwrap(),
                &current,
            )
            .unwrap();
            for _ in 0..1000 {
                match selection
                    .consider(
                        candidate(Some(HistoryRetentionAnchor::Closed(at(110)))),
                        HistoryExpiryInputBytes::new(1).unwrap(),
                        &[],
                    )
                    .unwrap()
                {
                    HistoryExpirySelectionOutcome::Selected => {}
                    HistoryExpirySelectionOutcome::BudgetReached => break,
                    HistoryExpirySelectionOutcome::Blocked(_) => {
                        panic!("Fixture members must remain eligible")
                    }
                }
            }
            let manifest = selection.finish().unwrap();
            let count = manifest.members().len();
            let admitted = current
                .control()
                .admit_expiry(manifest, current.fence())
                .expect(
                    "Selection must preserve enough capacity to admit and finish its entire batch",
                );
            current = HistoryLifecycleSnapshot::new(
                admitted.next().clone(),
                current.fence().incarnation(),
                current.fence().deletion_revision(),
                at(200),
            )
            .unwrap();
            if count < 1000 {
                return (current, count);
            }
        }
        panic!("Fixture must reach completion-reserved capacity");
    }

    #[rstest]
    #[case::journal(0)]
    #[case::control_with_pending_holds(16)]
    fn selection_stops_before_exceeding_retained_completion_capacity(#[case] holds: usize) {
        let (filled, count) = fill_completion_capacity(holds);
        assert!((1..1000).contains(&count));
        assert!(filled.control().encode().is_ok());
    }

    #[rstest]
    #[case::journal(0)]
    #[case::control_with_pending_holds(16)]
    fn no_completion_capacity_rejects_without_retaining_membership(#[case] holds: usize) {
        let (filled, _) = fill_completion_capacity(holds);
        let mut selection = HistoryExpirySelection::new(
            Uuid::new_v4(),
            HistoryExpiryOrigin::Manual {
                principal: PrincipalIdentity::generate(),
            },
            HistoryLifecycleScan::new(key(), 1000).unwrap(),
            &filled,
        )
        .unwrap();
        assert!(
            selection
                .consider(
                    candidate(Some(HistoryRetentionAnchor::Closed(at(110)))),
                    HistoryExpiryInputBytes::new(1).unwrap(),
                    &[]
                )
                .is_err()
        );
        assert!(selection.is_empty());
    }

    #[test]
    fn an_ineligible_source_needs_no_new_completion_capacity() {
        let (filled, _) = fill_completion_capacity(0);
        let mut selection = HistoryExpirySelection::new(
            Uuid::new_v4(),
            HistoryExpiryOrigin::Manual {
                principal: PrincipalIdentity::generate(),
            },
            HistoryLifecycleScan::new(key(), 1000).unwrap(),
            &filled,
        )
        .unwrap();
        assert!(matches!(
            selection
                .consider(
                    candidate(None),
                    HistoryExpiryInputBytes::new(1).unwrap(),
                    &[]
                )
                .unwrap(),
            HistoryExpirySelectionOutcome::Blocked(_)
        ));
        assert!(selection.is_empty());
    }

    #[rstest::rstest]
    #[case::missing_copy(0, false)]
    #[case::exact_copy(1, true)]
    #[case::multiple_copies(2, false)]
    fn removal_clock_requires_an_exact_delete(#[case] affected_rows: u64, #[case] accepted: bool) {
        let target = candidate(Some(HistoryRetentionAnchor::Closed(at(110)))).target();
        let proof = HistoryExpiryExactDeletion::from_affected_rows(
            target,
            checkpoint('a'),
            at(210),
            affected_rows,
        );
        assert_eq!(proof.is_ok(), accepted);
    }

    #[rstest::rstest]
    #[case::closed(
        Some(HistoryRetentionAnchor::Closed(at(110))),
        at(100),
        Some(at(110)),
        true
    )]
    #[case::changed_closure(
        Some(HistoryRetentionAnchor::Closed(at(110))),
        at(100),
        Some(at(111)),
        false
    )]
    #[case::zero_interval(
        Some(HistoryRetentionAnchor::Closed(at(110))),
        at(110),
        Some(at(110)),
        false
    )]
    #[case::deleted(Some(HistoryRetentionAnchor::Deleted(at(110))), at(110), None, true)]
    #[case::point_tombstone(
        Some(HistoryRetentionAnchor::Deleted(at(110))),
        at(110),
        Some(at(110)),
        true
    )]
    #[case::changed_deletion(Some(HistoryRetentionAnchor::Deleted(at(110))), at(111), None, false)]
    #[case::bounded_tombstone(
        Some(HistoryRetentionAnchor::Deleted(at(110))),
        at(110),
        Some(at(120)),
        false
    )]
    #[case::open(None, at(100), None, true)]
    #[case::invented_closure(None, at(100), Some(at(110)), false)]
    fn native_interval_preserves_original_clocks(
        #[case] anchor: Option<HistoryRetentionAnchor>,
        #[case] from: DateTime<Utc>,
        #[case] until: Option<DateTime<Utc>>,
        #[case] accepted: bool,
    ) {
        let result = candidate(anchor).with_native_interval(from, until);
        assert_eq!(result.is_ok(), accepted);
        if anchor.is_none()
            && let Ok(candidate) = result
        {
            assert!(
                candidate.read_span().is_none(),
                "Open versions have no expiry interval"
            );
        }
    }

    #[rstest::rstest]
    #[case::before(at(109), false)]
    #[case::at_deletion(at(110), true)]
    #[case::after_deletion(at(120), true)]
    fn point_tombstone_preserves_its_interval_and_protects_the_deletion_gap(
        #[case] queried_at: DateTime<Utc>,
        #[case] excluded: bool,
    ) {
        let original = candidate(Some(HistoryRetentionAnchor::Deleted(at(110))))
            .with_native_interval(at(110), Some(at(110)))
            .unwrap();
        let span = original.read_span().unwrap();
        assert_eq!(span.valid_to(), Some(at(110)));
        let decoded: HistoryExpiryReadSpan =
            serde_json::from_value(serde_json::to_value(span).unwrap()).unwrap();
        assert_eq!(decoded.contains(queried_at), excluded);
    }

    #[test]
    fn selection_preserves_exact_identity_original_anchor_and_authority_fence() {
        let snapshot = snapshot();
        let run = Uuid::new_v4();
        let candidate = candidate(Some(HistoryRetentionAnchor::Closed(at(110))));
        let mut selection = HistoryExpirySelection::new(
            run,
            HistoryExpiryOrigin::Manual {
                principal: PrincipalIdentity::generate(),
            },
            HistoryLifecycleScan::new(key(), 1).unwrap(),
            &snapshot,
        )
        .unwrap();
        assert!(matches!(
            selection
                .consider(
                    candidate.clone(),
                    HistoryExpiryInputBytes::new(4096).unwrap(),
                    &[]
                )
                .unwrap(),
            HistoryExpirySelectionOutcome::Selected
        ));
        let manifest = selection.finish().unwrap();
        assert_eq!(manifest.run(), run);
        assert_eq!(manifest.fence(), snapshot.fence());
        assert_eq!(manifest.selected_at(), at(200));
        assert_eq!(manifest.members()[0].target(), candidate.target());
        assert_eq!(manifest.members()[0].anchor(), candidate.anchor().unwrap());
        assert_eq!(
            manifest.members()[0].attributions(),
            candidate.attributions()
        );
        assert_eq!(manifest.input_bytes(), 4096);
    }
    #[test]
    fn selection_cannot_turn_an_open_version_into_executable_membership() {
        let snapshot = snapshot();
        let mut selection = HistoryExpirySelection::new(
            Uuid::new_v4(),
            HistoryExpiryOrigin::Manual {
                principal: PrincipalIdentity::generate(),
            },
            HistoryLifecycleScan::new(key(), 1).unwrap(),
            &snapshot,
        )
        .unwrap();
        assert!(matches!(
            selection
                .consider(
                    candidate(None),
                    HistoryExpiryInputBytes::new(1).unwrap(),
                    &[]
                )
                .unwrap(),
            HistoryExpirySelectionOutcome::Blocked(_)
        ));
        assert!(selection.finish().is_err());
    }
    #[test]
    fn byte_exhaustion_does_not_add_a_partial_or_replacement_member() {
        let snapshot = snapshot();
        let mut selection = HistoryExpirySelection::new(
            Uuid::new_v4(),
            HistoryExpiryOrigin::Manual {
                principal: PrincipalIdentity::generate(),
            },
            HistoryLifecycleScan::new(key(), 2).unwrap(),
            &snapshot,
        )
        .unwrap();
        selection
            .consider(
                candidate(Some(HistoryRetentionAnchor::Closed(at(110)))),
                HistoryExpiryInputBytes::new(u64::from(HistoryExpiryInputBytes::MAX_BATCH_BYTES))
                    .unwrap(),
                &[],
            )
            .unwrap();
        assert!(matches!(
            selection
                .consider(
                    candidate(Some(HistoryRetentionAnchor::Closed(at(120)))),
                    HistoryExpiryInputBytes::new(1).unwrap(),
                    &[]
                )
                .unwrap(),
            HistoryExpirySelectionOutcome::BudgetReached
        ));
        assert_eq!(selection.finish().unwrap().members().len(), 1);
    }
    #[test]
    fn duplicate_exact_membership_is_rejected() {
        let snapshot = snapshot();
        let mut selection = HistoryExpirySelection::new(
            Uuid::new_v4(),
            HistoryExpiryOrigin::Manual {
                principal: PrincipalIdentity::generate(),
            },
            HistoryLifecycleScan::new(key(), 2).unwrap(),
            &snapshot,
        )
        .unwrap();
        let candidate = candidate(Some(HistoryRetentionAnchor::Closed(at(110))));
        let bytes = HistoryExpiryInputBytes::new(1).unwrap();
        selection.consider(candidate.clone(), bytes, &[]).unwrap();
        assert!(selection.consider(candidate, bytes, &[]).is_err());
    }
    fn intent() -> HistoryExpiryIntent {
        HistoryExpiryIntent::new(manifest(&snapshot()))
    }
    fn manifest(snapshot: &HistoryLifecycleSnapshot) -> HistoryExpiryManifest {
        let mut selection = HistoryExpirySelection::new(
            Uuid::new_v4(),
            HistoryExpiryOrigin::Manual {
                principal: PrincipalIdentity::generate(),
            },
            HistoryLifecycleScan::new(key(), 2).unwrap(),
            snapshot,
        )
        .unwrap();
        for seconds in [110, 120] {
            selection
                .consider(
                    candidate(Some(HistoryRetentionAnchor::Closed(at(seconds)))),
                    HistoryExpiryInputBytes::new(100).unwrap(),
                    &[],
                )
                .unwrap();
        }
        selection.finish().unwrap()
    }
    #[rstest]
    #[case::original("original", true)]
    #[case::known_alias("alias", true)]
    #[case::new_authored_version("authored", false)]
    #[case::unrelated_source("other_source", false)]
    #[case::unknown_source("unknown", true)]
    #[case::inherited_unknown("inherited_unknown", true)]
    #[case::other_resource("other_resource", false)]
    #[case::other_family("other_family", false)]
    #[case::missing_evidence("missing", true)]
    #[case::missing_other_family("missing_other_family", false)]
    fn retained_source_versions_cannot_reappear_through_restore_aliases(
        #[case] mode: &str,
        #[case] excluded: bool,
    ) {
        let intent = intent();
        let target = intent.manifest().members()[0].target();
        let journal = HistoryExpiryJournal::new(vec![intent]).unwrap();
        let derived = || {
            let mut bytes = *VersionIdentity::generate().as_bytes();
            bytes[6] = (bytes[6] & 0x0f) | 0x80;
            VersionIdentity::from_bytes(bytes).unwrap()
        };
        let resource = if mode == "other_resource" {
            ResourceIdentity::generate()
        } else {
            target.resource()
        };
        let version = match mode {
            "authored" | "other_source" => VersionIdentity::generate(),
            "unknown" | "inherited_unknown" => derived(),
            _ => target.version(),
        };
        let evidence = crate::StorageIdentityEvidence::try_new(
            Some(crate::StorageIdentitySubject::Resource {
                identity: IdentityReference::Known { identity: resource },
                version,
            }),
            Default::default(),
        )
        .unwrap();
        let evidence = match mode {
            "alias" | "other_source" | "other_resource" | "inherited_unknown" => {
                evidence.at_restore_baseline(derived()).unwrap()
            }
            _ => evidence,
        };
        let family = if matches!(mode, "other_family" | "missing_other_family") {
            StorageIdentityHistoryKind::Class
        } else {
            StorageIdentityHistoryKind::Object
        };
        assert_eq!(
            journal.excludes_evidence(
                family,
                (!matches!(mode, "missing" | "missing_other_family")).then_some(&evidence)
            ),
            excluded
        );
    }

    fn checkpoint(byte: char) -> crate::DeletionCheckpointIdentity {
        crate::DeletionCheckpointIdentity::new(byte.to_string().repeat(64)).unwrap()
    }
    #[test]
    fn cancellation_preserves_pending_exact_exclusions() {
        let intent = intent();
        let target = intent.manifest().members()[0].target();
        let cancelled = intent
            .request_cancellation(PrincipalIdentity::generate(), at(210))
            .unwrap();
        assert!(cancelled.excludes(StorageIdentityHistoryKind::Object, target));
        assert!(!cancelled.is_complete());
        assert!(cancelled.covers(&intent));
    }
    #[test]
    fn replay_preserves_original_removal_clock_and_checkpoint() {
        let intent = intent();
        let target = intent.manifest().members()[0].target();
        let removed = intent
            .record_removed(
                HistoryExpiryExactDeletion::from_affected_rows(target, checkpoint('a'), at(210), 1)
                    .unwrap(),
            )
            .unwrap();
        let replayed = removed
            .record_removed(
                HistoryExpiryExactDeletion::from_affected_rows(target, checkpoint('b'), at(220), 1)
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(replayed.removals(), removed.removals());
        assert!(replayed.excludes(StorageIdentityHistoryKind::Object, target));
    }
    #[test]
    fn recovery_merge_preserves_both_compatible_descendant_outcomes() {
        let intent = intent();
        let first = intent.manifest().members()[0].target();
        let second = intent.manifest().members()[1].target();
        let local = intent
            .record_removed(
                HistoryExpiryExactDeletion::from_affected_rows(first, checkpoint('a'), at(210), 1)
                    .unwrap(),
            )
            .unwrap();
        let required = intent
            .record_removed(
                HistoryExpiryExactDeletion::from_affected_rows(second, checkpoint('b'), at(220), 1)
                    .unwrap(),
            )
            .unwrap();
        let merged = local.merge(&required).unwrap();
        assert!(merged.covers(&local));
        assert!(merged.covers(&required));
        assert!(merged.is_complete());
        assert!(merged.excludes(StorageIdentityHistoryKind::Object, first));
        assert!(merged.excludes(StorageIdentityHistoryKind::Object, second));
    }
    #[test]
    fn recovery_rejects_conflicting_original_removal_evidence() {
        let intent = intent();
        let target = intent.manifest().members()[0].target();
        let local = intent
            .record_removed(
                HistoryExpiryExactDeletion::from_affected_rows(target, checkpoint('a'), at(210), 1)
                    .unwrap(),
            )
            .unwrap();
        let contradictory = intent
            .record_removed(
                HistoryExpiryExactDeletion::from_affected_rows(target, checkpoint('b'), at(220), 1)
                    .unwrap(),
            )
            .unwrap();
        assert!(local.merge(&contradictory).is_err());
    }
    #[rstest::rstest]
    #[case("duplicate")]
    #[case("policy_family")]
    #[case("input_total")]
    fn retained_manifest_rejects_corrupted_membership(#[case] corruption: &str) {
        let intent = intent();
        let mut encoded = serde_json::to_value(intent.manifest()).unwrap();
        match corruption {
            "duplicate" => encoded["members"][1] = encoded["members"][0].clone(),
            "policy_family" => {
                encoded["members"][0]["policies"][0]["key"]["family"] = serde_json::json!("class");
            }
            "input_total" => encoded["input_bytes"] = serde_json::json!(0),
            _ => unreachable!(),
        }
        assert!(HistoryExpiryManifest::decode(&serde_json::to_vec(&encoded).unwrap()).is_err());
    }
    #[test]
    fn retained_intent_roundtrip_preserves_cancellation_and_original_removal() {
        let original = intent();
        let target = original.manifest().members()[0].target();
        let retained = original
            .record_removed(
                HistoryExpiryExactDeletion::from_affected_rows(target, checkpoint('a'), at(210), 1)
                    .unwrap(),
            )
            .unwrap()
            .request_cancellation(PrincipalIdentity::generate(), at(220))
            .unwrap();
        let decoded: HistoryExpiryIntent =
            serde_json::from_slice(&serde_json::to_vec(&retained).unwrap()).unwrap();
        assert_eq!(decoded, retained);
        assert!(decoded.covers(&original));
        assert!(decoded.excludes(StorageIdentityHistoryKind::Object, target));
    }
    #[test]
    fn retained_intent_rejects_duplicate_removal_receipts() {
        let original = intent();
        let target = original.manifest().members()[0].target();
        let retained = original
            .record_removed(
                HistoryExpiryExactDeletion::from_affected_rows(target, checkpoint('a'), at(210), 1)
                    .unwrap(),
            )
            .unwrap();
        let mut encoded = serde_json::to_value(retained).unwrap();
        let receipt = encoded["removals"][0].clone();
        encoded["removals"].as_array_mut().unwrap().push(receipt);
        assert!(serde_json::from_value::<HistoryExpiryIntent>(encoded).is_err());
    }

    fn reverse_ordered_intent() -> HistoryExpiryIntent {
        let snapshot = snapshot();
        let mut selection = HistoryExpirySelection::new(
            Uuid::new_v4(),
            HistoryExpiryOrigin::Manual {
                principal: PrincipalIdentity::generate(),
            },
            HistoryLifecycleScan::new(key(), 32).unwrap(),
            &snapshot,
        )
        .unwrap();
        for _ in 0..32 {
            selection
                .consider(
                    candidate(Some(HistoryRetentionAnchor::Closed(at(110)))),
                    HistoryExpiryInputBytes::new(100).unwrap(),
                    &[],
                )
                .unwrap();
        }
        let mut manifest = selection.finish().unwrap();
        manifest.members.sort_by_key(|member| {
            (
                *member.target().resource().as_bytes(),
                *member.target().version().as_bytes(),
            )
        });
        manifest.members.reverse();
        HistoryExpiryIntent::new(
            HistoryExpiryManifest::decode(&serde_json::to_vec(&manifest).unwrap()).unwrap(),
        )
    }

    fn remove_fixture_member(
        intent: &HistoryExpiryIntent,
        index: usize,
        byte: char,
    ) -> HistoryExpiryIntent {
        intent
            .record_removed(
                HistoryExpiryExactDeletion::from_affected_rows(
                    intent.manifest().members()[index].target(),
                    checkpoint(byte),
                    at(210),
                    1,
                )
                .unwrap(),
            )
            .unwrap()
    }

    #[test]
    fn sparse_receipts_preserve_unsorted_membership_and_next_original_copy() {
        let original = reverse_ordered_intent();
        let retained = [31, 0, 15]
            .into_iter()
            .fold(original.clone(), |intent, index| {
                remove_fixture_member(&intent, index, 'a')
            });
        let decoded: HistoryExpiryIntent =
            serde_json::from_value(serde_json::to_value(&retained).unwrap()).unwrap();
        assert_eq!(decoded, retained);
        assert_eq!(
            decoded.pending_member(),
            Some(&original.manifest().members()[1])
        );
    }

    #[rstest]
    #[case::missing_receipt("missing")]
    #[case::different_original_checkpoint("checkpoint")]
    #[case::different_original_clock("clock")]
    fn sparse_receipt_coverage_rejects_lost_or_changed_original_facts(#[case] corruption: &str) {
        let original = reverse_ordered_intent();
        let required = [0, 15, 31].into_iter().fold(original, |intent, index| {
            remove_fixture_member(&intent, index, 'a')
        });
        let mut encoded = serde_json::to_value(&required).unwrap();
        match corruption {
            "missing" => {
                encoded["removals"].as_array_mut().unwrap().remove(1);
            }
            "checkpoint" => {
                encoded["removals"][1]["checkpoint"] = serde_json::json!(checkpoint('b'))
            }
            "clock" => encoded["removals"][1]["removed_at"] = serde_json::json!(at(211)),
            _ => unreachable!(),
        }
        let changed: HistoryExpiryIntent = serde_json::from_value(encoded).unwrap();
        assert!(!changed.covers(&required));
    }

    #[test]
    fn interleaved_receipt_merge_preserves_original_facts_and_completes_membership() {
        let original = reverse_ordered_intent();
        let local = (0..32).step_by(2).fold(original.clone(), |intent, index| {
            remove_fixture_member(&intent, index, 'a')
        });
        let required = (1..32).step_by(2).fold(original, |intent, index| {
            remove_fixture_member(&intent, index, 'b')
        });
        let merged = local.merge(&required).unwrap();
        assert!(merged.covers(&local) && merged.covers(&required));
        assert!(merged.is_complete());
        assert!(merged.pending_member().is_none());
    }

    #[test]
    fn sparse_receipt_audit_emits_only_the_new_exact_outcome() {
        let old = [31, 0, 15]
            .into_iter()
            .fold(reverse_ordered_intent(), |intent, index| {
                remove_fixture_member(&intent, index, 'a')
            });
        let next = remove_fixture_member(&old, 8, 'b');
        let events = HistoryExpiryJournal::new(vec![next])
            .unwrap()
            .audit_events_since(&HistoryExpiryJournal::new(vec![old]).unwrap())
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].metadata()["lifecycle"]["name"],
            "lifecycle.erasure.copy_removed"
        );
    }
    #[test]
    fn journal_recovery_merges_descendants_without_losing_local_obligations() {
        let intent = intent();
        let target = intent.manifest().members()[0].target();
        let local = HistoryExpiryJournal::new(vec![
            intent
                .record_removed(
                    HistoryExpiryExactDeletion::from_affected_rows(
                        target,
                        checkpoint('a'),
                        at(210),
                        1,
                    )
                    .unwrap(),
                )
                .unwrap(),
        ])
        .unwrap();
        let required = HistoryExpiryJournal::new(vec![
            intent
                .request_cancellation(PrincipalIdentity::generate(), at(220))
                .unwrap(),
        ])
        .unwrap();
        let recovered = local.merge(&required).unwrap();
        assert!(recovered.covers(&local));
        assert!(recovered.covers(&required));
        assert!(recovered.excludes(StorageIdentityHistoryKind::Object, target));
        assert_eq!(recovered.revision().value(), 3);
    }
    #[test]
    fn journal_refuses_to_rollback_a_retained_original_removal() {
        let intent = intent();
        let target = intent.manifest().members()[0].target();
        let journal = HistoryExpiryJournal::new(vec![
            intent
                .record_removed(
                    HistoryExpiryExactDeletion::from_affected_rows(
                        target,
                        checkpoint('a'),
                        at(210),
                        1,
                    )
                    .unwrap(),
                )
                .unwrap(),
        ])
        .unwrap();
        assert!(journal.retain(intent).is_err());
    }
    #[test]
    fn journal_refuses_a_second_intent_for_the_same_exact_copy() {
        let intent = intent();
        let journal = HistoryExpiryJournal::new(vec![intent.clone()]).unwrap();
        let mut encoded = serde_json::to_value(intent.manifest()).unwrap();
        encoded["identity"] = serde_json::json!(Uuid::new_v4());
        let overlapping: HistoryExpiryManifest = serde_json::from_value(encoded).unwrap();
        assert!(journal.admit(overlapping).is_err());
    }
    #[test]
    fn online_audit_replay_preserves_scoped_counts_identity_and_original_clocks() {
        let original = intent();
        let target = original.manifest().members()[0].target();
        let original = original
            .record_removed(
                HistoryExpiryExactDeletion::from_affected_rows(target, checkpoint('a'), at(210), 1)
                    .unwrap(),
            )
            .unwrap();
        let journal = HistoryExpiryJournal::new(vec![original]).unwrap();
        let events = journal
            .audit_events_since(&HistoryExpiryJournal::default())
            .unwrap();
        let replayed = journal
            .audit_events_since(&HistoryExpiryJournal::default())
            .unwrap();
        assert!(!events.is_empty());
        assert_eq!(
            events
                .iter()
                .map(|event| (event.event_id(), event.occurred_at()))
                .collect::<Vec<_>>(),
            replayed
                .iter()
                .map(|event| (event.event_id(), event.occurred_at()))
                .collect::<Vec<_>>()
        );
        assert!(
            events
                .iter()
                .all(|event| event.metadata()["lifecycle"]["scope"]["kind"] == "collection")
        );
        assert!(
            events
                .iter()
                .all(|event| event.metadata()["lifecycle"]["counts"]["versions"] == 1)
        );
        assert!(
            events
                .iter()
                .all(|event| event.metadata()["lifecycle"]["evidence"]["membership"]["count"] == 1)
        );
        assert!(journal.audit_events_since(&journal).unwrap().is_empty());
    }
    #[test]
    fn authority_admission_retains_manifest_and_recoverable_scoped_audit() {
        let snapshot = snapshot();
        let manifest = manifest(&snapshot);
        let identity = manifest.identity();
        let admitted = snapshot
            .control()
            .admit_expiry(manifest, snapshot.fence())
            .unwrap();
        assert_eq!(
            admitted.next().authority().revision().value(),
            snapshot.control().authority().revision().value() + 1
        );
        assert!(
            admitted
                .next()
                .authority()
                .expiry()
                .intent(identity)
                .is_some()
        );
        let decoded = HistoryLifecycleControl::decode(&admitted.next().encode().unwrap()).unwrap();
        assert_eq!(decoded, *admitted.next());
        assert!(
            !decoded
                .authority()
                .audit_events_since(snapshot.control().authority())
                .unwrap()
                .is_empty()
        );
    }
    fn admitted_snapshot() -> (HistoryLifecycleSnapshot, Uuid, StorageRecordIdentity) {
        let before = snapshot();
        let manifest = manifest(&before);
        let identity = manifest.identity();
        let target = manifest.members()[0].target();
        let admitted = before
            .control()
            .admit_expiry(manifest, before.fence())
            .unwrap();
        (
            HistoryLifecycleSnapshot::new(
                admitted.next().clone(),
                before.fence().incarnation(),
                before.fence().deletion_revision(),
                at(210),
            )
            .unwrap(),
            identity,
            target,
        )
    }
    #[rstest]
    #[case::task_control(HistoryPublicationKind::TaskOutput, false, false, true)]
    #[case::unknown_read_proof(HistoryPublicationKind::TaskOutput, false, true, false)]
    #[case::assigned_task(HistoryPublicationKind::TaskOutput, true, false, false)]
    #[case::http(HistoryPublicationKind::HttpResponse, false, false, false)]
    #[case::backup(HistoryPublicationKind::Backup, false, false, false)]
    #[case::export(HistoryPublicationKind::Export, false, false, false)]
    #[case::archive(HistoryPublicationKind::ArchiveRead, false, false, false)]
    fn receipt_progress_keeps_original_control_owner_only(
        #[case] kind: HistoryPublicationKind,
        #[case] assigned: bool,
        #[case] unknown: bool,
        #[case] productive: bool,
    ) {
        use crate::{ApplicationFence, RuntimeRegistration, StorageExecutionPermit};
        let (before, identity, target) = admitted_snapshot();
        let runtime = RuntimeRegistration::generate_history_aware();
        let application = StorageExecutionPermit::new(
            Some(runtime.owner()),
            ApplicationFence::new(
                before.fence().incarnation(),
                before.fence().deletion_revision().value() as i64,
            )
            .unwrap(),
        );
        let mut registry = HistoryPublicationRegistry::default();
        let epoch = |snapshot: &HistoryLifecycleSnapshot| {
            if unknown {
                HistoryReadEpoch::new(
                    application.clone(),
                    snapshot.fence(),
                    HistoryPublicationGeneration::new(0).unwrap(),
                )
                .unwrap()
            } else {
                HistoryReadEpoch::for_publication(
                    kind,
                    application.clone(),
                    snapshot,
                    HistoryPublicationGeneration::new(0).unwrap(),
                )
                .unwrap()
            }
        };
        let owner = registry.admit(kind, epoch(&before)).unwrap();
        let original = owner.context();
        let child = assigned.then(|| registry.admit_producer(&original, &epoch(&before)).unwrap());
        let removal = before
            .control()
            .record_expiry_removal(
                identity,
                HistoryExpiryExactDeletion::from_affected_rows(target, checkpoint('a'), at(210), 1)
                    .unwrap(),
                before.fence(),
            )
            .unwrap();
        let after = HistoryLifecycleSnapshot::new(
            removal.next().clone(),
            before.fence().incarnation(),
            before.fence().deletion_revision(),
            at(220),
        )
        .unwrap();
        assert_ne!(before.fence(), after.fence());
        assert_eq!(
            before.control().authority().read_revision(),
            after.control().authority().read_revision()
        );
        assert_eq!(
            registry.check(&original, &epoch(&after)).is_ok(),
            productive
        );
        assert_eq!(owner.context(), original);
        assert!(registry.admit_producer(&original, &epoch(&after)).is_err());
        if let Some(child) = child {
            let mut completed = pin!(child.after_completed(ready(Ok(()))));
            let Poll::Ready(completed) =
                Future::poll(completed.as_mut(), &mut Context::from_waker(Waker::noop()))
            else {
                panic!("ready completion")
            };
            let completed = completed.unwrap();
            registry.settle_producer(&completed).unwrap();
            assert!(registry.check(&original, &epoch(&after)).is_err());
        }
    }

    #[test]
    fn removal_transition_recovers_original_receipt_and_audit() {
        let (before, identity, target) = admitted_snapshot();
        let removed = before
            .control()
            .record_expiry_removal(
                identity,
                HistoryExpiryExactDeletion::from_affected_rows(target, checkpoint('a'), at(210), 1)
                    .unwrap(),
                before.fence(),
            )
            .unwrap();
        assert_eq!(
            removed.next().authority().revision().value(),
            before.control().authority().revision().value() + 1
        );
        let decoded = HistoryLifecycleControl::decode(&removed.next().encode().unwrap()).unwrap();
        let recovery =
            HistoryLifecycleRecovery::new(before.clone(), decoded.authority().clone()).unwrap();
        let receipt = &recovery
            .recovered()
            .authority()
            .expiry()
            .intent(identity)
            .unwrap()
            .removals()[0];
        assert_eq!(receipt.target(), target);
        assert_eq!(receipt.removed_at(), at(210));
        assert_eq!(receipt.checkpoint(), &checkpoint('a'));
        let events = recovery
            .recovered()
            .audit_events_since(before.control())
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].occurred_at(), Some(at(210)));
        assert_eq!(
            events[0].metadata()["lifecycle"]["name"],
            "lifecycle.erasure.copy_removed"
        );
        assert_eq!(
            events[0].event_id(),
            removed.next().audit_events_since(before.control()).unwrap()[0].event_id()
        );
        assert!(
            recovery
                .recovered()
                .authority()
                .expiry()
                .excludes(StorageIdentityHistoryKind::Object, target)
        );
    }
    #[test]
    fn repeated_removal_does_not_advance_authority_or_emit_another_audit_fact() {
        let (before, identity, target) = admitted_snapshot();
        let removed = before
            .control()
            .record_expiry_removal(
                identity,
                HistoryExpiryExactDeletion::from_affected_rows(target, checkpoint('a'), at(210), 1)
                    .unwrap(),
                before.fence(),
            )
            .unwrap();
        let current = HistoryLifecycleSnapshot::new(
            removed.next().clone(),
            before.fence().incarnation(),
            before.fence().deletion_revision(),
            at(220),
        )
        .unwrap();
        let retried = current
            .control()
            .record_expiry_removal(
                identity,
                HistoryExpiryExactDeletion::from_affected_rows(target, checkpoint('b'), at(220), 1)
                    .unwrap(),
                current.fence(),
            )
            .unwrap();
        assert_eq!(retried.next(), current.control());
        assert!(
            retried
                .next()
                .audit_events_since(current.control())
                .unwrap()
                .is_empty()
        );
    }
    #[rstest]
    #[case::unknown_intent("intent")]
    #[case::different_version("target")]
    #[case::before_selection("clock")]
    #[case::stale_authority("fence")]
    fn removal_transition_rejects_unbound_outcomes(#[case] mismatch: &str) {
        let (before, mut identity, mut target) = admitted_snapshot();
        let mut removed_at = at(210);
        let mut fence = before.fence();
        match mismatch {
            "intent" => identity = Uuid::new_v4(),
            "target" => {
                target = StorageRecordIdentity::new(target.resource(), VersionIdentity::generate())
            }
            "clock" => removed_at = at(199),
            "fence" => {
                fence = HistoryReviewFence::new(
                    fence.incarnation(),
                    HistoryAuthorityRevision::new(0).unwrap(),
                    fence.deletion_revision(),
                )
                .unwrap()
            }
            _ => unreachable!(),
        }
        assert!(
            before
                .control()
                .record_expiry_removal(
                    identity,
                    HistoryExpiryExactDeletion::from_affected_rows(
                        target,
                        checkpoint('a'),
                        removed_at,
                        1
                    )
                    .unwrap(),
                    fence,
                )
                .is_err()
        );
    }
    #[test]
    fn a_hold_after_selection_invalidates_intent_admission() {
        let snapshot = snapshot();
        let manifest = manifest(&snapshot);
        let hold = HistoryRetentionHold::new(
            Uuid::new_v4(),
            key(),
            at(200),
            None,
            PrincipalIdentity::generate(),
        )
        .unwrap();
        let held = snapshot
            .control()
            .create_hold(hold, snapshot.fence())
            .unwrap();
        assert!(
            held.next()
                .admit_expiry(manifest, snapshot.fence())
                .is_err()
        );
    }
    #[test]
    fn cancelled_intent_recovery_preserves_protected_membership_and_original_fact() {
        let snapshot = snapshot();
        let manifest = manifest(&snapshot);
        let identity = manifest.identity();
        let target = manifest.members()[0].target();
        let admitted = snapshot
            .control()
            .admit_expiry(manifest, snapshot.fence())
            .unwrap();
        let admitted_snapshot = HistoryLifecycleSnapshot::new(
            admitted.next().clone(),
            snapshot.fence().incarnation(),
            snapshot.fence().deletion_revision(),
            at(210),
        )
        .unwrap();
        let cancelled = admitted_snapshot
            .control()
            .cancel_expiry(
                identity,
                PrincipalIdentity::generate(),
                admitted_snapshot.fence(),
                at(210),
            )
            .unwrap();
        let recovery =
            HistoryLifecycleRecovery::new(admitted_snapshot, cancelled.next().authority().clone())
                .unwrap();
        assert!(
            recovery
                .recovered()
                .authority()
                .expiry()
                .excludes(StorageIdentityHistoryKind::Object, target)
        );
        assert!(
            recovery
                .recovered()
                .authority()
                .covers(cancelled.next().authority())
        );
    }
    #[rstest::rstest]
    #[case(105, 90, true)]
    #[case(105, 101, false)]
    #[case(110, 90, false)]
    fn retained_as_of_gap_does_not_resurrect_an_older_value_or_hide_a_newer_descendant(
        #[case] query_at: i64,
        #[case] selected_from: i64,
        #[case] restricted: bool,
    ) {
        let intent = intent();
        let resource = intent.manifest().members()[0].target().resource();
        let journal = HistoryExpiryJournal::new(vec![intent]).unwrap();
        assert_eq!(
            journal
                .restriction_as_of(
                    StorageIdentityHistoryKind::Object,
                    resource,
                    at(query_at),
                    Some(at(selected_from))
                )
                .is_some(),
            restricted
        );
    }
    #[test]
    fn schema_event_span_keeps_event_time_without_inventing_a_closure() {
        let span = HistoryExpiryReadSpan::schema(at(100), None).unwrap();
        let decoded: HistoryExpiryReadSpan =
            serde_json::from_slice(&serde_json::to_vec(&span).unwrap()).unwrap();
        assert!(decoded.matches(
            StorageIdentityHistoryKind::ClassSchema,
            HistoryRetentionAnchor::SchemaEvent(at(100))
        ));
        assert!(decoded.contains(at(200)));
    }
    #[test]
    fn recovered_expiry_cannot_reference_a_policy_absent_from_retained_catalog() {
        let snapshot = snapshot();
        let admitted = snapshot
            .control()
            .admit_expiry(manifest(&snapshot), snapshot.fence())
            .unwrap();
        let mut encoded = serde_json::to_value(admitted.next()).unwrap();
        encoded["authority"]["expiry"][0]["manifest"]["members"][0]["policies"][0]["revision"] =
            serde_json::json!("9999");
        assert!(serde_json::from_value::<HistoryLifecycleControl>(encoded).is_err());
    }
    #[rstest::rstest]
    #[case(true)]
    #[case(false)]
    fn retained_intent_attribution_keeps_deleted_parent_scope_governable(#[case] known: bool) {
        let snapshot = snapshot();
        let manifest = manifest(&snapshot);
        let collection = manifest.members()[0].attributions()[0].collection();
        let admitted = snapshot
            .control()
            .admit_expiry(manifest, snapshot.fence())
            .unwrap();
        let identity = if known {
            collection
        } else {
            ResourceIdentity::generate()
        };
        assert_eq!(
            admitted
                .next()
                .retains_scope(HistoryPolicyScope::Collection { identity }),
            known
        );
    }
}
