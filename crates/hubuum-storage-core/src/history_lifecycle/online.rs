//! Exact online selection material. Selection is neither a protected intent nor
//! physical cleanup authority; adapters must recheck its fence and protections.

use std::collections::BTreeSet;

use uuid::Uuid;

use super::*;
use crate::StorageRecordIdentity;

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
            (HistoryExpiryReadSpanKind::Deleted, None) => Ok(Self::deleted(value.valid_from)),
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
        at >= self.valid_from && self.valid_to.is_none_or(|until| at < until)
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
    pub const fn input_bytes(&self) -> HistoryExpiryInputBytes {
        self.input_bytes
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
    },
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
    BudgetReached,
}

/// The adapter measures native full-row input and evaluates the current source;
/// this builder preserves those validated facts across the admission boundary.
pub struct HistoryExpirySelection<'a> {
    snapshot: &'a HistoryLifecycleSnapshot,
    scan: HistoryLifecycleScan,
    manifest: HistoryExpiryManifest,
    identities: BTreeSet<([u8; 16], [u8; 16])>,
}
impl<'a> HistoryExpirySelection<'a> {
    pub fn new(
        run: Uuid,
        origin: HistoryExpiryOrigin,
        scan: HistoryLifecycleScan,
        snapshot: &'a HistoryLifecycleSnapshot,
    ) -> Result<Self, StorageValidationError> {
        if run.get_variant() != uuid::Variant::RFC4122 || run.get_version().is_none() {
            return Err(invalid("History expiry run requires an RFC UUID"));
        }
        Ok(Self {
            snapshot,
            manifest: HistoryExpiryManifest {
                identity: Uuid::new_v4(),
                run,
                origin,
                key: scan.key(),
                fence: snapshot.fence(),
                selected_at: snapshot.observed_at(),
                input_bytes: 0,
                members: Vec::new(),
            },
            scan,
            identities: BTreeSet::new(),
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
        self.identities.insert(identity);
        self.manifest.input_bytes += input_bytes.value();
        self.manifest.members.push(HistoryExpiryMember {
            family: candidate.family(),
            target: candidate.target(),
            anchor,
            read_span,
            attributions: candidate.attributions().to_vec(),
            policies,
            input_bytes,
        });
        Ok(HistoryExpirySelectionOutcome::Selected)
    }

    pub fn finish(self) -> Result<HistoryExpiryManifest, StorageValidationError> {
        self.manifest.validate()?;
        Ok(self.manifest)
    }
}

#[cfg(test)]
mod tests {
    use hubuum_domain::{IdentityReference, VersionIdentity};
    use rstest::rstest;

    use super::*;
    use crate::StorageRecordIdentity;

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
            .record_removed(target, checkpoint('a'), at(210))
            .unwrap();
        let replayed = removed
            .record_removed(target, checkpoint('b'), at(220))
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
            .record_removed(first, checkpoint('a'), at(210))
            .unwrap();
        let required = intent
            .record_removed(second, checkpoint('b'), at(220))
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
            .record_removed(target, checkpoint('a'), at(210))
            .unwrap();
        let contradictory = intent
            .record_removed(target, checkpoint('b'), at(220))
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
            .record_removed(target, checkpoint('a'), at(210))
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
            .record_removed(target, checkpoint('a'), at(210))
            .unwrap();
        let mut encoded = serde_json::to_value(retained).unwrap();
        let receipt = encoded["removals"][0].clone();
        encoded["removals"].as_array_mut().unwrap().push(receipt);
        assert!(serde_json::from_value::<HistoryExpiryIntent>(encoded).is_err());
    }
    #[test]
    fn journal_recovery_merges_descendants_without_losing_local_obligations() {
        let intent = intent();
        let target = intent.manifest().members()[0].target();
        let local = HistoryExpiryJournal::new(vec![
            intent
                .record_removed(target, checkpoint('a'), at(210))
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
                .record_removed(target, checkpoint('a'), at(210))
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
            .record_removed(target, checkpoint('a'), at(210))
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
