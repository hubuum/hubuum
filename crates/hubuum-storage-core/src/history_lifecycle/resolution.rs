use super::*;
use crate::StorageRecordIdentity;

/// Immutable policy history. Records are globally ordered but resolution checks
/// original anchors on every read, so late commits are covered without a cursor.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    try_from = "Vec<HistoryPolicyRecord>",
    into = "Vec<HistoryPolicyRecord>"
)]
pub struct HistoryPolicyCatalog(Vec<HistoryPolicyRecord>);
impl HistoryPolicyCatalog {
    pub const MAX_REVISIONS: usize = 4096;
    pub fn new(records: Vec<HistoryPolicyRecord>) -> Result<Self, StorageValidationError> {
        if records.len() > Self::MAX_REVISIONS {
            return Err(invalid("History policy revision capacity exhausted"));
        }
        let mut keys = std::collections::BTreeMap::new();
        let mut operations = std::collections::BTreeSet::new();
        let mut previous = None;
        for record in &records {
            if !operations.insert(record.operation()) {
                return Err(invalid("Duplicate policy operation identity"));
            }
            if previous.is_some_and(|old: &HistoryPolicyRecord| {
                old.revision() >= record.revision() || old.applied_at() > record.applied_at()
            }) {
                return Err(invalid(
                    "Policy revisions must increase and application time cannot move backwards",
                ));
            }
            let expected = keys
                .get(&record.proposal().key())
                .copied()
                .unwrap_or(HistoryPolicyRevision::new(0)?);
            if record.proposal().expected_policy_revision() != expected {
                return Err(invalid(
                    "Policy history contains an invalid revision predecessor",
                ));
            }
            keys.insert(record.proposal().key(), record.revision());
            previous = Some(record);
        }
        Ok(Self(records))
    }
    pub fn records(&self) -> &[HistoryPolicyRecord] {
        &self.0
    }
    pub fn exact(
        &self,
        reference: &HistoryPolicyReference,
    ) -> Result<HistoryEffectivePolicy, StorageValidationError> {
        let record = self
            .0
            .iter()
            .find(|record| HistoryPolicyReference::from(*record) == *reference)
            .ok_or_else(|| invalid("Unknown immutable history policy revision"))?;
        Ok(HistoryEffectivePolicy {
            source: Some(reference.clone()),
            inherited: Vec::new(),
            binding: record.proposal().binding().clone(),
        })
    }
    pub fn current_revision(&self, key: HistoryPolicyKey) -> HistoryPolicyRevision {
        self.0
            .iter()
            .rev()
            .find(|record| record.proposal().key() == key)
            .map(HistoryPolicyRecord::revision)
            .unwrap_or(HistoryPolicyRevision(0))
    }
    pub fn append(
        &self,
        proposal: HistoryPolicyChange,
        principal: PrincipalIdentity,
        now: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        if proposal.expected_policy_revision() != self.current_revision(proposal.key()) {
            return Err(invalid(
                "Policy revision changed; review the current binding",
            ));
        }
        let revision = self
            .0
            .last()
            .map_or(0, |record| record.revision().value())
            .checked_add(1)
            .ok_or_else(|| invalid("History policy revision exhausted"))?;
        let mut records = self.0.clone();
        records.push(HistoryPolicyRecord::new(
            HistoryPolicyRevision::new(revision)?,
            proposal,
            now,
            principal,
        )?);
        Self::new(records)
    }
    pub(super) fn bind_last_review(
        mut self,
        review: HistoryPolicyReviewEvidence,
    ) -> Result<Self, StorageValidationError> {
        let record = self
            .0
            .pop()
            .ok_or_else(|| invalid("Review requires an applied policy"))?;
        self.0.push(record.bind_review(review)?);
        Ok(self)
    }
    pub fn resolve(
        &self,
        family: StorageIdentityHistoryKind,
        attribution: HistoryScopeAttribution,
        anchor: HistoryRetentionAnchor,
    ) -> HistoryEffectivePolicy {
        self.resolve_at(family, attribution, anchor.at())
    }
    /// Accounting includes open versions without manufacturing a closure proof.
    pub fn resolve_accounting(
        &self,
        family: StorageIdentityHistoryKind,
        attribution: HistoryScopeAttribution,
        anchor: Option<HistoryRetentionAnchor>,
        observed_at: DateTime<Utc>,
    ) -> HistoryEffectivePolicy {
        self.resolve_at(
            family,
            attribution,
            anchor.map_or(observed_at, HistoryRetentionAnchor::at),
        )
    }
    fn resolve_at(
        &self,
        family: StorageIdentityHistoryKind,
        attribution: HistoryScopeAttribution,
        at: DateTime<Utc>,
    ) -> HistoryEffectivePolicy {
        let scopes = attribution
            .class()
            .map(|identity| HistoryPolicyScope::Class { identity })
            .into_iter()
            .chain([
                HistoryPolicyScope::Collection {
                    identity: attribution.collection(),
                },
                HistoryPolicyScope::Deployment,
            ]);
        let mut inherited = Vec::new();
        for scope in scopes {
            let record = self.0.iter().rev().find(|record| {
                record.proposal().key().family() == family
                    && record.proposal().key().scope() == scope
                    && record.governs_at(at)
            });
            if let Some(record) = record {
                if *record.proposal().binding() == HistoryPolicyBinding::Inherit {
                    inherited.push(HistoryPolicyReference::from(record));
                    continue;
                }
                return HistoryEffectivePolicy {
                    source: Some(HistoryPolicyReference::from(record)),
                    inherited,
                    binding: record.proposal().binding().clone(),
                };
            }
        }
        HistoryEffectivePolicy {
            source: None,
            inherited,
            binding: HistoryPolicyBinding::DisabledExpiry,
        }
    }
}
impl TryFrom<Vec<HistoryPolicyRecord>> for HistoryPolicyCatalog {
    type Error = StorageValidationError;
    fn try_from(value: Vec<HistoryPolicyRecord>) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<HistoryPolicyCatalog> for Vec<HistoryPolicyRecord> {
    fn from(value: HistoryPolicyCatalog) -> Self {
        value.0
    }
}

#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryPolicyReference {
    key: HistoryPolicyKey,
    revision: HistoryPolicyRevision,
}
impl HistoryPolicyReference {
    pub const fn key(&self) -> HistoryPolicyKey {
        self.key
    }
    pub const fn revision(&self) -> HistoryPolicyRevision {
        self.revision
    }
}
impl From<&HistoryPolicyRecord> for HistoryPolicyReference {
    fn from(value: &HistoryPolicyRecord) -> Self {
        Self {
            key: value.proposal().key(),
            revision: value.revision(),
        }
    }
}
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryEffectivePolicy {
    source: Option<HistoryPolicyReference>,
    inherited: Vec<HistoryPolicyReference>,
    binding: HistoryPolicyBinding,
}
impl HistoryEffectivePolicy {
    pub const fn source(&self) -> Option<&HistoryPolicyReference> {
        self.source.as_ref()
    }
    pub fn inherited(&self) -> &[HistoryPolicyReference] {
        &self.inherited
    }
    pub const fn binding(&self) -> &HistoryPolicyBinding {
        &self.binding
    }
    pub fn inspect(
        &self,
        anchor: HistoryRetentionAnchor,
        now: DateTime<Utc>,
        size: Option<&HistorySizePressure>,
    ) -> Result<HistoryEndpointDecision, StorageValidationError> {
        let mut decision = HistoryEndpointDecision {
            policy: self.clone(),
            deadline: None,
            minimum_until: None,
            blocker: None,
        };
        match &self.binding {
            HistoryPolicyBinding::Inherit | HistoryPolicyBinding::DisabledExpiry => {
                decision.blocker = Some(HistoryExpiryBlocker::ExpiryDisabled)
            }
            HistoryPolicyBinding::Indefinite => {
                decision.blocker = Some(HistoryExpiryBlocker::IndefiniteRetention)
            }
            HistoryPolicyBinding::Expire {
                trigger,
                minimum_age_seconds,
            } => {
                decision.minimum_until = Some(policy::deadline(anchor.at(), *minimum_age_seconds)?);
                decision.deadline = trigger.age().map(|age| age.deadline(anchor)).transpose()?;
                if decision.minimum_until.is_some_and(|at| at > now) {
                    decision.blocker = Some(HistoryExpiryBlocker::ProtectedMinimum);
                } else {
                    let age_due = decision.deadline.is_some_and(|at| at <= now);
                    let needs_size = match trigger {
                        HistoryExpiryTrigger::Age { .. } => false,
                        HistoryExpiryTrigger::LogicalSize { .. } => true,
                        HistoryExpiryTrigger::AgeOrSize { .. } => !age_due,
                        HistoryExpiryTrigger::AgeAndSize { .. } => age_due,
                    };
                    let size_due = if let Some(rule) = trigger.size().filter(|_| needs_size) {
                        let Some(pressure) =
                            size.filter(|pressure| self.source.as_ref() == Some(pressure.policy()))
                        else {
                            decision.blocker = Some(HistoryExpiryBlocker::AccountingUnavailable);
                            return Ok(decision);
                        };
                        pressure.reclaiming() && pressure.measured_bytes() > rule.lower()
                    } else {
                        false
                    };
                    let due = match trigger {
                        HistoryExpiryTrigger::Age { .. } => age_due,
                        HistoryExpiryTrigger::LogicalSize { .. } => size_due,
                        HistoryExpiryTrigger::AgeOrSize { .. } => age_due || size_due,
                        HistoryExpiryTrigger::AgeAndSize { .. } => age_due && size_due,
                    };
                    if !due {
                        decision.blocker = Some(HistoryExpiryBlocker::TriggerNotReached);
                    }
                }
            }
        }
        Ok(decision)
    }
}

/// A complete logical-byte measurement for one exact effective policy. The
/// adapter must measure the full governed set; bounded preview samples cannot
/// establish this value. Hysteresis persists until the lower target is reached.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "PressureInput")]
pub struct HistorySizePressure {
    policy: HistoryPolicyReference,
    measured_bytes: HistoryLogicalBytes,
    reclaiming: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PressureInput {
    policy: HistoryPolicyReference,
    measured_bytes: HistoryLogicalBytes,
    reclaiming: bool,
}
impl HistorySizePressure {
    pub fn measure(
        policy: &HistoryEffectivePolicy,
        bytes: HistoryLogicalBytes,
        previous: Option<&Self>,
    ) -> Result<Self, StorageValidationError> {
        let Some(source) = policy.source() else {
            return Err(invalid("Size accounting needs a configured policy"));
        };
        let HistoryPolicyBinding::Expire { trigger, .. } = policy.binding() else {
            return Err(invalid("Size accounting needs an expiry rule"));
        };
        let rule = trigger
            .size()
            .ok_or_else(|| invalid("Policy has no logical-size trigger"))?;
        let was_reclaiming = previous.is_some_and(|old| old.policy() == source && old.reclaiming());
        Ok(Self {
            policy: source.clone(),
            measured_bytes: bytes,
            reclaiming: bytes > rule.upper() || (was_reclaiming && bytes > rule.lower()),
        })
    }
    pub const fn policy(&self) -> &HistoryPolicyReference {
        &self.policy
    }
    pub const fn measured_bytes(&self) -> HistoryLogicalBytes {
        self.measured_bytes
    }
    pub const fn reclaiming(&self) -> bool {
        self.reclaiming
    }
}
impl TryFrom<PressureInput> for HistorySizePressure {
    type Error = StorageValidationError;
    fn try_from(value: PressureInput) -> Result<Self, Self::Error> {
        if value.policy.revision().value() == 0 {
            return Err(invalid("Size accounting needs a positive policy revision"));
        }
        Ok(Self {
            policy: value.policy,
            measured_bytes: value.measured_bytes,
            reclaiming: value.reclaiming,
        })
    }
}

#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryExpiryBlocker {
    OpenVersion,
    UnknownAttribution,
    RetainedDependency,
    Hold,
    ExpiryDisabled,
    IndefiniteRetention,
    ProtectedMinimum,
    AccountingUnavailable,
    TriggerNotReached,
}
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryEndpointDecision {
    policy: HistoryEffectivePolicy,
    #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, format = DateTime))]
    deadline: Option<DateTime<Utc>>,
    #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, format = DateTime))]
    minimum_until: Option<DateTime<Utc>>,
    blocker: Option<HistoryExpiryBlocker>,
}
impl HistoryEndpointDecision {
    pub const fn policy(&self) -> &HistoryEffectivePolicy {
        &self.policy
    }
    pub const fn deadline(&self) -> Option<DateTime<Utc>> {
        self.deadline
    }
    pub const fn minimum_until(&self) -> Option<DateTime<Utc>> {
        self.minimum_until
    }
    pub const fn blocker(&self) -> Option<HistoryExpiryBlocker> {
        self.blocker
    }
}

/// Adapter-captured facts, with one attribution per endpoint for relations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryExpiryCandidate {
    family: StorageIdentityHistoryKind,
    target: StorageRecordIdentity,
    anchor: Option<HistoryRetentionAnchor>,
    attributions: Vec<HistoryScopeAttribution>,
    has_dependency: bool,
    read_span: Option<HistoryExpiryReadSpan>,
    object_authorization: Option<HistoryObjectAuthorizationFacts>,
}
impl HistoryExpiryCandidate {
    pub fn new(
        family: StorageIdentityHistoryKind,
        target: StorageRecordIdentity,
        anchor: Option<HistoryRetentionAnchor>,
        attributions: Vec<HistoryScopeAttribution>,
        has_dependency: bool,
    ) -> Result<Self, StorageValidationError> {
        let expected = if matches!(
            family,
            StorageIdentityHistoryKind::ClassRelation | StorageIdentityHistoryKind::ObjectRelation
        ) {
            2
        } else {
            1
        };
        if !attributions.is_empty() && attributions.len() != expected {
            return Err(invalid(
                "Historical attribution must cover every endpoint or remain explicitly unresolved",
            ));
        }
        Ok(Self {
            family,
            target,
            anchor,
            attributions,
            has_dependency,
            read_span: None,
            object_authorization: None,
        })
    }
    pub fn with_read_span(
        mut self,
        span: HistoryExpiryReadSpan,
    ) -> Result<Self, StorageValidationError> {
        if self
            .anchor
            .is_none_or(|anchor| !span.matches(self.family, anchor))
        {
            return Err(invalid(
                "Native expiry read span differs from its original anchor",
            ));
        }
        if self
            .object_authorization
            .as_ref()
            .is_some_and(|facts| facts.read_span() != &span)
        {
            return Err(invalid(
                "Native interval cannot retarget original authorization facts",
            ));
        }
        self.read_span = Some(span);
        Ok(self)
    }
    /// Preserve the native interval before payload removal. Open versions remain
    /// unclaimable; validation never supplies a replacement closure clock.
    pub fn with_native_interval(
        self,
        from: DateTime<Utc>,
        until: Option<DateTime<Utc>>,
    ) -> Result<Self, StorageValidationError> {
        let span = match self.anchor {
            None if until.is_none() => return Ok(self),
            None => return Err(invalid("An open version cannot carry a closure interval")),
            Some(HistoryRetentionAnchor::Closed(at)) => {
                if until != Some(at) {
                    return Err(invalid("Native closure differs from its retention anchor"));
                }
                HistoryExpiryReadSpan::closed(from, at)?
            }
            Some(HistoryRetentionAnchor::Deleted(_)) => {
                HistoryExpiryReadSpan::tombstone(from, until)?
            }
            Some(HistoryRetentionAnchor::SchemaEvent(_)) => {
                HistoryExpiryReadSpan::schema(from, until)?
            }
        };
        self.with_read_span(span)
    }
    pub fn with_object_authorization(
        mut self,
        facts: HistoryObjectAuthorizationFacts,
    ) -> Result<Self, StorageValidationError> {
        if self.family != StorageIdentityHistoryKind::Object
            || self.target != facts.target()
            || self.read_span.as_ref() != Some(facts.read_span())
            || self.attributions.as_slice() != [facts.attribution()]
        {
            return Err(invalid(
                "Object authorization differs from exact expiry selection",
            ));
        }
        if self
            .object_authorization
            .as_ref()
            .is_some_and(|original| original != &facts)
        {
            return Err(invalid(
                "Original object authorization facts cannot be replaced",
            ));
        }
        self.object_authorization = Some(facts);
        Ok(self)
    }
    pub const fn object_authorization(&self) -> Option<&HistoryObjectAuthorizationFacts> {
        self.object_authorization.as_ref()
    }
    pub const fn read_span(&self) -> Option<&HistoryExpiryReadSpan> {
        self.read_span.as_ref()
    }
    pub const fn family(&self) -> StorageIdentityHistoryKind {
        self.family
    }
    pub const fn target(&self) -> StorageRecordIdentity {
        self.target
    }
    pub const fn anchor(&self) -> Option<HistoryRetentionAnchor> {
        self.anchor
    }
    pub fn attributions(&self) -> &[HistoryScopeAttribution] {
        &self.attributions
    }
    pub fn inspect(
        &self,
        catalog: &HistoryPolicyCatalog,
        now: DateTime<Utc>,
        held: bool,
        measurements: &[HistorySizePressure],
    ) -> Result<HistoryExpiryDecision, StorageValidationError> {
        let mut decision = HistoryExpiryDecision {
            target: self.target,
            anchor: self.anchor,
            endpoints: Vec::new(),
            blocker: None,
        };
        let Some(anchor) = self.anchor else {
            decision.blocker = Some(HistoryExpiryBlocker::OpenVersion);
            return Ok(decision);
        };
        if self.attributions.is_empty() {
            decision.blocker = Some(HistoryExpiryBlocker::UnknownAttribution);
        } else if self.has_dependency {
            decision.blocker = Some(HistoryExpiryBlocker::RetainedDependency);
        } else if held {
            decision.blocker = Some(HistoryExpiryBlocker::Hold);
        }
        for attribution in &self.attributions {
            let policy = catalog.resolve(self.family, *attribution, anchor);
            let measurement = measurements
                .iter()
                .find(|measurement| policy.source() == Some(measurement.policy()));
            let endpoint = policy.inspect(anchor, now, measurement)?;
            if decision.blocker.is_none() {
                decision.blocker = endpoint.blocker();
            }
            decision.endpoints.push(endpoint);
        }
        Ok(decision)
    }
}

#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryExpiryDecision {
    target: StorageRecordIdentity,
    anchor: Option<HistoryRetentionAnchor>,
    endpoints: Vec<HistoryEndpointDecision>,
    blocker: Option<HistoryExpiryBlocker>,
}
impl HistoryExpiryDecision {
    pub const fn target(&self) -> StorageRecordIdentity {
        self.target
    }
    pub const fn anchor(&self) -> Option<HistoryRetentionAnchor> {
        self.anchor
    }
    pub fn endpoints(&self) -> &[HistoryEndpointDecision] {
        &self.endpoints
    }
    pub const fn blocker(&self) -> Option<HistoryExpiryBlocker> {
        self.blocker
    }
    pub const fn eligible(&self) -> bool {
        self.blocker.is_none()
    }
}
