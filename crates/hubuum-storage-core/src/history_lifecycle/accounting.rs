//! Versioned logical accounting, independent of physical/compressed storage.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::time::{Duration, Instant};

use bigdecimal::BigDecimal;
use hubuum_domain::VersionIdentity;
use serde_json::Value;

use super::*;

impl HistoryLogicalBytes {
    /// Compact UTF-8 JSON, with normalized plain decimal numbers (no exponent
    /// or insignificant zeros). Object ordering does not affect its byte count.
    /// This measures the complete retained record, before compression.
    pub fn measure_fields_json_v1(
        value: &serde_json::Map<String, Value>,
    ) -> Result<Self, StorageValidationError> {
        Self::new(object_bytes(value, 0)?)
    }
    pub fn measure_json_v1(value: &Value) -> Result<Self, StorageValidationError> {
        Self::new(json_bytes(value, 0)?)
    }
}

fn add(left: u64, right: u64) -> Result<u64, StorageValidationError> {
    left.checked_add(right)
        .filter(|total| *total <= i64::MAX as u64)
        .ok_or_else(|| invalid("Logical history bytes exceed their supported range"))
}

fn string_bytes(value: &str) -> Result<u64, StorageValidationError> {
    value.bytes().try_fold(2, |count, byte| {
        add(
            count,
            match byte {
                b'"' | b'\\' | b'\n' | b'\r' | b'\t' | 8 | 12 => 2,
                0..=31 => 6,
                _ => 1,
            },
        )
    })
}

fn json_bytes(value: &Value, depth: u16) -> Result<u64, StorageValidationError> {
    if depth > 128 {
        return Err(invalid("Logical history JSON exceeds the v1 depth bound"));
    }
    match value {
        Value::Null => Ok(4),
        Value::Bool(value) => Ok(if *value { 4 } else { 5 }),
        Value::String(value) => string_bytes(value),
        Value::Number(value) => {
            let decimal = BigDecimal::from_str(&value.to_string())
                .map_err(|_| invalid("Logical history number exceeds the v1 format"))?;
            let (coefficient, scale) = decimal.into_bigint_and_scale();
            let coefficient = coefficient.to_string();
            if coefficient == "0" {
                return Ok(1);
            }
            let sign = u64::from(coefficient.starts_with('-'));
            let magnitude = coefficient.trim_start_matches('-');
            let significant = magnitude.trim_end_matches('0');
            let trailing_zeros = (magnitude.len() - significant.len()) as i64;
            let scale = scale
                .checked_sub(trailing_zeros)
                .ok_or_else(|| invalid("Logical history bytes exceed their supported range"))?;
            let digits = significant.len() as u64;
            // Count expansion without allocating an exponent-sized string.
            let unsigned = if scale <= 0 {
                add(digits, scale.unsigned_abs())?
            } else if (scale as u64) < digits {
                add(digits, 1)?
            } else {
                add(2, scale as u64)?
            };
            add(sign, unsigned)
        }
        Value::Array(values) => values.iter().try_fold(
            add(2, values.len().saturating_sub(1) as u64)?,
            |count, value| add(count, json_bytes(value, depth + 1)?),
        ),
        Value::Object(values) => object_bytes(values, depth),
    }
}

fn object_bytes(
    values: &serde_json::Map<String, Value>,
    depth: u16,
) -> Result<u64, StorageValidationError> {
    values.iter().try_fold(
        add(2, values.len().saturating_sub(1) as u64)?,
        |count, (key, value)| {
            add(
                add(add(count, string_bytes(key)?)?, 1)?,
                json_bytes(value, depth + 1)?,
            )
        },
    )
}

#[cfg(test)]
mod tests;

/// Complete bounded census for one immutable size-bearing policy. This proof is
/// produced only after the adapter exhausts the family source in one snapshot.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Serialize)]
pub struct HistorySizeAccounting {
    format: &'static str,
    fence: HistoryReviewFence,
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = DateTime))]
    observed_at: DateTime<Utc>,
    scanned_rows: String,
    governed_rows: String,
    pressure: HistorySizePressure,
    #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, format = "uuid"))]
    transition: Option<uuid::Uuid>,
}
impl HistorySizeAccounting {
    pub const fn pressure(&self) -> &HistorySizePressure {
        &self.pressure
    }
    pub const fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
    }
    pub const fn fence(&self) -> HistoryReviewFence {
        self.fence
    }
    pub const fn transition(&self) -> Option<uuid::Uuid> {
        self.transition
    }
}

/// Adapter-owned fold. Never finalize a page, timeout or truncated prefix.
/// ponytail: complete scans are capped at 100,000 rows / 64 MiB metadata;
/// replace with transactionally maintained counters when larger scopes need it.
pub struct HistorySizeCensus {
    snapshot: HistoryLifecycleSnapshot,
    family: StorageIdentityHistoryKind,
    policies: Vec<(HistoryEffectivePolicy, u64, u64)>,
    policy_indices: BTreeMap<(HistoryPolicyKey, HistoryPolicyRevision), usize>,
    pending: BTreeSet<(ResourceIdentity, VersionIdentity)>,
    reserved: BTreeMap<(HistoryPolicyKey, HistoryPolicyRevision), u64>,
    seen: BTreeMap<(ResourceIdentity, VersionIdentity), HistoryLogicalBytes>,
    metadata_bytes: u64,
    failed: bool,
    started: Instant,
}
impl HistorySizeCensus {
    pub const MAX_ROWS: usize = 100_000;
    pub const MAX_METADATA_BYTES: u64 = 64 * 1024 * 1024;
    pub fn new(
        snapshot: HistoryLifecycleSnapshot,
        reference: &HistoryPolicyReference,
    ) -> Result<Self, StorageValidationError> {
        let started = Instant::now();
        let policy = snapshot.control().authority().policies().exact(reference)?;
        HistorySizePressure::measure(&policy, HistoryLogicalBytes::new(0)?, None)?;
        let family = reference.key().family();
        Ok(Self::with_policies(snapshot, family, vec![policy], started))
    }
    /// One complete family fold shares identity and byte budgets across all
    /// immutable size-bearing sources, including distinct relation endpoints.
    pub fn for_family(
        snapshot: HistoryLifecycleSnapshot,
        family: StorageIdentityHistoryKind,
    ) -> Result<Self, StorageValidationError> {
        let started = Instant::now();
        let policies = snapshot.control().authority().policies().records().iter()
            .filter(|record| record.proposal().key().family() == family)
            .filter(|record| matches!(record.proposal().binding(), HistoryPolicyBinding::Expire { trigger, .. } if trigger.size().is_some()))
            .map(HistoryEffectivePolicy::from_record)
            .collect();
        Ok(Self::with_policies(snapshot, family, policies, started))
    }
    fn with_policies(
        snapshot: HistoryLifecycleSnapshot,
        family: StorageIdentityHistoryKind,
        policies: Vec<HistoryEffectivePolicy>,
        started: Instant,
    ) -> Self {
        let policy_indices = policies
            .iter()
            .enumerate()
            .map(|(index, policy)| {
                let source = policy.source().expect("exact accounting policy");
                ((source.key(), source.revision()), index)
            })
            .collect();
        let mut pending = BTreeSet::new();
        for intent in snapshot.control().authority().expiry().intents() {
            for member in intent.manifest().members() {
                let target = member.target();
                if member.family() == family && intent.receipt(target).is_none() {
                    pending.insert((target.resource(), target.version()));
                }
            }
        }
        Self {
            snapshot,
            family,
            policies: policies.into_iter().map(|policy| (policy, 0, 0)).collect(),
            policy_indices,
            pending,
            reserved: Default::default(),
            seen: Default::default(),
            metadata_bytes: 0,
            failed: false,
            started,
        }
    }
    pub fn observe(
        &mut self,
        candidate: Option<&HistoryExpiryCandidate>,
        bytes: HistoryLogicalBytes,
        metadata_bytes: u64,
    ) -> Result<(), StorageValidationError> {
        let result = self.observe_row(candidate, bytes, metadata_bytes);
        self.failed |= result.is_err();
        result
    }
    fn observe_row(
        &mut self,
        candidate: Option<&HistoryExpiryCandidate>,
        bytes: HistoryLogicalBytes,
        metadata_bytes: u64,
    ) -> Result<(), StorageValidationError> {
        if self.started.elapsed() > Duration::from_secs(5)
            || self.seen.len() == Self::MAX_ROWS
            || metadata_bytes > HistoryLifecycleScan::MAX_INPUT_BYTES as u64
        {
            return Err(invalid(
                "Complete history accounting exceeds its time, row or metadata budget",
            ));
        }
        self.metadata_bytes = add(self.metadata_bytes, metadata_bytes)?;
        if self.metadata_bytes > Self::MAX_METADATA_BYTES {
            return Err(invalid(
                "Complete history accounting exceeds its metadata budget",
            ));
        }
        let candidate = candidate
            .filter(|candidate| {
                candidate.family() == self.family && !candidate.attributions().is_empty()
            })
            .ok_or_else(|| {
                invalid("Complete history accounting has unresolved historical attribution")
            })?;
        let identity = (candidate.target().resource(), candidate.target().version());
        if self.seen.insert(identity, bytes).is_some() {
            return Err(invalid(
                "Complete history accounting has duplicate version identities",
            ));
        }
        // Both relation endpoints can resolve to the same source: charge the
        // exact logical version once, independently of protection/eligibility.
        let sources = candidate
            .attributions()
            .iter()
            .filter_map(|attribution| {
                self.snapshot
                    .control()
                    .authority()
                    .policies()
                    .resolve_accounting(
                        candidate.family(),
                        *attribution,
                        candidate.anchor(),
                        self.snapshot.observed_at(),
                    )
                    .source()
                    .map(|source| (source.key(), source.revision()))
            })
            .collect::<BTreeSet<_>>();
        let pending = self.pending.remove(&identity);
        for source in sources {
            if let Some(index) = self.policy_indices.get(&source) {
                let (_, governed_rows, total) = &mut self.policies[*index];
                *governed_rows += 1;
                *total = add(*total, bytes.value())?;
                if pending {
                    // Reserve exactly the bytes this census charged to this source.
                    *self.reserved.entry(source).or_default() += bytes.value();
                }
            }
        }
        Ok(())
    }
    /// The adapter must call this only after positively exhausting its source.
    pub fn finish(self) -> Result<HistorySizeAccounting, StorageValidationError> {
        if self.policies.len() != 1 {
            return Err(invalid(
                "A single-policy accounting result requires exactly one source",
            ));
        }
        Ok(self
            .finish_family()?
            .accounting
            .pop()
            .expect("one census source"))
    }
    pub fn finish_family(self) -> Result<HistoryFamilySizeAccounting, StorageValidationError> {
        if self.failed || self.started.elapsed() > Duration::from_secs(5) {
            return Err(invalid(
                "A failed or expired history census cannot produce complete accounting",
            ));
        }
        let authority = self.snapshot.control().authority();
        let accounting = self
            .policies
            .iter()
            .map(|(policy, governed_rows, bytes)| {
                Ok(HistorySizeAccounting {
                    format: "compact_json_v1",
                    fence: self.snapshot.fence(),
                    observed_at: self.snapshot.observed_at(),
                    scanned_rows: self.seen.len().to_string(),
                    governed_rows: governed_rows.to_string(),
                    pressure: HistorySizePressure::measure(
                        policy,
                        HistoryLogicalBytes::new(*bytes)?,
                        authority.size_pressure(policy.source().expect("exact census policy")),
                    )?,
                    transition: None,
                })
            })
            .collect::<Result<Vec<_>, StorageValidationError>>()?;
        if self.started.elapsed() > Duration::from_secs(5) {
            return Err(invalid("History accounting exceeded its time budget"));
        }
        Ok(HistoryFamilySizeAccounting {
            family: self.family,
            snapshot: self.snapshot,
            accounting,
            reserved: self.reserved,
            rows: self.seen,
            metadata_bytes: self.metadata_bytes,
            started: self.started,
        })
    }
}

/// Complete, adapter-observed family accounting. This cannot be deserialized
/// from a caller's gauges or from an inspection sample.
pub struct HistoryFamilySizeAccounting {
    pub(super) family: StorageIdentityHistoryKind,
    pub(super) snapshot: HistoryLifecycleSnapshot,
    pub(super) accounting: Vec<HistorySizeAccounting>,
    pub(super) reserved: BTreeMap<(HistoryPolicyKey, HistoryPolicyRevision), u64>,
    pub(super) rows: BTreeMap<(ResourceIdentity, VersionIdentity), HistoryLogicalBytes>,
    pub(super) metadata_bytes: u64,
    pub(super) started: Instant,
}

impl HistoryFamilySizeAccounting {
    pub(super) fn retain(
        mut self,
        initiator: PrincipalIdentity,
        source: HistorySizeObservationSource,
    ) -> Result<(Self, HistoryLifecycleControlChange), StorageValidationError> {
        let original = self.snapshot.clone();
        let mut control = original.control().clone();
        for accounting in &mut self.accounting {
            let previous = control
                .authority()
                .size_pressure(accounting.pressure().policy());
            if previous.map_or(!accounting.pressure().reclaiming(), |old| {
                old.reclaiming() == accounting.pressure().reclaiming()
            }) {
                continue;
            }
            accounting.fence = HistoryReviewFence::new(
                original.fence().incarnation(),
                control.authority().revision(),
                original.fence().deletion_revision(),
            )?;
            control = control
                .observe_size_from(accounting, initiator, source)?
                .next()
                .clone();
        }
        let change = original.control().change(control, original.fence())?;
        self.snapshot = HistoryLifecycleSnapshot::new(
            change.next().clone(),
            original.fence().incarnation(),
            original.fence().deletion_revision(),
            original.observed_at(),
        )?;
        Ok((self, change))
    }
}

/// Identifies who performed the observation on behalf of its initiating principal.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistorySizeObservationSource {
    Administrator,
    ManualRun,
    ScheduledRun,
}

/// Immutable pressure start/end receipt; gauges between transitions are not
/// retained as new authority. Exact policy revision and original clock survive
/// restore, with a terminal slot reserved for every active episode.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistorySizeTransition {
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = "uuid"))]
    operation: uuid::Uuid,
    revision: HistoryAuthorityRevision,
    /// Initiating principal. The source distinguishes a direct observation from worker execution.
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = "uuid"))]
    actor: PrincipalIdentity,
    /// Older receipts lack this field and retain their original canonical audit projection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source: Option<HistorySizeObservationSource>,
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = DateTime))]
    observed_at: DateTime<Utc>,
    pressure: HistorySizePressure,
}
impl HistorySizeTransition {
    pub const fn operation(&self) -> uuid::Uuid {
        self.operation
    }
    pub const fn revision(&self) -> HistoryAuthorityRevision {
        self.revision
    }
    pub const fn actor(&self) -> PrincipalIdentity {
        self.actor
    }
    pub const fn source(&self) -> Option<HistorySizeObservationSource> {
        self.source
    }
    pub const fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
    }
    pub const fn pressure(&self) -> &HistorySizePressure {
        &self.pressure
    }
}
impl HistoryLifecycleAuthority {
    pub const MAX_SIZE_TRANSITIONS: usize = 1024;
    pub fn size_transitions(&self) -> &[HistorySizeTransition] {
        &self.size_transitions
    }
    pub fn size_pressure(&self, policy: &HistoryPolicyReference) -> Option<&HistorySizePressure> {
        self.size_transitions
            .iter()
            .rev()
            .find(|entry| entry.pressure.policy() == policy)
            .map(|entry| &entry.pressure)
    }
    pub fn pending_size_completions(&self) -> usize {
        let mut seen = std::collections::BTreeSet::new();
        self.size_transitions
            .iter()
            .rev()
            .filter(|entry| {
                seen.insert((
                    entry.pressure.policy().key(),
                    entry.pressure.policy().revision(),
                )) && entry.pressure.reclaiming()
            })
            .count()
    }
    pub(super) fn validate_size_transitions(&self) -> Result<(), StorageValidationError> {
        let mut previous = Vec::<&HistorySizeTransition>::new();
        let mut operations = std::collections::BTreeSet::new();
        let mut last = 0;
        for entry in &self.size_transitions {
            let policy = self.policies().exact(entry.pressure.policy())?;
            let old = previous
                .iter()
                .rev()
                .find(|old| old.pressure.policy() == entry.pressure.policy());
            let expected = HistorySizePressure::measure(
                &policy,
                entry.pressure.measured_bytes(),
                old.map(|old| &old.pressure),
            )?;
            if expected != entry.pressure
                || old.is_some_and(|old| old.observed_at > entry.observed_at)
                || old.is_some_and(|old| old.pressure.reclaiming() == entry.pressure.reclaiming())
                || (old.is_none() && !entry.pressure.reclaiming())
                || entry.operation.get_variant() != uuid::Variant::RFC4122
                || entry.operation.get_version().is_none()
                || !operations.insert(entry.operation)
                || entry.revision.value() <= last
                || entry.revision > self.revision()
                || self.policies().records().iter().any(|record| {
                    record.operation() == entry.operation
                        || record.review().is_some_and(|review| {
                            review.reference().preview_id() == entry.operation
                        })
                        || (HistoryPolicyReference::from(record) == *entry.pressure.policy()
                            && record.applied_at() > entry.observed_at)
                })
                || self.hold_revisions().iter().any(|record| {
                    record.operation() == entry.operation || record.revision() == entry.revision
                })
                || self
                    .expiry()
                    .intents()
                    .iter()
                    .any(|intent| intent.identity() == entry.operation)
            {
                return Err(invalid("Invalid retained history size-pressure lineage"));
            }
            last = entry.revision.value();
            previous.push(entry);
        }
        if self.size_transitions.len() + self.pending_size_completions()
            > Self::MAX_SIZE_TRANSITIONS
        {
            return Err(invalid(
                "History size-pressure capacity must reserve terminal transitions",
            ));
        }
        Ok(())
    }
    pub(super) fn observe_size(
        &self,
        accounting: &mut HistorySizeAccounting,
        actor: PrincipalIdentity,
        source: HistorySizeObservationSource,
    ) -> Result<Self, StorageValidationError> {
        if accounting.fence.authority_revision() != self.revision() {
            return Err(invalid("History accounting authority changed"));
        }
        let pressure = &accounting.pressure;
        let previous = self.size_pressure(pressure.policy());
        let policy = self.policies().exact(pressure.policy())?;
        if HistorySizePressure::measure(&policy, pressure.measured_bytes(), previous)? != *pressure
        {
            return Err(invalid("History accounting pressure predecessor changed"));
        }
        if previous.is_some_and(|old| old.reclaiming() == pressure.reclaiming())
            || (previous.is_none() && !pressure.reclaiming())
        {
            return Ok(self.clone());
        }
        let mut next = self.clone();
        next.revision = HistoryAuthorityRevision::new(self.revision().value() + 1)?;
        let operation = uuid::Uuid::new_v4();
        next.size_transitions.push(HistorySizeTransition {
            operation,
            revision: next.revision,
            actor,
            source: Some(source),
            observed_at: accounting.observed_at,
            pressure: pressure.clone(),
        });
        next.validate_size_transitions()?;
        accounting.transition = Some(operation);
        Ok(next)
    }
}
impl HistoryLifecycleControl {
    pub fn observe_size(
        &self,
        accounting: &mut HistorySizeAccounting,
        actor: PrincipalIdentity,
    ) -> Result<HistoryLifecycleControlChange, StorageValidationError> {
        self.observe_size_from(
            accounting,
            actor,
            HistorySizeObservationSource::Administrator,
        )
    }

    fn observe_size_from(
        &self,
        accounting: &mut HistorySizeAccounting,
        initiator: PrincipalIdentity,
        source: HistorySizeObservationSource,
    ) -> Result<HistoryLifecycleControlChange, StorageValidationError> {
        let mut next = self.clone();
        next.authority = self
            .authority()
            .observe_size(accounting, initiator, source)?;
        self.change(next, accounting.fence)
    }
}

/// Atomic adapter result; a service must independently protect the returned
/// snapshot before acknowledging a pressure transition to an administrator.
#[derive(Clone, Debug)]
pub struct HistorySizeObservation {
    accounting: HistorySizeAccounting,
    snapshot: HistoryLifecycleSnapshot,
}
impl HistorySizeObservation {
    pub const fn new(
        accounting: HistorySizeAccounting,
        snapshot: HistoryLifecycleSnapshot,
    ) -> Self {
        Self {
            accounting,
            snapshot,
        }
    }
    pub fn into_parts(self) -> (HistorySizeAccounting, HistoryLifecycleSnapshot) {
        (self.accounting, self.snapshot)
    }
}

#[cfg(test)]
mod census_tests;
