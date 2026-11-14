//! Versioned logical accounting, independent of physical/compressed storage.

use std::str::FromStr;

use bigdecimal::BigDecimal;
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
                .map_err(|_| invalid("Logical history number exceeds the v1 format"))?
                .normalized();
            let (coefficient, scale) = decimal.as_bigint_and_exponent();
            let coefficient = coefficient.to_string();
            if coefficient == "0" {
                return Ok(1);
            }
            let sign = u64::from(coefficient.starts_with('-'));
            let digits = coefficient.trim_start_matches('-').len() as u64;
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
    policy: HistoryEffectivePolicy,
    seen: std::collections::BTreeSet<(ResourceIdentity, hubuum_domain::VersionIdentity)>,
    metadata_bytes: u64,
    failed: bool,
    started: std::time::Instant,
    governed_rows: u64,
    bytes: u64,
}
impl HistorySizeCensus {
    pub const MAX_ROWS: usize = 100_000;
    pub const MAX_METADATA_BYTES: u64 = 64 * 1024 * 1024;
    pub fn new(
        snapshot: HistoryLifecycleSnapshot,
        reference: &HistoryPolicyReference,
    ) -> Result<Self, StorageValidationError> {
        let policy = snapshot.control().authority().policies().exact(reference)?;
        HistorySizePressure::measure(&policy, HistoryLogicalBytes::new(0)?, None)?;
        Ok(Self {
            snapshot,
            policy,
            seen: Default::default(),
            metadata_bytes: 0,
            failed: false,
            started: std::time::Instant::now(),
            governed_rows: 0,
            bytes: 0,
        })
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
        let reference = self.policy.source().expect("census has an exact policy");
        if self.started.elapsed() > std::time::Duration::from_secs(5)
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
                candidate.family() == reference.key().family()
                    && !candidate.attributions().is_empty()
            })
            .ok_or_else(|| {
                invalid("Complete history accounting has unresolved historical attribution")
            })?;
        if !self
            .seen
            .insert((candidate.target().resource(), candidate.target().version()))
        {
            return Err(invalid(
                "Complete history accounting has duplicate version identities",
            ));
        }
        // Both relation endpoints can resolve to the same source: charge the
        // exact logical version once, independently of protection/eligibility.
        if candidate.attributions().iter().any(|attribution| {
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
                == Some(reference)
        }) {
            self.governed_rows += 1;
            self.bytes = add(self.bytes, bytes.value())?;
        }
        Ok(())
    }
    /// The adapter must call this only after positively exhausting its source.
    pub fn finish(self) -> Result<HistorySizeAccounting, StorageValidationError> {
        if self.failed || self.started.elapsed() > std::time::Duration::from_secs(5) {
            return Err(invalid(
                "A failed or expired history census cannot produce complete accounting",
            ));
        }
        let authority = self.snapshot.control().authority();
        Ok(HistorySizeAccounting {
            format: "compact_json_v1",
            fence: self.snapshot.fence(),
            observed_at: self.snapshot.observed_at(),
            scanned_rows: self.seen.len().to_string(),
            governed_rows: self.governed_rows.to_string(),
            pressure: HistorySizePressure::measure(
                &self.policy,
                HistoryLogicalBytes::new(self.bytes)?,
                authority.size_pressure(self.policy.source().expect("exact census policy")),
            )?,
            transition: None,
        })
    }
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
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = "uuid"))]
    actor: PrincipalIdentity,
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
        let mut next = self.clone();
        next.authority = self.authority().observe_size(accounting, actor)?;
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
