//! Complete accounting followed by bounded oldest-eligible selection. Only an
//! exhausted native family census can construct the selection proof.
use super::*;
use std::collections::BTreeMap;
use std::time::Duration;

/// Original selection cause, retained independently of later pressure refreshes.
/// Reserved bytes cover pending copies and earlier members of this same batch.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "SizeAuthorizationInput")]
pub struct HistoryExpirySizeAuthorization {
    policy: HistoryPolicyReference,
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = "uuid"))]
    episode: Uuid,
    observed_bytes: HistoryLogicalBytes,
    reserved_bytes: HistoryLogicalBytes,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SizeAuthorizationInput {
    policy: HistoryPolicyReference,
    episode: Uuid,
    observed_bytes: HistoryLogicalBytes,
    reserved_bytes: HistoryLogicalBytes,
}
impl TryFrom<SizeAuthorizationInput> for HistoryExpirySizeAuthorization {
    type Error = StorageValidationError;
    fn try_from(value: SizeAuthorizationInput) -> Result<Self, Self::Error> {
        if value.policy.revision().value() == 0
            || value.episode.get_variant() != uuid::Variant::RFC4122
            || value.episode.get_version().is_none()
            || value.reserved_bytes >= value.observed_bytes
        {
            return Err(invalid("Invalid retained size-selection authorization"));
        }
        Ok(Self {
            policy: value.policy,
            episode: value.episode,
            observed_bytes: value.observed_bytes,
            reserved_bytes: value.reserved_bytes,
        })
    }
}
impl HistoryExpirySizeAuthorization {
    pub const fn policy(&self) -> &HistoryPolicyReference {
        &self.policy
    }
    fn measurement(
        &self,
        authority: &HistoryLifecycleAuthority,
    ) -> Result<HistorySizePressure, StorageValidationError> {
        let episode = authority
            .size_transitions()
            .iter()
            .find(|entry| {
                entry.operation() == self.episode
                    && entry.pressure().policy() == &self.policy
                    && entry.pressure().reclaiming()
            })
            .ok_or_else(|| invalid("Size selection lost its original pressure episode"))?;
        let policy = authority.policies().exact(&self.policy)?;
        let remaining = self
            .observed_bytes
            .value()
            .checked_sub(self.reserved_bytes.value())
            .ok_or_else(|| invalid("Size reservations exceed complete observed bytes"))?;
        let pressure = HistorySizePressure::measure(
            &policy,
            HistoryLogicalBytes::new(remaining)?,
            Some(episode.pressure()),
        )?;
        if !pressure.reclaiming() {
            return Err(invalid(
                "Size selection had already reached its lower target",
            ));
        }
        Ok(pressure)
    }
    fn measurement_at(
        &self,
        authority: &HistoryLifecycleAuthority,
        fence: HistoryReviewFence,
        selected_at: DateTime<Utc>,
    ) -> Result<HistorySizePressure, StorageValidationError> {
        let original = authority.size_transitions().iter().rev().find(|entry| {
            entry.pressure().policy() == &self.policy
                && entry.revision() <= fence.authority_revision()
        });
        if !original.is_some_and(|entry| {
            entry.operation() == self.episode && entry.observed_at() <= selected_at
        }) {
            return Err(invalid(
                "Size membership does not retain its selection-time episode",
            ));
        }
        self.measurement(authority)
    }
}
impl HistoryExpiryMember {
    pub(in crate::history_lifecycle) fn selection_measurements(
        &self,
        authority: &HistoryLifecycleAuthority,
        manifest: &HistoryExpiryManifest,
    ) -> Result<Vec<HistorySizePressure>, StorageValidationError> {
        self.size_authorizations
            .iter()
            .map(|authorization| {
                authorization.measurement_at(authority, manifest.fence(), manifest.selected_at())
            })
            .collect()
    }
    pub(crate) fn size_measurements(
        &self,
        authority: &HistoryLifecycleAuthority,
    ) -> Result<Vec<HistorySizePressure>, StorageValidationError> {
        self.size_authorizations
            .iter()
            .map(|authorization| authorization.measurement(authority))
            .collect()
    }
}
impl HistoryExpiryManifest {
    pub(in crate::history_lifecycle) fn validate_triggers(
        &self,
        authority: &HistoryLifecycleAuthority,
    ) -> Result<(), StorageValidationError> {
        for member in self.members() {
            let measurements = member.selection_measurements(authority, self)?;
            for source in member.policies() {
                let policy = authority.policies().exact(source)?;
                if policy
                    .inspect(
                        member.anchor(),
                        self.selected_at(),
                        measurements
                            .iter()
                            .find(|pressure| pressure.policy() == source),
                    )?
                    .blocker()
                    .is_some()
                {
                    return Err(invalid(
                        "Retained expiry lost its original trigger authorization",
                    ));
                }
            }
        }
        Ok(())
    }
}

type Identity = (ResourceIdentity, hubuum_domain::VersionIdentity);
type Order = (DateTime<Utc>, [u8; 16], [u8; 16]);
struct OldestCandidate {
    candidate: HistoryExpiryCandidate,
    input_bytes: HistoryExpiryInputBytes,
    logical_bytes: HistoryLogicalBytes,
    metadata_bytes: usize,
}

/// Holds only a bounded oldest prefix. Native full-record payloads never enter
/// this heap. A failed, truncated or expired second pass cannot be finalized.
pub struct HistoryExpirySizeSelection {
    request: HistoryExpiryAdmissionRequest,
    accounting: HistoryFamilySizeAccounting,
    pressure_change: HistoryLifecycleControlChange,
    remaining: BTreeMap<Identity, HistoryLogicalBytes>,
    measurements: Vec<HistorySizePressure>,
    candidates: BTreeMap<Order, OldestCandidate>,
    retained_bytes: usize,
    manifest_header_bytes: usize,
    scanned: u32,
    oversized: u32,
    failed: bool,
}
impl HistoryExpirySizeSelection {
    pub fn new(
        mut request: HistoryExpiryAdmissionRequest,
        accounting: HistoryFamilySizeAccounting,
    ) -> Result<Self, StorageValidationError> {
        if accounting.family != request.scan().key().family() {
            return Err(invalid(
                "Complete size accounting belongs to another history family",
            ));
        }
        request.selection(&accounting.snapshot)?;
        if !request.needs_size_selection(accounting.snapshot.control().authority()) {
            return Err(invalid(
                "Size selection requires an initial finite family pass",
            ));
        }
        let (initiator, source) = match &request.origin {
            HistoryExpiryOrigin::Manual { principal } => {
                (*principal, HistorySizeObservationSource::ManualRun)
            }
            HistoryExpiryOrigin::Scheduled { initiator, .. } => (
                initiator.ok_or_else(|| {
                    invalid("Size selection requires its original initiating principal")
                })?,
                HistorySizeObservationSource::ScheduledRun,
            ),
        };
        let (mut accounting, pressure_change) = accounting.retain(initiator, source)?;
        request.fence = accounting.snapshot.fence();
        let authority = accounting.snapshot.control().authority();
        let measurements = accounting
            .accounting
            .iter()
            .map(|entry| {
                let pressure = entry.pressure();
                let policy = pressure.policy();
                let Some(reserved) = accounting.reserved.get(&(policy.key(), policy.revision()))
                else {
                    return Ok(pressure.clone());
                };
                HistorySizePressure::measure(
                    &authority.policies().exact(policy)?,
                    HistoryLogicalBytes::new(pressure.measured_bytes().value() - reserved)?,
                    Some(pressure),
                )
            })
            .collect::<Result<Vec<_>, StorageValidationError>>()?;
        let manifest_header_bytes = request.selection(&accounting.snapshot)?.retained_bytes;
        let scanned = accounting.rows.len() as u32;
        let remaining = std::mem::take(&mut accounting.rows);
        Ok(Self {
            request,
            accounting,
            pressure_change,
            remaining,
            measurements,
            candidates: BTreeMap::new(),
            retained_bytes: 0,
            manifest_header_bytes,
            scanned,
            oversized: 0,
            failed: false,
        })
    }
    /// Apply this remaining budget to the next native statement as well.
    pub fn remaining_time(&self) -> Result<Duration, StorageValidationError> {
        Duration::from_secs(5)
            .checked_sub(self.accounting.started.elapsed())
            .filter(|duration| !duration.is_zero())
            .ok_or_else(|| {
                invalid("Complete size selection exceeded its original five-second budget")
            })
    }
    pub fn observe(
        &mut self,
        candidate: HistoryExpiryCandidate,
        input_bytes: u64,
        metadata_bytes: u64,
        visible: bool,
    ) -> Result<(), StorageValidationError> {
        let result = self.observe_row(candidate, input_bytes, metadata_bytes, visible);
        self.failed |= result.is_err();
        result
    }
    fn observe_row(
        &mut self,
        candidate: HistoryExpiryCandidate,
        input_bytes: u64,
        metadata_bytes: u64,
        visible: bool,
    ) -> Result<(), StorageValidationError> {
        self.remaining_time()?;
        if metadata_bytes > HistoryLifecycleScan::MAX_INPUT_BYTES as u64 {
            return Err(invalid(
                "Size selection exceeded its per-row metadata budget",
            ));
        }
        if candidate.family() != self.request.scan().key().family() {
            return Err(invalid("Size selection changed its complete census family"));
        }
        self.accounting.metadata_bytes = self
            .accounting
            .metadata_bytes
            .checked_add(metadata_bytes)
            .filter(|bytes| *bytes <= HistorySizeCensus::MAX_METADATA_BYTES)
            .ok_or_else(|| invalid("Size selection exceeded its complete metadata budget"))?;
        let logical_bytes = self
            .remaining
            .remove(&(candidate.target().resource(), candidate.target().version()))
            .ok_or_else(|| invalid("Size selection changed its complete census membership"))?;
        self.scanned += 1;
        if !visible || !self.request.scan.includes(&candidate) || candidate.anchor().is_none() {
            return Ok(());
        }
        let Ok(input_bytes) = HistoryExpiryInputBytes::new(input_bytes) else {
            self.oversized += 1;
            return Ok(());
        };
        if candidate.family() == StorageIdentityHistoryKind::Object
            && candidate.object_authorization().is_none()
        {
            self.oversized += 1;
            return Ok(());
        }
        let decision = self.accounting.snapshot.control().authority().inspect(
            &candidate,
            self.accounting.snapshot.observed_at(),
            &self.measurements,
        )?;
        if !decision.eligible() {
            return Ok(());
        }
        let authorizations = self.authorizations(&candidate)?;
        let growth = authorizations
            .iter()
            .map(|authorization| {
                authorization.observed_bytes.value().to_string().len()
                    - authorization.reserved_bytes.value().to_string().len()
            })
            .sum::<usize>();
        let member = HistoryExpiryMember::from_candidate(
            &candidate,
            input_bytes,
            &decision,
            authorizations,
        )?;
        let encoded_bytes = serde_json::to_vec(&member)
            .map(|bytes| retained_json_len(&bytes))
            .map_err(|_| invalid("Cannot encode oldest expiry membership"))?;
        // The full retained member includes both endpoint witnesses. Reserve
        // decimal growth and fixed heap allocation before retaining the prefix.
        let metadata_bytes = encoded_bytes + growth + 512;
        if metadata_bytes > HistoryExpiryManifest::MAX_BYTES
            || self.manifest_header_bytes + encoded_bytes + growth + 8
                > HistoryExpiryManifest::MAX_BYTES
        {
            self.oversized += 1;
            return Ok(());
        }
        let order = (
            candidate.anchor().expect("closed candidate").at(),
            *candidate.target().resource().as_bytes(),
            *candidate.target().version().as_bytes(),
        );
        self.retained_bytes += metadata_bytes;
        self.candidates.insert(
            order,
            OldestCandidate {
                candidate,
                input_bytes,
                logical_bytes,
                metadata_bytes,
            },
        );
        while self.candidates.len() > self.request.scan.limit() as usize
            || self.retained_bytes > HistoryExpiryManifest::MAX_BYTES
        {
            self.retained_bytes -= self
                .candidates
                .pop_last()
                .expect("nonempty oldest prefix")
                .1
                .metadata_bytes;
        }
        Ok(())
    }
    fn authorizations(
        &self,
        candidate: &HistoryExpiryCandidate,
    ) -> Result<Vec<HistoryExpirySizeAuthorization>, StorageValidationError> {
        let snapshot = &self.accounting.snapshot;
        let authority = snapshot.control().authority();
        let without_size = authority.inspect(candidate, snapshot.observed_at(), &[])?;
        let mut authorizations = Vec::new();
        for endpoint in without_size.endpoints().iter().filter(|endpoint| {
            endpoint.blocker() == Some(HistoryExpiryBlocker::AccountingUnavailable)
        }) {
            let policy = endpoint
                .policy()
                .source()
                .expect("size endpoint has an immutable source");
            if authorizations
                .iter()
                .any(|entry: &HistoryExpirySizeAuthorization| entry.policy() == policy)
            {
                continue;
            }
            let Some(pressure) = self
                .measurements
                .iter()
                .find(|pressure| pressure.policy() == policy && pressure.reclaiming())
            else {
                continue;
            };
            let observed = self
                .accounting
                .accounting
                .iter()
                .find(|entry| entry.pressure().policy() == policy)
                .expect("complete measured source");
            let episode = authority
                .size_transitions()
                .iter()
                .rev()
                .find(|entry| entry.pressure().policy() == policy && entry.pressure().reclaiming())
                .expect("retained pressure start");
            authorizations.push(HistoryExpirySizeAuthorization {
                policy: policy.clone(),
                episode: episode.operation(),
                observed_bytes: observed.pressure().measured_bytes(),
                reserved_bytes: HistoryLogicalBytes::new(
                    observed.pressure().measured_bytes().value()
                        - pressure.measured_bytes().value(),
                )?,
            });
        }
        Ok(authorizations)
    }
    pub fn finish(mut self) -> Result<HistoryExpirySizePlan, StorageValidationError> {
        self.remaining_time()?;
        if self.failed || !self.remaining.is_empty() {
            return Err(invalid(
                "Only a complete unchanged second size pass may select membership",
            ));
        }
        let snapshot = &self.accounting.snapshot;
        let authority = snapshot.control().authority();
        let mut selection = self.request.selection(snapshot)?;
        for row in std::mem::take(&mut self.candidates).into_values() {
            let authorizations = self.authorizations(&row.candidate)?;
            let sources = row
                .candidate
                .attributions()
                .iter()
                .map(|attribution| {
                    authority.policies().resolve_accounting(
                        row.candidate.family(),
                        *attribution,
                        row.candidate.anchor(),
                        snapshot.observed_at(),
                    )
                })
                .collect::<Vec<_>>();
            match selection.consider_authorized(
                row.candidate,
                row.input_bytes,
                &self.measurements,
                authorizations,
            )? {
                HistoryExpirySelectionOutcome::BudgetReached => break,
                HistoryExpirySelectionOutcome::Blocked(_) => continue,
                HistoryExpirySelectionOutcome::Selected => {
                    for pressure in &mut self.measurements {
                        if sources
                            .iter()
                            .any(|source| source.source() == Some(pressure.policy()))
                        {
                            *pressure = HistorySizePressure::measure(
                                &authority.policies().exact(pressure.policy())?,
                                HistoryLogicalBytes::new(
                                    pressure
                                        .measured_bytes()
                                        .value()
                                        .saturating_sub(row.logical_bytes.value()),
                                )?,
                                Some(pressure),
                            )?;
                        }
                    }
                }
            }
        }
        let manifest = if selection.is_empty() {
            None
        } else {
            Some(selection.finish()?)
        };
        if self.accounting.started.elapsed() > Duration::from_secs(5) {
            return Err(invalid("Size selection exceeded its original deadline"));
        }
        Ok(HistoryExpirySizePlan {
            pressure_change: self.pressure_change,
            result: HistoryExpiryAdmissionResult::complete_size_pass(
                manifest,
                self.scanned,
                self.oversized,
            )?,
        })
    }
}

/// Both pressure and exact intent must commit with required audit in one native
/// transaction. Independent protection still precedes every physical deletion.
pub struct HistoryExpirySizePlan {
    pressure_change: HistoryLifecycleControlChange,
    result: HistoryExpiryAdmissionResult,
}
impl HistoryExpirySizePlan {
    pub fn into_parts(self) -> (HistoryLifecycleControlChange, HistoryExpiryAdmissionResult) {
        (self.pressure_change, self.result)
    }
}

#[cfg(test)]
mod tests;
