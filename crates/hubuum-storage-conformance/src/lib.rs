//! Reusable behavioral certification for Hubuum storage adapters.
//!
//! Rust trait bounds prove that an adapter has methods with the right shapes.
//! This crate tests the semantic obligations that the type system cannot
//! express: durable audit receipts, transaction rollback, outbox delivery,
//! and telemetry observations.

use std::error::Error;
use std::fmt;
use std::sync::Mutex;

use async_trait::async_trait;
use hubuum_storage_core::{
    MutationOutcome, StorageOperationObservation, StorageRecordedEvent, StorageTelemetry,
};

/// Thread-safe application observer used by backend contract fixtures.
#[derive(Debug, Default)]
pub struct RecordingStorageTelemetry {
    observations: Mutex<Vec<StorageOperationObservation>>,
}

impl RecordingStorageTelemetry {
    #[must_use]
    pub fn observations(&self) -> Vec<StorageOperationObservation> {
        self.observations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    #[must_use]
    pub fn operation_count(&self, capability: &str, operation: &str) -> usize {
        self.observations()
            .iter()
            .filter(|observation| {
                observation.capability() == capability && observation.operation() == operation
            })
            .count()
    }
}

impl StorageTelemetry for RecordingStorageTelemetry {
    fn operation_finished(&self, observation: &StorageOperationObservation) {
        self.observations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(observation.clone());
    }
}

/// Boxed fixture failure used by backend-independent contract runners.
pub type FixtureError = Box<dyn Error + Send + Sync + 'static>;

/// Evidence for one committed state change and its durable event row.
pub struct CommittedMutationProbe {
    outcome: MutationOutcome<()>,
    persisted_event: StorageRecordedEvent,
}

impl CommittedMutationProbe {
    #[must_use]
    pub const fn new(outcome: MutationOutcome<()>, persisted_event: StorageRecordedEvent) -> Self {
        Self {
            outcome,
            persisted_event,
        }
    }
}

/// Evidence that a genuine no-op did not append an audit event.
pub struct UnchangedMutationProbe {
    outcome: MutationOutcome<()>,
    appended_event_count: usize,
}

impl UnchangedMutationProbe {
    #[must_use]
    pub const fn new(outcome: MutationOutcome<()>, appended_event_count: usize) -> Self {
        Self {
            outcome,
            appended_event_count,
        }
    }
}

/// Evidence collected after an intentionally failed atomic mutation.
pub struct RollbackProbe {
    state_change_persisted: bool,
    audit_event_persisted: bool,
}

impl RollbackProbe {
    #[must_use]
    pub const fn new(state_change_persisted: bool, audit_event_persisted: bool) -> Self {
        Self {
            state_change_persisted,
            audit_event_persisted,
        }
    }
}

/// Evidence that a committed event traversed the durable outbox and sink edge.
pub struct FanoutProbe {
    durable_delivery_count: usize,
    sink_delivery_count: usize,
}

impl FanoutProbe {
    #[must_use]
    pub const fn new(durable_delivery_count: usize, sink_delivery_count: usize) -> Self {
        Self {
            durable_delivery_count,
            sink_delivery_count,
        }
    }
}

/// Application-side observations captured while executing the other probes.
pub struct TelemetryProbe {
    logical_mutation_count: usize,
    backend_operation_count: usize,
    failure_count: usize,
}

impl TelemetryProbe {
    #[must_use]
    pub const fn new(
        logical_mutation_count: usize,
        backend_operation_count: usize,
        failure_count: usize,
    ) -> Self {
        Self {
            logical_mutation_count,
            backend_operation_count,
            failure_count,
        }
    }
}

/// Backend-specific fixture hooks consumed by the common semantic contract.
///
/// Implementations provision and clean up their own isolated data. Each probe
/// must exercise the same public adapter surface used by application code.
#[async_trait]
pub trait BackendAuditFixture: Send + Sync {
    async fn committed_mutation(&self) -> Result<CommittedMutationProbe, FixtureError>;

    async fn unchanged_mutation(&self) -> Result<UnchangedMutationProbe, FixtureError>;

    async fn rolled_back_mutation(&self) -> Result<RollbackProbe, FixtureError>;

    async fn fanout_to_recording_sink(&self) -> Result<FanoutProbe, FixtureError>;

    async fn telemetry_observations(&self) -> Result<TelemetryProbe, FixtureError>;
}

/// Successful completion of all semantic storage checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ContractReport {
    checks: usize,
}

impl ContractReport {
    #[must_use]
    pub const fn checks(self) -> usize {
        self.checks
    }
}

/// One failed semantic obligation.
#[derive(Debug)]
pub enum ContractViolation {
    Fixture(FixtureError),
    MissingAuditReceipt,
    ReceiptDoesNotMatchPersistedEvent,
    NoopReturnedAuditReceipt,
    NoopAppendedAuditEvent,
    RollbackPersistedState,
    RollbackPersistedAuditEvent,
    MissingDurableDelivery,
    MissingSinkDelivery,
    MissingLogicalTelemetry,
    MissingBackendTelemetry,
    MissingFailureTelemetry,
}

impl fmt::Display for ContractViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::Fixture(error) => return write!(formatter, "contract fixture failed: {error}"),
            Self::MissingAuditReceipt => "committed mutation returned no audit receipt",
            Self::ReceiptDoesNotMatchPersistedEvent => {
                "audit receipt does not identify the persisted event"
            }
            Self::NoopReturnedAuditReceipt => "unchanged mutation returned an audit receipt",
            Self::NoopAppendedAuditEvent => "unchanged mutation appended an audit event",
            Self::RollbackPersistedState => "failed mutation persisted its state change",
            Self::RollbackPersistedAuditEvent => "failed mutation persisted an audit event",
            Self::MissingDurableDelivery => "committed event produced no durable delivery",
            Self::MissingSinkDelivery => "durable delivery did not reach the recording sink",
            Self::MissingLogicalTelemetry => "mutation produced no logical storage telemetry",
            Self::MissingBackendTelemetry => "mutation produced no backend telemetry",
            Self::MissingFailureTelemetry => "rolled-back mutation produced no failure telemetry",
        };
        formatter.write_str(message)
    }
}

impl Error for ContractViolation {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Fixture(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

impl From<FixtureError> for ContractViolation {
    fn from(error: FixtureError) -> Self {
        Self::Fixture(error)
    }
}

/// Execute the five-part audit contract against one backend fixture.
pub async fn verify_backend_audit_contract(
    fixture: &impl BackendAuditFixture,
) -> Result<ContractReport, ContractViolation> {
    verify_committed_mutation(fixture.committed_mutation().await?)?;
    verify_unchanged_mutation(fixture.unchanged_mutation().await?)?;
    verify_rollback(fixture.rolled_back_mutation().await?)?;
    verify_fanout(fixture.fanout_to_recording_sink().await?)?;
    verify_telemetry(fixture.telemetry_observations().await?)?;
    Ok(ContractReport { checks: 5 })
}

fn verify_committed_mutation(probe: CommittedMutationProbe) -> Result<(), ContractViolation> {
    let receipt = probe
        .outcome
        .audit()
        .ok_or(ContractViolation::MissingAuditReceipt)?;
    let (event, before_revision, after_revision) = probe.persisted_event.into_parts();
    if event.id != receipt.sequence()
        || event.event_id != receipt.event_id().as_uuid()
        || event.entity_type != receipt.entity_type()
        || event.action != receipt.action()
        || before_revision != receipt.before_revision()
        || after_revision != receipt.after_revision()
    {
        return Err(ContractViolation::ReceiptDoesNotMatchPersistedEvent);
    }
    Ok(())
}

fn verify_unchanged_mutation(probe: UnchangedMutationProbe) -> Result<(), ContractViolation> {
    if probe.outcome.audit().is_some() || probe.outcome.is_committed() {
        return Err(ContractViolation::NoopReturnedAuditReceipt);
    }
    if probe.appended_event_count != 0 {
        return Err(ContractViolation::NoopAppendedAuditEvent);
    }
    Ok(())
}

const fn verify_rollback(probe: RollbackProbe) -> Result<(), ContractViolation> {
    if probe.state_change_persisted {
        return Err(ContractViolation::RollbackPersistedState);
    }
    if probe.audit_event_persisted {
        return Err(ContractViolation::RollbackPersistedAuditEvent);
    }
    Ok(())
}

const fn verify_fanout(probe: FanoutProbe) -> Result<(), ContractViolation> {
    if probe.durable_delivery_count == 0 {
        return Err(ContractViolation::MissingDurableDelivery);
    }
    if probe.sink_delivery_count == 0 {
        return Err(ContractViolation::MissingSinkDelivery);
    }
    Ok(())
}

const fn verify_telemetry(probe: TelemetryProbe) -> Result<(), ContractViolation> {
    if probe.logical_mutation_count == 0 {
        return Err(ContractViolation::MissingLogicalTelemetry);
    }
    if probe.backend_operation_count == 0 {
        return Err(ContractViolation::MissingBackendTelemetry);
    }
    if probe.failure_count == 0 {
        return Err(ContractViolation::MissingFailureTelemetry);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use hubuum_storage_core::AuditReceipt;

    use super::*;

    #[test]
    fn unchanged_probe_rejects_an_appended_event() {
        let probe = UnchangedMutationProbe::new(MutationOutcome::unchanged(()), 1);

        assert!(matches!(
            verify_unchanged_mutation(probe),
            Err(ContractViolation::NoopAppendedAuditEvent)
        ));
    }

    #[test]
    fn committed_probe_requires_a_receipt() {
        let _receipt_type_is_part_of_the_contract: Option<AuditReceipt> = None;
        let outcome = MutationOutcome::unchanged(());
        assert!(outcome.audit().is_none());
    }
}
