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
use hubuum_domain::ResourceRevision;
use hubuum_storage_core::{
    EventRetentionBatchId, EventRetentionSummary, MutationOutcome, StorageError, StorageErrorKind,
    StorageOperationObservation, StorageRecordedEvent, StorageTelemetry,
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

/// Backend-neutral observations from one application/service/HTTP smoke run.
///
/// The root application owns framework setup and authentication fixtures. The
/// conformance crate owns the compatibility expectations so another adapter
/// does not have to copy PostgreSQL's Actix harness or assertion logic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplicationCompatibilityProbe {
    expected_backend_name: String,
    observed_backend_name: String,
    expected_resource_id: i32,
    service_resource_id: i32,
    success_status: u16,
    readiness_status: u16,
    point_status: u16,
    point_resource_id: i32,
    list_status: u16,
    listed_resource_ids: Vec<i32>,
}

impl ApplicationCompatibilityProbe {
    #[must_use]
    pub fn builder(
        expected_backend_name: impl Into<String>,
        observed_backend_name: impl Into<String>,
        expected_resource_id: i32,
        success_status: u16,
    ) -> ApplicationCompatibilityProbeBuilder {
        ApplicationCompatibilityProbeBuilder {
            expected_backend_name: expected_backend_name.into(),
            observed_backend_name: observed_backend_name.into(),
            expected_resource_id,
            success_status,
            service_resource_id: None,
            readiness_status: None,
            point_status: None,
            point_resource_id: None,
            list_status: None,
            listed_resource_ids: None,
        }
    }
}

/// Validating construction for a complete application compatibility probe.
pub struct ApplicationCompatibilityProbeBuilder {
    expected_backend_name: String,
    observed_backend_name: String,
    expected_resource_id: i32,
    success_status: u16,
    service_resource_id: Option<i32>,
    readiness_status: Option<u16>,
    point_status: Option<u16>,
    point_resource_id: Option<i32>,
    list_status: Option<u16>,
    listed_resource_ids: Option<Vec<i32>>,
}

impl ApplicationCompatibilityProbeBuilder {
    #[must_use]
    pub const fn service_resource_id(mut self, value: i32) -> Self {
        self.service_resource_id = Some(value);
        self
    }

    #[must_use]
    pub const fn readiness_status(mut self, value: u16) -> Self {
        self.readiness_status = Some(value);
        self
    }

    #[must_use]
    pub const fn point(mut self, status: u16, resource_id: i32) -> Self {
        self.point_status = Some(status);
        self.point_resource_id = Some(resource_id);
        self
    }

    #[must_use]
    pub fn list(mut self, status: u16, resource_ids: Vec<i32>) -> Self {
        self.list_status = Some(status);
        self.listed_resource_ids = Some(resource_ids);
        self
    }

    pub fn build(self) -> Result<ApplicationCompatibilityProbe, FixtureError> {
        let missing = |field| -> FixtureError {
            std::io::Error::other(format!(
                "application compatibility probe is missing {field}"
            ))
            .into()
        };
        Ok(ApplicationCompatibilityProbe {
            expected_backend_name: self.expected_backend_name,
            observed_backend_name: self.observed_backend_name,
            expected_resource_id: self.expected_resource_id,
            service_resource_id: self
                .service_resource_id
                .ok_or_else(|| missing("the service resource identifier"))?,
            success_status: self.success_status,
            readiness_status: self
                .readiness_status
                .ok_or_else(|| missing("the readiness status"))?,
            point_status: self
                .point_status
                .ok_or_else(|| missing("the point status"))?,
            point_resource_id: self
                .point_resource_id
                .ok_or_else(|| missing("the point resource identifier"))?,
            list_status: self.list_status.ok_or_else(|| missing("the list status"))?,
            listed_resource_ids: self
                .listed_resource_ids
                .ok_or_else(|| missing("the listed resource identifiers"))?,
        })
    }
}

/// Root-owned framework hook consumed by the common application contract.
#[async_trait(?Send)]
pub trait ApplicationCompatibilityFixture {
    async fn application_compatibility_probe(
        &self,
    ) -> Result<ApplicationCompatibilityProbe, FixtureError>;
}

/// Evidence for one committed state change and its durable event row.
pub struct CommittedMutationProbe {
    outcome: MutationOutcome<()>,
    persisted_events: Vec<StorageRecordedEvent>,
}

impl CommittedMutationProbe {
    #[must_use]
    pub fn new(outcome: MutationOutcome<()>, persisted_event: StorageRecordedEvent) -> Self {
        Self {
            outcome,
            persisted_events: vec![persisted_event],
        }
    }

    /// Evidence for a mutation that legitimately emits more than one audit
    /// event, such as bulk revocation.
    #[must_use]
    pub const fn with_events(
        outcome: MutationOutcome<()>,
        persisted_events: Vec<StorageRecordedEvent>,
    ) -> Self {
        Self {
            outcome,
            persisted_events,
        }
    }
}

/// Evidence that optimistic-concurrency metadata survived the adapter edge.
pub struct RevisionConflictProbe {
    error: StorageError,
    expected_current_revision: ResourceRevision,
}

impl RevisionConflictProbe {
    #[must_use]
    pub const fn new(error: StorageError, expected_current_revision: ResourceRevision) -> Self {
        Self {
            error,
            expected_current_revision,
        }
    }
}

/// Evidence for the durable retention claim/archive/complete protocol.
pub struct RetentionRetryProbe {
    failed_batch_id: EventRetentionBatchId,
    retried_batch_id: EventRetentionBatchId,
    event_survived_archive_failure: bool,
    first_completion: EventRetentionSummary,
    repeated_completion: EventRetentionSummary,
}

impl RetentionRetryProbe {
    #[must_use]
    pub const fn new(
        failed_batch_id: EventRetentionBatchId,
        retried_batch_id: EventRetentionBatchId,
        event_survived_archive_failure: bool,
        first_completion: EventRetentionSummary,
        repeated_completion: EventRetentionSummary,
    ) -> Self {
        Self {
            failed_batch_id,
            retried_batch_id,
            event_survived_archive_failure,
            first_completion,
            repeated_completion,
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

    async fn revision_conflict(&self) -> Result<RevisionConflictProbe, FixtureError>;
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
    AuditReceiptCountMismatch,
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
    WrongRevisionConflictKind,
    MissingCurrentRevision,
    RetentionClaimWasReplaced,
    RetentionPurgedBeforeArchiveSucceeded,
    RetentionCompletionWasNotIdempotent,
    WrongApplicationBackend,
    ServicePointMismatch,
    ReadinessFailed,
    HttpPointFailed,
    HttpPointMismatch,
    HttpListFailed,
    HttpListOmittedResource,
}

impl fmt::Display for ContractViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::Fixture(error) => return write!(formatter, "contract fixture failed: {error}"),
            Self::MissingAuditReceipt => "committed mutation returned no audit receipt",
            Self::AuditReceiptCountMismatch => {
                "committed mutation receipts do not match the persisted event count"
            }
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
            Self::WrongRevisionConflictKind => {
                "stale mutation did not return a revision-conflict error"
            }
            Self::MissingCurrentRevision => {
                "revision-conflict error omitted the authoritative current revision"
            }
            Self::RetentionClaimWasReplaced => {
                "retention retry did not return the durable pending claim"
            }
            Self::RetentionPurgedBeforeArchiveSucceeded => {
                "retention purged an event before archival succeeded"
            }
            Self::RetentionCompletionWasNotIdempotent => {
                "retention completion returned a different result when repeated"
            }
            Self::WrongApplicationBackend => {
                "application composition selected a different storage backend"
            }
            Self::ServicePointMismatch => {
                "application service returned the wrong compatibility resource"
            }
            Self::ReadinessFailed => "application readiness did not report success",
            Self::HttpPointFailed => "authenticated HTTP point read did not report success",
            Self::HttpPointMismatch => "authenticated HTTP point read returned the wrong resource",
            Self::HttpListFailed => "authenticated HTTP list read did not report success",
            Self::HttpListOmittedResource => {
                "authenticated HTTP list read omitted the compatibility resource"
            }
        };
        formatter.write_str(message)
    }
}

/// Execute the portable application/service/HTTP compatibility expectations.
pub async fn verify_application_compatibility(
    fixture: &impl ApplicationCompatibilityFixture,
) -> Result<ContractReport, ContractViolation> {
    let probe = fixture.application_compatibility_probe().await?;
    if probe.observed_backend_name != probe.expected_backend_name {
        return Err(ContractViolation::WrongApplicationBackend);
    }
    if probe.service_resource_id != probe.expected_resource_id {
        return Err(ContractViolation::ServicePointMismatch);
    }
    if probe.readiness_status != probe.success_status {
        return Err(ContractViolation::ReadinessFailed);
    }
    if probe.point_status != probe.success_status {
        return Err(ContractViolation::HttpPointFailed);
    }
    if probe.point_resource_id != probe.expected_resource_id {
        return Err(ContractViolation::HttpPointMismatch);
    }
    if probe.list_status != probe.success_status {
        return Err(ContractViolation::HttpListFailed);
    }
    if !probe
        .listed_resource_ids
        .contains(&probe.expected_resource_id)
    {
        return Err(ContractViolation::HttpListOmittedResource);
    }
    Ok(ContractReport { checks: 7 })
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

/// Execute the audited-mutation contract against one backend fixture.
pub async fn verify_backend_audit_contract(
    fixture: &impl BackendAuditFixture,
) -> Result<ContractReport, ContractViolation> {
    verify_committed_mutation(fixture.committed_mutation().await?)?;
    verify_unchanged_mutation(fixture.unchanged_mutation().await?)?;
    verify_rollback(fixture.rolled_back_mutation().await?)?;
    verify_fanout(fixture.fanout_to_recording_sink().await?)?;
    verify_telemetry(fixture.telemetry_observations().await?)?;
    verify_revision_conflict(fixture.revision_conflict().await?)?;
    Ok(ContractReport { checks: 6 })
}

fn verify_committed_mutation(probe: CommittedMutationProbe) -> Result<(), ContractViolation> {
    let receipts = probe
        .outcome
        .audits()
        .ok_or(ContractViolation::MissingAuditReceipt)?;
    if receipts.len() != probe.persisted_events.len() {
        return Err(ContractViolation::AuditReceiptCountMismatch);
    }
    let persisted_events = probe
        .persisted_events
        .into_iter()
        .map(StorageRecordedEvent::into_parts)
        .collect::<Vec<_>>();
    for receipt in receipts.iter() {
        let Some((event, before_revision, after_revision)) = persisted_events
            .iter()
            .find(|(event, _, _)| event.id == receipt.sequence())
        else {
            return Err(ContractViolation::ReceiptDoesNotMatchPersistedEvent);
        };
        if event.event_id != receipt.event_id().as_uuid()
            || event.entity_type != receipt.entity_type().as_str()
            || event.action != receipt.action().as_str()
            || *before_revision != receipt.before_revision()
            || *after_revision != receipt.after_revision()
        {
            return Err(ContractViolation::ReceiptDoesNotMatchPersistedEvent);
        }
    }
    Ok(())
}

fn verify_revision_conflict(probe: RevisionConflictProbe) -> Result<(), ContractViolation> {
    let (kind, _, current_revision) = probe.error.into_parts();
    if !matches!(kind, StorageErrorKind::RevisionConflict) {
        return Err(ContractViolation::WrongRevisionConflictKind);
    }
    if !matches!(current_revision, Some(revision) if revision.get() == probe.expected_current_revision.get())
    {
        return Err(ContractViolation::MissingCurrentRevision);
    }
    Ok(())
}

/// Verify retry safety and completion idempotence for one retention batch.
pub fn verify_retention_retry_contract(
    probe: RetentionRetryProbe,
) -> Result<(), ContractViolation> {
    if probe.failed_batch_id.as_uuid() != probe.retried_batch_id.as_uuid() {
        return Err(ContractViolation::RetentionClaimWasReplaced);
    }
    if !probe.event_survived_archive_failure {
        return Err(ContractViolation::RetentionPurgedBeforeArchiveSucceeded);
    }
    if probe.first_completion.purged_events() != probe.repeated_completion.purged_events()
        || probe.first_completion.purged_terminal_deliveries()
            != probe.repeated_completion.purged_terminal_deliveries()
    {
        return Err(ContractViolation::RetentionCompletionWasNotIdempotent);
    }
    Ok(())
}

fn verify_unchanged_mutation(probe: UnchangedMutationProbe) -> Result<(), ContractViolation> {
    if probe.outcome.audits().is_some() || probe.outcome.is_committed() {
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
        assert!(outcome.audits().is_none());
    }
}
