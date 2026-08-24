//! Reusable behavioral certification for Hubuum storage adapters.
//!
//! Rust trait bounds prove that an adapter has methods with the right shapes.
//! This crate tests the semantic obligations that the type system cannot
//! express: durable audit receipts, transaction rollback, outbox delivery,
//! and bounded logical/native observations.

use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::sync::Mutex;

use async_trait::async_trait;
use hubuum_domain::ResourceRevision;
use hubuum_storage_core::{
    EventRetentionBatchId, EventRetentionSummary, MutationOutcome, StorageError, StorageErrorKind,
    StorageObservation, StorageObserver, StorageRecordedEvent,
};

/// Thread-safe application observer used by backend contract fixtures.
#[derive(Debug, Default)]
pub struct RecordingStorageObserver {
    observations: Mutex<Vec<StorageObservation>>,
}

impl RecordingStorageObserver {
    #[must_use]
    pub fn observations(&self) -> Vec<StorageObservation> {
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

impl StorageObserver for RecordingStorageObserver {
    fn operation_finished(&self, observation: &StorageObservation) {
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
    observed_backend_name: String,
    service_resource_id: i32,
    readiness_status: u16,
    point_status: u16,
    point_resource_id: i32,
    list_status: u16,
    listed_resource_ids: Vec<i32>,
}

impl ApplicationCompatibilityProbe {
    #[must_use]
    pub fn builder(
        observed_backend_name: impl Into<String>,
    ) -> ApplicationCompatibilityProbeBuilder {
        ApplicationCompatibilityProbeBuilder {
            observed_backend_name: observed_backend_name.into(),
            service_resource_id: None,
            readiness_status: None,
            point_status: None,
            point_resource_id: None,
            list_status: None,
            listed_resource_ids: None,
        }
    }
}

/// Expectations supplied independently of the backend fixture evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplicationCompatibilityExpectations {
    backend_name: String,
    resource_id: i32,
    success_status: u16,
}

impl ApplicationCompatibilityExpectations {
    #[must_use]
    pub fn new(backend_name: impl Into<String>, resource_id: i32, success_status: u16) -> Self {
        Self {
            backend_name: backend_name.into(),
            resource_id,
            success_status,
        }
    }
}

/// Validating construction for a complete application compatibility probe.
pub struct ApplicationCompatibilityProbeBuilder {
    observed_backend_name: String,
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
            observed_backend_name: self.observed_backend_name,
            service_resource_id: self
                .service_resource_id
                .ok_or_else(|| missing("the service resource identifier"))?,
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
pub struct ObservationProbe {
    logical_resource_operation_count: usize,
    logical_capability_operation_count: usize,
    native_operation_count: usize,
    failure_observation_count: usize,
}

impl ObservationProbe {
    #[must_use]
    pub const fn new(
        logical_resource_operation_count: usize,
        logical_capability_operation_count: usize,
        native_operation_count: usize,
        failure_observation_count: usize,
    ) -> Self {
        Self {
            logical_resource_operation_count,
            logical_capability_operation_count,
            native_operation_count,
            failure_observation_count,
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

    async fn observations(&self) -> Result<ObservationProbe, FixtureError>;

    async fn revision_conflict(&self) -> Result<RevisionConflictProbe, FixtureError>;

    /// Remove every resource provisioned by this fixture. The conformance
    /// runner invokes this even when an earlier probe fails.
    async fn cleanup(&self) -> Result<(), FixtureError>;
}

/// One injected transactional failure and evidence that its state transition
/// was rolled back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransactionFaultProbe {
    error_kind: StorageErrorKind,
    rollback_preserved: bool,
}

impl TransactionFaultProbe {
    #[must_use]
    pub const fn new(error_kind: StorageErrorKind, rollback_preserved: bool) -> Self {
        Self {
            error_kind,
            rollback_preserved,
        }
    }
}

/// Recovery evidence after delivery claim and acknowledgement failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeliveryRecoveryProbe {
    failure_persisted: bool,
    attempt_preserved: bool,
    claim_token_rotated: bool,
    retry_completed: bool,
}

impl DeliveryRecoveryProbe {
    #[must_use]
    pub const fn new(
        failure_persisted: bool,
        attempt_preserved: bool,
        claim_token_rotated: bool,
        retry_completed: bool,
    ) -> Self {
        Self {
            failure_persisted,
            attempt_preserved,
            claim_token_rotated,
            retry_completed,
        }
    }
}

/// Evidence for deterministic delivery failure and retry behavior.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeliveryFaultProbe {
    claim_failure: TransactionFaultProbe,
    acknowledgement_failure: TransactionFaultProbe,
    recovery: DeliveryRecoveryProbe,
}

impl DeliveryFaultProbe {
    #[must_use]
    pub const fn new(
        claim_failure: TransactionFaultProbe,
        acknowledgement_failure: TransactionFaultProbe,
        recovery: DeliveryRecoveryProbe,
    ) -> Self {
        Self {
            claim_failure,
            acknowledgement_failure,
            recovery,
        }
    }
}

/// Evidence for rollback-safe restore coordinator transitions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestoreCoordinationFaultProbe {
    heartbeat_failure: TransactionFaultProbe,
    transition_failure: TransactionFaultProbe,
    coordinator_remained_normal: bool,
}

impl RestoreCoordinationFaultProbe {
    #[must_use]
    pub const fn new(
        heartbeat_failure: TransactionFaultProbe,
        transition_failure: TransactionFaultProbe,
        coordinator_remained_normal: bool,
    ) -> Self {
        Self {
            heartbeat_failure,
            transition_failure,
            coordinator_remained_normal,
        }
    }
}

/// Evidence that lease ownership is lost permanently after renewal failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaseLossFaultProbe {
    renewal_error_kind: StorageErrorKind,
    recovered_as_failed: bool,
    lease_cleared: bool,
    request_payload_cleared: bool,
    stale_renewal_rejected: bool,
}

impl LeaseLossFaultProbe {
    #[must_use]
    pub const fn new(
        renewal_error_kind: StorageErrorKind,
        recovered_as_failed: bool,
        lease_cleared: bool,
        request_payload_cleared: bool,
        stale_renewal_rejected: bool,
    ) -> Self {
        Self {
            renewal_error_kind,
            recovered_as_failed,
            lease_cleared,
            request_payload_cleared,
            stale_renewal_rejected,
        }
    }
}

/// Adapter-owned delivery fault injection consumed by the portable runner.
#[async_trait]
pub trait DeliveryFaultFixture: Send + Sync {
    async fn delivery_fault_probe(&self) -> Result<DeliveryFaultProbe, FixtureError>;
}

/// Adapter-owned restore fault injection consumed by the portable runner.
#[async_trait]
pub trait RestoreCoordinationFaultFixture: Send + Sync {
    async fn restore_coordination_fault_probe(
        &self,
    ) -> Result<RestoreCoordinationFaultProbe, FixtureError>;
}

/// Adapter-owned lease fault injection consumed by the portable runner.
#[async_trait]
pub trait LeaseLossFaultFixture: Send + Sync {
    async fn lease_loss_fault_probe(&self) -> Result<LeaseLossFaultProbe, FixtureError>;
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
    MissingLogicalResourceObservation,
    MissingLogicalCapabilityObservation,
    MissingNativeObservation,
    MissingFailureObservation,
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
    FaultDidNotReportBackendFailure,
    DeliveryClaimWasNotRecoverable,
    DeliveryAcknowledgementWasNotRecoverable,
    DeliveryFailureWasNotPersisted,
    DeliveryAttemptWasNotPreserved,
    DeliveryClaimTokenWasReused,
    DeliveryRetryDidNotComplete,
    RestoreHeartbeatWasPublished,
    RestoreTransitionWasPersisted,
    RestoreCoordinatorStateChanged,
    LeaseWasNotRecoveredAsFailed,
    LeaseWasNotCleared,
    LeasePayloadWasNotCleared,
    StaleLeaseRegainedOwnership,
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
            Self::MissingLogicalResourceObservation => {
                "resource mutation produced no logical storage observation"
            }
            Self::MissingLogicalCapabilityObservation => {
                "non-resource capability produced no logical storage observation"
            }
            Self::MissingNativeObservation => "mutation produced no native backend observation",
            Self::MissingFailureObservation => {
                "rolled-back mutation produced no failure observation"
            }
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
            Self::FaultDidNotReportBackendFailure => {
                "injected storage fault did not report a backend failure"
            }
            Self::DeliveryClaimWasNotRecoverable => {
                "failed delivery claim did not roll back to claimable state"
            }
            Self::DeliveryAcknowledgementWasNotRecoverable => {
                "failed delivery acknowledgement invalidated the active claim"
            }
            Self::DeliveryFailureWasNotPersisted => {
                "delivery failure state was not persisted after recovery"
            }
            Self::DeliveryAttemptWasNotPreserved => {
                "delivery retry did not preserve the authoritative attempt count"
            }
            Self::DeliveryClaimTokenWasReused => "delivery retry reused the previous claim token",
            Self::DeliveryRetryDidNotComplete => "delivery retry did not complete successfully",
            Self::RestoreHeartbeatWasPublished => {
                "failed restore heartbeat was visible after rollback"
            }
            Self::RestoreTransitionWasPersisted => {
                "failed restore transition changed the staged job"
            }
            Self::RestoreCoordinatorStateChanged => {
                "failed restore transition changed coordinator state"
            }
            Self::LeaseWasNotRecoveredAsFailed => "expired task lease was not finalized as failed",
            Self::LeaseWasNotCleared => "expired task lease retained its ownership state",
            Self::LeasePayloadWasNotCleared => "expired task lease retained its request payload",
            Self::StaleLeaseRegainedOwnership => "stale task lease regained ownership",
        };
        formatter.write_str(message)
    }
}

/// Execute the portable application/service/HTTP compatibility expectations.
pub async fn verify_application_compatibility(
    fixture: &impl ApplicationCompatibilityFixture,
    expectations: &ApplicationCompatibilityExpectations,
) -> Result<ContractReport, ContractViolation> {
    let probe = fixture.application_compatibility_probe().await?;
    if probe.observed_backend_name != expectations.backend_name {
        return Err(ContractViolation::WrongApplicationBackend);
    }
    if probe.service_resource_id != expectations.resource_id {
        return Err(ContractViolation::ServicePointMismatch);
    }
    if probe.readiness_status != expectations.success_status {
        return Err(ContractViolation::ReadinessFailed);
    }
    if probe.point_status != expectations.success_status {
        return Err(ContractViolation::HttpPointFailed);
    }
    if probe.point_resource_id != expectations.resource_id {
        return Err(ContractViolation::HttpPointMismatch);
    }
    if probe.list_status != expectations.success_status {
        return Err(ContractViolation::HttpListFailed);
    }
    if !probe
        .listed_resource_ids
        .contains(&expectations.resource_id)
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
    let verification = async {
        verify_committed_mutation(fixture.committed_mutation().await?)?;
        verify_unchanged_mutation(fixture.unchanged_mutation().await?)?;
        verify_rollback(fixture.rolled_back_mutation().await?)?;
        verify_fanout(fixture.fanout_to_recording_sink().await?)?;
        verify_observations(fixture.observations().await?)?;
        verify_revision_conflict(fixture.revision_conflict().await?)?;
        Ok(ContractReport { checks: 6 })
    }
    .await;
    let cleanup = fixture.cleanup().await.map_err(ContractViolation::Fixture);

    match (verification, cleanup) {
        (Err(violation), _) | (Ok(_), Err(violation)) => Err(violation),
        (Ok(report), Ok(())) => Ok(report),
    }
}

/// Execute portable delivery claim, acknowledgement, and retry expectations.
pub async fn verify_delivery_fault_contract(
    fixture: &impl DeliveryFaultFixture,
) -> Result<ContractReport, ContractViolation> {
    let probe = fixture.delivery_fault_probe().await?;
    verify_backend_fault(
        probe.claim_failure,
        ContractViolation::DeliveryClaimWasNotRecoverable,
    )?;
    verify_backend_fault(
        probe.acknowledgement_failure,
        ContractViolation::DeliveryAcknowledgementWasNotRecoverable,
    )?;
    if !probe.recovery.failure_persisted {
        return Err(ContractViolation::DeliveryFailureWasNotPersisted);
    }
    if !probe.recovery.attempt_preserved {
        return Err(ContractViolation::DeliveryAttemptWasNotPreserved);
    }
    if !probe.recovery.claim_token_rotated {
        return Err(ContractViolation::DeliveryClaimTokenWasReused);
    }
    if !probe.recovery.retry_completed {
        return Err(ContractViolation::DeliveryRetryDidNotComplete);
    }
    Ok(ContractReport { checks: 8 })
}

/// Execute portable restore-coordination rollback expectations.
pub async fn verify_restore_coordination_fault_contract(
    fixture: &impl RestoreCoordinationFaultFixture,
) -> Result<ContractReport, ContractViolation> {
    let probe = fixture.restore_coordination_fault_probe().await?;
    verify_backend_fault(
        probe.heartbeat_failure,
        ContractViolation::RestoreHeartbeatWasPublished,
    )?;
    verify_backend_fault(
        probe.transition_failure,
        ContractViolation::RestoreTransitionWasPersisted,
    )?;
    if !probe.coordinator_remained_normal {
        return Err(ContractViolation::RestoreCoordinatorStateChanged);
    }
    Ok(ContractReport { checks: 5 })
}

/// Execute portable lease-loss recovery expectations.
pub async fn verify_lease_loss_fault_contract(
    fixture: &impl LeaseLossFaultFixture,
) -> Result<ContractReport, ContractViolation> {
    let probe = fixture.lease_loss_fault_probe().await?;
    if probe.renewal_error_kind != StorageErrorKind::Backend {
        return Err(ContractViolation::FaultDidNotReportBackendFailure);
    }
    if !probe.recovered_as_failed {
        return Err(ContractViolation::LeaseWasNotRecoveredAsFailed);
    }
    if !probe.lease_cleared {
        return Err(ContractViolation::LeaseWasNotCleared);
    }
    if !probe.request_payload_cleared {
        return Err(ContractViolation::LeasePayloadWasNotCleared);
    }
    if !probe.stale_renewal_rejected {
        return Err(ContractViolation::StaleLeaseRegainedOwnership);
    }
    Ok(ContractReport { checks: 5 })
}

fn verify_backend_fault(
    probe: TransactionFaultProbe,
    rollback_violation: ContractViolation,
) -> Result<(), ContractViolation> {
    if probe.error_kind != StorageErrorKind::Backend {
        return Err(ContractViolation::FaultDidNotReportBackendFailure);
    }
    if !probe.rollback_preserved {
        return Err(rollback_violation);
    }
    Ok(())
}

fn verify_committed_mutation(probe: CommittedMutationProbe) -> Result<(), ContractViolation> {
    let receipts = probe
        .outcome
        .audits()
        .ok_or(ContractViolation::MissingAuditReceipt)?;
    if receipts.len() != probe.persisted_events.len() {
        return Err(ContractViolation::AuditReceiptCountMismatch);
    }
    let mut persisted_events = probe
        .persisted_events
        .into_iter()
        .map(StorageRecordedEvent::into_parts)
        .collect::<Vec<_>>();
    let mut receipt_sequences = HashSet::with_capacity(receipts.len());
    let mut receipt_event_ids = HashSet::with_capacity(receipts.len());
    for receipt in receipts.iter() {
        if !receipt_sequences.insert(receipt.sequence())
            || !receipt_event_ids.insert(receipt.event_id())
        {
            return Err(ContractViolation::ReceiptDoesNotMatchPersistedEvent);
        }
        let Some(event_index) = persisted_events
            .iter()
            .position(|(event, _, _)| event.id() == receipt.sequence())
        else {
            return Err(ContractViolation::ReceiptDoesNotMatchPersistedEvent);
        };
        let (event, before_revision, after_revision) = persisted_events.swap_remove(event_index);
        if event.event_id() != receipt.event_id().as_uuid()
            || event.entity_type() != receipt.entity_type()
            || event.action() != receipt.action()
            || before_revision != receipt.before_revision()
            || after_revision != receipt.after_revision()
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

const fn verify_observations(probe: ObservationProbe) -> Result<(), ContractViolation> {
    if probe.logical_resource_operation_count == 0 {
        return Err(ContractViolation::MissingLogicalResourceObservation);
    }
    if probe.logical_capability_operation_count == 0 {
        return Err(ContractViolation::MissingLogicalCapabilityObservation);
    }
    if probe.native_operation_count == 0 {
        return Err(ContractViolation::MissingNativeObservation);
    }
    if probe.failure_observation_count == 0 {
        return Err(ContractViolation::MissingFailureObservation);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use hubuum_events_core::{
        Action, ActorKind, EntityType, EventEnvelope, EventId, EventSequence,
    };
    use hubuum_storage_core::{AuditReceipt, AuditReceipts};

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

    #[test]
    fn committed_probe_rejects_duplicate_receipts_that_hide_a_persisted_event() {
        let first_event = recorded_event(1);
        let duplicated_receipt = first_event
            .clone()
            .into_audit_receipt()
            .expect("valid audit receipt");
        let outcome = MutationOutcome::committed_with_audits(
            (),
            AuditReceipts::new(duplicated_receipt.clone(), vec![duplicated_receipt]),
        );
        let probe =
            CommittedMutationProbe::with_events(outcome, vec![first_event, recorded_event(2)]);

        assert!(matches!(
            verify_committed_mutation(probe),
            Err(ContractViolation::ReceiptDoesNotMatchPersistedEvent)
        ));
    }

    #[test]
    fn observation_probe_requires_non_resource_capability_evidence() {
        let probe = ObservationProbe::new(1, 0, 1, 1);

        assert!(matches!(
            verify_observations(probe),
            Err(ContractViolation::MissingLogicalCapabilityObservation)
        ));
    }

    fn recorded_event(sequence: i64) -> StorageRecordedEvent {
        let event = EventEnvelope::builder()
            .id(EventSequence::new(sequence).expect("positive event sequence"))
            .event_id(EventId::new().as_uuid())
            .occurred_at(
                "2026-08-24T12:00:00Z"
                    .parse()
                    .expect("valid event timestamp"),
            )
            .entity_type(EntityType::Collection)
            .action(Action::Created)
            .actor_kind(ActorKind::System)
            .summary("storage conformance test event".to_string())
            .try_build()
            .expect("valid event envelope");
        StorageRecordedEvent::new(
            event,
            None,
            Some(ResourceRevision::new(sequence).expect("positive revision")),
        )
    }
}
