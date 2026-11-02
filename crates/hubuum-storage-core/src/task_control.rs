//! Durable task control values and operation-shaped cancellation requests.

mod snapshot;

pub(crate) const SNAPSHOT_FIELDS: &[&str] = &[
    "cancel_requested_at",
    "cancel_requested_by",
    "cancel_reason",
    "execution_deadline_at",
    "import_effects_committed_at",
    "remote_dispatched_at",
    "terminal_reason",
];

use chrono::{DateTime, Utc};
use hubuum_domain::{PrincipalId, TaskId};
use hubuum_events_core::EventContext;
use hubuum_task_core::{TaskCancellationReason, TaskExecutionLimit, TaskStopReason};

use crate::{
    StorageRemoteCallArtifactTarget, StorageTask, StorageTaskKind, StorageTaskLease,
    StorageTaskStatus, StorageValidationError,
};

/// Persisted cancellation intent. Actor deletion can remove the live principal
/// reference; the accompanying audit event retains the historical provenance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageTaskCancellation {
    requested_at: DateTime<Utc>,
    requested_by: Option<PrincipalId>,
    reason: Option<TaskCancellationReason>,
}

impl StorageTaskCancellation {
    #[must_use]
    pub const fn new(
        requested_at: DateTime<Utc>,
        requested_by: Option<PrincipalId>,
        reason: Option<TaskCancellationReason>,
    ) -> Self {
        Self {
            requested_at,
            requested_by,
            reason,
        }
    }

    #[must_use]
    pub const fn requested_at(&self) -> DateTime<Utc> {
        self.requested_at
    }

    #[must_use]
    pub const fn requested_by(&self) -> Option<PrincipalId> {
        self.requested_by
    }

    #[must_use]
    pub const fn reason(&self) -> Option<&TaskCancellationReason> {
        self.reason.as_ref()
    }
}

/// Durable evidence determining whether cancellation can prevent effects.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StorageTaskExecutionPhase {
    #[default]
    Uncommitted,
    ImportCommitted {
        at: DateTime<Utc>,
    },
    RemoteDispatched {
        at: DateTime<Utc>,
    },
}

/// Validated state carried intact from persistence into task projections.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageTaskControl {
    kind: StorageTaskKind,
    cancellation: Option<StorageTaskCancellation>,
    deadline: Option<DateTime<Utc>>,
    phase: StorageTaskExecutionPhase,
    terminal_reason: Option<TaskStopReason>,
}

impl StorageTaskControl {
    #[must_use]
    pub const fn new(kind: StorageTaskKind) -> Self {
        Self {
            kind,
            cancellation: None,
            deadline: None,
            phase: StorageTaskExecutionPhase::Uncommitted,
            terminal_reason: None,
        }
    }

    #[must_use]
    pub const fn kind(&self) -> StorageTaskKind {
        self.kind
    }

    pub fn request_cancellation(&mut self, cancellation: StorageTaskCancellation) {
        self.cancellation.get_or_insert(cancellation);
    }

    /// Mirror a deleted actor's nullable foreign key without erasing durable
    /// cancellation intent or its original timestamp and bounded explanation.
    pub fn forget_actor(&mut self, actor: PrincipalId) {
        if let Some(cancellation) = &mut self.cancellation
            && cancellation.requested_by == Some(actor)
        {
            cancellation.requested_by = None;
        }
    }

    pub fn admit_deadline(&mut self, deadline: DateTime<Utc>) {
        self.deadline.get_or_insert(deadline);
    }

    pub fn record_import_commit(
        &mut self,
        at: DateTime<Utc>,
    ) -> Result<(), StorageValidationError> {
        if self.kind != StorageTaskKind::Import {
            return Err(StorageValidationError::invalid(
                "Only imports can commit import effects",
            ));
        }
        self.phase = StorageTaskExecutionPhase::ImportCommitted { at };
        Ok(())
    }

    pub fn record_remote_dispatch(
        &mut self,
        at: DateTime<Utc>,
    ) -> Result<(), StorageValidationError> {
        if self.kind != StorageTaskKind::RemoteCall
            || self.phase != StorageTaskExecutionPhase::Uncommitted
        {
            return Err(StorageValidationError::invalid(
                "Remote dispatch requires a remote task that has not dispatched",
            ));
        }
        self.phase = StorageTaskExecutionPhase::RemoteDispatched { at };
        Ok(())
    }

    pub fn acknowledge_stop(
        &mut self,
        at: DateTime<Utc>,
    ) -> Result<TaskStopReason, StorageValidationError> {
        let reason = self.stop_reason(at).ok_or_else(|| {
            StorageValidationError::invalid("Task has no stop request or expired deadline")
        })?;
        self.terminal_reason = Some(reason);
        Ok(reason)
    }

    pub fn try_new(
        kind: StorageTaskKind,
        cancellation: Option<StorageTaskCancellation>,
        deadline: Option<DateTime<Utc>>,
        phase: StorageTaskExecutionPhase,
        terminal_reason: Option<TaskStopReason>,
    ) -> Result<Self, StorageValidationError> {
        match phase {
            StorageTaskExecutionPhase::ImportCommitted { .. }
                if kind != StorageTaskKind::Import =>
            {
                return Err(StorageValidationError::invalid(
                    "Only imports can have committed import effects",
                ));
            }
            StorageTaskExecutionPhase::RemoteDispatched { .. }
                if kind != StorageTaskKind::RemoteCall =>
            {
                return Err(StorageValidationError::invalid(
                    "Only remote calls can have dispatched HTTP effects",
                ));
            }
            _ => {}
        }
        if terminal_reason == Some(TaskStopReason::Cancelled) && cancellation.is_none() {
            return Err(StorageValidationError::invalid(
                "Acknowledged cancellation requires persisted intent",
            ));
        }
        if terminal_reason == Some(TaskStopReason::DeadlineExceeded) && deadline.is_none() {
            return Err(StorageValidationError::invalid(
                "Deadline termination requires a persisted deadline",
            ));
        }
        Ok(Self {
            kind,
            cancellation,
            deadline,
            phase,
            terminal_reason,
        })
    }

    #[must_use]
    pub const fn cancellation(&self) -> Option<&StorageTaskCancellation> {
        self.cancellation.as_ref()
    }

    #[must_use]
    pub const fn deadline(&self) -> Option<DateTime<Utc>> {
        self.deadline
    }

    #[must_use]
    pub const fn phase(&self) -> StorageTaskExecutionPhase {
        self.phase
    }

    #[must_use]
    pub const fn terminal_reason(&self) -> Option<TaskStopReason> {
        self.terminal_reason
    }

    /// The earliest durable cause wins. Callers still arbitrate against already
    /// committed effects inside the owning transaction before finalization.
    #[must_use]
    pub fn stop_reason(&self, now: DateTime<Utc>) -> Option<TaskStopReason> {
        if let Some(cancellation) = &self.cancellation
            && self
                .deadline
                .is_none_or(|deadline| cancellation.requested_at <= deadline)
        {
            return Some(TaskStopReason::Cancelled);
        }
        if self.deadline.is_some_and(|deadline| deadline <= now) {
            return Some(TaskStopReason::DeadlineExceeded);
        }
        self.cancellation
            .as_ref()
            .map(|_| TaskStopReason::Cancelled)
    }
}

/// A control observation evaluated against the backend's authoritative clock.
/// Consumers must not reconstruct it using a replica's wall clock.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageTaskExecutionObservation {
    control: StorageTaskControl,
    observed_at: DateTime<Utc>,
}
impl StorageTaskExecutionObservation {
    #[must_use]
    pub const fn new(control: StorageTaskControl, observed_at: DateTime<Utc>) -> Self {
        Self {
            control,
            observed_at,
        }
    }
    #[must_use]
    pub const fn control(&self) -> &StorageTaskControl {
        &self.control
    }
    #[must_use]
    pub fn stop_reason(&self) -> Option<TaskStopReason> {
        self.control.stop_reason(self.observed_at)
    }
    #[must_use]
    pub fn remaining(&self) -> Option<std::time::Duration> {
        self.control.deadline().map(|deadline| {
            (deadline - self.observed_at)
                .to_std()
                .unwrap_or(std::time::Duration::ZERO)
        })
    }
}

/// Cancellation of the exact task identity/ownership authorized by the service.
/// Adapters must compare the captured kind and submitter again under their lock.
#[derive(Clone, Debug)]
pub struct StorageTaskCancellationRequest {
    task_id: TaskId,
    kind: StorageTaskKind,
    submitted_by: Option<PrincipalId>,
    context: EventContext,
    reason: Option<TaskCancellationReason>,
    expected_status: Option<StorageTaskStatus>,
}

impl StorageTaskCancellationRequest {
    #[must_use]
    pub fn new(task: &StorageTask, context: EventContext) -> Self {
        Self {
            task_id: task.id(),
            kind: task.kind(),
            submitted_by: task.submitted_by(),
            context,
            reason: None,
            expected_status: None,
        }
    }

    #[must_use]
    pub fn reason(mut self, reason: Option<TaskCancellationReason>) -> Self {
        self.reason = reason;
        self
    }

    #[must_use]
    pub const fn expected_status(mut self, status: Option<StorageTaskStatus>) -> Self {
        self.expected_status = status;
        self
    }

    #[must_use]
    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }
    #[must_use]
    pub const fn kind(&self) -> StorageTaskKind {
        self.kind
    }
    #[must_use]
    pub const fn submitted_by(&self) -> Option<PrincipalId> {
        self.submitted_by
    }
    #[must_use]
    pub const fn event_context(&self) -> &EventContext {
        &self.context
    }
    #[must_use]
    pub const fn cancellation_reason(&self) -> Option<&TaskCancellationReason> {
        self.reason.as_ref()
    }
    #[must_use]
    pub const fn required_status(&self) -> Option<StorageTaskStatus> {
        self.expected_status
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageTaskCancellationChange {
    CancelledQueued,
    Requested,
    Unchanged,
}

#[derive(Clone, Debug)]
pub struct StorageTaskCancellationOutcome {
    task: StorageTask,
    change: StorageTaskCancellationChange,
}

impl StorageTaskCancellationOutcome {
    #[must_use]
    pub const fn new(task: StorageTask, change: StorageTaskCancellationChange) -> Self {
        Self { task, change }
    }
    #[must_use]
    pub const fn task(&self) -> &StorageTask {
        &self.task
    }
    #[must_use]
    pub const fn change(&self) -> StorageTaskCancellationChange {
        self.change
    }
    #[must_use]
    pub fn into_task(self) -> StorageTask {
        self.task
    }
}

/// First-claim admission pins a policy-derived deadline using the adapter's
/// trusted clock. Subsequent claims must retain the original deadline.
#[derive(Clone, Debug)]
pub struct StorageTaskExecutionAdmission {
    lease: StorageTaskLease,
    limit: TaskExecutionLimit,
}

impl StorageTaskExecutionAdmission {
    #[must_use]
    pub const fn new(lease: StorageTaskLease, limit: TaskExecutionLimit) -> Self {
        Self { lease, limit }
    }
    #[must_use]
    pub fn into_parts(self) -> (StorageTaskLease, TaskExecutionLimit) {
        (self.lease, self.limit)
    }
}

/// A remote dispatch may start only after this record is durably admitted under
/// the live lease and an uncancelled, unexpired task control state.
#[derive(Clone, Debug)]
pub struct StorageTaskRemoteDispatch {
    lease: StorageTaskLease,
    target: StorageRemoteCallArtifactTarget,
}

impl StorageTaskRemoteDispatch {
    #[must_use]
    pub const fn new(lease: StorageTaskLease, target: StorageRemoteCallArtifactTarget) -> Self {
        Self { lease, target }
    }
    #[must_use]
    pub fn into_parts(self) -> (StorageTaskLease, StorageRemoteCallArtifactTarget) {
        (self.lease, self.target)
    }
}

pub(crate) fn validate_control_snapshot(
    row: &crate::StorageBackupRow,
) -> Result<(), StorageValidationError> {
    if SNAPSHOT_FIELDS
        .iter()
        .all(|field| row.get(field).is_none_or(serde_json::Value::is_null))
    {
        return Ok(());
    }
    let kind = row
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .and_then(StorageTaskKind::from_persisted)
        .ok_or_else(|| {
            StorageValidationError::invalid("Task control requires a recognized task kind")
        })?;
    StorageTaskControl::from_snapshot(kind, row)?;
    Ok(())
}
