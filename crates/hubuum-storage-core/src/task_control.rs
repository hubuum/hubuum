//! Durable task control values and operation-shaped cancellation requests.

use chrono::{DateTime, Utc};
use hubuum_domain::PrincipalId;
use hubuum_task_core::{TaskCancellationReason, TaskStopReason};

use crate::StorageValidationError;

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
    /// Expiry effects belong to retained history authority, independently of
    /// task cancellation, detail retention and physical cleanup completion.
    HistoryAuthority,
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
            phase: if matches!(kind, StorageTaskKind::HistoryExpiry) {
                StorageTaskExecutionPhase::HistoryAuthority
            } else {
                StorageTaskExecutionPhase::Uncommitted
            },
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
        if (kind == StorageTaskKind::HistoryExpiry)
            != (phase == StorageTaskExecutionPhase::HistoryAuthority)
        {
            return Err(StorageValidationError::invalid(
                "History expiry effects must retain their separate authority",
            ));
        }
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

pub use hubuum_domain::TaskKind as StorageTaskKind;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StorageTaskStatus {
    Queued,
    Validating,
    Running,
    Waiting,
    Succeeded,
    Failed,
    PartiallySucceeded,
    Cancelled,
}

impl StorageTaskStatus {
    pub const ALL: [Self; 8] = [
        Self::Queued,
        Self::Validating,
        Self::Running,
        Self::Waiting,
        Self::Succeeded,
        Self::Failed,
        Self::PartiallySucceeded,
        Self::Cancelled,
    ];
    pub const ACTIVE: [Self; 2] = [Self::Validating, Self::Running];
    pub const TERMINAL: [Self; 4] = [
        Self::Succeeded,
        Self::Failed,
        Self::PartiallySucceeded,
        Self::Cancelled,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Validating => "validating",
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::PartiallySucceeded => "partially_succeeded",
            Self::Cancelled => "cancelled",
        }
    }

    #[must_use]
    pub fn from_persisted(value: &str) -> Option<Self> {
        match value {
            "queued" => Some(Self::Queued),
            "validating" => Some(Self::Validating),
            "running" => Some(Self::Running),
            "waiting" => Some(Self::Waiting),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "partially_succeeded" => Some(Self::PartiallySucceeded),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }

    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Validating | Self::Running)
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::PartiallySucceeded | Self::Cancelled
        )
    }
}
