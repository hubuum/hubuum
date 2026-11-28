use std::time::Duration;

use chrono::{DateTime, Utc};
use hubuum_domain::TaskIdentity;

use super::*;
use crate::{StorageTaskKind, StorageTaskTerminalStatus, TaskRemoteSideEffectState};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskReceiptOutcome {
    MayExecute,
    Terminal {
        status: StorageTaskTerminalStatus,
        finished_at: DateTime<Utc>,
        remote_side_effect: Option<TaskRemoteSideEffectState>,
    },
    RecoveryUnknown {
        settled_at: DateTime<Utc>,
    },
}

impl TaskReceiptOutcome {
    pub const fn terminal_at(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::MayExecute => None,
            Self::Terminal { finished_at, .. } => Some(*finished_at),
            Self::RecoveryUnknown { settled_at } => Some(*settled_at),
        }
    }
}

#[derive(Clone, Debug)]
pub struct TaskReceiptAdmission {
    lookup: TaskSubmissionLookup,
    task: TaskIdentity,
    incarnation: Uuid,
    admitted_at: DateTime<Utc>,
}

impl TaskReceiptAdmission {
    pub fn new(
        lookup: TaskSubmissionLookup,
        task: TaskIdentity,
        incarnation: Uuid,
        admitted_at: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        if incarnation.is_nil() {
            return Err(invalid("Task admission incarnation must not be nil"));
        }
        Ok(Self {
            lookup,
            task,
            incarnation,
            admitted_at,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "ReceiptInput")]
pub struct TaskReceipt {
    ticket: TaskAdmissionTicket,
    kind: StorageTaskKind,
    fingerprint: TaskFingerprint,
    admitted_at: DateTime<Utc>,
    policy_revision: TaskRetentionPolicyRevision,
    window_secs: u64,
    details_after_terminal_secs: Option<u64>,
    outcome: TaskReceiptOutcome,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptInput {
    ticket: TaskAdmissionTicket,
    kind: StorageTaskKind,
    fingerprint: TaskFingerprint,
    admitted_at: DateTime<Utc>,
    policy_revision: TaskRetentionPolicyRevision,
    window_secs: u64,
    details_after_terminal_secs: Option<u64>,
    outcome: TaskReceiptOutcome,
}

pub(super) fn add_duration(
    at: DateTime<Utc>,
    duration: Duration,
) -> Result<DateTime<Utc>, StorageValidationError> {
    chrono::Duration::from_std(duration)
        .ok()
        .and_then(|duration| at.checked_add_signed(duration))
        .ok_or_else(|| invalid("Task retention deadline overflow"))
}

impl TaskReceipt {
    pub fn admit(
        request: TaskReceiptAdmission,
        policy: &TaskRetentionPolicy,
        revision: TaskControlRevision,
    ) -> Result<Self, StorageValidationError> {
        let ticket = TaskAdmissionTicket::new(
            TaskReceiptReference::new(
                request.lookup.identity().clone(),
                TaskReceiptGeneration::generate(),
                request.task,
            ),
            revision,
            request.incarnation,
            add_duration(request.admitted_at, policy.pending_protection_timeout())?,
        )?;
        Self::from_parts(
            ticket,
            request.lookup.kind(),
            TaskFingerprint::CanonicalV1(request.lookup.canonical().clone()),
            request.admitted_at,
            policy,
            TaskReceiptOutcome::MayExecute,
        )
    }

    /// Adapter construction for validated persisted/backfilled values. The
    /// original admission and terminal clocks must be retained during backfill.
    pub fn from_parts(
        ticket: TaskAdmissionTicket,
        kind: StorageTaskKind,
        fingerprint: TaskFingerprint,
        admitted_at: DateTime<Utc>,
        policy: &TaskRetentionPolicy,
        outcome: TaskReceiptOutcome,
    ) -> Result<Self, StorageValidationError> {
        Self::try_from(ReceiptInput {
            ticket,
            kind,
            fingerprint,
            admitted_at,
            policy_revision: policy.revision(),
            window_secs: policy.deduplication_after_terminal().as_secs(),
            details_after_terminal_secs: policy
                .details_after_terminal()
                .map(|value| value.as_secs()),
            outcome,
        })
    }

    pub const fn ticket(&self) -> &TaskAdmissionTicket {
        &self.ticket
    }
    pub const fn reference(&self) -> &TaskReceiptReference {
        self.ticket.receipt()
    }
    pub const fn kind(&self) -> StorageTaskKind {
        self.kind
    }
    pub const fn fingerprint(&self) -> &TaskFingerprint {
        &self.fingerprint
    }
    pub const fn admitted_at(&self) -> DateTime<Utc> {
        self.admitted_at
    }
    pub const fn policy_revision(&self) -> TaskRetentionPolicyRevision {
        self.policy_revision
    }
    pub const fn window(&self) -> Duration {
        Duration::from_secs(self.window_secs)
    }
    pub fn details_after_terminal(&self) -> Option<Duration> {
        self.details_after_terminal_secs.map(Duration::from_secs)
    }
    pub const fn outcome(&self) -> &TaskReceiptOutcome {
        &self.outcome
    }
    pub fn expires_at(&self) -> Option<DateTime<Utc>> {
        self.outcome
            .terminal_at()
            .map(|at| add_duration(at, self.window()).expect("validated receipt deadline"))
    }
    pub fn details_expires_at(&self) -> Option<DateTime<Utc>> {
        self.outcome
            .terminal_at()
            .zip(self.details_after_terminal())
            .map(|(at, duration)| add_duration(at, duration).expect("validated receipt deadline"))
    }
    pub fn expired_projection(&self) -> ExpiredTaskSubmission {
        ExpiredTaskSubmission::new(
            self.reference().task(),
            self.kind,
            self.expires_at(),
            self.reference().submission().principal(),
        )
    }
    pub fn settle(&self, outcome: TaskReceiptOutcome) -> Result<Self, StorageValidationError> {
        if self.outcome != TaskReceiptOutcome::MayExecute && self.outcome != outcome {
            return Err(invalid(
                "Original task terminal outcome and anchor are immutable",
            ));
        }
        let mut next = self.clone();
        next.outcome = outcome;
        next.validate()?;
        Ok(next)
    }
    pub fn covers(&self, previous: &Self) -> bool {
        let mut immutable = self.clone();
        immutable.outcome = previous.outcome.clone();
        immutable == *previous
            && (previous.outcome == TaskReceiptOutcome::MayExecute
                || self.outcome == previous.outcome)
    }
    pub fn encoded_bytes(&self) -> Result<usize, StorageValidationError> {
        serde_json::to_vec(self)
            .map(|bytes| bytes.len())
            .map_err(|_| invalid("Task receipt cannot be encoded"))
    }
    fn validate(&self) -> Result<(), StorageValidationError> {
        if self.window_secs == 0
            || policy::duration_seconds(self.window()).is_err()
            || self.ticket.protection_deadline() <= self.admitted_at
        {
            return Err(invalid("Invalid task receipt clocks"));
        }
        if let Some(duration) = self.details_after_terminal() {
            policy::duration_seconds(duration)?;
        }
        if let Some(at) = self.outcome.terminal_at() {
            if at < self.admitted_at {
                return Err(invalid("Task terminal anchor precedes admission"));
            }
            add_duration(at, self.window())?;
            if let Some(duration) = self.details_after_terminal() {
                add_duration(at, duration)?;
            }
        }
        if let TaskReceiptOutcome::Terminal {
            remote_side_effect, ..
        } = &self.outcome
            && remote_side_effect.is_some() != (self.kind == StorageTaskKind::RemoteCall)
        {
            return Err(invalid(
                "Remote side-effect knowledge must match the receipt task kind",
            ));
        }
        if self.encoded_bytes()? > TASK_CONTROL_MAX_RECEIPT_BYTES {
            return Err(invalid("Task receipt exceeds its encoded byte bound"));
        }
        Ok(())
    }
}

impl TryFrom<ReceiptInput> for TaskReceipt {
    type Error = StorageValidationError;
    fn try_from(v: ReceiptInput) -> Result<Self, Self::Error> {
        let value = Self {
            ticket: v.ticket,
            kind: v.kind,
            fingerprint: v.fingerprint,
            admitted_at: v.admitted_at,
            policy_revision: v.policy_revision,
            window_secs: v.window_secs,
            details_after_terminal_secs: v.details_after_terminal_secs,
            outcome: v.outcome,
        };
        value.validate()?;
        Ok(value)
    }
}
