use chrono::{DateTime, Utc};
use hubuum_domain::TaskIdentity;

use super::*;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskHoldReason {
    Legal,
    Incident,
    Operator,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "HoldInput")]
pub struct TaskRetentionHold {
    id: TaskHoldId,
    task: TaskIdentity,
    created_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
    reason: TaskHoldReason,
    released_at: Option<DateTime<Utc>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HoldInput {
    id: TaskHoldId,
    task: TaskIdentity,
    created_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
    reason: TaskHoldReason,
    released_at: Option<DateTime<Utc>>,
}

impl TaskRetentionHold {
    pub fn new(
        id: TaskHoldId,
        task: TaskIdentity,
        created_at: DateTime<Utc>,
        expires_at: Option<DateTime<Utc>>,
        reason: TaskHoldReason,
    ) -> Result<Self, StorageValidationError> {
        HoldInput {
            id,
            task,
            created_at,
            expires_at,
            reason,
            released_at: None,
        }
        .try_into()
    }
    pub const fn id(&self) -> TaskHoldId {
        self.id
    }
    pub const fn task(&self) -> TaskIdentity {
        self.task
    }
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
    pub const fn expires_at(&self) -> Option<DateTime<Utc>> {
        self.expires_at
    }
    pub const fn reason(&self) -> TaskHoldReason {
        self.reason
    }
    pub const fn released_at(&self) -> Option<DateTime<Utc>> {
        self.released_at
    }
    pub fn is_active(&self, at: DateTime<Utc>) -> bool {
        self.released_at.is_none() && self.expires_at.is_none_or(|expiry| at < expiry)
    }
    pub fn release(&self, at: DateTime<Utc>) -> Result<Self, StorageValidationError> {
        if at < self.created_at {
            return Err(invalid("Hold release precedes creation"));
        }
        let mut next = self.clone();
        next.released_at.get_or_insert(at);
        Ok(next)
    }
    pub fn covers(&self, old: &Self) -> bool {
        let mut immutable = self.clone();
        immutable.released_at = old.released_at;
        immutable == *old
            && old
                .released_at
                .is_none_or(|at| self.released_at == Some(at))
    }
}
impl TryFrom<HoldInput> for TaskRetentionHold {
    type Error = StorageValidationError;
    fn try_from(v: HoldInput) -> Result<Self, Self::Error> {
        if v.expires_at.is_some_and(|at| at <= v.created_at)
            || v.released_at.is_some_and(|at| at < v.created_at)
        {
            return Err(invalid("Invalid task hold time interval"));
        }
        Ok(Self {
            id: v.id,
            task: v.task,
            created_at: v.created_at,
            expires_at: v.expires_at,
            reason: v.reason,
            released_at: v.released_at,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaskPurgeSelection {
    max_tasks: u32,
    budget: TaskPurgeBudget,
}
impl TaskPurgeSelection {
    pub fn new(max_tasks: u32, budget: TaskPurgeBudget) -> Result<Self, StorageValidationError> {
        if max_tasks == 0 || max_tasks as usize > TASK_CONTROL_MAX_PURGE_TASKS {
            return Err(invalid("Task purge selection must contain 1..=64 tasks"));
        }
        Ok(Self { max_tasks, budget })
    }
    pub const fn max_tasks(self) -> u32 {
        self.max_tasks
    }
    pub const fn budget(self) -> TaskPurgeBudget {
        self.budget
    }
}

/// Distinct task identities form one declared task-details aggregate. They are
/// not physical copies of one task or history version.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskPurgeScope {
    TaskDetails,
    ExpiredOutputs,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "ManifestInput")]
pub struct TaskPurgeManifest {
    id: TaskPurgeBatchId,
    scope: TaskPurgeScope,
    tasks: Vec<TaskIdentity>,
    policy_revision: TaskRetentionPolicyRevision,
    eligible_at: DateTime<Utc>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestInput {
    id: TaskPurgeBatchId,
    scope: TaskPurgeScope,
    tasks: Vec<TaskIdentity>,
    policy_revision: TaskRetentionPolicyRevision,
    eligible_at: DateTime<Utc>,
}
impl TaskPurgeManifest {
    pub fn new(
        id: TaskPurgeBatchId,
        scope: TaskPurgeScope,
        tasks: Vec<TaskIdentity>,
        policy_revision: TaskRetentionPolicyRevision,
        eligible_at: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        if tasks.is_empty()
            || tasks.len() > TASK_CONTROL_MAX_PURGE_TASKS
            || tasks.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(invalid(
                "Task purge manifest requires 1..=64 sorted unique task identities",
            ));
        }
        Ok(Self {
            id,
            scope,
            tasks,
            policy_revision,
            eligible_at,
        })
    }
    pub const fn id(&self) -> TaskPurgeBatchId {
        self.id
    }
    pub const fn scope(&self) -> TaskPurgeScope {
        self.scope
    }
    pub fn tasks(&self) -> &[TaskIdentity] {
        &self.tasks
    }
    pub const fn policy_revision(&self) -> TaskRetentionPolicyRevision {
        self.policy_revision
    }
    pub const fn eligible_at(&self) -> DateTime<Utc> {
        self.eligible_at
    }
}
impl TryFrom<ManifestInput> for TaskPurgeManifest {
    type Error = StorageValidationError;
    fn try_from(v: ManifestInput) -> Result<Self, Self::Error> {
        Self::new(v.id, v.scope, v.tasks, v.policy_revision, v.eligible_at)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "PurgeTicketInput")]
pub struct TaskPurgeTicket {
    manifest: TaskPurgeManifest,
    generation: i64,
    prepared_revision: TaskControlRevision,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PurgeTicketInput {
    manifest: TaskPurgeManifest,
    generation: i64,
    prepared_revision: TaskControlRevision,
}
impl TaskPurgeTicket {
    pub fn new(
        manifest: TaskPurgeManifest,
        generation: i64,
        prepared_revision: TaskControlRevision,
    ) -> Result<Self, StorageValidationError> {
        if generation <= 0 || prepared_revision.value() == 0 {
            return Err(invalid(
                "Task purge requires a positive generation and revision",
            ));
        }
        Ok(Self {
            manifest,
            generation,
            prepared_revision,
        })
    }
    pub const fn manifest(&self) -> &TaskPurgeManifest {
        &self.manifest
    }
    pub const fn generation(&self) -> i64 {
        self.generation
    }
    pub const fn prepared_revision(&self) -> TaskControlRevision {
        self.prepared_revision
    }
}
impl TryFrom<PurgeTicketInput> for TaskPurgeTicket {
    type Error = StorageValidationError;
    fn try_from(v: PurgeTicketInput) -> Result<Self, Self::Error> {
        Self::new(v.manifest, v.generation, v.prepared_revision)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "PurgeDecisionInput")]
pub struct TaskPurgeDecision {
    ticket: TaskPurgeTicket,
    removed: Option<TaskControlCheckpoint>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PurgeDecisionInput {
    ticket: TaskPurgeTicket,
    removed: Option<TaskControlCheckpoint>,
}
impl TaskPurgeDecision {
    pub const fn pending(ticket: TaskPurgeTicket) -> Self {
        Self {
            ticket,
            removed: None,
        }
    }
    pub const fn ticket(&self) -> &TaskPurgeTicket {
        &self.ticket
    }
    pub const fn removed(&self) -> Option<&TaskControlCheckpoint> {
        self.removed.as_ref()
    }
    pub fn record_removed(
        &self,
        checkpoint: TaskControlCheckpoint,
    ) -> Result<Self, StorageValidationError> {
        let mut next = self.clone();
        if checkpoint.task_revision() < self.ticket.prepared_revision() {
            return Err(invalid(
                "Purge checkpoint does not cover its immutable intent",
            ));
        }
        next.removed.get_or_insert(checkpoint);
        Ok(next)
    }
    pub fn covers(&self, old: &Self) -> bool {
        self.ticket == old.ticket
            && old
                .removed
                .as_ref()
                .is_none_or(|receipt| self.removed.as_ref() == Some(receipt))
    }
}
impl TryFrom<PurgeDecisionInput> for TaskPurgeDecision {
    type Error = StorageValidationError;
    fn try_from(v: PurgeDecisionInput) -> Result<Self, Self::Error> {
        let value = Self::pending(v.ticket);
        match v.removed {
            Some(checkpoint) => value.record_removed(checkpoint),
            None => Ok(value),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProtectedTaskPurge {
    ticket: TaskPurgeTicket,
    checkpoint: TaskControlCheckpoint,
}
impl ProtectedTaskPurge {
    pub fn new(
        ticket: TaskPurgeTicket,
        checkpoint: TaskControlCheckpoint,
    ) -> Result<Self, StorageValidationError> {
        if checkpoint.task_revision() < ticket.prepared_revision() {
            return Err(invalid("Task purge checkpoint does not cover its intent"));
        }
        Ok(Self { ticket, checkpoint })
    }
    pub const fn ticket(&self) -> &TaskPurgeTicket {
        &self.ticket
    }
    pub const fn checkpoint(&self) -> &TaskControlCheckpoint {
        &self.checkpoint
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskPurgeBlock {
    Held,
    OutputAvailable,
    Dependency,
    UnknownIdentity,
    Nonterminal,
    OversizedRow,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskPurgeProgress {
    More {
        rows: u32,
        bytes: u64,
    },
    Blocked(TaskPurgeBlock),
    Removed {
        revision: TaskControlRevision,
        rows: u32,
        bytes: u64,
    },
}
