use chrono::{DateTime, Utc};
use hubuum_domain::TaskIdentity;

use super::*;
use crate::{ApplicationFence, StorageError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReceiptExpirySelection {
    limit: u32,
}
impl ReceiptExpirySelection {
    pub const MAX_RECEIPTS: u32 = 1000;

    pub fn new(limit: u32) -> Result<Self, StorageValidationError> {
        if limit == 0 || limit > Self::MAX_RECEIPTS {
            return Err(invalid(
                "Receipt expiry batch must contain 1..=1000 generations",
            ));
        }
        Ok(Self { limit })
    }
    pub const fn limit(self) -> u32 {
        self.limit
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReceiptExpiryBatch {
    expected_revision: TaskControlRevision,
    floor: DateTime<Utc>,
    receipts: Vec<TaskReceiptReference>,
}
impl ReceiptExpiryBatch {
    pub fn new(
        expected_revision: TaskControlRevision,
        floor: DateTime<Utc>,
        receipts: Vec<TaskReceiptReference>,
    ) -> Result<Self, StorageValidationError> {
        if receipts.is_empty()
            || receipts.len() > ReceiptExpirySelection::MAX_RECEIPTS as usize
            || receipts
                .windows(2)
                .any(|pair| pair[0].submission() >= pair[1].submission())
        {
            return Err(invalid(
                "Receipt expiry batch requires sorted unique bounded generations",
            ));
        }
        Ok(Self {
            expected_revision,
            floor,
            receipts,
        })
    }
    pub const fn expected_revision(&self) -> TaskControlRevision {
        self.expected_revision
    }
    pub const fn floor(&self) -> DateTime<Utc> {
        self.floor
    }
    pub fn receipts(&self) -> &[TaskReceiptReference] {
        &self.receipts
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProtectedReceiptExpiry {
    batch: ReceiptExpiryBatch,
    checkpoint: TaskControlCheckpoint,
}
impl ProtectedReceiptExpiry {
    pub fn new(
        batch: ReceiptExpiryBatch,
        checkpoint: TaskControlCheckpoint,
    ) -> Result<Self, StorageValidationError> {
        if checkpoint.task_revision() < batch.expected_revision() {
            return Err(invalid(
                "Expiry checkpoint does not cover the terminal anchors",
            ));
        }
        Ok(Self { batch, checkpoint })
    }
    pub const fn batch(&self) -> &ReceiptExpiryBatch {
        &self.batch
    }
    pub const fn checkpoint(&self) -> &TaskControlCheckpoint {
        &self.checkpoint
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Compare against the policy revision so unrelated receipt activity cannot
/// invalidate an administrator decision. Competing policy edits still conflict.
pub struct TaskRetentionPolicyChange {
    expected: TaskRetentionPolicyRevision,
    policy: TaskRetentionPolicy,
}
impl TaskRetentionPolicyChange {
    pub const fn new(expected: TaskRetentionPolicyRevision, policy: TaskRetentionPolicy) -> Self {
        Self { expected, policy }
    }
    pub const fn expected(&self) -> TaskRetentionPolicyRevision {
        self.expected
    }
    pub const fn policy(&self) -> &TaskRetentionPolicy {
        &self.policy
    }
}

/// Compare the captured decision for this hold, independently of receipt activity
/// and other holds. The snapshot binds the expected record to the desired ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskHoldChange {
    expected: Option<TaskRetentionHold>,
    hold: TaskRetentionHold,
}
impl TaskHoldChange {
    pub fn new(snapshot: &TaskControlState, hold: TaskRetentionHold) -> Self {
        Self {
            expected: snapshot.holds().get(&hold.id()).cloned(),
            hold,
        }
    }
    pub const fn expected(&self) -> Option<&TaskRetentionHold> {
        self.expected.as_ref()
    }
    pub const fn hold(&self) -> &TaskRetentionHold {
        &self.hold
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProtectedTaskReapplication {
    expected: TaskControlState,
    fence: ApplicationFence,
    checkpoint: TaskControlCheckpoint,
}
impl ProtectedTaskReapplication {
    pub fn new(
        expected: TaskControlState,
        fence: ApplicationFence,
        checkpoint: TaskControlCheckpoint,
    ) -> Result<Self, StorageValidationError> {
        if !expected.validates_checkpoint(&checkpoint)
            || checkpoint.task_revision() != expected.revision()
        {
            return Err(invalid(
                "Task reapplication checkpoint must cover exact authority",
            ));
        }
        Ok(Self {
            expected,
            fence,
            checkpoint,
        })
    }
    pub const fn expected(&self) -> &TaskControlState {
        &self.expected
    }
    pub const fn fence(&self) -> ApplicationFence {
        self.fence
    }
    pub const fn checkpoint(&self) -> &TaskControlCheckpoint {
        &self.checkpoint
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskReapplicationProgress {
    More {
        revision: TaskControlRevision,
        rows: u32,
        bytes: u64,
    },
    Blocked(TaskPurgeBlock),
    Complete(TaskControlReapplication),
}

/// Caller-supplied HMAC comparison material for one adapter-resolved legacy
/// task. The adapter retains authoritative original clocks and identity checks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskLegacyReceiptBinding {
    task: TaskIdentity,
    submission: TaskSubmissionIdentity,
    fingerprint: TaskFingerprint,
}
impl TaskLegacyReceiptBinding {
    pub const fn new(
        task: TaskIdentity,
        submission: TaskSubmissionIdentity,
        fingerprint: TaskFingerprint,
    ) -> Self {
        Self {
            task,
            submission,
            fingerprint,
        }
    }
    pub const fn task(&self) -> TaskIdentity {
        self.task
    }
    pub const fn submission(&self) -> &TaskSubmissionIdentity {
        &self.submission
    }
    pub const fn fingerprint(&self) -> &TaskFingerprint {
        &self.fingerprint
    }
}

/// Administrative backfill input; payload remains replaceable task data and
/// never enters receipt snapshots. Bounded selection accounts for these bytes.
#[derive(Clone, Debug)]
pub struct TaskLegacyReceiptCandidate {
    task: crate::StorageTask,
    identity: TaskIdentity,
    principal: hubuum_domain::PrincipalIdentity,
}
impl TaskLegacyReceiptCandidate {
    pub const fn new(
        task: crate::StorageTask,
        identity: TaskIdentity,
        principal: hubuum_domain::PrincipalIdentity,
    ) -> Self {
        Self {
            task,
            identity,
            principal,
        }
    }
    pub const fn task(&self) -> &crate::StorageTask {
        &self.task
    }
    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }
    pub const fn principal(&self) -> hubuum_domain::PrincipalIdentity {
        self.principal
    }
}

/// Required protected lifecycle capability. Runtime-facing admission stays on
/// TaskQueueStorage; control publication/recovery requires control authority.
#[async_trait::async_trait]
pub trait TaskRetentionStorage: Send + Sync {
    async fn prepare_expired_task_outputs(
        &self,
        selection: TaskPurgeSelection,
    ) -> Result<Option<TaskPurgeTicket>, StorageError>;
    async fn read_task_receipt_activation(&self) -> Result<TaskReceiptActivation, StorageError>;
    async fn list_pending_task_admissions(
        &self,
        limit: u32,
    ) -> Result<Vec<TaskAdmissionTicket>, StorageError>;
    async fn read_task_control(&self) -> Result<TaskControlState, StorageError>;
    async fn capture_control_checkpoint(&self) -> Result<ControlCheckpointCapture, StorageError>;
    async fn commit_task_control(
        &self,
        change: TaskControlChange,
    ) -> Result<TaskControlState, StorageError>;
    async fn recover_task_control(
        &self,
        change: TaskControlRecovery,
    ) -> Result<TaskControlRevision, StorageError>;
    async fn read_task_retention_policy(&self) -> Result<TaskRetentionPolicy, StorageError>;
    async fn replace_task_retention_policy(
        &self,
        change: TaskRetentionPolicyChange,
    ) -> Result<TaskControlRevision, StorageError>;
    async fn change_task_hold(
        &self,
        change: TaskHoldChange,
    ) -> Result<TaskControlRevision, StorageError>;
    async fn select_expired_task_receipts(
        &self,
        request: ReceiptExpirySelection,
    ) -> Result<Option<ReceiptExpiryBatch>, StorageError>;
    async fn expire_task_receipts(
        &self,
        request: ProtectedReceiptExpiry,
    ) -> Result<TaskControlRevision, StorageError>;
    async fn prepare_task_details_purge(
        &self,
        selection: TaskPurgeSelection,
    ) -> Result<Option<TaskPurgeTicket>, StorageError>;
    async fn purge_task_details_batch(
        &self,
        request: ProtectedTaskPurge,
        budget: TaskPurgeBudget,
    ) -> Result<TaskPurgeProgress, StorageError>;
    async fn list_pending_task_purges(
        &self,
        limit: u32,
    ) -> Result<Vec<TaskPurgeTicket>, StorageError>;
    async fn reapply_task_retention(
        &self,
        request: ProtectedTaskReapplication,
        budget: TaskPurgeBudget,
    ) -> Result<TaskReapplicationProgress, StorageError>;
    async fn list_legacy_task_receipts(
        &self,
        budget: TaskPurgeBudget,
    ) -> Result<Vec<TaskLegacyReceiptCandidate>, StorageError>;
    async fn backfill_task_receipts(
        &self,
        expected: TaskControlRevision,
        bindings: Vec<TaskLegacyReceiptBinding>,
    ) -> Result<TaskControlRevision, StorageError>;
}
