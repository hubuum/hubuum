use chrono::{DateTime, Utc};
use hubuum_domain::{PrincipalIdentity, TaskIdentity};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::*;
use crate::{ApplicationFence, DeletionCheckpointIdentity, StorageTask, StorageTaskKind};

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TaskSubmissionIdentity {
    principal: PrincipalIdentity,
    protected_key: ProtectedTaskKey,
}

impl TaskSubmissionIdentity {
    pub const fn new(principal: PrincipalIdentity, protected_key: ProtectedTaskKey) -> Self {
        Self {
            principal,
            protected_key,
        }
    }
    pub const fn principal(&self) -> PrincipalIdentity {
        self.principal
    }
    pub const fn protected_key(&self) -> &ProtectedTaskKey {
        &self.protected_key
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "version",
    content = "digest",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum TaskFingerprint {
    CanonicalV1(TaskRequestFingerprint),
    LegacyV1(TaskRequestFingerprint),
    LegacyUnverifiable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskSubmissionLookup {
    identity: TaskSubmissionIdentity,
    kind: StorageTaskKind,
    canonical: TaskRequestFingerprint,
    legacy_v1: Option<TaskRequestFingerprint>,
}

impl TaskSubmissionLookup {
    pub const fn new(
        identity: TaskSubmissionIdentity,
        kind: StorageTaskKind,
        canonical: TaskRequestFingerprint,
    ) -> Self {
        Self {
            identity,
            kind,
            canonical,
            legacy_v1: None,
        }
    }
    pub fn with_legacy_v1(mut self, legacy_v1: Option<TaskRequestFingerprint>) -> Self {
        self.legacy_v1 = legacy_v1;
        self
    }
    pub const fn identity(&self) -> &TaskSubmissionIdentity {
        &self.identity
    }
    pub const fn kind(&self) -> StorageTaskKind {
        self.kind
    }
    pub const fn canonical(&self) -> &TaskRequestFingerprint {
        &self.canonical
    }
    pub const fn legacy_v1(&self) -> Option<&TaskRequestFingerprint> {
        self.legacy_v1.as_ref()
    }
    pub fn matches(&self, kind: StorageTaskKind, fingerprint: &TaskFingerprint) -> bool {
        self.kind == kind
            && match fingerprint {
                TaskFingerprint::CanonicalV1(digest) => digest == &self.canonical,
                TaskFingerprint::LegacyV1(digest) => self.legacy_v1.as_ref() == Some(digest),
                TaskFingerprint::LegacyUnverifiable => false,
            }
    }

    pub fn can_compare(&self, fingerprint: &TaskFingerprint) -> bool {
        match fingerprint {
            TaskFingerprint::CanonicalV1(_) => true,
            TaskFingerprint::LegacyV1(_) => self.legacy_v1.is_some(),
            TaskFingerprint::LegacyUnverifiable => false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskReceiptBinding {
    lookup: TaskSubmissionLookup,
    key_fingerprint: ControlKeyFingerprint,
}

impl TaskReceiptBinding {
    pub const fn new(lookup: TaskSubmissionLookup, key_fingerprint: ControlKeyFingerprint) -> Self {
        Self {
            lookup,
            key_fingerprint,
        }
    }
    pub const fn lookup(&self) -> &TaskSubmissionLookup {
        &self.lookup
    }
    pub const fn key_fingerprint(&self) -> &ControlKeyFingerprint {
        &self.key_fingerprint
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TaskReceiptReference {
    submission: TaskSubmissionIdentity,
    generation: TaskReceiptGeneration,
    task: TaskIdentity,
}

impl TaskReceiptReference {
    pub const fn new(
        submission: TaskSubmissionIdentity,
        generation: TaskReceiptGeneration,
        task: TaskIdentity,
    ) -> Self {
        Self {
            submission,
            generation,
            task,
        }
    }
    pub const fn submission(&self) -> &TaskSubmissionIdentity {
        &self.submission
    }
    pub const fn generation(&self) -> TaskReceiptGeneration {
        self.generation
    }
    pub const fn task(&self) -> TaskIdentity {
        self.task
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "AdmissionTicketInput")]
pub struct TaskAdmissionTicket {
    receipt: TaskReceiptReference,
    created_revision: TaskControlRevision,
    admitted_incarnation: Uuid,
    protection_deadline: DateTime<Utc>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AdmissionTicketInput {
    receipt: TaskReceiptReference,
    created_revision: TaskControlRevision,
    admitted_incarnation: Uuid,
    protection_deadline: DateTime<Utc>,
}

impl TaskAdmissionTicket {
    pub fn new(
        receipt: TaskReceiptReference,
        created_revision: TaskControlRevision,
        admitted_incarnation: Uuid,
        protection_deadline: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        if admitted_incarnation.is_nil() || created_revision.value() == 0 {
            return Err(invalid(
                "Task admission requires a nonzero revision and application incarnation",
            ));
        }
        Ok(Self {
            receipt,
            created_revision,
            admitted_incarnation,
            protection_deadline,
        })
    }
    pub const fn receipt(&self) -> &TaskReceiptReference {
        &self.receipt
    }
    pub const fn created_revision(&self) -> TaskControlRevision {
        self.created_revision
    }
    pub const fn admitted_incarnation(&self) -> Uuid {
        self.admitted_incarnation
    }
    pub const fn protection_deadline(&self) -> DateTime<Utc> {
        self.protection_deadline
    }
}

impl TryFrom<AdmissionTicketInput> for TaskAdmissionTicket {
    type Error = StorageValidationError;
    fn try_from(value: AdmissionTicketInput) -> Result<Self, Self::Error> {
        Self::new(
            value.receipt,
            value.created_revision,
            value.admitted_incarnation,
            value.protection_deadline,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpiredTaskSubmission {
    task: TaskIdentity,
    kind: StorageTaskKind,
    expires_at: Option<DateTime<Utc>>,
    owner: PrincipalIdentity,
}

impl ExpiredTaskSubmission {
    pub const fn new(
        task: TaskIdentity,
        kind: StorageTaskKind,
        expires_at: Option<DateTime<Utc>>,
        owner: PrincipalIdentity,
    ) -> Self {
        Self {
            task,
            kind,
            expires_at,
            owner,
        }
    }
    pub const fn task(&self) -> TaskIdentity {
        self.task
    }
    pub const fn kind(&self) -> StorageTaskKind {
        self.kind
    }
    pub const fn expires_at(&self) -> Option<DateTime<Utc>> {
        self.expires_at
    }
    pub const fn owner(&self) -> PrincipalIdentity {
        self.owner
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum TaskSubmissionResolution {
    ProtectionDisabled,
    Fresh,
    PendingProtection(TaskAdmissionTicket),
    Available(Box<StorageTask>),
    DetailsExpired(ExpiredTaskSubmission),
    ExpiryProtectionRequired(TaskReceiptReference),
    LegacyUnverifiable(ExpiredTaskSubmission),
}

#[derive(Clone, Debug, PartialEq)]
pub enum StorageTaskSubmissionOutcome {
    CreatedReady(StorageTask),
    PendingProtection(TaskAdmissionTicket),
    DuplicatePendingProtection(TaskAdmissionTicket),
    DuplicateAvailable(StorageTask),
    DuplicateDetailsExpired(ExpiredTaskSubmission),
    ExpiryProtectionRequired(TaskReceiptReference),
    LegacyUnverifiable(ExpiredTaskSubmission),
}

#[derive(Clone, Debug, PartialEq)]
pub enum TaskActivationOutcome {
    Activated(StorageTask),
    AlreadyActive(StorageTask),
    DetailsExpired(ExpiredTaskSubmission),
}

/// Checkpoint identity and covered revision. Construction validates shape, not
/// independent durability; the application owns publication and its session.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "CheckpointInput")]
pub struct TaskControlCheckpoint {
    lineage: Uuid,
    epoch: Uuid,
    task_revision: TaskControlRevision,
    checkpoint: DeletionCheckpointIdentity,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointInput {
    lineage: Uuid,
    epoch: Uuid,
    task_revision: TaskControlRevision,
    checkpoint: DeletionCheckpointIdentity,
}

impl TaskControlCheckpoint {
    pub fn new(
        lineage: Uuid,
        epoch: Uuid,
        task_revision: TaskControlRevision,
        checkpoint: DeletionCheckpointIdentity,
    ) -> Result<Self, StorageValidationError> {
        if lineage.is_nil() || epoch.is_nil() {
            return Err(invalid("Task checkpoint lineage and epoch must not be nil"));
        }
        Ok(Self {
            lineage,
            epoch,
            task_revision,
            checkpoint,
        })
    }
    pub const fn lineage(&self) -> Uuid {
        self.lineage
    }
    pub const fn epoch(&self) -> Uuid {
        self.epoch
    }
    pub const fn task_revision(&self) -> TaskControlRevision {
        self.task_revision
    }
    pub const fn checkpoint(&self) -> &DeletionCheckpointIdentity {
        &self.checkpoint
    }
}

impl TryFrom<CheckpointInput> for TaskControlCheckpoint {
    type Error = StorageValidationError;
    fn try_from(value: CheckpointInput) -> Result<Self, Self::Error> {
        Self::new(
            value.lineage,
            value.epoch,
            value.task_revision,
            value.checkpoint,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProtectedTaskAdmission {
    ticket: TaskAdmissionTicket,
    checkpoint: TaskControlCheckpoint,
    execution_fence: ApplicationFence,
}

impl ProtectedTaskAdmission {
    pub fn new(
        ticket: TaskAdmissionTicket,
        checkpoint: TaskControlCheckpoint,
        execution_fence: ApplicationFence,
    ) -> Result<Self, StorageValidationError> {
        if checkpoint.task_revision() < ticket.created_revision()
            || execution_fence.incarnation() != ticket.admitted_incarnation()
        {
            return Err(invalid(
                "Task admission checkpoint or application incarnation does not cover the ticket",
            ));
        }
        Ok(Self {
            ticket,
            checkpoint,
            execution_fence,
        })
    }
    pub const fn ticket(&self) -> &TaskAdmissionTicket {
        &self.ticket
    }
    pub const fn checkpoint(&self) -> &TaskControlCheckpoint {
        &self.checkpoint
    }
    pub const fn execution_fence(&self) -> ApplicationFence {
        self.execution_fence
    }
}
