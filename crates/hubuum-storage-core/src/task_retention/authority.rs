use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use hubuum_domain::TaskIdentity;

use super::*;
use crate::{ApplicationReapplication, DeletionControlCapture};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "StateInput")]
pub struct TaskControlState {
    lineage: Uuid,
    epoch: Uuid,
    revision: TaskControlRevision,
    activation: TaskReceiptActivation,
    receipt_expiry_floor: DateTime<Utc>,
    hold_release_revision: TaskControlRevision,
    receipts: Vec<TaskReceipt>,
    policies: BTreeMap<TaskRetentionPolicyRevision, TaskRetentionPolicy>,
    holds: BTreeMap<TaskHoldId, TaskRetentionHold>,
    purge_batches: BTreeMap<TaskPurgeBatchId, TaskPurgeDecision>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StateInput {
    lineage: Uuid,
    epoch: Uuid,
    revision: TaskControlRevision,
    activation: TaskReceiptActivation,
    receipt_expiry_floor: DateTime<Utc>,
    hold_release_revision: TaskControlRevision,
    receipts: Vec<TaskReceipt>,
    policies: BTreeMap<TaskRetentionPolicyRevision, TaskRetentionPolicy>,
    holds: BTreeMap<TaskHoldId, TaskRetentionHold>,
    purge_batches: BTreeMap<TaskPurgeBatchId, TaskPurgeDecision>,
}

pub struct TaskControlStateBuilder {
    state: TaskControlState,
}
impl TaskControlStateBuilder {
    pub const fn hold_release_revision(mut self, value: TaskControlRevision) -> Self {
        self.state.hold_release_revision = value;
        self
    }
    pub fn activation(mut self, value: TaskReceiptActivation) -> Self {
        self.state.activation = value;
        self
    }
    pub const fn receipt_expiry_floor(mut self, value: DateTime<Utc>) -> Self {
        self.state.receipt_expiry_floor = value;
        self
    }
    pub fn receipts(mut self, value: Vec<TaskReceipt>) -> Self {
        self.state.receipts = value;
        self
    }
    pub fn policies(
        mut self,
        value: BTreeMap<TaskRetentionPolicyRevision, TaskRetentionPolicy>,
    ) -> Self {
        self.state.policies = value;
        self
    }
    pub fn holds(mut self, value: BTreeMap<TaskHoldId, TaskRetentionHold>) -> Self {
        self.state.holds = value;
        self
    }
    pub fn purge_batches(mut self, value: BTreeMap<TaskPurgeBatchId, TaskPurgeDecision>) -> Self {
        self.state.purge_batches = value;
        self
    }
    pub fn build(self) -> Result<TaskControlState, StorageValidationError> {
        self.state.validate()?;
        Ok(self.state)
    }
}

impl TaskControlState {
    pub fn builder(
        lineage: Uuid,
        epoch: Uuid,
        revision: TaskControlRevision,
        initial_policy: TaskRetentionPolicy,
    ) -> TaskControlStateBuilder {
        TaskControlStateBuilder {
            state: Self {
                lineage,
                epoch,
                revision,
                activation: TaskReceiptActivation::Disabled,
                receipt_expiry_floor: DateTime::<Utc>::UNIX_EPOCH,
                hold_release_revision: TaskControlRevision::new(0).expect("zero revision"),
                receipts: Vec::new(),
                policies: BTreeMap::from([(initial_policy.revision(), initial_policy)]),
                holds: BTreeMap::new(),
                purge_batches: BTreeMap::new(),
            },
        }
    }
    pub fn initial(
        lineage: Uuid,
        epoch: Uuid,
        policy: TaskRetentionPolicy,
    ) -> Result<Self, StorageValidationError> {
        Self::builder(lineage, epoch, TaskControlRevision::new(0)?, policy).build()
    }
    pub const fn lineage(&self) -> Uuid {
        self.lineage
    }
    pub const fn epoch(&self) -> Uuid {
        self.epoch
    }
    pub const fn revision(&self) -> TaskControlRevision {
        self.revision
    }
    pub const fn activation(&self) -> &TaskReceiptActivation {
        &self.activation
    }
    pub const fn receipt_expiry_floor(&self) -> DateTime<Utc> {
        self.receipt_expiry_floor
    }
    pub const fn hold_release_revision(&self) -> TaskControlRevision {
        self.hold_release_revision
    }
    pub fn receipts(&self) -> &[TaskReceipt] {
        &self.receipts
    }
    pub const fn policies(&self) -> &BTreeMap<TaskRetentionPolicyRevision, TaskRetentionPolicy> {
        &self.policies
    }
    pub const fn holds(&self) -> &BTreeMap<TaskHoldId, TaskRetentionHold> {
        &self.holds
    }
    pub const fn purge_batches(&self) -> &BTreeMap<TaskPurgeBatchId, TaskPurgeDecision> {
        &self.purge_batches
    }
    pub fn policy(&self) -> &TaskRetentionPolicy {
        self.policies
            .last_key_value()
            .expect("validated nonempty policy history")
            .1
    }
    pub fn receipt(&self, identity: &TaskSubmissionIdentity) -> Option<&TaskReceipt> {
        self.receipts
            .binary_search_by(|receipt| receipt.reference().submission().cmp(identity))
            .ok()
            .map(|index| &self.receipts[index])
    }
    pub fn receipt_for_task(&self, task: TaskIdentity) -> Option<&TaskReceipt> {
        self.receipts
            .iter()
            .find(|receipt| receipt.reference().task() == task)
    }
    pub fn restricts_task(&self, task: TaskIdentity) -> bool {
        self.purge_batches.values().any(|batch| {
            batch.ticket().manifest().scope() == TaskPurgeScope::TaskDetails
                && batch
                    .ticket()
                    .manifest()
                    .tasks()
                    .binary_search(&task)
                    .is_ok()
        })
    }
    pub fn restricts_outputs(&self, task: TaskIdentity) -> bool {
        self.purge_batches.values().any(|batch| {
            batch
                .ticket()
                .manifest()
                .tasks()
                .binary_search(&task)
                .is_ok()
        })
    }
    pub fn holds_task(&self, task: TaskIdentity, at: DateTime<Utc>) -> bool {
        self.holds
            .values()
            .any(|hold| hold.task() == task && hold.is_active(at))
    }
    pub fn validates_checkpoint(&self, checkpoint: &TaskControlCheckpoint) -> bool {
        checkpoint.lineage() == self.lineage
            && checkpoint.epoch() == self.epoch
            && checkpoint.task_revision() <= self.revision
    }

    pub fn with_activation(
        &self,
        activation: TaskReceiptActivation,
    ) -> Result<Self, StorageValidationError> {
        let valid = match (&self.activation, &activation) {
            (TaskReceiptActivation::Disabled, TaskReceiptActivation::Backfilling { .. }) => true,
            (
                TaskReceiptActivation::Backfilling {
                    key_fingerprint: old,
                },
                TaskReceiptActivation::Protected {
                    key_fingerprint: new,
                },
            ) => old == new,
            (old, new) => old == new,
        };
        if !valid {
            return Err(invalid(
                "Task receipt activation cannot discard or replace protected key ownership",
            ));
        }
        if self.activation == activation {
            return Ok(self.clone());
        }
        let mut next = self.clone();
        next.activation = activation;
        next.finish_change()
    }
    /// Reserve a new canonical revision for a completed application reconciliation proof.
    /// The adapter must first verify and settle every retained obligation in the same transaction.
    pub fn record_reapplication(&self) -> Result<Self, StorageValidationError> {
        self.clone().finish_change()
    }
    pub fn with_policy(&self, policy: TaskRetentionPolicy) -> Result<Self, StorageValidationError> {
        if let Some(existing) = self.policies.get(&policy.revision()) {
            return if existing == &policy {
                Ok(self.clone())
            } else {
                Err(invalid("Task policy revision is immutable"))
            };
        }
        if policy.revision() != self.policy().revision().next()? {
            return Err(invalid("Task policy revision must advance once"));
        }
        let mut next = self.clone();
        next.policies.insert(policy.revision(), policy);
        next.finish_change()
    }
    pub fn with_hold(&self, hold: TaskRetentionHold) -> Result<Self, StorageValidationError> {
        let mut released = false;
        if let Some(existing) = self.holds.get(&hold.id()) {
            if existing == &hold {
                return Ok(self.clone());
            }
            if !hold.covers(existing) {
                return Err(invalid(
                    "Task hold identity and original decision are immutable",
                ));
            }
            released = existing.released_at().is_none() && hold.released_at().is_some();
        } else if hold.released_at().is_some() {
            return Err(invalid("A new hold must first record its active decision"));
        }
        let mut next = self.clone();
        if released {
            next.hold_release_revision = self.revision.next()?;
        }
        next.holds.insert(hold.id(), hold);
        next.finish_change()
    }
    pub fn with_receipt(&self, receipt: TaskReceipt) -> Result<Self, StorageValidationError> {
        let mut next = self.clone();
        match next.receipts.binary_search_by(|value| {
            value
                .reference()
                .submission()
                .cmp(receipt.reference().submission())
        }) {
            Ok(index) => {
                let old = &next.receipts[index];
                if old == &receipt {
                    return Ok(self.clone());
                }
                if !receipt.covers(old) {
                    return Err(invalid(
                        "Task receipt generation must expire before replacement",
                    ));
                }
                next.receipts[index] = receipt;
            }
            Err(index) => {
                if receipt.ticket().created_revision() != self.revision.next()? {
                    return Err(invalid("New task receipt must bind its creation revision"));
                }
                next.receipts.insert(index, receipt);
            }
        }
        next.finish_change()
    }
    pub fn with_purge(&self, decision: TaskPurgeDecision) -> Result<Self, StorageValidationError> {
        let id = decision.ticket().manifest().id();
        if let Some(old) = self.purge_batches.get(&id) {
            if old == &decision {
                return Ok(self.clone());
            }
            if !decision.covers(old) {
                return Err(invalid("Task purge identity and manifest are immutable"));
            }
        } else if decision.ticket().prepared_revision() != self.revision.next()? {
            return Err(invalid("New task purge must bind its preparation revision"));
        }
        let mut next = self.clone();
        next.purge_batches.insert(id, decision);
        next.finish_change()
    }
    pub fn expire(&self, batch: &ReceiptExpiryBatch) -> Result<Self, StorageValidationError> {
        if self.revision < batch.expected_revision() {
            return Err(invalid("Task expiry authority changed"));
        }
        let mut next = self.clone();
        for reference in batch.receipts() {
            let index = next
                .receipts
                .binary_search_by(|value| {
                    value.reference().submission().cmp(reference.submission())
                })
                .ok();
            let Some(index) = index.filter(|index| next.receipts[*index].reference() == reference)
            else {
                if self.receipt_expiry_floor >= batch.floor() {
                    continue;
                }
                return Err(invalid(
                    "Expired receipt generation is absent or replaced without a covering floor",
                ));
            };
            let receipt = &next.receipts[index];
            if receipt.reference() != reference
                || receipt
                    .expires_at()
                    .is_none_or(|expiry| expiry > batch.floor())
            {
                return Err(invalid(
                    "Task expiry does not cover the exact terminal receipt generation",
                ));
            }
            next.receipts.remove(index);
        }
        next.receipt_expiry_floor = batch.floor().max(self.receipt_expiry_floor);
        if next == *self {
            return Ok(next);
        }
        next.finish_change()
    }
    fn finish_change(mut self) -> Result<Self, StorageValidationError> {
        self.revision = self.revision.next()?;
        self.validate()?;
        Ok(self)
    }

    /// Complete bounded snapshot comparison; revision alone proves nothing.
    pub fn covers(&self, old: &Self) -> bool {
        self.compatible_successor(old, false)
    }

    /// Compare an independently authenticated latest complete snapshot with an
    /// older primary or superseded managed checkpoint. Publication must still
    /// use `covers`: only the verified chain proves that omitted generations
    /// passed through protected terminal evidence before expiry.
    pub fn supersedes_for_recovery(&self, old: &Self) -> bool {
        self.compatible_successor(old, true)
    }

    fn compatible_successor(&self, old: &Self, authenticated_supersession: bool) -> bool {
        if self.lineage != old.lineage
            || self.epoch != old.epoch
            || self.revision < old.revision
            || self.receipt_expiry_floor < old.receipt_expiry_floor
            || self.hold_release_revision < old.hold_release_revision
        {
            return false;
        }
        if self.revision == old.revision {
            return self == old;
        }
        match (&old.activation, &self.activation) {
            (TaskReceiptActivation::Disabled, _) => {}
            (
                TaskReceiptActivation::Backfilling {
                    key_fingerprint: before,
                },
                TaskReceiptActivation::Backfilling {
                    key_fingerprint: after,
                }
                | TaskReceiptActivation::Protected {
                    key_fingerprint: after,
                },
            ) if before == after => {}
            (
                TaskReceiptActivation::Protected {
                    key_fingerprint: before,
                },
                TaskReceiptActivation::Protected {
                    key_fingerprint: after,
                },
            ) if before == after => {}
            _ => return false,
        }
        if !old
            .policies
            .iter()
            .all(|(id, policy)| self.policies.get(id) == Some(policy))
            || !old
                .holds
                .iter()
                .all(|(id, hold)| self.holds.get(id).is_some_and(|value| value.covers(hold)))
            || !old.purge_batches.iter().all(|(id, batch)| {
                self.purge_batches
                    .get(id)
                    .is_some_and(|value| value.covers(batch))
            })
        {
            return false;
        }
        if old.holds.iter().any(|(id, hold)| {
            hold.released_at().is_none()
                && self
                    .holds
                    .get(id)
                    .is_some_and(|current| current.released_at().is_some())
                && self.hold_release_revision <= old.revision
        }) || (!authenticated_supersession
            && self
                .holds
                .iter()
                .any(|(id, hold)| hold.released_at().is_some() && !old.holds.contains_key(id)))
        {
            return false;
        }
        old.receipts.iter().all(
            |receipt| match self.receipt(receipt.reference().submission()) {
                Some(current)
                    if current.reference().generation() == receipt.reference().generation() =>
                {
                    current.covers(receipt)
                }
                _ => {
                    authenticated_supersession
                        || receipt
                            .expires_at()
                            .is_some_and(|expiry| expiry <= self.receipt_expiry_floor)
                }
            },
        )
    }
    pub fn validate(&self) -> Result<(), StorageValidationError> {
        if self.lineage.is_nil()
            || self.epoch.is_nil()
            || self.policies.is_empty()
            || self.policies.len() > TASK_CONTROL_MAX_POLICIES
            || self.holds.len() > TASK_CONTROL_MAX_HOLDS
            || self.purge_batches.len() > TASK_CONTROL_MAX_PURGE_BATCHES
            || self.receipts.len() > self.policy().max_receipts() as usize
            || self.hold_release_revision > self.revision
            || (self.hold_release_revision.value() == 0
                && self.holds.values().any(|hold| hold.released_at().is_some()))
        {
            return Err(invalid(
                "Task authority exceeds its identity or record bounds",
            ));
        }
        if self
            .policies
            .iter()
            .any(|(id, policy)| *id != policy.revision())
            || self.holds.iter().any(|(id, hold)| *id != hold.id())
        {
            return Err(invalid("Task authority map identity mismatch"));
        }
        if self
            .receipts
            .windows(2)
            .any(|pair| pair[0].reference().submission() >= pair[1].reference().submission())
        {
            return Err(invalid(
                "Task receipt entries must be canonically ordered and unique",
            ));
        }
        let mut tasks = BTreeSet::new();
        let mut generations = BTreeSet::new();
        for receipt in &self.receipts {
            if !tasks.insert(receipt.reference().task())
                || !generations.insert(receipt.reference().generation())
                || receipt.ticket().created_revision() > self.revision
                || !self.policies.contains_key(&receipt.policy_revision())
            {
                return Err(invalid(
                    "Task receipt identity, generation, revision or policy mismatch",
                ));
            }
        }
        let mut targets = BTreeSet::new();
        for (id, batch) in &self.purge_batches {
            if *id != batch.ticket().manifest().id()
                || batch.ticket().prepared_revision() > self.revision
                || !self
                    .policies
                    .contains_key(&batch.ticket().manifest().policy_revision())
                || batch
                    .ticket()
                    .manifest()
                    .tasks()
                    .iter()
                    .any(|task| !targets.insert((*task, batch.ticket().manifest().scope())))
            {
                return Err(invalid(
                    "Task purge identity, subject, revision or policy mismatch",
                ));
            }
            if batch
                .removed()
                .is_some_and(|checkpoint| !self.validates_checkpoint(checkpoint))
            {
                return Err(invalid("Task purge receipt refers to different authority"));
            }
        }
        let reserved_bytes = 2 + self.receipts.len() * (TASK_CONTROL_MAX_RECEIPT_BYTES + 1);
        if reserved_bytes > self.policy().max_receipt_bytes() as usize {
            return Err(invalid(
                "Task receipt capacity must reserve terminal growth",
            ));
        }
        let receipt_bytes = serde_json::to_vec(&self.receipts)
            .map_err(|_| invalid("Cannot encode task receipts"))?
            .len();
        if receipt_bytes > self.policy().max_receipt_bytes() as usize
            || receipt_bytes > TASK_CONTROL_MAX_RECEIPTS_BYTES
        {
            return Err(invalid("Task receipts exceed their encoded byte bound"));
        }
        let retention_bytes = serde_json::to_vec(&(
            &self.activation,
            &self.policies,
            &self.holds,
            &self.purge_batches,
        ))
        .map_err(|_| invalid("Cannot encode task retention"))?
        .len();
        let reserved_completion = self
            .purge_batches
            .values()
            .filter(|batch| batch.removed().is_none())
            .count()
            * TASK_CONTROL_PURGE_COMPLETION_RESERVE;
        if retention_bytes + reserved_completion > TASK_CONTROL_MAX_RETENTION_BYTES {
            return Err(invalid("Task retention exceeds its encoded byte bound"));
        }
        Ok(())
    }
}

impl TryFrom<StateInput> for TaskControlState {
    type Error = StorageValidationError;
    fn try_from(v: StateInput) -> Result<Self, Self::Error> {
        let value = Self {
            lineage: v.lineage,
            epoch: v.epoch,
            revision: v.revision,
            activation: v.activation,
            receipt_expiry_floor: v.receipt_expiry_floor,
            hold_release_revision: v.hold_release_revision,
            receipts: v.receipts,
            policies: v.policies,
            holds: v.holds,
            purge_batches: v.purge_batches,
        };
        value.validate()?;
        Ok(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskControlChange {
    expected: TaskControlState,
    next: TaskControlState,
}
impl TaskControlChange {
    pub fn new(
        expected: TaskControlState,
        next: TaskControlState,
    ) -> Result<Self, StorageValidationError> {
        if !next.covers(&expected)
            || (next != expected && next.revision() != expected.revision().next()?)
        {
            return Err(invalid("Task control change is not one monotonic revision"));
        }
        Ok(Self { expected, next })
    }
    pub const fn expected(&self) -> &TaskControlState {
        &self.expected
    }
    pub const fn next(&self) -> &TaskControlState {
        &self.next
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskControlRecovery {
    expected: TaskControlState,
    recovered: TaskControlState,
}
impl TaskControlRecovery {
    pub fn new(
        expected: TaskControlState,
        recovered: TaskControlState,
    ) -> Result<Self, StorageValidationError> {
        if !expected.covers(&recovered) && !recovered.supersedes_for_recovery(&expected) {
            return Err(invalid(
                "Task recovery cannot prove compatible complete authority",
            ));
        }
        Ok(Self {
            expected,
            recovered,
        })
    }
    pub const fn expected(&self) -> &TaskControlState {
        &self.expected
    }
    pub const fn recovered(&self) -> &TaskControlState {
        &self.recovered
    }
    pub fn merged(&self) -> &TaskControlState {
        if self.expected.covers(&self.recovered) {
            &self.expected
        } else {
            &self.recovered
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "ReapplicationInput")]
pub struct TaskControlReapplication {
    incarnation: Uuid,
    task_revision: TaskControlRevision,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReapplicationInput {
    incarnation: Uuid,
    task_revision: TaskControlRevision,
}
impl TaskControlReapplication {
    pub fn new(
        incarnation: Uuid,
        task_revision: TaskControlRevision,
    ) -> Result<Self, StorageValidationError> {
        if incarnation.is_nil() {
            return Err(invalid("Task reapplication incarnation must not be nil"));
        }
        Ok(Self {
            incarnation,
            task_revision,
        })
    }
    pub const fn incarnation(&self) -> Uuid {
        self.incarnation
    }
    pub const fn task_revision(&self) -> TaskControlRevision {
        self.task_revision
    }
}
impl TryFrom<ReapplicationInput> for TaskControlReapplication {
    type Error = StorageValidationError;
    fn try_from(v: ReapplicationInput) -> Result<Self, Self::Error> {
        Self::new(v.incarnation, v.task_revision)
    }
}

#[derive(Clone, Debug)]
pub struct ControlCheckpointCapture {
    deletion: DeletionControlCapture,
    task_control: TaskControlState,
    task_reapplication: Option<TaskControlReapplication>,
}
impl ControlCheckpointCapture {
    pub fn new(
        deletion: DeletionControlCapture,
        task_control: TaskControlState,
    ) -> Result<Self, StorageValidationError> {
        if deletion.state().lineage() != task_control.lineage()
            || deletion.state().epoch() != task_control.epoch()
        {
            return Err(invalid("Composite authority lineage or epoch differs"));
        }
        Ok(Self {
            deletion,
            task_control,
            task_reapplication: None,
        })
    }
    pub fn with_task_reapplication(
        mut self,
        proof: TaskControlReapplication,
    ) -> Result<Self, StorageValidationError> {
        if proof.task_revision() > self.task_control.revision()
            || self
                .deletion
                .reapplication()
                .is_some_and(|deletion| deletion.incarnation() != proof.incarnation())
        {
            return Err(invalid(
                "Task reapplication does not cover the captured authority",
            ));
        }
        self.task_reapplication = Some(proof);
        Ok(self)
    }
    pub const fn deletion(&self) -> &DeletionControlCapture {
        &self.deletion
    }
    pub const fn task_control(&self) -> &TaskControlState {
        &self.task_control
    }
    pub const fn task_reapplication(&self) -> Option<&TaskControlReapplication> {
        self.task_reapplication.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationExposureProof {
    deletion: ApplicationReapplication,
    tasks: Option<TaskControlReapplication>,
}
impl ApplicationExposureProof {
    pub fn new(
        deletion: ApplicationReapplication,
        tasks: Option<TaskControlReapplication>,
    ) -> Result<Self, StorageValidationError> {
        if tasks
            .as_ref()
            .is_some_and(|tasks| tasks.incarnation() != deletion.incarnation())
        {
            return Err(invalid(
                "Exposure proofs refer to different application incarnations",
            ));
        }
        Ok(Self { deletion, tasks })
    }
    pub const fn deletion(&self) -> &ApplicationReapplication {
        &self.deletion
    }
    pub const fn tasks(&self) -> Option<&TaskControlReapplication> {
        self.tasks.as_ref()
    }
}
