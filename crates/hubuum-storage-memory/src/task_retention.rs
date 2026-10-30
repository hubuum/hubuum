use super::*;

mod admission;
mod purge;
pub(crate) use purge::{PurgeWork, measured};
mod recovery;
#[cfg(test)]
mod tests;

#[derive(Clone)]
pub(super) struct MemoryTaskAdmission {
    pub(super) ticket: TaskAdmissionTicket,
    pub(super) activated: Option<TaskControlCheckpoint>,
    pub(super) emit_admission_event: bool,
}

#[derive(Clone)]
pub(super) struct MemoryTaskInvocation {
    request: StorageTaskRemoteInvocation,
    target_name: String,
    target_identity: ResourceIdentity,
    collection: CollectionId,
    collection_identity: ResourceIdentity,
    actor_identity: Option<PrincipalIdentity>,
    initiator_identity: Option<PrincipalIdentity>,
}

impl MemoryState {
    fn require_task_revision(&self, expected: TaskControlRevision) -> Result<(), StorageError> {
        if self.task_control.revision() != expected {
            return Err(StorageError::conflict(
                "Task control changed; recapture before retrying",
            ));
        }
        Ok(())
    }
    fn require_task_checkpoint(
        &self,
        checkpoint: &TaskControlCheckpoint,
    ) -> Result<(), StorageError> {
        if !self.task_control.validates_checkpoint(checkpoint) {
            return Err(StorageError::conflict(
                "Task checkpoint does not cover this authority",
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl TaskRetentionStorage for MemoryStorage {
    async fn prepare_expired_task_outputs(
        &self,
        selection: TaskPurgeSelection,
    ) -> Result<Option<TaskPurgeTicket>, StorageError> {
        self.prepare_task_purge_scope(selection, TaskPurgeScope::ExpiredOutputs)
            .await
    }
    async fn read_task_receipt_activation(&self) -> Result<TaskReceiptActivation, StorageError> {
        Ok(self
            .state
            .read_control()
            .await
            .task_control
            .activation()
            .clone())
    }
    async fn list_pending_task_admissions(
        &self,
        limit: u32,
    ) -> Result<Vec<TaskAdmissionTicket>, StorageError> {
        if limit == 0 || limit as usize > TASK_CONTROL_MAX_PURGE_TASKS {
            return Err(StorageError::invalid_input(
                "Pending admission listing requires 1..=64 tickets",
            ));
        }
        let state = self.state.read_control().await;
        Ok(state
            .tasks
            .values()
            .filter(|task| !task.status.is_terminal())
            .filter_map(|task| task.receipt_admission.as_ref())
            .filter(|admission| {
                admission.activated.is_none() && admission.ticket.protection_deadline() > Utc::now()
            })
            .take(limit as usize)
            .map(|admission| admission.ticket.clone())
            .collect())
    }
    async fn read_task_control(&self) -> Result<TaskControlState, StorageError> {
        Ok(self.state.read_control().await.task_control.clone())
    }
    async fn capture_control_checkpoint(&self) -> Result<ControlCheckpointCapture, StorageError> {
        let state = self.state.read_control().await;
        let mut deletion = DeletionControlCapture::new(
            state.deletion_control.clone(),
            DeletionRecoveryPosition::memory(state.deletion_control.lineage())
                .map_err(invalid_contract_value)?,
        );
        if let Some(proof) = state
            .application_control
            .reapplication()
            .filter(|proof| proof.revision() == state.deletion_control.revision())
        {
            deletion = deletion
                .with_reapplication(proof.clone())
                .map_err(invalid_contract_value)?;
        }
        let mut capture = ControlCheckpointCapture::new(deletion, state.task_control.clone())
            .map_err(invalid_contract_value)?;
        if let Some(proof) = state.task_reapplication.as_ref().filter(|proof| {
            proof.incarnation() == state.application_control.fence().incarnation()
                && proof.task_revision() <= state.task_control.revision()
        }) {
            capture = capture
                .with_task_reapplication(proof.clone())
                .map_err(invalid_contract_value)?;
        }
        Ok(capture)
    }
    async fn commit_task_control(
        &self,
        change: TaskControlChange,
    ) -> Result<TaskControlState, StorageError> {
        let mut state = self.state.write_control().await;
        if &state.task_control != change.expected() {
            return Err(StorageError::conflict("Task control changed"));
        }
        if change.next().receipts() != change.expected().receipts()
            || change.next().purge_batches() != change.expected().purge_batches()
            || change.next().holds() != change.expected().holds()
            || change.next().receipt_expiry_floor() != change.expected().receipt_expiry_floor()
        {
            return Err(StorageError::invalid_input(
                "Admission, hold, expiry and purge require their operation-specific protocols",
            ));
        }
        if !state.task_control.activation().is_protected()
            && change.next().activation().is_protected()
            && !state
                .legacy_receipt_candidates(
                    TaskPurgeBudget::new(1, TaskPurgeBudget::MAX_BYTES)
                        .map_err(invalid_contract_value)?,
                )?
                .is_empty()
        {
            return Err(StorageError::unavailable(
                "Legacy keyed tasks remain unprotected",
            ));
        }
        if !state.task_control.activation().is_protected()
            && change.next().activation().is_protected()
            && state
                .tasks
                .values()
                .filter(|task| !task.status.is_terminal())
                .any(|task| {
                    task.receipt_admission
                        .as_ref()
                        .is_some_and(|admission| admission.activated.is_none())
                })
        {
            return Err(StorageError::unavailable(
                "Legacy task admissions remain unacknowledged",
            ));
        }
        state.task_control = change.next().clone();
        Ok(state.task_control.clone())
    }
    async fn recover_task_control(
        &self,
        change: TaskControlRecovery,
    ) -> Result<TaskControlRevision, StorageError> {
        let mut state = self.state.write_control().await;
        if &state.task_control != change.expected() {
            return Err(StorageError::conflict(
                "Task authority changed before recovery",
            ));
        }
        if state
            .application_control
            .is_released_for(&state.deletion_control)
        {
            return Err(StorageError::unavailable(
                "Task recovery requires quarantined application state",
            ));
        }
        state.require_runtime_drain()?;
        state.task_control = change.merged().clone();
        state.task_reapplication = None;
        Ok(state.task_control.revision())
    }
    async fn read_task_retention_policy(&self) -> Result<TaskRetentionPolicy, StorageError> {
        Ok(self
            .state
            .read_control()
            .await
            .task_control
            .policy()
            .clone())
    }
    async fn replace_task_retention_policy(
        &self,
        change: TaskRetentionPolicyChange,
    ) -> Result<TaskControlRevision, StorageError> {
        let mut state = self.state.write_control().await;
        if state.task_control.policy().revision() != change.expected() {
            return Err(StorageError::conflict("Task retention policy changed"));
        }
        state.task_control = state
            .task_control
            .with_policy(change.policy().clone())
            .map_err(invalid_contract_value)?;
        Ok(state.task_control.revision())
    }
    async fn change_task_hold(
        &self,
        change: TaskHoldChange,
    ) -> Result<TaskControlRevision, StorageError> {
        let mut state = self.state.write_control().await;
        if state.task_control.holds().get(&change.hold().id()) != change.expected() {
            return Err(StorageError::conflict("Task hold decision changed"));
        }
        state.task_control = state
            .task_control
            .with_hold(change.hold().clone())
            .map_err(invalid_contract_value)?;
        Ok(state.task_control.revision())
    }
    async fn select_expired_task_receipts(
        &self,
        request: ReceiptExpirySelection,
    ) -> Result<Option<ReceiptExpiryBatch>, StorageError> {
        let state = self.state.read_control().await;
        let now = Utc::now();
        let receipts = state
            .task_control
            .receipts()
            .iter()
            .filter(|receipt| receipt.expires_at().is_some_and(|at| at <= now))
            .take(request.limit() as usize)
            .map(|receipt| receipt.reference().clone())
            .collect::<Vec<_>>();
        if receipts.is_empty() {
            return Ok(None);
        }
        ReceiptExpiryBatch::new(
            state.task_control.revision(),
            now.max(state.task_control.receipt_expiry_floor()),
            receipts,
        )
        .map(Some)
        .map_err(invalid_contract_value)
    }
    async fn expire_task_receipts(
        &self,
        request: ProtectedReceiptExpiry,
    ) -> Result<TaskControlRevision, StorageError> {
        let mut state = self.state.write_control().await;
        state.require_task_checkpoint(request.checkpoint())?;
        if request.batch().floor() > Utc::now() {
            return Err(StorageError::invalid_input(
                "Receipt expiry floor is in the future",
            ));
        }
        state.task_control = state
            .task_control
            .expire(request.batch())
            .map_err(invalid_contract_value)?;
        Ok(state.task_control.revision())
    }
    async fn prepare_task_details_purge(
        &self,
        selection: TaskPurgeSelection,
    ) -> Result<Option<TaskPurgeTicket>, StorageError> {
        self.prepare_task_purge_scope(selection, TaskPurgeScope::TaskDetails)
            .await
    }
    async fn purge_task_details_batch(
        &self,
        request: ProtectedTaskPurge,
        budget: TaskPurgeBudget,
    ) -> Result<TaskPurgeProgress, StorageError> {
        self.purge_task_batch(request, budget).await
    }
    async fn list_pending_task_purges(
        &self,
        limit: u32,
    ) -> Result<Vec<TaskPurgeTicket>, StorageError> {
        if limit == 0 || limit as usize > TASK_CONTROL_MAX_PURGE_TASKS {
            return Err(StorageError::invalid_input(
                "Task purge listing exceeds its bounds",
            ));
        }
        let state = self.state.read_control().await;
        let mut pending = state
            .task_control
            .purge_batches()
            .values()
            .filter(|batch| batch.removed().is_none())
            .map(|batch| batch.ticket().clone())
            .collect::<Vec<_>>();
        pending.sort_by_key(|ticket| {
            (
                state
                    .task_purge_attempts
                    .get(&ticket.manifest().id())
                    .copied()
                    .unwrap_or(0),
                ticket.manifest().id(),
            )
        });
        pending.truncate(limit as usize);
        Ok(pending)
    }
    async fn reapply_task_retention(
        &self,
        request: ProtectedTaskReapplication,
        budget: TaskPurgeBudget,
    ) -> Result<TaskReapplicationProgress, StorageError> {
        self.reapply_task_control(request, budget).await
    }
    async fn list_legacy_task_receipts(
        &self,
        budget: TaskPurgeBudget,
    ) -> Result<Vec<TaskLegacyReceiptCandidate>, StorageError> {
        self.state
            .read_control()
            .await
            .legacy_receipt_candidates(budget)
    }
    async fn backfill_task_receipts(
        &self,
        expected: TaskControlRevision,
        bindings: Vec<TaskLegacyReceiptBinding>,
    ) -> Result<TaskControlRevision, StorageError> {
        self.backfill_receipts(expected, bindings).await
    }
}
