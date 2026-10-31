use super::*;
use hubuum_storage_core::{
    DELETION_CONTROL_MAX_RUNTIMES, HotObjectHistoryCleanup, HotObjectHistoryDeletion,
    RuntimeRegistration, StorageIdentitySubject, StorageRecordIdentity,
};

#[async_trait]
impl DeletionControlStorage for MemoryStorage {
    async fn register_runtime(&self) -> Result<RuntimeRegistration, StorageError> {
        let mut state = self.state.write().await;
        state.ensure_task_execution_allowed()?;
        if state.runtime_registrations.len() >= DELETION_CONTROL_MAX_RUNTIMES {
            return Err(StorageError::unavailable(
                "Runtime registration capacity is exhausted; retain existing registrations",
            ));
        }
        let registration = RuntimeRegistration::generate();
        state
            .runtime_registrations
            .insert(registration.id(), registration.ownership_token());
        Ok(registration)
    }

    async fn unregister_runtime(
        &self,
        registration: RuntimeRegistration,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write().await;
        if let Some(owner) = state.runtime_registrations.get(&registration.id()) {
            if *owner != registration.ownership_token() {
                return Err(StorageError::conflict(
                    "Runtime registration belongs to another owner",
                ));
            }
            state.runtime_registrations.remove(&registration.id());
        }
        Ok(())
    }

    async fn prepare_hot_object_history_deletion(
        &self,
        expected: DeletionControlState,
        request: HotObjectHistoryDeletion,
    ) -> Result<DeletionControlState, StorageError> {
        let mut state = self.state.write().await;
        state.require_offline_deletion()?;
        if state.deletion_control != expected {
            return Err(StorageError::conflict(
                "Deletion authority changed; recapture before retrying",
            ));
        }
        let change = state
            .deletion_control
            .record_hot_object_history_intent(&request)
            .map_err(invalid_contract_value)?;
        let existing = state
            .deletion_control
            .intents()
            .contains_key(&request.intent());
        if state
            .closed_object_history_copy(request.target())?
            .is_none()
            && !existing
        {
            return Err(StorageError::not_found(
                "The exact closed object-history copy was not found",
            ));
        }
        state.deletion_control = change.next().clone();
        Ok(state.deletion_control.clone())
    }

    async fn cleanup_hot_object_history(
        &self,
        request: HotObjectHistoryCleanup,
    ) -> Result<DeletionControlState, StorageError> {
        let mut state = self.state.write().await;
        state.require_offline_deletion()?;
        if &state.deletion_control != request.expected() {
            return Err(StorageError::conflict(
                "Deletion authority changed; recapture before retrying",
            ));
        }
        let change = state
            .deletion_control
            .record_hot_object_history_removed(&request)
            .map_err(invalid_contract_value)?;
        // A removed receipt is historical evidence, not proof that a restore
        // has not recreated the copy. Always inspect the current application.
        if let Some(index) = state.closed_object_history_copy(request.request().target())? {
            state.history.remove(index);
        }
        state.deletion_control = change.next().clone();
        Ok(state.deletion_control.clone())
    }

    async fn capture_deletion_control(&self) -> Result<DeletionControlCapture, StorageError> {
        let state = self.state.read().await;
        Ok(DeletionControlCapture::new(
            state.deletion_control.clone(),
            DeletionRecoveryPosition::memory(state.deletion_control.lineage())
                .map_err(invalid_contract_value)?,
        ))
    }

    async fn commit_deletion_control(
        &self,
        change: DeletionControlChange,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write().await;
        if &state.deletion_control != change.previous() {
            return Err(StorageError::conflict(
                "Deletion authority changed; recapture before retrying",
            ));
        }
        state.deletion_control = change.next().clone();
        Ok(())
    }
}

impl MemoryState {
    fn require_offline_deletion(&self) -> Result<(), StorageError> {
        if !self.runtime_registrations.is_empty() {
            return Err(StorageError::unavailable(
                "Registered runtimes prevent offline deletion; stop every runtime cleanly",
            ));
        }
        Ok(())
    }

    fn closed_object_history_copy(
        &self,
        target: StorageRecordIdentity,
    ) -> Result<Option<usize>, StorageError> {
        let mut found = None;
        for (index, entry) in self.history.iter().enumerate() {
            let Some(subject @ StorageIdentitySubject::Resource { version, .. }) =
                entry.logical_identity.subject()
            else {
                continue;
            };
            if version != target.version() {
                continue;
            }
            if !matches!(entry.value, MemoryHistoryValue::Object(_))
                || subject.record_identity() != Some(target)
                || entry.valid_to.is_none()
                || found.is_some()
            {
                return Err(StorageError::conflict(
                    "Object-history copy is open, ambiguous, or has different identity evidence",
                ));
            }
            found = Some(index);
        }
        Ok(found)
    }
}

#[cfg(test)]
mod tests;
