use super::*;
use hubuum_storage_core::{
    ApplicationReapplication, ApplicationWorkAdmission, DELETION_CONTROL_MAX_RUNTIMES,
    DeletionCheckpointIdentity, HotObjectHistoryCleanup, HotObjectHistoryDeletion,
    RuntimeRegistration, StorageExecutionPermit, StorageIdentityResourceKind,
    StorageIdentitySubject, StorageIdentityTarget, StorageRecordIdentity,
};

#[async_trait]
impl DeletionControlStorage for MemoryStorage {
    async fn register_runtime_isolated(
        &self,
        isolation: Option<RuntimeIsolationDomain>,
    ) -> Result<RuntimeRegistration, StorageError> {
        let mut state = self.state.write_control().await;
        state.ensure_task_execution_allowed()?;
        if state.runtime_registrations.len() >= DELETION_CONTROL_MAX_RUNTIMES {
            return Err(StorageError::unavailable(
                "Runtime registration capacity is exhausted; retain existing registrations",
            ));
        }
        let registration = RuntimeRegistration::generate();
        state.runtime_registrations.insert(
            registration.id(),
            MemoryRuntimeRegistration {
                ownership_token: registration.ownership_token(),
                isolation,
                drained: None,
            },
        );
        Ok(registration)
    }

    async fn capture_application_execution(
        &self,
        owner: Option<RuntimeOwner>,
    ) -> Result<StorageExecutionPermit, StorageError> {
        let mut state = self.state.write_control().await;
        let permit = StorageExecutionPermit::new(owner.clone(), state.application_control.fence());
        state.ensure_application_permit(&permit)?;
        if let Some(owner) = owner
            && state.runtime_registrations[&owner.id()].drained.is_some()
        {
            state
                .runtime_registrations
                .get_mut(&owner.id())
                .expect("validated owner")
                .drained = None;
        }
        Ok(permit)
    }

    async fn capture_application_execution_with_maintenance(
        &self,
        owner: Option<RuntimeOwner>,
    ) -> Result<ApplicationWorkAdmission, StorageError> {
        if self
            .state
            .runtime_owner()
            .is_some_and(|bound| Some(bound) != owner.as_ref())
        {
            return Err(StorageError::unavailable(
                "Worker admission requires its bound runtime owner",
            ));
        }
        let inherited = self
            .current_execution_scope()
            .application_access_override()
            .cloned();
        let mut state = self.state.write_control().await;
        let permit = if let Some(permit) = inherited {
            if permit.owner() != owner.as_ref() {
                return Err(StorageError::unavailable(
                    "Inherited worker permit belongs to another runtime",
                ));
            }
            state.ensure_application_permit(&permit)?;
            permit
        } else {
            let permit =
                StorageExecutionPermit::new(owner.clone(), state.application_control.fence());
            state.ensure_application_permit(&permit)?;
            if let Some(owner) = owner
                && state.runtime_registrations[&owner.id()].drained.is_some()
            {
                state
                    .runtime_registrations
                    .get_mut(&owner.id())
                    .expect("validated owner")
                    .drained = None;
            }
            permit
        };
        Ok(ApplicationWorkAdmission::new(
            permit,
            state.maintenance_state,
        ))
    }

    async fn read_application_control(&self) -> Result<ApplicationControlState, StorageError> {
        Ok(self.state.read_control().await.application_control.clone())
    }

    async fn acknowledge_runtime_drain(
        &self,
        owner: RuntimeOwner,
        fence: ApplicationFence,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write_control().await;
        state.require_runtime_owner(&owner)?;
        if state.application_control.fence() != fence
            || state
                .application_control
                .is_released_for(&state.deletion_control)
        {
            return Err(StorageError::conflict(
                "Application is released or its fence changed before runtime drain",
            ));
        }
        state
            .runtime_registrations
            .get_mut(&owner.id())
            .expect("validated owner")
            .drained = Some(fence);
        Ok(())
    }

    async fn reapply_deletion_decisions(
        &self,
        expected: DeletionControlState,
        fence: ApplicationFence,
        checkpoint: DeletionCheckpointIdentity,
    ) -> Result<ApplicationReapplication, StorageError> {
        let mut state = self.state.write_control().await;
        if state.deletion_control != expected || state.application_control.fence() != fence {
            return Err(StorageError::conflict(
                "Deletion authority or application changed before reapplication",
            ));
        }
        state.require_runtime_drain()?;
        let mut next = expected.clone();
        let mut removals = BTreeSet::new();
        for (intent_id, intent) in expected.intents() {
            if intent.bindings().len() != 1 || intent.copies().len() != 1 {
                return Err(StorageError::unavailable(
                    "Unsupported or unbound deletion obligations prevent application reapplication",
                ));
            }
            let (copy, binding) = intent.bindings().first_key_value().expect("one binding");
            let request = HotObjectHistoryDeletion::new(*intent_id, *copy, binding.target())
                .map_err(invalid_contract_value)?;
            let cleanup = HotObjectHistoryCleanup::new(next.clone(), request, checkpoint.clone())
                .map_err(invalid_contract_value)?;
            if let Some(index) = state.closed_object_history_copy(binding.target())? {
                removals.insert(index);
            }
            next = next
                .record_hot_object_history_removed(&cleanup)
                .map_err(invalid_contract_value)?
                .next()
                .clone();
        }
        let existing = state
            .application_control
            .reapplication()
            .filter(|receipt| receipt.fence() == fence)
            .cloned();
        let receipt = if let Some(receipt) = existing {
            if next != expected {
                return Err(StorageError::conflict(
                    "Reapplication receipt does not cover current copy outcomes",
                ));
            }
            receipt
        } else {
            next = next
                .record_application_reapplication()
                .map_err(invalid_contract_value)?
                .next()
                .clone();
            ApplicationReapplication::new(
                ApplicationFence::new(fence.incarnation(), next.revision())
                    .map_err(invalid_contract_value)?,
            )
        };
        let application = if state.application_control.reapplication() == Some(&receipt) {
            state.application_control.clone()
        } else {
            state
                .application_control
                .record_reapplication(receipt.clone())
                .map_err(invalid_contract_value)?
        };
        // Every target and authority transition is validated before any physical
        // removal, so a later unsupported obligation cannot partially apply.
        for index in removals.into_iter().rev() {
            state.history.remove(index);
        }
        state.deletion_control = next;
        state.application_control = application;
        Ok(receipt)
    }

    async fn release_application_exposure(
        &self,
        receipt: ApplicationReapplication,
        checkpoint: DeletionCheckpointIdentity,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write_control().await;
        if receipt.revision() != state.deletion_control.revision() {
            return Err(StorageError::conflict(
                "Deletion authority changed before exposure release",
            ));
        }
        let released = state
            .application_control
            .release(&receipt, checkpoint)
            .map_err(invalid_contract_value)?;
        state.application_control = released;
        if state.pending_restore {
            state.maintenance_state = MaintenanceState::Normal;
            state.maintenance_restore_job_id = None;
            state.restore_instances.clear();
            state.pending_restore = false;
        }
        Ok(())
    }

    async fn recover_application_control(
        &self,
        change: DeletionControlChange,
        isolation: RuntimeIsolationDomain,
    ) -> Result<ApplicationFence, StorageError> {
        let mut state = self.state.write_control().await;
        if &state.deletion_control != change.previous() {
            return Err(StorageError::conflict(
                "Deletion authority changed before isolated recovery",
            ));
        }
        if state
            .runtime_registrations
            .values()
            .any(|registration| registration.isolation.as_ref() != Some(&isolation))
        {
            return Err(StorageError::unavailable(
                "Unknown or different runtime isolation domains prevent recovery",
            ));
        }
        let fence = ApplicationFence::new(Uuid::new_v4(), change.next().revision())
            .map_err(invalid_contract_value)?;
        state.runtime_registrations.clear();
        state.deletion_control = change.next().clone();
        state.application_control = ApplicationControlState::quarantined(fence);
        Ok(fence)
    }

    async fn unregister_runtime(
        &self,
        registration: RuntimeRegistration,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write_control().await;
        if state.runtime_registrations.contains_key(&registration.id()) {
            state.require_runtime_owner(&registration.owner())?;
            state.runtime_registrations.remove(&registration.id());
        }
        Ok(())
    }

    async fn prepare_hot_object_history_deletion(
        &self,
        expected: DeletionControlState,
        request: HotObjectHistoryDeletion,
    ) -> Result<DeletionControlState, StorageError> {
        let mut state = self.state.write_control().await;
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
        state.apply_deletion_authority(change.next().clone())?;
        Ok(state.deletion_control.clone())
    }

    async fn cleanup_hot_object_history(
        &self,
        request: HotObjectHistoryCleanup,
    ) -> Result<DeletionControlState, StorageError> {
        let mut state = self.state.write_control().await;
        state.require_runtime_drain()?;
        if &state.deletion_control != request.expected() {
            return Err(StorageError::conflict(
                "Deletion authority changed; recapture before retrying",
            ));
        }
        let change = state
            .deletion_control
            .record_hot_object_history_removed(&request)
            .map_err(invalid_contract_value)?;
        let removal = state.closed_object_history_copy(request.request().target())?;
        state.apply_deletion_authority(change.next().clone())?;
        if let Some(index) = removal {
            state.history.remove(index);
        }
        Ok(state.deletion_control.clone())
    }

    async fn capture_deletion_control(&self) -> Result<DeletionControlCapture, StorageError> {
        let state = self.state.read_control().await;
        let capture = DeletionControlCapture::new(
            state.deletion_control.clone(),
            DeletionRecoveryPosition::memory(state.deletion_control.lineage())
                .map_err(invalid_contract_value)?,
        );
        if let Some(receipt) = state
            .application_control
            .reapplication()
            .filter(|receipt| receipt.revision() == state.deletion_control.revision())
        {
            capture
                .with_reapplication(receipt.clone())
                .map_err(invalid_contract_value)
        } else {
            Ok(capture)
        }
    }

    async fn commit_deletion_control(
        &self,
        change: DeletionControlChange,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write_control().await;
        if &state.deletion_control != change.previous() {
            return Err(StorageError::conflict(
                "Deletion authority changed; recapture before retrying",
            ));
        }
        state.apply_deletion_authority(change.next().clone())
    }
}

impl MemoryState {
    pub(super) fn acknowledge_coordinator_drain(
        &mut self,
        owner: &RuntimeOwner,
        local_work_is_settled: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<(), StorageError> {
        if !self
            .application_control
            .is_released_for(&self.deletion_control)
            && local_work_is_settled()
        {
            self.require_runtime_owner(owner)?;
            self.runtime_registrations
                .get_mut(&owner.id())
                .expect("validated owner")
                .drained = Some(self.application_control.fence());
        }
        Ok(())
    }

    fn require_runtime_owner(&self, owner: &RuntimeOwner) -> Result<(), StorageError> {
        if !self
            .runtime_registrations
            .get(&owner.id())
            .is_some_and(|registration| registration.ownership_token == owner.ownership_token())
        {
            return Err(StorageError::conflict(
                "Runtime registration is absent or belongs to another owner",
            ));
        }
        Ok(())
    }

    pub(super) fn ensure_application_permit(
        &self,
        permit: &StorageExecutionPermit,
    ) -> Result<(), StorageError> {
        if let Some(owner) = permit.owner() {
            self.require_runtime_owner(owner)?;
        }
        self.application_control
            .validate_execution(&self.deletion_control, permit)
    }

    fn require_runtime_drain(&self) -> Result<(), StorageError> {
        if self
            .runtime_registrations
            .values()
            .any(|registration| registration.drained != Some(self.application_control.fence()))
        {
            return Err(StorageError::unavailable(
                "Every registered runtime must acknowledge the exact application fence",
            ));
        }
        Ok(())
    }

    fn apply_deletion_authority(&mut self, next: DeletionControlState) -> Result<(), StorageError> {
        if self.deletion_control != next {
            let fence = ApplicationFence::new(
                self.application_control.fence().incarnation(),
                next.revision(),
            )
            .map_err(invalid_contract_value)?;
            self.application_control = ApplicationControlState::quarantined(fence);
            self.deletion_control = next;
        }
        Ok(())
    }

    fn closed_object_history_copy(
        &self,
        target: StorageRecordIdentity,
    ) -> Result<Option<usize>, StorageError> {
        for object in self.objects.values() {
            let evidence = self
                .logical_identities
                .record(StorageIdentityTarget::Resource {
                    kind: StorageIdentityResourceKind::Object,
                    id: object.id().into(),
                })?;
            if evidence
                .subject()
                .and_then(StorageIdentitySubject::record_identity)
                .is_none()
            {
                return Err(StorageError::conflict(
                    "Unresolved current object identity prevents proving deletion target absence",
                ));
            }
            if evidence
                .restore_source_versions()
                .contains(target.version())
                || (evidence.has_unknown_restore_source()
                    && evidence
                        .subject()
                        .and_then(StorageIdentitySubject::record_identity)
                        .is_some_and(|identity| {
                            identity.resource() == target.resource()
                                && identity.version() != target.version()
                        }))
            {
                return Err(StorageError::conflict(
                    "Current object restore ancestry prevents proving deletion target absence",
                ));
            }
            if evidence.subject().is_some_and(|subject| {
                matches!(subject, StorageIdentitySubject::Resource { version, .. } if version == target.version())
            }) {
                return Err(StorageError::conflict("Deletion target is a current live object version"));
            }
        }
        let mut found = None;
        for (index, entry) in self.history.iter().enumerate() {
            if matches!(entry.value, MemoryHistoryValue::Object(_))
                && (entry
                    .logical_identity
                    .restore_source_versions()
                    .contains(target.version())
                    || (entry.logical_identity.has_unknown_restore_source()
                        && entry
                            .logical_identity
                            .subject()
                            .and_then(StorageIdentitySubject::record_identity)
                            .is_some_and(|identity| {
                                identity.resource() == target.resource()
                                    && identity.version() != target.version()
                            })))
            {
                return Err(StorageError::conflict(
                    "Object-history restore ancestry prevents proving deletion target absence",
                ));
            }
            if matches!(entry.value, MemoryHistoryValue::Object(_))
                && entry
                    .logical_identity
                    .subject()
                    .and_then(StorageIdentitySubject::record_identity)
                    .is_none()
            {
                return Err(StorageError::conflict(
                    "Unresolved object-history identity prevents proving deletion target absence",
                ));
            }
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

#[cfg(test)]
mod reapplication_tests;
