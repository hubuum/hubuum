use super::*;

impl MemoryState {
    pub(crate) fn receipt_lookup(
        &self,
        lookup: &TaskSubmissionLookup,
    ) -> Result<TaskSubmissionResolution, StorageError> {
        if matches!(
            self.task_control.activation(),
            TaskReceiptActivation::Disabled
        ) {
            return Ok(TaskSubmissionResolution::ProtectionDisabled);
        }
        let Some(receipt) = self.task_control.receipt(lookup.identity()) else {
            return if matches!(
                self.task_control.activation(),
                TaskReceiptActivation::Backfilling { .. }
            ) {
                Err(StorageError::unavailable(
                    "Protected task receipt backfill is incomplete",
                ))
            } else {
                Ok(TaskSubmissionResolution::Fresh)
            };
        };
        if receipt.expires_at().is_some_and(|at| at <= Utc::now()) {
            return Ok(TaskSubmissionResolution::ExpiryProtectionRequired(
                receipt.reference().clone(),
            ));
        }
        if receipt.kind() != lookup.kind() {
            return Err(StorageError::conflict(
                "Idempotency key is already in use for a different task kind",
            ));
        }
        if !lookup.can_compare(receipt.fingerprint()) {
            return Ok(TaskSubmissionResolution::LegacyUnverifiable(
                receipt.expired_projection(),
            ));
        }
        if !lookup.matches(receipt.kind(), receipt.fingerprint()) {
            return Err(StorageError::conflict(
                "Idempotency key is already in use for a different request",
            ));
        }
        let task = self
            .tasks
            .values()
            .find(|task| self.task_identity(task.id).ok() == Some(receipt.reference().task()));
        let Some(task) =
            task.filter(|_| !self.task_control.restricts_task(receipt.reference().task()))
        else {
            return Ok(TaskSubmissionResolution::DetailsExpired(
                receipt.expired_projection(),
            ));
        };
        if !self.receipt_details_match(task, receipt) {
            return Ok(TaskSubmissionResolution::DetailsExpired(
                receipt.expired_projection(),
            ));
        }
        if task
            .receipt_admission
            .as_ref()
            .is_some_and(|admission| admission.activated.is_none())
            && matches!(receipt.outcome(), TaskReceiptOutcome::MayExecute)
        {
            return Ok(TaskSubmissionResolution::PendingProtection(
                receipt.ticket().clone(),
            ));
        }
        Ok(TaskSubmissionResolution::Available(Box::new(
            self.task_discovery_projection(task)?,
        )))
    }
    fn receipt_details_match(&self, task: &MemoryTaskRecord, receipt: &TaskReceipt) -> bool {
        match receipt.outcome() {
            TaskReceiptOutcome::MayExecute => task
                .receipt_admission
                .as_ref()
                .is_some_and(|admission| admission.ticket == *receipt.ticket()),
            TaskReceiptOutcome::Terminal {
                status,
                finished_at,
                ..
            } => task.status == status.as_status() && task.finished_at == Some(*finished_at),
            TaskReceiptOutcome::RecoveryUnknown { settled_at } => {
                task.status == StorageTaskStatus::Failed && task.finished_at == Some(*settled_at)
            }
        }
    }
    pub(crate) fn validate_receipt_binding(
        &self,
        request: &StorageTaskCreateRequest,
    ) -> Result<(), StorageError> {
        let Some(binding) = request.receipt_binding() else {
            return Err(StorageError::unavailable(
                "Protected keyed admission requires the configured receipt signing key",
            ));
        };
        if self.task_control.activation().key_fingerprint() != Some(binding.key_fingerprint()) {
            return Err(StorageError::unavailable(
                "Task receipt signing key does not match protected authority",
            ));
        }
        match self
            .logical_identities
            .record(StorageIdentityTarget::Principal(request.submitted_by()))?
            .subject()
        {
            Some(StorageIdentitySubject::Principal {
                identity: IdentityReference::Known { identity },
            }) if identity == binding.lookup().identity().principal() => Ok(()),
            _ => Err(StorageError::conflict(
                "Task receipt principal identity does not match its submitting principal",
            )),
        }
    }
    pub(crate) fn task_details_visible(&self, id: TaskId) -> bool {
        self.tasks
            .get(&id.id())
            .is_some_and(|task| !task.recovery_unavailable)
            && self
                .task_identity(id)
                .is_ok_and(|identity| !self.task_control.restricts_task(identity))
    }
    pub(crate) fn task_outputs_visible(&self, id: TaskId) -> bool {
        self.task_details_visible(id)
            && self
                .task_identity(id)
                .is_ok_and(|identity| !self.task_control.restricts_outputs(identity))
    }
    pub(crate) fn require_task_outputs(&self, id: TaskId) -> Result<(), StorageError> {
        if !self.task_outputs_visible(id) {
            return Err(StorageError::not_found("Task outputs are unavailable"));
        }
        Ok(())
    }
    pub(crate) fn require_task_details(&self, id: TaskId) -> Result<(), StorageError> {
        if !self.task_details_visible(id) {
            return Err(StorageError::not_found("Task details are unavailable"));
        }
        Ok(())
    }
    pub(crate) fn task_admission_claimable(&self, task: &MemoryTaskRecord) -> bool {
        let Some(admission) = &task.receipt_admission else {
            return task.idempotency_key.is_none()
                || matches!(
                    self.task_control.activation(),
                    TaskReceiptActivation::Disabled
                );
        };
        self.task_control.activation().is_protected()
            && admission.activated.is_some()
            && admission.ticket.admitted_incarnation()
                == self.application_control.fence().incarnation()
            && self
                .task_control
                .receipt(admission.ticket.receipt().submission())
                .is_some_and(|receipt| {
                    receipt.ticket() == &admission.ticket
                        && matches!(receipt.outcome(), TaskReceiptOutcome::MayExecute)
                })
    }
    pub(crate) fn capture_task_invocation(
        &self,
        request: &StorageTaskRemoteInvocation,
    ) -> Result<MemoryTaskInvocation, StorageError> {
        let target = self
            .remote_targets
            .get(&request.target_id().id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Remote target is unavailable"))?;
        let target_identity = self.invocation_resource_identity(
            StorageIdentityResourceKind::RemoteTarget,
            request.target_id().into(),
        )?;
        let (_, collection, target_name, _) = target.into_parts();
        let collection_identity = self.invocation_resource_identity(
            StorageIdentityResourceKind::Collection,
            collection.into(),
        )?;
        let actor_identity = request
            .event_context()
            .actor_user_id()
            .map(|id| self.invocation_principal_identity(id))
            .transpose()?;
        let initiator_identity = request
            .event_context()
            .initiator_user_id()
            .map(|id| self.invocation_principal_identity(id))
            .transpose()?;
        Ok(MemoryTaskInvocation {
            request: request.clone(),
            target_name,
            target_identity,
            collection,
            collection_identity,
            actor_identity,
            initiator_identity,
        })
    }
    fn invocation_resource_identity(
        &self,
        kind: StorageIdentityResourceKind,
        id: ResourceId,
    ) -> Result<ResourceIdentity, StorageError> {
        match self
            .logical_identities
            .record(StorageIdentityTarget::Resource { kind, id })?
            .subject()
        {
            Some(StorageIdentitySubject::Resource {
                identity: IdentityReference::Known { identity },
                ..
            }) => Ok(identity),
            _ => Err(StorageError::unavailable(
                "Invocation resource identity is unresolved",
            )),
        }
    }
    fn invocation_principal_identity(
        &self,
        id: PrincipalId,
    ) -> Result<PrincipalIdentity, StorageError> {
        if !self.principals.contains_key(&id.id()) {
            return Err(StorageError::conflict(
                "Invocation actor is no longer present",
            ));
        }
        match self
            .logical_identities
            .record(StorageIdentityTarget::Principal(id))?
            .subject()
        {
            Some(StorageIdentitySubject::Principal {
                identity: IdentityReference::Known { identity },
            }) => Ok(identity),
            _ => Err(StorageError::unavailable(
                "Invocation actor identity is unresolved",
            )),
        }
    }
    fn validate_task_invocation(
        &self,
        invocation: &MemoryTaskInvocation,
    ) -> Result<(), StorageError> {
        if !self
            .remote_targets
            .contains_key(&invocation.request.target_id().id())
            || !self.collections.contains_key(&invocation.collection.id())
            || self.invocation_resource_identity(
                StorageIdentityResourceKind::RemoteTarget,
                invocation.request.target_id().into(),
            )? != invocation.target_identity
            || self.invocation_resource_identity(
                StorageIdentityResourceKind::Collection,
                invocation.collection.into(),
            )? != invocation.collection_identity
            || invocation
                .request
                .event_context()
                .actor_user_id()
                .map(|id| self.invocation_principal_identity(id))
                .transpose()?
                != invocation.actor_identity
            || invocation
                .request
                .event_context()
                .initiator_user_id()
                .map(|id| self.invocation_principal_identity(id))
                .transpose()?
                != invocation.initiator_identity
        {
            return Err(StorageError::conflict(
                "Invocation target, collection or principal changed before activation",
            ));
        }
        Ok(())
    }
    fn validate_admission_principals(
        &self,
        task: &MemoryTaskRecord,
        ticket: &TaskAdmissionTicket,
    ) -> Result<(), StorageError> {
        let submitter = task
            .submitted_by
            .ok_or_else(|| StorageError::conflict("Original task submitter is unavailable"))?;
        if self.invocation_principal_identity(submitter)?
            != ticket.receipt().submission().principal()
        {
            return Err(StorageError::conflict(
                "Original task submitter changed before activation",
            ));
        }
        if let Some(initiator) = task.initiator_principal_id {
            let expected = self
                .logical_identities
                .record(StorageIdentityTarget::Task(task.id))?
                .principal_reference("/initiator_principal_id")
                .ok_or_else(|| {
                    StorageError::conflict("Original task initiator identity is unresolved")
                })?;
            if self.invocation_principal_identity(initiator)? != expected {
                return Err(StorageError::conflict(
                    "Original task initiator changed before activation",
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn emit_task_admission(&mut self, id: TaskId) -> Result<(), StorageError> {
        if let Some(invocation) = self
            .tasks
            .get(&id.id())
            .and_then(|task| task.invocation.as_ref())
        {
            self.validate_task_invocation(invocation)?;
        }
        self.append_task_event_record(
            id,
            StorageTaskEventInput::new(StorageTaskStatus::Queued.as_str(), "Task queued"),
        )?;
        if let Some(invocation) = self
            .tasks
            .get_mut(&id.id())
            .and_then(|task| task.invocation.take())
        {
            self.append_scoped_simple_event_record(MemoryScopedSimpleEventAppend {
                entity_type: EntityType::RemoteTarget,
                entity_id: invocation.request.target_id().id(),
                entity_name: Some(&invocation.target_name),
                collection_id: Some(invocation.collection),
                action: Action::Invoked,
                context: invocation.request.event_context(),
                summary: format!(
                    "Remote target '{}' invoked for {} {} by task {}",
                    invocation.target_name,
                    invocation.request.subject_type().as_str(),
                    invocation.request.subject_id().id(),
                    id.id()
                ),
            })?;
        }
        Ok(())
    }
    pub(crate) fn settle_task_receipt(&mut self, id: TaskId) -> Result<(), StorageError> {
        let task = self
            .tasks
            .get(&id.id())
            .ok_or_else(|| StorageError::not_found("Task is missing"))?;
        if !task.status.is_terminal() {
            return Ok(());
        }
        let finished_at = task.finished_at.ok_or_else(|| {
            StorageError::unavailable("Terminal task has no original completion anchor")
        })?;
        let policy = task
            .retention_policy
            .and_then(|revision| self.task_control.policies().get(&revision));
        let details_expires_at = policy
            .and_then(TaskRetentionPolicy::details_after_terminal)
            .map(|duration| {
                chrono::Duration::from_std(duration)
                    .ok()
                    .and_then(|duration| finished_at.checked_add_signed(duration))
                    .ok_or_else(|| StorageError::invalid_input("Task detail deadline overflow"))
            })
            .transpose()?;
        let identity = self.task_identity(id)?;
        if let Some(receipt) = self.task_control.receipt_for_task(identity)
            && !matches!(receipt.outcome(), TaskReceiptOutcome::RecoveryUnknown { settled_at } if *settled_at == finished_at && task.status == StorageTaskStatus::Failed)
            && !task.recovery_unavailable
        {
            let remote_side_effect =
                (task.kind == StorageTaskKind::RemoteCall).then_some(match task.control.phase() {
                    StorageTaskExecutionPhase::RemoteDispatched { .. } => {
                        hubuum_storage_core::TaskRemoteSideEffectState::PossiblySent
                    }
                    _ => hubuum_storage_core::TaskRemoteSideEffectState::NotSent,
                });
            let next = receipt
                .settle(TaskReceiptOutcome::Terminal {
                    status: StorageTaskTerminalStatus::try_from_status(task.status)
                        .map_err(invalid_contract_value)?,
                    finished_at,
                    remote_side_effect,
                })
                .map_err(invalid_contract_value)?;
            self.task_control = self
                .task_control
                .with_receipt(next)
                .map_err(invalid_contract_value)?;
        }
        if let Some(deadline) = details_expires_at {
            self.tasks
                .get_mut(&id.id())
                .expect("existing task")
                .details_expires_at
                .get_or_insert(deadline);
        }
        Ok(())
    }
}

impl MemoryStorage {
    pub(crate) async fn activate_receipt(
        &self,
        request: ProtectedTaskAdmission,
    ) -> Result<TaskActivationOutcome, StorageError> {
        let mut guard = self.state.write().await?;
        let mut state = guard.clone();
        state.require_task_checkpoint(request.checkpoint())?;
        if state.application_control.fence() != request.execution_fence() {
            return Err(StorageError::conflict(
                "Application changed before task admission activation",
            ));
        }
        let receipt = state
            .task_control
            .receipt(request.ticket().receipt().submission())
            .cloned()
            .ok_or_else(|| {
                StorageError::conflict("Task receipt generation is no longer retained")
            })?;
        if receipt.ticket() != request.ticket() {
            return Err(StorageError::conflict(
                "Task receipt generation changed before activation",
            ));
        }
        let id = state
            .tasks
            .values()
            .find(|task| state.task_identity(task.id).ok() == Some(receipt.reference().task()))
            .map(|task| task.id);
        let Some(id) = id.filter(|id| state.task_details_visible(*id)) else {
            return Ok(TaskActivationOutcome::DetailsExpired(
                receipt.expired_projection(),
            ));
        };
        let task = state.tasks.get(&id.id()).expect("selected task");
        let admission = task
            .receipt_admission
            .as_ref()
            .filter(|admission| admission.ticket == *request.ticket())
            .ok_or_else(|| StorageError::conflict("Task row lacks its original receipt binding"))?;
        if admission.activated.is_some() {
            return task.projection().map(TaskActivationOutcome::AlreadyActive);
        }
        if !matches!(receipt.outcome(), TaskReceiptOutcome::MayExecute)
            || task.status.is_terminal()
            || (admission.emit_admission_event && task.status != StorageTaskStatus::Queued)
            || request.ticket().protection_deadline() <= Utc::now()
        {
            return Err(StorageError::conflict(
                "Task admission is terminal or its protection deadline expired",
            ));
        }
        let emit_admission_event = admission.emit_admission_event;
        if emit_admission_event {
            state.validate_admission_principals(task, request.ticket())?;
        }
        state
            .tasks
            .get_mut(&id.id())
            .expect("selected task")
            .receipt_admission
            .as_mut()
            .expect("validated admission")
            .activated = Some(request.checkpoint().clone());
        if emit_admission_event {
            state.emit_task_admission(id)?;
        }
        let result = state.tasks[&id.id()].projection()?;
        *guard = state;
        Ok(TaskActivationOutcome::Activated(result))
    }
}
