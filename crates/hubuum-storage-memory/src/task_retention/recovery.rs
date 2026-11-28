use super::purge::{PurgeWork, measured};
use super::*;

impl MemoryState {
    fn legacy_owner(&self, id: TaskId) -> Result<PrincipalIdentity, StorageError> {
        self.logical_identities
            .record(StorageIdentityTarget::Task(id))?
            .principal_reference("/submitted_by")
            .ok_or_else(|| StorageError::unavailable("Legacy task owner identity is unresolved"))
    }
    pub(super) fn legacy_receipt_candidates(
        &self,
        budget: TaskPurgeBudget,
    ) -> Result<Vec<TaskLegacyReceiptCandidate>, StorageError> {
        let mut output = Vec::new();
        let mut work = PurgeWork::new(budget);
        for task in self
            .tasks
            .values()
            .filter(|task| task.idempotency_key.is_some())
        {
            let identity = self.task_identity(task.id)?;
            if self.task_control.receipt_for_task(identity).is_some() {
                continue;
            }
            match work.charge(task.detail_bytes()?) {
                Ok(true) => {}
                Ok(false) => break,
                Err(_) => {
                    return Err(StorageError::unavailable(
                        "Legacy task exceeds the bounded backfill row budget",
                    ));
                }
            }
            output.push(TaskLegacyReceiptCandidate::new(
                task.projection()?,
                identity,
                self.legacy_owner(task.id)?,
            ));
        }
        Ok(output)
    }

    fn settle_recovered_task(
        &mut self,
        id: TaskId,
        now: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        let identity = self.task_identity(id)?;
        let receipt = self.task_control.receipt_for_task(identity).cloned();
        let mut anchor = now;
        let mut status = StorageTaskStatus::Failed;
        let mut unavailable = false;
        if let Some(receipt) = &receipt {
            match receipt.outcome() {
                TaskReceiptOutcome::MayExecute => {
                    let settled = receipt
                        .settle(TaskReceiptOutcome::RecoveryUnknown { settled_at: now })
                        .map_err(invalid_contract_value)?;
                    self.task_control = self
                        .task_control
                        .with_receipt(settled)
                        .map_err(invalid_contract_value)?;
                }
                TaskReceiptOutcome::Terminal {
                    status: terminal,
                    finished_at,
                    ..
                } => {
                    status = terminal.as_status();
                    anchor = *finished_at;
                    unavailable = true;
                }
                TaskReceiptOutcome::RecoveryUnknown { settled_at } => {
                    anchor = *settled_at;
                    unavailable = true;
                }
            }
        }
        let task = self.tasks.get_mut(&id.id()).expect("selected task");
        task.status = status;
        task.finished_at = Some(anchor);
        task.updated_at = now;
        task.summary = Some(
            "Task execution outcome is unknown after recovery; automatic replay is disabled".into(),
        );
        task.request_payload = None;
        task.request_redacted_at.get_or_insert(now);
        task.lease_expires_at = None;
        task.claim_token = None;
        task.execution_fence = None;
        task.invocation = None;
        task.recovery_unavailable = unavailable;
        if let Some(receipt) = receipt {
            task.details_expires_at = receipt.details_expires_at();
        }
        self.append_task_event_record(
            id,
            StorageTaskEventInput::new(
                status.as_str(),
                "Task reconciled without replay after recovery",
            ),
        )?;
        Ok(())
    }
}

impl MemoryStorage {
    pub(super) async fn backfill_receipts(
        &self,
        expected: TaskControlRevision,
        bindings: Vec<TaskLegacyReceiptBinding>,
    ) -> Result<TaskControlRevision, StorageError> {
        if bindings.is_empty() || bindings.len() > TASK_CONTROL_MAX_PURGE_TASKS {
            return Err(StorageError::invalid_input(
                "Receipt backfill requires 1..=64 bindings",
            ));
        }
        let mut guard = self.state.write_control().await;
        let mut state = guard.clone();
        state.require_task_revision(expected)?;
        if !matches!(
            state.task_control.activation(),
            TaskReceiptActivation::Backfilling { .. }
        ) {
            return Err(StorageError::conflict(
                "Task receipts are not in backfill mode",
            ));
        }
        let now = Utc::now();
        let original_policy = state.task_control.policy();
        let policy = TaskRetentionPolicy::new(
            original_policy.revision(),
            original_policy.deduplication_after_terminal(),
            None,
        )
        .map_err(invalid_contract_value)?
        .with_pending_protection_timeout(original_policy.pending_protection_timeout())
        .map_err(invalid_contract_value)?;
        let deadline = now
            .checked_add_signed(
                Duration::from_std(policy.pending_protection_timeout()).map_err(|_| {
                    StorageError::invalid_input("Backfill protection timeout overflow")
                })?,
            )
            .ok_or_else(|| StorageError::invalid_input("Backfill protection deadline overflow"))?;
        let mut seen = BTreeSet::new();
        for binding in bindings {
            if !seen.insert(binding.task()) {
                return Err(StorageError::invalid_input(
                    "Backfill task identities must be unique",
                ));
            }
            let task = state
                .tasks
                .values()
                .find(|task| state.task_identity(task.id).ok() == Some(binding.task()))
                .cloned()
                .ok_or_else(|| StorageError::not_found("Legacy task identity is absent"))?;
            if task.idempotency_key.is_none()
                || state.legacy_owner(task.id)? != binding.submission().principal()
            {
                return Err(StorageError::conflict(
                    "Legacy task does not match its original owner and key",
                ));
            }
            if state
                .task_control
                .receipt_for_task(binding.task())
                .is_some()
            {
                return Err(StorageError::conflict(
                    "Legacy task already has a receipt generation",
                ));
            }
            let ticket = TaskAdmissionTicket::new(
                TaskReceiptReference::new(
                    binding.submission().clone(),
                    TaskReceiptGeneration::generate(),
                    binding.task(),
                ),
                state
                    .task_control
                    .revision()
                    .next()
                    .map_err(invalid_contract_value)?,
                state.application_control.fence().incarnation(),
                deadline,
            )
            .map_err(invalid_contract_value)?;
            let outcome = if task.status.is_terminal() {
                TaskReceiptOutcome::Terminal {
                    status: StorageTaskTerminalStatus::try_from_status(task.status)
                        .map_err(invalid_contract_value)?,
                    finished_at: task.finished_at.ok_or_else(|| {
                        StorageError::unavailable(
                            "Legacy terminal task has no original completion timestamp",
                        )
                    })?,
                    remote_side_effect: (task.kind == StorageTaskKind::RemoteCall)
                        .then_some(hubuum_storage_core::TaskRemoteSideEffectState::LegacyUnknown),
                }
            } else {
                TaskReceiptOutcome::MayExecute
            };
            let receipt = TaskReceipt::from_parts(
                ticket.clone(),
                task.kind,
                binding.fingerprint().clone(),
                task.created_at,
                &policy,
                outcome,
            )
            .map_err(invalid_contract_value)?;
            state.task_control = state
                .task_control
                .with_receipt(receipt)
                .map_err(invalid_contract_value)?;
            state
                .tasks
                .get_mut(&task.id.id())
                .expect("selected task")
                .receipt_admission = Some(MemoryTaskAdmission {
                ticket,
                activated: None,
                emit_admission_event: false,
            });
        }
        let revision = state.task_control.revision();
        *guard = state;
        Ok(revision)
    }

    pub(super) async fn reapply_task_control(
        &self,
        request: ProtectedTaskReapplication,
        budget: TaskPurgeBudget,
    ) -> Result<TaskReapplicationProgress, StorageError> {
        let mut guard = self.state.write_control().await;
        let mut state = guard.clone();
        if state.task_control != *request.expected()
            || state.application_control.fence() != request.fence()
        {
            return Err(StorageError::conflict(
                "Task authority or application changed before reapplication",
            ));
        }
        if state
            .application_control
            .is_released_for(&state.deletion_control)
        {
            return Err(StorageError::unavailable(
                "Task reapplication requires quarantined application state",
            ));
        }
        state.require_runtime_drain()?;
        state.require_task_checkpoint(request.checkpoint())?;
        if !state.task_control.receipts().is_empty() {
            let retained: BTreeSet<_> = state
                .task_control
                .receipts()
                .iter()
                .map(|receipt| receipt.reference().submission().principal())
                .collect();
            for principal in state.principals.values() {
                let evidence = state
                    .logical_identities
                    .record(StorageIdentityTarget::Principal(principal.id()))?;
                let Some(StorageIdentitySubject::Principal {
                    identity: IdentityReference::Known { identity },
                }) = evidence.subject()
                else {
                    return Ok(TaskReapplicationProgress::Blocked(
                        TaskPurgeBlock::UnknownIdentity,
                    ));
                };
                if evidence.has_unknown_principal_restore_source() && !retained.contains(&identity)
                {
                    return Ok(TaskReapplicationProgress::Blocked(
                        TaskPurgeBlock::UnknownIdentity,
                    ));
                }
            }
        }
        if !state.task_control.purge_batches().is_empty() {
            let retained: BTreeSet<_> = state
                .task_control
                .purge_batches()
                .values()
                .flat_map(|decision| decision.ticket().manifest().tasks().iter().copied())
                .collect();
            for task in state.tasks.values() {
                let evidence = state
                    .logical_identities
                    .record(StorageIdentityTarget::Task(task.id))?;
                if !matches!(
                    evidence.subject(),
                    Some(StorageIdentitySubject::Task {
                        identity: IdentityReference::Known { .. },
                    })
                ) {
                    return Ok(TaskReapplicationProgress::Blocked(
                        TaskPurgeBlock::UnknownIdentity,
                    ));
                }
                if evidence.has_unknown_task_restore_source()
                    && !retained.contains(&state.task_identity(task.id)?)
                {
                    return Ok(TaskReapplicationProgress::Blocked(
                        TaskPurgeBlock::UnknownIdentity,
                    ));
                }
            }
        }
        if let Some(proof) = &state.task_reapplication
            && proof.incarnation() == request.fence().incarnation()
            && proof.task_revision() == state.task_control.revision()
        {
            return Ok(TaskReapplicationProgress::Complete(proof.clone()));
        }
        let mut work = PurgeWork::new(budget);
        let pending: Vec<_> = state
            .tasks
            .values()
            .filter(|task| !task.status.is_terminal())
            .map(|task| task.id)
            .collect();
        for id in pending {
            match work.charge(state.tasks[&id.id()].detail_bytes()?) {
                Ok(true) => state.settle_recovered_task(id, Utc::now())?,
                Ok(false) => {
                    let revision = state.task_control.revision();
                    *guard = state;
                    return Ok(TaskReapplicationProgress::More {
                        revision,
                        rows: work.rows,
                        bytes: work.bytes,
                    });
                }
                Err(block) => {
                    let revision = state.task_control.revision();
                    *guard = state;
                    return Ok(if work.rows > 0 {
                        TaskReapplicationProgress::More {
                            revision,
                            rows: work.rows,
                            bytes: work.bytes,
                        }
                    } else {
                        TaskReapplicationProgress::Blocked(block)
                    });
                }
            }
        }
        // A logical backup can omit all nonterminal tasks. Their protected admissions
        // still mean execution may have occurred and must acquire a fixed outcome.
        let unresolved: Vec<_> = state
            .task_control
            .receipts()
            .iter()
            .filter(|receipt| matches!(receipt.outcome(), TaskReceiptOutcome::MayExecute))
            .cloned()
            .collect();
        for receipt in unresolved {
            match work.charge(receipt.encoded_bytes().map_err(invalid_contract_value)? as u64) {
                Ok(true) => {}
                result => {
                    let revision = state.task_control.revision();
                    *guard = state;
                    return Ok(match result {
                        Err(block) if work.rows == 0 => TaskReapplicationProgress::Blocked(block),
                        _ => TaskReapplicationProgress::More {
                            revision,
                            rows: work.rows,
                            bytes: work.bytes,
                        },
                    });
                }
            }
            state.task_control = state
                .task_control
                .with_receipt(
                    receipt
                        .settle(TaskReceiptOutcome::RecoveryUnknown {
                            settled_at: Utc::now(),
                        })
                        .map_err(invalid_contract_value)?,
                )
                .map_err(invalid_contract_value)?;
        }
        let mismatched: Vec<_> = state
            .tasks
            .values()
            .filter(|task| task.status.is_terminal() && !task.recovery_unavailable)
            .filter_map(|task| {
                let identity = state.task_identity(task.id).ok()?;
                let receipt = state.task_control.receipt_for_task(identity)?;
                let matches = match receipt.outcome() {
                    TaskReceiptOutcome::Terminal {
                        status,
                        finished_at,
                        ..
                    } => {
                        task.status == status.as_status() && task.finished_at == Some(*finished_at)
                    }
                    TaskReceiptOutcome::RecoveryUnknown { settled_at } => {
                        task.status == StorageTaskStatus::Failed
                            && task.finished_at == Some(*settled_at)
                    }
                    TaskReceiptOutcome::MayExecute => false,
                };
                (!matches).then_some(task.id)
            })
            .collect();
        for id in mismatched {
            match work.charge(state.tasks[&id.id()].detail_bytes()?) {
                Ok(true) => {
                    state
                        .tasks
                        .get_mut(&id.id())
                        .expect("selected task")
                        .recovery_unavailable = true
                }
                result => {
                    let revision = state.task_control.revision();
                    *guard = state;
                    return Ok(match result {
                        Err(block) if work.rows == 0 => TaskReapplicationProgress::Blocked(block),
                        _ => TaskReapplicationProgress::More {
                            revision,
                            rows: work.rows,
                            bytes: work.bytes,
                        },
                    });
                }
            }
        }
        let unfinished_schema: Vec<_> = state
            .schema_work
            .iter()
            .filter(|(id, item)| {
                item.status() == StorageSchemaWorkStatus::Running
                    && state
                        .tasks
                        .get(id)
                        .is_some_and(|task| task.status.is_terminal())
            })
            .map(|(id, _)| *id)
            .collect();
        for id in unfinished_schema {
            match work.charge(measured(&state.schema_work[&id])?) {
                Ok(true) => {}
                Ok(false) => {
                    let revision = state.task_control.revision();
                    *guard = state;
                    return Ok(TaskReapplicationProgress::More {
                        revision,
                        rows: work.rows,
                        bytes: work.bytes,
                    });
                }
                Err(block) => {
                    let revision = state.task_control.revision();
                    *guard = state;
                    return Ok(if work.rows > 0 {
                        TaskReapplicationProgress::More {
                            revision,
                            rows: work.rows,
                            bytes: work.bytes,
                        }
                    } else {
                        TaskReapplicationProgress::Blocked(block)
                    });
                }
            }
            let class = state.schema_work[&id].target().class_id();
            let epoch = state.schema_epochs.get(&class.id()).copied().unwrap_or(0);
            state
                .schema_work
                .get_mut(&id)
                .expect("selected schema work")
                .finish(StorageSchemaWorkStatus::Failed, epoch);
        }
        let unfinished_computations: Vec<_> = state
            .computation_states
            .iter()
            .filter(|(_, item)| {
                item.active_task_id()
                    .and_then(|id| state.tasks.get(&id.id()))
                    .is_some_and(|task| task.status.is_terminal())
            })
            .map(|(id, _)| *id)
            .collect();
        for id in unfinished_computations {
            let previous = &state.computation_states[&id];
            let bytes = measured(&(
                previous.class_id().id(),
                previous.evaluation_revision().get(),
                previous.rebuild_status().as_str(),
                previous.active_task_id().map(|id| id.id()),
                previous.last_error_message(),
                previous.created_at(),
                previous.updated_at(),
            ))?;
            match work.charge(bytes) {
                Ok(true) => {}
                result => {
                    let revision = state.task_control.revision();
                    *guard = state;
                    return Ok(match result {
                        Err(block) if work.rows == 0 => TaskReapplicationProgress::Blocked(block),
                        _ => TaskReapplicationProgress::More {
                            revision,
                            rows: work.rows,
                            bytes: work.bytes,
                        },
                    });
                }
            }
            let settled = StorageClassComputationState::try_new(
                previous.class_id(),
                previous.evaluation_revision(),
                StorageComputationRebuildState::Failed {
                    last_error: "Task execution outcome is unknown after recovery".into(),
                },
                previous.created_at(),
                Utc::now(),
            )
            .map_err(invalid_contract_value)?;
            state.computation_states.insert(id, settled);
        }
        let decisions: Vec<_> = state
            .task_control
            .purge_batches()
            .values()
            .cloned()
            .collect();
        for decision in decisions {
            match state.purge_manifest_rows(decision.ticket(), &mut work)? {
                Err(block) => {
                    let revision = state.task_control.revision();
                    *guard = state;
                    return Ok(if work.rows > 0 {
                        TaskReapplicationProgress::More {
                            revision,
                            rows: work.rows,
                            bytes: work.bytes,
                        }
                    } else {
                        TaskReapplicationProgress::Blocked(block)
                    });
                }
                Ok(false) => {
                    let revision = state.task_control.revision();
                    *guard = state;
                    return Ok(TaskReapplicationProgress::More {
                        revision,
                        rows: work.rows,
                        bytes: work.bytes,
                    });
                }
                Ok(true) => {
                    state.task_control = state
                        .task_control
                        .with_purge(
                            decision
                                .record_removed(request.checkpoint().clone())
                                .map_err(invalid_contract_value)?,
                        )
                        .map_err(invalid_contract_value)?;
                }
            }
        }
        state.task_capacity = state.task_accounting_snapshot()?;
        state.task_control = state
            .task_control
            .record_reapplication()
            .map_err(invalid_contract_value)?;
        let proof = TaskControlReapplication::new(
            state.application_control.fence().incarnation(),
            state.task_control.revision(),
        )
        .map_err(invalid_contract_value)?;
        state.task_reapplication = Some(proof.clone());
        *guard = state;
        Ok(TaskReapplicationProgress::Complete(proof))
    }
}
