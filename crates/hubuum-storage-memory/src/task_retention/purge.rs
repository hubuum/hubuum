use std::io::{self, Write};

use super::*;

#[derive(Default)]
struct EncodedBytes(u64);
impl Write for EncodedBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("Encoded row size overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub(crate) fn measured<T: serde::Serialize>(value: &T) -> Result<u64, StorageError> {
    let mut count = EncodedBytes::default();
    serde_json::to_writer(&mut count, value)
        .map_err(|_| StorageError::internal("Cannot measure persisted task detail"))?;
    Ok(count.0)
}

pub(crate) struct PurgeWork {
    budget: TaskPurgeBudget,
    pub(crate) rows: u32,
    pub(crate) bytes: u64,
}
impl PurgeWork {
    pub(crate) fn new(budget: TaskPurgeBudget) -> Self {
        Self {
            budget,
            rows: 0,
            bytes: 0,
        }
    }
    pub(crate) fn charge(&mut self, bytes: u64) -> Result<bool, TaskPurgeBlock> {
        if bytes > self.budget.max_bytes() {
            return Err(TaskPurgeBlock::OversizedRow);
        }
        if self.rows == self.budget.max_rows() || bytes > self.budget.max_bytes() - self.bytes {
            return Ok(false);
        }
        self.rows += 1;
        self.bytes += bytes;
        Ok(true)
    }

    pub(crate) fn remaining_rows(&self) -> u32 {
        self.budget.max_rows() - self.rows
    }
}

impl MemoryTaskRecord {
    pub(crate) fn detail_bytes(&self) -> Result<u64, StorageError> {
        // Borrow payloads when measuring; counting must not duplicate a maximum-sized row.
        measured(&(
            (
                self.id.id(),
                self.kind.as_str(),
                self.status.as_str(),
                self.submitted_by,
                &self.idempotency_key,
                &self.request_hash,
                &self.request_payload,
                self.metadata.as_ref().map(StorageTaskMetadata::to_value),
                &self.summary,
            ),
            (
                self.progress.total(),
                self.progress.processed(),
                self.progress.succeeded(),
                self.progress.failed(),
                self.scope_snapshot.scopes(),
                self.scope_snapshot.token_id(),
                self.scope_snapshot.scoped(),
                self.request_redacted_at,
                self.started_at,
                self.finished_at,
                self.deleted_at,
                self.deleted_by,
            ),
            (
                self.created_at,
                self.updated_at,
                self.lease_expires_at,
                self.attempt_count,
                self.initiator_principal_id,
                &self.claim_token,
                self.execution_fence,
                self.control.snapshot_fields(),
                self.retention_policy,
                self.details_expires_at,
                self.recovery_unavailable,
                &self.trace_link,
            ),
            self.receipt_admission
                .as_ref()
                .map(|value| (&value.ticket, &value.activated, value.emit_admission_event)),
            self.invocation.as_ref().map(|value| {
                (
                    value.request.target_id(),
                    value.request.subject_type().as_str(),
                    value.request.subject_id(),
                    value.request.event_context(),
                    &value.target_name,
                    value.target_identity,
                    value.collection,
                    value.collection_identity,
                    value.actor_identity,
                    value.initiator_identity,
                )
            }),
        ))
    }
}

impl MemoryState {
    pub(crate) fn task_purge_block(
        &self,
        task: &MemoryTaskRecord,
        scope: TaskPurgeScope,
        now: DateTime<Utc>,
    ) -> Result<Option<TaskPurgeBlock>, StorageError> {
        let identity = self.task_identity(task.id)?;
        if !task.status.is_terminal() {
            return Ok(Some(TaskPurgeBlock::Nonterminal));
        }
        if self.task_control.holds_task(identity, now) {
            return Ok(Some(TaskPurgeBlock::Held));
        }
        let id = task.id.id();
        if scope == TaskPurgeScope::TaskDetails && self.schema_repair_reports.contains_key(&id) {
            return Ok(Some(TaskPurgeBlock::OutputAvailable));
        }
        if self
            .export_outputs
            .get(&id)
            .is_some_and(|value| value.output_expires_at() > now)
            || self
                .backup_outputs
                .get(&id)
                .is_some_and(|value| value.output_expires_at() > now)
            || self
                .schema_profile_outputs
                .get(&id)
                .is_some_and(|value| value.expires_at() > now)
            || self.artifacts.has_available_task_output(identity, now)
        {
            return Ok(Some(TaskPurgeBlock::OutputAvailable));
        }
        if self
            .computation_states
            .values()
            .any(|state| state.active_task_id() == Some(task.id))
            || self
                .schema_work
                .get(&id)
                .is_some_and(|work| work.status() == StorageSchemaWorkStatus::Running)
        {
            return Ok(Some(TaskPurgeBlock::Dependency));
        }
        Ok(None)
    }

    pub(crate) fn purge_manifest_rows(
        &mut self,
        ticket: &TaskPurgeTicket,
        work: &mut PurgeWork,
    ) -> Result<Result<bool, TaskPurgeBlock>, StorageError> {
        let mut blocked = None;
        let mut more = false;
        'targets: for identity in ticket.manifest().tasks() {
            if self.task_control.holds_task(*identity, Utc::now()) {
                blocked.get_or_insert(TaskPurgeBlock::Held);
                continue;
            }
            let mut selected = None;
            for task in self.tasks.values() {
                let actual = self.task_identity(task.id)?;
                if actual == *identity && selected.replace(task.id).is_some() {
                    return Err(StorageError::unavailable(
                        "Task identity aliases multiple live detail records",
                    ));
                }
            }
            if let Some(id) = selected
                && let Some(block) = self.task_purge_block(
                    &self.tasks[&id.id()],
                    ticket.manifest().scope(),
                    Utc::now(),
                )?
            {
                blocked.get_or_insert(block);
                continue;
            }
            match self.artifacts.purge_task(*identity, work)? {
                Ok(true) => {}
                Ok(false) => {
                    more = true;
                    continue;
                }
                Err(block) => {
                    blocked.get_or_insert(block);
                    continue;
                }
            }
            let Some(id) = selected else {
                continue;
            };
            macro_rules! charge {
                ($bytes:expr) => {
                    match work.charge($bytes) {
                        Ok(true) => {}
                        Ok(false) => {
                            more = true;
                            continue 'targets;
                        }
                        Err(block) => {
                            blocked.get_or_insert(block);
                            continue 'targets;
                        }
                    }
                };
            }
            if let Some(value) = self.schema_profile_outputs.get(&id.id()) {
                charge!(measured(&(value.report(), value.expires_at()))?);
                self.schema_profile_outputs.remove(&id.id());
            }
            if let Some(value) = self.export_outputs.get(&id.id()) {
                charge!(export_bytes(value)?);
                self.export_outputs.remove(&id.id());
            }
            if let Some(value) = self.backup_outputs.get(&id.id()) {
                charge!(backup_bytes(value)?);
                self.backup_outputs.remove(&id.id());
            }
            if ticket.manifest().scope() == TaskPurgeScope::ExpiredOutputs {
                continue;
            }
            while let Some(result) = self
                .import_task_results
                .get(&id.id())
                .and_then(|values| values.last())
            {
                let bytes = measured(
                    &serde_json::json!({"id":result.id().id(),"task_id":result.task_id().id(),"item_ref":result.item_ref(),"entity_kind":result.entity_kind(),"action":result.action(),"identifier":result.identifier(),"outcome":result.outcome(),"error":result.error(),"details":result.details(),"created_at":result.created_at()}),
                )?;
                charge!(bytes);
                self.import_task_results
                    .get_mut(&id.id())
                    .expect("existing result")
                    .pop();
            }
            self.import_task_results.remove(&id.id());
            while let Some(event) = self
                .task_events
                .get(&id.id())
                .and_then(|values| values.last())
            {
                let bytes = measured(
                    &serde_json::json!({"task_id":event.task_id().id(),"event_type":event.event_type(),"message":event.message(),"data":event.data(),"created_at":event.created_at(),"actor":event.actor_principal_id().map(PrincipalId::id),"actor_kind":event.actor_kind(),"initiator":event.initiator_principal_id().map(PrincipalId::id),"provenance":event.provenance_task_id().map(TaskId::id)}),
                )?;
                charge!(bytes);
                self.task_events
                    .get_mut(&id.id())
                    .expect("existing task event index")
                    .pop();
            }
            self.task_events.remove(&id.id());
            while let Some((key, finding)) = self
                .schema_findings
                .range((id.id(), i32::MIN)..=(id.id(), i32::MAX))
                .next()
            {
                let key = *key;
                charge!(measured(finding)?);
                self.schema_findings.remove(&key);
            }
            while let Some(index) = self.remote_call_results.iter().position(|row| {
                row.get("task_id").and_then(serde_json::Value::as_i64) == Some(i64::from(id.id()))
            }) {
                charge!(measured(&self.remote_call_results[index])?);
                self.remote_call_results.remove(index);
            }
            while let Some(entry) = self
                .import_execution_receipts
                .range((id.id(), 0)..=(id.id(), usize::MAX))
                .next()
                .copied()
            {
                charge!(measured(&entry)?);
                self.import_execution_receipts.remove(&entry);
            }
            if let Some(value) = self.schema_repair_reports.get(&id.id()) {
                charge!(measured(value)?);
                self.schema_repair_reports.remove(&id.id());
            }
            if let Some(value) = self.schema_work.get(&id.id()) {
                charge!(measured(value)?);
                self.schema_work.remove(&id.id());
            }
            if let Some(value) = self.computed_rebuild_tasks.get(&id.id()) {
                charge!(measured(&(id.id(), value.id()))?);
                self.computed_rebuild_tasks.remove(&id.id());
            }
            charge!(self.tasks[&id.id()].detail_bytes()?);
            self.tasks.remove(&id.id());
        }
        self.task_capacity = self.task_accounting_snapshot()?;
        Ok(if more {
            Ok(false)
        } else if let Some(block) = blocked {
            Err(block)
        } else {
            Ok(true)
        })
    }

    pub(crate) fn sweep_expired_outputs(
        &mut self,
        kind: StorageTaskKind,
    ) -> Result<usize, StorageError> {
        if !matches!(
            self.task_control.activation(),
            TaskReceiptActivation::Disabled
        ) {
            return Err(StorageError::unavailable(
                "Protected output cleanup requires its checkpointed immutable manifest",
            ));
        }
        let now = Utc::now();
        let budget = self.task_control.policy().cleanup_budget();
        let mut work = PurgeWork::new(budget);
        let ids: Vec<i32> = match kind {
            StorageTaskKind::Export => self.export_outputs.keys().copied().collect(),
            StorageTaskKind::Backup => self.backup_outputs.keys().copied().collect(),
            StorageTaskKind::SchemaProfiling => {
                self.schema_profile_outputs.keys().copied().collect()
            }
            _ => {
                return Err(StorageError::invalid_input(
                    "Unsupported task output family",
                ));
            }
        };
        for id in ids {
            let task_id =
                TaskId::new(id).map_err(|error| StorageError::invalid_input(error.to_string()))?;
            if self
                .task_control
                .holds_task(self.task_identity(task_id)?, now)
            {
                continue;
            }
            let (expired, bytes) = match kind {
                StorageTaskKind::Export => {
                    let value = &self.export_outputs[&id];
                    (value.output_expires_at() <= now, export_bytes(value)?)
                }
                StorageTaskKind::Backup => {
                    let value = &self.backup_outputs[&id];
                    (value.output_expires_at() <= now, backup_bytes(value)?)
                }
                StorageTaskKind::SchemaProfiling => {
                    let value = &self.schema_profile_outputs[&id];
                    (
                        value.expires_at() <= now,
                        measured(&(value.report(), value.expires_at()))?,
                    )
                }
                _ => unreachable!("validated output family"),
            };
            if !expired {
                continue;
            }
            match work.charge(bytes) {
                Ok(true) => {}
                Ok(false) => break,
                Err(_) => continue,
            }
            match kind {
                StorageTaskKind::Export => {
                    self.export_outputs.remove(&id);
                }
                StorageTaskKind::Backup => {
                    self.backup_outputs.remove(&id);
                }
                StorageTaskKind::SchemaProfiling => {
                    self.schema_profile_outputs.remove(&id);
                }
                _ => unreachable!("validated output family"),
            }
        }
        self.task_capacity = self.task_accounting_snapshot()?;
        Ok(work.rows as usize)
    }
}

fn export_bytes(value: &StorageExportOutput) -> Result<u64, StorageError> {
    measured(&(
        value.task_id().id(),
        value.template_name(),
        value.content_type(),
        value.json_output(),
        value.text_output(),
        value.metadata(),
        value.warnings(),
        value.warning_count(),
        value.truncated(),
        value.output_expires_at(),
        value.created_at(),
    ))
}
fn backup_bytes(value: &StorageBackupOutput) -> Result<u64, StorageError> {
    measured(&(
        value.task_id().id(),
        value.byte_size(),
        value.sha256(),
        value.output_expires_at(),
        value.created_at(),
    ))?
    .checked_add(value.byte_size() as u64)
    .ok_or_else(|| StorageError::internal("Backup row size overflow"))
}

impl MemoryStorage {
    pub(crate) async fn prepare_task_purge_scope(
        &self,
        selection: TaskPurgeSelection,
        scope: TaskPurgeScope,
    ) -> Result<Option<TaskPurgeTicket>, StorageError> {
        let mut state = self.state.write_control().await;
        if !state.task_control.activation().is_protected() {
            return Err(StorageError::unavailable(
                "Protected task retention is not enabled",
            ));
        }
        let now = Utc::now();
        let mut policy = None;
        let mut targets = Vec::new();
        for task in state.tasks.values() {
            if !task.status.is_terminal() {
                continue;
            }
            let identity = state.task_identity(task.id)?;
            let already_selected = match scope {
                TaskPurgeScope::TaskDetails => state.task_control.restricts_task(identity),
                TaskPurgeScope::ExpiredOutputs => state.task_control.restricts_outputs(identity),
            };
            let due = match scope {
                TaskPurgeScope::TaskDetails => task.details_expires_at.is_some_and(|at| at <= now),
                TaskPurgeScope::ExpiredOutputs => {
                    state
                        .export_outputs
                        .get(&task.id.id())
                        .is_some_and(|value| value.output_expires_at() <= now)
                        || state
                            .backup_outputs
                            .get(&task.id.id())
                            .is_some_and(|value| value.output_expires_at() <= now)
                        || state
                            .schema_profile_outputs
                            .get(&task.id.id())
                            .is_some_and(|value| value.expires_at() <= now)
                        || state.artifacts.has_task_output_for_cleanup(identity, now)
                }
            };
            if !due || already_selected || state.task_purge_block(task, scope, now)?.is_some() {
                continue;
            }
            let revision = match task.retention_policy {
                Some(value) => value,
                None if scope == TaskPurgeScope::ExpiredOutputs => {
                    state.task_control.policy().revision()
                }
                None => continue,
            };
            if policy.is_some_and(|selected| selected != revision) {
                continue;
            }
            policy = Some(revision);
            targets.push(identity);
            if targets.len() == selection.max_tasks().min(selection.budget().max_rows()) as usize {
                break;
            }
        }
        if targets.is_empty() {
            return Ok(None);
        }
        targets.sort_unstable();
        let manifest = TaskPurgeManifest::new(
            TaskPurgeBatchId::generate(),
            scope,
            targets,
            policy.expect("selected policy"),
            now,
        )
        .map_err(invalid_contract_value)?;
        let ticket = TaskPurgeTicket::new(
            manifest,
            1,
            state
                .task_control
                .revision()
                .next()
                .map_err(invalid_contract_value)?,
        )
        .map_err(invalid_contract_value)?;
        if measured(&ticket)? > selection.budget().max_bytes() {
            return Err(StorageError::unavailable(
                "Task purge manifest exceeds the selection byte budget",
            ));
        }
        state.task_control = state
            .task_control
            .with_purge(TaskPurgeDecision::pending(ticket.clone()))
            .map_err(invalid_contract_value)?;
        Ok(Some(ticket))
    }
    pub(crate) async fn purge_task_batch(
        &self,
        request: ProtectedTaskPurge,
        budget: TaskPurgeBudget,
    ) -> Result<TaskPurgeProgress, StorageError> {
        let mut guard = self.state.write_control().await;
        let mut state = guard.clone();
        state.require_task_checkpoint(request.checkpoint())?;
        if request.checkpoint().task_revision() < state.task_control.hold_release_revision() {
            return Err(StorageError::unavailable(
                "Task hold release requires an independently protected checkpoint",
            ));
        }
        let decision = state
            .task_control
            .purge_batches()
            .get(&request.ticket().manifest().id())
            .filter(|value| value.ticket() == request.ticket())
            .cloned()
            .ok_or_else(|| {
                StorageError::conflict("Task purge ticket does not match its retained manifest")
            })?;
        let attempt = state.next_task_purge_attempt;
        state.next_task_purge_attempt = attempt
            .checked_add(1)
            .ok_or_else(|| StorageError::internal("Task purge scheduler sequence exhausted"))?;
        state
            .task_purge_attempts
            .insert(request.ticket().manifest().id(), attempt);
        let mut work = PurgeWork::new(budget);
        let result = match state.purge_manifest_rows(request.ticket(), &mut work)? {
            Err(_) if work.rows > 0 => TaskPurgeProgress::More {
                rows: work.rows,
                bytes: work.bytes,
            },
            Err(block) => TaskPurgeProgress::Blocked(block),
            Ok(false) => TaskPurgeProgress::More {
                rows: work.rows,
                bytes: work.bytes,
            },
            Ok(true) => {
                state.task_control = state
                    .task_control
                    .with_purge(
                        decision
                            .record_removed(request.checkpoint().clone())
                            .map_err(invalid_contract_value)?,
                    )
                    .map_err(invalid_contract_value)?;
                TaskPurgeProgress::Removed {
                    revision: state.task_control.revision(),
                    rows: work.rows,
                    bytes: work.bytes,
                }
            }
        };
        *guard = state;
        Ok(result)
    }
}
