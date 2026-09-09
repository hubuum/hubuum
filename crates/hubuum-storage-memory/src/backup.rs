//! Memory-owned projection of the logical backup contract.

use super::*;
use serde_json::{Map, Value, json};

mod identity;
mod resources;
mod workflows;

fn row(value: Value) -> Result<StorageBackupRow, StorageError> {
    StorageBackupRow::try_from_value(value).map_err(invalid_contract_value)
}

fn invalid(field: &str) -> StorageError {
    StorageError::backend_failure(format!("Invalid logical backup field '{field}'"))
}

struct Row<'a>(&'a StorageBackupRow);

impl Row<'_> {
    fn value(&self, field: &str) -> Result<&Value, StorageError> {
        self.0.get(field).ok_or_else(|| invalid(field))
    }
    fn text(&self, field: &str) -> Result<&str, StorageError> {
        self.value(field)?.as_str().ok_or_else(|| invalid(field))
    }
    fn optional_text(&self, field: &str) -> Result<Option<String>, StorageError> {
        self.optional_value(field)
            .map(|v| {
                v.as_str()
                    .map(ToOwned::to_owned)
                    .ok_or_else(|| invalid(field))
            })
            .transpose()
    }
    fn optional_value(&self, field: &str) -> Option<&Value> {
        self.0.get(field).filter(|v| !v.is_null())
    }
    fn number(&self, field: &str) -> Result<i64, StorageError> {
        self.value(field)?.as_i64().ok_or_else(|| invalid(field))
    }
    fn integer(&self, field: &str) -> Result<i32, StorageError> {
        i32::try_from(self.number(field)?).map_err(|_| invalid(field))
    }
    fn optional_integer(&self, field: &str) -> Result<Option<i32>, StorageError> {
        self.optional_value(field)
            .map(|_| self.integer(field))
            .transpose()
    }
    fn boolean(&self, field: &str) -> Result<bool, StorageError> {
        self.value(field)?.as_bool().ok_or_else(|| invalid(field))
    }
    fn time(&self, field: &str) -> Result<DateTime<Utc>, StorageError> {
        DateTime::parse_from_rfc3339(self.text(field)?)
            .map(|t| t.to_utc())
            .map_err(|_| invalid(field))
    }
    fn optional_time(&self, field: &str) -> Result<Option<DateTime<Utc>>, StorageError> {
        self.optional_value(field)
            .map(|_| self.time(field))
            .transpose()
    }
    fn revision(&self) -> Result<ResourceRevision, StorageError> {
        ResourceRevision::new(self.number("revision")?).map_err(|_| invalid("revision"))
    }
    fn metadata(&self) -> Result<StorageRecordMetadata, StorageError> {
        StorageRecordMetadata::try_new(
            ResourceId::new(self.integer("id")?).map_err(|_| invalid("id"))?,
            self.time("created_at")?,
            self.time("updated_at")?,
            self.revision()?,
        )
        .map_err(invalid_contract_value)
    }
}

pub(super) fn capture(
    state: &MemoryState,
    include_history: bool,
) -> Result<StorageBackupSnapshot, StorageError> {
    let mut sections = StorageBackupStateSections::new();
    resources::capture(state, &mut sections)?;
    identity::capture(state, &mut sections)?;
    workflows::capture(state, &mut sections)?;
    let history = include_history
        .then(|| workflows::capture_history(state))
        .transpose()?;
    StorageBackupSnapshot::try_new(sections, history).map_err(invalid_contract_value)
}

pub(super) fn restore(snapshot: StorageBackupSnapshot) -> Result<MemoryState, StorageError> {
    let (sections, history) = snapshot.into_parts();
    let mut state = MemoryState::new();
    resources::restore(&sections, &mut state)?;
    identity::restore(&sections, &mut state)?;
    workflows::restore(&sections, &mut state)?;
    workflows::restore_history(
        history.ok_or_else(|| invalid("prepared history"))?,
        &mut state,
    )?;
    enqueue_computed_rebuilds(&mut state)?;
    Ok(state)
}

fn enqueue_computed_rebuilds(state: &mut MemoryState) -> Result<(), StorageError> {
    let classes = state
        .computed_fields
        .values()
        .filter(|definition| definition.visibility() == StorageComputedFieldVisibility::Shared)
        .map(StorageComputedFieldDefinition::class_id)
        .collect::<BTreeSet<_>>();
    for class_id in classes {
        let now = Utc::now();
        let id = TaskId::new(state.next_task_id).map_err(|_| invalid("rebuild task id"))?;
        state.next_task_id = state
            .next_task_id
            .checked_add(1)
            .ok_or_else(|| invalid("task sequence"))?;
        let record = MemoryTaskRecord {
            id,
            kind: StorageTaskKind::Reindex,
            status: StorageTaskStatus::Queued,
            submitted_by: None,
            idempotency_key: None,
            request_hash: None,
            request_payload: Some(json!({"class_id": class_id.id()})),
            summary: None,
            progress: StorageTaskProgress::try_new(0, 0, 0, 0).map_err(invalid_contract_value)?,
            scope_snapshot: StorageTaskScopeSnapshot::unscoped(),
            request_redacted_at: None,
            started_at: None,
            finished_at: None,
            deleted_at: None,
            deleted_by: None,
            created_at: now,
            updated_at: now,
            lease_expires_at: None,
            attempt_count: 0,
            initiator_principal_id: None,
            trace_link: None,
            claim_token: None,
        };
        record.projection()?;
        state.tasks.insert(id.id(), record);
        state.computed_rebuild_tasks.insert(id.id(), class_id);
        state.computation_states.insert(
            class_id.id(),
            StorageClassComputationState::try_new(
                class_id,
                StorageComputationRevision::try_new(1).map_err(invalid_contract_value)?,
                StorageComputationRebuildState::Rebuilding { active_task_id: id },
                now,
                now,
            )
            .map_err(invalid_contract_value)?,
        );
    }
    Ok(())
}

fn next_id<T>(values: &BTreeMap<i32, T>) -> Result<i32, StorageError> {
    values
        .keys()
        .next_back()
        .copied()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| invalid("id sequence"))
}

pub(super) fn append_restore_event(
    state: &mut MemoryState,
    job: &MemoryRestoreRecord,
    metadata: StorageRestoreDocumentMetadata,
    includes_history: bool,
) -> Result<(), StorageError> {
    let (version, created_at, source_version) = metadata.into_parts();
    let (id, _, initiator, artifact, _, _) = job.job.summary().clone().into_parts();
    let initiator = initiator.into_parts();
    let document = AuditDocument::try_new("System restore completed", None, None, json!({
        "restore_job_id": id.id(), "backup_sha256": artifact.sha256(), "backup_version": version,
        "backup_source_version": source_version, "backup_created_at": created_at, "includes_history": includes_history,
        "initiated_by": {"principal_id": initiator.principal_id().map(PrincipalId::id), "identity_scope": initiator.identity_scope(), "name": initiator.name()}
    })).map_err(|_| invalid("restore provenance"))?;
    let envelope = EventEnvelope::builder()
        .id(EventSequence::new(state.next_event_sequence).map_err(|_| invalid("event sequence"))?)
        .event_id(Uuid::new_v4())
        .occurred_at(Utc::now())
        .entity_type(EntityType::Restore)
        .entity_name(Some(artifact.sha256().to_string()))
        .action(Action::Succeeded)
        .actor_kind(ActorKind::System)
        .summary(document.summary_text().to_string())
        .metadata(document.metadata().clone())
        .schema_version(document.schema_version())
        .try_build()
        .map_err(|_| invalid("restore provenance"))?;
    state.next_event_sequence = state
        .next_event_sequence
        .checked_add(1)
        .ok_or_else(|| invalid("event sequence"))?;
    state
        .events
        .push(StorageRecordedEvent::new(envelope, None, None));
    Ok(())
}

pub(super) fn record_class_relation_history(
    state: &mut MemoryState,
    value: &StorageClassRelation,
    operation: StorageHistoryOperation,
    context: &EventContext,
) -> Result<(), StorageError> {
    record_relation_history(
        state,
        StorageBackupHistorySection::ClassRelationHistory,
        resources::classrelations(value)?,
        operation,
        context,
    )
}

pub(super) fn record_object_relation_history(
    state: &mut MemoryState,
    value: &StorageObjectRelation,
    operation: StorageHistoryOperation,
    context: &EventContext,
) -> Result<(), StorageError> {
    record_relation_history(
        state,
        StorageBackupHistorySection::ObjectRelationHistory,
        resources::objectrelations(value)?,
        operation,
        context,
    )
}

fn record_relation_history(
    state: &mut MemoryState,
    section: StorageBackupHistorySection,
    snapshot: StorageBackupRow,
    operation: StorageHistoryOperation,
    context: &EventContext,
) -> Result<(), StorageError> {
    let now = Utc::now();
    let rows = state.relation_history.entry(section).or_default();
    for previous in rows.iter_mut() {
        if previous.get("id") == snapshot.get("id")
            && previous.get("valid_to").is_some_and(Value::is_null)
        {
            let mut fields = previous.fields().clone();
            fields.insert("valid_to".to_string(), json!(now));
            *previous = row(Value::Object(fields))?;
        }
    }
    let mut fields = snapshot.fields().clone();
    fields.extend(json!({"history_entry_id": state.next_history_id, "operation": operation.as_str(), "valid_from": now,
        "valid_to": (operation == StorageHistoryOperation::Delete).then_some(now), "actor_kind": context.actor_kind().as_str(),
        "actor_principal_id": context.actor_user_id().map(PrincipalId::id), "initiator_principal_id": context.initiator_user_id().map(PrincipalId::id), "task_id": context.task_id().map(TaskId::id)
    }).as_object().expect("literal object").clone());
    rows.push(row(Value::Object(fields))?);
    state.next_history_id = state
        .next_history_id
        .checked_add(1)
        .ok_or_else(|| invalid("history sequence"))?;
    Ok(())
}

pub(super) fn store_remote_call_result(
    state: &mut MemoryState,
    task_id: TaskId,
    artifact: StorageRemoteCallTaskArtifact,
    created_at: DateTime<Utc>,
) -> Result<(), StorageError> {
    let (target, response, outcome) = artifact.into_parts();
    let target = target.into_parts();
    let (response_status, response_headers, response_body_preview) = response.into_parts();
    let (duration_ms, success, error) = outcome.into_parts();
    let id = state
        .remote_call_results
        .iter()
        .filter_map(|r| r.get("id").and_then(Value::as_i64))
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| invalid("remote result sequence"))?;
    state.remote_call_results.push(row(json!({
        "id": id, "task_id": task_id.id(), "target_id": target.target_id().map(RemoteTargetId::id),
        "subject_type": target.subject_type().as_str(), "subject_id": target.subject_id().id(),
        "method": target.method().map_or("unknown", |method| method.as_str()), "rendered_url": target.rendered_url(),
        "response_status": response_status, "response_headers": response_headers, "response_body_preview": response_body_preview,
        "duration_ms": duration_ms, "success": success, "error": error, "created_at": created_at
    }))?);
    Ok(())
}
