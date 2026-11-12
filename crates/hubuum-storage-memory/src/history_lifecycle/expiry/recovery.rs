//! Atomic adoption and bounded wakeup of retained obligations.
use super::*;

pub(crate) async fn recover_task(
    storage: &MemoryStorage,
    batch: Uuid,
) -> Result<Option<StorageTask>, StorageError> {
    let application = current_scope(storage)?;
    let mut guard = storage.state.write_history_control(&application).await?;
    let snapshot = guard.history_lifecycle_snapshot()?;
    let intent = snapshot
        .control()
        .authority()
        .expiry()
        .intent(batch)
        .ok_or_else(|| StorageError::not_found("Retained history batch was not found"))?;
    let payload = HistoryExpiryTaskPayload::resume_retained(intent.manifest())
        .map_err(invalid_contract_value)?;
    if payload.application().incarnation() != snapshot.fence().incarnation()
        || payload.application().revision() as u64 != snapshot.fence().deletion_revision().value()
    {
        return Err(StorageError::unavailable(
            "History recovery requires the original application image",
        ));
    }
    if guard.maintenance_state != MaintenanceState::Normal {
        return Err(StorageError::unavailable(
            "History task recovery is paused during maintenance",
        ));
    }
    guard.ensure_task_execution_allowed()?;
    let original = guard
        .tasks
        .values()
        .find(|task| {
            guard.task_identity(task.id).ok().is_some_and(|identity| {
                Uuid::from_bytes(*identity.as_bytes()) == intent.manifest().run()
            })
        })
        .map(|task| task.id);
    let cleanup = guard
        .tasks
        .values()
        .filter(|task| matches!(task.metadata.as_ref().map(StorageTaskMetadata::details),
            Some(TaskMetadataDetails::HistoryExpiry { recovery_batch: Some(owner_batch) }) if *owner_batch == batch))
        .map(|task| task.id)
        .max_by_key(|id| id.id());
    let mut state = guard.clone();
    for id in original.into_iter().chain(cleanup) {
        let task = state.tasks.get(&id.id()).expect("selected owner");
        if task.kind != StorageTaskKind::HistoryExpiry {
            return Err(StorageError::conflict(
                "Retained history task identity changed kind",
            ));
        }
        let stored = task.projection()?;
        if stored.status().is_terminal() && stored.status() != StorageTaskStatus::Succeeded {
            continue;
        }
        if stored.status() == StorageTaskStatus::Succeeded {
            if !intent.is_complete()
                || stored.progress().succeeded() as usize != intent.removals().len()
            {
                return Err(StorageError::conflict(
                    "Successful history task differs from retained receipts",
                ));
            }
            // Finalization validated protected completion before clearing the checkpoint.
            return Ok(Some(stored));
        }
        let evidence = state
            .logical_identities
            .record(StorageIdentityTarget::Task(id))?;
        let checkpoint = HistoryExpiryTaskCheckpoint::from_task(&stored, &evidence)
            .map_err(invalid_contract_value)?;
        if checkpoint.batch() != batch {
            return Err(StorageError::conflict("History task batch changed"));
        }
        checkpoint
            .retained_manifest(&snapshot)
            .map_err(invalid_contract_value)?;
        if stored.status() != StorageTaskStatus::Waiting {
            return Ok(Some(stored));
        }
        if stored.control().stop_reason(Utc::now()).is_none()
            && Utc::now() < stored.updated_at() + chrono::Duration::seconds(30)
        {
            return Ok(Some(stored));
        }
        match state.resume_history_wait_on(id)? {
            StorageTaskResumeOutcome::Queued(task) => {
                *guard = state;
                return Ok(Some(task));
            }
            StorageTaskResumeOutcome::Stopped(_) => {}
            StorageTaskResumeOutcome::Unchanged => unreachable!("locked waiting owner"),
        }
    }
    let id = TaskId::new(state.next_task_id)
        .map_err(|error| StorageError::internal(error.to_string()))?;
    state.next_task_id += 1;
    let now = Utc::now();
    let removed = intent.removals().len() as i32;
    let record = MemoryTaskRecord {
        checkpoint: None,
        wait_token: None,
        control: StorageTaskControl::new(StorageTaskKind::HistoryExpiry),
        id,
        kind: StorageTaskKind::HistoryExpiry,
        status: StorageTaskStatus::Queued,
        submitted_by: None,
        idempotency_key: None,
        request_hash: None,
        request_payload: Some(
            serde_json::to_value(payload)
                .map_err(|_| StorageError::internal("Cannot encode history recovery"))?,
        ),
        metadata: Some(
            StorageTaskMetadata::new(TaskMetadataDetails::HistoryExpiry {
                recovery_batch: Some(batch),
            })
            .map_err(invalid_contract_value)?,
        ),
        summary: None,
        progress: StorageTaskProgress::try_new(
            intent.manifest().members().len() as i32,
            removed,
            removed,
            0,
        )
        .map_err(invalid_contract_value)?,
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
        execution_fence: None,
        receipt_admission: None,
        retention_policy: None,
        details_expires_at: None,
        recovery_unavailable: false,
        invocation: None,
    };
    state.store_task(id.id(), record)?;
    let task = state
        .tasks
        .get(&id.id())
        .expect("new cleanup task")
        .projection()?;
    let evidence = state
        .logical_identities
        .record(StorageIdentityTarget::Task(id))?;
    let checkpoint =
        HistoryExpiryTaskCheckpoint::from_task(&task, &evidence).map_err(invalid_contract_value)?;
    checkpoint
        .retained_manifest(&snapshot)
        .map_err(invalid_contract_value)?;
    state.append_task_event_record(
        id,
        StorageTaskEventInput::new("queued", "Retained history batch adopted for cleanup")
            .with_data(Some(
                serde_json::json!({"batch": batch,"original_run":intent.manifest().run()}),
            )),
    )?;
    let result = state.tasks.get(&id.id()).expect("new task").projection()?;
    *guard = state;
    Ok(Some(result))
}
