//! Both initiation paths share the authority transaction and capacity accounting.
use super::*;

pub(crate) async fn submit(
    storage: &MemoryStorage,
    request: HistoryExpiryTaskSubmission,
) -> Result<HistoryExpiryTaskAdmission, StorageError> {
    let application = current_scope(storage)?;
    let mut guard = storage.state.write_history_control(&application).await?;
    let mut state = guard.clone();
    let snapshot = state.history_lifecycle_snapshot()?;
    request
        .validate(&snapshot)
        .map_err(invalid_contract_value)?;
    if state.maintenance_state != MaintenanceState::Normal {
        return Err(StorageError::unavailable(
            "History task admission is paused during maintenance",
        ));
    }
    state.ensure_task_execution_allowed()?;
    if !state.history_policy_scope_known(request.payload().scan().key().scope())? {
        return Err(StorageError::not_found(
            "History policy scope was not found",
        ));
    }
    if let Some(submitter) = request.submitter() {
        if !state.principals.contains_key(&submitter.id()) {
            return Err(StorageError::not_found(
                "History initiating principal was deleted",
            ));
        }
        let evidence = state
            .logical_identities
            .record(StorageIdentityTarget::Principal(submitter))?;
        request
            .validate_principal(&evidence)
            .map_err(invalid_contract_value)?;
    }
    let key = request.payload().scan().key();
    let result = if let Some(task) = active_on(&mut state, key)? {
        HistoryExpiryTaskAdmission::new(task, false)
    } else {
        let mut recovered = None;
        for intent in snapshot.control().authority().expiry().intents() {
            if intent.manifest().key() == key {
                match super::recovery::recover_on(&mut state, intent.manifest().identity())? {
                    Some(task) if !task.status().is_terminal() => {
                        recovered = Some(task);
                        break;
                    }
                    _ => {}
                }
            }
        }
        match recovered {
            Some(task) => HistoryExpiryTaskAdmission::new(task, true),
            None => HistoryExpiryTaskAdmission::new(
                super::recovery::create_on(
                    &mut state,
                    request.payload(),
                    request.submitter(),
                    0,
                    0,
                )?,
                true,
            ),
        }
    };
    *guard = state;
    Ok(result)
}

pub(super) fn active_on(
    state: &mut MemoryState,
    key: HistoryPolicyKey,
) -> Result<Option<StorageTask>, StorageError> {
    let key = serde_json::to_value(key)
        .map_err(|_| StorageError::internal("Cannot encode history scope"))?;
    let ids: Vec<_> = state
        .tasks
        .values()
        .filter(|task| task.kind == StorageTaskKind::HistoryExpiry && !task.status.is_terminal())
        .map(|task| task.id)
        .collect();
    for id in ids {
        let task = &state.tasks[&id.id()];
        let stored_key = task
            .metadata
            .as_ref()
            .and_then(|metadata| match metadata.details() {
                TaskMetadataDetails::HistoryExpiry {
                    admission_scope: Some(scope),
                    ..
                } => serde_json::to_value(scope).ok(),
                _ => None,
            })
            .or_else(|| {
                task.request_payload
                    .as_ref()?
                    .get("scan")?
                    .get("key")
                    .cloned()
            });
        if stored_key.as_ref() == Some(&key) {
            let stored = task.projection()?;
            let payload = HistoryExpiryTaskPayload::decode(
                stored
                    .request_payload()
                    .ok_or_else(|| StorageError::conflict("History task payload is absent"))?,
            )
            .map_err(invalid_contract_value)?;
            if payload.application() != state.application_control.fence() {
                return Err(StorageError::conflict(
                    "Active history task belongs to another application image",
                ));
            }
            if stored.control().stop_reason(Utc::now()).is_some() {
                state.finish_stopped_task(id, Utc::now(), None)?;
                continue;
            }
            let evidence = state
                .logical_identities
                .record(StorageIdentityTarget::Task(task.id))?;
            HistoryExpiryTaskCheckpoint::from_task(&stored, &evidence)
                .map_err(invalid_contract_value)?;
            return Ok(Some(stored));
        }
    }
    Ok(None)
}
