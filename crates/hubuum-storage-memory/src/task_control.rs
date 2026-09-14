use super::*;

impl MemoryStorage {
    pub(super) async fn request_cancellation(
        &self,
        request: StorageTaskCancellationRequest,
    ) -> Result<StorageTaskCancellationOutcome, StorageError> {
        let mut state = self.state.write().await;
        let task = state
            .tasks
            .get_mut(&request.task_id().id())
            .filter(|task| task.deleted_at.is_none())
            .ok_or_else(|| StorageError::not_found("Task not found"))?;
        if task.kind != request.kind() || task.submitted_by != request.submitted_by() {
            return Err(StorageError::conflict(
                "Task ownership changed; authorize cancellation again",
            ));
        }
        if task.status.is_terminal() || task.control.cancellation().is_some() {
            return Ok(StorageTaskCancellationOutcome::new(
                task.projection()?,
                StorageTaskCancellationChange::Unchanged,
            ));
        }
        if request
            .required_status()
            .is_some_and(|status| status != task.status)
        {
            return Err(StorageError::conflict(
                "Task status no longer matches expected_status",
            ));
        }
        let now = Utc::now();
        task.control
            .request_cancellation(StorageTaskCancellation::new(
                now,
                request.event_context().actor_user_id(),
                request.cancellation_reason().cloned(),
            ));
        task.updated_at = now;
        if task.status == StorageTaskStatus::Queued {
            let task =
                state.finish_stopped_task(request.task_id(), now, Some(request.event_context()))?;
            return Ok(StorageTaskCancellationOutcome::new(
                task,
                StorageTaskCancellationChange::CancelledQueued,
            ));
        }
        let projection = task.projection()?;
        state.append_task_event_record_with_context(request.task_id(),
            StorageTaskEventInput::new("cancel_requested", "Task cancellation requested")
                .with_data(Some(serde_json::json!({"reason": "cancel_requested", "cancel_requested_by": request.event_context().actor_user_id()}))),
            Some(request.event_context()))?;
        Ok(StorageTaskCancellationOutcome::new(
            projection,
            StorageTaskCancellationChange::Requested,
        ))
    }

    pub(super) async fn admit_execution(
        &self,
        request: StorageTaskExecutionAdmission,
    ) -> Result<StorageTaskExecutionObservation, StorageError> {
        let (lease, limit) = request.into_parts();
        let mut state = self.state.write().await;
        let task = state
            .tasks
            .get_mut(&lease.task_id().id())
            .filter(|task| task.status.is_active() && task.lease_matches(&lease))
            .ok_or_else(invalid_task_lease)?;
        if task.control.deadline().is_none() {
            let duration = Duration::from_std(limit.duration())
                .map_err(|_| StorageError::invalid_input("Execution duration is out of range"))?;
            let started = task.started_at.unwrap_or_else(Utc::now);
            let deadline = started
                .checked_add_signed(duration)
                .ok_or_else(|| StorageError::invalid_input("Execution deadline is out of range"))?;
            task.control.admit_deadline(deadline);
        }
        Ok(StorageTaskExecutionObservation::new(
            task.control.clone(),
            Utc::now(),
        ))
    }

    pub(super) async fn poll_execution(
        &self,
        lease: StorageTaskLease,
    ) -> Result<StorageTaskExecutionObservation, StorageError> {
        let state = self.state.read().await;
        let task = state
            .tasks
            .get(&lease.task_id().id())
            .filter(|task| task.status.is_active() && task.lease_matches(&lease))
            .ok_or_else(invalid_task_lease)?;
        Ok(StorageTaskExecutionObservation::new(
            task.control.clone(),
            Utc::now(),
        ))
    }

    pub(super) async fn acknowledge_stop(
        &self,
        lease: StorageTaskLease,
    ) -> Result<StorageTask, StorageError> {
        let mut state = self.state.write().await;
        let task = state
            .tasks
            .get(&lease.task_id().id())
            .filter(|task| task.status.is_active() && task.lease_matches(&lease))
            .ok_or_else(invalid_task_lease)?;
        let now = Utc::now();
        if task.control.stop_reason(now).is_none() {
            return Err(StorageError::conflict(
                "Task has no cancellation request or expired deadline",
            ));
        }
        state.finish_stopped_task(lease.task_id(), now, None)
    }

    pub(super) async fn begin_dispatch(
        &self,
        request: StorageTaskRemoteDispatch,
    ) -> Result<(), StorageError> {
        let (lease, target) = request.into_parts();
        let mut state = self.state.write().await;
        let task = state
            .tasks
            .get(&lease.task_id().id())
            .filter(|task| task.status.is_active() && task.lease_matches(&lease))
            .ok_or_else(invalid_task_lease)?;
        if task.kind != StorageTaskKind::RemoteCall {
            return Err(StorageError::invalid_input(
                "Only remote-call tasks may dispatch HTTP requests",
            ));
        }
        let now = Utc::now();
        if let Some(reason) = task.control.stop_reason(now) {
            return Err(StorageError::task_stopped(reason));
        }
        let mut control = task.control.clone();
        control
            .record_remote_dispatch(now)
            .map_err(|error| StorageError::conflict(error.to_string()))?;
        let artifact = StorageRemoteCallTaskArtifact::new(
            target,
            StorageRemoteCallArtifactResponse::new(None, None, None),
            StorageRemoteCallArtifactOutcome::new(
                0,
                false,
                Some("Remote dispatch admitted; outcome is not yet known".to_string()),
            ),
        );
        crate::backup::store_remote_call_result(&mut state, lease.task_id(), artifact, now)?;
        if let Some(row) = state.remote_call_results.iter_mut().find(|row| {
            row.get("task_id").and_then(serde_json::Value::as_i64)
                == Some(i64::from(lease.task_id().id()))
        }) {
            let mut fields = row.fields().clone();
            fields.insert(
                "side_effect_state".to_string(),
                serde_json::json!("possibly_sent"),
            );
            *row = StorageBackupRow::try_from_value(serde_json::Value::Object(fields))
                .map_err(invalid_contract_value)?;
        }
        state
            .tasks
            .get_mut(&lease.task_id().id())
            .ok_or_else(invalid_task_lease)?
            .control = control;
        Ok(())
    }
}

impl MemoryState {
    pub(super) fn finish_stopped_task(
        &mut self,
        id: TaskId,
        now: DateTime<Utc>,
        context: Option<&EventContext>,
    ) -> Result<StorageTask, StorageError> {
        let task = self.tasks.get(&id.id()).ok_or_else(invalid_task_lease)?;
        let reason = task.control.stop_reason(now).ok_or_else(|| {
            StorageError::conflict("Task has no stop request or expired deadline")
        })?;
        let kind = task.kind;
        let counts = if kind == StorageTaskKind::Import {
            let results = self.import_task_results.get(&id.id());
            let processed = results.map_or(0, |results| {
                results
                    .iter()
                    .filter(|result| result.outcome() != "unattempted")
                    .count()
            });
            let failed = results.map_or(0, |results| {
                results
                    .iter()
                    .filter(|result| matches!(result.outcome(), "failed" | "stale_revision"))
                    .count()
            });
            StorageTaskResultCounts::try_new(
                i32::try_from(processed)
                    .map_err(|_| StorageError::internal("Import result count overflow"))?,
                i32::try_from(processed - failed)
                    .map_err(|_| StorageError::internal("Import result count overflow"))?,
                i32::try_from(failed)
                    .map_err(|_| StorageError::internal("Import result count overflow"))?,
            )
            .map_err(invalid_contract_value)?
        } else {
            StorageTaskResultCounts::try_new(
                task.progress.processed(),
                task.progress.succeeded(),
                task.progress.failed(),
            )
            .map_err(invalid_contract_value)?
        };
        let committed = kind == StorageTaskKind::Import
            && (matches!(
                task.control.phase(),
                StorageTaskExecutionPhase::ImportCommitted { .. }
            ) || (counts.processed() > 0 && counts.processed() == task.progress.total()));
        let status = if committed {
            if counts.failed() == 0 {
                StorageTaskStatus::Succeeded
            } else if counts.succeeded() > 0 {
                StorageTaskStatus::PartiallySucceeded
            } else {
                StorageTaskStatus::Failed
            }
        } else {
            StorageTaskStatus::Cancelled
        };
        let unattempted = task
            .progress
            .total()
            .saturating_sub(counts.processed())
            .max(0);
        let remote_possible = kind == StorageTaskKind::RemoteCall
            && (matches!(
                task.control.phase(),
                StorageTaskExecutionPhase::RemoteDispatched { .. }
            ) || (task.control.deadline().is_none()
                && (task.started_at.is_some() || task.attempt_count > 0)));
        let cancellation = task.control.cancellation().cloned();
        let summary = if committed {
            "Import effects committed before cancellation could stop execution".to_string()
        } else if kind == StorageTaskKind::Import {
            format!(
                "Task stopped: {}; {} committed/successful items, {} failed, {} unattempted",
                reason.as_str(),
                counts.succeeded(),
                counts.failed(),
                unattempted
            )
        } else if remote_possible {
            format!(
                "Task stopped: {}; remote side effects may have occurred; do not retry without reconciliation",
                reason.as_str()
            )
        } else {
            format!("Task stopped: {}", reason.as_str())
        };
        if status == StorageTaskStatus::Cancelled {
            self.export_outputs.remove(&id.id());
            self.export_output_ids.remove(&id.id());
            self.backup_outputs.remove(&id.id());
            if let Some(class_id) = self.computed_rebuild_tasks.remove(&id.id())
                && let Some(previous) = self.computation_states.get(&class_id.id()).cloned()
            {
                let state = StorageClassComputationState::try_new(
                    class_id,
                    previous.evaluation_revision(),
                    StorageComputationRebuildState::Failed {
                        last_error: summary.clone(),
                    },
                    previous.created_at(),
                    now,
                )
                .map_err(invalid_contract_value)?;
                self.computation_states.insert(class_id.id(), state);
            }
            if let Some(work) = self.schema_work.get_mut(&id.id()) {
                let epoch = self
                    .schema_epochs
                    .get(&work.target().class_id().id())
                    .copied()
                    .unwrap_or(0);
                work.finish(StorageSchemaWorkStatus::Cancelled, epoch);
            }
        }
        if kind == StorageTaskKind::Import && unattempted > 0 {
            let result_id = ImportTaskResultId::new(self.next_import_result_id)
                .map_err(|error| StorageError::internal(error.to_string()))?;
            self.next_import_result_id += 1;
            let result = StorageImportTaskResult::builder(
                result_id,
                id,
                "task",
                "not_attempted",
                "unattempted",
                now,
            )
            .details(Some(
                serde_json::json!({"count": unattempted, "reason": reason.as_str()}),
            ))
            .build();
            self.import_task_results
                .entry(id.id())
                .or_default()
                .push(result);
        }
        if remote_possible {
            for row in &mut self.remote_call_results {
                if row.get("task_id").and_then(serde_json::Value::as_i64)
                    == Some(i64::from(id.id()))
                {
                    let mut fields = row.fields().clone();
                    fields.insert("error".into(), serde_json::json!(summary));
                    fields.insert(
                        "side_effect_state".into(),
                        serde_json::json!(if fields
                            .get("response_status")
                            .is_some_and(|value| !value.is_null())
                        {
                            "response_received"
                        } else {
                            "possibly_sent"
                        }),
                    );
                    *row = StorageBackupRow::try_from_value(serde_json::Value::Object(fields))
                        .map_err(invalid_contract_value)?;
                }
            }
        }
        let task = self
            .tasks
            .get_mut(&id.id())
            .ok_or_else(invalid_task_lease)?;
        task.status = status;
        if status == StorageTaskStatus::Cancelled {
            task.control
                .acknowledge_stop(now)
                .map_err(invalid_contract_value)?;
        }
        task.summary = Some(summary.clone());
        task.progress = StorageTaskProgress::try_new(
            task.progress.total(),
            counts.processed(),
            counts.succeeded(),
            counts.failed(),
        )
        .map_err(invalid_contract_value)?;
        task.finished_at = Some(now);
        task.updated_at = now;
        task.request_payload = None;
        task.request_redacted_at = Some(now);
        task.claim_token = None;
        task.lease_expires_at = None;
        let projection = task.projection()?;
        self.append_task_event_record_with_context(id, StorageTaskEventInput::new(status.as_str(), summary).with_data(Some(serde_json::json!({
            "reason": reason.as_str(), "cancel_requested_by": cancellation.as_ref().and_then(StorageTaskCancellation::requested_by),
            "processed_items": counts.processed(), "success_items": counts.succeeded(), "failed_items": counts.failed(),
            "unattempted_items": unattempted, "remote_side_effects_possible": remote_possible, "effects_committed": committed,
        }))), context)?;
        Ok(projection)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(future: impl Future<Output = ()>) {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(future);
    }

    async fn queued(storage: &MemoryStorage) -> StorageTask {
        storage
            .create_task(
                StorageTaskCreateRequest::builder(
                    StorageTaskKind::Import,
                    PrincipalId::new(1).unwrap(),
                    serde_json::json!({}),
                    2,
                )
                .try_build(10)
                .unwrap(),
            )
            .await
            .unwrap()
    }

    #[test]
    fn queued_cancellation_prevents_claiming() {
        run(async {
            let storage = MemoryStorage::new();
            let task = queued(&storage).await;
            storage
                .request_task_cancellation(StorageTaskCancellationRequest::new(
                    &task,
                    EventContext::system(),
                ))
                .await
                .unwrap();
            let claim = storage
                .claim_next_task(StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap())
                .await
                .unwrap();
            assert!(claim.is_none());
        });
    }

    #[test]
    fn repeated_cancellation_does_not_emit_another_terminal_event() {
        run(async {
            let storage = MemoryStorage::new();
            let task = queued(&storage).await;
            let request = StorageTaskCancellationRequest::new(&task, EventContext::system());
            storage
                .request_task_cancellation(request.clone())
                .await
                .unwrap();
            let events = storage.state.read().await.task_events.clone();
            storage.request_task_cancellation(request).await.unwrap();
            assert!(storage.state.read().await.task_events == events);
        });
    }

    #[test]
    fn cancelled_import_snapshot_cannot_commit() {
        run(async {
            let storage = MemoryStorage::new();
            let task = queued(&storage).await;
            let claim = storage
                .claim_next_task(StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap())
                .await
                .unwrap()
                .unwrap();
            let (_, lease) = claim.into_parts();
            let mut staged = storage.state.read().await.clone();
            let generation = staged.generation.clone();
            staged.next_collection_id += 1;
            storage
                .request_task_cancellation(StorageTaskCancellationRequest::new(
                    &task,
                    EventContext::system(),
                ))
                .await
                .unwrap();
            let result = storage
                .state
                .commit_import(&generation, staged, &lease)
                .await;
            assert_eq!(result.unwrap_err().kind(), StorageErrorKind::TaskCancelled);
        });
    }
}
