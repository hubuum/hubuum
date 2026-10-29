//! Lease-fenced continuations retain admission and input accounting while
//! releasing execution capacity. Wake tokens are consumed under the state lock.

use super::*;

impl MemoryStorage {
    pub(super) async fn enter_wait(
        &self,
        request: StorageTaskWaitRequest,
    ) -> Result<StorageTaskWait, StorageError> {
        let (lease, checkpoint) = request.into_parts();
        let mut guard = self.state.write().await;
        let task = guard
            .tasks
            .get(&lease.task_id().id())
            .filter(|task| task.status.is_active() && task.lease_matches(&lease))
            .ok_or_else(invalid_task_lease)?;
        if !matches!(task.kind, StorageTaskKind::Export | StorageTaskKind::Backup)
            || task.control.phase() != StorageTaskExecutionPhase::Uncommitted
        {
            return Err(StorageError::invalid_input(
                "Only uncommitted export and backup tasks may wait",
            ));
        }
        let now = Utc::now();
        if let Some(reason) = task.control.stop_reason(now) {
            return Err(StorageError::task_stopped(reason));
        }
        if task.control.deadline().is_none() {
            return Err(StorageError::conflict(
                "Task execution requires a pinned deadline before waiting",
            ));
        }
        let event = StorageTaskEventInput::new(
            "waiting",
            "Task checkpoint persisted; waiting for continuation",
        )
        .with_data(Some(serde_json::json!({
            "checkpoint_format": checkpoint.format(),
            "checkpoint_version": checkpoint.version(),
            "checkpoint_bytes": checkpoint.byte_size(),
        })));
        let token = StorageTaskWaitToken::try_new(Uuid::new_v4().to_string())
            .map_err(invalid_contract_value)?;
        // Capacity validation or event construction must not partially release
        // the original lease when the checkpoint cannot be committed.
        let mut state = guard.clone();
        let task = state
            .tasks
            .get_mut(&lease.task_id().id())
            .expect("validated task");
        task.status = StorageTaskStatus::Waiting;
        task.checkpoint = Some(checkpoint);
        task.wait_token = Some(token.clone());
        task.claim_token = None;
        task.lease_expires_at = None;
        task.updated_at = now;
        let waiting =
            StorageTaskWait::try_new(task.projection()?, token).map_err(invalid_contract_value)?;
        state.account_task_checkpoint(lease.task_id())?;
        state.append_task_event_record(lease.task_id(), event)?;
        *guard = state;
        Ok(waiting)
    }

    pub(super) async fn resume_wait(
        &self,
        request: StorageTaskResumeRequest,
    ) -> Result<StorageTaskResumeOutcome, StorageError> {
        let mut guard = self.state.write().await;
        let Some(task) = guard.tasks.get(&request.task_id().id()).filter(|task| {
            task.status == StorageTaskStatus::Waiting
                && task
                    .wait_token
                    .as_ref()
                    .is_some_and(|token| token.adapter_value() == request.token().adapter_value())
        }) else {
            return Ok(StorageTaskResumeOutcome::Unchanged);
        };
        let now = Utc::now();
        if task.control.stop_reason(now).is_some() {
            let mut state = guard.clone();
            let task = state.finish_stopped_task(request.task_id(), now, None)?;
            state.task_capacity = state.task_accounting_snapshot()?;
            *guard = state;
            return Ok(StorageTaskResumeOutcome::Stopped(task));
        }
        if guard.maintenance_state != MaintenanceState::Normal {
            return Err(StorageError::unavailable(
                "Task resumption is paused during maintenance",
            ));
        }
        let mut state = guard.clone();
        let task = state
            .tasks
            .get_mut(&request.task_id().id())
            .expect("matched wait");
        task.status = StorageTaskStatus::Queued;
        task.wait_token = None;
        task.updated_at = now;
        let task = task.projection()?;
        state.task_capacity = state.task_accounting_snapshot()?;
        state.append_task_event_record(
            request.task_id(),
            StorageTaskEventInput::new("queued", "Task resumed from durable checkpoint"),
        )?;
        *guard = state;
        Ok(StorageTaskResumeOutcome::Queued(task))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hubuum_storage_core::{
        TaskAdmissionPolicy, TaskByteLimit, TaskCapacityLimit, TaskScopeCapacity,
    };
    use rstest::rstest;

    fn run(future: impl Future<Output = ()>) {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(future);
    }

    fn checkpoint() -> StorageTaskCheckpoint {
        StorageTaskCheckpoint::try_new("test.export", 1, serde_json::json!({"after": 7})).unwrap()
    }

    fn lease_duration() -> StorageTaskLeaseDuration {
        StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap()
    }

    async fn claimed(
        storage: &MemoryStorage,
        kind: StorageTaskKind,
    ) -> (StorageTask, StorageTaskLease) {
        storage
            .create_task(
                StorageTaskCreateRequest::builder(
                    kind,
                    PrincipalId::new(1).unwrap(),
                    serde_json::json!({}),
                    10,
                )
                .try_build()
                .unwrap(),
            )
            .await
            .unwrap();
        let (_, lease) = storage
            .claim_next_task(lease_duration())
            .await
            .unwrap()
            .unwrap()
            .into_parts();
        let mut state = storage.state.write().await;
        let task = state.tasks.get_mut(&lease.task_id().id()).unwrap();
        task.control.admit_deadline(Utc::now() + Duration::hours(1));
        task.progress = StorageTaskProgress::try_new(10, 3, 3, 0).unwrap();
        (task.projection().unwrap(), lease)
    }

    async fn waiting(storage: &MemoryStorage) -> StorageTaskWait {
        let (_, lease) = claimed(storage, StorageTaskKind::Export).await;
        storage
            .wait_task(StorageTaskWaitRequest::new(lease, checkpoint()))
            .await
            .unwrap()
    }

    fn resume(wait: &StorageTaskWait) -> StorageTaskResumeRequest {
        StorageTaskResumeRequest::new(wait.task().id(), wait.token().clone())
    }

    async fn expire_deadline(storage: &MemoryStorage, id: TaskId) {
        let mut state = storage.state.write().await;
        let task = state.tasks.get_mut(&id.id()).unwrap();
        task.control = StorageTaskControl::try_new(
            task.kind,
            None,
            Some(Utc::now() - Duration::seconds(1)),
            StorageTaskExecutionPhase::Uncommitted,
            None,
        )
        .unwrap();
    }

    #[rstest]
    #[case(StorageTaskKind::Export)]
    #[case(StorageTaskKind::Backup)]
    #[test]
    fn waiting_releases_execution_and_preserves_admission_and_work(#[case] kind: StorageTaskKind) {
        run(async {
            let storage = MemoryStorage::new();
            storage
                .initialize_task_policy(TaskAdmissionPolicy::default().with_deployment(
                    TaskScopeCapacity::new(
                        TaskCapacityLimit::new(2).unwrap(),
                        TaskCapacityLimit::new(1).unwrap(),
                        TaskByteLimit::new(100_000).unwrap(),
                    ),
                ))
                .await
                .unwrap();
            let (_, lease) = claimed(&storage, kind).await;
            let before = storage.task_capacity_snapshot().await.unwrap().ordinary();
            let checkpoint = checkpoint();
            let bytes = checkpoint.byte_size();
            storage
                .wait_task(StorageTaskWaitRequest::new(lease, checkpoint))
                .await
                .unwrap();
            let after = storage.task_capacity_snapshot().await.unwrap().ordinary();
            assert_eq!(
                (
                    after.outstanding(),
                    after.executing(),
                    after.resident_bytes(),
                    after.cumulative_input_bytes(),
                    after.cumulative_output_bytes(),
                    after.cumulative_items()
                ),
                (
                    before.outstanding(),
                    0,
                    before.resident_bytes() + bytes,
                    before.cumulative_input_bytes(),
                    before.cumulative_output_bytes(),
                    before.cumulative_items()
                )
            );
            let (other, _) = claimed(&storage, StorageTaskKind::Export).await;
            assert!(other.status().is_active());
        });
    }

    #[test]
    fn checkpoint_capacity_rejection_keeps_the_original_claim_intact() {
        run(async {
            let storage = MemoryStorage::new();
            storage
                .initialize_task_policy(TaskAdmissionPolicy::default().with_deployment(
                    TaskScopeCapacity::new(
                        TaskCapacityLimit::new(2).unwrap(),
                        TaskCapacityLimit::new(1).unwrap(),
                        TaskByteLimit::new(2).unwrap(),
                    ),
                ))
                .await
                .unwrap();
            let (original, lease) = claimed(&storage, StorageTaskKind::Export).await;
            let before = storage.task_capacity_snapshot().await.unwrap().ordinary();
            let events = storage.state.read().await.task_events.clone();
            let error = storage
                .wait_task(StorageTaskWaitRequest::new(lease.clone(), checkpoint()))
                .await
                .unwrap_err();
            assert_eq!(
                error.task_capacity_rejection(),
                Some(TaskCapacityRejection::ResidentBytes)
            );
            let state = storage.state.read().await;
            assert_eq!(
                state.tasks[&original.id().id()].projection().unwrap(),
                original
            );
            assert!(state.tasks[&original.id().id()].lease_matches(&lease));
            assert!(state.task_events == events, "failed wait appended an event");
            drop(state);
            assert_eq!(
                storage.task_capacity_snapshot().await.unwrap().ordinary(),
                before
            );
        });
    }

    #[test]
    fn concurrent_duplicate_wakes_enqueue_exactly_once() {
        run(async {
            let storage = MemoryStorage::new();
            let wait = waiting(&storage).await;
            let events_before =
                storage.state.read().await.task_events[&wait.task().id().id()].len();
            let first_storage = storage.clone();
            let first_request = resume(&wait);
            let first =
                tokio::spawn(
                    async move { first_storage.resume_task(first_request).await.unwrap() },
                );
            let second_storage = storage.clone();
            let second_request = resume(&wait);
            let second =
                tokio::spawn(
                    async move { second_storage.resume_task(second_request).await.unwrap() },
                );
            let outcomes = [first.await.unwrap(), second.await.unwrap()];
            assert_eq!(
                outcomes
                    .iter()
                    .filter(|value| matches!(value, StorageTaskResumeOutcome::Queued(_)))
                    .count(),
                1
            );
            assert_eq!(
                outcomes
                    .iter()
                    .filter(|value| matches!(value, StorageTaskResumeOutcome::Unchanged))
                    .count(),
                1
            );
            assert_eq!(
                storage.state.read().await.task_events[&wait.task().id().id()].len(),
                events_before + 1
            );
        });
    }

    #[test]
    fn resumed_claim_retains_checkpoint_deadline_and_monotonic_work() {
        run(async {
            let storage = MemoryStorage::new();
            let (original, old_lease) = claimed(&storage, StorageTaskKind::Export).await;
            let wait = storage
                .wait_task(StorageTaskWaitRequest::new(old_lease.clone(), checkpoint()))
                .await
                .unwrap();
            let before = storage.task_capacity_snapshot().await.unwrap().ordinary();
            storage.resume_task(resume(&wait)).await.unwrap();
            let (resumed, _) = storage
                .claim_next_task(lease_duration())
                .await
                .unwrap()
                .unwrap()
                .into_parts();
            assert_eq!(resumed.checkpoint(), wait.task().checkpoint());
            assert_eq!(resumed.control(), original.control());
            assert_eq!(resumed.started_at(), original.started_at());
            assert_eq!(resumed.progress(), original.progress());
            assert_eq!(resumed.request_payload(), original.request_payload());
            let after = storage.task_capacity_snapshot().await.unwrap().ordinary();
            assert_eq!(
                after.cumulative_input_bytes(),
                before.cumulative_input_bytes() + before.resident_bytes()
            );
            assert!(
                !storage
                    .renew_task_lease(old_lease, lease_duration())
                    .await
                    .unwrap()
            );
        });
    }

    #[test]
    fn previous_wait_token_cannot_wake_a_later_wait() {
        run(async {
            let storage = MemoryStorage::new();
            let first = waiting(&storage).await;
            storage.resume_task(resume(&first)).await.unwrap();
            let (_, lease) = storage
                .claim_next_task(lease_duration())
                .await
                .unwrap()
                .unwrap()
                .into_parts();
            let second = storage
                .wait_task(StorageTaskWaitRequest::new(lease, checkpoint()))
                .await
                .unwrap();
            assert!(matches!(
                storage.resume_task(resume(&first)).await.unwrap(),
                StorageTaskResumeOutcome::Unchanged
            ));
            assert_eq!(
                storage.state.read().await.tasks[&second.task().id().id()].status,
                StorageTaskStatus::Waiting
            );
        });
    }

    #[rstest]
    #[case::requested("requested")]
    #[case::principal_disabled("principal_disabled")]
    #[case::deadline_sweep("deadline_sweep")]
    #[case::deadline_wake("deadline_wake")]
    #[test]
    fn stopped_waits_release_resident_capacity_and_reject_late_wakes(#[case] stop: &str) {
        run(async {
            let storage = MemoryStorage::new();
            let wait = waiting(&storage).await;
            match stop {
                "requested" => {
                    let outcome = storage
                        .request_task_cancellation(StorageTaskCancellationRequest::new(
                            wait.task(),
                            EventContext::system(),
                        ))
                        .await
                        .unwrap();
                    assert_eq!(
                        outcome.change(),
                        StorageTaskCancellationChange::CancelledWaiting
                    );
                }
                "principal_disabled" => {
                    storage
                        .state
                        .write()
                        .await
                        .cancel_queued_tasks_for_principal(PrincipalId::new(1).unwrap())
                        .unwrap();
                }
                "deadline_sweep" => {
                    expire_deadline(&storage, wait.task().id()).await;
                    assert_eq!(
                        storage.recover_expired_task_leases(10).await.unwrap().len(),
                        1
                    );
                }
                "deadline_wake" => {
                    expire_deadline(&storage, wait.task().id()).await;
                    assert!(matches!(
                        storage.resume_task(resume(&wait)).await.unwrap(),
                        StorageTaskResumeOutcome::Stopped(_)
                    ));
                }
                _ => unreachable!(),
            }
            let task = storage.state.read().await.tasks[&wait.task().id().id()].clone();
            assert_eq!(task.status, StorageTaskStatus::Cancelled);
            assert!(task.checkpoint.is_none() && task.wait_token.is_none());
            let usage = storage.task_capacity_snapshot().await.unwrap().ordinary();
            assert_eq!(
                (
                    usage.outstanding(),
                    usage.executing(),
                    usage.resident_bytes()
                ),
                (0, 0, 0)
            );
            assert!(matches!(
                storage.resume_task(resume(&wait)).await.unwrap(),
                StorageTaskResumeOutcome::Unchanged
            ));
        });
    }

    #[test]
    fn waiting_is_not_lease_recovered_before_its_deadline() {
        run(async {
            let storage = MemoryStorage::new();
            let wait = waiting(&storage).await;
            assert!(
                storage
                    .recover_expired_task_leases(10)
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(
                storage
                    .claim_next_task(lease_duration())
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                storage.state.read().await.tasks[&wait.task().id().id()].status,
                StorageTaskStatus::Waiting
            );
        });
    }

    #[test]
    fn waiting_is_reported_separately_from_queue_and_worker_activity() {
        run(async {
            let storage = MemoryStorage::new();
            waiting(&storage).await;
            let queue = storage.get_task_queue_snapshot().await.unwrap();
            assert_eq!(
                (
                    queue.statuses().total(),
                    queue.statuses().waiting(),
                    queue.statuses().queued(),
                    queue.statuses().validating(),
                    queue.statuses().running()
                ),
                (1, 1, 0, 0, 0)
            );
            assert_eq!(
                (queue.oldest_queued_at(), queue.oldest_active_at()),
                (None, None)
            );
            let metrics = storage.get_task_metrics_snapshot().await.unwrap();
            assert_eq!(
                metrics.counts(),
                &[StorageTaskGaugeCount::try_new(
                    StorageTaskKind::Export,
                    StorageTaskStatus::Waiting,
                    1,
                )
                .unwrap()]
            );
            assert!(metrics.ages().iter().all(|age| age.oldest_queued_at().is_none() && age.oldest_active_at().is_none()));
        });
    }

    #[test]
    fn expired_resumed_claim_removes_the_checkpoint_without_replaying() {
        run(async {
            let storage = MemoryStorage::new();
            let wait = waiting(&storage).await;
            storage.resume_task(resume(&wait)).await.unwrap();
            let (_, lease) = storage
                .claim_next_task(lease_duration())
                .await
                .unwrap()
                .unwrap()
                .into_parts();
            storage
                .state
                .write()
                .await
                .tasks
                .get_mut(&lease.task_id().id())
                .unwrap()
                .lease_expires_at = Some(Utc::now() - Duration::seconds(1));
            let recovered = storage.recover_expired_task_leases(1).await.unwrap();
            assert_eq!(recovered.len(), 1);
            assert_eq!(recovered[0].status(), StorageTaskStatus::Failed);
            assert!(recovered[0].checkpoint().is_none());
            assert!(
                storage
                    .claim_next_task(lease_duration())
                    .await
                    .unwrap()
                    .is_none()
            );
        });
    }

    #[test]
    fn exhausted_resumed_attempt_does_not_charge_or_claim() {
        run(async {
            let storage = MemoryStorage::new();
            let wait = waiting(&storage).await;
            storage.resume_task(resume(&wait)).await.unwrap();
            storage
                .state
                .write()
                .await
                .tasks
                .get_mut(&wait.task().id().id())
                .unwrap()
                .attempt_count = i32::MAX;
            let before = storage.task_capacity_snapshot().await.unwrap().ordinary();
            assert!(storage.claim_next_task(lease_duration()).await.is_err());
            let after = storage.task_capacity_snapshot().await.unwrap().ordinary();
            assert_eq!(after, before);
            assert_eq!(
                storage.state.read().await.tasks[&wait.task().id().id()].status,
                StorageTaskStatus::Queued
            );
        });
    }

    #[rstest]
    #[case::unadmitted("unadmitted")]
    #[case::cancelled("cancelled")]
    #[case::expired_deadline("expired_deadline")]
    #[case::expired_lease("expired_lease")]
    #[case::import("import")]
    #[case::remote_call("remote_call")]
    #[test]
    fn wait_rejects_invalid_execution_without_releasing_the_claim(#[case] invalid: &str) {
        run(async {
            let storage = MemoryStorage::new();
            let kind = match invalid {
                "import" => StorageTaskKind::Import,
                "remote_call" => StorageTaskKind::RemoteCall,
                _ => StorageTaskKind::Export,
            };
            let (_, lease) = claimed(&storage, kind).await;
            {
                let mut state = storage.state.write().await;
                let task = state.tasks.get_mut(&lease.task_id().id()).unwrap();
                match invalid {
                    "unadmitted" => task.control = StorageTaskControl::new(kind),
                    "cancelled" => task
                        .control
                        .request_cancellation(StorageTaskCancellation::new(Utc::now(), None, None)),
                    "expired_lease" => {
                        task.lease_expires_at = Some(Utc::now() - Duration::seconds(1))
                    }
                    _ => {}
                }
            }
            if invalid == "expired_deadline" {
                expire_deadline(&storage, lease.task_id()).await;
            }
            let original = storage.state.read().await.tasks[&lease.task_id().id()]
                .projection()
                .unwrap();
            assert!(
                storage
                    .wait_task(StorageTaskWaitRequest::new(lease, checkpoint()))
                    .await
                    .is_err()
            );
            assert_eq!(
                storage.state.read().await.tasks[&original.id().id()]
                    .projection()
                    .unwrap(),
                original
            );
        });
    }

    #[test]
    fn maintenance_preserves_wait_token_but_allows_deadline_finalization() {
        run(async {
            let storage = MemoryStorage::new();
            let (task, lease) = claimed(&storage, StorageTaskKind::Export).await;
            storage.state.write().await.maintenance_state = MaintenanceState::Draining;
            let wait = storage
                .wait_task(StorageTaskWaitRequest::new(lease, checkpoint()))
                .await
                .unwrap();
            assert_eq!(
                storage.resume_task(resume(&wait)).await.unwrap_err().kind(),
                StorageErrorKind::Unavailable
            );
            expire_deadline(&storage, task.id()).await;
            assert!(matches!(
                storage.resume_task(resume(&wait)).await.unwrap(),
                StorageTaskResumeOutcome::Stopped(_)
            ));
        });
    }

    #[test]
    fn restore_discards_waits_and_old_tokens_cannot_wake_reused_task_ids() {
        run(async {
            let storage = MemoryStorage::new();
            let first = waiting(&storage).await;
            let snapshot = storage
                .capture_backup_replay(true, StorageBackupBudget::new(1_000_000, 10_000).unwrap())
                .await
                .unwrap()
                .materialize()
                .unwrap();
            let capacity = storage.state.read().await.task_capacity.clone();
            let replacement = crate::backup::restore(snapshot, capacity).unwrap();
            *storage.state.write().await = replacement;
            let second = waiting(&storage).await;
            assert_eq!(first.task().id(), second.task().id());
            assert!(matches!(
                storage.resume_task(resume(&first)).await.unwrap(),
                StorageTaskResumeOutcome::Unchanged
            ));
            assert!(matches!(
                storage.resume_task(resume(&second)).await.unwrap(),
                StorageTaskResumeOutcome::Queued(_)
            ));
        });
    }
}
