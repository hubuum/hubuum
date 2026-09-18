use super::*;
use hubuum_storage_core::{
    StorageTask, StorageTaskCancellationChange, StorageTaskCancellationRequest,
    StorageTaskExecutionAdmission,
};
use hubuum_task_core::{TaskCancellationReason, TaskExecutionLimit, TaskStopReason};
use rstest::rstest;

struct Fixture {
    environment: BackendTestEnvironment,
    backend: StorageHandle,
    user: BackendUserFixture,
    task: StorageTask,
}
impl Fixture {
    async fn new(environment: BackendTestEnvironment, kind: StorageTaskKind) -> Self {
        Self::with_total(environment, kind, 1).await
    }
    async fn with_total(
        environment: BackendTestEnvironment,
        kind: StorageTaskKind,
        total: i32,
    ) -> Self {
        Self::with_metadata(environment, kind, total, None).await
    }
    async fn with_metadata(
        environment: BackendTestEnvironment,
        kind: StorageTaskKind,
        total: i32,
        metadata: Option<hubuum_storage_core::StorageTaskMetadata>,
    ) -> Self {
        let backend = environment.storage();
        let user = create_backend_user(&backend, &prefix("task_control_user")).await;
        let task = backend
            .create_task(
                StorageTaskCreateRequest::builder(
                    kind,
                    user.principal_id,
                    serde_json::json!({"private": "redact me"}),
                    total,
                )
                .metadata(metadata)
                .try_build(10)
                .unwrap(),
            )
            .await
            .unwrap();
        Self {
            environment,
            backend,
            user,
            task,
        }
    }
    fn request(&self) -> StorageTaskCancellationRequest {
        StorageTaskCancellationRequest::new(&self.task, EventContext::system()).reason(Some(
            TaskCancellationReason::new("withdraw this task").unwrap(),
        ))
    }
    async fn claim(&self) -> StorageTaskLease {
        self.claim_with_duration(60_000).await
    }
    async fn claim_with_duration(&self, millis: i64) -> StorageTaskLease {
        let duration = StorageTaskLeaseDuration::from_milliseconds(millis).unwrap();
        let claim = match &self.environment {
            BackendTestEnvironment::Postgres { pool } => {
                hubuum_storage_postgres::test_support::claim_task_by_id_with_lease(
                    pool,
                    self.task.id(),
                    duration,
                )
                .await
                .unwrap()
            }
            BackendTestEnvironment::Memory { .. } => self
                .backend
                .claim_next_task(duration)
                .await
                .unwrap()
                .unwrap(),
        };
        assert_eq!(claim.task().id(), self.task.id());
        claim.lease().clone()
    }
    async fn events(&self) -> Vec<String> {
        self.backend
            .list_task_events(StorageTaskChildListQuery::new(
                self.task.id(),
                QueryOptions::new(Vec::new(), Vec::new(), Some(100), None, true).unwrap(),
            ))
            .await
            .unwrap()
            .into_parts()
            .0
            .into_iter()
            .map(|event| event.event_type().to_owned())
            .collect()
    }
    async fn cleanup(self) {
        if let BackendTestEnvironment::Postgres { pool } = self.environment {
            hubuum_storage_postgres::test_support::delete_task(&pool, self.task.id())
                .await
                .unwrap();
        }
        delete_backend_user(&self.backend, self.user).await;
    }
}

#[rstest]
#[case(StorageTaskKind::Import)]
#[case(StorageTaskKind::Export)]
#[case(StorageTaskKind::Backup)]
#[case(StorageTaskKind::Reindex)]
#[case(StorageTaskKind::RemoteCall)]
#[case(StorageTaskKind::SchemaValidation)]
#[actix_web::test]
async fn queued_stop_is_terminal_redacted_and_idempotent(#[case] kind: StorageTaskKind) {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, kind).await;
        let stopped = fixture
            .backend
            .request_task_cancellation(fixture.request())
            .await
            .unwrap();
        assert_eq!(
            stopped.change(),
            StorageTaskCancellationChange::CancelledQueued
        );
        assert_eq!(stopped.task().status(), StorageTaskStatus::Cancelled);
        assert!(stopped.task().request_payload().is_none());
        assert!(stopped.task().lease_expires_at().is_none());
        assert_eq!(
            stopped.task().control().terminal_reason(),
            Some(TaskStopReason::Cancelled)
        );
        let repeated = fixture
            .backend
            .request_task_cancellation(
                fixture
                    .request()
                    .expected_status(Some(StorageTaskStatus::Queued)),
            )
            .await
            .unwrap();
        assert_eq!(repeated.change(), StorageTaskCancellationChange::Unchanged);
        assert_eq!(
            fixture
                .events()
                .await
                .iter()
                .filter(|event| event.as_str() == "cancelled")
                .count(),
            1
        );
        fixture.cleanup().await;
    }
}

#[actix_web::test]
async fn active_stop_is_durable_and_retains_lease_until_acknowledgement() {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Import).await;
        let lease = fixture.claim().await;
        let requested = fixture
            .backend
            .request_task_cancellation(fixture.request())
            .await
            .unwrap();
        assert_eq!(requested.change(), StorageTaskCancellationChange::Requested);
        assert!(requested.task().status().is_active());
        // A separately composed handle represents a different replica.
        let replica = fixture.environment.storage();
        assert_eq!(
            replica
                .poll_task_execution(lease.clone())
                .await
                .unwrap()
                .stop_reason(),
            Some(TaskStopReason::Cancelled)
        );
        assert!(
            replica
                .renew_task_lease(
                    lease.clone(),
                    StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap()
                )
                .await
                .unwrap()
        );
        let finished = replica.acknowledge_task_stop(lease.clone()).await.unwrap();
        assert_eq!(finished.status(), StorageTaskStatus::Cancelled);
        assert!(
            !replica
                .renew_task_lease(
                    lease,
                    StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap()
                )
                .await
                .unwrap()
        );
        fixture.cleanup().await;
    }
}

#[actix_web::test]
async fn queued_only_withdrawal_rejects_a_claimed_task_without_requesting_stop() {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Import).await;
        let lease = fixture.claim().await;
        let error = fixture
            .backend
            .request_task_cancellation(
                fixture
                    .request()
                    .expected_status(Some(StorageTaskStatus::Queued)),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::Conflict);
        assert!(
            fixture
                .backend
                .poll_task_execution(lease)
                .await
                .unwrap()
                .control()
                .cancellation()
                .is_none()
        );
        fixture.cleanup().await;
    }
}

#[actix_web::test]
async fn execution_deadline_is_pinned_independently_of_renewed_leases() {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Import).await;
        let lease = fixture.claim().await;
        let first = fixture
            .backend
            .admit_task_execution(StorageTaskExecutionAdmission::new(
                lease.clone(),
                TaskExecutionLimit::from_milliseconds(60_000).unwrap(),
            ))
            .await
            .unwrap();
        fixture
            .backend
            .renew_task_lease(
                lease.clone(),
                StorageTaskLeaseDuration::from_milliseconds(120_000).unwrap(),
            )
            .await
            .unwrap();
        let second = fixture
            .environment
            .storage()
            .admit_task_execution(StorageTaskExecutionAdmission::new(
                lease,
                TaskExecutionLimit::from_milliseconds(180_000).unwrap(),
            ))
            .await
            .unwrap();
        assert_eq!(first.control().deadline(), second.control().deadline());
        fixture.cleanup().await;
    }
}

#[actix_web::test]
async fn requested_cancellation_wins_later_success_finalization() {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Import).await;
        let lease = fixture.claim().await;
        fixture
            .backend
            .request_task_cancellation(fixture.request())
            .await
            .unwrap();
        let completed = fixture
            .backend
            .complete_task(StorageTaskCompletion::new(
                StorageTaskTerminalUpdate::new(
                    lease,
                    StorageTaskTerminalStatus::Succeeded,
                    StorageTaskResultCounts::try_new(1, 1, 0).unwrap(),
                ),
                StorageTaskEventInput::new("succeeded", "must not be recorded"),
                StorageTaskCompletionPayload::Import,
            ))
            .await
            .unwrap();
        assert_eq!(completed.status(), StorageTaskStatus::Cancelled);
        assert!(
            !fixture
                .events()
                .await
                .iter()
                .any(|event| event == "succeeded")
        );
        fixture.cleanup().await;
    }
}

#[actix_web::test]
async fn success_committed_before_cancel_remains_successful() {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Import).await;
        let lease = fixture.claim().await;
        fixture
            .backend
            .complete_task(StorageTaskCompletion::new(
                StorageTaskTerminalUpdate::new(
                    lease,
                    StorageTaskTerminalStatus::Succeeded,
                    StorageTaskResultCounts::try_new(1, 1, 0).unwrap(),
                ),
                StorageTaskEventInput::new("succeeded", "completed"),
                StorageTaskCompletionPayload::Import,
            ))
            .await
            .unwrap();
        let outcome = fixture
            .backend
            .request_task_cancellation(fixture.request())
            .await
            .unwrap();
        assert_eq!(outcome.task().status(), StorageTaskStatus::Succeeded);
        assert_eq!(outcome.change(), StorageTaskCancellationChange::Unchanged);
        assert!(
            !fixture
                .events()
                .await
                .iter()
                .any(|event| event == "cancelled")
        );
        fixture.cleanup().await;
    }
}

#[rstest]
#[case::cancel_requested(false)]
#[case::deadline_expired(true)]
#[actix_web::test]
async fn another_replica_recovers_a_stopped_worker_without_replaying(#[case] deadline: bool) {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Import).await;
        let lease = fixture.claim().await;
        let reason = if deadline {
            fixture
                .backend
                .admit_task_execution(StorageTaskExecutionAdmission::new(
                    lease.clone(),
                    TaskExecutionLimit::from_milliseconds(1).unwrap(),
                ))
                .await
                .unwrap();
            TaskStopReason::DeadlineExceeded
        } else {
            fixture
                .backend
                .request_task_cancellation(fixture.request())
                .await
                .unwrap();
            TaskStopReason::Cancelled
        };
        // The worker disappears after admission. A short final renewal lets
        // this test exercise the real expired-lease recovery path on both adapters.
        fixture
            .backend
            .renew_task_lease(
                lease.clone(),
                StorageTaskLeaseDuration::from_milliseconds(1).unwrap(),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let replica = fixture.environment.storage();
        replica.recover_expired_task_leases(100).await.unwrap();
        let (recovered, _) = replica
            .get_task_access(fixture.task.id())
            .await
            .unwrap()
            .into_parts();
        assert_eq!(recovered.status(), StorageTaskStatus::Cancelled);
        assert_eq!(recovered.control().terminal_reason(), Some(reason));
        assert!(
            replica.acknowledge_task_stop(lease).await.is_err(),
            "the abandoned worker is fenced"
        );
        assert_eq!(
            fixture
                .events()
                .await
                .iter()
                .filter(|event| *event == "cancelled")
                .count(),
            1
        );
        fixture.cleanup().await;
    }
}

#[rstest]
#[case::export(StorageTaskKind::Export)]
#[case::backup(StorageTaskKind::Backup)]
#[actix_web::test]
async fn stop_before_artifact_commit_never_publishes_output(#[case] kind: StorageTaskKind) {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, kind).await;
        let lease = fixture.claim().await;
        fixture
            .backend
            .request_task_cancellation(fixture.request())
            .await
            .unwrap();
        let finished = fixture
            .backend
            .complete_task(StorageTaskCompletion::new(
                StorageTaskTerminalUpdate::new(
                    lease,
                    StorageTaskTerminalStatus::Succeeded,
                    StorageTaskResultCounts::try_new(1, 1, 0).unwrap(),
                ),
                StorageTaskEventInput::new("succeeded", "must not publish"),
                compatibility_completion_payload(kind),
            ))
            .await
            .unwrap();
        assert_eq!(finished.status(), StorageTaskStatus::Cancelled);
        let available = match kind {
            StorageTaskKind::Export => matches!(
                fixture
                    .backend
                    .get_export_output(fixture.task.id())
                    .await
                    .unwrap(),
                StorageTaskOutputLookup::Available(_)
            ),
            StorageTaskKind::Backup => matches!(
                fixture
                    .backend
                    .get_backup_output(fixture.task.id())
                    .await
                    .unwrap(),
                StorageTaskOutputLookup::Available(_)
            ),
            _ => unreachable!(),
        };
        assert!(!available);
        fixture.cleanup().await;
    }
}

#[actix_web::test]
async fn best_effort_stop_preserves_commits_and_reports_unattempted_count() {
    use hubuum_storage_core::{FencedImportItem, FencedImportPlan};
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::with_total(environment, StorageTaskKind::Import, 3).await;
        let lease = fixture.claim().await;
        let name = prefix("cancel_best_effort_collection");
        let operation = crate::services::import_boundary::import_operation_to_storage(
            ApplicationImportOperation::CreateCollection(ImportCollectionInput {
                ref_: None,
                name: name.clone(),
                description: "survives cancellation".into(),
                parent_collection_ref: None,
                parent_collection_key: None,
                condition: None,
                timestamps: None,
            }),
        )
        .unwrap();
        let plan = FencedImportPlan::try_new(
            lease.clone(),
            vec![FencedImportItem::new(
                0,
                Some(operation),
                StorageImportResult::builder(
                    fixture.task.id(),
                    "collection",
                    "create",
                    "succeeded",
                )
                .identifier(Some(name.clone()))
                .build(),
            )],
        )
        .unwrap();
        fixture
            .backend
            .apply_claimed_import_best_effort(
                plan,
                crate::services::import_boundary::import_mode_to_storage(ImportMode::default()),
            )
            .await
            .unwrap();
        fixture
            .backend
            .request_task_cancellation(fixture.request())
            .await
            .unwrap();
        let stopped = fixture.backend.acknowledge_task_stop(lease).await.unwrap();
        assert_eq!(stopped.status(), StorageTaskStatus::Cancelled);
        assert_eq!(
            (
                stopped.progress().processed(),
                stopped.progress().succeeded()
            ),
            (1, 1)
        );
        let (rows, _) = fixture
            .backend
            .list_import_task_results(StorageTaskChildListQuery::new(
                fixture.task.id(),
                QueryOptions::new(Vec::new(), Vec::new(), Some(10), None, true).unwrap(),
            ))
            .await
            .unwrap()
            .into_parts();
        let unattempted = rows
            .iter()
            .find(|row| row.outcome() == "unattempted")
            .unwrap();
        assert_eq!(unattempted.details().unwrap()["count"], 2);
        let root = fixture.backend.get_import_root_collection().await.unwrap();
        let collection = fixture
            .backend
            .get_import_collection_child_by_name(root.id(), &name)
            .await
            .unwrap()
            .expect("committed collection remains");
        fixture
            .backend
            .collection_store()
            .delete_collection(collection.id(), &EventContext::system())
            .await
            .unwrap()
            .into_value();
        fixture.cleanup().await;
    }
}

#[rstest]
#[case::before_dispatch(false)]
#[case::after_dispatch(true)]
#[actix_web::test]
async fn remote_stop_preserves_dispatch_knowledge_and_forbids_replay(#[case] dispatched: bool) {
    use hubuum_storage_core::{
        StorageRemoteCallArtifactTarget, StorageRemoteTargetHttpMethod,
        StorageRemoteTargetSubjectType, StorageTaskExecutionPhase, StorageTaskRemoteDispatch,
    };
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::RemoteCall).await;
        let lease = fixture.claim().await;
        let dispatch = || {
            StorageTaskRemoteDispatch::new(
                lease.clone(),
                StorageRemoteCallArtifactTarget::new(
                    None,
                    StorageRemoteTargetSubjectType::Object,
                    ResourceId::new(1).unwrap(),
                    Some(StorageRemoteTargetHttpMethod::Post),
                    "https://example.invalid/dispatch",
                ),
            )
        };
        if dispatched {
            fixture
                .backend
                .begin_remote_dispatch(dispatch())
                .await
                .unwrap();
        }
        fixture
            .backend
            .request_task_cancellation(fixture.request())
            .await
            .unwrap();
        assert!(
            fixture
                .backend
                .begin_remote_dispatch(dispatch())
                .await
                .is_err()
        );
        let stopped = fixture.backend.acknowledge_task_stop(lease).await.unwrap();
        assert_eq!(
            matches!(
                stopped.control().phase(),
                StorageTaskExecutionPhase::RemoteDispatched { .. }
            ),
            dispatched
        );
        assert_eq!(stopped.status(), StorageTaskStatus::Cancelled);
        fixture.cleanup().await;
    }
}

#[actix_web::test]
async fn deleting_cancellation_actor_preserves_the_stop_request() {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Import).await;
        let actor = create_backend_user(&fixture.backend, &prefix("deleted_cancel_actor")).await;
        let lease = fixture.claim().await;
        fixture
            .backend
            .request_task_cancellation(StorageTaskCancellationRequest::new(
                &fixture.task,
                EventContext::user(actor.principal_id, None, None),
            ))
            .await
            .unwrap();
        delete_backend_user(&fixture.backend, actor).await;
        let observation = fixture
            .backend
            .poll_task_execution(lease.clone())
            .await
            .unwrap();
        assert_eq!(observation.stop_reason(), Some(TaskStopReason::Cancelled));
        assert!(
            observation
                .control()
                .cancellation()
                .unwrap()
                .requested_by()
                .is_none()
        );
        let stopped = fixture.backend.acknowledge_task_stop(lease).await.unwrap();
        assert_eq!(stopped.status(), StorageTaskStatus::Cancelled);
        fixture.cleanup().await;
    }
}

#[actix_web::test]
async fn cancelled_reindex_stays_incomplete_and_a_later_rebuild_can_finish() {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let backend = environment.storage();
        let user = create_backend_user(&backend, &prefix("cancel_reindex_owner")).await;
        let resources = create_backend_object_fixture(
            &backend,
            &prefix("cancel_reindex"),
            vec![serde_json::json!({"value":"one"})],
        )
        .await;
        let definition = StorageComputedFieldDefinitionInput::new(
            Definition::new(
                FieldKey::new("cancel_value").unwrap(),
                "Cancellation value",
                "rebuild fixture",
                Operation::FirstNonNull {
                    paths: vec![JsonPointer::new("/value").unwrap()],
                },
                ResultType::String,
                true,
            )
            .unwrap(),
        );
        backend
            .create_shared_computed_field(StorageSharedComputedFieldCreate::new(
                resources.class.id(),
                resources.collection.id(),
                user.principal_id,
                definition,
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value();
        let request = || {
            StorageComputedFieldRebuildRequest::new(
                resources.class.id(),
                resources.collection.id(),
                Some(user.principal_id),
            )
        };
        let state = backend
            .request_computed_field_rebuild(request())
            .await
            .unwrap();
        let task_id = state.active_task_id().unwrap();
        let (task, _) = backend.get_task_access(task_id).await.unwrap().into_parts();
        let fixture = Fixture {
            environment,
            backend: backend.clone(),
            user,
            task,
        };
        let lease = fixture.claim().await;
        backend
            .request_task_cancellation(fixture.request())
            .await
            .unwrap();
        backend.acknowledge_task_stop(lease).await.unwrap();
        let state = backend
            .get_computed_field_state(resources.class.id())
            .await
            .unwrap();
        assert_eq!(state.rebuild_status().as_str(), "failed");
        let next = backend
            .request_computed_field_rebuild(StorageComputedFieldRebuildRequest::new(
                resources.class.id(),
                resources.collection.id(),
                Some(fixture.user.principal_id),
            ))
            .await
            .unwrap()
            .active_task_id()
            .unwrap();
        assert_ne!(next, task_id);
        let duration = StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap();
        let next_lease = match &fixture.environment {
            BackendTestEnvironment::Postgres { pool } => {
                hubuum_storage_postgres::test_support::claim_task_by_id_with_lease(
                    pool, next, duration,
                )
                .await
                .unwrap()
                .lease()
                .clone()
            }
            BackendTestEnvironment::Memory { .. } => backend
                .claim_next_task(duration)
                .await
                .unwrap()
                .unwrap()
                .lease()
                .clone(),
        };
        backend
            .execute_computed_field_rebuild(next_lease)
            .await
            .unwrap();
        assert_eq!(
            backend
                .get_computed_field_state(resources.class.id())
                .await
                .unwrap()
                .rebuild_status()
                .as_str(),
            "ready"
        );
        if let BackendTestEnvironment::Postgres { pool } = &fixture.environment {
            hubuum_storage_postgres::test_support::delete_task(pool, next)
                .await
                .unwrap();
        }
        delete_backend_object_fixture(&backend, resources).await;
        fixture.cleanup().await;
    }
}

#[rstest]
#[case(StorageTaskKind::Import)]
#[case(StorageTaskKind::Export)]
#[case(StorageTaskKind::Backup)]
#[case(StorageTaskKind::Reindex)]
#[case(StorageTaskKind::RemoteCall)]
#[case(StorageTaskKind::SchemaValidation)]
#[actix_web::test]
async fn task_search_filters_before_counting(#[case] kind: StorageTaskKind) {
    use hubuum_storage_core::{StorageTaskSearch, TaskTimeRange};
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, kind).await;
        let query = |search| {
            StorageTaskListQuery::new(
                Some(fixture.user.principal_id),
                None,
                None,
                QueryOptions::new(Vec::new(), Vec::new(), Some(1), None, true).unwrap(),
            )
            .searching(search)
        };
        let search = StorageTaskSearch::default()
            .lifecycle(
                Some(vec![kind]),
                Some(vec![StorageTaskStatus::Queued, StorageTaskStatus::Running]),
                Some(false),
            )
            .unwrap()
            .time_ranges(
                TaskTimeRange::try_new(Some(fixture.task.created_at()), None).unwrap(),
                TaskTimeRange::default(),
                TaskTimeRange::default(),
            );
        let (rows, count) = fixture
            .backend
            .list_tasks(query(search))
            .await
            .unwrap()
            .into_parts();
        assert_eq!(
            (rows.iter().map(StorageTask::id).collect::<Vec<_>>(), count),
            (vec![fixture.task.id()], Some(1))
        );
        fixture.cleanup().await;
    }
}

#[rstest]
#[case::created_upper_bound("created")]
#[case::created_fractional_lower_bound("created_fractional")]
#[case::null_start("started")]
#[case::null_finish("finished")]
#[case::cancellation("cancel")]
#[case::terminal("terminal")]
#[case::trace("trace")]
#[case::terminal_reason("reason")]
#[actix_web::test]
async fn task_search_excludes_nonmatching_rows_and_counts(#[case] filter: &str) {
    use hubuum_storage_core::{StorageTaskSearch, TaskTimeRange};
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Export).await;
        let bound = || TaskTimeRange::try_new(None, Some(fixture.task.created_at())).unwrap();
        let search = match filter {
            "created_fractional" => StorageTaskSearch::default().time_ranges(
                TaskTimeRange::try_new(
                    Some(fixture.task.created_at() + chrono::Duration::nanoseconds(1)),
                    None,
                )
                .unwrap(),
                TaskTimeRange::default(),
                TaskTimeRange::default(),
            ),
            "created" => StorageTaskSearch::default().time_ranges(
                bound(),
                TaskTimeRange::default(),
                TaskTimeRange::default(),
            ),
            "started" => StorageTaskSearch::default().time_ranges(
                TaskTimeRange::default(),
                bound(),
                TaskTimeRange::default(),
            ),
            "finished" => StorageTaskSearch::default().time_ranges(
                TaskTimeRange::default(),
                TaskTimeRange::default(),
                bound(),
            ),
            "cancel" => StorageTaskSearch::default().operations(Some(true), None),
            "terminal" => StorageTaskSearch::default()
                .lifecycle(None, None, Some(true))
                .unwrap(),
            "trace" => StorageTaskSearch::default()
                .with_trace_id(Some("abcdef1234567890abcdef1234567890".into()))
                .unwrap(),
            "reason" => {
                StorageTaskSearch::default().operations(None, Some(TaskStopReason::Cancelled))
            }
            _ => unreachable!(),
        };
        let (rows, count) = fixture
            .backend
            .list_tasks(
                StorageTaskListQuery::new(
                    Some(fixture.user.principal_id),
                    None,
                    None,
                    QueryOptions::new(Vec::new(), Vec::new(), Some(1), None, true).unwrap(),
                )
                .searching(search),
            )
            .await
            .unwrap()
            .into_parts();
        assert_eq!((rows.len(), count), (0, Some(0)));
        fixture.cleanup().await;
    }
}

#[rstest]
#[case(StorageTaskKind::Import, serde_json::json!({"kind":"import","dry_run":false,"atomicity":"best_effort","collision_policy":"overwrite","permission_policy":"continue"}), "import")]
#[case(StorageTaskKind::Export, serde_json::json!({"kind":"export","scope_kind":"objects_in_class","target":{"type":"class","class_id":42},"template_id":7,"warning_count":0,"truncated":false,"output":{"state":"not_produced"}}), "export")]
#[case(StorageTaskKind::Backup, serde_json::json!({"kind":"backup","include_history":false,"output":{"state":"not_produced"}}), "backup")]
#[case(StorageTaskKind::Reindex, serde_json::json!({"kind":"reindex","class_id":42,"computation_revision":3}), "reindex")]
#[case(StorageTaskKind::SchemaValidation, serde_json::json!({"kind":"schema_validation","class_id":42,"schema_revision":3,"work_kind":"impact"}), "schema")]
#[case(StorageTaskKind::RemoteCall, serde_json::json!({"kind":"remote_call","remote_target_id":7,"target":{"type":"object","class_id":42,"object_id":8}}), "remote_object")]
#[case(StorageTaskKind::RemoteCall, serde_json::json!({"kind":"remote_call","target":{"type":"collection","collection_id":42}}), "collection")]
#[case(StorageTaskKind::RemoteCall, serde_json::json!({"kind":"remote_call","target":{"type":"class_relation","relation_id":42}}), "class_relation")]
#[case(StorageTaskKind::RemoteCall, serde_json::json!({"kind":"remote_call","target":{"type":"object_relation","relation_id":42}}), "object_relation")]
#[actix_web::test]
async fn discovery_survives_terminal_redaction_and_filters_before_counting(
    #[case] kind: StorageTaskKind,
    #[case] data: serde_json::Value,
    #[case] filter: &str,
) {
    use hubuum_domain::{
        ClassId, ClassRelationId, CollectionId, ExportTemplateId, ObjectId, ObjectRelationId,
        RemoteTargetId, SchemaRevision,
    };
    use hubuum_storage_core::{
        StorageTaskMetadata, StorageTaskSearch, TaskDiscoveryPredicate as P, TaskDiscoverySearch,
    };
    use hubuum_storage_core::{
        TaskExportScopeKind, TaskImportAtomicity, TaskImportCollisionPolicy,
        TaskImportPermissionPolicy, TaskOutputState, TaskRemoteSideEffectState,
    };
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let metadata =
            StorageTaskMetadata::from_persisted(kind, serde_json::json!({"version":1,"data":data}))
                .unwrap();
        let fixture = Fixture::with_metadata(environment, kind, 1, Some(metadata.clone())).await;
        fixture
            .backend
            .request_task_cancellation(fixture.request())
            .await
            .unwrap();
        let class = P::Class(ClassId::new(42).unwrap());
        let predicates = match filter {
            "import" => vec![
                P::ImportDryRun(false),
                P::ImportAtomicity(TaskImportAtomicity::BestEffort),
                P::ImportCollisionPolicy(TaskImportCollisionPolicy::Overwrite),
                P::ImportPermissionPolicy(TaskImportPermissionPolicy::Continue),
                P::ImportHasFailedItems(false),
            ],
            "export" => vec![
                class,
                P::ExportScope(TaskExportScopeKind::ObjectsInClass),
                P::ExportTemplate(ExportTemplateId::new(7).unwrap()),
                P::ExportHasWarnings(false),
                P::ExportTruncated(false),
                P::OutputState(TaskOutputState::NotProduced),
            ],
            "backup" => vec![
                P::BackupIncludeHistory(false),
                P::OutputState(TaskOutputState::NotProduced),
            ],
            "reindex" => vec![class, P::ComputationRevision(3)],
            "schema" => vec![
                class,
                P::SchemaRevision(SchemaRevision::try_from(3).unwrap()),
            ],
            "remote_object" => vec![
                class,
                P::Object(ObjectId::new(8).unwrap()),
                P::RemoteTarget(RemoteTargetId::new(7).unwrap()),
                P::RemoteSideEffect(TaskRemoteSideEffectState::NotSent),
            ],
            "collection" => vec![P::Collection(CollectionId::new(42).unwrap())],
            "class_relation" => vec![P::ClassRelation(ClassRelationId::new(42).unwrap())],
            "object_relation" => vec![P::ObjectRelation(ObjectRelationId::new(42).unwrap())],
            _ => unreachable!(),
        };
        let search = StorageTaskSearch::default()
            .discovering(TaskDiscoverySearch::try_new(predicates, chrono::Utc::now()).unwrap())
            .unwrap();
        let (rows, count) = fixture
            .backend
            .list_tasks(
                StorageTaskListQuery::new(
                    Some(fixture.user.principal_id),
                    None,
                    None,
                    QueryOptions::new(Vec::new(), Vec::new(), Some(1), None, true).unwrap(),
                )
                .searching(search),
            )
            .await
            .unwrap()
            .into_parts();
        assert_eq!(count, Some(1));
        assert_eq!(rows[0].id(), fixture.task.id());
        assert!(rows[0].request_payload().is_none());
        let mut expected = metadata;
        expected.record_terminal(0);
        assert_eq!(rows[0].metadata(), Some(&expected));
        fixture.cleanup().await;
    }
}

#[rstest]
#[case(
    StorageTaskKind::Import,
    hubuum_storage_core::TaskDiscoveryPredicate::ImportDryRun(false)
)]
#[case(
    StorageTaskKind::Export,
    hubuum_storage_core::TaskDiscoveryPredicate::ExportHasWarnings(false)
)]
#[case(
    StorageTaskKind::Export,
    hubuum_storage_core::TaskDiscoveryPredicate::ExportTruncated(false)
)]
#[case(
    StorageTaskKind::Backup,
    hubuum_storage_core::TaskDiscoveryPredicate::BackupIncludeHistory(false)
)]
#[actix_web::test]
async fn unknown_discovery_does_not_match_false(
    #[case] kind: StorageTaskKind,
    #[case] predicate: hubuum_storage_core::TaskDiscoveryPredicate,
) {
    use hubuum_storage_core::{StorageTaskSearch, TaskDiscoverySearch};
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, kind).await;
        let search = StorageTaskSearch::default()
            .discovering(
                TaskDiscoverySearch::try_new(vec![predicate.clone()], chrono::Utc::now()).unwrap(),
            )
            .unwrap();
        let (rows, count) = fixture
            .backend
            .list_tasks(
                StorageTaskListQuery::new(
                    Some(fixture.user.principal_id),
                    None,
                    None,
                    QueryOptions::new(Vec::new(), Vec::new(), Some(1), None, true).unwrap(),
                )
                .searching(search),
            )
            .await
            .unwrap()
            .into_parts();
        assert!(rows.is_empty());
        assert_eq!(count, Some(0));
        fixture.cleanup().await;
    }
}

#[rstest]
#[case(StorageTaskKind::Export)]
#[case(StorageTaskKind::Backup)]
#[actix_web::test]
async fn produced_output_discovery_survives_artifact_purge(#[case] kind: StorageTaskKind) {
    use hubuum_storage_core::{
        StorageTaskSearch, TaskDiscoveryPredicate, TaskDiscoverySearch, TaskOutputState,
    };
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, kind).await;
        let lease = fixture.claim().await;
        let expires = chrono::Utc::now() - chrono::Duration::hours(1);
        let payload = if kind == StorageTaskKind::Export {
            StorageTaskCompletionPayload::Export(
                StorageExportTaskArtifact::builder(
                    "application/json",
                    crate::storage::StorageExportTaskArtifactContent::Json(serde_json::json!({})),
                    serde_json::json!({}),
                    serde_json::json!([]),
                    expires,
                )
                .warning_state(2, true)
                .try_build()
                .unwrap(),
            )
        } else {
            StorageTaskCompletionPayload::Backup(
                StorageBackupTaskArtifact::try_new(b"{}".to_vec(), expires).unwrap(),
            )
        };
        fixture
            .backend
            .complete_task(StorageTaskCompletion::new(
                StorageTaskTerminalUpdate::new(
                    lease,
                    StorageTaskTerminalStatus::Succeeded,
                    StorageTaskResultCounts::try_new(1, 1, 0).unwrap(),
                ),
                StorageTaskEventInput::new("succeeded", "artifact produced"),
                payload,
            ))
            .await
            .unwrap();
        if kind == StorageTaskKind::Export {
            fixture
                .backend
                .purge_expired_export_outputs()
                .await
                .unwrap();
        } else {
            fixture
                .backend
                .purge_expired_backup_outputs()
                .await
                .unwrap();
        }
        let mut predicates = vec![TaskDiscoveryPredicate::OutputState(
            TaskOutputState::Expired,
        )];
        if kind == StorageTaskKind::Export {
            predicates.extend([
                TaskDiscoveryPredicate::ExportHasWarnings(true),
                TaskDiscoveryPredicate::ExportTruncated(true),
            ]);
        }
        let search = StorageTaskSearch::default()
            .discovering(TaskDiscoverySearch::try_new(predicates, chrono::Utc::now()).unwrap())
            .unwrap();
        let (rows, count) = fixture
            .backend
            .list_tasks(
                StorageTaskListQuery::new(
                    Some(fixture.user.principal_id),
                    None,
                    None,
                    QueryOptions::new(Vec::new(), Vec::new(), Some(1), None, true).unwrap(),
                )
                .searching(search),
            )
            .await
            .unwrap()
            .into_parts();
        assert_eq!(count, Some(1));
        assert_eq!(rows[0].id(), fixture.task.id());
        assert!(!rows[0].discovery_state().output_present());
        fixture.cleanup().await;
    }
}

#[actix_web::test]
async fn postgres_task_discovery_projection_has_a_fixed_query_budget() {
    use hubuum_storage_core::{StorageTaskMetadata, TaskMetadataDetails};
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let BackendTestEnvironment::Postgres { pool } = &environment else {
            continue;
        };
        let pool = pool.clone();
        let metadata = StorageTaskMetadata::new(TaskMetadataDetails::SchemaValidation {
            class_id: Some(hubuum_domain::ClassId::new(42).unwrap()),
            schema_revision: Some(hubuum_domain::SchemaRevision::try_from(1).unwrap()),
            work_kind: Some(hubuum_storage_core::StorageSchemaWorkKind::Impact),
        })
        .unwrap();
        let fixture = Fixture::with_metadata(
            environment,
            StorageTaskKind::SchemaValidation,
            0,
            Some(metadata.clone()),
        )
        .await;
        let mut extra = Vec::new();
        for _ in 0..9 {
            extra.push(
                fixture
                    .backend
                    .create_task(
                        StorageTaskCreateRequest::builder(
                            StorageTaskKind::SchemaValidation,
                            fixture.user.principal_id,
                            serde_json::json!({}),
                            0,
                        )
                        .metadata(Some(metadata.clone()))
                        .try_build(20)
                        .unwrap(),
                    )
                    .await
                    .unwrap()
                    .id(),
            );
        }
        let (result, queries) = hubuum_storage_postgres::capture_queries(
            fixture.backend.list_tasks(StorageTaskListQuery::new(
                Some(fixture.user.principal_id),
                None,
                None,
                QueryOptions::new(Vec::new(), Vec::new(), Some(20), None, true).unwrap(),
            )),
        )
        .await;
        assert_eq!(result.unwrap().into_parts().0.len(), 10);
        assert_eq!(queries.domain_queries(), 3, "{:?}", queries.query_counts());
        assert_eq!(queries.queries_matching("schema_repair_reports"), 0);
        assert_eq!(queries.queries_matching("schema_impact_findings"), 0);
        hubuum_storage_postgres::test_support::delete_tasks(&pool, &extra)
            .await
            .unwrap();
        fixture.cleanup().await;
    }
}

#[rstest]
#[case(StorageTaskKind::Export)]
#[case(StorageTaskKind::Backup)]
#[actix_web::test]
async fn restoring_unchanged_artifact_facts_preserves_task_timestamps(
    #[case] kind: StorageTaskKind,
) {
    use diesel::sql_types::{Integer, Jsonb};
    use diesel_async::RunQueryDsl;
    use hubuum_storage_postgres::with_connection;
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let BackendTestEnvironment::Postgres { pool } = &environment else {
            continue;
        };
        let pool = pool.clone();
        let fixture = Fixture::new(environment, kind).await;
        let expires = chrono::DateTime::from_timestamp(1_800_000_000, 0).unwrap();
        let payload = if kind == StorageTaskKind::Export {
            StorageTaskCompletionPayload::Export(
                StorageExportTaskArtifact::builder(
                    "application/json",
                    crate::storage::StorageExportTaskArtifactContent::Json(serde_json::json!({})),
                    serde_json::json!({}),
                    serde_json::json!([]),
                    expires,
                )
                .try_build()
                .unwrap(),
            )
        } else {
            StorageTaskCompletionPayload::Backup(
                StorageBackupTaskArtifact::try_new(b"{}".to_vec(), expires).unwrap(),
            )
        };
        fixture
            .backend
            .complete_task(StorageTaskCompletion::new(
                StorageTaskTerminalUpdate::new(
                    fixture.claim().await,
                    StorageTaskTerminalStatus::Succeeded,
                    StorageTaskResultCounts::try_new(1, 1, 0).unwrap(),
                ),
                StorageTaskEventInput::new("succeeded", "artifact produced"),
                payload,
            ))
            .await
            .unwrap();
        let (task, _) = fixture
            .backend
            .get_task_access(fixture.task.id())
            .await
            .unwrap()
            .into_parts();
        // Backups canonicalize timestamps through the typed representation.
        // This intentionally removes redundant fractional zeros from JSON.
        let metadata = task.metadata().unwrap().to_value();
        let id = task.id().id();
        with_connection(&pool, async |connection| {
            diesel::sql_query("UPDATE tasks SET discovery_metadata = $1 WHERE id = $2")
                .bind::<Jsonb, _>(metadata)
                .bind::<Integer, _>(id)
                .execute(connection)
                .await
        })
        .await
        .unwrap();
        let (before, _) = fixture
            .backend
            .get_task_access(fixture.task.id())
            .await
            .unwrap()
            .into_parts();
        let sql = if kind == StorageTaskKind::Export {
            "UPDATE export_task_outputs SET task_id = task_id WHERE task_id = $1"
        } else {
            "UPDATE backup_task_outputs SET task_id = task_id WHERE task_id = $1"
        };
        with_connection(&pool, async |connection| {
            diesel::sql_query(sql)
                .bind::<Integer, _>(id)
                .execute(connection)
                .await
        })
        .await
        .unwrap();
        let (after, _) = fixture
            .backend
            .get_task_access(fixture.task.id())
            .await
            .unwrap()
            .into_parts();
        assert_eq!(before.updated_at(), after.updated_at());
        assert_eq!(before.metadata(), after.metadata());
        fixture.cleanup().await;
    }
}
