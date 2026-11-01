use super::*;
use hubuum_domain::{GroupId, ServiceAccountId};
use hubuum_storage_conformance::verify_task_wait_resume;
use hubuum_storage_core::{
    StorageTask, StorageTaskCancellationChange, StorageTaskCancellationRequest,
    StorageTaskCheckpoint, StorageTaskClaimRequest, StorageTaskExecutionAdmission,
    StorageTaskResumeOutcome, StorageTaskResumeRequest, StorageTaskWait, StorageTaskWaitRequest,
    StorageTaskWaitToken, TaskCapacityStorage, TaskWorkerId,
};
use hubuum_task_core::{TaskExecutionLimit, TaskStopReason};
use rstest::rstest;

struct Fixture {
    environment: BackendTestEnvironment,
    backend: StorageHandle,
    user: BackendUserFixture,
    service_account: Option<ServiceAccountId>,
    service_account_owner: Option<GroupId>,
    task: StorageTask,
}

impl Fixture {
    async fn new(environment: BackendTestEnvironment, kind: StorageTaskKind) -> Self {
        Self::with_principal(environment, kind, false).await
    }

    async fn with_principal(
        environment: BackendTestEnvironment,
        kind: StorageTaskKind,
        service_account: bool,
    ) -> Self {
        let backend = environment.storage();
        let user = create_backend_user(&backend, &prefix("task_wait_user")).await;
        let (service_account, service_account_owner) = if service_account {
            let owner = backend
                .create_group(
                    StorageGroupCreate::new(
                        None,
                        prefix("task_wait_owner"),
                        Some("durable wait contract owner".to_string()),
                    ),
                    &EventContext::system(),
                )
                .await
                .unwrap()
                .into_value()
                .id();
            let id = backend
                .create_service_account(StorageServiceAccountCreate::new(
                    prefix("task_wait_service_account"),
                    "durable wait contract",
                    owner,
                    Some(user.principal_id),
                    EventContext::system(),
                ))
                .await
                .unwrap()
                .into_value()
                .id();
            (Some(id), Some(owner))
        } else {
            (None, None)
        };
        let submitted_by = service_account
            .map(|id| principal_id(id.id()))
            .unwrap_or(user.principal_id);
        let task = backend
            .create_task(
                StorageTaskCreateRequest::builder(
                    kind,
                    submitted_by,
                    serde_json::json!({"private": "retained request"}),
                    1,
                )
                .try_build()
                .unwrap(),
            )
            .await
            .map(expect_legacy_task)
            .unwrap();
        Self {
            environment,
            backend,
            user,
            service_account,
            service_account_owner,
            task,
        }
    }

    async fn claim(&self) -> StorageTaskLease {
        let duration = StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap();
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
                .claim_next_task(StorageTaskClaimRequest::new(
                    TaskWorkerId::generate(),
                    duration,
                ))
                .await
                .unwrap()
                .unwrap(),
        };
        assert_eq!(claim.task().id(), self.task.id());
        claim.lease().clone()
    }

    async fn admit(&self, lease: &StorageTaskLease, milliseconds: u64) {
        self.backend
            .admit_task_execution(StorageTaskExecutionAdmission::new(
                lease.clone(),
                TaskExecutionLimit::from_milliseconds(milliseconds).unwrap(),
            ))
            .await
            .unwrap();
    }

    async fn wait(&self, lease: StorageTaskLease) -> StorageTaskWait {
        self.backend
            .wait_task(StorageTaskWaitRequest::new(lease, checkpoint()))
            .await
            .unwrap()
    }

    async fn load(&self) -> StorageTask {
        self.backend
            .get_task_access(self.task.id())
            .await
            .unwrap()
            .into_parts()
            .0
    }

    async fn event_count(&self) -> usize {
        self.backend
            .list_task_events(StorageTaskChildListQuery::new(
                self.task.id(),
                QueryOptions::new(vec![], vec![], Some(100), None, true).unwrap(),
            ))
            .await
            .unwrap()
            .into_parts()
            .0
            .len()
    }

    async fn cleanup(self) {
        if let BackendTestEnvironment::Postgres { pool } = self.environment {
            hubuum_storage_postgres::test_support::delete_task(&pool, self.task.id())
                .await
                .unwrap();
        }
        if let Some(id) = self.service_account {
            self.backend
                .delete_service_account(StorageServiceAccountMutation::new(
                    id,
                    EventContext::system(),
                ))
                .await
                .unwrap()
                .into_value();
        }
        if let Some(id) = self.service_account_owner {
            self.backend
                .delete_group(id, &EventContext::system())
                .await
                .unwrap()
                .into_value();
        }
        delete_backend_user(&self.backend, self.user).await;
    }
}

fn checkpoint() -> StorageTaskCheckpoint {
    StorageTaskCheckpoint::try_new(
        "contract.wait",
        1,
        serde_json::json!({"cursor": "private checkpoint"}),
    )
    .unwrap()
}

fn resume_request(wait: &StorageTaskWait) -> StorageTaskResumeRequest {
    StorageTaskResumeRequest::new(wait.task().id(), wait.token().clone())
}

#[rstest]
#[case::export(StorageTaskKind::Export)]
#[case::backup(StorageTaskKind::Backup)]
#[actix_web::test]
async fn every_backend_satisfies_the_packaged_wait_resume_contract(#[case] kind: StorageTaskKind) {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, kind).await;
        let lease = fixture.claim().await;
        fixture.admit(&lease, 60_000).await;
        let result = verify_task_wait_resume(
            &fixture.backend,
            StorageTaskWaitRequest::new(lease, checkpoint()),
        )
        .await;
        fixture.cleanup().await;
        result.unwrap();
    }
}

#[rstest]
#[case::export(StorageTaskKind::Export)]
#[case::backup(StorageTaskKind::Backup)]
#[actix_web::test]
async fn waiting_retains_work_without_an_execution_lease(#[case] kind: StorageTaskKind) {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, kind).await;
        let lease = fixture.claim().await;
        fixture.admit(&lease, 60_000).await;
        let before = fixture.load().await;
        let wait = fixture.wait(lease).await;
        let after = fixture.load().await;
        assert_eq!(wait.task(), &after);
        assert_eq!(after.status(), StorageTaskStatus::Waiting);
        assert!(!after.has_lease());
        assert_eq!(after.request_payload(), before.request_payload());
        assert_eq!(after.control().deadline(), before.control().deadline());
        assert_eq!(after.progress(), before.progress());
        assert_eq!(after.checkpoint(), Some(&checkpoint()));
        fixture.cleanup().await;
    }
}

#[actix_web::test]
async fn waiting_response_exposes_the_status_without_checkpoint_contents() {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Export).await;
        let lease = fixture.claim().await;
        fixture.admit(&lease, 60_000).await;
        let wait = fixture.wait(lease).await;
        let record = crate::services::tasks::task_from_storage(wait.task().clone()).unwrap();
        let response = serde_json::to_value(record.to_response().unwrap()).unwrap();
        assert_eq!(response["status"], "waiting");
        assert!(!response.to_string().contains("private checkpoint"));
        assert!(!format!("{record:?}").contains("private checkpoint"));
        fixture.cleanup().await;
    }
}

#[actix_web::test]
async fn waiting_events_expose_only_bounded_checkpoint_metadata() {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Export).await;
        let lease = fixture.claim().await;
        fixture.admit(&lease, 60_000).await;
        let wait = fixture.wait(lease).await;
        let (events, _) = fixture
            .backend
            .list_task_events(StorageTaskChildListQuery::new(
                fixture.task.id(),
                QueryOptions::new(vec![], vec![], Some(100), None, true).unwrap(),
            ))
            .await
            .unwrap()
            .into_parts();
        let event = events
            .iter()
            .find(|event| event.event_type() == "waiting")
            .expect("entering wait emits a lifecycle event");
        assert_eq!(
            event.data(),
            Some(&serde_json::json!({
                "checkpoint_format": checkpoint().format(),
                "checkpoint_version": checkpoint().version(),
                "checkpoint_bytes": checkpoint().byte_size(),
            }))
        );
        assert!(!event.message().contains("private checkpoint"));
        assert!(!event.message().contains(wait.token().adapter_value()));
        fixture.cleanup().await;
    }
}

#[rstest]
#[case::import(StorageTaskKind::Import)]
#[case::reindex(StorageTaskKind::Reindex)]
#[case::remote(StorageTaskKind::RemoteCall)]
#[case::schema_validation(StorageTaskKind::SchemaValidation)]
#[case::schema_profiling(StorageTaskKind::SchemaProfiling)]
#[actix_web::test]
async fn unsupported_task_kinds_cannot_enter_wait(#[case] kind: StorageTaskKind) {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, kind).await;
        let lease = fixture.claim().await;
        fixture.admit(&lease, 60_000).await;
        assert!(
            fixture
                .backend
                .wait_task(StorageTaskWaitRequest::new(lease, checkpoint()))
                .await
                .is_err()
        );
        assert_eq!(fixture.load().await.status(), StorageTaskStatus::Validating);
        fixture.cleanup().await;
    }
}

#[actix_web::test]
async fn waiting_requires_an_admitted_execution_deadline() {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Export).await;
        let lease = fixture.claim().await;
        assert!(
            fixture
                .backend
                .wait_task(StorageTaskWaitRequest::new(lease, checkpoint()))
                .await
                .is_err()
        );
        assert!(fixture.load().await.checkpoint().is_none());
        fixture.cleanup().await;
    }
}

#[actix_web::test]
async fn ordinary_lease_recovery_does_not_resume_a_waiting_task() {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Export).await;
        let lease = fixture.claim().await;
        fixture.admit(&lease, 60_000).await;
        fixture.wait(lease).await;
        let recovered = fixture
            .environment
            .storage()
            .recover_expired_task_leases(100)
            .await
            .unwrap();
        assert!(!recovered.iter().any(|task| task.id() == fixture.task.id()));
        assert_eq!(fixture.load().await.status(), StorageTaskStatus::Waiting);
        fixture.cleanup().await;
    }
}

#[actix_web::test]
async fn concurrent_notifications_resume_a_wait_exactly_once() {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Export).await;
        let lease = fixture.claim().await;
        fixture.admit(&lease, 60_000).await;
        let wait = fixture.wait(lease).await;
        let first = fixture.environment.storage();
        let second = fixture.environment.storage();
        let (first, second) = tokio::join!(
            first.resume_task(resume_request(&wait)),
            second.resume_task(resume_request(&wait)),
        );
        let outcomes = [first.unwrap(), second.unwrap()];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, StorageTaskResumeOutcome::Queued(_)))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, StorageTaskResumeOutcome::Unchanged))
                .count(),
            1
        );
        fixture.cleanup().await;
    }
}

#[actix_web::test]
async fn resumed_work_keeps_its_checkpoint_and_original_deadline() {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Export).await;
        let lease = fixture.claim().await;
        fixture.admit(&lease, 60_000).await;
        let wait = fixture.wait(lease.clone()).await;
        let resumed = fixture
            .backend
            .resume_task(resume_request(&wait))
            .await
            .unwrap();
        let StorageTaskResumeOutcome::Queued(queued) = resumed else {
            panic!("the first notification must queue the waiting task");
        };
        assert!(!queued.has_lease());
        let next_lease = fixture.claim().await;
        assert_ne!(next_lease, lease);
        fixture.admit(&next_lease, 120_000).await;
        let resumed = fixture.load().await;
        assert_eq!(resumed.checkpoint(), wait.task().checkpoint());
        assert_eq!(
            resumed.control().deadline(),
            wait.task().control().deadline()
        );
        fixture.cleanup().await;
    }
}

#[rstest]
#[case::while_waiting(false)]
#[case::after_new_claim(true)]
#[actix_web::test]
async fn entering_wait_fences_the_old_lease(#[case] reclaim: bool) {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Export).await;
        let lease = fixture.claim().await;
        fixture.admit(&lease, 60_000).await;
        let wait = fixture.wait(lease.clone()).await;
        if reclaim {
            fixture
                .backend
                .resume_task(resume_request(&wait))
                .await
                .unwrap();
            fixture.claim().await;
        }
        assert!(
            !fixture
                .backend
                .renew_task_lease(
                    lease.clone(),
                    StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap(),
                )
                .await
                .unwrap()
        );
        assert!(
            fixture
                .backend
                .update_task_state(StorageTaskActiveUpdate::new(
                    lease,
                    StorageTaskActiveStatus::Running,
                    StorageTaskResultCounts::default(),
                ))
                .await
                .is_err()
        );
        fixture.cleanup().await;
    }
}

#[rstest]
#[case::unrecognized(false)]
#[case::already_consumed(true)]
#[actix_web::test]
async fn obsolete_wait_notifications_have_no_effect(#[case] consumed: bool) {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Export).await;
        let lease = fixture.claim().await;
        fixture.admit(&lease, 60_000).await;
        let wait = fixture.wait(lease).await;
        let token = if consumed {
            fixture
                .backend
                .resume_task(resume_request(&wait))
                .await
                .unwrap();
            wait.token().clone()
        } else {
            StorageTaskWaitToken::try_new(uuid::Uuid::new_v4().to_string()).unwrap()
        };
        let before = fixture.load().await;
        let event_count = fixture.event_count().await;
        let outcome = fixture
            .backend
            .resume_task(StorageTaskResumeRequest::new(fixture.task.id(), token))
            .await
            .unwrap();
        assert!(matches!(outcome, StorageTaskResumeOutcome::Unchanged));
        assert_eq!(fixture.load().await, before);
        assert_eq!(fixture.event_count().await, event_count);
        fixture.cleanup().await;
    }
}

#[actix_web::test]
async fn an_earlier_wait_token_cannot_resume_a_later_wait() {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Export).await;
        let lease = fixture.claim().await;
        fixture.admit(&lease, 60_000).await;
        let first = fixture.wait(lease).await;
        fixture
            .backend
            .resume_task(resume_request(&first))
            .await
            .unwrap();
        let next_lease = fixture.claim().await;
        let second = fixture.wait(next_lease).await;
        let result = fixture
            .backend
            .resume_task(resume_request(&first))
            .await
            .unwrap();
        assert!(matches!(result, StorageTaskResumeOutcome::Unchanged));
        assert_eq!(fixture.load().await, *second.task());
        fixture.cleanup().await;
    }
}

#[actix_web::test]
async fn cancellation_settles_waiting_work_without_reacquiring_execution() {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Export).await;
        let lease = fixture.claim().await;
        fixture.admit(&lease, 60_000).await;
        let wait = fixture.wait(lease).await;
        let cancelled = fixture
            .backend
            .request_task_cancellation(StorageTaskCancellationRequest::new(
                wait.task(),
                EventContext::system(),
            ))
            .await
            .unwrap();
        assert_eq!(
            cancelled.change(),
            StorageTaskCancellationChange::CancelledWaiting
        );
        assert_eq!(cancelled.task().status(), StorageTaskStatus::Cancelled);
        assert!(cancelled.task().checkpoint().is_none());
        assert!(cancelled.task().request_payload().is_none());
        assert!(matches!(
            fixture
                .backend
                .resume_task(resume_request(&wait))
                .await
                .unwrap(),
            StorageTaskResumeOutcome::Unchanged
        ));
        fixture.cleanup().await;
    }
}

#[actix_web::test]
async fn disabling_the_submitting_service_account_cancels_waiting_work() {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::with_principal(environment, StorageTaskKind::Export, true).await;
        let lease = fixture.claim().await;
        fixture.admit(&lease, 60_000).await;
        let wait = fixture.wait(lease).await;
        let (disabled, cancelled_kinds) = fixture
            .backend
            .disable_service_account(StorageServiceAccountMutation::new(
                fixture.service_account.unwrap(),
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value()
            .into_parts();
        assert!(disabled.disabled_at().is_some());
        assert_eq!(cancelled_kinds, vec![StorageTaskKind::Export]);
        let cancelled = fixture.load().await;
        assert_eq!(cancelled.status(), StorageTaskStatus::Cancelled);
        assert!(cancelled.checkpoint().is_none());
        assert!(matches!(
            fixture
                .backend
                .resume_task(resume_request(&wait))
                .await
                .unwrap(),
            StorageTaskResumeOutcome::Unchanged
        ));
        fixture.cleanup().await;
    }
}

#[rstest]
#[case::notification(true)]
#[case::recovery(false)]
#[actix_web::test]
async fn waiting_deadlines_settle_without_reexecuting(#[case] notify: bool) {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Export).await;
        let lease = fixture.claim().await;
        fixture.admit(&lease, 2_000).await;
        let wait = fixture.wait(lease).await;
        let remaining = (wait.task().control().deadline().unwrap() - chrono::Utc::now())
            .to_std()
            .unwrap_or_default();
        tokio::time::sleep(remaining + Duration::from_millis(10)).await;
        // Parallel contracts run global recovery sweeps. A sweep may hold our
        // expired task or finish it before its notification arrives; both paths
        // must converge on the same terminal stop without replaying execution.
        let stopped = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if notify {
                    match fixture.backend.resume_task(resume_request(&wait)).await {
                        Ok(StorageTaskResumeOutcome::Stopped(_)
                        | StorageTaskResumeOutcome::Unchanged) => {
                            let task = fixture.load().await;
                            assert!(task.status().is_terminal(), "an expired notification must stop the task or observe completed recovery");
                            break task;
                        }
                        Ok(StorageTaskResumeOutcome::Queued(_)) => {
                            panic!("an expired notification must not requeue execution");
                        }
                        Err(error)
                            if error.kind() == StorageErrorKind::Conflict
                                && error.message() == "Task is busy; retry resuming" => {}
                        Err(error) => panic!("unexpected expired-notification failure: {error}"),
                    }
                } else {
                    fixture.backend.recover_expired_task_leases(100).await.unwrap();
                    let task = fixture.load().await;
                    if task.status().is_terminal() {
                        break task;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("expired waiting work must settle despite concurrent recovery within five seconds");
        assert!(stopped.status().is_terminal());
        assert_eq!(
            stopped.control().terminal_reason(),
            Some(TaskStopReason::DeadlineExceeded)
        );
        assert!(stopped.checkpoint().is_none());
        assert!(!stopped.has_lease());
        let events = fixture
            .backend
            .list_task_events(StorageTaskChildListQuery::new(
                fixture.task.id(),
                QueryOptions::new(vec![], vec![], Some(100), None, true).unwrap(),
            ))
            .await
            .unwrap()
            .into_parts()
            .0;
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event_type() == "cancelled")
                .count(),
            1
        );
        fixture.cleanup().await;
    }
}

#[actix_web::test]
async fn waiting_preserves_admission_and_charges_checkpoint_residency() {
    let fixture = Fixture::new(
        BackendTestEnvironment::Memory {
            storage: MemoryStorage::new(),
        },
        StorageTaskKind::Export,
    )
    .await;
    let lease = fixture.claim().await;
    fixture.admit(&lease, 60_000).await;
    let before = fixture
        .backend
        .task_capacity_snapshot()
        .await
        .unwrap()
        .ordinary();
    fixture.wait(lease).await;
    let after = fixture
        .backend
        .task_capacity_snapshot()
        .await
        .unwrap()
        .ordinary();
    assert_eq!(after.outstanding(), before.outstanding());
    assert_eq!(after.executing() + 1, before.executing());
    assert_eq!(
        after.resident_bytes(),
        before.resident_bytes() + checkpoint().byte_size()
    );
    assert_eq!(
        after.cumulative_input_bytes(),
        before.cumulative_input_bytes()
    );
    fixture.cleanup().await;
}

#[actix_web::test]
async fn the_next_claim_charges_retained_request_and_checkpoint_once() {
    let fixture = Fixture::new(
        BackendTestEnvironment::Memory {
            storage: MemoryStorage::new(),
        },
        StorageTaskKind::Export,
    )
    .await;
    let lease = fixture.claim().await;
    fixture.admit(&lease, 60_000).await;
    let wait = fixture.wait(lease).await;
    let before = fixture
        .backend
        .task_capacity_snapshot()
        .await
        .unwrap()
        .ordinary();
    fixture
        .backend
        .resume_task(resume_request(&wait))
        .await
        .unwrap();
    let queued = fixture
        .backend
        .task_capacity_snapshot()
        .await
        .unwrap()
        .ordinary();
    assert_eq!(
        queued.cumulative_input_bytes(),
        before.cumulative_input_bytes()
    );
    fixture.claim().await;
    let claimed = fixture
        .backend
        .task_capacity_snapshot()
        .await
        .unwrap()
        .ordinary();
    assert_eq!(
        claimed.cumulative_input_bytes(),
        before.cumulative_input_bytes() + before.resident_bytes()
    );
    fixture.cleanup().await;
}
