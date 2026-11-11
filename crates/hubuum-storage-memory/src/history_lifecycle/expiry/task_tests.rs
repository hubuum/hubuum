use super::progress_tests::{fixture_write, seed};
use super::*;
use hubuum_storage_core::{
    StorageTaskClaimRequest, TaskByteLimit, TaskCapacityLimit, TaskCapacityRejection,
    TaskKindCapacity, TaskScopeCapacity, TaskWorkerId, TaskWorkerKinds,
};
use hubuum_task_core::TaskExecutionLimit;
use rstest::rstest;

fn claim_request() -> StorageTaskClaimRequest {
    StorageTaskClaimRequest::new(
        TaskWorkerId::generate(),
        StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap(),
    )
    .with_kinds(TaskWorkerKinds::new(&[StorageTaskKind::HistoryExpiry]).unwrap())
}

#[rstest]
#[case::kind_outstanding(true)]
#[case::principal_outstanding(false)]
#[tokio::test]
async fn expiry_admission_enforces_its_own_internal_lane_limits(#[case] kind_limit: bool) {
    let storage = MemoryStorage::new();
    let (runtime, scope, lease) = prepare(&storage, StorageIdentityHistoryKind::Object).await;
    storage
        .run_in_scope(scope, async {
            let current = fixture_write(&storage, storage.task_policy())
                .await
                .unwrap();
            let old = current.policy().for_kind(StorageTaskKind::HistoryExpiry);
            let capacity = old.capacity();
            let policy = current.policy().clone().with_kind(TaskKindCapacity::new(
                StorageTaskKind::HistoryExpiry,
                TaskScopeCapacity::new(
                    TaskCapacityLimit::new(if kind_limit { 1 } else { 64 }).unwrap(),
                    capacity.executing(),
                    capacity.resident_bytes(),
                ),
                TaskCapacityLimit::new(if kind_limit { 2 } else { 1 }).unwrap(),
            ));
            fixture_write(
                &storage,
                storage
                    .replace_task_policy(StorageTaskPolicyUpdate::new(current.revision(), policy)),
            )
            .await
            .unwrap();
            let payload = storage.state.read_control().await.tasks[&lease.task_id().id()]
                .request_payload
                .clone()
                .unwrap();
            let result = fixture_write(
                &storage,
                storage.create_task(
                    StorageTaskCreateRequest::builder(
                        StorageTaskKind::HistoryExpiry,
                        PrincipalId::new(1).unwrap(),
                        payload,
                        0,
                    )
                    .try_build()
                    .unwrap(),
                ),
            )
            .await;
            assert_eq!(
                result.unwrap_err().task_capacity_rejection(),
                Some(if kind_limit {
                    TaskCapacityRejection::Kind
                } else {
                    TaskCapacityRejection::PrincipalKind
                })
            );
        })
        .await;
    storage.unregister_runtime(runtime).await.unwrap();
}

async fn claim(storage: &MemoryStorage) -> StorageTaskLease {
    fixture_write(storage, storage.claim_next_task(claim_request()))
        .await
        .unwrap()
        .unwrap()
        .into_parts()
        .1
}

async fn prepare(
    storage: &MemoryStorage,
    family: StorageIdentityHistoryKind,
) -> (RuntimeRegistration, StorageExecutionScope, StorageTaskLease) {
    seed(storage, family, "task history", "before").await;
    let runtime = storage.register_history_runtime(None).await.unwrap();
    let application = storage
        .capture_application_execution(Some(runtime.owner()))
        .await
        .unwrap();
    let payload = HistoryExpiryTaskPayload::new(
        HistoryLifecycleScan::new(
            HistoryPolicyKey::new(family, HistoryPolicyScope::Deployment).unwrap(),
            1000,
        )
        .unwrap(),
        HistoryExpiryOrigin::Scheduled { initiator: None },
        application.fence(),
    );
    let scope = StorageExecutionScope::default().with_application_access(application);
    let lease = storage
        .run_in_scope(scope.clone(), async {
            fixture_write(
                storage,
                storage.create_task(
                    StorageTaskCreateRequest::builder(
                        StorageTaskKind::HistoryExpiry,
                        PrincipalId::new(1).unwrap(),
                        serde_json::to_value(payload).unwrap(),
                        0,
                    )
                    .try_build()
                    .unwrap(),
                ),
            )
            .await
            .unwrap();
            let lease = claim(storage).await;
            fixture_write(
                storage,
                storage.admit_task_execution(StorageTaskExecutionAdmission::new(
                    lease.clone(),
                    TaskExecutionLimit::from_milliseconds(7_200_000).unwrap(),
                )),
            )
            .await
            .unwrap();
            lease
        })
        .await;
    (runtime, scope, lease)
}

#[rstest]
#[case::collection(StorageIdentityHistoryKind::Collection)]
#[case::class(StorageIdentityHistoryKind::Class)]
#[case::object(StorageIdentityHistoryKind::Object)]
#[case::class_relation(StorageIdentityHistoryKind::ClassRelation)]
#[case::object_relation(StorageIdentityHistoryKind::ObjectRelation)]
#[case::template(StorageIdentityHistoryKind::ExportTemplate)]
#[case::remote(StorageIdentityHistoryKind::RemoteTarget)]
#[case::computed(StorageIdentityHistoryKind::ComputedDefinitions)]
#[case::schema(StorageIdentityHistoryKind::ClassSchema)]
#[tokio::test]
async fn restart_resumes_exact_batch_without_fresh_approval(
    #[case] family: StorageIdentityHistoryKind,
) {
    let storage = MemoryStorage::new();
    let (runtime, scope, lease) = prepare(&storage, family).await;
    storage
        .run_in_scope(scope, async {
            let first = storage
                .process_history_expiry_task(HistoryExpiryTaskRequest::new(
                    lease.clone(),
                    Some(HistoryExpiryTaskAuthorization::scheduled()),
                ))
                .await
                .unwrap();
            let before = storage.capture_history_lifecycle().await.unwrap();
            let manifest = first.retained_manifest(&before).unwrap().clone();
            storage
                .state
                .write_control()
                .await
                .tasks
                .get_mut(&lease.task_id().id())
                .unwrap()
                .lease_expires_at = Some(Utc::now() - Duration::seconds(1));
            fixture_write(&storage, storage.recover_expired_task_leases(10))
                .await
                .unwrap();
            let replacement = claim(&storage).await;
            assert!(
                storage
                    .process_history_expiry_task(HistoryExpiryTaskRequest::new(lease, None))
                    .await
                    .is_err()
            );
            let resumed = storage
                .process_history_expiry_task(HistoryExpiryTaskRequest::new(replacement, None))
                .await
                .unwrap();
            let after = storage.capture_history_lifecycle().await.unwrap();
            assert_eq!(resumed, first);
            assert_eq!(resumed.retained_manifest(&after).unwrap(), &manifest);
        })
        .await;
    storage.unregister_runtime(runtime).await.unwrap();
}

#[rstest]
#[case::missing_approval("approval")]
#[case::required_audit("audit")]
#[case::checkpoint_internal_capacity("capacity")]
#[case::checkpoint_kind_capacity("kind_capacity")]
#[case::unknown_checkpoint("unknown")]
#[tokio::test]
async fn failed_task_step_keeps_checkpoint_and_authority_atomic(#[case] mode: &str) {
    let storage = MemoryStorage::new();
    let (runtime, scope, lease) = prepare(&storage, StorageIdentityHistoryKind::Object).await;
    storage
        .run_in_scope(scope, async {
            if mode == "audit" {
                storage.state.write_control().await.next_event_sequence = i64::MAX;
            }
            if mode == "unknown" {
                storage
                    .state
                    .write_control()
                    .await
                    .tasks
                    .get_mut(&lease.task_id().id())
                    .unwrap()
                    .checkpoint = Some(
                    StorageTaskCheckpoint::try_new("future.history", 1, serde_json::json!({}))
                        .unwrap(),
                );
            }
            if matches!(mode, "capacity" | "kind_capacity") {
                let current = fixture_write(&storage, storage.task_policy())
                    .await
                    .unwrap();
                let capacity = current.policy().internal();
                let reduced = TaskScopeCapacity::new(
                    capacity.outstanding(),
                    capacity.executing(),
                    TaskByteLimit::new(1).unwrap(),
                );
                let policy = if mode == "kind_capacity" {
                    let old = current.policy().for_kind(StorageTaskKind::HistoryExpiry);
                    current.policy().clone().with_kind(TaskKindCapacity::new(
                        StorageTaskKind::HistoryExpiry,
                        reduced,
                        old.principal_outstanding(),
                    ))
                } else {
                    current.policy().clone().with_internal(reduced)
                };
                let revision = current.revision();
                fixture_write(
                    &storage,
                    storage.replace_task_policy(StorageTaskPolicyUpdate::new(revision, policy)),
                )
                .await
                .unwrap();
            }
            let before = storage.capture_history_lifecycle().await.unwrap();
            let checkpoint = storage.state.read_control().await.tasks[&lease.task_id().id()]
                .checkpoint
                .clone();
            let result = storage
                .process_history_expiry_task(HistoryExpiryTaskRequest::new(
                    lease.clone(),
                    (mode != "approval").then(HistoryExpiryTaskAuthorization::scheduled),
                ))
                .await;
            let after = storage.capture_history_lifecycle().await.unwrap();
            assert!(result.is_err());
            assert_eq!(after.control(), before.control());
            assert_eq!(
                storage.state.read_control().await.tasks[&lease.task_id().id()].checkpoint,
                checkpoint
            );
        })
        .await;
    storage.unregister_runtime(runtime).await.unwrap();
}

#[rstest]
#[case::future_payload("payload")]
#[case::future_checkpoint("checkpoint")]
#[case::disabled_production_kind("production")]
#[tokio::test]
async fn unsupported_expiry_work_stays_queued(#[case] mode: &str) {
    let storage = MemoryStorage::new();
    let (runtime, scope, lease) = prepare(&storage, StorageIdentityHistoryKind::Collection).await;
    storage
        .run_in_scope(scope, async {
            storage
                .state
                .write_control()
                .await
                .tasks
                .get_mut(&lease.task_id().id())
                .unwrap()
                .lease_expires_at = Some(Utc::now() - Duration::seconds(1));
            fixture_write(&storage, storage.recover_expired_task_leases(10))
                .await
                .unwrap();
            {
                let mut state = storage.state.write_control().await;
                let task = state.tasks.get_mut(&lease.task_id().id()).unwrap();
                if mode == "payload" {
                    task.request_payload.as_mut().unwrap()["version"] = serde_json::json!(2);
                }
                if mode == "checkpoint" {
                    task.checkpoint = Some(
                        StorageTaskCheckpoint::try_new(
                            "hubuum.history_expiry",
                            2,
                            serde_json::json!({"future":"preserve"}),
                        )
                        .unwrap(),
                    );
                }
            }
            let before = storage.state.read_control().await.tasks[&lease.task_id().id()]
                .projection()
                .unwrap();
            assert_eq!(before.status(), StorageTaskStatus::Queued);
            let request = if mode == "production" {
                claim_request().with_kinds(TaskWorkerKinds::STANDARD)
            } else {
                claim_request()
            };
            let claimed = fixture_write(&storage, storage.claim_next_task(request))
                .await
                .unwrap();
            let after = storage.state.read_control().await.tasks[&lease.task_id().id()]
                .projection()
                .unwrap();
            assert!(claimed.is_none());
            assert_eq!(after.status(), StorageTaskStatus::Queued);
            assert_eq!(after.checkpoint(), before.checkpoint());
            assert_eq!(after.request_payload(), before.request_payload());
        })
        .await;
    storage.unregister_runtime(runtime).await.unwrap();
}
