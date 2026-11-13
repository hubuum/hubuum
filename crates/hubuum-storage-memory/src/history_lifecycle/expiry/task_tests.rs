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
            let mut payload = storage.state.read_control().await.tasks[&lease.task_id().id()]
                .request_payload
                .clone()
                .unwrap();
            payload["scan"]["key"]["family"] = serde_json::json!("class");
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
        HistoryExpiryOrigin::Scheduled {
            initiator: None,
            policy: None,
        },
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

#[tokio::test]
async fn oversized_source_task_commits_its_warning_and_continuation() {
    let storage = MemoryStorage::new();
    let oversized = seed(
        &storage,
        StorageIdentityHistoryKind::Collection,
        "oversized task source",
        &"x".repeat(17 * 1024 * 1024),
    )
    .await;
    let (runtime, scope, lease) = prepare(&storage, StorageIdentityHistoryKind::Collection).await;
    storage
        .run_in_scope(scope, async {
            let checkpoint = storage
                .process_history_expiry_task(HistoryExpiryTaskRequest::new(
                    lease.clone(),
                    Some(HistoryExpiryTaskAuthorization::scheduled()),
                ))
                .await
                .unwrap();
            let events = fixture_write(
                &storage,
                storage.list_task_events(StorageTaskChildListQuery::new(
                    lease.task_id(),
                    QueryOptions::new(vec![], vec![], Some(100), None, false).unwrap(),
                )),
            )
            .await
            .unwrap()
            .into_parts()
            .0;
            let warning = events
                .iter()
                .find(|event| {
                    event.data().is_some_and(|data| {
                        data["oversized_source_rows"]
                            .as_u64()
                            .is_some_and(|count| count > 0)
                    })
                })
                .expect("Oversized source progress must retain an explicit task warning");
            assert_eq!(warning.event_type(), "running");
            assert!(warning.message().contains("warning"));
            let task = fixture_write(&storage, storage.get_task_access(lease.task_id()))
                .await
                .unwrap()
                .into_parts()
                .0;
            assert_eq!(task.checkpoint(), Some(&checkpoint.encode().unwrap()));
            let snapshot = storage.capture_history_lifecycle().await.unwrap();
            assert!(
                snapshot
                    .control()
                    .authority()
                    .expiry()
                    .intents()
                    .iter()
                    .flat_map(|intent| intent.manifest().members())
                    .all(|member| member
                        .attributions()
                        .iter()
                        .all(|scope| !scope.matches(oversized.scope())))
            );
            let state = storage.state.read_control().await;
            assert!(state.history.iter().any(|row| matches!(
                &row.value, MemoryHistoryValue::Collection(value)
                    if value.description().len() > 16 * 1024 * 1024
            )));
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

fn protection(snapshot: &HistoryLifecycleSnapshot) -> HistoryExposureProtection {
    HistoryExposureProtection::new(
        snapshot.control().authority().clone(),
        ApplicationFence::new(
            snapshot.fence().incarnation(),
            snapshot.fence().deletion_revision().value() as i64,
        )
        .unwrap(),
        DeletionCheckpointIdentity::new("a".repeat(64)).unwrap(),
    )
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
async fn quarantined_replay_removes_restored_exact_copies_without_refreshing_receipts(
    #[case] family: StorageIdentityHistoryKind,
) {
    let storage = MemoryStorage::new();
    let (runtime, scope, lease) = prepare(&storage, family).await;
    let first = storage
        .run_in_scope(
            scope,
            storage.process_history_expiry_task(HistoryExpiryTaskRequest::new(
                lease,
                Some(HistoryExpiryTaskAuthorization::scheduled()),
            )),
        )
        .await
        .unwrap();
    let original = storage.capture_history_lifecycle().await.unwrap();
    let manifest = first.retained_manifest(&original).unwrap().clone();
    let restored = storage.state.read_control().await.clone();
    let request = HistoryExpiryReapplication::new(
        protection(&original),
        manifest.identity(),
        manifest.members()[0].target(),
    )
    .unwrap();
    assert!(
        storage.reapply_history_expiry(request).await.is_err(),
        "released application cannot replay"
    );
    storage.unregister_runtime(runtime).await.unwrap();
    {
        let mut state = storage.state.write_control().await;
        let fence =
            ApplicationFence::new(Uuid::new_v4(), state.deletion_control.revision()).unwrap();
        state.application_control = ApplicationControlState::quarantined(fence);
    }
    // Pending first receipts require actual deletion after recovery.
    for member in manifest.members() {
        let snapshot = storage.capture_history_lifecycle().await.unwrap();
        let request = HistoryExpiryReapplication::new(
            protection(&snapshot),
            manifest.identity(),
            member.target(),
        )
        .unwrap();
        assert!(storage.reapply_history_expiry(request).await.unwrap());
    }
    let completed = storage.capture_history_lifecycle().await.unwrap();
    storage
        .state
        .read_control()
        .await
        .require_history_expiry_reapplied()
        .unwrap();
    {
        let mut state = storage.state.write_control().await;
        state.history = restored.history;
        state.schema_history = restored.schema_history;
        state.computed_history = restored.computed_history;
        state.relation_history = restored.relation_history;
        assert!(
            state.require_history_expiry_reapplied().is_err(),
            "receipts alone cannot release restored copies"
        );
    }
    for member in manifest.members() {
        let request = HistoryExpiryReapplication::new(
            protection(&completed),
            manifest.identity(),
            member.target(),
        )
        .unwrap();
        assert!(
            storage
                .reapply_history_expiry(request.clone())
                .await
                .unwrap()
        );
        assert!(!storage.reapply_history_expiry(request).await.unwrap());
    }
    let after = storage.capture_history_lifecycle().await.unwrap();
    assert_eq!(
        after.control(),
        completed.control(),
        "replay preserves every first clock and canonical audit identity"
    );
    storage
        .state
        .read_control()
        .await
        .require_history_expiry_reapplied()
        .unwrap();
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
async fn protected_batch_removes_native_copies_before_protected_completion(
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
            let original = protection(&before);
            let copies = |state: &MemoryState| {
                state.history.len()
                    + state.schema_history.len()
                    + state.computed_history.len()
                    + state.relation_history.values().map(Vec::len).sum::<usize>()
            };
            for (index, _) in manifest.members().iter().enumerate() {
                let count = copies(&*storage.state.read_control().await);
                let next = storage
                    .process_history_expiry_task(
                        HistoryExpiryTaskRequest::new(lease.clone(), None)
                            .with_protection(original.clone()),
                    )
                    .await
                    .unwrap();
                assert!(next.admitted());
                assert_eq!(
                    next.needs_receipt_protection(),
                    index + 1 == manifest.members().len()
                );
                assert_eq!(copies(&*storage.state.read_control().await), count - 1);
            }
            let removed = storage.capture_history_lifecycle().await.unwrap();
            assert!(
                storage
                    .process_history_expiry_task(
                        HistoryExpiryTaskRequest::new(lease.clone(), None)
                            .with_protection(original)
                    )
                    .await
                    .is_err()
            );
            let completed = storage
                .process_history_expiry_task(
                    HistoryExpiryTaskRequest::new(lease.clone(), None)
                        .with_protection(protection(&removed)),
                )
                .await
                .unwrap();
            completed.validate_completion(&removed).unwrap();
            assert_eq!(completed.removed_count() as usize, manifest.members().len());
            assert_eq!(
                storage
                    .process_history_expiry_task(HistoryExpiryTaskRequest::new(lease.clone(), None))
                    .await
                    .unwrap(),
                completed
            );
            assert_eq!(
                storage.capture_history_lifecycle().await.unwrap().control(),
                removed.control()
            );
            let finish = |count| {
                StorageTaskCompletion::new(
                    StorageTaskTerminalUpdate::new(
                        lease.clone(),
                        StorageTaskTerminalStatus::Succeeded,
                        StorageTaskResultCounts::try_new(count, count, 0).unwrap(),
                    ),
                    StorageTaskEventInput::new("succeeded", "Protected bounded batch complete"),
                    StorageTaskCompletionPayload::HistoryExpiry,
                )
            };
            assert!(
                fixture_write(&storage, storage.complete_task(finish(0)))
                    .await
                    .is_err()
            );
            let finished = fixture_write(
                &storage,
                storage.complete_task(finish(completed.removed_count() as i32)),
            )
            .await
            .unwrap();
            assert_eq!(finished.status(), StorageTaskStatus::Succeeded);
        })
        .await;
    storage.unregister_runtime(runtime).await.unwrap();
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
async fn pre_exclusion_publication_requires_positive_settlement(
    #[case] family: StorageIdentityHistoryKind,
) {
    let storage = MemoryStorage::new();
    let (runtime, scope, lease) = prepare(&storage, family).await;
    storage
        .run_in_scope(scope, async {
            let owner = storage
                .admit_history_publication(HistoryPublicationKind::TaskOutput)
                .await
                .unwrap();
            let first = storage
                .process_history_expiry_task(HistoryExpiryTaskRequest::new(
                    lease.clone(),
                    Some(HistoryExpiryTaskAuthorization::scheduled()),
                ))
                .await
                .unwrap();
            let before = storage.capture_history_lifecycle().await.unwrap();
            assert!(
                storage
                    .process_history_expiry_task(
                        HistoryExpiryTaskRequest::new(lease.clone(), None)
                            .with_protection(protection(&before))
                    )
                    .await
                    .is_err()
            );
            assert_eq!(
                storage.capture_history_lifecycle().await.unwrap().control(),
                before.control()
            );
            let settled = owner
                .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
                .await
                .unwrap();
            storage.settle_history_publication(settled).await.unwrap();
            storage
                .process_history_expiry_task(
                    HistoryExpiryTaskRequest::new(lease, None).with_protection(protection(&before)),
                )
                .await
                .unwrap();
            let after = storage.capture_history_lifecycle().await.unwrap();
            assert_eq!(
                after
                    .control()
                    .authority()
                    .expiry()
                    .intent(first.batch())
                    .unwrap()
                    .removals()
                    .len(),
                1
            );
        })
        .await;
    storage.unregister_runtime(runtime).await.unwrap();
}

#[rstest]
#[case::independent_membership("protection")]
#[case::current_hold("hold")]
#[case::canonical_audit("audit")]
#[case::cancellation("cancel")]
#[case::deadline("deadline")]
#[tokio::test]
async fn removal_failure_preserves_copy_receipt_and_checkpoint(#[case] mode: &str) {
    let storage = MemoryStorage::new();
    let (runtime, scope, lease) = prepare(&storage, StorageIdentityHistoryKind::Object).await;
    storage
        .run_in_scope(scope, async {
            let checkpoint = storage
                .process_history_expiry_task(HistoryExpiryTaskRequest::new(
                    lease.clone(),
                    Some(HistoryExpiryTaskAuthorization::scheduled()),
                ))
                .await
                .unwrap();
            let admitted = storage.capture_history_lifecycle().await.unwrap();
            let mut proof = protection(&admitted);
            if mode == "protection" {
                proof = HistoryExposureProtection::new(
                    HistoryLifecycleAuthority::empty(),
                    proof.fence(),
                    proof.checkpoint().clone(),
                );
            }
            if mode == "hold" {
                let hold = HistoryRetentionHold::new(
                    Uuid::new_v4(),
                    checkpoint.retained_manifest(&admitted).unwrap().key(),
                    admitted.observed_at(),
                    None,
                    PrincipalIdentity::generate(),
                )
                .unwrap();
                fixture_write(
                    &storage,
                    storage.commit_history_lifecycle(
                        admitted
                            .control()
                            .create_hold(hold, admitted.fence())
                            .unwrap(),
                    ),
                )
                .await
                .unwrap();
            }
            {
                let mut state = storage.state.write_control().await;
                if mode == "audit" {
                    state.next_event_sequence = i64::MAX;
                }
                let task = state.tasks.get_mut(&lease.task_id().id()).unwrap();
                if mode == "cancel" {
                    task.control
                        .request_cancellation(StorageTaskCancellation::new(Utc::now(), None, None));
                }
                if mode == "deadline" {
                    task.control = StorageTaskControl::try_new(
                        StorageTaskKind::HistoryExpiry,
                        None,
                        Some(Utc::now() - Duration::seconds(1)),
                        StorageTaskExecutionPhase::HistoryAuthority,
                        None,
                    )
                    .unwrap();
                }
            }
            let before = storage.capture_history_lifecycle().await.unwrap();
            let (copies, stored_checkpoint) = {
                let state = storage.state.read_control().await;
                (
                    state.history.len(),
                    state.tasks[&lease.task_id().id()].checkpoint.clone(),
                )
            };
            assert!(
                storage
                    .process_history_expiry_task(
                        HistoryExpiryTaskRequest::new(lease.clone(), None).with_protection(proof)
                    )
                    .await
                    .is_err(),
                "{mode}"
            );
            let state = storage.state.read_control().await;
            assert_eq!(state.history.len(), copies);
            assert_eq!(
                state.tasks[&lease.task_id().id()].checkpoint,
                stored_checkpoint
            );
            assert_eq!(&state.history_lifecycle, before.control());
        })
        .await;
    storage.unregister_runtime(runtime).await.unwrap();
}

#[tokio::test]
async fn recovery_wakes_wait_once_and_preserves_original_deadline() {
    let storage = MemoryStorage::new();
    let (runtime, scope, lease) = prepare(&storage, StorageIdentityHistoryKind::Object).await;
    storage
        .run_in_scope(scope, async {
            let checkpoint = storage
                .process_history_expiry_task(HistoryExpiryTaskRequest::new(
                    lease.clone(),
                    Some(HistoryExpiryTaskAuthorization::scheduled()),
                ))
                .await
                .unwrap();
            let waiting = fixture_write(
                &storage,
                storage.wait_task(StorageTaskWaitRequest::new(
                    lease.clone(),
                    checkpoint.encode().unwrap(),
                )),
            )
            .await
            .unwrap();
            assert_eq!(
                storage
                    .recover_history_expiry_task(checkpoint.batch())
                    .await
                    .unwrap()
                    .unwrap()
                    .status(),
                StorageTaskStatus::Waiting
            );
            {
                let mut state = storage.state.write_control().await;
                let task = state.tasks.get_mut(&lease.task_id().id()).unwrap();
                task.created_at -= Duration::seconds(31);
                task.started_at = task.started_at.map(|at| at - Duration::seconds(31));
                task.updated_at -= Duration::seconds(31);
            }
            let (left, right) = tokio::join!(
                storage.recover_history_expiry_task(checkpoint.batch()),
                storage.recover_history_expiry_task(checkpoint.batch())
            );
            for recovered in [left.unwrap().unwrap(), right.unwrap().unwrap()] {
                assert_eq!(recovered.id(), lease.task_id());
                assert_eq!(recovered.status(), StorageTaskStatus::Queued);
                assert_eq!(
                    recovered.control().deadline(),
                    waiting.task().control().deadline()
                );
                assert_eq!(recovered.checkpoint(), waiting.task().checkpoint());
            }
            assert!(matches!(
                fixture_write(
                    &storage,
                    storage.resume_task(StorageTaskResumeRequest::new(
                        lease.task_id(),
                        waiting.token().clone()
                    ))
                )
                .await
                .unwrap(),
                StorageTaskResumeOutcome::Unchanged
            ));
        })
        .await;
    storage.unregister_runtime(runtime).await.unwrap();
}

#[rstest]
#[case::cancelled("cancelled")]
#[case::deadline("deadline")]
#[case::purged("purged")]
#[case::audit_rollback("audit")]
#[case::fresh_admission("admission")]
#[tokio::test]
async fn recovery_adopts_partial_batch_without_reviving_original_task(#[case] mode: &str) {
    let storage = MemoryStorage::new();
    let (runtime, scope, lease) = prepare(&storage, StorageIdentityHistoryKind::Object).await;
    storage
        .run_in_scope(scope, async {
            seed(
                &storage,
                StorageIdentityHistoryKind::Object,
                "second retained object",
                "before",
            )
            .await;
            let checkpoint = storage
                .process_history_expiry_task(HistoryExpiryTaskRequest::new(
                    lease.clone(),
                    Some(HistoryExpiryTaskAuthorization::scheduled()),
                ))
                .await
                .unwrap();
            let snapshot = storage.capture_history_lifecycle().await.unwrap();
            storage
                .process_history_expiry_task(
                    HistoryExpiryTaskRequest::new(lease.clone(), None)
                        .with_protection(protection(&snapshot)),
                )
                .await
                .unwrap();
            let before = storage.capture_history_lifecycle().await.unwrap();
            let original = before
                .control()
                .authority()
                .expiry()
                .intent(checkpoint.batch())
                .unwrap()
                .clone();
            let wait = fixture_write(
                &storage,
                storage.wait_task(StorageTaskWaitRequest::new(
                    lease.clone(),
                    checkpoint.encode().unwrap(),
                )),
            )
            .await
            .unwrap();
            if mode == "deadline" {
                let mut state = storage.state.write_control().await;
                state.tasks.get_mut(&lease.task_id().id()).unwrap().control =
                    StorageTaskControl::try_new(
                        StorageTaskKind::HistoryExpiry,
                        None,
                        Some(Utc::now() - Duration::seconds(1)),
                        StorageTaskExecutionPhase::HistoryAuthority,
                        None,
                    )
                    .unwrap();
            } else {
                fixture_write(
                    &storage,
                    storage.request_task_cancellation(StorageTaskCancellationRequest::new(
                        wait.task(),
                        hubuum_events_core::EventContext::system(),
                    )),
                )
                .await
                .unwrap();
                if mode == "purged" {
                    let mut state = storage.state.write_control().await;
                    state.tasks.remove(&lease.task_id().id());
                    state.task_capacity = state.task_accounting_snapshot().unwrap();
                }
            }
            if mode == "audit" {
                let (sequence, task_count) = {
                    let mut state = storage.state.write_control().await;
                    let before = (state.next_event_sequence, state.tasks.len());
                    state.next_event_sequence = i64::MAX;
                    before
                };
                assert!(
                    storage
                        .recover_history_expiry_task(checkpoint.batch())
                        .await
                        .is_err()
                );
                let mut state = storage.state.write_control().await;
                assert_eq!(
                    state.tasks.len(),
                    task_count,
                    "failed audit must roll back adoption"
                );
                state.next_event_sequence = sequence;
            }
            if mode == "admission" {
                let evidence = fixture_write(
                    &storage,
                    storage.resolve_logical_identity(StorageIdentityTarget::Principal(
                        PrincipalId::new(1).unwrap(),
                    )),
                )
                .await
                .unwrap();
                let Some(StorageIdentitySubject::Principal { identity }) = evidence.subject()
                else {
                    panic!("fixture principal")
                };
                let request = HistoryExpiryTaskSubmission::manual(
                    HistoryLifecycleScan::new(original.manifest().key(), 1000).unwrap(),
                    ApplicationFence::new(
                        before.fence().incarnation(),
                        before.fence().deletion_revision().value() as i64,
                    )
                    .unwrap(),
                    PrincipalId::new(1).unwrap(),
                    identity.known_identity().unwrap(),
                );
                let admission = storage.submit_history_expiry_task(request).await.unwrap();
                assert!(
                    admission.created(),
                    "new cleanup ownership precedes fresh selection"
                );
                assert_eq!(
                    HistoryExpiryTaskPayload::decode(admission.task().request_payload().unwrap())
                        .unwrap()
                        .recovery_batch(),
                    Some(checkpoint.batch())
                );
            }
            let (left, right) = tokio::join!(
                storage.recover_history_expiry_task(checkpoint.batch()),
                storage.recover_history_expiry_task(checkpoint.batch())
            );
            let recovered = left.unwrap().unwrap();
            assert_eq!(right.unwrap().unwrap().id(), recovered.id());
            assert_ne!(recovered.id(), lease.task_id());
            assert_eq!(
                recovered.progress().succeeded(),
                original.removals().len() as i32
            );
            assert!(
                storage
                    .process_history_expiry_task(HistoryExpiryTaskRequest::new(lease.clone(), None))
                    .await
                    .is_err()
            );
            let replacement = claim(&storage).await;
            fixture_write(
                &storage,
                storage.admit_task_execution(StorageTaskExecutionAdmission::new(
                    replacement.clone(),
                    TaskExecutionLimit::from_milliseconds(7_200_000).unwrap(),
                )),
            )
            .await
            .unwrap();
            let resumed = storage
                .process_history_expiry_task(HistoryExpiryTaskRequest::new(
                    replacement.clone(),
                    None,
                ))
                .await
                .unwrap();
            assert_eq!(
                resumed.retained_manifest(&before).unwrap(),
                original.manifest()
            );
            loop {
                let current = storage.capture_history_lifecycle().await.unwrap();
                let next = storage
                    .process_history_expiry_task(
                        HistoryExpiryTaskRequest::new(replacement.clone(), None)
                            .with_protection(protection(&current)),
                    )
                    .await
                    .unwrap();
                if next.finished() {
                    assert_eq!(
                        next.removed_count() as usize,
                        original.manifest().members().len()
                    );
                    break;
                }
            }
            let after = storage.capture_history_lifecycle().await.unwrap();
            let completed = after
                .control()
                .authority()
                .expiry()
                .intent(checkpoint.batch())
                .unwrap();
            assert!(completed.covers(&original));
            assert!(
                original
                    .removals()
                    .iter()
                    .all(|receipt| completed.removals().contains(receipt))
            );
            let count = completed.removals().len() as i32;
            let finished = fixture_write(
                &storage,
                storage.complete_task(StorageTaskCompletion::new(
                    StorageTaskTerminalUpdate::new(
                        replacement,
                        StorageTaskTerminalStatus::Succeeded,
                        StorageTaskResultCounts::try_new(count, count, 0).unwrap(),
                    ),
                    StorageTaskEventInput::new(
                        "succeeded",
                        "Recovered batch protected and complete",
                    ),
                    StorageTaskCompletionPayload::HistoryExpiry,
                )),
            )
            .await
            .unwrap();
            assert!(finished.checkpoint().is_none());
            assert_eq!(
                storage
                    .recover_history_expiry_task(checkpoint.batch())
                    .await
                    .unwrap()
                    .unwrap()
                    .id(),
                finished.id()
            );
            if mode != "purged" {
                assert!(
                    storage.state.read_control().await.tasks[&lease.task_id().id()]
                        .status
                        .is_terminal()
                );
            }
        })
        .await;
    storage.unregister_runtime(runtime).await.unwrap();
}

#[rstest]
#[case::manual_first(true)]
#[case::periodic_first(false)]
#[tokio::test]
async fn concurrent_triggers_share_scope_without_refreshing_original_task(
    #[case] manual_first: bool,
) {
    let storage = MemoryStorage::new();
    let key = seed(
        &storage,
        StorageIdentityHistoryKind::Object,
        "shared admission",
        "closed",
    )
    .await;
    let runtime = storage.register_history_runtime(None).await.unwrap();
    let application = storage
        .capture_application_execution(Some(runtime.owner()))
        .await
        .unwrap();
    let scope = StorageExecutionScope::default().with_application_access(application.clone());
    storage
        .run_in_scope(scope, async {
            let snapshot = storage.capture_history_lifecycle().await.unwrap();
            let principal = fixture_write(
                &storage,
                storage.resolve_logical_identity(StorageIdentityTarget::Principal(
                    PrincipalId::new(1).unwrap(),
                )),
            )
            .await
            .unwrap();
            let Some(StorageIdentitySubject::Principal { identity }) = principal.subject() else {
                panic!("fixture principal")
            };
            let scan = HistoryLifecycleScan::new(key, 1000).unwrap();
            let manual = HistoryExpiryTaskSubmission::manual(
                scan.clone(),
                application.fence(),
                PrincipalId::new(1).unwrap(),
                identity.known_identity().unwrap(),
            );
            let periodic = HistoryExpiryTaskSubmission::periodic(
                scan,
                application.fence(),
                snapshot
                    .control()
                    .authority()
                    .policies()
                    .records()
                    .last()
                    .unwrap(),
            )
            .unwrap();
            let (first, second) = if manual_first {
                (manual, periodic)
            } else {
                (periodic, manual)
            };
            let original = storage.submit_history_expiry_task(first).await.unwrap();
            assert!(original.created());
            let (left, right) = tokio::join!(
                storage.submit_history_expiry_task(second.clone()),
                storage.submit_history_expiry_task(second)
            );
            for result in [left.unwrap(), right.unwrap()] {
                assert!(!result.created());
                assert_eq!(result.task(), original.task());
            }
            assert_eq!(storage.state.read_control().await.tasks.len(), 1);
        })
        .await;
    storage.unregister_runtime(runtime).await.unwrap();
}

#[rstest]
#[case::stale_image("image")]
#[case::changed_principal("principal")]
#[case::unknown_scope("scope")]
#[case::deleted_principal("deleted")]
#[tokio::test]
async fn rejected_shared_admission_leaves_no_task_or_capacity_charge(#[case] mode: &str) {
    let storage = MemoryStorage::new();
    let key = seed(
        &storage,
        StorageIdentityHistoryKind::Object,
        "rejected admission",
        "closed",
    )
    .await;
    let runtime = storage.register_history_runtime(None).await.unwrap();
    let application = storage
        .capture_application_execution(Some(runtime.owner()))
        .await
        .unwrap();
    let scope = StorageExecutionScope::default().with_application_access(application.clone());
    storage
        .run_in_scope(scope, async {
            let evidence = fixture_write(
                &storage,
                storage.resolve_logical_identity(StorageIdentityTarget::Principal(
                    PrincipalId::new(1).unwrap(),
                )),
            )
            .await
            .unwrap();
            let Some(StorageIdentitySubject::Principal { identity }) = evidence.subject() else {
                panic!("fixture principal")
            };
            let principal = if mode == "principal" {
                PrincipalIdentity::generate()
            } else {
                identity.known_identity().unwrap()
            };
            if mode == "deleted" {
                fixture_write(
                    &storage,
                    storage.delete_user(StorageUserDelete::new(
                        UserId::new(1).unwrap(),
                        EventContext::system(),
                    )),
                )
                .await
                .unwrap()
                .into_value();
            }
            let fence = if mode == "image" {
                ApplicationFence::new(Uuid::new_v4(), 0).unwrap()
            } else {
                application.fence()
            };
            let key = if mode == "scope" {
                HistoryPolicyKey::new(
                    key.family(),
                    HistoryPolicyScope::Collection {
                        identity: ResourceIdentity::generate(),
                    },
                )
                .unwrap()
            } else {
                key
            };
            let request = HistoryExpiryTaskSubmission::manual(
                HistoryLifecycleScan::new(key, 1000).unwrap(),
                fence,
                PrincipalId::new(1).unwrap(),
                principal,
            );
            let before = fixture_write(&storage, storage.task_capacity_snapshot())
                .await
                .unwrap();
            assert!(storage.submit_history_expiry_task(request).await.is_err());
            let state = storage.state.read_control().await;
            assert!(state.tasks.is_empty());
            drop(state);
            assert_eq!(
                fixture_write(&storage, storage.task_capacity_snapshot())
                    .await
                    .unwrap(),
                before
            );
        })
        .await;
    storage.unregister_runtime(runtime).await.unwrap();
}
