use super::*;
use diesel::sql_types::Integer;
use hubuum_domain::{PrincipalId, TaskIdentity, UserId};
use hubuum_events_core::EventContext;
use hubuum_task_core::TaskExecutionLimit;
use std::future::{Future, ready};

async fn phase<T>(
    fixture: &Fixture,
    scope: &StorageExecutionScope,
    work: impl Future<Output = Result<T, StorageError>>,
) -> Result<T, StorageError> {
    let owner = fixture
        .storage
        .run_in_scope(
            scope.clone(),
            fixture
                .storage
                .admit_history_publication(HistoryPublicationKind::TaskOutput),
        )
        .await?;
    let result = fixture
        .storage
        .run_in_scope(scope.clone().with_history_read(owner.context()), work)
        .await;
    let settled = owner.after_drained(ready(Ok(())), ready(Ok(()))).await?;
    fixture.storage.settle_history_publication(settled).await?;
    result
}

struct TaskFixture {
    fixture: Fixture,
    collection: i32,
    runtime: RuntimeRegistration,
    scope: StorageExecutionScope,
    task: StorageTask,
    lease: StorageTaskLease,
    task_identity: TaskIdentity,
    user: UserId,
}

#[rstest::rstest]
#[case::kind_outstanding(true)]
#[case::principal_outstanding(false)]
#[tokio::test]
async fn native_expiry_admission_enforces_internal_lane_kind_limits(#[case] kind_limit: bool) {
    use hubuum_storage_postgres::test_support::{TestTaskCreate, create_task};
    let _guard = CONTROL_LOCK.lock().await;
    let test = TaskFixture::new(StorageIdentityHistoryKind::Object).await;
    let current = phase(
        &test.fixture,
        &test.scope,
        test.fixture.storage.task_policy(),
    )
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
    let updated = phase(
        &test.fixture,
        &test.scope,
        test.fixture
            .storage
            .replace_task_policy(StorageTaskPolicyUpdate::new(current.revision(), policy)),
    )
    .await
    .unwrap();
    let result = create_task(
        &test.fixture.owner_pool,
        TestTaskCreate::new(
            StorageTaskKind::HistoryExpiry,
            StorageTaskStatus::Queued,
            test.task.submitted_by().unwrap(),
        )
        .request_payload(test.task.request_payload().cloned()),
    )
    .await;
    phase(
        &test.fixture,
        &test.scope,
        test.fixture
            .storage
            .replace_task_policy(StorageTaskPolicyUpdate::new(
                updated.revision(),
                current.policy().clone(),
            )),
    )
    .await
    .unwrap();
    test.cleanup().await;
    assert!(
        result.is_err(),
        "expiry admission must respect its own capacity"
    );
}

impl TaskFixture {
    async fn new(family: StorageIdentityHistoryKind) -> Self {
        use hubuum_storage_postgres::test_support::{TestTaskCreate, create_task};
        let (fixture, _) = bounded_scan::restore_fixture().await;
        let (fixture, _, collection) = expiry_fixture_on(fixture, family, 32).await;
        let user = fixture
            .storage
            .create_user(StorageUserCreate::new(
                None,
                format!("history_expiry_task_{}", Uuid::new_v4().simple()),
                "test-hash",
                None,
                None,
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value()
            .into_parts()
            .id();
        bounded_scan::schema_collection_policy(&fixture, family, collection).await;
        // Relations need live endpoints before a later source can be appended.
        // The bounded-scan helper creates them through ordinary native triggers.
        bounded_scan::live_edges(&fixture, family, collection).await;
        let runtime = fixture
            .storage
            .register_history_runtime(None)
            .await
            .unwrap();
        let application = fixture
            .storage
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
        let task = create_task(
            &fixture.owner_pool,
            TestTaskCreate::new(
                StorageTaskKind::HistoryExpiry,
                StorageTaskStatus::Queued,
                PrincipalId::new(user.id()).unwrap(),
            )
            .request_payload(Some(serde_json::to_value(payload).unwrap())),
        )
        .await
        .unwrap();
        let scope = StorageExecutionScope::default().with_application_access(application);
        let claim = phase(
            &fixture,
            &scope,
            fixture.storage.claim_next_task(Self::claim_request()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(claim.task().id(), task.id());
        let (_, lease) = claim.into_parts();
        phase(
            &fixture,
            &scope,
            fixture
                .storage
                .admit_task_execution(StorageTaskExecutionAdmission::new(
                    lease.clone(),
                    TaskExecutionLimit::from_milliseconds(7_200_000).unwrap(),
                )),
        )
        .await
        .unwrap();
        let identity = fixture
            .storage
            .resolve_logical_identity(StorageIdentityTarget::Task(task.id()))
            .await
            .unwrap();
        let Some(StorageIdentitySubject::Task { identity }) = identity.subject() else {
            panic!("task identity missing")
        };
        let task_identity = identity.known_identity().unwrap();
        Self {
            fixture,
            collection,
            runtime,
            scope,
            task,
            lease,
            task_identity,
            user,
        }
    }

    fn claim_request() -> StorageTaskClaimRequest {
        StorageTaskClaimRequest::new(
            TaskWorkerId::generate(),
            StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap(),
        )
        .with_kinds(TaskWorkerKinds::new(&[StorageTaskKind::HistoryExpiry]).unwrap())
    }

    async fn process(&self, approval: bool) -> Result<HistoryExpiryTaskCheckpoint, StorageError> {
        self.fixture
            .storage
            .run_in_scope(
                self.scope.clone(),
                self.fixture
                    .storage
                    .process_history_expiry_task(HistoryExpiryTaskRequest::new(
                        self.lease.clone(),
                        approval.then(HistoryExpiryTaskAuthorization::scheduled),
                    )),
            )
            .await
    }

    async fn cleanup(self) {
        let Self {
            fixture,
            collection,
            runtime,
            task,
            task_identity,
            user,
            scope,
            ..
        } = self;
        phase(
            &fixture,
            &scope,
            fixture
                .storage
                .delete_user(StorageUserDelete::new(user, EventContext::system())),
        )
        .await
        .unwrap()
        .into_value();
        fixture.storage.unregister_runtime(runtime).await.unwrap();
        with_transaction(&fixture.owner_pool, async |connection| {
            owner(connection).await?;
            sql_query("DELETE FROM public.tasks WHERE id=$1").bind::<Integer,_>(task.id().id()).execute(connection).await?;
            sql_query("DELETE FROM hubuum_control.task_accounting WHERE task_identity=$1").bind::<SqlUuid,_>(Uuid::from_bytes(*task_identity.as_bytes())).execute(connection).await?;
            sql_query("DELETE FROM hubuum_control.history_expiry_members WHERE intent_uuid IN (SELECT (intent #>> '{manifest,identity}')::uuid FROM hubuum_control.history_lifecycle_control c CROSS JOIN LATERAL jsonb_array_elements(c.material #> '{authority,expiry}') intent WHERE intent #>> '{manifest,run}'=$1)")
                .bind::<Text,_>(Uuid::from_bytes(*task_identity.as_bytes()).to_string()).execute(connection).await?;
            Ok::<_,PostgresStorageError>(())
        }).await.unwrap();
        cleanup_expiry_fixture(fixture, collection, Uuid::nil()).await;
    }
}

#[rstest::rstest]
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
async fn native_task_restart_keeps_original_admitted_membership(
    #[case] family: StorageIdentityHistoryKind,
) {
    let _guard = CONTROL_LOCK.lock().await;
    let mut test = TaskFixture::new(family).await;
    let outcome = async {
        let first = test.process(true).await?;
        let snapshot = test.fixture.storage.capture_history_lifecycle().await?;
        let original = first.retained_manifest(&snapshot).unwrap().clone();
        bounded_scan::append_closed_source(&test.fixture, family, test.collection).await?;
        with_transaction(&test.fixture.owner_pool, async |connection| {
            owner(connection).await?;
            sql_query("UPDATE public.tasks SET lease_expires_at=clock_timestamp()-INTERVAL '1 second' WHERE id=$1")
                .bind::<Integer,_>(test.task.id().id()).execute(connection).await?;
            Ok::<_,PostgresStorageError>(())
        }).await?;
        let recovered = phase(&test.fixture, &test.scope, test.fixture.storage.recover_expired_task_leases(10)).await?;
        assert!(recovered.iter().any(|task| task.id() == test.task.id() && task.status() == StorageTaskStatus::Queued));
        let claim = phase(&test.fixture, &test.scope, test.fixture.storage.claim_next_task(TaskFixture::claim_request())).await?.unwrap();
        let old = test.process(false).await;
        test.lease = claim.into_parts().1;
        let resumed = test.process(false).await?;
        let current = test.fixture.storage.capture_history_lifecycle().await?;
        Ok::<_,StorageError>((first, resumed, original, current, old))
    }.await;
    test.cleanup().await;
    let (first, resumed, original, current, old) = outcome.unwrap();
    assert!(old.is_err(), "recovery must fence the old claim");
    assert_eq!(first, resumed);
    assert_eq!(resumed.retained_manifest(&current).unwrap(), &original);
}

#[rstest::rstest]
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
async fn native_task_cancellation_preserves_retained_history_obligations(
    #[case] family: StorageIdentityHistoryKind,
) {
    let _guard = CONTROL_LOCK.lock().await;
    let test = TaskFixture::new(family).await;
    let outcome = async {
        let checkpoint = test.process(true).await?;
        let before = test.fixture.storage.capture_history_lifecycle().await?;
        let original = checkpoint.retained_manifest(&before).unwrap().clone();
        let waiting = phase(
            &test.fixture,
            &test.scope,
            test.fixture.storage.wait_task(StorageTaskWaitRequest::new(
                test.lease.clone(),
                checkpoint.encode().unwrap(),
            )),
        )
        .await?;
        let cancelled =
            phase(
                &test.fixture,
                &test.scope,
                test.fixture.storage.request_task_cancellation(
                    StorageTaskCancellationRequest::new(waiting.task(), EventContext::system()),
                ),
            )
            .await?;
        let after = test.fixture.storage.capture_history_lifecycle().await?;
        Ok::<_, StorageError>((cancelled, checkpoint, original, after))
    }
    .await;
    test.cleanup().await;
    let (cancelled, checkpoint, original, after) = outcome.unwrap();
    assert_eq!(cancelled.task().status(), StorageTaskStatus::Cancelled);
    assert_eq!(checkpoint.retained_manifest(&after).unwrap(), &original);
}

#[rstest::rstest]
#[case::missing_approval("approval")]
#[case::required_audit("audit")]
#[case::checkpoint_internal_capacity("capacity")]
#[case::checkpoint_kind_capacity("kind_capacity")]
#[case::unknown_checkpoint("unknown")]
#[tokio::test]
async fn native_failed_task_step_rolls_back_admission_and_checkpoint(#[case] mode: &str) {
    let _guard = CONTROL_LOCK.lock().await;
    let test = TaskFixture::new(StorageIdentityHistoryKind::Object).await;
    if mode == "audit" {
        with_transaction(&test.fixture.owner_pool, async |connection| {
            owner(connection).await?;
            connection.batch_execute("CREATE FUNCTION public.history_expiry_task_audit_fault() RETURNS trigger LANGUAGE plpgsql AS $fault$ BEGIN IF NEW.metadata #>> '{lifecycle,name}'='lifecycle.erasure.requested' THEN RAISE EXCEPTION 'required expiry audit fault'; END IF; RETURN NEW; END $fault$; CREATE TRIGGER history_expiry_task_audit_fault BEFORE INSERT ON public.events FOR EACH ROW EXECUTE FUNCTION public.history_expiry_task_audit_fault()").await?;
            Ok::<_,PostgresStorageError>(())
        }).await.unwrap();
    }
    if mode == "unknown" {
        let encoded =
            StorageTaskCheckpoint::try_new("future.history_expiry", 1, serde_json::json!({}))
                .unwrap();
        with_transaction(&test.fixture.owner_pool, async |connection| {
            owner(connection).await?;
            sql_query("UPDATE public.tasks SET task_checkpoint=$2 WHERE id=$1")
                .bind::<Integer, _>(test.task.id().id())
                .bind::<Text, _>(encoded.encoded())
                .execute(connection)
                .await?;
            Ok::<_, PostgresStorageError>(())
        })
        .await
        .unwrap();
    }
    let policy = if matches!(mode, "capacity" | "kind_capacity") {
        let current = phase(
            &test.fixture,
            &test.scope,
            test.fixture.storage.task_policy(),
        )
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
        phase(
            &test.fixture,
            &test.scope,
            test.fixture
                .storage
                .replace_task_policy(StorageTaskPolicyUpdate::new(current.revision(), policy)),
        )
        .await
        .unwrap();
        Some(current.policy().clone())
    } else {
        None
    };
    let before = test
        .fixture
        .storage
        .capture_history_lifecycle()
        .await
        .unwrap();
    let task_before = phase(
        &test.fixture,
        &test.scope,
        test.fixture.storage.get_task_access(test.task.id()),
    )
    .await
    .unwrap()
    .into_parts()
    .0;
    let result = test.process(mode != "approval").await;
    let after = test
        .fixture
        .storage
        .capture_history_lifecycle()
        .await
        .unwrap();
    let task_after = phase(
        &test.fixture,
        &test.scope,
        test.fixture.storage.get_task_access(test.task.id()),
    )
    .await
    .unwrap()
    .into_parts()
    .0;
    if mode == "audit" {
        with_transaction(&test.fixture.owner_pool, async |connection| {
            owner(connection).await?;
            connection.batch_execute("DROP TRIGGER history_expiry_task_audit_fault ON public.events; DROP FUNCTION public.history_expiry_task_audit_fault()").await?;
            Ok::<_,PostgresStorageError>(())
        }).await.unwrap();
    }
    if let Some(policy) = policy {
        let current = phase(
            &test.fixture,
            &test.scope,
            test.fixture.storage.task_policy(),
        )
        .await
        .unwrap();
        phase(
            &test.fixture,
            &test.scope,
            test.fixture
                .storage
                .replace_task_policy(StorageTaskPolicyUpdate::new(current.revision(), policy)),
        )
        .await
        .unwrap();
    }
    test.cleanup().await;
    assert!(result.is_err());
    assert_eq!(after.control(), before.control());
    assert_eq!(task_after.checkpoint(), task_before.checkpoint());
    assert_eq!(task_after.status(), task_before.status());
}

#[rstest::rstest]
#[case::checkpoint("checkpoint")]
#[case::payload("payload")]
#[tokio::test]
async fn native_worker_leaves_future_expiry_formats_unclaimed(#[case] field: &str) {
    let _guard = CONTROL_LOCK.lock().await;
    let test = TaskFixture::new(StorageIdentityHistoryKind::Collection).await;
    with_transaction(&test.fixture.owner_pool, async |connection| {
        owner(connection).await?;
        sql_query("UPDATE public.tasks SET lease_expires_at=clock_timestamp()-INTERVAL '1 second' WHERE id=$1")
            .bind::<Integer,_>(test.task.id().id()).execute(connection).await?;
        Ok::<_,PostgresStorageError>(())
    }).await.unwrap();
    phase(
        &test.fixture,
        &test.scope,
        test.fixture.storage.recover_expired_task_leases(10),
    )
    .await
    .unwrap();
    with_transaction(&test.fixture.owner_pool, async |connection| {
        owner(connection).await?;
        if field == "checkpoint" {
            let encoded = StorageTaskCheckpoint::try_new("hubuum.history_expiry", 2, serde_json::json!({"future":"preserve"})).unwrap();
            sql_query("UPDATE public.tasks SET task_checkpoint=$2 WHERE id=$1")
                .bind::<Integer,_>(test.task.id().id()).bind::<Text,_>(encoded.encoded()).execute(connection).await?;
        } else {
            sql_query("UPDATE public.tasks SET request_payload=jsonb_set(request_payload,'{version}','2'::jsonb) WHERE id=$1")
                .bind::<Integer,_>(test.task.id().id()).execute(connection).await?;
        }
        Ok::<_,PostgresStorageError>(())
    }).await.unwrap();
    let before = phase(
        &test.fixture,
        &test.scope,
        test.fixture.storage.get_task_access(test.task.id()),
    )
    .await
    .unwrap()
    .into_parts()
    .0;
    let claimed = phase(
        &test.fixture,
        &test.scope,
        test.fixture
            .storage
            .claim_next_task(TaskFixture::claim_request()),
    )
    .await;
    let after = phase(
        &test.fixture,
        &test.scope,
        test.fixture.storage.get_task_access(test.task.id()),
    )
    .await
    .unwrap()
    .into_parts()
    .0;
    test.cleanup().await;
    assert!(claimed.unwrap().is_none());
    assert_eq!(after.status(), StorageTaskStatus::Queued);
    assert_eq!(after.checkpoint(), before.checkpoint());
    assert_eq!(after.request_payload(), before.request_payload());
}

#[tokio::test]
async fn vocabulary_upgrade_preserves_every_existing_task_limit() {
    let _guard = CONTROL_LOCK.lock().await;
    let (fixture, target) = bounded_scan::restore_fixture().await;
    // Exercise only the vocabulary portion of the real migration. The full
    // lifecycle rollback has independent publication/history refusal guards;
    // this test does not bypass those guards to claim rollback is safe.
    let down = include_str!("../../migrations/2026-11-03-000005_history_lifecycle/down.sql")
        .split_once("-- No retained eighth-kind work exists after the rollback guard above.\n")
        .unwrap()
        .1
        .split_once("CREATE OR REPLACE FUNCTION public.hubuum_manage_logical_identity()")
        .unwrap()
        .0;
    let up = include_str!("../../migrations/2026-11-03-000005_history_lifecycle/up.sql")
        .split_once("-- Coordinated closed task vocabulary upgrade.")
        .unwrap()
        .1;
    // The opening comment continues onto the next lines; retain its SQL comment.
    let up = format!("-- Coordinated closed task vocabulary upgrade.{up}");
    with_transaction(&fixture.owner_pool, async |connection| {
        target.validate_connection(connection).await?;
        owner(connection).await?;
        connection.batch_execute("SAVEPOINT task_vocabulary_fixture").await?;
        let existing = sql_query("SELECT to_jsonb(EXISTS(SELECT 1 FROM public.tasks WHERE kind='history_expiry') OR EXISTS(SELECT 1 FROM hubuum_control.task_accounting WHERE kind='history_expiry')) AS material").get_result::<Material>(connection).await?.material;
        assert_eq!(existing, Value::Bool(false), "vocabulary fixture must not erase retained work");
        connection.batch_execute(down).await?;
        connection.batch_execute("UPDATE hubuum_control.task_policy SET initialized=true,revision=revision+1,
            policy=jsonb_set(jsonb_set(jsonb_set(jsonb_set(policy,
                '{deployment,executing}','3'),'{internal,outstanding}','71'),
                '{kinds,0,principal_outstanding}','11'),'{scheduling,weights,0,weight}','7');
            SELECT public.hubuum_validate_task_policy(policy) FROM hubuum_control.task_policy WHERE singleton").await?;
        let before = sql_query("SELECT jsonb_build_object('revision',revision,'policy',policy) AS material FROM hubuum_control.task_policy WHERE singleton").get_result::<Material>(connection).await?.material;
        connection.batch_execute(&up).await?;
        let after = sql_query("SELECT jsonb_build_object('revision',revision,'policy',policy) AS material FROM hubuum_control.task_policy WHERE singleton").get_result::<Material>(connection).await?.material;
        let mut preserved = after["policy"].clone();
        let extra = preserved["kinds"].as_array_mut().unwrap().pop().unwrap();
        let weight = preserved["scheduling"]["weights"].as_array_mut().unwrap().pop().unwrap();
        assert_eq!(preserved, before["policy"]);
        assert_eq!(after["revision"].as_i64(), before["revision"].as_i64().map(|revision| revision+1));
        assert_eq!(extra, serde_json::json!({"kind":"history_expiry","capacity":{"outstanding":64,"executing":1,"resident_bytes":67108864},"principal_outstanding":1}));
        assert_eq!(weight, serde_json::json!({"kind":"history_expiry","weight":1}));
        connection.batch_execute("ROLLBACK TO SAVEPOINT task_vocabulary_fixture").await?;
        Ok::<_,PostgresStorageError>(())
    }).await.unwrap();
}
