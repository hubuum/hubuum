use super::*;
use diesel::OptionalExtension;
use futures_util::TryFutureExt;
use std::time::Duration as WaitDuration;
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout};

#[derive(QueryableByName)]
struct Backend {
    #[diesel(sql_type = Integer)]
    pid: i32,
}

async fn waiting_backend(observer: &mut PostgresConnection, blocker: i32) -> i32 {
    timeout(WaitDuration::from_secs(5), async {
        loop {
            let waiter = sql_query(
                "SELECT pid FROM pg_stat_activity
                 WHERE datname=current_database() AND backend_type='client backend'
                   AND $1=ANY(pg_blocking_pids(pid))",
            )
            .bind::<Integer, _>(blocker)
            .get_result::<Backend>(observer)
            .await
            .optional()
            .unwrap();
            if let Some(waiter) = waiter {
                return waiter.pid;
            }
            sleep(WaitDuration::from_millis(5)).await;
        }
    })
    .await
    .expect("expected to observe the competing database transaction")
}

async fn commit_in_order(
    test: &TaskFixture,
    first: impl Future<Output = Result<(), StorageError>>,
    second: impl Future<Output = Result<(), StorageError>>,
) -> StorageError {
    tokio::pin!(first, second);
    let mut observer = test.fixture.owner_pool.get().await.unwrap();
    let (locked, gate_pid) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let gate = with_transaction(&test.fixture.owner_pool, async move |gate| {
        test.restore_target.validate_connection(gate).await?;
        owner(gate).await?;
        gate.batch_execute("LOCK TABLE public.events IN ACCESS EXCLUSIVE MODE")
            .await?;
        let blocker = sql_query("SELECT pg_backend_pid() AS pid")
            .get_result::<Backend>(gate)
            .await?
            .pid;
        locked.send(blocker).unwrap();
        released.await.unwrap();
        Ok::<_, PostgresStorageError>(())
    });
    let compete = async {
        let blocker = gate_pid.await.unwrap();
        // The winner holds history authority while waiting for its required
        // audit append. Start the second operation only after observing this.
        let winner = tokio::select! {
            result = &mut first => panic!("first operation escaped the audit lock: {result:?}"),
            pid = waiting_backend(&mut observer, blocker) => pid,
        };
        tokio::select! {
            result = &mut first => panic!("first operation escaped the audit lock: {result:?}"),
            result = &mut second => panic!("second operation escaped the winner: {result:?}"),
            _ = waiting_backend(&mut observer, winner) => {}
        }
        release.send(()).unwrap();
        timeout(WaitDuration::from_secs(5), async {
            tokio::join!(first, second)
        })
        .await
        .expect("both operations must finish after the audit lock is released")
    };
    let (gate, (winner, loser)) = tokio::join!(gate, compete);
    gate.unwrap();
    winner.unwrap();
    loser.expect_err("the queued operation must recheck the committed authority")
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
async fn hold_and_first_copy_removal_serialize(
    #[case] family: StorageIdentityHistoryKind,
    #[values(true, false)] hold_first: bool,
) {
    let _guard = CONTROL_LOCK.lock().await;
    let test = TaskFixture::new(family).await;
    let result = AssertUnwindSafe(async {
        let checkpoint = test.process(true).await.unwrap();
        let storage = &test.fixture.storage;
        let admitted = storage.capture_history_lifecycle().await.unwrap();
        let manifest = checkpoint.retained_manifest(&admitted).unwrap();
        let target = manifest.members()[0].target();
        assert_eq!(copy_count(&test, family, target).await, 1);
        let hold = HistoryRetentionHold::new(
            Uuid::new_v4(),
            manifest.key(),
            admitted.observed_at(),
            None,
            PrincipalIdentity::generate(),
        )
        .unwrap();
        let change = admitted
            .control()
            .create_hold(hold, admitted.fence())
            .unwrap();
        let expected_hold = change.next().clone();
        let commit_hold = storage
            .run_in_scope(test.scope.clone(), storage.commit_history_lifecycle(change))
            .map_ok(|_| ());
        let remove_copy = remove(&test, protection(&admitted)).map_ok(|_| ());
        if hold_first {
            let error = commit_in_order(&test, commit_hold, remove_copy).await;
            assert!(
                error
                    .message()
                    .contains("Current policy, hold or dependency blocks history removal"),
                "{error}"
            );
        } else {
            let error = commit_in_order(&test, remove_copy, commit_hold).await;
            assert_eq!(error.kind(), StorageErrorKind::Conflict, "{error}");
        }
        let after = storage.capture_history_lifecycle().await.unwrap();
        let retained = after
            .control()
            .authority()
            .expiry()
            .intent(checkpoint.batch())
            .unwrap();
        assert_eq!(retained.manifest(), manifest);
        let copies = copy_count(&test, family, target).await;
        if hold_first {
            assert_eq!(after.control(), &expected_hold);
            assert_eq!(copies, 1);
            assert!(retained.removals().is_empty());
        } else {
            assert_eq!(
                after.control().authority().hold_revisions(),
                admitted.control().authority().hold_revisions()
            );
            assert_eq!(copies, 0);
            let [receipt] = retained.removals() else {
                panic!("expected exactly one first-removal receipt")
            };
            assert_eq!(receipt.target(), target);
        }
    })
    .catch_unwind()
    .await;
    test.cleanup().await;
    if let Err(panic) = result {
        resume_unwind(panic);
    }
}
