use super::*;
use diesel::sql_types::Bool;
use futures_util::TryFutureExt;
use hubuum_storage_postgres::test_support::integration_test_restore_database_url;
use std::time::Duration as WaitDuration;
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout};

#[derive(QueryableByName)]
struct Backend {
    #[diesel(sql_type = Integer)]
    pid: i32,
}

async fn runtime_session(test: &TaskFixture) -> (PostgresStorage, i32) {
    let pool = bounded_scan::restore_pool(integration_test_restore_database_url(), 1);
    let mut connection = pool.get().await.unwrap();
    test.restore_target
        .validate_connection(&mut connection)
        .await
        .unwrap();
    let pid = sql_query("SELECT pg_backend_pid() AS pid")
        .get_result::<Backend>(&mut connection)
        .await
        .unwrap()
        .pid;
    drop(connection);
    (PostgresStorage::unobserved(pool), pid)
}

async fn wait_for_blocker(observer: &mut PostgresConnection, waiter: i32, blocker: i32) {
    #[derive(QueryableByName)]
    struct Wait {
        #[diesel(sql_type = Bool)]
        blocked: bool,
    }
    timeout(WaitDuration::from_secs(5), async {
        loop {
            let wait = sql_query("SELECT $2=ANY(pg_blocking_pids($1)) AS blocked")
                .bind::<Integer, _>(waiter)
                .bind::<Integer, _>(blocker)
                .get_result::<Wait>(observer)
                .await
                .unwrap();
            if wait.blocked {
                break;
            }
            sleep(WaitDuration::from_millis(5)).await;
        }
    })
    .await
    .expect("expected to observe the competing database transaction")
}

async fn commit_in_order(
    test: &TaskFixture,
    (first_pid, first): (i32, impl Future<Output = Result<(), StorageError>>),
    (second_pid, second): (i32, impl Future<Output = Result<(), StorageError>>),
) -> Result<(), StorageError> {
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
        tokio::select! {
            result = &mut first => panic!("first operation escaped the audit lock: {result:?}"),
            () = wait_for_blocker(&mut observer, first_pid, blocker) => {}
        };
        tokio::select! {
            result = &mut first => panic!("first operation escaped the audit lock: {result:?}"),
            result = &mut second => panic!("second operation escaped the winner: {result:?}"),
            () = wait_for_blocker(&mut observer, second_pid, first_pid) => {}
        }
        release.send(()).unwrap();
        timeout(WaitDuration::from_secs(5), async {
            tokio::join!(first, second)
        })
        .await
        .expect("both operations must finish after the audit lock is released")
    };
    let (gate, (first, second)) = tokio::join!(gate, compete);
    gate.unwrap();
    first.unwrap();
    second
}

#[derive(Clone, Copy, Debug)]
enum AdminChange {
    Hold,
    DisableProspectively,
    DisableBeforeAnchor,
    DisableThroughAnchor,
    ReissueThroughAnchor,
}

impl AdminChange {
    fn blocks_removal(self) -> bool {
        matches!(
            self,
            Self::Hold | Self::DisableThroughAnchor | Self::ReissueThroughAnchor
        )
    }

    async fn prepare(
        self,
        test: &TaskFixture,
        snapshot: &HistoryLifecycleSnapshot,
        manifest: &HistoryExpiryManifest,
    ) -> HistoryLifecycleControlChange {
        let principal = PrincipalIdentity::generate();
        let member = &manifest.members()[0];
        let anchor = member.anchor().at();
        assert!(anchor < snapshot.observed_at());
        let application = match self {
            Self::Hold => {
                let hold = HistoryRetentionHold::new(
                    Uuid::new_v4(),
                    manifest.key(),
                    snapshot.observed_at(),
                    None,
                    principal,
                )
                .unwrap();
                return snapshot
                    .control()
                    .create_hold(hold, snapshot.fence())
                    .unwrap();
            }
            Self::DisableProspectively => HistoryPolicyApplication::Prospective,
            Self::DisableBeforeAnchor => HistoryPolicyApplication::Retrospective {
                closed_through: anchor - Duration::microseconds(1),
            },
            Self::DisableThroughAnchor | Self::ReissueThroughAnchor => {
                HistoryPolicyApplication::Retrospective {
                    closed_through: anchor,
                }
            }
        };
        let policies = snapshot.control().authority().policies();
        let source = &member.policies()[0];
        let binding = if matches!(self, Self::ReissueThroughAnchor) {
            policies.exact(source).unwrap().binding().clone()
        } else {
            HistoryPolicyBinding::DisabledExpiry
        };
        let proposal = HistoryPolicyChange::new(
            source.key(),
            policies.current_revision(source.key()),
            binding,
            application,
        )
        .unwrap();
        let issued;
        let (current, review) = if matches!(
            proposal.application(),
            HistoryPolicyApplication::Retrospective { .. }
        ) {
            let preview = HistoryPolicyPreview::issue(
                principal,
                proposal.clone(),
                snapshot.fence(),
                snapshot.observed_at(),
                HistoryPreviewImpact::new(HistoryPreviewCoverage::Bounded, 0, 0, Vec::new())
                    .unwrap(),
            )
            .unwrap();
            let reference = preview.reference().clone();
            let issuance = snapshot
                .control()
                .issue_preview(preview, snapshot.fence())
                .unwrap();
            let storage = &test.fixture.storage;
            issued = storage
                .run_in_scope(
                    test.scope.clone(),
                    storage.commit_history_lifecycle(issuance),
                )
                .await
                .unwrap();
            (&issued, Some(reference))
        } else {
            (snapshot, None)
        };
        current
            .control()
            .apply(
                HistoryPolicyApplyRequest::new(proposal, review),
                principal,
                current.fence(),
                current.observed_at(),
            )
            .unwrap()
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
async fn admin_change_and_first_copy_removal_serialize(
    #[case] family: StorageIdentityHistoryKind,
    #[values(
        AdminChange::Hold,
        AdminChange::DisableProspectively,
        AdminChange::DisableBeforeAnchor,
        AdminChange::DisableThroughAnchor,
        AdminChange::ReissueThroughAnchor
    )]
    edit: AdminChange,
    #[values(true, false)] edit_first: bool,
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
        let change = edit.prepare(&test, &admitted, manifest).await;
        let expected_control = if edit_first {
            change.next()
        } else {
            change.expected()
        }
        .clone();
        let (administrator, admin_pid) = runtime_session(&test).await;
        let (worker, worker_pid) = runtime_session(&test).await;
        let commit = administrator
            .run_in_scope(
                test.scope.clone(),
                administrator.commit_history_lifecycle(change),
            )
            .map_ok(|_| ());
        let proof = protection(&admitted);
        let request =
            HistoryExpiryTaskRequest::new(test.lease.clone(), None).with_protection(proof.clone());
        let remove_copy = worker
            .run_in_scope(
                test.scope.clone(),
                worker.process_history_expiry_task(request),
            )
            .map_ok(|_| ());
        let admin = (admin_pid, commit);
        let removal = (worker_pid, remove_copy);
        if edit_first {
            let removal = commit_in_order(&test, admin, removal).await;
            if edit.blocks_removal() {
                let error = removal.unwrap_err();
                assert!(
                    error
                        .message()
                        .contains("Current policy, hold or dependency blocks history removal"),
                    "{error}"
                );
            } else {
                removal.unwrap();
            }
        } else {
            let error = commit_in_order(&test, removal, admin).await.unwrap_err();
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
        if edit_first && edit.blocks_removal() {
            assert_eq!(after.control(), &expected_control);
            assert_eq!(copies, 1);
            assert!(retained.removals().is_empty());
        } else {
            let authority = after.control().authority();
            assert_eq!(
                authority.policies(),
                expected_control.authority().policies()
            );
            assert_eq!(
                authority.hold_revisions(),
                expected_control.authority().hold_revisions()
            );
            assert_eq!(copies, 0);
            let [receipt] = retained.removals() else {
                panic!("expected exactly one first-removal receipt")
            };
            assert_eq!(receipt.target(), target);
            assert_eq!(receipt.checkpoint(), proof.checkpoint());
        }
    })
    .catch_unwind()
    .await;
    test.cleanup().await;
    if let Err(panic) = result {
        resume_unwind(panic);
    }
}
