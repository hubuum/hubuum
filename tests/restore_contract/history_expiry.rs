//! Real signed full-history restore and exact-copy replay through the coordinator.
use super::*;
use futures_util::FutureExt;
use hubuum_domain::PrincipalIdentity;
use std::os::unix::fs::PermissionsExt;
use std::panic::{AssertUnwindSafe, resume_unwind};

#[rstest]
#[tokio::test]
async fn full_history_restore_replays_exact_expiry_and_preserves_first_receipts(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    let _guard = RESTORE_TEST_LOCK.lock().await;
    let pool = postgres_test_pool_with_timeout(&database_url(), 8, DEFAULT_DB_STATEMENT_TIMEOUT_MS);
    let fixture = RestoreContractFixture::new(kind, pool.clone())
        .await
        .unwrap();
    let storage = fixture.storage();
    let before = storage.capture_history_lifecycle().await.unwrap();
    let before_deletion = storage.read_deletion_control().await.unwrap();
    let root = std::env::temp_dir().join(format!("hubuum-567-expiry-restore-{}", Uuid::new_v4()));
    let repository = root.join("repository");
    std::fs::create_dir_all(&repository).unwrap();
    std::fs::set_permissions(&repository, std::fs::Permissions::from_mode(0o700)).unwrap();
    let key_file = root.join("key");
    std::fs::write(&key_file, [79; 32]).unwrap();
    std::fs::set_permissions(&key_file, std::fs::Permissions::from_mode(0o600)).unwrap();
    fixture
        .initialize_recovery(&repository, &key_file)
        .await
        .unwrap();
    let mut batch = None;
    let result = AssertUnwindSafe(async {
        let class = fixture.create_class("expiry_restore_scope").await.unwrap();
        let collection = storage
            .resolve_logical_identity(StorageIdentityTarget::Resource {
                kind: StorageIdentityResourceKind::Collection,
                id: class.collection_id().into(),
            })
            .await
            .unwrap()
            .subject()
            .and_then(StorageIdentitySubject::record_identity)
            .unwrap()
            .resource();
        let key = HistoryPolicyKey::new(
            StorageIdentityHistoryKind::Collection,
            HistoryPolicyScope::Collection {
                identity: collection,
            },
        )
        .unwrap();
        let proposal = HistoryPolicyChange::new(
            key,
            before
                .control()
                .authority()
                .policies()
                .current_revision(key),
            HistoryPolicyBinding::Expire {
                trigger: HistoryExpiryTrigger::Age {
                    age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
                    deleted_age_seconds: None,
                },
                minimum_age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
            },
            HistoryPolicyApplication::Prospective,
        )
        .unwrap();
        storage
            .commit_history_lifecycle(
                before
                    .control()
                    .apply(
                        HistoryPolicyApplyRequest::new(proposal, None),
                        PrincipalIdentity::generate(),
                        before.fence(),
                        before.observed_at(),
                    )
                    .unwrap(),
            )
            .await
            .unwrap();
        fixture.close_collection_history().await.unwrap();
        let backup = fixture.capture(true).await.unwrap();
        let runtime = storage.register_history_runtime(None).await.unwrap();
        let application = storage
            .capture_application_execution(Some(runtime.owner()))
            .await
            .unwrap();
        let fence = application.fence();
        let snapshot = storage.capture_history_lifecycle().await.unwrap();
        let admitted = storage
            .run_in_scope(
                StorageExecutionScope::default().with_application_access(application),
                storage.admit_history_expiry(
                    HistoryExpiryAdmissionRequest::new(
                        Uuid::new_v4(),
                        Uuid::new_v4(),
                        HistoryExpiryOrigin::Scheduled {
                            initiator: None,
                            policy: None,
                        },
                        HistoryLifecycleScan::new(key, 1000).unwrap(),
                        snapshot.fence(),
                    )
                    .unwrap(),
                ),
            )
            .await;
        storage.unregister_runtime(runtime).await.unwrap();
        let manifest = admitted
            .unwrap()
            .manifest()
            .expect("real closed collection is eligible")
            .clone();
        batch = Some(manifest.identity());
        let protected = storage.capture_history_lifecycle().await.unwrap();
        let request = HistoryExpiryReapplication::new(
            HistoryExposureProtection::new(
                protected.control().authority().clone(),
                fence,
                DeletionCheckpointIdentity::new("a".repeat(64)).unwrap(),
            ),
            manifest.identity(),
            manifest.members()[0].target(),
        )
        .unwrap();
        assert!(
            storage.reapply_history_expiry(request).await.is_err(),
            "direct contract call cannot replay a released application"
        );
        fixture
            .restore_with_recovery(backup.clone(), &repository, &key_file)
            .await
            .unwrap();
        let completed = storage.capture_history_lifecycle().await.unwrap();
        let receipts = completed
            .control()
            .authority()
            .expiry()
            .intent(manifest.identity())
            .unwrap()
            .removals()
            .to_vec();
        assert_eq!(receipts.len(), manifest.members().len());
        assert_ne!(
            completed.fence().incarnation(),
            manifest.fence().incarnation()
        );
        fixture
            .restore_with_recovery(backup, &repository, &key_file)
            .await
            .unwrap();
        let repeated = storage.capture_history_lifecycle().await.unwrap();
        assert_eq!(
            repeated
                .control()
                .authority()
                .expiry()
                .intent(manifest.identity())
                .unwrap()
                .removals(),
            receipts
        );
        let application = storage.read_application_control().await.unwrap();
        assert!(application.is_released_for(&storage.read_deletion_control().await.unwrap()));
    })
    .catch_unwind()
    .await;
    fixture
        .resume_restore_with_recovery(&repository, &key_file)
        .await
        .expect("settle any committed fixture restore before replacing its source image");
    let restored = fixture
        .restore_original_with_recovery(&repository, &key_file)
        .await;
    if kind == StorageBackendKind::Postgres {
        // The isolated restore database owns this exact fixture obligation.
        // CAS teardown cannot erase another test's authority or productive owners.
        let expected = storage.capture_history_lifecycle().await.unwrap();
        let deletion = storage.read_deletion_control().await.unwrap();
        let application = storage.read_application_control().await.unwrap();
        super::history_remapping::restore_fixture_authority(
            &pool,
            before.control(),
            &before_deletion,
            expected.control(),
            &deletion,
            batch,
            &application,
        )
        .await
        .unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
    if let Err(panic) = result {
        resume_unwind(panic);
    }
    restored.unwrap();
}
