//! Real signed full-history restore and exact-copy replay through the coordinator.
use super::*;
use futures_util::FutureExt;
use hubuum::test_support::history_replay_fixture_protection;
use hubuum_domain::{ClassId, CollectionId, ObjectId, PrincipalIdentity};
use hubuum_storage_core::StorageIdentityHistoryKind as Family;
use std::os::unix::fs::PermissionsExt;
use std::panic::{AssertUnwindSafe, resume_unwind};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

mod race;
use race::{replay_with_queued_restore, restore_with_queued_replay};

#[derive(Default)]
struct ReplayTrace {
    operations: Mutex<Vec<String>>,
    data_replaced: Notify,
}

impl StorageObserver for ReplayTrace {
    fn operation_finished(&self, observation: &StorageObservation) {
        if observation.result() != "ok" {
            return;
        }
        if observation.operation() == "apply_restore_replay" {
            self.data_replaced.notify_waiters();
        }
        if matches!(
            observation.operation(),
            "reapply_history_expiry"
                | "capture_control_checkpoint"
                | "release_application_exposure"
        ) {
            self.operations
                .lock()
                .unwrap()
                .push(observation.operation().into());
        }
    }
}

impl ReplayTrace {
    fn assert_captures(&self, members: usize, existing_receipts: usize) {
        let operations = std::mem::take(&mut *self.operations.lock().unwrap());
        let copies: Vec<_> = operations
            .iter()
            .enumerate()
            .filter_map(|(i, operation)| (operation == "reapply_history_expiry").then_some(i))
            .collect();
        assert_eq!(copies.len(), members);
        assert!(members >= 3, "Exercise repeated replay, not a single copy");
        for (index, pair) in copies.windows(2).enumerate() {
            assert_eq!(
                operations[pair[0] + 1..pair[1]]
                    .iter()
                    .any(|operation| operation == "capture_control_checkpoint"),
                index >= existing_receipts,
                "Only new first receipts require capture between copies: {operations:?}"
            );
        }
        let release = operations
            .iter()
            .position(|operation| operation == "release_application_exposure")
            .expect("Successful restore must release quarantine");
        assert!(
            operations[copies.last().unwrap() + 1..release]
                .iter()
                .any(|operation| operation == "capture_control_checkpoint"),
            "Final release must still capture the complete control image"
        );
    }
}

#[derive(Clone, Copy, Debug)]
enum HistorySource {
    Collection(CollectionId),
    Class(ClassId),
    Object(ObjectId),
}

impl HistorySource {
    fn target(self) -> StorageIdentityTarget {
        let (kind, id) = match self {
            Self::Collection(id) => (StorageIdentityResourceKind::Collection, id.into()),
            Self::Class(id) => (StorageIdentityResourceKind::Class, id.into()),
            Self::Object(id) => (StorageIdentityResourceKind::Object, id.into()),
        };
        StorageIdentityTarget::Resource { kind, id }
    }

    async fn update(self, fixture: &RestoreContractFixture, value: &str) {
        match self {
            Self::Collection(id) => {
                fixture
                    .update_collection_description(id, value.into())
                    .await
            }
            Self::Class(id) => fixture.update_class_description(id, value.into()).await,
            Self::Object(id) => fixture.rename_object(id, value).await,
        }
        .unwrap();
    }
}

async fn current_identity(
    fixture: &RestoreContractFixture,
    target: StorageIdentityTarget,
) -> StorageRecordIdentity {
    fixture
        .storage()
        .resolve_logical_identity(target)
        .await
        .unwrap()
        .subject()
        .and_then(StorageIdentitySubject::record_identity)
        .unwrap()
}

async fn assert_closed_object_present(
    fixture: &RestoreContractFixture,
    object: &StorageObject,
    expected: StorageRecordIdentity,
) {
    let rows = fixture
        .storage()
        .list_object_history(StorageObjectHistoryListQuery::new(
            object.id(),
            object.class_id(),
            QueryOptions::empty(),
            StorageHistoryCollectionScope::All,
        ))
        .await
        .unwrap()
        .into_parts()
        .0;
    assert!(
        rows.into_iter().any(|row| {
            let (_, metadata) = row.into_parts();
            metadata.valid_to().is_some()
                && metadata
                    .logical_identity()
                    .and_then(StorageIdentityEvidence::subject)
                    .and_then(StorageIdentitySubject::record_identity)
                    == Some(expected)
        }),
        "the sibling class must retain its closed object version"
    );
}

#[rstest]
#[case::postgres_restore_first(StorageBackendKind::Postgres, false)]
#[case::postgres_expiry_first(StorageBackendKind::Postgres, true)]
#[case::memory(StorageBackendKind::Memory, false)]
#[tokio::test]
async fn full_history_restore_replays_exact_expiry_and_preserves_first_receipts(
    #[case] kind: StorageBackendKind,
    #[case] expiry_first: bool,
    #[values(Family::Collection, Family::Class, Family::Object)] family: Family,
    #[values(false, true)] size: bool,
) {
    let _guard = RESTORE_TEST_LOCK.lock().await;
    let pool = postgres_test_pool_with_timeout(&database_url(), 8, DEFAULT_DB_STATEMENT_TIMEOUT_MS);
    let trace = Arc::new(ReplayTrace::default());
    let fixture = RestoreContractFixture::new_with_observer(kind, pool.clone(), trace.clone())
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
    let actor = PrincipalIdentity::generate();
    let result = AssertUnwindSafe(async {
        let class = fixture.create_class("expiry_restore_scope").await.unwrap();
        let versions = [
            "expiry_original",
            "expiry_second",
            "expiry_third",
            "expiry_live",
        ];
        let source = match family {
            Family::Collection => HistorySource::Collection(class.collection_id()),
            Family::Class => HistorySource::Class(class.id()),
            Family::Object => HistorySource::Object(
                fixture
                    .create_object(class.id(), versions[0])
                    .await
                    .unwrap()
                    .id(),
            ),
            _ => unreachable!("unsupported restore fixture family"),
        };
        let subject = source.target();
        let scope = match source {
            HistorySource::Collection(_) => HistoryPolicyScope::Collection {
                identity: current_identity(&fixture, subject).await.resource(),
            },
            HistorySource::Class(_) | HistorySource::Object(_) => HistoryPolicyScope::Class {
                identity: current_identity(&fixture, HistorySource::Class(class.id()).target())
                    .await
                    .resource(),
            },
        };
        let key = HistoryPolicyKey::new(family, scope).unwrap();
        let proposal = HistoryPolicyChange::new(
            key,
            before
                .control()
                .authority()
                .policies()
                .current_revision(key),
            HistoryPolicyBinding::Expire {
                trigger: if size {
                    HistoryExpiryTrigger::LogicalSize {
                        upper_logical_bytes: HistoryLogicalBytes::new(1).unwrap(),
                        lower_logical_bytes: HistoryLogicalBytes::new(0).unwrap(),
                    }
                } else {
                    HistoryExpiryTrigger::Age {
                        age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
                        deleted_age_seconds: None,
                    }
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
                        actor,
                        before.fence(),
                        before.observed_at(),
                    )
                    .unwrap(),
            )
            .await
            .unwrap();
        // Close a version in the same collection but outside the governing class.
        let sibling = if let HistorySource::Object(_) = source {
            let other = fixture.create_class("expiry_sibling_class").await.unwrap();
            let object = fixture
                .create_object(other.id(), "sibling_original")
                .await
                .unwrap();
            let original =
                current_identity(&fixture, HistorySource::Object(object.id()).target()).await;
            fixture
                .rename_object(object.id(), "sibling_live")
                .await
                .unwrap();
            Some((object, original))
        } else {
            None
        };
        let mut closed = Vec::new();
        for next in &versions[1..] {
            closed.push(current_identity(&fixture, subject).await);
            source.update(&fixture, next).await;
        }
        let live = current_identity(&fixture, subject).await;
        let backup = fixture.capture(true).await.unwrap();
        let runtime = storage.register_history_runtime(None).await.unwrap();
        let application = storage
            .capture_application_execution(Some(runtime.owner()))
            .await
            .unwrap();
        let snapshot = storage.capture_history_lifecycle().await.unwrap();
        let admitted = storage
            .run_in_scope(
                StorageExecutionScope::default().with_application_access(application),
                storage.admit_history_expiry(
                    HistoryExpiryAdmissionRequest::new(
                        Uuid::new_v4(),
                        Uuid::new_v4(),
                        HistoryExpiryOrigin::Scheduled {
                            initiator: Some(actor),
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
            .expect("closed fixture versions are eligible")
            .clone();
        batch = Some(manifest.identity());
        assert_eq!(manifest.members().len(), closed.len());
        for (identity, name) in closed.into_iter().zip(versions) {
            let member = manifest
                .members()
                .iter()
                .find(|member| member.target() == identity)
                .expect("every closed version is in the manifest");
            assert_eq!(member.family(), family);
            if let HistorySource::Object(id) = source {
                let facts = member
                    .object_authorization()
                    .expect("object expiry retains the original authorization facts");
                assert_eq!(facts.target(), identity);
                assert_eq!(facts.object_id(), id);
                assert_eq!(facts.class_id(), class.id());
                assert_eq!(facts.collection_id(), class.collection_id());
                assert_eq!(facts.name(), name);
            }
        }
        let target = manifest.members()[0].target();
        let request = HistoryExpiryReapplication::new(
            history_replay_fixture_protection(storage, &repository, &key_file)
                .await
                .unwrap(),
            manifest.identity(),
            target,
        )
        .unwrap();
        assert!(
            storage
                .reapply_history_expiry(request.clone())
                .await
                .is_err(),
            "direct contract call cannot replay a released application"
        );
        if expiry_first {
            let protection = fixture
                .prepare_restore_during_history_recovery(backup.clone(), &repository, &key_file)
                .await
                .unwrap();
            let request =
                HistoryExpiryReapplication::new(protection, manifest.identity(), target).unwrap();
            trace.operations.lock().unwrap().clear();
            replay_with_queued_restore(
                &pool,
                fixture.resume_restore_with_recovery(&repository, &key_file),
                request,
                &trace,
            )
            .await;
        } else {
            trace.operations.lock().unwrap().clear();
            restore_with_queued_replay(
                kind,
                &pool,
                fixture.restore_with_recovery(backup.clone(), &repository, &key_file),
                request,
            )
            .await;
        }
        trace.assert_captures(manifest.members().len(), usize::from(expiry_first));
        assert_eq!(current_identity(&fixture, subject).await, live);
        if let Some((object, original)) = &sibling {
            assert_closed_object_present(&fixture, object, *original).await;
        }
        let completed = storage.capture_history_lifecycle().await.unwrap();
        let completed_intent = completed
            .control()
            .authority()
            .expiry()
            .intent(manifest.identity())
            .unwrap();
        assert_eq!(completed_intent.manifest(), &manifest);
        let receipts = completed_intent.removals().to_vec();
        assert_eq!(receipts.len(), manifest.members().len());
        assert_ne!(
            completed.fence().incarnation(),
            manifest.fence().incarnation()
        );
        let request = HistoryExpiryReapplication::new(
            history_replay_fixture_protection(storage, &repository, &key_file)
                .await
                .unwrap(),
            manifest.identity(),
            target,
        )
        .unwrap();
        assert!(request.has_receipt());
        trace.operations.lock().unwrap().clear();
        restore_with_queued_replay(
            kind,
            &pool,
            fixture.restore_with_recovery(backup, &repository, &key_file),
            request,
        )
        .await;
        trace.assert_captures(manifest.members().len(), manifest.members().len());
        assert_eq!(current_identity(&fixture, subject).await, live);
        if let Some((object, original)) = &sibling {
            assert_closed_object_present(&fixture, object, *original).await;
        }
        let repeated = storage.capture_history_lifecycle().await.unwrap();
        let repeated_intent = repeated
            .control()
            .authority()
            .expiry()
            .intent(manifest.identity())
            .unwrap();
        assert_eq!(repeated_intent.manifest(), &manifest);
        assert_eq!(repeated_intent.removals(), receipts);
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
