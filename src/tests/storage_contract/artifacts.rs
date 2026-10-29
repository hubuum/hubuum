use super::task_control::Fixture;
use super::*;
use chrono::Utc;
use hubuum_storage_core::artifacts::*;
use hubuum_storage_core::{StorageExportTaskArtifactContent, TaskCapacityStorage};
use rstest::rstest;

async fn stage(
    fixture: &Fixture,
    lease: &StorageTaskLease,
    size: usize,
) -> (ArtifactIdentity, ArtifactLimits) {
    let store = fixture.backend.artifact_store();
    let destination =
        Destination::new(DestinationId::generate(), prefix("artifact_contract")).unwrap();
    store.register_destination(&destination).await.unwrap();
    let identity = ArtifactIdentity::new(destination.id());
    let limits = ArtifactLimits::builder()
        .block_bytes(1024)
        .artifact_bytes(size as u64)
        .blocks((size as u32).div_ceil(1024))
        .build()
        .unwrap();
    store.stage(&identity, limits).await.unwrap();
    store
        .bind_task(
            &identity,
            lease,
            ArtifactReservation::new(*uuid::Uuid::new_v4().as_bytes(), size as u64, 1 << 20)
                .unwrap(),
            Utc::now() + chrono::Duration::hours(1),
        )
        .await
        .unwrap();
    (identity, limits)
}

fn completion(lease: StorageTaskLease, proof: VerifiedArtifact) -> StorageTaskCompletion {
    StorageTaskCompletion::new(
        StorageTaskTerminalUpdate::new(
            lease,
            StorageTaskTerminalStatus::Succeeded,
            StorageTaskResultCounts::try_new(1, 1, 0).unwrap(),
        ),
        StorageTaskEventInput::new("succeeded", "Verified output"),
        StorageTaskCompletionPayload::Export(
            StorageExportTaskArtifact::builder(
                "text/plain",
                StorageExportTaskArtifactContent::Verified(proof),
                serde_json::json!({}),
                serde_json::json!([]),
                Utc::now() + chrono::Duration::hours(1),
            )
            .try_build()
            .unwrap(),
        ),
    )
}

async fn reconcile(store: &dyn TaskArtifactStore) {
    let deadline = tokio::time::Instant::now() + LOCAL_SPOOL_CLEANUP_GRACE + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        assert!(store.cleanup().await.unwrap() <= 16);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn drain(store: &dyn TaskArtifactStore, reference: &ArtifactReference) -> Vec<u8> {
    let mut reader = ArtifactReader::open(store, reference).await.unwrap();
    let mut result = Vec::new();
    while let Some(block) = reader.next_block().await.unwrap() {
        result.extend(block);
    }
    result
}

#[rstest]
#[case::empty(0)]
#[case::partial(123)]
#[case::multiple_blocks(3500)]
#[actix_web::test]
async fn replica_can_verify_and_atomically_publish_without_the_producer_spool(#[case] size: usize) {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Export).await;
        let lease = fixture.claim().await;
        let (identity, limits) = stage(&fixture, &lease, size).await;
        let store = fixture.backend.artifact_store();
        let content = vec![b'x'; size];
        let mut writer = ArtifactWriter::new(store.as_ref(), identity.clone(), limits)
            .await
            .unwrap();
        writer.write(&content).await.unwrap();
        let manifest = writer.seal().await.unwrap();
        let reference = ArtifactReference::from_catalog(identity, manifest.manifest_digest());
        assert!(
            ArtifactReader::open(store.as_ref(), &reference)
                .await
                .is_err()
        );
        let replica = fixture.environment.storage();
        let remote = replica.artifact_store();
        let durable = ArtifactManifest::decode(
            &remote.manifest(reference.identity()).await.unwrap(),
            reference.identity(),
            ArtifactLimits::format_ceiling(),
        )
        .unwrap();
        let proof = verify(remote.as_ref(), &durable).await.unwrap();
        assert!(
            remote.publish(&proof).await.is_err(),
            "task output needs its task transaction"
        );
        replica
            .complete_task(completion(lease, proof))
            .await
            .unwrap();
        assert_eq!(drain(remote.as_ref(), &reference).await, content);
        let StorageTaskOutputLookup::Available(output) =
            replica.get_export_output(fixture.task.id()).await.unwrap()
        else {
            panic!("completed output missing")
        };
        assert_eq!(output.artifact_reference(), Some(&reference));
        fixture.cleanup().await;
        store.cleanup().await.unwrap();
    }
}

#[rstest]
#[case::staging(0)]
#[case::uploaded(1)]
#[case::sealed(2)]
#[case::verified(3)]
#[actix_web::test]
async fn cancellation_fences_each_publication_boundary(#[case] boundary: u8) {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Export).await;
        let lease = fixture.claim().await;
        let (identity, limits) = stage(&fixture, &lease, 20 * 1024).await;
        let store = fixture.backend.artifact_store();
        let mut writer = ArtifactWriter::new(store.as_ref(), identity.clone(), limits)
            .await
            .unwrap();
        if boundary > 0 {
            writer.write(&vec![b'c'; 20 * 1024]).await.unwrap();
        }
        let manifest = if boundary > 1 {
            Some(writer.seal().await.unwrap())
        } else {
            drop(writer);
            None
        };
        let proof = if boundary > 2 {
            Some(
                verify(store.as_ref(), manifest.as_ref().unwrap())
                    .await
                    .unwrap(),
            )
        } else {
            None
        };
        fixture
            .backend
            .request_task_cancellation(fixture.request())
            .await
            .unwrap();
        // The worker still owns local resources until it acknowledges the stop.
        store.cleanup().await.unwrap();
        if let Some(manifest) = &manifest {
            assert_eq!(store.manifest(&identity).await.unwrap(), manifest.encode());
        }
        fixture
            .backend
            .acknowledge_task_stop(lease.clone())
            .await
            .unwrap();
        let before_cleanup = fixture.backend.task_capacity_snapshot().await.unwrap();
        store.cleanup().await.unwrap();
        if fixture.environment.kind() == StorageBackendKind::Memory {
            // This fixture has a private store, so other tests cannot change
            // its capacity between snapshots. Fencing must not free local bytes.
            assert_eq!(
                fixture
                    .backend
                    .task_capacity_snapshot()
                    .await
                    .unwrap()
                    .ordinary()
                    .resident_bytes(),
                before_cleanup.ordinary().resident_bytes()
            );
        }
        if let Some(proof) = proof {
            assert!(
                fixture
                    .backend
                    .complete_task(completion(lease, proof))
                    .await
                    .is_err()
            );
        }
        // Each batch is bounded, and reservations survive renderer quiescence.
        reconcile(store.as_ref()).await;
        if fixture.environment.kind() == StorageBackendKind::Memory {
            assert_eq!(
                fixture
                    .backend
                    .task_capacity_snapshot()
                    .await
                    .unwrap()
                    .ordinary()
                    .resident_bytes()
                    + 2 * limits.artifact_bytes(),
                before_cleanup.ordinary().resident_bytes()
            );
        }
        assert!(store.manifest(&identity).await.is_err());
        assert!(store.stage(&identity, limits).await.is_err());
        fixture.cleanup().await;
    }
}

#[actix_web::test]
async fn artifact_publication_rolls_back_with_task_finalization() {
    let _permit = postgres_permit().await;
    let fixture = Fixture::new(
        BackendTestEnvironment::Postgres {
            pool: pool().get_ref().clone(),
        },
        StorageTaskKind::Export,
    )
    .await;
    let lease = fixture.claim().await;
    let (identity, limits) = stage(&fixture, &lease, 123).await;
    let store = fixture.backend.artifact_store();
    let mut writer = ArtifactWriter::new(store.as_ref(), identity, limits)
        .await
        .unwrap();
    writer.write(b"transactional output").await.unwrap();
    let manifest = writer.seal().await.unwrap();
    let proof = verify(store.as_ref(), &manifest).await.unwrap();
    let reference = proof.reference();
    let completion = completion(lease, proof);
    PostgresFaultController::failing(PostgresFaultPoint::TaskFinalizeAfterEvent)
        .run(fixture.backend.complete_task(completion.clone()))
        .await
        .unwrap_err();
    assert!(
        ArtifactReader::open(store.as_ref(), &reference)
            .await
            .is_err()
    );
    fixture.backend.complete_task(completion).await.unwrap();
    assert_eq!(
        drain(store.as_ref(), &reference).await,
        b"transactional output"
    );
    fixture.cleanup().await;
    store.cleanup().await.unwrap();
}

#[rstest]
#[case::partial(false)]
#[case::sealed(true)]
#[actix_web::test]
async fn lost_worker_fails_without_replaying_and_reconciles_its_bytes(#[case] sealed: bool) {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Export).await;
        let lease = fixture.claim().await;
        let (identity, limits) = stage(&fixture, &lease, 20 * 1024).await;
        let store = fixture.backend.artifact_store();
        let mut writer = ArtifactWriter::new(store.as_ref(), identity.clone(), limits)
            .await
            .unwrap();
        writer.write(&vec![b'l'; 20 * 1024]).await.unwrap();
        if sealed {
            writer.seal().await.unwrap();
        } else {
            drop(writer);
        }
        fixture
            .backend
            .renew_task_lease(
                lease,
                StorageTaskLeaseDuration::from_milliseconds(1).unwrap(),
            )
            .await
            .unwrap();
        let replica = fixture.environment.storage();
        let recovered = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                replica.recover_expired_task_leases(100).await.unwrap();
                let (task, _) = replica
                    .get_task_access(fixture.task.id())
                    .await
                    .unwrap()
                    .into_parts();
                if task.status().is_terminal() {
                    break task;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(recovered.status(), StorageTaskStatus::Failed);
        reconcile(store.as_ref()).await;
        assert!(store.manifest(&identity).await.is_err());
        assert!(matches!(
            replica.get_export_output(fixture.task.id()).await.unwrap(),
            StorageTaskOutputLookup::Missing
        ));
        fixture.cleanup().await;
    }
}

#[actix_web::test]
async fn published_bytes_are_immutable_and_repeatedly_verifiable() {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let storage = environment.storage();
        let store = storage.artifact_store();
        let destination =
            Destination::new(DestinationId::generate(), prefix("immutable_artifact")).unwrap();
        store.register_destination(&destination).await.unwrap();
        let identity = ArtifactIdentity::new(destination.id());
        let limits = ArtifactLimits::default();
        store.stage(&identity, limits).await.unwrap();
        store.append(&identity, 0, b"immutable").await.unwrap();
        let mut builder = ManifestBuilder::new(identity.clone(), limits);
        builder.push(b"immutable").unwrap();
        let manifest = builder.finish().unwrap();
        store.seal(&manifest).await.unwrap();
        let proof = verify(store.as_ref(), &manifest).await.unwrap();
        let reference = store.publish(&proof).await.unwrap();
        assert!(store.append(&identity, 0, b"different").await.is_err());
        assert!(store.abort(&identity).await.is_err());
        assert_eq!(
            store.published_manifest(&reference).await.unwrap(),
            manifest.encode()
        );
        store.cleanup().await.unwrap();
        assert_eq!(
            store.block(&identity, 0).await.unwrap().unwrap(),
            b"immutable"
        );
    }
}

#[actix_web::test]
async fn admitted_artifact_cannot_be_rebound_to_another_task() {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Export).await;
        let lease = fixture.claim().await;
        let (identity, _) = stage(&fixture, &lease, 1024).await;
        let store = fixture.backend.artifact_store();
        let second = Fixture::new(fixture.environment.clone(), StorageTaskKind::Export).await;
        let other = second.claim().await;
        assert!(
            store
                .bind_task(
                    &identity,
                    &other,
                    ArtifactReservation::new(*uuid::Uuid::new_v4().as_bytes(), 1024, 4096).unwrap(),
                    Utc::now() + chrono::Duration::hours(1)
                )
                .await
                .is_err()
        );
        store.abort(&identity).await.unwrap();
        reconcile(store.as_ref()).await;
        second.cleanup().await;
        fixture.cleanup().await;
    }
}
