use super::*;
use hubuum_storage_core::artifacts::*;

#[rstest]
#[tokio::test]
async fn restored_copy_requires_space_in_addition_to_retained_obligations(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    super::task_capacity::with_fixture(kind, async |fixture| {
        let (_, reference) = seed(fixture).await;
        let source = fixture.capture(true).await.unwrap();
        let storage = fixture.storage();
        let snapshot = storage.task_capacity_snapshot().await.unwrap();
        let policy = snapshot.policy().policy().clone();
        let deployment = policy.deployment();
        let policy = policy.with_deployment(TaskScopeCapacity::new(
            deployment.outstanding(),
            deployment.executing(),
            TaskByteLimit::new(snapshot.ordinary().resident_bytes() + 1024).unwrap(),
        ));
        super::task_capacity::set_policy(storage, policy).await;
        assert!(fixture.restore(source).await.is_err());
        assert!(ArtifactReader::open(storage, &reference).await.is_ok());
    })
    .await;
}

async fn seed(fixture: &RestoreContractFixture) -> (hubuum_domain::TaskId, ArtifactReference) {
    let storage = fixture.storage();
    let pending = storage
        .list_tasks(StorageTaskListQuery::new(
            None,
            None,
            Some(StorageTaskStatus::Queued),
            hubuum_query::QueryOptions::new(vec![], vec![], Some(100), None, true).unwrap(),
        ))
        .await
        .unwrap()
        .into_parts()
        .0;
    for task in pending {
        storage
            .request_task_cancellation(StorageTaskCancellationRequest::new(
                &task,
                hubuum::events::EventContext::system(),
            ))
            .await
            .unwrap();
    }
    let task_id = fixture.create_task(StorageTaskKind::Export).await.unwrap();
    let claim = storage
        .claim_next_task(StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.task().id(), task_id);
    let destination = Destination::new(
        DestinationId::generate(),
        format!("restore-{}", Uuid::new_v4()),
    )
    .unwrap();
    storage.register_destination(&destination).await.unwrap();
    let identity = ArtifactIdentity::new(destination.id());
    let limits = ArtifactLimits::builder()
        .block_bytes(1024)
        .artifact_bytes(20 * 1024)
        .blocks(20)
        .build()
        .unwrap();
    storage.stage(&identity, limits).await.unwrap();
    storage
        .bind_task(
            &identity,
            claim.lease(),
            ArtifactReservation::new(*Uuid::new_v4().as_bytes(), 20 * 1024, 1024 * 1024).unwrap(),
            Utc::now() + chrono::Duration::hours(1),
        )
        .await
        .unwrap();
    let mut writer = ArtifactWriter::new(storage, identity, limits)
        .await
        .unwrap();
    writer.write(&vec![b'x'; 20 * 1024]).await.unwrap();
    let proof = verify(storage, &writer.seal().await.unwrap())
        .await
        .unwrap();
    let reference = proof.reference();
    storage
        .complete_task(StorageTaskCompletion::new(
            StorageTaskTerminalUpdate::new(
                claim.lease().clone(),
                StorageTaskTerminalStatus::Succeeded,
                StorageTaskResultCounts::try_new(1, 1, 0).unwrap(),
            ),
            StorageTaskEventInput::new("succeeded", "portable artifact"),
            export_payload(StorageExportTaskArtifactContent::Verified(proof)),
        ))
        .await
        .unwrap();
    (task_id, reference)
}

#[rstest]
#[tokio::test]
async fn portable_export_restore_uses_fresh_identity_and_preserves_cleanup(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    let _guard = RESTORE_TEST_LOCK.lock().await;
    let pool = postgres_test_pool_with_timeout(&database_url(), 4, DEFAULT_DB_STATEMENT_TIMEOUT_MS);
    let fixture = RestoreContractFixture::new(kind, pool).await.unwrap();
    let (task_id, old) = seed(&fixture).await;
    let source = fixture.capture(true).await.unwrap();
    fixture.restore(source).await.unwrap();
    let storage = fixture.storage();
    let StorageTaskOutputLookup::Available(output) =
        storage.get_export_output(task_id).await.unwrap()
    else {
        panic!("restored output missing")
    };
    let reference = output.artifact_reference().unwrap();
    assert_ne!(reference.identity(), old.identity());
    let mut reader = ArtifactReader::open(storage, reference).await.unwrap();
    let mut bytes = Vec::new();
    while let Some(block) = reader.next_block().await.unwrap() {
        bytes.extend(block);
    }
    assert_eq!(bytes, vec![b'x'; 20 * 1024]);
    // Old physical bytes survived logical replacement and require real cleanup.
    assert!(storage.manifest(old.identity()).await.is_ok());
    for _ in 0..100 {
        assert!(storage.cleanup().await.unwrap() <= 16);
    }
    assert!(storage.manifest(old.identity()).await.is_err());
    assert!(ArtifactReader::open(storage, reference).await.is_ok());
    fixture.restore_original().await.unwrap();
    for _ in 0..100 {
        storage.cleanup().await.unwrap();
    }
}
