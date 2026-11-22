use super::*;
use crate::errors::ApiError;
use crate::services::deletion_recovery::{
    RuntimeRecoveryLifetime, reapply, recover, recover_with_archive_cleanup,
    require_runtime_permit,
    tests::{Fixture, after_repository_release},
};
use crate::test_support::{
    archive_test_runtime_database_url, claim_test_migration_database_url,
    claim_test_runtime_database_url, crash_test_runtime_database_url, postgres_test_pool,
    restore_test_runtime_database_url,
};
use diesel::{QueryableByName, sql_query, sql_types};
use diesel_async::RunQueryDsl;
use hubuum_artifacts::{
    ArtifactCreationIntent, ArtifactStore, FilesystemArtifactStore, OwnedArtifactAdmission,
    OwnedArtifactPhase, OwnedArtifactResolution, OwnedArtifactStore,
};
use hubuum_storage_postgres::test_support::{IsolatedRestoreTarget, integration_test_database_url};
use hubuum_storage_postgres::{PostgresPool, PostgresStorageError, with_connection};
use std::fs;
use std::os::unix::fs::DirBuilderExt;
use std::path::Path;
use tokio::process::Command;
use tokio::time::{sleep, timeout};
use uuid::Uuid;

#[tokio::test]
#[ignore = "subprocess for a committed archive claim interrupted during file staging"]
async fn archive_producer() {
    let root = std::env::current_dir().unwrap();
    let storage =
        StorageHandle::postgres(postgres_test_pool(&claim_test_runtime_database_url(), 4));
    let (id, _lifetime) = stage_archive(storage, &root, true).await;
    fs::write(root.join("staged.tmp"), id.as_bytes()).unwrap();
    fs::rename(root.join("staged.tmp"), root.join("staged")).unwrap();
    std::future::pending::<()>().await;
}

async fn stage_archive(
    storage: StorageHandle,
    root: &Path,
    bound: bool,
) -> (Uuid, RuntimeRecoveryLifetime) {
    let options = serde_json::from_value(json!({
        "directory": root.join("repository"),
        "key": root.join("key"),
    }))
    .unwrap();
    let isolation = require_runtime_permit(&storage, &options)
        .await
        .unwrap()
        .into_runtime_lifetime();
    let fixture = create_backend_object_fixture(&storage, &prefix("claim_crash"), vec![]).await;
    let collection = fixture.collection.id();
    let identity = storage
        .resolve_logical_identity(StorageIdentityTarget::Resource {
            kind: StorageIdentityResourceKind::Collection,
            id: collection.into(),
        })
        .await
        .unwrap()
        .subject()
        .and_then(StorageIdentitySubject::record_identity)
        .unwrap();
    storage
        .collection_store()
        .update_collection(
            collection,
            StorageCollectionUpdate::new(None, Some("close original version".into())),
            &EventContext::system(),
        )
        .await
        .unwrap()
        .into_value();
    let registration = storage
        .register_history_runtime(isolation.isolation_domain())
        .await
        .unwrap();
    let storage = storage.with_runtime_owner(registration.owner());
    let application = storage.admit_application_work().await.unwrap();
    let owner = storage
        .run_in_scope(
            StorageExecutionScope::default().with_application_access(application),
            storage.admit_history_archive_execution(),
        )
        .await
        .unwrap();
    let permit = owner.permit();
    let scope = permit.execution_scope();
    let capture = storage
        .run_in_scope(
            scope.clone(),
            storage.capture_history_archive(
                HistoryArchiveCaptureRequest::new(
                    PrincipalIdentity::generate(),
                    HistoryArchiveSelection::new(
                        StorageBackupHistorySection::CollectionHistory,
                        identity.resource(),
                        HistoryArchivePoint::Exact {
                            version: identity.version(),
                        },
                    ),
                    HistoryArchiveCatalogGeneration::default(),
                    Utc::now() + TimeDelta::hours(1),
                ),
                permit,
            ),
        )
        .await
        .unwrap();

    let store = FilesystemArtifactStore::open(root.join("provider"))
        .await
        .unwrap();
    let destination = Destination::new(DestinationId::generate(), "claim-crash").unwrap();
    store.register_destination(&destination).await.unwrap();
    let manifest = HistoryArchivePublicationManifest::from_capture(
        &capture,
        permit,
        ArtifactIdentity::new(destination.id()),
        Utc::now(),
    )
    .unwrap();
    let limits = ArtifactLimits::builder()
        .artifact_bytes(HISTORY_ARCHIVE_PUBLICATION_BYTES)
        .build()
        .unwrap();
    let intent = ArtifactCreationIntent::new(
        manifest.location().clone(),
        limits,
        *permit.producer().id().as_bytes(),
        *permit.application().fence().incarnation().as_bytes(),
        manifest.request_commitment().unwrap(),
    )
    .unwrap();
    let intent = if bound {
        store.bind_intent(intent).await.unwrap()
    } else {
        intent
    };
    let encoded = HistoryArchiveEncodedArtifactIntent::try_new(
        intent.identity().clone(),
        limits,
        intent.encode().unwrap(),
        *intent.request_commitment(),
    )
    .unwrap();
    let request = HistoryArchivePublicationRequest::try_new(manifest, encoded).unwrap();
    let claim = storage
        .run_in_scope(
            scope.clone(),
            storage.claim_history_archive_publication(request, permit),
        )
        .await
        .unwrap();
    let intent = ArtifactCreationIntent::decode(claim.request().intent().encoded()).unwrap();
    let OwnedArtifactAdmission::Created(receipt) = store.create_owned(&intent).await.unwrap()
    else {
        panic!("fresh claim must create its own attempt");
    };
    let row = serde_json::to_vec(capture.hot().expect("captured closed version")).unwrap();
    store.append_owned(&receipt, 0, &row[..16]).await.unwrap();
    (permit.producer().parent().id(), isolation)
}

// Read the exact protected claim and pin through the migration login. The
// recovery observation must not depend on the killed producer's in-memory intent.
async fn retained_claim(pool: &PostgresPool, id: Uuid) -> (ArtifactCreationIntent, Uuid) {
    #[derive(QueryableByName)]
    struct Claim {
        #[diesel(sql_type = sql_types::Binary)]
        encoded_intent: Vec<u8>,
        #[diesel(sql_type = sql_types::Uuid)]
        capture_id: Uuid,
    }
    let row = with_connection(pool, async |connection| {
        sql_query("SELECT a.encoded_intent, a.capture_id FROM hubuum_control.history_archive_publication_claims a
            JOIN hubuum_control.history_publication_owners p ON p.id=a.publication_id AND p.ownership_token=a.publication_ownership
            JOIN hubuum_control.history_publication_producers c ON c.publication_id=p.id AND c.id=a.producer_id AND c.ownership_token=a.producer_ownership
            JOIN hubuum_control.history_archive_captures h ON h.id=a.capture_id
            WHERE p.id=$1 AND p.producer_count=1 AND c.publication_claimed AND NOT c.retired AND h.publication_claimed")
            .bind::<sql_types::Uuid,_>(id).get_result::<Claim>(connection).await.map_err(PostgresStorageError::from)
    }).await.unwrap();
    (
        ArtifactCreationIntent::decode(&row.encoded_intent).unwrap(),
        row.capture_id,
    )
}

async fn interrupted_claim(root: &Path) -> Uuid {
    let mut producer = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "tests::storage_contract::history_archive::claim_crash::archive_producer",
            "--ignored",
            "--nocapture",
        ])
        .current_dir(root)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let id = timeout(Duration::from_secs(30), async {
        loop {
            match fs::read(root.join("staged")) {
                Ok(bytes) => break Uuid::from_slice(&bytes).unwrap(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                Err(error) => panic!("reading archive producer signal: {error}"),
            }
            assert!(
                producer.try_wait().unwrap().is_none(),
                "producer exited before staging"
            );
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("archive producer did not finish staging");
    producer.kill().await.unwrap();
    producer.wait().await.unwrap();
    id
}

async fn assert_claim_blocks_release(fixture: &Fixture) {
    let result =
        after_repository_release(|| reapply(&fixture.storage, &fixture.directory, &fixture.key))
            .await;
    assert!(
        matches!(result, Err(ApiError::ServiceUnavailable(ref message))
        if message.contains("History publication drivers or producers lack positive retirement")),
        "expected the outstanding publication to block reapplication: {:?}",
        result.err()
    );
    assert!(fixture.storage.admit_application_work().await.is_err());
}

#[tokio::test]
async fn interrupted_filesystem_publication_requires_exact_cleanup_before_release() {
    let runtime = claim_test_runtime_database_url();
    let target = IsolatedRestoreTarget::new(&runtime, &integration_test_database_url()).unwrap();
    for unrelated in [
        restore_test_runtime_database_url(),
        archive_test_runtime_database_url(),
        crash_test_runtime_database_url(),
    ] {
        IsolatedRestoreTarget::new(&runtime, &unrelated).unwrap();
    }
    let admin = postgres_test_pool(&claim_test_migration_database_url(), 2);
    target
        .validate_connection(&mut admin.get().await.unwrap())
        .await
        .unwrap();
    target
        .validate_connection(&mut postgres_test_pool(&runtime, 1).get().await.unwrap())
        .await
        .unwrap();
    let fixture = Fixture::with_storage(StorageHandle::postgres(admin.clone())).await;
    let provider = fixture.root.join("provider");
    fs::DirBuilder::new().mode(0o700).create(&provider).unwrap();
    let provider = provider.canonicalize().unwrap();
    let id = interrupted_claim(&fixture.root).await;
    after_repository_release(|| recover(&fixture.storage, &fixture.directory, &fixture.key))
        .await
        .unwrap();
    assert_claim_blocks_release(&fixture).await;

    let (intent, capture) = retained_claim(&admin, id).await;
    let store = FilesystemArtifactStore::open(&provider).await.unwrap();
    let OwnedArtifactResolution::Established {
        artifact,
        phase: OwnedArtifactPhase::Staging,
    } = store.resolve_owned(&intent).await.unwrap()
    else {
        panic!("interrupted attempt must still be staged");
    };
    store.abort_owned(&artifact).await.unwrap();
    assert!(matches!(
        store.resolve_owned(&intent).await.unwrap(),
        OwnedArtifactResolution::Established {
            phase: OwnedArtifactPhase::Fenced,
            ..
        }
    ));
    assert_claim_blocks_release(&fixture).await;
    assert_eq!(retained_claim(&admin, id).await, (intent.clone(), capture));

    // Simulate a lost acknowledgement after physical removal. A fresh provider
    // session can issue the same receipt, but only the control transaction can
    // retire the original native claim and unpin its capture.
    let receipt = store.discard_owned_step(&intent).await.unwrap().unwrap();
    assert_eq!(receipt.encoded_intent(), intent.encode().unwrap());
    assert_claim_blocks_release(&fixture).await;
    after_repository_release(|| {
        recover_with_archive_cleanup(
            &fixture.storage,
            &fixture.directory,
            &fixture.key,
            Some((id, &provider)),
        )
    })
    .await
    .unwrap();
    assert!(fixture.storage.admit_application_work().await.is_err());
    after_repository_release(|| reapply(&fixture.storage, &fixture.directory, &fixture.key))
        .await
        .unwrap();
    #[derive(QueryableByName)]
    struct Released {
        #[diesel(sql_type = sql_types::Bool)]
        complete: bool,
    }
    let released = with_connection(&admin, async |connection| {
        sql_query("SELECT EXISTS(SELECT 1 FROM hubuum_control.history_archive_captures WHERE id=$2 AND NOT publication_claimed)
            AND NOT EXISTS(SELECT 1 FROM hubuum_control.history_archive_publication_claims WHERE publication_id=$1)
            AND NOT EXISTS(SELECT 1 FROM hubuum_control.history_publication_producers WHERE publication_id=$1)
            AND NOT EXISTS(SELECT 1 FROM hubuum_control.history_publication_owners WHERE id=$1) AS complete")
            .bind::<sql_types::Uuid,_>(id).bind::<sql_types::Uuid,_>(capture)
            .get_result::<Released>(connection).await.map_err(PostgresStorageError::from)
    }).await.unwrap();
    assert!(released.complete);
    fixture.storage.admit_application_work().await.unwrap();
}

#[rstest::rstest]
#[case::original_root(true, true)]
#[case::different_root(true, false)]
#[case::legacy_unbound_intent(false, true)]
#[tokio::test]
async fn isolated_memory_recovery_requires_original_filesystem_receipt(
    #[case] bound: bool,
    #[case] original_root: bool,
) {
    let fixture = Fixture::with_storage(StorageHandle::memory()).await;
    let root = fixture.root.canonicalize().unwrap();
    let provider = root.join("provider");
    fs::DirBuilder::new().mode(0o700).create(&provider).unwrap();
    let (id, lifetime) = stage_archive(fixture.storage.clone(), &root, bound).await;
    lifetime.release();
    let selected = if original_root {
        provider
    } else {
        let other = root.join("other");
        fs::DirBuilder::new().mode(0o700).create(&other).unwrap();
        FilesystemArtifactStore::open(&other).await.unwrap();
        other
    };
    let result = after_repository_release(|| {
        recover_with_archive_cleanup(
            &fixture.storage,
            &fixture.directory,
            &fixture.key,
            Some((id, &selected)),
        )
    })
    .await;
    assert!(fixture.storage.admit_application_work().await.is_err());
    assert_eq!(result.is_ok(), bound && original_root);
    let release =
        after_repository_release(|| reapply(&fixture.storage, &fixture.directory, &fixture.key))
            .await;
    assert_eq!(release.is_ok(), bound && original_root);
}

#[tokio::test]
async fn rotated_recovery_fence_rejects_verified_cleanup_settlement() {
    use hubuum_storage_core::HistoryArchiveCleanupRequest;
    let fixture = Fixture::with_storage(StorageHandle::memory()).await;
    let root = fixture.root.canonicalize().unwrap();
    let provider = root.join("provider");
    fs::DirBuilder::new().mode(0o700).create(&provider).unwrap();
    let (id, lifetime) = stage_archive(fixture.storage.clone(), &root, true).await;
    lifetime.release();
    after_repository_release(|| recover(&fixture.storage, &fixture.directory, &fixture.key))
        .await
        .unwrap();
    let isolation = fixture.exclusive_isolation().await;
    let fence = fixture
        .storage
        .read_application_control()
        .await
        .unwrap()
        .fence();
    let request = HistoryArchiveCleanupRequest::new(id, fence, isolation.domain()).unwrap();
    let claim = fixture
        .storage
        .load_history_archive_cleanup(&request)
        .await
        .unwrap();
    let intent = ArtifactCreationIntent::decode(claim.encoded_intent()).unwrap();
    let receipt = FilesystemArtifactStore::open_existing(&provider)
        .await
        .unwrap()
        .discard_owned_step(&intent)
        .await
        .unwrap()
        .unwrap();
    let settlement = claim.after_cleanup(&receipt).unwrap();
    drop(isolation);

    after_repository_release(|| recover(&fixture.storage, &fixture.directory, &fixture.key))
        .await
        .unwrap();
    let isolation = fixture.exclusive_isolation().await;
    assert!(
        fixture
            .storage
            .settle_history_archive_cleanup(settlement)
            .await
            .is_err()
    );
    let fence = fixture
        .storage
        .read_application_control()
        .await
        .unwrap()
        .fence();
    let current = HistoryArchiveCleanupRequest::new(id, fence, isolation.domain()).unwrap();
    let retained = fixture
        .storage
        .load_history_archive_cleanup(&current)
        .await
        .unwrap();
    assert_eq!(retained.encoded_intent(), receipt.encoded_intent());
}
