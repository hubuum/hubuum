use super::*;
use crate::{
    ArtifactId, ArtifactReader, ArtifactWriter, AttemptId, DestinationId, ManifestBuilder,
    ObjectVersion, verify, verify_and_publish,
};
use aws_sdk_s3::types::{ObjectLockLegalHold, ObjectLockLegalHoldStatus, VersioningConfiguration};
use rstest::rstest;

fn builder() -> S3ConfigBuilder {
    S3Config::builder(Destination::new(DestinationId::generate(), "unit-s3").unwrap())
        .bucket("private-artifacts")
        .region("us-east-1")
        .credentials("fixture-access", "fixture-secret", None)
}

#[rstest]
#[case("http://localhost:9000")]
#[case("https://access:secret@localhost")]
#[case("https://localhost/?secret=value")]
#[case("https://localhost/path")]
#[case("https://localhost/#secret")]
fn rejects_unsafe_endpoints(#[case] endpoint: &str) {
    assert!(builder().endpoint(endpoint).build().is_err());
}

#[test]
fn configuration_debug_redacts_credentials() {
    let config = builder().build().unwrap();
    let debug = format!("{config:?}");
    assert!(!debug.contains("fixture-access") && !debug.contains("fixture-secret"));
}

#[test]
fn missing_credentials_fail_before_connecting() {
    assert!(
        S3Config::builder(Destination::new(DestinationId::generate(), "unit-s3").unwrap())
            .bucket("private-artifacts")
            .region("us-east-1")
            .build()
            .is_err()
    );
}

#[rstest]
#[case(b"not a PEM certificate")]
#[case(b"-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----")]
fn invalid_private_ca_is_rejected(#[case] pem: &[u8]) {
    assert!(builder().private_ca_pem(pem.to_vec()).build().is_err());
}

#[test]
fn corrupted_control_limits_cannot_enlarge_provider_reads() {
    let state = State {
        owner: None,
        identity: ArtifactIdentity::new(DestinationId::generate()),
        limits: [u64::MAX, 256, 32, 1024],
        expires: 0,
        phase: Phase::Staging,
        blocks: Vec::new(),
        manifest: None,
        cleanup_after: None,
    };
    assert!(State::decode(&serde_json::to_vec(&state).unwrap()).is_err());
}

#[rstest]
#[case(vec![String::new()])]
#[case(vec!["x".repeat(4097)])]
#[case((0..17).map(|id| id.to_string()).collect())]
fn upload_intent_rejects_invalid_id_sets(#[case] ids: Vec<String>) {
    assert!(UploadIds::decode(&serde_json::to_vec(&ids).unwrap()).is_err());
}

fn fixture_config(destination: Destination, bucket: &str, trust_ca: bool) -> S3Config {
    let mut builder = S3Config::builder(destination)
        .bucket(bucket)
        .region("us-east-1")
        .endpoint(
            std::env::var("HUBUUM_TEST_S3_ENDPOINT").expect("run the s3-artifacts fixture harness"),
        )
        .credentials("hubuum-fixture", "hubuum-fixture-secret", None);
    if trust_ca {
        builder = builder
            .private_ca_pem(std::fs::read(std::env::var("HUBUUM_TEST_S3_CA").unwrap()).unwrap());
    }
    builder.build().unwrap()
}

async fn fixture() -> S3ArtifactStore {
    let destination = Destination::new(
        DestinationId::generate(),
        format!("fixture-{}", Uuid::new_v4()),
    )
    .unwrap();
    let bucket = format!("fixture-{}", Uuid::new_v4());
    let config = fixture_config(destination, &bucket, true);
    let client = config.client().unwrap();
    client
        .create_bucket()
        .bucket(&bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();
    client
        .put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    S3ArtifactStore::connect(config).await.unwrap()
}

fn limits() -> ArtifactLimits {
    ArtifactLimits::builder()
        .block_bytes(8)
        .artifact_bytes(256)
        .blocks(32)
        .build()
        .unwrap()
}

async fn sealed(store: &S3ArtifactStore, bytes: &[u8]) -> ArtifactManifest {
    let mut writer = ArtifactWriter::new(
        store,
        ArtifactIdentity::new(store.destination().id()),
        limits(),
    )
    .await
    .unwrap();
    writer.write(bytes).await.unwrap();
    writer.seal().await.unwrap()
}

#[rstest]
#[case(b"")]
#[case(b"partial")]
#[case(b"fullblock")]
#[case(b"more than one block with a final partial")]
#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture: python3 tests/python/run.py integration s3-artifacts"]
async fn live_round_trip(#[case] bytes: &[u8]) {
    let store = fixture().await;
    let manifest = sealed(&store, bytes).await;
    let reference = verify_and_publish(&store, &manifest).await.unwrap();
    let mut reader = ArtifactReader::open(&store, &reference).await.unwrap();
    let mut actual = Vec::new();
    while let Some(block) = reader.next_block().await.unwrap() {
        actual.extend(block);
    }
    assert_eq!(actual, bytes);
}

#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_sealed_state_resumes_with_a_new_client() {
    let store = fixture().await;
    let manifest = sealed(&store, b"restart survives local client state loss").await;
    lifecycle("restart").await;
    let reopened = S3ArtifactStore::connect(fixture_config(
        store.destination().clone(),
        &store.config.bucket,
        true,
    ))
    .await
    .unwrap();
    assert!(verify_and_publish(&reopened, &manifest).await.is_ok());
}

async fn lifecycle(action: &str) {
    let directory = std::path::PathBuf::from(std::env::var("HUBUUM_TEST_S3_CONTROL").unwrap());
    let request = directory.join("request");
    let ack = directory.join("ack");
    let _ = std::fs::remove_file(&ack);
    std::fs::write(directory.join("request.tmp"), action).unwrap();
    std::fs::rename(directory.join("request.tmp"), request).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(40), async {
        loop {
            if std::fs::read_to_string(&ack).is_ok_and(|value| value == action) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_outage_preserves_pending_cleanup_until_provider_recovers() {
    let store = fixture().await;
    let manifest = sealed(&store, b"original").await;
    store.abort(manifest.identity()).await.unwrap();
    lifecycle("stop").await;
    let result = store.cleanup().await;
    lifecycle("start").await;
    assert!(result.is_err());
    assert_eq!(store.cleanup().await.unwrap(), 1);
}

#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_prepare_benchmark_bucket() {
    let destination = Destination::new(
        DestinationId::from_bytes([0x53; 16]).unwrap(),
        "s3-benchmark",
    )
    .unwrap();
    let config = fixture_config(destination, "hubuum-artifact-bench", true);
    let client = config.client().unwrap();
    client
        .create_bucket()
        .bucket("hubuum-artifact-bench")
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();
    client
        .put_bucket_versioning()
        .bucket("hubuum-artifact-bench")
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_publication_retry_is_identical() {
    let store = fixture().await;
    let manifest = sealed(&store, b"idempotent publication").await;
    let proof = verify(&store, &manifest).await.unwrap();
    let first = store.publish(&proof).await.unwrap();
    assert_eq!(first, store.publish(&proof).await.unwrap());
}

#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_abort_fences_a_previously_verified_proof() {
    let store = fixture().await;
    let manifest = sealed(&store, b"cancel this verified attempt").await;
    let proof = verify(&store, &manifest).await.unwrap();
    store.abort(manifest.identity()).await.unwrap();
    assert!(store.publish(&proof).await.is_err());
}

#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_abort_unknown_attempt_has_no_fence() {
    let store = fixture().await;
    assert!(matches!(
        store
            .abort(&ArtifactIdentity::new(store.destination().id()))
            .await,
        Err(ArtifactError::NotFound)
    ));
}

#[rstest]
#[case(true)]
#[case(false)]
#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_identity_ownership_survives_cleanup_and_reopen(#[case] reuse_attempt: bool) {
    let store = fixture().await;
    let original = ArtifactIdentity::new(store.destination().id());
    store.stage(&original, limits()).await.unwrap();
    store.append(&original, 0, b"original").await.unwrap();
    store.abort(&original).await.unwrap();
    store.cleanup().await.unwrap();
    let reopened = S3ArtifactStore::connect(fixture_config(
        store.destination().clone(),
        &store.config.bucket,
        true,
    ))
    .await
    .unwrap();
    let identity = ArtifactIdentity::from_parts(
        ArtifactId::generate(),
        if reuse_attempt {
            original.attempt()
        } else {
            AttemptId::generate()
        },
        store.destination().id(),
        if reuse_attempt {
            ObjectVersion::generate()
        } else {
            original.version()
        },
    );
    assert!(matches!(
        reopened.stage(&identity, limits()).await,
        Err(ArtifactError::Conflict(_))
    ));
}

#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_append_retry_reuses_the_committed_block() {
    let store = fixture().await;
    let identity = ArtifactIdentity::new(store.destination().id());
    store.stage(&identity, limits()).await.unwrap();
    store.append(&identity, 0, b"original").await.unwrap();
    store.append(&identity, 0, b"original").await.unwrap();
    assert_eq!(store.load(&identity).await.unwrap().0.blocks.len(), 1);
}

#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_competing_appends_cannot_replace_the_winner() {
    let store = fixture().await;
    let identity = ArtifactIdentity::new(store.destination().id());
    store.stage(&identity, limits()).await.unwrap();
    let (first, second) = tokio::join!(
        store.append(&identity, 0, b"original"),
        store.append(&identity, 0, b"modified")
    );
    assert_ne!(first.is_ok(), second.is_ok());
}

#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_stale_conditional_state_cannot_reverse_abort() {
    let store = fixture().await;
    let identity = ArtifactIdentity::new(store.destination().id());
    store.stage(&identity, limits()).await.unwrap();
    let (state, etag) = store.load(&identity).await.unwrap();
    store.abort(&identity).await.unwrap();
    assert!(matches!(
        store.save(&state, Some(&etag)).await,
        Err(ArtifactError::Conflict(_))
    ));
}

#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_deleted_exact_version_cannot_be_hidden_by_a_delete_marker() {
    let store = fixture().await;
    let manifest = sealed(&store, b"original").await;
    let state = store.load(manifest.identity()).await.unwrap().0;
    let block = &state.blocks[0];
    store
        .client
        .delete_object()
        .bucket(&store.config.bucket)
        .key(&block.key)
        .send()
        .await
        .unwrap();
    // Version-pinned reads still find the actual bytes beneath the marker.
    verify(&store, &manifest).await.unwrap();
    store.abort(manifest.identity()).await.unwrap();
    assert_eq!(store.cleanup().await.unwrap(), 2);
    assert!(
        store
            .client
            .list_object_versions()
            .bucket(&store.config.bucket)
            .prefix(store.data_prefix(manifest.identity()))
            .send()
            .await
            .unwrap()
            .versions()
            .is_empty()
    );
}

#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_locked_copy_remains_a_cleanup_obligation() {
    let store = fixture().await;
    let manifest = sealed(&store, b"original").await;
    let block = store
        .load(manifest.identity())
        .await
        .unwrap()
        .0
        .blocks
        .remove(0);
    store
        .client
        .put_object_legal_hold()
        .bucket(&store.config.bucket)
        .key(&block.key)
        .version_id(&block.version)
        .legal_hold(
            ObjectLockLegalHold::builder()
                .status(ObjectLockLegalHoldStatus::On)
                .build(),
        )
        .send()
        .await
        .unwrap();
    store.abort(manifest.identity()).await.unwrap();
    assert!(store.cleanup().await.is_err());
    assert_eq!(
        store.load(manifest.identity()).await.unwrap().0.phase,
        Phase::Deleting
    );
}

#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_duplicate_exact_delete_is_idempotent() {
    let store = fixture().await;
    let manifest = sealed(&store, b"original").await;
    let block = store
        .load(manifest.identity())
        .await
        .unwrap()
        .0
        .blocks
        .remove(0);
    store.abort(manifest.identity()).await.unwrap();
    store
        .delete_version(&block.key, &block.version)
        .await
        .unwrap();
    assert!(
        store
            .delete_version(&block.key, &block.version)
            .await
            .is_ok()
    );
}

#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_missing_provider_version_fails_independent_verification() {
    let store = fixture().await;
    let manifest = sealed(&store, b"original").await;
    let block = store
        .load(manifest.identity())
        .await
        .unwrap()
        .0
        .blocks
        .remove(0);
    store
        .client
        .delete_object()
        .bucket(&store.config.bucket)
        .key(&block.key)
        .version_id(&block.version)
        .send()
        .await
        .unwrap();
    assert!(verify(&store, &manifest).await.is_err());
}

#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_corrupt_uploaded_bytes_cannot_create_a_verification_proof() {
    let store = fixture().await;
    let identity = ArtifactIdentity::new(store.destination().id());
    let mut manifest = ManifestBuilder::new(identity.clone(), limits());
    store.stage(&identity, limits()).await.unwrap();
    // Deliberately disagree with the independent expected producer manifest.
    store.append(&identity, 0, b"tampered").await.unwrap();
    manifest.push(b"original").unwrap();
    let manifest = manifest.finish().unwrap();
    store.seal(&manifest).await.unwrap();
    assert!(matches!(
        verify(&store, &manifest).await,
        Err(ArtifactError::Corrupt(_))
    ));
}

#[rstest]
#[case(false)]
#[case(true)]
#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_cleanup_aborts_interrupted_multipart_uploads(#[case] persist_upload_id: bool) {
    let store = fixture().await;
    let identity = ArtifactIdentity::new(store.destination().id());
    store.stage(&identity, limits()).await.unwrap();
    let upload_identity = Uuid::new_v4().to_string();
    let key = format!("{}{}", store.data_prefix(&identity), upload_identity);
    store
        .put_control(
            &format!("{}{}", store.intent_prefix(&identity), upload_identity),
            Vec::new(),
            None,
        )
        .await
        .unwrap();
    let upload = store
        .client
        .create_multipart_upload()
        .bucket(&store.config.bucket)
        .key(&key)
        .send()
        .await
        .unwrap();
    if persist_upload_id {
        let intent = format!("{}{}", store.intent_prefix(&identity), upload_identity);
        let (_, etag) = store.get_bytes(&intent, None, 4096).await.unwrap();
        store
            .put_control(
                &intent,
                serde_json::to_vec(&[upload.upload_id().unwrap()]).unwrap(),
                Some(&etag),
            )
            .await
            .unwrap();
    }
    store
        .client
        .upload_part()
        .bucket(&store.config.bucket)
        .key(&key)
        .upload_id(upload.upload_id().unwrap())
        .part_number(1)
        .body(ByteStream::from_static(b"partial"))
        .send()
        .await
        .unwrap();
    lifecycle("restart").await;
    // The durable intent finds the exact key even after real provider restart.
    let store = S3ArtifactStore::connect(fixture_config(
        store.destination().clone(),
        &store.config.bucket,
        true,
    ))
    .await
    .unwrap();
    assert_eq!(
        store
            .client
            .list_multipart_uploads()
            .bucket(&store.config.bucket)
            .prefix(&key)
            .send()
            .await
            .unwrap()
            .uploads()
            .len(),
        1
    );
    store.abort(&identity).await.unwrap();
    assert_eq!(store.cleanup().await.unwrap(), 1);
    assert!(
        store
            .client
            .list_multipart_uploads()
            .bucket(&store.config.bucket)
            .prefix(store.data_prefix(&identity))
            .send()
            .await
            .unwrap()
            .uploads()
            .is_empty()
    );
}

#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_cleanup_is_bounded_to_sixteen_physical_blocks() {
    let store = fixture().await;
    let identity = ArtifactIdentity::new(store.destination().id());
    store.stage(&identity, limits()).await.unwrap();
    for ordinal in 0..17 {
        store.append(&identity, ordinal, b"original").await.unwrap();
    }
    store.abort(&identity).await.unwrap();
    assert_eq!(store.cleanup().await.unwrap(), 16);
    // Reaching the end of the control listing wraps the local cursor.
    store.cleanup().await.unwrap();
    assert_eq!(store.cleanup().await.unwrap(), 1);
}

#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_expired_staging_is_fenced_before_cleanup() {
    let store = fixture().await;
    let identity = ArtifactIdentity::new(store.destination().id());
    store.stage(&identity, limits()).await.unwrap();
    store.append(&identity, 0, b"original").await.unwrap();
    let (mut state, etag) = store.load(&identity).await.unwrap();
    state.expires = now().unwrap() - 1;
    store.save(&state, Some(&etag)).await.unwrap();
    assert_eq!(store.cleanup().await.unwrap(), 1);
    assert!(store.stage(&identity, limits()).await.is_err());
}

#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_cancelled_uncommitted_upload_is_reconciled_after_restart() {
    let store = fixture().await;
    let identity = ArtifactIdentity::new(store.destination().id());
    store.stage(&identity, limits()).await.unwrap();
    // An upload finished but the caller disappeared before conditional append.
    store.upload(&identity, b"original").await.unwrap();
    store.abort(&identity).await.unwrap();
    lifecycle("restart").await;
    assert_eq!(store.cleanup().await.unwrap(), 1);
}

#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_published_orphan_cleanup_preserves_the_verified_version() {
    let store = fixture().await;
    let manifest = sealed(&store, b"original").await;
    store.upload(manifest.identity(), b"orphan").await.unwrap();
    let reference = verify_and_publish(&store, &manifest).await.unwrap();
    assert_eq!(store.cleanup().await.unwrap(), 1);
    assert!(
        ArtifactReader::open(&store, &reference)
            .await
            .unwrap()
            .next_block()
            .await
            .is_ok()
    );
}

#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_private_ca_is_required_for_fixture_tls() {
    let store = fixture().await;
    assert!(
        S3ArtifactStore::connect(fixture_config(
            store.destination().clone(),
            &store.config.bucket,
            false
        ))
        .await
        .is_err()
    );
}

#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_destination_default_change_does_not_rebind_old_references() {
    let first = fixture().await;
    let second = fixture().await;
    let manifest = sealed(&first, b"original").await;
    let reference = verify_and_publish(&first, &manifest).await.unwrap();
    assert!(ArtifactReader::open(&second, &reference).await.is_err());
    assert!(ArtifactReader::open(&first, &reference).await.is_ok());
}

#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_public_bucket_policy_is_rejected() {
    let store = fixture().await;
    let policy = serde_json::json!({"Version":"2012-10-17","Statement":[{
        "Effect":"Allow","Principal":"*","Action":["s3:GetObject"],
        "Resource":[format!("arn:aws:s3:::{}/*", store.config.bucket)]
    }]})
    .to_string();
    store
        .client
        .put_bucket_policy()
        .bucket(&store.config.bucket)
        .policy(policy)
        .send()
        .await
        .unwrap();
    assert!(
        S3ArtifactStore::connect(fixture_config(
            store.destination().clone(),
            &store.config.bucket,
            true
        ))
        .await
        .is_err()
    );
}

#[rstest]
#[case(false)]
#[case(true)]
#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_unversioned_or_suspended_bucket_is_rejected(#[case] suspended: bool) {
    let destination = Destination::new(
        DestinationId::generate(),
        format!("fixture-{}", Uuid::new_v4()),
    )
    .unwrap();
    let bucket = format!("fixture-{}", Uuid::new_v4());
    let config = fixture_config(destination, &bucket, true);
    let client = config.client().unwrap();
    client.create_bucket().bucket(&bucket).send().await.unwrap();
    if suspended {
        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Suspended)
                    .build(),
            )
            .send()
            .await
            .unwrap();
    }
    assert!(matches!(
        S3ArtifactStore::connect(config).await,
        Err(ArtifactError::Invalid(_))
    ));
}

#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_destination_identity_cannot_be_rebound() {
    let store = fixture().await;
    let different = Destination::new(store.destination().id(), "different-name").unwrap();
    assert!(matches!(
        S3ArtifactStore::connect(fixture_config(different, &store.config.bucket, true)).await,
        Err(ArtifactError::Conflict(_))
    ));
}

fn owned_intent(identity: ArtifactIdentity, operation: u8) -> ArtifactCreationIntent {
    ArtifactCreationIntent::new(identity, limits(), [operation; 16], [1; 16], [7; 32]).unwrap()
}
fn owned_receipt(admission: OwnedArtifactAdmission) -> OwnedArtifact {
    match admission {
        OwnedArtifactAdmission::Created(artifact)
        | OwnedArtifactAdmission::ResumedSameOwner { artifact, .. } => artifact,
    }
}
async fn owned_sealed(
    store: &S3ArtifactStore,
) -> (ArtifactCreationIntent, OwnedArtifact, ArtifactManifest) {
    let intent = owned_intent(ArtifactIdentity::new(store.destination().id()), 1);
    let artifact = owned_receipt(store.create_owned(&intent).await.unwrap());
    let session = artifact.session(store);
    let mut writer = ArtifactWriter::new(&session, intent.identity().clone(), limits())
        .await
        .unwrap();
    writer.write(b"original").await.unwrap();
    let manifest = writer.seal().await.unwrap();
    (intent, artifact, manifest)
}
#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_owned_publication_recovers_on_reconnect() {
    let store = fixture().await;
    let (intent, artifact, manifest) = owned_sealed(&store).await;
    let reference = store
        .publish_owned(&artifact, &verify(&store, &manifest).await.unwrap())
        .await
        .unwrap();
    let reopened = S3ArtifactStore::connect(fixture_config(
        store.destination().clone(),
        &store.config.bucket,
        true,
    ))
    .await
    .unwrap();
    assert!(
        matches!(reopened.resolve_owned(&intent).await.unwrap(), OwnedArtifactResolution::Established { phase: OwnedArtifactPhase::Published(found), .. } if found == reference)
    );
    let mut reader = ArtifactReader::open(&reopened, &reference).await.unwrap();
    assert_eq!(reader.next_block().await.unwrap().unwrap(), b"original");
}
#[rstest]
#[case("stage")]
#[case("append")]
#[case("seal")]
#[case("publish")]
#[case("abort")]
#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_all_legacy_mutations_reject_owned_mode(#[case] operation: &str) {
    let store = fixture().await;
    let intent = owned_intent(ArtifactIdentity::new(store.destination().id()), 1);
    let artifact = owned_receipt(store.create_owned(&intent).await.unwrap());
    let mut builder = ManifestBuilder::new(intent.identity().clone(), limits());
    builder.push(b"original").unwrap();
    let manifest = builder.finish().unwrap();
    if operation == "seal" || operation == "publish" {
        store.append_owned(&artifact, 0, b"original").await.unwrap();
    }
    if operation == "publish" {
        store.seal_owned(&artifact, &manifest).await.unwrap();
    }
    let result = match operation {
        "stage" => store.stage(intent.identity(), limits()).await,
        "append" => store.append(intent.identity(), 0, b"original").await,
        "seal" => store.seal(&manifest).await,
        "publish" => store
            .publish(&verify(&store, &manifest).await.unwrap())
            .await
            .map(|_| ()),
        "abort" => store.abort(intent.identity()).await,
        _ => unreachable!(),
    };
    assert!(result.is_err());
}
#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_owned_create_cannot_adopt_legacy_attempt() {
    let store = fixture().await;
    let intent = owned_intent(ArtifactIdentity::new(store.destination().id()), 1);
    store.stage(intent.identity(), limits()).await.unwrap();
    assert!(store.create_owned(&intent).await.is_err());
    store
        .append(intent.identity(), 0, b"original")
        .await
        .unwrap();
}
#[rstest]
#[case("operation")]
#[case("incarnation")]
#[case("request_commitment")]
#[case("limits")]
#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_owned_changed_intent_cannot_take_over(#[case] changed: &str) {
    let store = fixture().await;
    let intent = owned_intent(ArtifactIdentity::new(store.destination().id()), 1);
    let artifact = owned_receipt(store.create_owned(&intent).await.unwrap());
    let other = ArtifactCreationIntent::new(
        intent.identity().clone(),
        if changed == "limits" {
            ArtifactLimits::default()
        } else {
            limits()
        },
        if changed == "operation" {
            [2; 16]
        } else {
            [1; 16]
        },
        if changed == "incarnation" {
            [2; 16]
        } else {
            [1; 16]
        },
        if changed == "request_commitment" {
            [8; 32]
        } else {
            [7; 32]
        },
    )
    .unwrap();
    assert!(store.create_owned(&other).await.is_err());
    store.append_owned(&artifact, 0, b"original").await.unwrap();
}
#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_owned_preparing_state_is_resumed_after_create_reply_loss() {
    let store = fixture().await;
    let intent = owned_intent(ArtifactIdentity::new(store.destination().id()), 1);
    let l = limits();
    // Commit only the first conditional ownership write; simulate losing its reply.
    let state = State {
        owner: Some(intent.clone()),
        identity: intent.identity().clone(),
        limits: [
            u64::from(l.block_bytes()),
            l.artifact_bytes(),
            u64::from(l.blocks()),
            u64::from(l.manifest_bytes()),
        ],
        expires: 0,
        phase: Phase::Preparing,
        blocks: Vec::new(),
        manifest: None,
        cleanup_after: None,
    };
    store.save(&state, None).await.unwrap();
    assert!(matches!(
        store.resolve_owned(&intent).await.unwrap(),
        OwnedArtifactResolution::Pending
    ));
    assert!(
        store
            .create_owned(&owned_intent(intent.identity().clone(), 2))
            .await
            .is_err()
    );
    assert!(matches!(
        store.create_owned(&intent).await.unwrap(),
        OwnedArtifactAdmission::ResumedSameOwner {
            phase: OwnedArtifactPhase::Staging,
            ..
        }
    ));
}
#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_owned_expiry_does_not_authorize_cleanup() {
    let store = fixture().await;
    let intent = owned_intent(ArtifactIdentity::new(store.destination().id()), 1);
    let artifact = owned_receipt(store.create_owned(&intent).await.unwrap());
    store.append_owned(&artifact, 0, b"original").await.unwrap();
    store.cleanup().await.unwrap();
    assert!(matches!(
        store.resolve_owned(&intent).await.unwrap(),
        OwnedArtifactResolution::Established {
            phase: OwnedArtifactPhase::Staging,
            ..
        }
    ));
}
#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_owned_abort_and_publication_have_one_winner() {
    let store = fixture().await;
    let (_, artifact, manifest) = owned_sealed(&store).await;
    let proof = verify(&store, &manifest).await.unwrap();
    let (published, aborted) = tokio::join!(
        store.publish_owned(&artifact, &proof),
        store.abort_owned(&artifact)
    );
    assert_ne!(published.is_ok(), aborted.is_ok());
}
#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_owned_tombstone_retains_owner_and_identity_claims() {
    let store = fixture().await;
    let (intent, artifact, _) = owned_sealed(&store).await;
    store.abort_owned(&artifact).await.unwrap();
    store.cleanup().await.unwrap();
    assert!(matches!(
        store.create_owned(&intent).await.unwrap(),
        OwnedArtifactAdmission::ResumedSameOwner {
            phase: OwnedArtifactPhase::Fenced,
            ..
        }
    ));
    assert!(
        store
            .create_owned(&owned_intent(intent.identity().clone(), 2))
            .await
            .is_err()
    );
}
#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_owned_orphan_cleanup_preserves_exact_published_version() {
    let store = fixture().await;
    let (_, artifact, manifest) = owned_sealed(&store).await;
    let reference = store
        .publish_owned(&artifact, &verify(&store, &manifest).await.unwrap())
        .await
        .unwrap();
    store.upload(manifest.identity(), b"orphan").await.unwrap();
    store.cleanup().await.unwrap();
    let mut reader = ArtifactReader::open(&store, &reference).await.unwrap();
    assert_eq!(reader.next_block().await.unwrap().unwrap(), b"original");
}
// The exact landed field set and phases are intentionally pinned here. Every
// landed mutation/cleanup path loads this deny-unknown-fields control record.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyS3State {
    identity: ArtifactIdentity,
    limits: [u64; 4],
    expires: u64,
    phase: LegacyS3Phase,
    blocks: Vec<Block>,
    manifest: Option<Vec<u8>>,
    cleanup_after: Option<String>,
}
#[derive(Deserialize)]
enum LegacyS3Phase {
    Staging,
    Sealed,
    Published,
    Deleting,
}
impl LegacyS3State {
    fn decode(bytes: &[u8]) -> ArtifactResult<State> {
        let old: Self = serde_json::from_slice(bytes)
            .map_err(|_| ArtifactError::Corrupt("legacy S3 state decode failed".into()))?;
        let phase = match old.phase {
            LegacyS3Phase::Staging => Phase::Staging,
            LegacyS3Phase::Sealed => Phase::Sealed,
            LegacyS3Phase::Published => Phase::Published,
            LegacyS3Phase::Deleting => Phase::Deleting,
        };
        let state = State {
            owner: None,
            identity: old.identity,
            limits: old.limits,
            expires: old.expires,
            phase,
            blocks: old.blocks,
            manifest: old.manifest,
            cleanup_after: old.cleanup_after,
        };
        State::decode(&serde_json::to_vec(&state).unwrap())
    }
}
#[test]
fn owned_state_is_rejected_by_landed_s3_decoder() {
    let identity = ArtifactIdentity::new(DestinationId::generate());
    let intent = owned_intent(identity.clone(), 1);
    let l = limits();
    let state = State {
        owner: Some(intent),
        identity,
        limits: [
            u64::from(l.block_bytes()),
            l.artifact_bytes(),
            u64::from(l.blocks()),
            u64::from(l.manifest_bytes()),
        ],
        expires: 0,
        phase: Phase::Staging,
        blocks: Vec::new(),
        manifest: None,
        cleanup_after: None,
    };
    let mut wire = serde_json::to_value(state).unwrap();
    assert!(LegacyS3State::decode(&serde_json::to_vec(&wire).unwrap()).is_err());
    wire.as_object_mut().unwrap().remove("owner");
    assert!(LegacyS3State::decode(&serde_json::to_vec(&wire).unwrap()).is_ok());
}
#[rstest]
#[case("create")]
#[case("append")]
#[case("seal")]
#[case("publish")]
#[case("abort")]
#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_owned_committed_state_with_lost_reply_is_recoverable(#[case] operation: &str) {
    use std::sync::atomic::Ordering;
    let store = fixture().await;
    let intent = owned_intent(ArtifactIdentity::new(store.destination().id()), 1);
    if operation == "create" {
        store.lose_save_reply.store(true, Ordering::SeqCst);
        assert!(store.create_owned(&intent).await.is_err());
    }
    let artifact = owned_receipt(store.create_owned(&intent).await.unwrap());
    if operation == "append" {
        store.lose_save_reply.store(true, Ordering::SeqCst);
        assert!(store.append_owned(&artifact, 0, b"original").await.is_err());
    }
    store.append_owned(&artifact, 0, b"original").await.unwrap();
    let mut builder = ManifestBuilder::new(intent.identity().clone(), limits());
    builder.push(b"original").unwrap();
    let manifest = builder.finish().unwrap();
    if operation == "seal" {
        store.lose_save_reply.store(true, Ordering::SeqCst);
        assert!(store.seal_owned(&artifact, &manifest).await.is_err());
    }
    store.seal_owned(&artifact, &manifest).await.unwrap();
    if operation == "abort" {
        store.lose_save_reply.store(true, Ordering::SeqCst);
        assert!(store.abort_owned(&artifact).await.is_err());
        assert!(matches!(
            store.resolve_owned(&intent).await.unwrap(),
            OwnedArtifactResolution::Established {
                phase: OwnedArtifactPhase::Fenced,
                ..
            }
        ));
    } else {
        let proof = verify(&store, &manifest).await.unwrap();
        if operation == "publish" {
            store.lose_save_reply.store(true, Ordering::SeqCst);
            assert!(store.publish_owned(&artifact, &proof).await.is_err());
        }
        let reference = store.publish_owned(&artifact, &proof).await.unwrap();
        assert!(
            matches!(store.resolve_owned(&intent).await.unwrap(), OwnedArtifactResolution::Established { phase: OwnedArtifactPhase::Published(found), .. } if found == reference)
        );
    }
}

#[rstest]
#[case(true)]
#[case(false)]
#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_owned_cross_artifact_identity_collision_survives_cleanup(
    #[case] reuse_attempt: bool,
) {
    let store = fixture().await;
    let (intent, artifact, _) = owned_sealed(&store).await;
    store.abort_owned(&artifact).await.unwrap();
    store.cleanup().await.unwrap();
    let fresh = ArtifactIdentity::new(store.destination().id());
    let collision = ArtifactIdentity::from_parts(
        fresh.artifact(),
        if reuse_attempt {
            intent.identity().attempt()
        } else {
            fresh.attempt()
        },
        fresh.destination(),
        if reuse_attempt {
            fresh.version()
        } else {
            intent.identity().version()
        },
    );
    assert!(
        store
            .create_owned(&owned_intent(collision, 2))
            .await
            .is_err()
    );
}
#[tokio::test]
#[ignore = "requires pinned TLS S3 fixture"]
async fn live_owned_receipt_cannot_publish_corrupt_bytes() {
    let store = fixture().await;
    let intent = owned_intent(ArtifactIdentity::new(store.destination().id()), 1);
    let artifact = owned_receipt(store.create_owned(&intent).await.unwrap());
    store.append_owned(&artifact, 0, b"tampered").await.unwrap();
    let mut builder = ManifestBuilder::new(intent.identity().clone(), limits());
    builder.push(b"original").unwrap();
    let manifest = builder.finish().unwrap();
    store.seal_owned(&artifact, &manifest).await.unwrap();
    assert!(verify(&store, &manifest).await.is_err());
}
