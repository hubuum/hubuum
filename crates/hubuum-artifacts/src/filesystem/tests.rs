use super::*;
use crate::{
    ArtifactReader, ArtifactWriter, AttemptId, ManifestBuilder, stage_producer, verify,
    verify_and_publish,
};
use rstest::rstest;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::sync::atomic::Ordering;
use tempfile::TempDir;

struct Fixture {
    root: TempDir,
    store: FilesystemArtifactStore,
    destination: Destination,
}
impl Fixture {
    async fn new() -> Self {
        let root = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap();
        let store = FilesystemArtifactStore::open(root.path().canonicalize().unwrap())
            .await
            .unwrap();
        let destination = Destination::new(DestinationId::generate(), "local").unwrap();
        store.register_destination(&destination).await.unwrap();
        Self {
            root,
            store,
            destination,
        }
    }
    fn identity(&self) -> ArtifactIdentity {
        ArtifactIdentity::new(self.destination.id())
    }
    async fn reopen(&self) -> FilesystemArtifactStore {
        FilesystemArtifactStore::open(self.root.path().canonicalize().unwrap())
            .await
            .unwrap()
    }
    fn directory(&self, identity: &ArtifactIdentity) -> std::path::PathBuf {
        self.root
            .path()
            .join("artifacts")
            .join(key(identity.artifact().as_bytes()))
    }
    async fn sealed(&self, bytes: &[u8]) -> ArtifactManifest {
        let mut writer = ArtifactWriter::new(&self.store, self.identity(), limits())
            .await
            .unwrap();
        writer.write(bytes).await.unwrap();
        writer.seal().await.unwrap()
    }
}
fn limits() -> ArtifactLimits {
    ArtifactLimits::builder()
        .block_bytes(4)
        .artifact_bytes(12)
        .blocks(3)
        .build()
        .unwrap()
}
async fn drain(store: &impl ArtifactStore, reference: &ArtifactReference) -> Vec<u8> {
    let mut reader = ArtifactReader::open(store, reference).await.unwrap();
    let mut result = Vec::new();
    while let Some(block) = reader.next_block().await.unwrap() {
        result.extend(block);
    }
    result
}

#[rstest]
#[case(b"".as_slice())]
#[case(b"abcd".as_slice())]
#[case(b"abcdefghij".as_slice())]
#[tokio::test]
async fn protocol_roundtrip_survives_reopen(#[case] bytes: &[u8]) {
    let fixture = Fixture::new().await;
    let mut writer = ArtifactWriter::new(&fixture.store, fixture.identity(), limits())
        .await
        .unwrap();
    for fragment in bytes.chunks(3) {
        writer.write(fragment).await.unwrap();
    }
    let reference = writer.finish().await.unwrap();
    assert_eq!(drain(&fixture.reopen().await, &reference).await, bytes);
}

#[tokio::test]
async fn producer_bridge_uses_the_same_verification_protocol() {
    let fixture = Fixture::new().await;
    let (verified, ()) = stage_producer(&fixture.store, fixture.identity(), limits(), |writer| {
        writer
            .write_all(b"abcdefghij")
            .map_err(|e| ArtifactError::Backend(e.to_string()))
    })
    .await
    .unwrap();
    let reference = fixture.store.publish(&verified).await.unwrap();
    assert_eq!(drain(&fixture.store, &reference).await, b"abcdefghij");
}

#[tokio::test]
async fn sealed_attempt_resumes_on_another_instance() {
    let fixture = Fixture::new().await;
    let manifest = fixture.sealed(b"abcdefghij").await;
    let other = fixture.reopen().await;
    let persisted = other.manifest(manifest.identity()).await.unwrap();
    let recovered = ArtifactManifest::decode(&persisted, manifest.identity(), limits()).unwrap();
    let reference = verify_and_publish(&other, &recovered).await.unwrap();
    assert_eq!(drain(&fixture.store, &reference).await, b"abcdefghij");
}

#[tokio::test]
async fn sealed_bytes_are_not_downloadable() {
    let fixture = Fixture::new().await;
    let manifest = fixture.sealed(b"abcd").await;
    let reference =
        ArtifactReference::from_catalog(manifest.identity().clone(), manifest.manifest_digest());
    assert!(
        ArtifactReader::open(&fixture.store, &reference)
            .await
            .is_err()
    );
}

#[rstest]
#[case(false)]
#[case(true)]
#[tokio::test]
async fn concurrent_append_retries_cannot_overwrite(#[case] conflicting: bool) {
    let fixture = Fixture::new().await;
    let identity = fixture.identity();
    fixture.store.stage(&identity, limits()).await.unwrap();
    let other = fixture.reopen().await;
    let (a, b) = tokio::join!(
        fixture.store.append(&identity, 0, b"abcd"),
        other.append(&identity, 0, if conflicting { b"efgh" } else { b"abcd" })
    );
    assert_eq!(
        u8::from(a.is_ok()) + u8::from(b.is_ok()),
        if conflicting { 1 } else { 2 }
    );
}

#[tokio::test]
async fn abort_and_publish_have_one_winner() {
    let fixture = Fixture::new().await;
    let manifest = fixture.sealed(b"abcd").await;
    let proof = verify(&fixture.store, &manifest).await.unwrap();
    let other = fixture.reopen().await;
    let (a, b) = tokio::join!(
        fixture.store.publish(&proof),
        other.abort(manifest.identity())
    );
    assert_ne!(a.is_ok(), b.is_ok());
}

#[tokio::test]
async fn old_proof_cannot_bypass_abort() {
    let fixture = Fixture::new().await;
    let manifest = fixture.sealed(b"abcd").await;
    let proof = verify(&fixture.store, &manifest).await.unwrap();
    fixture.store.abort(manifest.identity()).await.unwrap();
    assert!(fixture.reopen().await.publish(&proof).await.is_err());
}

#[rstest]
#[case(true)]
#[case(false)]
#[tokio::test]
async fn destinations_cannot_be_rebound(#[case] same_id: bool) {
    let fixture = Fixture::new().await;
    let replacement = Destination::new(
        if same_id {
            fixture.destination.id()
        } else {
            DestinationId::generate()
        },
        if same_id { "different" } else { "local" },
    )
    .unwrap();
    assert!(
        fixture
            .store
            .register_destination(&replacement)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn artifact_identity_cannot_be_rebound() {
    let fixture = Fixture::new().await;
    let identity = fixture.identity();
    fixture.store.stage(&identity, limits()).await.unwrap();
    let forged = ArtifactIdentity::from_parts(
        identity.artifact(),
        AttemptId::generate(),
        identity.destination(),
        identity.version(),
    );
    assert!(fixture.store.stage(&forged, limits()).await.is_err());
}

#[rstest]
#[case::unknown(0, b"abcd".as_slice())]
#[case::out_of_order(1, b"abcd".as_slice())]
#[case::oversize(0, b"abcde".as_slice())]
#[tokio::test]
async fn invalid_append_is_rejected(#[case] ordinal: u32, #[case] bytes: &[u8]) {
    let fixture = Fixture::new().await;
    let identity = fixture.identity();
    // The unknown case has no durable stage.
    if ordinal != 0 || bytes.len() != 4 {
        fixture.store.stage(&identity, limits()).await.unwrap();
    }
    assert!(
        fixture
            .store
            .append(&identity, ordinal, bytes)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn a_short_final_block_cannot_be_extended() {
    let fixture = Fixture::new().await;
    let identity = fixture.identity();
    fixture.store.stage(&identity, limits()).await.unwrap();
    fixture.store.append(&identity, 0, b"ab").await.unwrap();
    assert!(fixture.store.append(&identity, 1, b"cd").await.is_err());
}

#[tokio::test]
async fn sealing_prevents_even_identical_appends() {
    let fixture = Fixture::new().await;
    let manifest = fixture.sealed(b"abcd").await;
    assert!(
        fixture
            .store
            .append(manifest.identity(), 0, b"abcd")
            .await
            .is_err()
    );
}

#[rstest]
#[case::manifest("manifest", false)]
#[case::block("00000000.block", false)]
#[case::missing_block("00000000.block", true)]
#[tokio::test]
async fn corrupt_destination_cannot_finish_download(#[case] name: &str, #[case] missing: bool) {
    let fixture = Fixture::new().await;
    let manifest = fixture.sealed(b"abcd").await;
    let reference = verify_and_publish(&fixture.store, &manifest).await.unwrap();
    let path = fixture.directory(manifest.identity()).join(name);
    if missing {
        std::fs::remove_file(path).unwrap();
    } else {
        std::fs::write(path, b"efgh").unwrap();
    }
    let result = async {
        let mut reader = ArtifactReader::open(&fixture.store, &reference).await?;
        while reader.next_block().await?.is_some() {}
        Ok::<_, ArtifactError>(())
    }
    .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn wrong_catalog_digest_cannot_open() {
    let fixture = Fixture::new().await;
    let manifest = fixture.sealed(b"abcd").await;
    verify_and_publish(&fixture.store, &manifest).await.unwrap();
    let reference = ArtifactReference::from_catalog(manifest.identity().clone(), [0; 32]);
    assert!(
        ArtifactReader::open(&fixture.store, &reference)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn aborting_unknown_attempt_reports_no_fence() {
    let fixture = Fixture::new().await;
    assert_eq!(
        fixture.store.abort(&fixture.identity()).await,
        Err(ArtifactError::NotFound)
    );
}

#[tokio::test]
async fn cleanup_is_bounded_and_tombstones_survive_restart() {
    let fixture = Fixture::new().await;
    let identity = fixture.identity();
    let limits = ArtifactLimits::builder()
        .block_bytes(1)
        .artifact_bytes(17)
        .blocks(17)
        .build()
        .unwrap();
    fixture.store.stage(&identity, limits).await.unwrap();
    for ordinal in 0..17 {
        fixture
            .store
            .append(&identity, ordinal, b"x")
            .await
            .unwrap();
    }
    fixture.store.abort(&identity).await.unwrap();
    assert_eq!(fixture.reopen().await.cleanup().await.unwrap(), 16);
    assert_eq!(fixture.reopen().await.cleanup().await.unwrap(), 1);
    let reopened = fixture.reopen().await;
    assert_eq!(reopened.cleanup().await.unwrap(), 0);
    assert!(reopened.stage(&identity, limits).await.is_err());
}

#[tokio::test]
async fn cleanup_retains_published_artifacts() {
    let fixture = Fixture::new().await;
    let manifest = fixture.sealed(b"abcd").await;
    let reference = verify_and_publish(&fixture.store, &manifest).await.unwrap();
    fixture.store.cleanup().await.unwrap();
    assert_eq!(drain(&fixture.store, &reference).await, b"abcd");
}

#[tokio::test]
async fn expired_staging_cannot_publish_and_is_reclaimed() {
    let fixture = Fixture::new().await;
    let manifest = fixture.sealed(b"abcd").await;
    let proof = verify(&fixture.store, &manifest).await.unwrap();
    let identity = manifest.identity().clone();
    fixture
        .store
        .run(move |store| {
            let (directory, mut state) = store.state(&identity)?;
            state.expires = 0;
            store.save(&directory, &state)
        })
        .await
        .unwrap();
    assert!(fixture.store.publish(&proof).await.is_err());
    assert_eq!(fixture.store.cleanup().await.unwrap(), 1);
}

#[rstest]
#[case("state")]
#[case("manifest")]
#[case("00000000.block")]
#[tokio::test]
async fn symlinked_artifact_files_are_not_followed(#[case] name: &str) {
    let fixture = Fixture::new().await;
    let manifest = fixture.sealed(b"abcd").await;
    let outside = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(outside.path(), b"sentinel").unwrap();
    let path = fixture.directory(manifest.identity()).join(name);
    std::fs::remove_file(&path).unwrap();
    symlink(outside.path(), path).unwrap();
    assert!(verify_and_publish(&fixture.store, &manifest).await.is_err());
    assert_eq!(std::fs::read(outside.path()).unwrap(), b"sentinel");
}

#[tokio::test]
async fn symlinked_pending_cannot_truncate_an_outside_file() {
    let fixture = Fixture::new().await;
    let identity = fixture.identity();
    fixture.store.stage(&identity, limits()).await.unwrap();
    let outside = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(outside.path(), b"sentinel").unwrap();
    symlink(outside.path(), fixture.directory(&identity).join("pending")).unwrap();
    assert!(fixture.store.append(&identity, 0, b"abcd").await.is_err());
    assert_eq!(std::fs::read(outside.path()).unwrap(), b"sentinel");
}

#[tokio::test]
async fn hardlinked_pending_cannot_truncate_an_outside_file() {
    let fixture = Fixture::new().await;
    let identity = fixture.identity();
    fixture.store.stage(&identity, limits()).await.unwrap();
    let outside = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(outside.path(), b"sentinel").unwrap();
    std::fs::hard_link(outside.path(), fixture.directory(&identity).join("pending")).unwrap();
    assert!(fixture.store.append(&identity, 0, b"abcd").await.is_err());
    assert_eq!(std::fs::read(outside.path()).unwrap(), b"sentinel");
}

#[tokio::test]
async fn symlinked_attempt_directory_cannot_escape_root() {
    let fixture = Fixture::new().await;
    let outside = tempfile::tempdir().unwrap();
    let identity = fixture.identity();
    symlink(outside.path(), fixture.directory(&identity)).unwrap();
    assert!(fixture.store.stage(&identity, limits()).await.is_err());
    assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn symlinked_root_component_is_rejected() {
    let parent = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    symlink(outside.path(), parent.path().join("link")).unwrap();
    assert!(
        FilesystemArtifactStore::open(parent.path().canonicalize().unwrap().join("link"))
            .await
            .is_err()
    );
}

#[rstest]
#[case("relative/root")]
#[case("/../tmp")]
#[tokio::test]
async fn unsafe_root_path_is_rejected(#[case] path: &str) {
    assert!(FilesystemArtifactStore::open(path).await.is_err());
}

#[rstest]
#[case(0o755)]
#[case(0o777)]
#[tokio::test]
async fn root_requires_private_permissions(#[case] mode: u32) {
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(mode)).unwrap();
    assert!(
        FilesystemArtifactStore::open(root.path().canonicalize().unwrap())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn permission_failure_is_reported_without_false_publication() {
    let fixture = Fixture::new().await;
    let identity = fixture.identity();
    fixture.store.stage(&identity, limits()).await.unwrap();
    let directory = fixture.directory(&identity);
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o500)).unwrap();
    assert!(fixture.store.append(&identity, 0, b"abcd").await.is_err());
    std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700)).unwrap();
}

#[rstest]
#[case(0)]
#[case(1)]
#[case(2)]
#[case(3)]
#[tokio::test]
async fn append_sync_failure_is_retryable(#[case] after: isize) {
    let fixture = Fixture::new().await;
    let identity = fixture.identity();
    fixture.store.stage(&identity, limits()).await.unwrap();
    fixture.store.fail_sync.store(after, Ordering::SeqCst);
    assert!(fixture.store.append(&identity, 0, b"abcd").await.is_err());
    let other = fixture.reopen().await;
    other.append(&identity, 0, b"abcd").await.unwrap();
    let mut builder = ManifestBuilder::new(identity, limits());
    builder.push(b"abcd").unwrap();
    let manifest = builder.finish().unwrap();
    other.seal(&manifest).await.unwrap();
    let reference = verify_and_publish(&other, &manifest).await.unwrap();
    assert_eq!(drain(&other, &reference).await, b"abcd");
}

#[rstest]
#[case(0)]
#[case(1)]
#[case(2)]
#[case(3)]
#[tokio::test]
async fn seal_sync_failure_can_resume(#[case] after: isize) {
    let fixture = Fixture::new().await;
    let identity = fixture.identity();
    fixture.store.stage(&identity, limits()).await.unwrap();
    fixture.store.append(&identity, 0, b"abcd").await.unwrap();
    let mut builder = ManifestBuilder::new(identity, limits());
    builder.push(b"abcd").unwrap();
    let manifest = builder.finish().unwrap();
    fixture.store.fail_sync.store(after, Ordering::SeqCst);
    assert!(fixture.store.seal(&manifest).await.is_err());
    let other = fixture.reopen().await;
    other.seal(&manifest).await.unwrap();
    assert!(verify_and_publish(&other, &manifest).await.is_ok());
}

#[rstest]
#[case(0)]
#[case(1)]
#[tokio::test]
async fn publication_sync_failure_never_reports_success(#[case] after: isize) {
    let fixture = Fixture::new().await;
    let manifest = fixture.sealed(b"abcd").await;
    let proof = verify(&fixture.store, &manifest).await.unwrap();
    fixture.store.fail_sync.store(after, Ordering::SeqCst);
    assert!(fixture.store.publish(&proof).await.is_err());
    let other = fixture.reopen().await;
    let reference = other.publish(&proof).await.unwrap();
    assert_eq!(drain(&other, &reference).await, b"abcd");
}

#[rstest]
#[case(0)]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(4)]
#[case(5)]
#[tokio::test]
async fn interrupted_cleanup_retains_fence(#[case] after: isize) {
    let fixture = Fixture::new().await;
    let manifest = fixture.sealed(b"abcdefghij").await;
    fixture.store.abort(manifest.identity()).await.unwrap();
    fixture.store.fail_sync.store(after, Ordering::SeqCst);
    assert!(fixture.store.cleanup().await.is_err());
    let other = fixture.reopen().await;
    other.cleanup().await.unwrap();
    assert!(other.stage(manifest.identity(), limits()).await.is_err());
    assert_eq!(
        std::fs::read_dir(fixture.directory(manifest.identity()))
            .unwrap()
            .count(),
        1
    );
}

#[test]
fn process_loss_recovers_renamed_block_and_releases_lock() {
    const CHILD_ROOT: &str = "ARTIFACT_FILESYSTEM_TEST_CRASH_ROOT";
    const CHILD_ID: &str = "ARTIFACT_FILESYSTEM_TEST_CRASH_ID";
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    if let Some(path) = std::env::var_os(CHILD_ROOT) {
        runtime.block_on(async {
            let store = FilesystemArtifactStore::open(path).await.unwrap();
            let identity: ArtifactIdentity =
                serde_json::from_str(&std::env::var(CHILD_ID).unwrap()).unwrap();
            store
                .run(move |store| -> Result<()> {
                    let (directory, _) = store.state(&identity)?;
                    store.atomic(&directory, &block_key(0), b"abcd")?;
                    // No destructors: simulate death after block rename, before state save.
                    std::process::exit(0);
                })
                .await
                .unwrap();
        });
        unreachable!();
    }
    let fixture = runtime.block_on(Fixture::new());
    let identity = fixture.identity();
    runtime
        .block_on(fixture.store.stage(&identity, limits()))
        .unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "filesystem::tests::process_loss_recovers_renamed_block_and_releases_lock",
        ])
        .env(CHILD_ROOT, fixture.root.path().canonicalize().unwrap())
        .env(CHILD_ID, serde_json::to_string(&identity).unwrap())
        .status()
        .unwrap();
    assert!(status.success());
    runtime.block_on(async {
        let other = fixture.reopen().await;
        other.append(&identity, 0, b"abcd").await.unwrap();
        let mut builder = ManifestBuilder::new(identity, limits());
        builder.push(b"abcd").unwrap();
        let manifest = builder.finish().unwrap();
        other.seal(&manifest).await.unwrap();
        let reference = verify_and_publish(&other, &manifest).await.unwrap();
        assert_eq!(drain(&other, &reference).await, b"abcd");
    });
}

#[tokio::test]
async fn stage_retry_preserves_the_original_deadline() {
    let fixture = Fixture::new().await;
    let identity = fixture.identity();
    fixture.store.stage(&identity, limits()).await.unwrap();
    let before = std::fs::read(fixture.directory(&identity).join("state")).unwrap();
    fixture
        .reopen()
        .await
        .stage(&identity, limits())
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(fixture.directory(&identity).join("state")).unwrap(),
        before
    );
}

#[tokio::test]
async fn pinned_root_is_not_redirected_by_path_replacement() {
    let fixture = Fixture::new().await;
    let original = fixture.root.path().canonicalize().unwrap();
    let moved = original.with_extension("moved");
    std::fs::rename(&original, &moved).unwrap();
    let outside = tempfile::tempdir().unwrap();
    symlink(outside.path(), &original).unwrap();
    {
        let manifest = fixture.sealed(b"abcd").await;
        let reference = verify_and_publish(&fixture.store, &manifest).await.unwrap();
        assert_eq!(drain(&fixture.store, &reference).await, b"abcd");
    }
    std::fs::remove_file(original).unwrap();
    std::fs::remove_dir_all(moved).unwrap();
    assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 0);
}
