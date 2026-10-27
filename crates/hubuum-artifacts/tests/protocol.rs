use std::sync::Mutex;

use async_trait::async_trait;
use hubuum_artifacts::{
    ArtifactError, ArtifactIdentity, ArtifactLimits, ArtifactManifest, ArtifactReader,
    ArtifactReference, ArtifactResult, ArtifactStore, ArtifactWriter, Destination, DestinationId,
    ManifestBuilder, VerifiedArtifact, verify_and_publish,
};
use rstest::rstest;

// A deliberately focused byte-store fault fixture, not a selectable backend.
#[derive(Default)]
struct FaultStore {
    state: Mutex<State>,
}
#[derive(Default)]
struct State {
    blocks: Vec<Vec<u8>>,
    manifest: Vec<u8>,
    sealed: bool,
    published: bool,
    aborted: bool,
    fault: Option<Fault>,
}
#[derive(Clone, Copy, Debug)]
enum Fault {
    Missing,
    Reorder,
    Duplicate,
    Substitute,
    Extra,
    Short,
    Manifest,
    StreamDigest,
    Unavailable,
}

#[async_trait]
impl ArtifactStore for FaultStore {
    async fn register_destination(&self, _: &Destination) -> ArtifactResult<()> {
        Ok(())
    }
    async fn stage(&self, _: &ArtifactIdentity, _: ArtifactLimits) -> ArtifactResult<()> {
        Ok(())
    }
    async fn append(&self, _: &ArtifactIdentity, ordinal: u32, bytes: &[u8]) -> ArtifactResult<()> {
        let mut state = self.state.lock().unwrap();
        assert!(!state.sealed);
        assert_eq!(ordinal as usize, state.blocks.len());
        state.blocks.push(bytes.to_vec());
        Ok(())
    }
    async fn seal(&self, manifest: &ArtifactManifest) -> ArtifactResult<()> {
        let mut state = self.state.lock().unwrap();
        state.sealed = true;
        state.manifest = manifest.encode();
        Ok(())
    }
    async fn manifest(&self, _: &ArtifactIdentity) -> ArtifactResult<Vec<u8>> {
        let state = self.state.lock().unwrap();
        let mut bytes = state.manifest.clone();
        match state.fault {
            Some(Fault::Manifest) => bytes[5] ^= 1,
            Some(Fault::StreamDigest) => bytes[81] ^= 1,
            _ => {}
        }
        Ok(bytes)
    }
    async fn published_manifest(&self, reference: &ArtifactReference) -> ArtifactResult<Vec<u8>> {
        if !self.state.lock().unwrap().published {
            return Err(ArtifactError::NotFound);
        }
        self.manifest(reference.identity()).await
    }
    async fn block(&self, _: &ArtifactIdentity, ordinal: u32) -> ArtifactResult<Option<Vec<u8>>> {
        let state = self.state.lock().unwrap();
        let index = ordinal as usize;
        if matches!(state.fault, Some(Fault::Unavailable)) {
            return Err(ArtifactError::Backend(
                "simulated destination failure".into(),
            ));
        }
        let block = match state.fault {
            Some(Fault::Missing) if index == 2 => None,
            Some(Fault::Reorder) if index < 2 => state.blocks.get(1 - index).cloned(),
            Some(Fault::Duplicate) if index == 1 => state.blocks.first().cloned(),
            Some(Fault::Extra) if index == state.blocks.len() => Some(vec![0]),
            _ => state.blocks.get(index).cloned(),
        };
        Ok(block.map(|mut bytes| {
            match state.fault {
                Some(Fault::Substitute) => bytes[0] ^= 1,
                Some(Fault::Short) => {
                    bytes.pop();
                }
                _ => {}
            }
            bytes
        }))
    }
    async fn publish(&self, verified: &VerifiedArtifact) -> ArtifactResult<ArtifactReference> {
        let mut state = self.state.lock().unwrap();
        if state.aborted {
            return Err(ArtifactError::Conflict("aborted".into()));
        }
        assert!(state.sealed);
        assert_eq!(state.manifest, verified.manifest().encode());
        state.published = true;
        Ok(ArtifactReference::from_catalog(
            verified.manifest().identity().clone(),
            verified.manifest().manifest_digest(),
        ))
    }
    async fn abort(&self, _: &ArtifactIdentity) -> ArtifactResult<()> {
        self.state.lock().unwrap().aborted = true;
        Ok(())
    }
    async fn cleanup(&self) -> ArtifactResult<u32> {
        Ok(0)
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
fn identity() -> ArtifactIdentity {
    ArtifactIdentity::new(DestinationId::generate())
}
fn manifest(id: ArtifactIdentity, bytes: &[u8]) -> ArtifactManifest {
    let mut builder = ManifestBuilder::new(id, limits());
    for block in bytes.chunks(4) {
        builder.push(block).unwrap();
    }
    builder.finish().unwrap()
}
async fn sealed(store: &FaultStore) -> ArtifactManifest {
    let manifest = manifest(identity(), b"abcdefghij");
    for (ordinal, bytes) in b"abcdefghij".chunks(4).enumerate() {
        store
            .append(manifest.identity(), ordinal as u32, bytes)
            .await
            .unwrap();
    }
    store.seal(&manifest).await.unwrap();
    manifest
}

#[rstest]
#[case(b"".as_slice())]
#[case(b"abcd".as_slice())]
#[case(b"abcdefghij".as_slice())]
#[case(b"abcdefghijkl".as_slice())]
#[tokio::test]
async fn arbitrary_producer_fragments_round_trip(#[case] bytes: &[u8]) {
    let store = FaultStore::default();
    let mut writer = ArtifactWriter::new(&store, identity(), limits())
        .await
        .unwrap();
    for fragment in bytes.chunks(3) {
        writer.write(fragment).await.unwrap();
    }
    let reference = writer.finish().await.unwrap();
    let mut reader = ArtifactReader::open(&store, &reference).await.unwrap();
    let mut actual = Vec::new();
    while let Some(block) = reader.next_block().await.unwrap() {
        assert!(block.len() <= 4);
        actual.extend(block);
    }
    assert_eq!(actual, bytes);
}

#[rstest]
#[case(Fault::Missing)]
#[case(Fault::Reorder)]
#[case(Fault::Duplicate)]
#[case(Fault::Substitute)]
#[case(Fault::Extra)]
#[case(Fault::Short)]
#[case(Fault::Manifest)]
#[case(Fault::StreamDigest)]
#[case(Fault::Unavailable)]
#[tokio::test]
async fn readback_fault_never_publishes(#[case] fault: Fault) {
    let store = FaultStore::default();
    let manifest = sealed(&store).await;
    store.state.lock().unwrap().fault = Some(fault);
    assert!(verify_and_publish(&store, &manifest).await.is_err());
    assert!(!store.state.lock().unwrap().published);
}

#[rstest]
#[case(Fault::Missing)]
#[case(Fault::Reorder)]
#[case(Fault::Duplicate)]
#[case(Fault::Substitute)]
#[case(Fault::Extra)]
#[case(Fault::Short)]
#[case(Fault::Unavailable)]
#[tokio::test]
async fn download_fault_cannot_become_success_on_retry(#[case] fault: Fault) {
    let store = FaultStore::default();
    let manifest = sealed(&store).await;
    let reference = verify_and_publish(&store, &manifest).await.unwrap();
    store.state.lock().unwrap().fault = Some(fault);
    let mut reader = ArtifactReader::open(&store, &reference).await.unwrap();
    loop {
        match reader.next_block().await {
            Ok(Some(_)) => {}
            Ok(None) => panic!("corrupt stream reported complete"),
            Err(_) => break,
        }
    }
    store.state.lock().unwrap().fault = None;
    assert!(reader.next_block().await.is_err());
}

#[tokio::test]
async fn manifest_substitution_is_rejected_before_download() {
    let store = FaultStore::default();
    let manifest = sealed(&store).await;
    let reference = verify_and_publish(&store, &manifest).await.unwrap();
    store.state.lock().unwrap().fault = Some(Fault::StreamDigest);
    assert!(ArtifactReader::open(&store, &reference).await.is_err());
}

#[tokio::test]
async fn sealed_output_is_not_a_published_download() {
    let store = FaultStore::default();
    let manifest = sealed(&store).await;
    let reference =
        ArtifactReference::from_catalog(manifest.identity().clone(), manifest.manifest_digest());
    assert!(ArtifactReader::open(&store, &reference).await.is_err());
}

#[tokio::test]
async fn interrupted_verification_resumes_without_a_producer_or_local_file() {
    let store = FaultStore::default();
    let manifest = sealed(&store).await;
    store.state.lock().unwrap().fault = Some(Fault::Unavailable);
    assert!(verify_and_publish(&store, &manifest).await.is_err());
    store.state.lock().unwrap().fault = None;
    let persisted = store.manifest(manifest.identity()).await.unwrap();
    let recovered = ArtifactManifest::decode(&persisted, manifest.identity(), limits()).unwrap();
    assert!(verify_and_publish(&store, &recovered).await.is_ok());
}

#[tokio::test]
async fn abort_fences_verified_publication() {
    let store = FaultStore::default();
    let manifest = sealed(&store).await;
    store.abort(manifest.identity()).await.unwrap();
    assert!(verify_and_publish(&store, &manifest).await.is_err());
}

#[tokio::test]
async fn overflow_poisoned_writer_cannot_publish_a_prefix() {
    let store = FaultStore::default();
    let mut writer = ArtifactWriter::new(&store, identity(), limits())
        .await
        .unwrap();
    writer.write(b"abcd").await.unwrap();
    assert!(writer.write(b"0123456789").await.is_err());
    assert!(writer.finish().await.is_err());
}

#[tokio::test]
async fn full_stream_digest_is_checked_independently_of_block_hashes() {
    let store = FaultStore::default();
    let original = sealed(&store).await;
    let mut bytes = original.encode();
    bytes[81] ^= 1;
    let manifest = ArtifactManifest::decode(&bytes, original.identity(), limits()).unwrap();
    store.state.lock().unwrap().manifest = bytes;
    assert!(verify_and_publish(&store, &manifest).await.is_err());
    assert!(!store.state.lock().unwrap().published);
}

#[rstest]
#[case(0)]
#[case(5)]
#[case(21)]
#[case(37)]
#[case(53)]
#[case(69)]
#[case(73)]
#[case(113)]
#[case(117)]
fn malformed_manifest_is_rejected(#[case] offset: usize) {
    let manifest = manifest(identity(), b"abcdefghij");
    let mut bytes = manifest.encode();
    bytes[offset] ^= 0xff;
    assert!(ArtifactManifest::decode(&bytes, manifest.identity(), limits()).is_err());
}

#[rstest]
#[case(0)]
#[case(116)]
#[case(117)]
#[case(224)]
fn truncated_manifest_is_rejected(#[case] length: usize) {
    let manifest = manifest(identity(), b"abcdefghij");
    assert!(
        ArtifactManifest::decode(&manifest.encode()[..length], manifest.identity(), limits())
            .is_err()
    );
}

#[test]
fn manifest_budget_is_checked_before_accepting_a_block() {
    let limits = ArtifactLimits::builder()
        .block_bytes(4)
        .artifact_bytes(4)
        .blocks(1)
        .manifest_bytes(152)
        .build()
        .unwrap();
    let mut builder = ManifestBuilder::new(identity(), limits);
    assert!(builder.push(b"abcd").is_err());
    assert!(builder.finish().is_err());
}

#[test]
fn exact_manifest_byte_limit_is_accepted() {
    let limits = ArtifactLimits::builder()
        .block_bytes(4)
        .artifact_bytes(4)
        .blocks(1)
        .manifest_bytes(153)
        .build()
        .unwrap();
    let mut builder = ManifestBuilder::new(identity(), limits);
    builder.push(b"abcd").unwrap();
    assert_eq!(builder.finish().unwrap().encode().len(), 153);
}

#[test]
fn partial_block_is_terminal() {
    let mut builder = ManifestBuilder::new(identity(), limits());
    builder.push(b"ab").unwrap();
    assert!(builder.push(b"cd").is_err());
}

#[test]
fn manifest_count_cannot_allocate_beyond_budget() {
    let manifest = manifest(identity(), b"abcd");
    let mut bytes = manifest.encode();
    bytes[113..117].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(ArtifactManifest::decode(&bytes, manifest.identity(), limits()).is_err());
}

#[rstest]
#[case::zero_block(0, 0, 0, 117)]
#[case::oversize_block(1_048_577, 0, 0, 117)]
#[case::oversize_artifact(1_048_576, 1_073_741_825, 1024, 262_144)]
#[case::oversize_count(1_048_576, 0, 1025, 117)]
#[case::missing_header(4, 4, 1, 116)]
#[case::oversize_manifest(4, 4, 1, 262_145)]
#[case::insufficient_blocks(4, 5, 1, 262_144)]
fn invalid_resource_limits_are_rejected(
    #[case] block_bytes: u32,
    #[case] artifact_bytes: u64,
    #[case] blocks: u32,
    #[case] manifest_bytes: u32,
) {
    assert!(
        ArtifactLimits::builder()
            .block_bytes(block_bytes)
            .artifact_bytes(artifact_bytes)
            .blocks(blocks)
            .manifest_bytes(manifest_bytes)
            .build()
            .is_err()
    );
}

#[test]
fn extra_manifest_bytes_are_rejected() {
    let manifest = manifest(identity(), b"abcd");
    let mut bytes = manifest.encode();
    bytes.push(0);
    assert!(ArtifactManifest::decode(&bytes, manifest.identity(), limits()).is_err());
}

#[tokio::test]
async fn final_block_is_withheld_when_only_the_full_digest_is_wrong() {
    let store = FaultStore::default();
    let original = sealed(&store).await;
    let mut bytes = original.encode();
    bytes[81] ^= 1;
    let manifest = ArtifactManifest::decode(&bytes, original.identity(), limits()).unwrap();
    {
        let mut state = store.state.lock().unwrap();
        state.manifest = bytes;
        state.published = true;
    }
    let reference =
        ArtifactReference::from_catalog(manifest.identity().clone(), manifest.manifest_digest());
    let mut reader = ArtifactReader::open(&store, &reference).await.unwrap();
    reader.next_block().await.unwrap();
    reader.next_block().await.unwrap();
    assert!(reader.next_block().await.is_err());
}
