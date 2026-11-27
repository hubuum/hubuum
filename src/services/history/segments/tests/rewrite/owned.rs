use std::fs;
use std::os::unix::fs::DirBuilderExt;
use std::path::PathBuf;

use hubuum_artifacts::{
    ArtifactCreationIntent, FilesystemArtifactStore, OwnedArtifact, OwnedArtifactAdmission,
    OwnedArtifactPhase, OwnedArtifactResolution, OwnedArtifactStore,
};
use uuid::Uuid;

use super::*;

struct Fixture {
    root: PathBuf,
    source: ArtifactReference,
    target: SegmentRecord,
    survivor: SegmentRecord,
}

impl Fixture {
    async fn new() -> Self {
        let root = std::env::temp_dir()
            .canonicalize()
            .unwrap()
            .join(format!("hubuum-owned-rewrite-{}", Uuid::new_v4()));
        fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
        let store = FilesystemArtifactStore::open(&root).await.unwrap();
        let destination = Destination::new(DestinationId::generate(), "retained-rewrite").unwrap();
        store.register_destination(&destination).await.unwrap();
        let scope = scope();
        let resource = ResourceIdentity::generate();
        let target = record(scope.clone(), resource, 1, 2);
        let survivor = record(scope.clone(), resource, 2, 3);
        let segment = HistorySegment::stage(
            &store,
            ArtifactIdentity::new(destination.id()),
            scope,
            [target.clone(), survivor.clone()],
        )
        .await
        .unwrap();
        let source = store.publish(segment.proof()).await.unwrap();
        Self {
            root,
            source,
            target,
            survivor,
        }
    }

    async fn admit(
        &self,
        store: &FilesystemArtifactStore,
        source: &RetainedHistorySegment,
    ) -> (
        ArtifactCreationIntent,
        OwnedArtifact,
        SegmentRewriteSelection,
    ) {
        let selection = SegmentRewriteSelection::new([identity(&self.target)]).unwrap();
        self.admit_selection(store, source, selection).await
    }

    async fn admit_selection(
        &self,
        store: &FilesystemArtifactStore,
        source: &RetainedHistorySegment,
        selection: SegmentRewriteSelection,
    ) -> (
        ArtifactCreationIntent,
        OwnedArtifact,
        SegmentRewriteSelection,
    ) {
        let intent = ArtifactCreationIntent::new(
            ArtifactIdentity::new(self.source.identity().destination()),
            HistorySegment::write_limits().unwrap(),
            *Uuid::new_v4().as_bytes(),
            *Uuid::new_v4().as_bytes(),
            source.rewrite_commitment(&selection),
        )
        .unwrap();
        let intent = store.bind_intent(intent).await.unwrap();
        let OwnedArtifactAdmission::Created(receipt) = store.create_owned(&intent).await.unwrap()
        else {
            panic!("fresh output must have a new owner");
        };
        (intent, receipt, selection)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[tokio::test]
async fn reopened_provider_can_fence_the_exact_sealed_replacement() {
    let fixture = Fixture::new().await;
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    let intent_bytes = {
        let store = FilesystemArtifactStore::open_existing(&fixture.root)
            .await
            .unwrap();
        let source = RetainedHistorySegment::open(&store, &fixture.source, &mut budget)
            .await
            .unwrap();
        let (intent, owner, selection) = fixture.admit(&store, &source).await;
        let rewrite = source
            .prepare_owned_subset_rewrite(&store, &store, &owner, &selection, &mut budget)
            .await
            .unwrap();
        assert_eq!(
            rewrite.segment().proof().manifest().identity(),
            intent.identity()
        );
        assert_eq!(
            rewrite
                .segment()
                .exact(
                    &store,
                    fixture.survivor.interval.resource,
                    fixture.survivor.interval.version,
                    &mut budget
                )
                .await
                .unwrap(),
            Some(fixture.survivor.clone())
        );
        intent.encode().unwrap()
    };
    let reopened = FilesystemArtifactStore::open_existing(&fixture.root)
        .await
        .unwrap();
    let intent = ArtifactCreationIntent::decode(&intent_bytes).unwrap();
    let OwnedArtifactResolution::Established {
        artifact,
        phase: OwnedArtifactPhase::Sealed,
    } = reopened.resolve_owned(&intent).await.unwrap()
    else {
        panic!("the original intent must resolve the staged replacement after reopen");
    };
    reopened.abort_owned(&artifact).await.unwrap();
    assert!(matches!(
        reopened.resolve_owned(&intent).await.unwrap(),
        OwnedArtifactResolution::Established {
            phase: OwnedArtifactPhase::Fenced,
            ..
        }
    ));
    let original = RetainedHistorySegment::open(&reopened, &fixture.source, &mut budget)
        .await
        .unwrap();
    assert_eq!(
        original
            .exact(
                &reopened,
                fixture.target.interval.resource,
                fixture.target.interval.version,
                &mut budget
            )
            .await
            .unwrap(),
        Some(fixture.target.clone())
    );
}

#[rstest]
#[case::different_selection(false)]
#[case::different_source_copy(true)]
#[tokio::test]
async fn a_mismatched_request_cannot_use_or_abort_an_admitted_rewrite(#[case] change_source: bool) {
    let fixture = Fixture::new().await;
    let store = FilesystemArtifactStore::open_existing(&fixture.root)
        .await
        .unwrap();
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    let source = RetainedHistorySegment::open(&store, &fixture.source, &mut budget)
        .await
        .unwrap();
    let (intent, owner, selection) = fixture.admit(&store, &source).await;
    let (source, selection) = if change_source {
        let copy = HistorySegment::stage(
            &store,
            ArtifactIdentity::new(fixture.source.identity().destination()),
            source.scope().clone(),
            [fixture.target.clone(), fixture.survivor.clone()],
        )
        .await
        .unwrap();
        let reference = store.publish(copy.proof()).await.unwrap();
        (
            RetainedHistorySegment::open(&store, &reference, &mut budget)
                .await
                .unwrap(),
            selection,
        )
    } else {
        (
            source,
            SegmentRewriteSelection::new([identity(&fixture.survivor)]).unwrap(),
        )
    };
    assert!(matches!(
        source
            .prepare_owned_subset_rewrite(&store, &store, &owner, &selection, &mut budget)
            .await,
        Err(ArtifactError::Conflict(_))
    ));
    assert!(matches!(
        store.resolve_owned(&intent).await.unwrap(),
        OwnedArtifactResolution::Established {
            phase: OwnedArtifactPhase::Staging,
            ..
        }
    ));
}

#[tokio::test]
async fn a_failed_scan_leaves_its_attempt_available_to_the_same_owner() {
    let fixture = Fixture::new().await;
    let store = FilesystemArtifactStore::open_existing(&fixture.root)
        .await
        .unwrap();
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    let source = RetainedHistorySegment::open(&store, &fixture.source, &mut budget)
        .await
        .unwrap();
    let (intent, owner, selection) = fixture.admit(&store, &source).await;
    let short = source
        .prepare_owned_subset_rewrite(
            &store,
            &store,
            &owner,
            &selection,
            &mut ByteLimit::new(0).start(),
        )
        .await;
    assert!(matches!(short, Err(ArtifactError::Limit(_))));
    let result = source
        .prepare_owned_subset_rewrite(&store, &store, &owner, &selection, &mut budget)
        .await
        .unwrap();
    assert_eq!(
        result.segment().proof().manifest().identity(),
        intent.identity()
    );
}

#[rstest]
#[case::after_seal(false)]
#[case::after_provider_publication(true)]
#[tokio::test]
async fn restart_reconciles_the_original_rewrite(#[case] publish: bool) {
    let mut fixture = Fixture::new().await;
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    let (encoded_intent, expected_manifest, expected_publication) = {
        let store = FilesystemArtifactStore::open_existing(&fixture.root)
            .await
            .unwrap();
        let scope = fixture.survivor.scope.clone();
        let mut large = record(scope.clone(), fixture.survivor.interval.resource, 3, 4)
            .with_dependencies(vec![VersionIdentity::generate()])
            .unwrap();
        large.payload = json!({"text": "x".repeat(2 * BLOCK_BYTES as usize)});
        let segment = HistorySegment::stage(
            &store,
            ArtifactIdentity::new(fixture.source.identity().destination()),
            scope,
            [fixture.survivor.clone(), fixture.target.clone(), large],
        )
        .await
        .unwrap();
        fixture.source = store.publish(segment.proof()).await.unwrap();
        let source = RetainedHistorySegment::open(&store, &fixture.source, &mut budget)
            .await
            .unwrap();
        let (intent, owner, selection) = fixture.admit(&store, &source).await;
        let rewrite = source
            .prepare_owned_subset_rewrite(&store, &store, &owner, &selection, &mut budget)
            .await
            .unwrap();
        let proof = rewrite.segment().proof();
        let published = if publish {
            Some(store.publish_owned(&owner, proof).await.unwrap())
        } else {
            None
        };
        (
            intent.encode().unwrap(),
            proof.manifest().clone(),
            published,
        )
    };
    let reopened = FilesystemArtifactStore::open_existing(&fixture.root)
        .await
        .unwrap();
    let intent = ArtifactCreationIntent::decode(&encoded_intent).unwrap();
    let OwnedArtifactResolution::Established { artifact, .. } =
        reopened.resolve_owned(&intent).await.unwrap()
    else {
        panic!("the original owned attempt must survive restart");
    };
    let source = RetainedHistorySegment::open(&reopened, &fixture.source, &mut budget)
        .await
        .unwrap();
    let selection = SegmentRewriteSelection::new([identity(&fixture.target)]).unwrap();
    let recovered = source
        .prepare_owned_subset_rewrite(&reopened, &reopened, &artifact, &selection, &mut budget)
        .await
        .unwrap();
    assert_eq!(recovered.segment().proof().manifest(), &expected_manifest);
    assert_eq!(
        recovered.published_reference(),
        expected_publication.as_ref()
    );
}

enum WrongReplacement {
    Payload,
    SelectedRow,
    ExtraRow,
    Scope,
}

#[rstest]
#[case::same_identity_and_length_with_changed_payload(WrongReplacement::Payload)]
#[case::selected_instead_of_surviving_version(WrongReplacement::SelectedRow)]
#[case::additional_version(WrongReplacement::ExtraRow)]
#[case::different_scope(WrongReplacement::Scope)]
#[tokio::test]
async fn a_valid_owned_artifact_must_still_match_the_surviving_frames(
    #[case] wrong: WrongReplacement,
) {
    let fixture = Fixture::new().await;
    let store = FilesystemArtifactStore::open_existing(&fixture.root)
        .await
        .unwrap();
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    let source = RetainedHistorySegment::open(&store, &fixture.source, &mut budget)
        .await
        .unwrap();
    let (intent, owner, selection) = fixture.admit(&store, &source).await;

    let mut survivor = fixture.survivor.clone();
    let mut scope = source.scope().clone();
    let records = match wrong {
        WrongReplacement::Payload => {
            survivor.payload = json!({"value": "modified"});
            vec![survivor]
        }
        WrongReplacement::SelectedRow => vec![fixture.target.clone()],
        WrongReplacement::ExtraRow => vec![fixture.target.clone(), survivor],
        WrongReplacement::Scope => {
            scope = super::scope();
            survivor.scope = scope.clone();
            vec![survivor]
        }
    };
    let session = owner.session(&store);
    HistorySegment::stage(&session, intent.identity().clone(), scope, records)
        .await
        .unwrap();
    let recovered = source
        .prepare_owned_subset_rewrite(&store, &store, &owner, &selection, &mut budget)
        .await;
    assert!(matches!(recovered, Err(ArtifactError::Conflict(_))));
}

#[tokio::test]
async fn reconciling_a_fenced_rewrite_cannot_reopen_its_output() {
    let fixture = Fixture::new().await;
    let store = FilesystemArtifactStore::open_existing(&fixture.root)
        .await
        .unwrap();
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    let source = RetainedHistorySegment::open(&store, &fixture.source, &mut budget)
        .await
        .unwrap();
    let (_, owner, selection) = fixture.admit(&store, &source).await;
    source
        .prepare_owned_subset_rewrite(&store, &store, &owner, &selection, &mut budget)
        .await
        .unwrap();
    store.abort_owned(&owner).await.unwrap();
    assert!(matches!(
        source
            .prepare_owned_subset_rewrite(&store, &store, &owner, &selection, &mut budget)
            .await,
        Err(ArtifactError::Conflict(_))
    ));
}

#[rstest]
#[case::matching_block(true)]
#[case::different_block(false)]
#[tokio::test]
async fn restart_resumes_only_an_identical_appended_block(#[case] matching: bool) {
    let fixture = Fixture::new().await;
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    let (encoded, expected_digest) = {
        let store = FilesystemArtifactStore::open_existing(&fixture.root)
            .await
            .unwrap();
        let source = RetainedHistorySegment::open(&store, &fixture.source, &mut budget)
            .await
            .unwrap();
        let (intent, owner, _) = fixture.admit(&store, &source).await;
        let original = HistorySegment::stage(
            &store,
            ArtifactIdentity::new(fixture.source.identity().destination()),
            source.scope().clone(),
            [fixture.survivor.clone()],
        )
        .await
        .unwrap();
        let manifest = original.proof().manifest();
        assert_eq!(manifest.block_count(), 1);
        let mut block = store.block(manifest.identity(), 0).await.unwrap().unwrap();
        if !matching {
            block[0] ^= 1;
        }
        // The block reached the provider, but the worker never sent the seal.
        store.append_owned(&owner, 0, &block).await.unwrap();
        (intent.encode().unwrap(), *manifest.stream_digest())
    };

    let store = FilesystemArtifactStore::open_existing(&fixture.root)
        .await
        .unwrap();
    let intent = ArtifactCreationIntent::decode(&encoded).unwrap();
    let OwnedArtifactResolution::Established {
        artifact,
        phase: OwnedArtifactPhase::Staging,
    } = store.resolve_owned(&intent).await.unwrap()
    else {
        panic!("partially written output must retain its owner");
    };
    let source = RetainedHistorySegment::open(&store, &fixture.source, &mut budget)
        .await
        .unwrap();
    let selection = SegmentRewriteSelection::new([identity(&fixture.target)]).unwrap();
    let result = source
        .prepare_owned_subset_rewrite(&store, &store, &artifact, &selection, &mut budget)
        .await;
    if matching {
        assert_eq!(
            result.unwrap().segment().proof().manifest().stream_digest(),
            &expected_digest
        );
    } else {
        assert!(matches!(result, Err(ArtifactError::Conflict(_))));
    }
}

#[derive(Clone, Copy)]
enum BatchChange {
    Reorder,
    OtherResource,
    OtherVersion,
    SourceMembership,
    ForgetOther,
}

#[rstest]
#[case::same_batch_in_different_order(BatchChange::Reorder)]
#[case::changed_remote_resource(BatchChange::OtherResource)]
#[case::changed_remote_version(BatchChange::OtherVersion)]
#[case::changed_source_membership(BatchChange::SourceMembership)]
#[case::single_source_retry(BatchChange::ForgetOther)]
#[tokio::test]
async fn sealed_replacement_is_bound_to_its_whole_batch(#[case] change: BatchChange) {
    let fixture = Fixture::new().await;
    let local = identity(&fixture.target);
    let other = local.next_version();
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    let intent = {
        let store = FilesystemArtifactStore::open_existing(&fixture.root)
            .await
            .unwrap();
        let source = RetainedHistorySegment::open(&store, &fixture.source, &mut budget)
            .await
            .unwrap();
        let selection = SegmentRewriteSelection::new([local, other])
            .unwrap()
            .for_source([local])
            .unwrap();
        let (intent, owner, selection) = fixture.admit_selection(&store, &source, selection).await;
        source
            .prepare_owned_subset_rewrite(&store, &store, &owner, &selection, &mut budget)
            .await
            .unwrap();
        intent
    };

    let store = FilesystemArtifactStore::open_existing(&fixture.root)
        .await
        .unwrap();
    let source = RetainedHistorySegment::open(&store, &fixture.source, &mut budget)
        .await
        .unwrap();
    let OwnedArtifactResolution::Established {
        artifact,
        phase: OwnedArtifactPhase::Sealed,
    } = store.resolve_owned(&intent).await.unwrap()
    else {
        panic!("missing sealed output")
    };
    let external = match change {
        BatchChange::OtherResource => {
            StorageRecordIdentity::new(ResourceIdentity::generate(), other.version())
        }
        BatchChange::OtherVersion => other.next_version(),
        _ => other,
    };
    let batch = SegmentRewriteSelection::new([external, local]).unwrap();
    let selection = match change {
        BatchChange::SourceMembership => batch.for_source([external]).unwrap(),
        BatchChange::ForgetOther => SegmentRewriteSelection::new([local]).unwrap(),
        _ => batch.for_source([local]).unwrap(),
    };
    let result = source
        .prepare_owned_subset_rewrite(&store, &store, &artifact, &selection, &mut budget)
        .await;
    if matches!(change, BatchChange::Reorder) {
        assert_eq!(result.unwrap().segment().record_count(), 1);
    } else {
        assert!(matches!(result, Err(ArtifactError::Conflict(_))));
    }
}

#[tokio::test]
async fn an_existing_single_source_intent_keeps_its_original_commitment() {
    use sha2::{Digest, Sha256};
    let fixture = Fixture::new().await;
    let store = FilesystemArtifactStore::open_existing(&fixture.root)
        .await
        .unwrap();
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    let source = RetainedHistorySegment::open(&store, &fixture.source, &mut budget)
        .await
        .unwrap();
    let target = identity(&fixture.target);
    let mut original = Sha256::new();
    original.update(b"hubuum/history/subset-rewrite/v1\0");
    original.update(fixture.source.manifest_digest());
    original.update(target.resource().as_bytes());
    original.update(target.version().as_bytes());
    assert_eq!(
        source.rewrite_commitment(&SegmentRewriteSelection::new([target]).unwrap()),
        <[u8; 32]>::from(original.finalize())
    );
}
