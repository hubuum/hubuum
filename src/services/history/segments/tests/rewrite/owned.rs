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
            .stage_owned_subset_rewrite(&store, &store, &owner, &selection, &mut budget)
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
            .stage_owned_subset_rewrite(&store, &store, &owner, &selection, &mut budget)
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
        .stage_owned_subset_rewrite(
            &store,
            &store,
            &owner,
            &selection,
            &mut ByteLimit::new(0).start(),
        )
        .await;
    assert!(matches!(short, Err(ArtifactError::Limit(_))));
    let result = source
        .stage_owned_subset_rewrite(&store, &store, &owner, &selection, &mut budget)
        .await
        .unwrap();
    assert_eq!(
        result.segment().proof().manifest().identity(),
        intent.identity()
    );
}
