use super::*;

#[rstest]
#[case::one_source(false)]
#[case::selection_spans_sources(true)]
#[tokio::test]
async fn fully_selected_source_binds_the_published_reference_and_original_batch(
    #[case] spans_sources: bool,
) {
    let scope = scope();
    let first = record(scope.clone(), ResourceIdentity::generate(), 1, 2);
    let second = record(scope.clone(), ResourceIdentity::generate(), 2, 3)
        .with_dependencies(vec![first.interval.version()])
        .unwrap();
    let local = [identity(&first), identity(&second)];
    let mut members = local.to_vec();
    if spans_sources {
        members.push(local[0].next_version());
    }
    let selection = SegmentRewriteSelection::new(members)
        .unwrap()
        .for_source(local)
        .unwrap();
    let (store, segment) = fixture(vec![first, second], scope).await.unwrap();
    let reference = store.publish(segment.proof()).await.unwrap();
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    let source = RetainedHistorySegment::open(&store, &reference, &mut budget)
        .await
        .unwrap();
    let proof = source
        .verify_fully_selected(&store, &selection, &mut budget)
        .await
        .unwrap();
    assert_eq!(proof.source(), &reference);
    assert_eq!(
        proof.selection_commitment(),
        &source.rewrite_commitment(&selection)
    );
    if spans_sources {
        let local_only = SegmentRewriteSelection::new(local).unwrap();
        assert_ne!(
            proof.selection_commitment(),
            &source.rewrite_commitment(&local_only)
        );
    }
}

#[rstest]
#[case::json(1)]
#[case::fixed_width(2)]
#[tokio::test]
async fn erasure_authenticates_the_legacy_and_current_copy_of_one_version(#[case] version: u8) {
    let scope = scope();
    let row = record(scope.clone(), ResourceIdentity::generate(), 10, 20);
    let member = identity(&row);
    let (legacy_store, legacy_proof) =
        verified_bytes(&legacy_bytes(&row, version), BLOCK_BYTES).await;
    let legacy_ref = legacy_store.publish(&legacy_proof).await.unwrap();
    let (current_store, current) = fixture(vec![row], scope).await.unwrap();
    let current_ref = current_store.publish(current.proof()).await.unwrap();

    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    let legacy = RetainedHistorySegment::open(&legacy_store, &legacy_ref, &mut budget)
        .await
        .unwrap();
    let current = RetainedHistorySegment::open(&current_store, &current_ref, &mut budget)
        .await
        .unwrap();
    let selection = SegmentRewriteSelection::new([member]).unwrap();
    let admitted = selection
        .start_admission()
        .include(&legacy, [member])
        .unwrap()
        .include(&current, [member])
        .unwrap()
        .finish()
        .unwrap();

    for (source, store, reference) in [
        (&legacy, &legacy_store, &legacy_ref),
        (&current, &current_store, &current_ref),
    ] {
        let proof = source
            .verify_fully_selected(store, admitted.for_source(source).unwrap(), &mut budget)
            .await
            .unwrap();
        assert_eq!(proof.source(), reference);
    }
}

#[derive(Clone, Copy, Debug)]
enum Mismatch {
    Partial,
    Missing,
    WrongResource,
    SameShortKey,
}

#[rstest]
#[case::partial(Mismatch::Partial)]
#[case::more_than_source(Mismatch::Missing)]
#[case::other_resource(Mismatch::WrongResource)]
#[case::short_key_collision(Mismatch::SameShortKey)]
#[tokio::test]
async fn whole_source_evidence_requires_exact_version_coverage(#[case] mismatch: Mismatch) {
    let scope = scope();
    let first = record(scope.clone(), ResourceIdentity::generate(), 1, 2);
    let second = record(scope.clone(), ResourceIdentity::generate(), 2, 3);
    let mut members = vec![identity(&first), identity(&second)];
    match mismatch {
        Mismatch::Partial => {
            members.pop();
        }
        Mismatch::Missing => members.push(members[0].next_version()),
        Mismatch::WrongResource => {
            members[1] =
                StorageRecordIdentity::new(ResourceIdentity::generate(), members[1].version());
        }
        Mismatch::SameShortKey => {
            let mut bytes = *members[1].version().as_bytes();
            bytes[15] ^= 1;
            members[1] = StorageRecordIdentity::new(
                members[1].resource(),
                VersionIdentity::from_bytes(bytes).unwrap(),
            );
        }
    }
    let selection = SegmentRewriteSelection::new(members).unwrap();
    let (store, segment) = fixture(vec![first, second], scope).await.unwrap();
    let reference = store.publish(segment.proof()).await.unwrap();
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    let source = RetainedHistorySegment::open(&store, &reference, &mut budget)
        .await
        .unwrap();
    assert!(matches!(
        source
            .verify_fully_selected(&store, &selection, &mut budget)
            .await,
        Err(ArtifactError::Conflict(_))
    ));
}
