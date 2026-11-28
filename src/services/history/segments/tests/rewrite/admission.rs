use super::*;

async fn published_source(rows: Vec<SegmentRecord>) -> (MemoryStorage, RetainedHistorySegment) {
    let scope = rows[0].scope.clone();
    let (store, segment) = fixture(rows, scope).await.unwrap();
    let reference = store.publish(segment.proof()).await.unwrap();
    let source = RetainedHistorySegment::open(
        &store,
        &reference,
        &mut ByteLimit::new(SEGMENT_BYTES).start(),
    )
    .await
    .unwrap();
    (store, source)
}

#[rstest]
#[case::under_limit(1_000_000, true)]
#[case::over_limit(3_000_000, false)]
#[tokio::test]
async fn selected_frame_budget_is_shared_across_published_sources(
    #[case] payload: usize,
    #[case] fits: bool,
) {
    let mut sources = Vec::new();
    for _ in 0..2 {
        let scope = scope();
        let rows: Vec<_> = (0..3)
            .map(|_| {
                let mut row = record(scope.clone(), ResourceIdentity::generate(), 1, 2);
                row.payload = json!("r".repeat(payload));
                row
            })
            .collect();
        let selected = rows.iter().map(identity).collect::<Vec<_>>();
        let (_, source) = published_source(rows).await;
        sources.push((source, selected));
    }
    let batch = SegmentRewriteSelection::new(
        sources
            .iter()
            .flat_map(|(_, members)| members.iter().copied()),
    )
    .unwrap();
    let admission = sources
        .iter()
        .try_fold(batch.start_admission(), |admission, (source, members)| {
            admission.include(source, members.iter().copied())
        })
        .and_then(RewriteSourceAdmission::finish);
    if fits {
        let admitted = admission.unwrap();
        for (source, _) in &sources {
            assert!(admitted.for_source(source).is_ok());
        }
    } else {
        assert!(matches!(admission, Err(ArtifactError::Limit(_))));
    }
}

#[rstest]
#[case::missing(0)]
#[case::missing_despite_multiple_copies(1)]
#[case::repeated_source(2)]
#[case::wrong_source(3)]
#[tokio::test]
async fn admission_requires_coverage_and_distinct_sources(#[case] problem: u8) {
    let scope = scope();
    let first = record(scope.clone(), ResourceIdentity::generate(), 1, 2);
    let second = record(scope, ResourceIdentity::generate(), 1, 2);
    let (a, b) = (identity(&first), identity(&second));
    let (_, source) = published_source(vec![first.clone(), second]).await;
    let (_, other) = published_source(vec![first]).await;
    let batch = SegmentRewriteSelection::new([a, b]).unwrap();
    let mut input = vec![(&source, vec![a])];
    match problem {
        0 => {}
        1 => input.push((&other, vec![a])),
        2 => input.push((&source, vec![b])),
        _ => input.push((&other, vec![b])),
    }
    assert!(matches!(
        input
            .into_iter()
            .try_fold(batch.start_admission(), |admission, (source, members)| {
                admission.include(source, members)
            })
            .and_then(RewriteSourceAdmission::finish),
        Err(ArtifactError::Invalid(_) | ArtifactError::Conflict(_))
    ));
}

#[tokio::test]
async fn a_projected_selection_is_bound_to_its_published_source() {
    let scope = scope();
    let row = record(scope, ResourceIdentity::generate(), 1, 2);
    let target = identity(&row);
    let (_, first) = published_source(vec![row.clone()]).await;
    let (store, copy) = published_source(vec![row]).await;
    let batch = SegmentRewriteSelection::new([target]).unwrap();
    let admitted = batch
        .start_admission()
        .include(&first, [target])
        .unwrap()
        .finish()
        .unwrap();
    let selection = admitted.for_source(&first).unwrap();
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    assert!(matches!(
        copy.verify_fully_selected(&store, selection, &mut budget)
            .await,
        Err(ArtifactError::Conflict(_))
    ));
    assert_eq!(budget.used(), 0);
}

#[tokio::test]
async fn each_published_copy_of_a_selected_version_can_be_verified() {
    let scope = scope();
    let target = record(scope.clone(), ResourceIdentity::generate(), 1, 2);
    let survivor = record(scope, ResourceIdentity::generate(), 1, 2);
    let selected = identity(&target);
    let (first_store, first) = published_source(vec![target.clone()]).await;
    let (other_store, other) = published_source(vec![target.clone(), survivor.clone()]).await;
    let batch = SegmentRewriteSelection::new([selected]).unwrap();
    let admitted = batch
        .start_admission()
        .include(&first, [selected])
        .unwrap()
        .include(&other, [selected])
        .unwrap()
        .finish()
        .unwrap();
    let whole = first
        .verify_fully_selected(
            &first_store,
            admitted.for_source(&first).unwrap(),
            &mut ByteLimit::new(SEGMENT_BYTES).start(),
        )
        .await
        .unwrap();
    assert_eq!(whole.source().identity(), first.manifest.identity());
    let mut budget = ByteLimit::new(2 * SEGMENT_BYTES).start();
    let replacement = other
        .rewrite_into(
            &other_store,
            &other_store,
            ArtifactIdentity::new(other.manifest.identity().destination()),
            admitted.for_source(&other).unwrap(),
            &mut budget,
        )
        .await
        .unwrap();
    assert_eq!(
        replacement
            .segment()
            .exact(
                &other_store,
                selected.resource(),
                selected.version(),
                &mut budget
            )
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        replacement
            .segment()
            .exact(
                &other_store,
                survivor.interval.resource(),
                survivor.interval.version(),
                &mut budget
            )
            .await
            .unwrap(),
        Some(survivor)
    );
}

#[rstest]
#[case::exact_row_limit(500, 500, 0, true)]
#[case::one_extra_physical_row(501, 500, 0, false)]
#[case::repeated_bytes_within_limit(3, 3, 1_000_000, true)]
#[case::repeated_bytes_above_limit(3, 3, 3_000_000, false)]
#[tokio::test]
async fn each_selected_copy_consumes_the_shared_input_allowance(
    #[case] first_count: usize,
    #[case] other_count: usize,
    #[case] payload: usize,
    #[case] fits: bool,
) {
    let scope = scope();
    let rows: Vec<_> = (0..first_count)
        .map(|_| {
            let mut row = record(scope.clone(), ResourceIdentity::generate(), 1, 2);
            row.payload = json!("r".repeat(payload));
            row
        })
        .collect();
    let selected: Vec<_> = rows.iter().map(identity).collect();
    let (_, other) = published_source(rows[..other_count].to_vec()).await;
    let (_, first) = published_source(rows).await;
    let batch = SegmentRewriteSelection::new(selected.iter().copied()).unwrap();
    let outcome = batch
        .start_admission()
        .include(&first, selected.iter().copied())
        .unwrap()
        .include(&other, selected[..other_count].iter().copied())
        .and_then(RewriteSourceAdmission::finish);
    if fits {
        assert!(outcome.is_ok());
    } else {
        assert!(matches!(outcome, Err(ArtifactError::Limit(_))));
    }
}
