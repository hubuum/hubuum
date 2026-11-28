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
#[case::repeated_version(1)]
#[case::repeated_source(2)]
#[case::wrong_source(3)]
#[tokio::test]
async fn admission_requires_each_selected_version_in_one_source(#[case] problem: u8) {
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
