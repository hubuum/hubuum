use std::collections::BTreeMap;

use hubuum_storage_core::StorageRecordIdentity;

use super::*;

#[cfg(unix)]
mod owned;

fn identity(record: &SegmentRecord) -> StorageRecordIdentity {
    StorageRecordIdentity::new(record.interval.resource(), record.interval.version())
}

#[rstest]
#[case::empty(0)]
#[case::repeated(1)]
#[case::over_limit(2)]
fn rewrite_selection_refuses_invalid_members(#[case] case: u8) {
    let id = StorageRecordIdentity::new(ResourceIdentity::generate(), VersionIdentity::generate());
    let members = match case {
        0 => vec![],
        1 => vec![id, id],
        _ => (0..=1000).map(|_| id.next_version()).collect(),
    };
    assert!(SegmentRewriteSelection::new(members).is_err());
}

#[rstest]
#[case::replacement_excludes_selected(0)]
#[case::survivor_preserves_record(1)]
#[case::source_stays_readable(2)]
#[case::replacement_is_unpublished(3)]
#[tokio::test]
async fn mixed_segment_stages_verified_survivors(#[case] case: u8) {
    let scope = scope();
    let resource = ResourceIdentity::generate();
    let target = record(scope.clone(), resource, 1, 2);
    let mut survivor = record(scope.clone(), resource, 2, 3)
        .with_dependencies(vec![VersionIdentity::generate()])
        .unwrap();
    survivor.payload = json!({"multi_block": "s".repeat(2 * BLOCK_BYTES as usize)});
    let selection = SegmentRewriteSelection::new([identity(&target)]).unwrap();
    let (store, source) = fixture(vec![target.clone(), survivor.clone()], scope)
        .await
        .unwrap();
    let reference = store.publish(source.proof()).await.unwrap();
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    let retained = RetainedHistorySegment::open(&store, &reference, &mut budget)
        .await
        .unwrap();
    let output = retained
        .rewrite_into(
            &store,
            &store,
            ArtifactIdentity::new(reference.identity().destination()),
            &selection,
            &mut budget,
        )
        .await
        .unwrap();
    let segment = output.segment();
    match case {
        0 => assert_eq!(
            segment
                .exact(&store, resource, target.interval.version, &mut budget)
                .await
                .unwrap(),
            None
        ),
        1 => assert_eq!(
            segment
                .exact(&store, resource, survivor.interval.version, &mut budget)
                .await
                .unwrap(),
            Some(survivor)
        ),
        2 => assert_eq!(
            retained
                .exact(&store, resource, target.interval.version, &mut budget)
                .await
                .unwrap(),
            Some(target)
        ),
        _ => assert!(
            store
                .published_manifest(&segment.proof().reference())
                .await
                .is_err()
        ),
    }
}

#[rstest]
#[case::stale_shortened_version(0)]
#[case::dependent_survivor(1)]
#[case::wrong_resource(2)]
#[case::read_budget(3)]
#[case::all_selected(4)]
#[tokio::test]
async fn failed_rewrite_preserves_published_source(#[case] case: u8) {
    let scope = scope();
    let resource = ResourceIdentity::generate();
    let target = record(scope.clone(), resource, 1, 2);
    let mut survivor = record(scope.clone(), resource, 2, 3);
    let mut members = vec![identity(&target)];
    match case {
        0 => {
            let mut bytes = *target.interval.version.as_bytes();
            bytes[15] ^= 1;
            members[0] =
                StorageRecordIdentity::new(resource, VersionIdentity::from_bytes(bytes).unwrap());
        }
        1 => {
            survivor = survivor
                .with_dependencies(vec![target.interval.version])
                .unwrap()
        }
        2 => {
            members[0] =
                StorageRecordIdentity::new(ResourceIdentity::generate(), target.interval.version)
        }
        4 => members.push(identity(&survivor)),
        _ => {}
    }
    let selection = SegmentRewriteSelection::new(members).unwrap();
    let (store, source) = fixture(vec![target.clone(), survivor], scope)
        .await
        .unwrap();
    let reference = store.publish(source.proof()).await.unwrap();
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    let retained = RetainedHistorySegment::open(&store, &reference, &mut budget)
        .await
        .unwrap();
    let mut scan = ByteLimit::new(if case == 3 { 0 } else { SEGMENT_BYTES }).start();
    let result = retained
        .rewrite_into(
            &store,
            &store,
            ArtifactIdentity::new(reference.identity().destination()),
            &selection,
            &mut scan,
        )
        .await;
    match case {
        3 => assert!(matches!(result, Err(ArtifactError::Limit(_)))),
        4 => assert!(matches!(result, Err(ArtifactError::Invalid(_)))),
        _ => assert!(matches!(result, Err(ArtifactError::Conflict(_)))),
    }
    assert_eq!(
        retained
            .exact(&store, resource, target.interval.version, &mut budget)
            .await
            .unwrap(),
        Some(target)
    );
}

#[tokio::test]
async fn a_rewrite_checks_corruption_even_in_a_frame_selected_for_omission() {
    let scope = scope();
    let target = record(scope.clone(), ResourceIdentity::generate(), 1, 2);
    let survivor = record(scope.clone(), ResourceIdentity::generate(), 1, 2);
    let selection = SegmentRewriteSelection::new([identity(&target)]).unwrap();
    let (store, original) = fixture(vec![target, survivor], scope).await.unwrap();
    let mut bytes = staged_bytes(&store, &original).await;
    // Keep the authenticated artifact and its retained index internally valid
    // while making the first JSON frame undecodable.
    bytes[original.reader.entries[0].offset as usize + 4] = b'!';
    let (store, proof) = verified_bytes(&bytes, BLOCK_BYTES).await;
    let reference = store.publish(&proof).await.unwrap();
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    let retained = RetainedHistorySegment::open(&store, &reference, &mut budget)
        .await
        .unwrap();
    assert!(matches!(
        retained
            .rewrite_into(
                &store,
                &store,
                ArtifactIdentity::new(reference.identity().destination()),
                &selection,
                &mut budget
            )
            .await,
        Err(ArtifactError::Corrupt(_))
    ));
}

#[tokio::test]
async fn selected_frame_budget_is_enforced_before_source_payload_io() {
    let scope = scope();
    let mut rows: Vec<_> = (0..6)
        .map(|_| record(scope.clone(), ResourceIdentity::generate(), 1, 2))
        .collect();
    for row in &mut rows[..5] {
        row.payload = json!("x".repeat(3_500_000));
    }
    let selection = SegmentRewriteSelection::new(rows[..5].iter().map(identity)).unwrap();
    let (store, source) = fixture(rows, scope).await.unwrap();
    let reference = store.publish(source.proof()).await.unwrap();
    let retained = RetainedHistorySegment::open(
        &store,
        &reference,
        &mut ByteLimit::new(SEGMENT_BYTES).start(),
    )
    .await
    .unwrap();
    let mut scan = ByteLimit::new(SEGMENT_BYTES).start();
    let result = retained
        .rewrite_into(
            &store,
            &store,
            ArtifactIdentity::new(reference.identity().destination()),
            &selection,
            &mut scan,
        )
        .await;
    assert!(matches!(result, Err(ArtifactError::Limit(_))));
    assert_eq!(scan.used(), 0);
}

#[rstest]
#[case::empty(|_| vec![])]
#[case::absent_member(|id: StorageRecordIdentity| vec![id.next_version()])]
#[case::wrong_resource(|id: StorageRecordIdentity| vec![StorageRecordIdentity::new(ResourceIdentity::generate(), id.version())])]
#[case::duplicate_member(|id| vec![id, id])]
fn source_projection_rejects_invalid_members(
    #[case] members: fn(StorageRecordIdentity) -> Vec<StorageRecordIdentity>,
) {
    let target =
        StorageRecordIdentity::new(ResourceIdentity::generate(), VersionIdentity::generate());
    let batch = SegmentRewriteSelection::new([target]).unwrap();
    assert!(matches!(
        batch.for_source(members(target)),
        Err(ArtifactError::Invalid(_))
    ));
}

enum OtherSegmentContents {
    Independent,
    Dependent,
    ContainsUnexpectedTarget,
}

#[rstest]
#[case::independent(OtherSegmentContents::Independent)]
#[case::dependent(OtherSegmentContents::Dependent)]
#[case::unexpected_selected_frame(OtherSegmentContents::ContainsUnexpectedTarget)]
#[tokio::test]
async fn other_published_segments_check_the_entire_rewrite_batch(
    #[case] contents: OtherSegmentContents,
    #[values(true, false)] mixed: bool,
) {
    let scope = scope();
    let resource = ResourceIdentity::generate();
    let other = record(scope.clone(), resource, 0, 1);
    let local = record(scope.clone(), resource, 1, 2);
    let mut survivor = record(scope.clone(), resource, 2, 3);
    if matches!(contents, OtherSegmentContents::Dependent) {
        survivor = survivor
            .with_dependencies(vec![other.interval.version])
            .unwrap();
    }
    let mut other_records = vec![other.clone()];
    if !mixed {
        other_records.push(local.clone());
    }
    let (store, first) = fixture(other_records, scope.clone()).await.unwrap();
    let first_ref = store.publish(first.proof()).await.unwrap();
    let mut records = vec![survivor.clone()];
    if mixed {
        records.push(local.clone());
    }
    if matches!(contents, OtherSegmentContents::ContainsUnexpectedTarget) {
        records.push(other.clone());
    }
    let second = HistorySegment::stage(
        &store,
        ArtifactIdentity::new(first_ref.identity().destination()),
        scope,
        records,
    )
    .await
    .unwrap();
    let reference = store.publish(second.proof()).await.unwrap();
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    let retained = RetainedHistorySegment::open(&store, &reference, &mut budget)
        .await
        .unwrap();
    let batch = SegmentRewriteSelection::new([identity(&other), identity(&local)]).unwrap();
    let result = if mixed {
        let selection = batch.for_source([identity(&local)]).unwrap();
        retained
            .rewrite_into(
                &store,
                &store,
                ArtifactIdentity::new(reference.identity().destination()),
                &selection,
                &mut budget,
            )
            .await
            .map(|_| ())
    } else {
        retained
            .verify_unaffected_by_rewrite(&store, &batch, &mut budget)
            .await
    };
    if matches!(contents, OtherSegmentContents::Independent) {
        result.unwrap();
    } else {
        assert!(matches!(result, Err(ArtifactError::Conflict(_))));
    }
    assert_eq!(
        retained
            .exact(&store, resource, survivor.interval.version, &mut budget)
            .await
            .unwrap(),
        Some(survivor)
    );
}

#[tokio::test]
async fn an_unaffected_segment_needs_budget_to_read_its_dependency_frames() {
    let scope = scope();
    let survivor = record(scope.clone(), ResourceIdentity::generate(), 0, 1);
    let batch = SegmentRewriteSelection::new([identity(&survivor).next_version()]).unwrap();
    let (store, segment) = fixture(vec![survivor], scope).await.unwrap();
    let reference = store.publish(segment.proof()).await.unwrap();
    let source = RetainedHistorySegment::open(
        &store,
        &reference,
        &mut ByteLimit::new(SEGMENT_BYTES).start(),
    )
    .await
    .unwrap();
    let result = source
        .verify_unaffected_by_rewrite(&store, &batch, &mut ByteLimit::new(0).start())
        .await;
    assert!(matches!(result, Err(ArtifactError::Limit(_))));
}

fn profile_class(object: usize) -> usize {
    let group = object / 100;
    let within = object % 100;
    if within < 60 {
        (group * 60 + within) % 8
    } else {
        8 + (group * 40 + within - 60) % 120
    }
}

/// Local data-path diagnostic for P8. The named deployment has separate
/// process, database, retention and recovery requirements.
#[rstest]
#[case::ten_thousand(10_000)]
#[case::hundred_thousand(100_000)]
#[tokio::test]
#[ignore = "archive-exact-v1 local subset-rewrite traffic diagnostic"]
async fn one_percent_subset_rewrite_traffic(#[case] versions: usize) {
    let objects: Vec<_> = (0..10_000).map(|_| ResourceIdentity::generate()).collect();
    // Rotate targets across classes and both payload sizes. All touched sources
    // see the same complete batch, including versions in other collections.
    let targets_by_ordinal: BTreeMap<_, _> = (0..versions)
        .filter(|ordinal| ordinal % 100 == (ordinal / 100) % 100)
        .map(|ordinal| {
            (
                ordinal,
                StorageRecordIdentity::new(
                    objects[ordinal % objects.len()],
                    VersionIdentity::generate(),
                ),
            )
        })
        .collect();
    let batch = SegmentRewriteSelection::new(targets_by_ordinal.values().copied()).unwrap();
    let mut collections: Vec<Vec<usize>> = vec![Vec::new(); 32];
    for ordinal in 0..versions {
        let object = ordinal % objects.len();
        let class = profile_class(object);
        collections[class / 4].push(ordinal);
    }
    let mut touched = 0;
    let mut removed = 0;
    let mut total_segments = 0;
    for (collection, ordinals) in collections.iter().enumerate() {
        let scope = scope();
        for (part, ordinals) in ordinals.chunks(MAX_RECORDS).enumerate() {
            total_segments += 1;
            let rows: Vec<_> = ordinals
                .iter()
                .map(|&ordinal| {
                    let object = ordinal % objects.len();
                    let revision = (ordinal / objects.len()) as i64;
                    let mut row = record(scope.clone(), objects[object], revision, revision + 1);
                    if let Some(target) = targets_by_ordinal.get(&ordinal) {
                        row.interval.version = target.version();
                    }
                    let size = if (object + 1).is_multiple_of(100) {
                        64 * 1024
                    } else {
                        256
                    };
                    row.payload =
                        json!({"class": profile_class(object), "object": object, "revision": revision, "data": "a".repeat(size)});
                    row
                })
                .collect();
            let targets: Vec<_> = ordinals
                .iter()
                .filter_map(|ordinal| targets_by_ordinal.get(ordinal).copied())
                .collect();
            if targets.is_empty() {
                continue;
            }
            touched += 1;
            removed += targets.len();
            let selection = batch.for_source(targets.iter().copied()).unwrap();
            let (store, source) = fixture(rows.clone(), scope.clone()).await.unwrap();
            let reference = store.publish(source.proof()).await.unwrap();
            let mut source_read = ByteLimit::new(2 * SEGMENT_BYTES).start();
            let retained = RetainedHistorySegment::open(&store, &reference, &mut source_read)
                .await
                .unwrap();
            let open_bytes = source_read.used();
            let output = retained
                .rewrite_into(
                    &store,
                    &store,
                    ArtifactIdentity::new(reference.identity().destination()),
                    &selection,
                    &mut source_read,
                )
                .await
                .unwrap();
            let replacement = output.segment();
            let destination = replacement.proof().manifest();
            let written = destination.byte_size() + destination.encoded_len() as u64;
            assert!(source_read.used() as u64 + written <= 2 * SEGMENT_BYTES as u64);
            assert_eq!(replacement.record_count() + targets.len(), rows.len());
            println!(
                "SUBSET_REWRITE_SEGMENT {}",
                json!({
                    "versions": versions, "collection": collection, "part": part, "rows": rows.len(),
                    "removed": targets.len(), "source_open_bytes": open_bytes,
                    "source_frame_scan_bytes": source_read.used() - open_bytes,
                    "destination_written_bytes": written, "destination_verification_bytes": output.verification_bytes(),
                    "source_plus_destination_bytes": source_read.used() as u64 + written,
                })
            );
            // Independently compare every surviving row and every omitted UUID.
            // These diagnostic assertions run after the counted rewrite.
            for (row, &ordinal) in rows.into_iter().zip(ordinals) {
                let actual = replacement
                    .exact(
                        &store,
                        row.interval.resource,
                        row.interval.version,
                        &mut ByteLimit::new(SEGMENT_BYTES).start(),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    actual,
                    (!targets_by_ordinal.contains_key(&ordinal)).then_some(row)
                );
            }
        }
    }
    assert_eq!(removed, versions / 100);
    println!(
        "SUBSET_REWRITE_TOTAL {}",
        json!({"versions":versions, "segments":total_segments, "touched":touched, "removed":removed})
    );
}
