use hubuum_artifacts::{ArtifactWriter, Destination, DestinationId, verify};
use hubuum_storage_memory::MemoryStorage;
use rstest::rstest;
use serde_json::json;

use super::*;

fn scope() -> SegmentScope {
    SegmentScope::new(
        StorageBackupHistorySection::ObjectHistory,
        ResourceIdentity::generate(),
    )
}

fn at(seconds: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(seconds, 0).unwrap()
}

fn record(scope: SegmentScope, resource: ResourceIdentity, from: i64, to: i64) -> SegmentRecord {
    SegmentRecord::new(
        scope,
        VersionInterval::new(resource, VersionIdentity::generate(), at(from), at(to)).unwrap(),
        json!({"value": "retained"}),
    )
}

async fn fixture(
    records: Vec<SegmentRecord>,
    scope: SegmentScope,
) -> ArtifactResult<(MemoryStorage, HistorySegment)> {
    let store = MemoryStorage::new();
    let destination = Destination::new(DestinationId::generate(), "history-prototype").unwrap();
    store.register_destination(&destination).await.unwrap();
    let segment = HistorySegment::stage(
        &store,
        ArtifactIdentity::new(destination.id()),
        scope,
        records,
    )
    .await?;
    Ok((store, segment))
}

#[tokio::test]
async fn exact_preserves_version_and_dependencies_across_blocks() {
    let scope = scope();
    let resource = ResourceIdentity::generate();
    let mut original = record(scope.clone(), resource, 10, 20)
        .with_dependencies(vec![VersionIdentity::generate()])
        .unwrap();
    original.payload = json!({"large": "x".repeat(2 * 1024 * 1024)});
    let (store, segment) = fixture(vec![original.clone()], scope).await.unwrap();
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    let restored = segment
        .exact(&store, resource, original.interval.version, &mut budget)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored, original);
}

#[rstest]
#[case::before(9, None)]
#[case::start(10, Some(10))]
#[case::inside(15, Some(10))]
#[case::adjacent_boundary(20, Some(20))]
#[case::end(30, None)]
#[case::gap(35, None)]
#[case::later_start(40, Some(40))]
#[tokio::test]
async fn as_of_uses_half_open_intervals(#[case] instant: i64, #[case] expected: Option<i64>) {
    let scope = scope();
    let resource = ResourceIdentity::generate();
    // Input order need not be chronological or grouped by resource.
    let records = vec![
        record(scope.clone(), resource, 40, 50),
        record(scope.clone(), ResourceIdentity::generate(), 1, 100),
        record(scope.clone(), resource, 20, 30),
        record(scope.clone(), resource, 10, 20),
    ];
    let (store, segment) = fixture(records, scope).await.unwrap();
    let result = segment
        .as_of(
            &store,
            resource,
            at(instant),
            &mut ByteLimit::new(SEGMENT_BYTES).start(),
        )
        .await
        .unwrap();
    assert_eq!(
        result.map(|record| record.interval.valid_from),
        expected.map(at)
    );
}

#[tokio::test]
async fn exact_does_not_match_reused_resource_identity() {
    let scope = scope();
    let row = record(scope.clone(), ResourceIdentity::generate(), 10, 20);
    let version = row.interval.version;
    let (store, segment) = fixture(vec![row], scope).await.unwrap();
    assert!(
        segment
            .exact(
                &store,
                ResourceIdentity::generate(),
                version,
                &mut ByteLimit::new(0).start()
            )
            .await
            .unwrap()
            .is_none()
    );
}

#[rstest]
#[case::duplicate_version(true)]
#[case::overlapping_intervals(false)]
#[tokio::test]
async fn invalid_membership_cannot_be_sealed(#[case] duplicate: bool) {
    let scope = scope();
    let resource = ResourceIdentity::generate();
    let first = record(scope.clone(), resource, 10, 20);
    let second = if duplicate {
        first.clone()
    } else {
        record(scope.clone(), resource, 15, 25)
    };
    assert!(matches!(
        fixture(vec![first, second], scope).await,
        Err(ArtifactError::Invalid(_))
    ));
}

#[tokio::test]
async fn mixed_collections_cannot_be_sealed() {
    let row = record(scope(), ResourceIdentity::generate(), 10, 20);
    assert!(matches!(
        fixture(vec![row], scope()).await,
        Err(ArtifactError::Invalid(_))
    ));
}

#[rstest]
#[case::empty(10, 10)]
#[case::reversed(20, 10)]
fn invalid_intervals_are_rejected_at_decode(#[case] from: i64, #[case] to: i64) {
    let encoded = json!({"resource": ResourceIdentity::generate(), "version": VersionIdentity::generate(), "valid_from": at(from), "valid_to": at(to)});
    assert!(serde_json::from_value::<VersionInterval>(encoded).is_err());
}

#[rstest]
#[case::self_reference(0)]
#[case::duplicate(1)]
#[case::too_many(2)]
fn invalid_dependencies_are_rejected_at_decode(#[case] variant: u8) {
    let row = record(scope(), ResourceIdentity::generate(), 10, 20);
    let dependency = VersionIdentity::generate();
    let dependencies = match variant {
        0 => vec![row.interval.version],
        1 => vec![dependency; 2],
        _ => (0..=MAX_DEPENDENCIES)
            .map(|_| VersionIdentity::generate())
            .collect(),
    };
    let mut encoded = serde_json::to_value(row).unwrap();
    encoded["dependencies"] = json!(dependencies);
    assert!(serde_json::from_value::<SegmentRecord>(encoded).is_err());
}

#[rstest]
#[case::exact(0, true)]
#[case::overflow(1, false)]
#[tokio::test]
async fn encoded_row_limit_includes_framing(#[case] extra: usize, #[case] accepted: bool) {
    let scope = scope();
    let mut row = record(scope.clone(), ResourceIdentity::generate(), 10, 20);
    row.payload = json!("");
    let overhead = serde_json::to_vec(&row).unwrap().len() + 4;
    row.payload = json!("x".repeat(ROW_BYTES - overhead + extra));
    assert_eq!(fixture(vec![row], scope).await.is_ok(), accepted);
}

#[tokio::test]
async fn repeated_reads_keep_charging_physical_work() {
    let scope = scope();
    let resource = ResourceIdentity::generate();
    let row = record(scope.clone(), resource, 10, 20);
    let version = row.interval.version;
    let (store, segment) = fixture(vec![row], scope).await.unwrap();
    let mut budget = ByteLimit::new(segment.proof.manifest().byte_size() as usize).start();
    segment
        .exact(&store, resource, version, &mut budget)
        .await
        .unwrap();
    assert!(matches!(
        segment.exact(&store, resource, version, &mut budget).await,
        Err(ArtifactError::Limit(_))
    ));
}

#[tokio::test]
async fn staging_does_not_publish_without_catalog_fencing() {
    let scope = scope();
    let (store, segment) = fixture(Vec::new(), scope).await.unwrap();
    assert!(
        store
            .published_manifest(&segment.proof().reference())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn missing_block_is_an_error() {
    let scope = scope();
    let resource = ResourceIdentity::generate();
    let row = record(scope.clone(), resource, 10, 20);
    let version = row.interval.version;
    let (_, segment) = fixture(vec![row], scope).await.unwrap();
    // A different store cannot resolve the pinned attempt.
    let store = MemoryStorage::new();
    assert!(
        segment
            .exact(
                &store,
                resource,
                version,
                &mut ByteLimit::new(SEGMENT_BYTES).start()
            )
            .await
            .is_err()
    );
}

#[test]
fn bounded_encoding_accounts_for_json_escaping() {
    assert!(matches!(
        encode(&"\n".repeat(32), 64),
        Err(ArtifactError::Limit(_))
    ));
}

async fn verified_bytes(bytes: &[u8], block_bytes: u32) -> (MemoryStorage, VerifiedArtifact) {
    let store = MemoryStorage::new();
    let destination =
        Destination::new(DestinationId::generate(), "malformed-history-fixture").unwrap();
    store.register_destination(&destination).await.unwrap();
    let mut writer = ArtifactWriter::new(
        &store,
        ArtifactIdentity::new(destination.id()),
        ArtifactLimits::builder()
            .block_bytes(block_bytes)
            .artifact_bytes(SEGMENT_BYTES as u64)
            .build()
            .unwrap(),
    )
    .await
    .unwrap();
    writer.write(bytes).await.unwrap();
    let manifest = writer.seal().await.unwrap();
    let proof = verify(&store, &manifest).await.unwrap();
    (store, proof)
}

#[rstest]
#[case::unknown_segment_version(0)]
#[case::unknown_index_version(1)]
#[case::index_outside_segment(2)]
#[case::oversized_header(3)]
#[case::oversized_index(4)]
#[tokio::test]
async fn invalid_retained_format_is_rejected(#[case] variant: u8) {
    let scope = scope();
    let header = encode(&scope, 512).unwrap();
    let mut bytes = MAGIC.to_vec();
    write_frame(&mut bytes, &header).unwrap();
    bytes.extend_from_slice(b"[]");
    bytes.extend_from_slice(&2_u32.to_le_bytes());
    bytes.extend_from_slice(FOOTER);
    let footer = bytes.len() - 9;
    match variant {
        0 => bytes[4] = 2,
        1 => *bytes.last_mut().unwrap() = 2,
        2 => bytes[footer..footer + 4].copy_from_slice(&((footer + 1) as u32).to_le_bytes()),
        3 => bytes[5..9].copy_from_slice(&513_u32.to_le_bytes()),
        _ => bytes[footer..footer + 4].copy_from_slice(&((INDEX_BYTES + 1) as u32).to_le_bytes()),
    }
    let (store, proof) = verified_bytes(&bytes, BLOCK_BYTES).await;
    assert!(
        HistorySegment::open_verified(&store, proof, &mut ByteLimit::new(SEGMENT_BYTES).start())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn index_cannot_omit_record_bytes() {
    let header = encode(&scope(), 512).unwrap();
    let mut bytes = MAGIC.to_vec();
    write_frame(&mut bytes, &header).unwrap();
    write_frame(&mut bytes, b"omitted").unwrap();
    bytes.extend_from_slice(b"[]");
    bytes.extend_from_slice(&2_u32.to_le_bytes());
    bytes.extend_from_slice(FOOTER);
    let (store, proof) = verified_bytes(&bytes, BLOCK_BYTES).await;
    assert!(matches!(
        HistorySegment::open_verified(&store, proof, &mut ByteLimit::new(SEGMENT_BYTES).start())
            .await,
        Err(ArtifactError::Corrupt(_))
    ));
}

#[rstest]
#[case::initial_blocks(1024 * 1024)]
#[case::current_blocks(BLOCK_BYTES)]
#[tokio::test]
async fn retained_index_reopens_without_writer_state(#[case] block_bytes: u32) {
    let scope = scope();
    let resource = ResourceIdentity::generate();
    let mut row = record(scope.clone(), resource, 10, 20);
    row.payload = json!("x".repeat(1024 * 1024));
    let (store, segment) = fixture(vec![row.clone()], scope).await.unwrap();
    let manifest = segment.proof().manifest();
    let bytes = read_range(
        &store,
        manifest,
        0,
        manifest.byte_size() as usize,
        &mut ByteLimit::new(SEGMENT_BYTES).start(),
    )
    .await
    .unwrap();
    drop(segment);
    drop(store);
    let (store, proof) = verified_bytes(&bytes, block_bytes).await;
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    let reopened = HistorySegment::open_verified(&store, proof, &mut budget)
        .await
        .unwrap();
    assert_eq!(
        reopened
            .exact(&store, resource, row.interval.version, &mut budget)
            .await
            .unwrap(),
        Some(row)
    );
}

#[tokio::test]
async fn candidate_overflow_cannot_be_sealed() {
    let scope = scope();
    let records = (0..=MAX_RECORDS)
        .map(|_| record(scope.clone(), ResourceIdentity::generate(), 10, 20))
        .collect();
    assert!(matches!(
        fixture(records, scope).await,
        Err(ArtifactError::Limit(_))
    ));
}

#[rstest]
#[case::wrong_frame_length(0)]
#[case::wrong_version(1)]
#[case::wrong_collection(2)]
#[case::invalid_json(3)]
#[tokio::test]
async fn authentic_artifact_with_invalid_record_is_rejected(#[case] variant: u8) {
    let scope = scope();
    let mut row = record(scope.clone(), ResourceIdentity::generate(), 10, 20);
    let expected = row.interval.clone();
    match variant {
        1 => row.interval.version = VersionIdentity::generate(),
        2 => row.scope.collection = ResourceIdentity::generate(),
        _ => {}
    }
    let mut payload = encode(&row, ROW_BYTES - 4).unwrap();
    if variant == 3 {
        payload[0] = b'!';
    }
    let mut bytes = MAGIC.to_vec();
    write_frame(&mut bytes, &encode(&scope, 512).unwrap()).unwrap();
    let offset = bytes.len() as u64;
    write_frame(&mut bytes, &payload).unwrap();
    if variant == 0 {
        bytes[offset as usize..offset as usize + 4].copy_from_slice(&0_u32.to_le_bytes());
    }
    let entry = Entry {
        interval: expected.clone(),
        offset,
        length: (payload.len() + 4) as u32,
    };
    let index = encode(&vec![entry], INDEX_BYTES).unwrap();
    bytes.extend_from_slice(&index);
    bytes.extend_from_slice(&(index.len() as u32).to_le_bytes());
    bytes.extend_from_slice(FOOTER);
    let (store, proof) = verified_bytes(&bytes, BLOCK_BYTES).await;
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    let segment = HistorySegment::open_verified(&store, proof, &mut budget)
        .await
        .unwrap();
    assert!(matches!(
        segment
            .exact(&store, expected.resource, expected.version, &mut budget)
            .await,
        Err(ArtifactError::Corrupt(_))
    ));
}

#[rstest]
#[case::length_prefix(2, 0)]
#[case::payload(128, 0)]
#[case::block_sized_frame(2, BLOCK_BYTES as usize)]
#[case::largest_frame(2, ROW_BYTES)]
#[tokio::test]
async fn cross_block_reads_fit_p2_budget(
    #[case] before_boundary: usize,
    #[case] frame_bytes: usize,
    #[values(false, true)] as_of: bool,
) {
    let scope = scope();
    let resource = ResourceIdentity::generate();
    let mut selected = record(scope.clone(), resource, 10, 20);
    if frame_bytes > 0 {
        selected.payload = json!("");
        let overhead = serde_json::to_vec(&selected).unwrap().len() + 4;
        selected.payload = json!("x".repeat(frame_bytes - overhead));
    }
    let version = selected.interval.version;
    let result_bytes = serde_json::to_vec(&selected).unwrap().len();
    let block_bytes = BLOCK_BYTES as usize;
    let header_bytes = MAGIC.len() + 4 + encode(&scope, 512).unwrap().len();
    let mut padding = record(scope.clone(), ResourceIdentity::generate(), 10, 20);
    padding.payload = json!("");
    let padding_overhead = serde_json::to_vec(&padding).unwrap().len() + 4;
    // Place the selected prefix or payload across a full block boundary.
    padding.payload =
        json!("x".repeat(block_bytes - before_boundary - header_bytes - padding_overhead));
    let mut trailing = record(scope.clone(), ResourceIdentity::generate(), 10, 20);
    trailing.payload = json!("x".repeat(block_bytes));
    let (store, segment) = fixture(vec![padding, selected.clone(), trailing], scope)
        .await
        .unwrap();
    let mib = 1024 * 1024;
    let p2_limit = if as_of {
        (16 * result_bytes + 2 * mib).min(result_bytes + 4 * mib)
    } else {
        (8 * result_bytes + mib).min(result_bytes + 2 * mib)
    };
    let mut budget = ByteLimit::new(p2_limit).start();
    let restored = if as_of {
        segment.as_of(&store, resource, at(10), &mut budget).await
    } else {
        segment.exact(&store, resource, version, &mut budget).await
    }
    .unwrap();
    assert_eq!(restored, Some(selected));
}
