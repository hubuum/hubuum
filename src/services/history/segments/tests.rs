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

async fn staged_bytes(store: &MemoryStorage, segment: &HistorySegment) -> Vec<u8> {
    let manifest = segment.proof().manifest();
    SegmentBlockReader::new(store, manifest, &mut ByteLimit::new(SEGMENT_BYTES).start())
        .range(0, manifest.byte_size() as usize)
        .await
        .unwrap()
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
            .artifact_bytes(bytes.len() as u64)
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
#[case::mismatched_header_footer(5)]
#[tokio::test]
async fn invalid_retained_format_is_rejected(#[case] variant: u8) {
    let scope = scope();
    let header = encode(&scope, 512).unwrap();
    let mut bytes = b"HUBH\x01".to_vec();
    write_frame(&mut bytes, &header).unwrap();
    bytes.extend_from_slice(b"[]");
    bytes.extend_from_slice(&2_u32.to_le_bytes());
    bytes.extend_from_slice(b"HUBI\x01");
    let footer = bytes.len() - 9;
    match variant {
        0 => bytes[4] = u8::MAX,
        1 => *bytes.last_mut().unwrap() = u8::MAX,
        2 => bytes[footer..footer + 4].copy_from_slice(&((footer + 1) as u32).to_le_bytes()),
        3 => bytes[5..9].copy_from_slice(&513_u32.to_le_bytes()),
        4 => bytes[footer..footer + 4].copy_from_slice(&((INDEX_BYTES + 1) as u32).to_le_bytes()),
        _ => bytes[4] = 2,
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
    let mut bytes = b"HUBH\x01".to_vec();
    write_frame(&mut bytes, &header).unwrap();
    write_frame(&mut bytes, b"omitted").unwrap();
    bytes.extend_from_slice(b"[]");
    bytes.extend_from_slice(&2_u32.to_le_bytes());
    bytes.extend_from_slice(b"HUBI\x01");
    let (store, proof) = verified_bytes(&bytes, BLOCK_BYTES).await;
    assert!(matches!(
        HistorySegment::open_verified(&store, proof, &mut ByteLimit::new(SEGMENT_BYTES).start())
            .await,
        Err(ArtifactError::Corrupt(_))
    ));
}

// Construct retained bytes independently of the current writer.
fn legacy_bytes(row: &SegmentRecord, version: u8) -> Vec<u8> {
    let mut bytes = b"HUBH".to_vec();
    bytes.push(version);
    write_frame(&mut bytes, &serde_json::to_vec(&row.scope).unwrap()).unwrap();
    let offset = bytes.len();
    write_frame(&mut bytes, &serde_json::to_vec(row).unwrap()).unwrap();
    let length = bytes.len() - offset;
    let index = match version {
        1 => serde_json::to_vec(&json!([{
            "interval": row.interval, "offset": offset, "length": length,
        }]))
        .unwrap(),
        2 => {
            let mut index = row.interval.resource.as_bytes().to_vec();
            index.extend_from_slice(row.interval.version.as_bytes());
            for time in [row.interval.valid_from, row.interval.valid_to] {
                index.extend_from_slice(&time.timestamp().to_le_bytes());
                index.extend_from_slice(&time.timestamp_subsec_nanos().to_le_bytes());
            }
            index.extend_from_slice(&(offset as u32).to_le_bytes());
            index.extend_from_slice(&(length as u32).to_le_bytes());
            index
        }
        _ => panic!("Unsupported retained fixture"),
    };
    bytes.extend_from_slice(&index);
    bytes.extend_from_slice(&(index.len() as u32).to_le_bytes());
    bytes.extend_from_slice(b"HUBI");
    bytes.push(version);
    bytes
}

#[rstest]
#[case::initial_v1_blocks(1, 1024 * 1024)]
#[case::later_v1_blocks(1, 512 * 1024)]
#[case::fixed_width_v2(2, 64 * 1024)]
#[tokio::test]
async fn retained_index_reopens_without_writer_state(
    #[case] version: u8,
    #[case] block_bytes: u32,
    #[values(false, true)] as_of: bool,
) {
    let mut row = record(scope(), ResourceIdentity::generate(), 10, 20);
    row.payload = json!("x".repeat(1024 * 1024));
    let (store, proof) = verified_bytes(&legacy_bytes(&row, version), block_bytes).await;
    let reference = store.publish(&proof).await.unwrap();
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    let segment = RetainedHistorySegment::open(&store, &reference, &mut budget)
        .await
        .unwrap();
    assert_eq!(segment.format_version(), u16::from(version));
    let restored = if as_of {
        segment
            .as_of(
                &store,
                row.interval.resource,
                row.interval.valid_from,
                &mut budget,
            )
            .await
    } else {
        segment
            .exact(
                &store,
                row.interval.resource,
                row.interval.version,
                &mut budget,
            )
            .await
    }
    .unwrap();
    assert_eq!(restored, Some(row));
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
async fn authentic_artifact_with_invalid_record_is_rejected(
    #[case] variant: u8,
    #[values(
        SegmentFormat::Json,
        SegmentFormat::Compact,
        SegmentFormat::CompressedIndex
    )]
    format: SegmentFormat,
    #[values(false, true)] as_of: bool,
) {
    let scope = scope();
    let mut row = record(scope.clone(), ResourceIdentity::generate(), 10, 20);
    let expected = row.interval.clone();
    match variant {
        1 => {
            let mut bytes = *expected.version.as_bytes();
            bytes[0] ^= 0xff;
            row.interval.version = VersionIdentity::from_bytes(bytes).unwrap();
        }
        2 => row.scope.collection = ResourceIdentity::generate(),
        _ => {}
    }
    let mut payload = encode(&row, ROW_BYTES - 4).unwrap();
    if variant == 3 {
        payload[0] = b'!';
    }
    let mut bytes = format.header().to_vec();
    write_frame(&mut bytes, &encode(&scope, 512).unwrap()).unwrap();
    let offset = bytes.len() as u32;
    write_frame(&mut bytes, &payload).unwrap();
    if variant == 0 {
        bytes[offset as usize..offset as usize + 4].copy_from_slice(&0_u32.to_le_bytes());
    }
    let entry = Entry {
        interval: expected.clone(),
        offset,
        length: (payload.len() + 4) as u32,
    };
    let entries = [entry];
    let index = format.encode_index(&entries).unwrap();
    bytes.extend_from_slice(&index);
    bytes.extend_from_slice(&(index.len() as u32).to_le_bytes());
    bytes.extend_from_slice(format.footer());
    let (store, proof) = verified_bytes(&bytes, BLOCK_BYTES).await;
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    let segment = HistorySegment::open_verified(&store, proof, &mut budget)
        .await
        .unwrap();
    let read = if as_of {
        segment
            .as_of(&store, expected.resource, expected.valid_from, &mut budget)
            .await
    } else {
        segment
            .exact(&store, expected.resource, expected.version, &mut budget)
            .await
    };
    assert!(matches!(read, Err(ArtifactError::Corrupt(_))));
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
    let header_bytes = MARKER_BYTES + encode(&scope, 512).unwrap().len();
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

#[rstest]
#[case::original_blocks(1024 * 1024)]
#[case::current_blocks(BLOCK_BYTES)]
#[tokio::test]
async fn published_reference_roundtrip_reads_retained_bytes(#[case] block_bytes: u32) {
    let scope = scope();
    let resource = ResourceIdentity::generate();
    let row = record(scope.clone(), resource, 10, 20);
    let (store, segment) = fixture(vec![row.clone()], scope).await.unwrap();
    let bytes = staged_bytes(&store, &segment).await;
    drop(segment);
    drop(store);
    let (store, proof) = verified_bytes(&bytes, block_bytes).await;
    let reference = store.publish(&proof).await.unwrap();
    // Retain only the catalog representation, as a different worker would.
    let persisted = serde_json::to_vec(&reference).unwrap();
    drop(proof);
    let restored: hubuum_artifacts::ArtifactReference = serde_json::from_slice(&persisted).unwrap();
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    let reader = RetainedHistorySegment::open(&store, &restored, &mut budget)
        .await
        .unwrap();
    assert_eq!(
        reader
            .exact(&store, resource, row.interval.version(), &mut budget)
            .await
            .unwrap(),
        Some(row)
    );
}

#[rstest]
#[case::single_block(Some(BLOCK_BYTES))]
#[case::index_spans_blocks(Some(128))]
#[case::footer_straddles_blocks(None)]
#[tokio::test]
async fn cold_open_charges_shared_framing_blocks_once_per_open(#[case] block_bytes: Option<u32>) {
    let scope = scope();
    let mut row = record(scope.clone(), ResourceIdentity::generate(), 10, 20);
    row.payload = json!({"data": "x".repeat(4096)});
    let mut records = vec![row];
    records.extend((0..3).map(|_| record(scope.clone(), ResourceIdentity::generate(), 10, 20)));
    let (original_store, segment) = fixture(records, scope).await.unwrap();
    let bytes = staged_bytes(&original_store, &segment).await;
    let block_bytes = block_bytes.unwrap_or(bytes.len() as u32 - 4);
    let (store, proof) = verified_bytes(&bytes, block_bytes).await;
    let reference = store.publish(&proof).await.unwrap();

    // The complete scope fits in the first block. The index and footer occupy
    // every block from the start of the index's block through the end.
    let block_size = block_bytes as usize;
    let index_offset = bytes.len() - MARKER_BYTES - segment.index_bytes();
    let tail_start = index_offset / block_size * block_size;
    let header_bytes_outside_tail = block_size.min(tail_start);
    let bytes_per_open =
        proof.manifest().encoded_len() + header_bytes_outside_tail + bytes.len() - tail_start;

    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    for _ in 0..2 {
        RetainedHistorySegment::open(&store, &reference, &mut budget)
            .await
            .unwrap();
    }
    assert_eq!(budget.used(), 2 * bytes_per_open);
}

#[tokio::test]
async fn retained_reader_requires_durable_publication() {
    let (store, segment) = fixture(Vec::new(), scope()).await.unwrap();
    assert!(
        RetainedHistorySegment::open(
            &store,
            &segment.proof().reference(),
            &mut ByteLimit::new(SEGMENT_BYTES).start()
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn retained_reader_rejects_wrong_catalog_digest() {
    let (store, segment) = fixture(Vec::new(), scope()).await.unwrap();
    let reference = store.publish(segment.proof()).await.unwrap();
    let wrong =
        hubuum_artifacts::ArtifactReference::from_catalog(reference.identity().clone(), [0; 32]);
    assert!(
        RetainedHistorySegment::open(&store, &wrong, &mut ByteLimit::new(SEGMENT_BYTES).start())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn retained_reader_cannot_rebind_object_version() {
    let (store, segment) = fixture(Vec::new(), scope()).await.unwrap();
    let reference = store.publish(segment.proof()).await.unwrap();
    let id = reference.identity();
    let wrong = hubuum_artifacts::ArtifactReference::from_catalog(
        ArtifactIdentity::from_parts(
            id.artifact(),
            id.attempt(),
            id.destination(),
            hubuum_artifacts::ObjectVersion::generate(),
        ),
        *reference.manifest_digest(),
    );
    assert!(
        RetainedHistorySegment::open(&store, &wrong, &mut ByteLimit::new(SEGMENT_BYTES).start())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn retained_open_charges_manifest_before_external_work() {
    let (store, segment) = fixture(Vec::new(), scope()).await.unwrap();
    let reference = store.publish(segment.proof()).await.unwrap();
    assert!(matches!(
        RetainedHistorySegment::open(&store, &reference, &mut ByteLimit::new(0).start()).await,
        Err(ArtifactError::Limit(_))
    ));
}

#[rstest]
#[case::before(9, false)]
#[case::start(10, true)]
#[case::end(20, false)]
#[tokio::test]
async fn published_as_of_preserves_original_half_open_interval(
    #[case] instant: i64,
    #[case] present: bool,
) {
    let scope = scope();
    let resource = ResourceIdentity::generate();
    let row = record(scope.clone(), resource, 10, 20);
    let (store, segment) = fixture(vec![row], scope).await.unwrap();
    let reference = store.publish(segment.proof()).await.unwrap();
    drop(segment);
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    let reader = RetainedHistorySegment::open(&store, &reference, &mut budget)
        .await
        .unwrap();
    assert_eq!(
        reader
            .as_of(&store, resource, at(instant), &mut budget)
            .await
            .unwrap()
            .is_some(),
        present
    );
}

#[rstest]
#[case::invalid_resource(0..16, 0)]
#[case::invalid_version(16..32, 0)]
#[case::invalid_timestamp(40..44, 0xff)]
#[case::reversed_interval(44..52, 0)]
#[case::wrong_offset(56..60, 0)]
#[case::wrong_length(60..64, 0)]
#[tokio::test]
async fn invalid_compact_entry_is_corrupt(#[case] field: std::ops::Range<usize>, #[case] fill: u8) {
    let scope = scope();
    let row = record(scope.clone(), ResourceIdentity::generate(), 10, 20);
    let mut bytes = legacy_bytes(&row, 2);
    let start = bytes.len() - MARKER_BYTES - 64;
    bytes[start + field.start..start + field.end].fill(fill);
    let (store, proof) = verified_bytes(&bytes, BLOCK_BYTES).await;
    assert!(matches!(
        HistorySegment::open_verified(&store, proof, &mut ByteLimit::new(SEGMENT_BYTES).start())
            .await,
        Err(ArtifactError::Corrupt(_))
    ));
}

#[test]
fn compact_index_rejects_a_partial_entry() {
    assert!(matches!(
        SegmentFormat::Compact.decode_index(&[0; 63]),
        Err(ArtifactError::Corrupt(_))
    ));
}

#[rstest]
#[case::one(0, 1)]
#[case::two(1, 2)]
#[case::four(2, 4)]
#[case::eight(6, 8)]
#[case::full(12, 16)]
#[tokio::test]
async fn compressed_index_widens_keys_until_versions_are_unambiguous(
    #[case] difference: usize,
    #[case] width: u8,
) {
    let scope = scope();
    let resource = ResourceIdentity::generate();
    let first = record(scope.clone(), resource, 1, 2);
    let mut second = record(scope.clone(), resource, 2, 3);
    let mut bytes = *first.interval.version.as_bytes();
    bytes[difference] ^= 1;
    second.interval.version = VersionIdentity::from_bytes(bytes).unwrap();
    let (store, segment) = fixture(vec![first, second.clone()], scope).await.unwrap();
    assert_eq!(segment.reader.key_width as u8, width);
    assert_eq!(
        segment
            .exact(
                &store,
                resource,
                second.interval.version,
                &mut ByteLimit::new(SEGMENT_BYTES).start()
            )
            .await
            .unwrap(),
        Some(second)
    );
}

#[tokio::test]
async fn verified_segment_rejects_a_duplicate_shortened_key_on_reopen() {
    let scope = scope();
    let resource = ResourceIdentity::generate();
    let mut rows = [
        record(scope.clone(), resource, 1, 2),
        record(scope.clone(), resource, 2, 3),
    ];
    for (row, byte) in rows.iter_mut().zip([1, 2]) {
        let mut bytes = *row.interval.version.as_bytes();
        bytes[0] = byte;
        row.interval.version = VersionIdentity::from_bytes(bytes).unwrap();
    }
    let (store, segment) = fixture(rows.to_vec(), scope).await.unwrap();
    let mut bytes = staged_bytes(&store, &segment).await;
    let index_start = bytes.len() - MARKER_BYTES - segment.index_bytes();
    assert_eq!(bytes[index_start], 1);
    let index_end = bytes.len() - MARKER_BYTES;
    let mut columns = zstd::bulk::decompress(&bytes[index_start + 1..index_end], 2 * 49).unwrap();
    // Sixteen two-byte resource columns precede the version prefix column.
    columns[33] = 0; // Zero difference makes the second key equal to the first.
    let frame = zstd::bulk::compress(&columns, 3).unwrap();
    bytes.truncate(index_start + 1);
    bytes.extend_from_slice(&frame);
    bytes.extend_from_slice(&((1 + frame.len()) as u32).to_le_bytes());
    bytes.extend_from_slice(SegmentFormat::CompressedIndex.footer());
    let (store, proof) = verified_bytes(&bytes, BLOCK_BYTES).await;
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    assert!(matches!(
        HistorySegment::open_verified(&store, proof, &mut budget).await,
        Err(ArtifactError::Corrupt(_))
    ));
}

#[tokio::test]
async fn an_absent_version_sharing_an_index_key_cannot_return_another_version() {
    let scope = scope();
    let resource = ResourceIdentity::generate();
    let original = record(scope.clone(), resource, 1, 2);
    let mut other = *original.interval.version.as_bytes();
    other[15] ^= 1;
    let missing = VersionIdentity::from_bytes(other).unwrap();
    let (store, segment) = fixture(vec![original], scope).await.unwrap();
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    assert_eq!(
        segment
            .exact(&store, resource, missing, &mut budget)
            .await
            .unwrap(),
        None
    );
    assert!(
        budget.used() > 0,
        "the candidate record was read and verified"
    );
}

#[tokio::test]
async fn compact_index_preserves_subsecond_and_pre_epoch_intervals() {
    let scope = scope();
    let interval = VersionInterval::new(
        ResourceIdentity::generate(),
        VersionIdentity::generate(),
        DateTime::from_timestamp(-1, 987_654_321).unwrap(),
        DateTime::from_timestamp(59, 1_123_456_789).unwrap(),
    )
    .unwrap();
    let row = SegmentRecord::new(scope.clone(), interval.clone(), json!(null));
    let (store, segment) = fixture(vec![row.clone()], scope).await.unwrap();
    assert_eq!(
        segment
            .as_of(
                &store,
                interval.resource,
                interval.valid_from,
                &mut ByteLimit::new(SEGMENT_BYTES).start()
            )
            .await
            .unwrap(),
        Some(row)
    );
}

#[rstest]
#[tokio::test]
async fn cold_lookup_with_a_full_compact_index_fits_p2(#[values(false, true)] as_of: bool) {
    let scope = scope();
    let records = (0..MAX_RECORDS)
        .map(|_| record(scope.clone(), ResourceIdentity::generate(), 10, 20))
        .collect::<Vec<_>>();
    let row = records[MAX_RECORDS / 2].clone();
    let (store, segment) = fixture(records, scope).await.unwrap();
    let reference = store.publish(segment.proof()).await.unwrap();
    drop(segment);
    // Manifest I/O reserves the artifact contract's full 4 MiB error envelope.
    // P2 measures actual physical bytes for the successful complete lookup.
    let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
    let reader = RetainedHistorySegment::open(&store, &reference, &mut budget)
        .await
        .unwrap();
    let result = if as_of {
        reader
            .as_of(
                &store,
                row.interval.resource,
                row.interval.valid_from,
                &mut budget,
            )
            .await
    } else {
        reader
            .exact(
                &store,
                row.interval.resource,
                row.interval.version,
                &mut budget,
            )
            .await
    }
    .unwrap();
    let result_bytes = serde_json::to_vec(&row).unwrap().len();
    assert_eq!(result, Some(row));
    let limit = if as_of {
        2 * 1024 * 1024 + 16 * result_bytes
    } else {
        1024 * 1024 + 8 * result_bytes
    };
    assert!(
        budget.used() <= limit,
        "charged {} bytes against {limit}",
        budget.used()
    );
}

mod rewrite;
