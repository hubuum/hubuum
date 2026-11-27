use super::*;
use crate::{RuntimeRegistration, StorageIdentitySubject};
use hubuum_domain::IdentityReference;
use rstest::rstest;
use serde_json::json;

fn at(value: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(value, 0).unwrap()
}
pub(super) fn row() -> HistoryArchiveRow {
    let identity =
        StorageRecordIdentity::new(ResourceIdentity::generate(), VersionIdentity::generate());
    let evidence = StorageIdentityEvidence::try_new(
        Some(StorageIdentitySubject::Resource {
            identity: IdentityReference::Known {
                identity: identity.resource(),
            },
            version: identity.version(),
        }),
        Default::default(),
    )
    .unwrap();
    HistoryArchiveRow::new(
        identity,
        at(10),
        None,
        json!({"id":1,"name":"collection","description":"","created_at":at(0),"updated_at":at(1),"revision":1,"logical_identity":evidence}),
    )
    .unwrap()
}
fn permit() -> StorageExecutionPermit {
    StorageExecutionPermit::new(
        Some(RuntimeRegistration::generate().owner()),
        ApplicationFence::new(Uuid::new_v4(), 0).unwrap(),
    )
}

fn capture() -> (HistoryArchiveCapture, StorageExecutionPermit) {
    let row = row();
    let permit = permit();
    let request = HistoryArchiveCaptureRequest::new(
        PrincipalIdentity::generate(),
        HistoryArchiveSelection::new(
            StorageBackupHistorySection::CollectionHistory,
            row.identity().resource(),
            HistoryArchivePoint::Exact {
                version: row.identity().version(),
            },
        ),
        HistoryArchiveCatalogGeneration::default(),
        at(100),
    );
    (
        HistoryArchiveCapture::capture(
            request,
            &HistoryArchiveCatalog::empty(row.identity().resource()),
            &permit,
            HistoryArchiveInput::Available(row),
            at(50),
        )
        .unwrap(),
        permit,
    )
}

#[rstest]
#[case::nil_id("id",json!(Uuid::nil()))]
#[case::nil_owner("runtime",json!(Uuid::nil()))]
#[case::negative_generation("generation",json!(-1))]
#[case::work_overflow("reserved_read_bytes",json!(HISTORY_ARCHIVE_READ_BYTES+1))]
#[case::renewed_lifetime("available_until",json!(at(50)+Duration::hours(49)))]
#[test]
fn persisted_capture_rejects_malformed_invariants(#[case] key: &str, #[case] value: Value) {
    let (capture, _) = capture();
    let mut encoded = serde_json::to_value(capture).unwrap();
    encoded[key] = value;
    assert!(serde_json::from_value::<HistoryArchiveCapture>(encoded).is_err());
}
#[test]
fn restored_application_cannot_reuse_capture_even_with_same_logical_identity() {
    let (capture, permit) = capture();
    let restored = StorageExecutionPermit::new(
        permit.owner().cloned(),
        ApplicationFence::new(Uuid::new_v4(), 0).unwrap(),
    );
    assert!(
        capture
            .validate_access(capture.principal(), &restored)
            .is_err()
    );
}
#[test]
fn later_deletion_revision_cannot_refresh_capture() {
    let (capture, permit) = capture();
    let later = StorageExecutionPermit::new(
        permit.owner().cloned(),
        ApplicationFence::new(permit.fence().incarnation(), 1).unwrap(),
    );
    assert!(
        capture
            .validate_access(capture.principal(), &later)
            .is_err()
    );
}
#[rstest]
#[case::ready(99, HistoryArchiveCoverageState::Complete)]
#[case::deadline(100, HistoryArchiveCoverageState::Expired)]
#[case::later(101, HistoryArchiveCoverageState::Expired)]
#[test]
fn original_lifetime_is_not_renewed_by_read_work(
    #[case] now: i64,
    #[case] expected: HistoryArchiveCoverageState,
) {
    let (mut capture, _) = capture();
    capture.reserve(1024, at(60)).unwrap();
    assert_eq!(capture.coverage(at(now)), expected);
}
#[test]
fn encoded_row_limit_counts_json_escaping() {
    let row = row();
    let mut payload = row.payload().clone();
    payload["description"] = json!("\n".repeat(HISTORY_ARCHIVE_ROW_BYTES / 2));
    assert_eq!(
        HistoryArchiveRow::new(row.identity(), row.valid_from(), None, payload)
            .unwrap_err()
            .kind(),
        crate::StorageErrorKind::InputTooLarge
    );
}
#[test]
fn cumulative_work_is_persisted_and_cannot_wrap() {
    let (mut capture, permit) = capture();
    capture.reserve(HISTORY_ARCHIVE_READ_BYTES, at(60)).unwrap();
    let mut restored: HistoryArchiveCapture = serde_json::from_slice(
        &history_archive_encoded_bytes(&capture, HISTORY_ARCHIVE_CAPTURE_BYTES).unwrap(),
    )
    .unwrap();
    restored
        .validate_access(restored.principal(), &permit)
        .unwrap();
    assert!(restored.reserve(u64::MAX, at(60)).is_err());
}

#[test]
fn staging_budget_evidence_is_bounded_and_redacted() {
    let (capture, permit) = capture();
    let request = HistoryArchiveCaptureRequest::new(
        capture.principal(),
        capture.selection().clone(),
        capture.generation(),
        capture.available_until(),
    );
    let error = capture.staging_budget_error();
    assert_eq!(error.kind(), crate::StorageErrorKind::InputTooLarge);
    let evidence = error.history_archive_budget_evidence().unwrap();
    evidence
        .validate_request(&request, &permit, at(60))
        .unwrap();
    let text = format!("{error:?} {error}");
    assert!(!text.contains(capture.authorization().unwrap().name()));
    assert!(!text.contains(&capture.runtime.to_string()));
    assert!(!text.contains(&capture.principal().to_string()));
    assert!(std::mem::size_of::<HistoryArchiveBudgetEvidence>() + 4096 <= 8192);
}

#[rstest]
#[case::principal("principal")]
#[case::selection("selection")]
#[case::generation("generation")]
#[case::deadline("deadline")]
#[case::owner("owner")]
#[case::fence("fence")]
#[case::expired("expired")]
#[case::clock_regression("clock")]
#[test]
fn staging_budget_evidence_rejects_changed_bindings(#[case] changed: &str) {
    let (capture, mut permit) = capture();
    let error = capture.staging_budget_error();
    let mut request = HistoryArchiveCaptureRequest::new(
        capture.principal(),
        capture.selection().clone(),
        capture.generation(),
        capture.available_until(),
    );
    let mut now = at(60);
    match changed {
        "principal" => request.principal = PrincipalIdentity::generate(),
        "selection" => request.selection.resource = ResourceIdentity::generate(),
        "generation" => request.generation = HistoryArchiveCatalogGeneration(1),
        "deadline" => request.available_until = at(101),
        "owner" => {
            permit = StorageExecutionPermit::new(
                Some(RuntimeRegistration::generate().owner()),
                permit.fence(),
            )
        }
        "fence" => {
            permit = StorageExecutionPermit::new(
                permit.owner().cloned(),
                ApplicationFence::new(Uuid::new_v4(), 0).unwrap(),
            )
        }
        "expired" => now = at(100),
        "clock" => now = at(49),
        _ => unreachable!(),
    }
    assert!(
        error
            .history_archive_budget_evidence()
            .unwrap()
            .validate_request(&request, &permit, now)
            .is_err()
    );
}

fn retained_fixture() -> (
    HistoryArchiveRow,
    HistoryArchiveCatalog,
    HistoryArchiveCaptureRequest,
) {
    use crate::artifacts::{ArtifactIdentity, DestinationId};

    let original = row();
    let mut payload = original.payload().clone();
    payload["description"] = json!("x".repeat(64 * 1024));
    let row = HistoryArchiveRow::new(original.identity(), at(10), Some(at(20)), payload).unwrap();
    let empty = HistoryArchiveCatalog::empty(row.identity().resource());
    let descriptor = HistoryArchiveDescriptor::new(
        &row,
        Vec::new(),
        3,
        ArtifactReference::from_catalog(ArtifactIdentity::new(DestinationId::generate()), [42; 32]),
    )
    .unwrap();
    let catalog = empty.publish(empty.generation(), vec![descriptor]).unwrap();
    let request = HistoryArchiveCaptureRequest::new(
        PrincipalIdentity::generate(),
        HistoryArchiveSelection::new(
            StorageBackupHistorySection::CollectionHistory,
            row.identity().resource(),
            HistoryArchivePoint::AsOf { at: at(15) },
        ),
        catalog.generation(),
        at(100),
    );
    (row, catalog, request)
}

fn retained_capture(compact: bool) -> (HistoryArchiveCapture, HistoryArchiveRow) {
    let (row, catalog, request) = retained_fixture();
    let request = if compact {
        request.for_retained_read()
    } else {
        request
    };
    let capture = HistoryArchiveCapture::capture(
        request,
        &catalog,
        &permit(),
        HistoryArchiveInput::Available(row.clone()),
        at(50),
    )
    .unwrap();
    (capture, row)
}

#[rstest]
#[case::missing(false, HistoryArchiveCoverageState::Unavailable)]
#[case::substituted(true, HistoryArchiveCoverageState::Corrupt)]
fn retained_read_requires_the_original_primary_at_capture(
    #[case] substituted: bool,
    #[case] expected: HistoryArchiveCoverageState,
) {
    let (row, catalog, request) = retained_fixture();
    let input = if substituted {
        let mut payload = row.payload().clone();
        payload["description"] = json!("different source content");
        HistoryArchiveInput::Available(
            HistoryArchiveRow::new(row.identity(), row.valid_from(), row.valid_to(), payload)
                .unwrap(),
        )
    } else {
        HistoryArchiveInput::Unavailable
    };
    let capture = HistoryArchiveCapture::capture(
        request.for_retained_read(),
        &catalog,
        &permit(),
        input,
        at(50),
    )
    .unwrap();
    assert_eq!(capture.coverage(at(50)), expected);
    assert!(
        capture
            .resolve_retained_row(row.identity(), at(10)..at(20), &[], row.payload().clone())
            .is_err()
    );
}

#[rstest]
#[case::original_full_capture(false)]
#[case::verified_primary(true)]
#[test]
fn persisted_primary_reconstructs_the_original_row(#[case] compact: bool) {
    let (capture, row) = retained_capture(compact);
    let encoded = serde_json::to_vec(&capture).unwrap();
    let document: Value = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(
        document.pointer("/hot/verified_primary_v1").is_some(),
        compact
    );
    assert_eq!(document.pointer("/hot/payload").is_some(), !compact);
    assert_eq!(encoded.len() < 8192, compact);
    let restored: HistoryArchiveCapture = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(restored.hot().is_none(), compact);
    assert_eq!(
        restored
            .resolve_retained_row(row.identity(), at(10)..at(20), &[], row.payload().clone())
            .unwrap(),
        row
    );
}

#[rstest]
#[case::missing_pin("archive", Value::Null)]
#[case::authorization_name("authorization.name", json!("other"))]
#[case::observed_name("hot.verified_primary_v1.authorization.name", json!("other"))]
#[case::observed_interval("hot.verified_primary_v1.valid_to", json!(at(16)))]
#[case::changed_selection("selection.point.at", json!(at(23)))]
#[case::unsupported_format("archive.format_version", json!(99))]
#[case::dependencies("archive.dependencies", json!([VersionIdentity::generate()]))]
#[case::content_commitment("hot.verified_primary_v1.payload_digest", json!(vec![0u8; 32]))]
#[test]
fn persisted_read_proof_rejects_a_substituted_binding(#[case] path: &str, #[case] value: Value) {
    let (capture, _) = retained_capture(true);
    let mut document = serde_json::to_value(capture).unwrap();
    let pointer = format!("/{}", path.replace('.', "/"));
    *document.pointer_mut(&pointer).expect("existing binding") = value;
    assert!(serde_json::from_value::<HistoryArchiveCapture>(document).is_err());
}

#[test]
fn full_and_verified_primary_encodings_cannot_be_combined() {
    let (capture, row) = retained_capture(true);
    let mut document = serde_json::to_value(capture).unwrap();
    let full = serde_json::to_value(row).unwrap();
    document["hot"]
        .as_object_mut()
        .unwrap()
        .extend(full.as_object().unwrap().clone());
    assert!(serde_json::from_value::<HistoryArchiveCapture>(document).is_err());
}

#[rstest]
#[case::identity("identity")]
#[case::interval("interval")]
#[case::dependencies("dependencies")]
#[case::content("content")]
#[case::timestamp_spelling("timestamp")]
#[test]
fn retained_row_requires_the_original_verified_frame(#[case] changed: &str) {
    let (capture, row) = retained_capture(true);
    let mut identity = row.identity();
    let mut interval = at(10)..at(20);
    let mut dependencies = Vec::new();
    let mut payload = row.payload().clone();
    match changed {
        "identity" => {
            identity = StorageRecordIdentity::new(identity.resource(), VersionIdentity::generate())
        }
        "interval" => interval.end = at(19),
        "dependencies" => dependencies.push(VersionIdentity::generate()),
        "content" => payload["description"] = json!("substituted"),
        "timestamp" => payload["created_at"] = json!("1970-01-01T00:00:00+00:00"),
        _ => unreachable!(),
    }
    assert!(
        capture
            .resolve_retained_row(identity, interval, &dependencies, payload)
            .is_err()
    );
}
