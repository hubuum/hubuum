use std::collections::BTreeMap;

use chrono::TimeZone;
use hubuum_domain::{
    ClassId, CollectionId, HistoryRecordId, IdentityReference, PrincipalIdentity, ResourceRevision,
    VersionIdentity,
};
use hubuum_storage_core::{
    HistoryDeletionRevision, HistoryExpiryAdmissionRequest, HistoryExpiryCandidate,
    HistoryExpiryIntent, HistoryExpirySelection, HistoryExpirySelectionOutcome,
    HistoryExpirySizeSelection, HistoryExpiryTrigger, HistoryLifecycleControl,
    HistoryLifecycleSnapshot, HistoryLogicalBytes, HistoryObjectAuthorizationFacts,
    HistoryRetentionSeconds, HistorySizeCensus, StorageCapturedIdentity, StorageHistoryMetadata,
    StorageHistoryOperation, StorageIdentityEvidence, StorageIdentitySubject, StorageObject,
    StorageObjectHistoryRecord, StorageRecordMetadata,
};

use super::*;

fn at(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0).single().unwrap()
}
fn intent(size: bool) -> HistoryExpiryIntent {
    let collection = ResourceIdentity::generate();
    let class = ResourceIdentity::generate();
    let evidence = StorageIdentityEvidence::try_new(
        Some(StorageIdentitySubject::Resource {
            identity: IdentityReference::Known {
                identity: ResourceIdentity::generate(),
            },
            version: VersionIdentity::generate(),
        }),
        BTreeMap::from([
            (
                "/collection_id".into(),
                StorageCapturedIdentity::Resource {
                    family: StorageIdentityResourceKind::Collection,
                    reference: IdentityReference::Known {
                        identity: collection,
                    },
                },
            ),
            (
                "/class_id".into(),
                StorageCapturedIdentity::Resource {
                    family: StorageIdentityResourceKind::Class,
                    reference: IdentityReference::Known { identity: class },
                },
            ),
        ]),
    )
    .unwrap();
    let record = StorageObject::new(
        StorageRecordMetadata::try_new(
            ResourceId::new(1).unwrap(),
            at(50),
            at(100),
            ResourceRevision::INITIAL,
        )
        .unwrap(),
        "protected_original_name_marker",
        CollectionId::new(1).unwrap(),
        ClassId::new(1).unwrap(),
        serde_json::json!({"private_payload_marker":true}),
        "private_description_marker",
    );
    let metadata = StorageHistoryMetadata::try_new(
        StorageHistoryOperation::Update,
        at(100),
        Some(at(150)),
        HistoryRecordId::new(1).unwrap(),
        ResourceRevision::INITIAL,
    )
    .unwrap()
    .with_logical_identity(evidence);
    let facts = HistoryObjectAuthorizationFacts::capture(
        StorageObjectHistoryRecord::try_new(record, metadata).unwrap(),
    )
    .unwrap();
    let candidate = HistoryExpiryCandidate::new(
        StorageIdentityHistoryKind::Object,
        facts.target(),
        Some(HistoryRetentionAnchor::Closed(at(150))),
        vec![facts.attribution()],
        false,
    )
    .unwrap()
    .with_read_span(facts.read_span().clone())
    .unwrap()
    .with_object_authorization(facts)
    .unwrap();
    let key = HistoryPolicyKey::new(
        StorageIdentityHistoryKind::Object,
        HistoryPolicyScope::Deployment,
    )
    .unwrap();
    let empty = HistoryLifecycleSnapshot::new(
        HistoryLifecycleControl::empty(),
        Uuid::new_v4(),
        HistoryDeletionRevision::new(0).unwrap(),
        at(0),
    )
    .unwrap();
    let proposal = HistoryPolicyChange::new(
        key,
        HistoryPolicyRevision::new(0).unwrap(),
        HistoryPolicyBinding::Expire {
            trigger: if size {
                HistoryExpiryTrigger::LogicalSize {
                    upper_logical_bytes: HistoryLogicalBytes::new(1).unwrap(),
                    lower_logical_bytes: HistoryLogicalBytes::new(0).unwrap(),
                }
            } else {
                HistoryExpiryTrigger::Age {
                    age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
                    deleted_age_seconds: None,
                }
            },
            minimum_age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
        },
        HistoryPolicyApplication::Prospective,
    )
    .unwrap();
    let applied = empty
        .control()
        .apply(
            HistoryPolicyApplyRequest::new(proposal, None),
            PrincipalIdentity::generate(),
            empty.fence(),
            at(0),
        )
        .unwrap();
    let snapshot = HistoryLifecycleSnapshot::new(
        applied.next().clone(),
        empty.fence().incarnation(),
        empty.fence().deletion_revision(),
        at(200),
    )
    .unwrap();
    if size {
        let mut census = HistorySizeCensus::for_family(snapshot.clone(), key.family()).unwrap();
        census
            .observe(
                Some(&candidate),
                HistoryLogicalBytes::new(4096).unwrap(),
                100,
            )
            .unwrap();
        let request = HistoryExpiryAdmissionRequest::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            HistoryExpiryOrigin::Manual {
                principal: PrincipalIdentity::generate(),
            },
            HistoryLifecycleScan::new(key, 1).unwrap(),
            snapshot.fence(),
        )
        .unwrap();
        let mut selection =
            HistoryExpirySizeSelection::new(request, census.finish_family().unwrap()).unwrap();
        selection.observe(candidate, 4096, 100, true).unwrap();
        let (_, result) = selection.finish().unwrap().into_parts();
        return HistoryExpiryIntent::new(result.into_manifest().unwrap());
    }
    let mut selection = HistoryExpirySelection::new(
        Uuid::new_v4(),
        HistoryExpiryOrigin::Manual {
            principal: PrincipalIdentity::generate(),
        },
        HistoryLifecycleScan::new(key, 1).unwrap(),
        &snapshot,
    )
    .unwrap();
    assert!(matches!(
        selection
            .consider(candidate, HistoryExpiryInputBytes::new(4096).unwrap(), &[])
            .unwrap(),
        HistoryExpirySelectionOutcome::Selected
    ));
    HistoryExpiryIntent::new(selection.finish().unwrap())
}

#[rstest::rstest]
fn public_operation_evidence_omits_stored_names_without_rewriting_retained_intent(
    #[values(false, true)] size: bool,
) {
    let intent = intent(size);
    let retained = serde_json::to_vec(&intent).unwrap();
    assert!(String::from_utf8_lossy(&retained).contains("protected_original_name_marker"));
    let expected_digest = Sha256::digest(serde_json::to_vec(intent.manifest()).unwrap())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let response = HistoryLifecycleOperationResponse::try_from(
        HistoryLifecycleOperationEvidence::ExpiryIntent(intent.clone()),
    )
    .unwrap();
    let encoded = serde_json::to_string(&response).unwrap();
    assert!(!encoded.contains("protected_original_name_marker"));
    assert!(!encoded.contains("object_authorization"));
    assert!(!encoded.contains("private_payload_marker"));
    assert_eq!(encoded.contains("size_authorizations"), size);
    assert!(encoded.contains(&intent.identity().to_string()));
    assert!(encoded.contains(&expected_digest));
    assert_eq!(serde_json::to_vec(&intent).unwrap(), retained);
}
