use std::collections::BTreeMap;

use chrono::TimeZone;
use hubuum_domain::{HistoryRecordId, IdentityReference, ResourceId};
use rstest::rstest;
use serde_json::{Value, json};
use uuid::Uuid;

use super::*;
use crate::{
    StorageHistoryMetadata, StorageIdentityEvidence, StorageIdentitySubject, StorageObject,
    StorageRecordMetadata,
};

fn at(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0).single().unwrap()
}
fn derived() -> VersionIdentity {
    let mut bytes = *VersionIdentity::generate().as_bytes();
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    VersionIdentity::from_bytes(bytes).unwrap()
}
struct Fixture {
    scope: HistoryScopeAttribution,
}
impl Fixture {
    fn new() -> Self {
        Self {
            scope: HistoryScopeAttribution::new(
                ResourceIdentity::generate(),
                Some(ResourceIdentity::generate()),
            ),
        }
    }
    fn row(&self, id: i32, name: &str) -> StorageObjectHistoryRecord {
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
                            identity: self.scope.collection(),
                        },
                    },
                ),
                (
                    "/class_id".into(),
                    StorageCapturedIdentity::Resource {
                        family: StorageIdentityResourceKind::Class,
                        reference: IdentityReference::Known {
                            identity: self.scope.class().unwrap(),
                        },
                    },
                ),
            ]),
        )
        .unwrap();
        let record = StorageObject::new(
            StorageRecordMetadata::try_new(
                ResourceId::new(id).unwrap(),
                at(50),
                at(100),
                ResourceRevision::INITIAL,
            )
            .unwrap(),
            name,
            CollectionId::new(1).unwrap(),
            ClassId::new(1).unwrap(),
            json!({"private_payload_marker":true}),
            "private_description_marker",
        );
        let metadata = StorageHistoryMetadata::try_new(
            StorageHistoryOperation::Update,
            at(100),
            Some(at(150)),
            HistoryRecordId::new(i64::from(id)).unwrap(),
            ResourceRevision::INITIAL,
        )
        .unwrap()
        .with_logical_identity(evidence);
        StorageObjectHistoryRecord::try_new(record, metadata).unwrap()
    }
    fn snapshot(&self) -> HistoryLifecycleSnapshot {
        let empty = HistoryLifecycleSnapshot::new(
            HistoryLifecycleControl::empty(),
            Uuid::new_v4(),
            HistoryDeletionRevision::new(0).unwrap(),
            at(0),
        )
        .unwrap();
        let proposal = HistoryPolicyChange::new(
            key(),
            HistoryPolicyRevision::new(0).unwrap(),
            HistoryPolicyBinding::Expire {
                trigger: HistoryExpiryTrigger::Age {
                    age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
                    deleted_age_seconds: None,
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
        HistoryLifecycleSnapshot::new(
            applied.next().clone(),
            empty.fence().incarnation(),
            empty.fence().deletion_revision(),
            at(200),
        )
        .unwrap()
    }
}
fn key() -> HistoryPolicyKey {
    HistoryPolicyKey::new(
        StorageIdentityHistoryKind::Object,
        HistoryPolicyScope::Deployment,
    )
    .unwrap()
}
fn candidate(facts: &HistoryObjectAuthorizationFacts) -> HistoryExpiryCandidate {
    let anchor = match facts.kind() {
        HistoryObjectAuthorizationKind::ClosedValue => {
            HistoryRetentionAnchor::Closed(facts.read_span().valid_to().unwrap())
        }
        HistoryObjectAuthorizationKind::Tombstone => {
            HistoryRetentionAnchor::Deleted(facts.read_span().valid_from())
        }
    };
    HistoryExpiryCandidate::new(
        StorageIdentityHistoryKind::Object,
        facts.target(),
        Some(anchor),
        vec![facts.attribution()],
        false,
    )
    .unwrap()
    .with_read_span(facts.read_span().clone())
    .unwrap()
}

#[rstest]
#[case::closed_value(false, Some(at(150)))]
#[case::unbounded_tombstone(true, None)]
#[case::point_tombstone(true, Some(at(150)))]
fn capture_retains_only_original_policy_inputs_with_redacted_diagnostics(
    #[case] deleted: bool,
    #[case] until: Option<DateTime<Utc>>,
) {
    let fixture = Fixture::new();
    let row = fixture.row(1, "original_policy_name_marker");
    let row = if deleted {
        let (record, metadata) = row.into_parts();
        let evidence = metadata.logical_identity().unwrap().clone();
        let metadata = StorageHistoryMetadata::try_new(
            StorageHistoryOperation::Delete,
            at(150),
            until,
            HistoryRecordId::new(1).unwrap(),
            ResourceRevision::INITIAL,
        )
        .unwrap()
        .with_logical_identity(evidence);
        StorageObjectHistoryRecord::try_new(record, metadata).unwrap()
    } else {
        row
    };
    let facts = HistoryObjectAuthorizationFacts::capture(row).unwrap();
    assert_eq!(facts.read_span().valid_to(), until);
    assert_eq!(
        facts.kind(),
        if deleted {
            HistoryObjectAuthorizationKind::Tombstone
        } else {
            HistoryObjectAuthorizationKind::ClosedValue
        }
    );
    assert_eq!(facts.attribution(), fixture.scope);
    assert_eq!(facts.object_id(), ObjectId::new(1).unwrap());
    assert_eq!(facts.name(), "original_policy_name_marker");
    let encoded = serde_json::to_vec(&facts).unwrap();
    assert_eq!(encoded.len(), facts.encoded_bytes().value() as usize);
    let text = String::from_utf8(encoded).unwrap();
    assert!(!text.contains("private_payload_marker"));
    assert!(!text.contains("private_description_marker"));
    assert!(!format!("{facts:?}").contains("original_policy_name_marker"));
    assert!(!format!("{facts:?}").contains(&facts.target().resource().to_string()));
    assert_eq!(
        serde_json::from_str::<HistoryObjectAuthorizationFacts>(&text).unwrap(),
        facts
    );
}

#[rstest]
#[case::missing_class("missing_class")]
#[case::different_state("state")]
#[case::self_ancestry("self")]
#[case::duplicate_ancestry("duplicate")]
#[case::unknown_derived("unknown")]
#[case::invented_permission("permission")]
#[case::oversized_name("oversized")]
#[case::missing_name("name")]
#[case::missing_ancestry("ancestry")]
fn stored_facts_reject_missing_or_contradictory_original_evidence(#[case] mode: &str) {
    let facts =
        HistoryObjectAuthorizationFacts::capture(Fixture::new().row(1, "original")).unwrap();
    let mut value = serde_json::to_value(&facts).unwrap();
    match mode {
        "missing_class" => value["attribution"]["class"] = Value::Null,
        "state" => value["kind"] = json!("tombstone"),
        "self" => value["source_versions"] = json!([facts.target().version()]),
        "duplicate" => {
            let version = VersionIdentity::generate();
            value["source_versions"] = json!([version, version]);
        }
        "unknown" => value["target"]["version"] = json!(derived()),
        "permission" => value["authorized"] = json!(true),
        "oversized" => value["name"] = json!("x".repeat(HistoryAuthorizationBytes::MAX_BYTES)),
        "name" => {
            value.as_object_mut().unwrap().remove("name");
        }
        "ancestry" => {
            value.as_object_mut().unwrap().remove("source_versions");
        }
        _ => unreachable!(),
    }
    assert!(serde_json::from_value::<HistoryObjectAuthorizationFacts>(value).is_err());
}

#[rstest]
#[case::open("open")]
#[case::missing_class("missing")]
#[case::wrong_class_family("family")]
#[case::unknown_restore("unknown")]
#[case::bounded_tombstone("bounded_tombstone")]
#[case::empty_interval("empty")]
fn capture_does_not_supply_missing_historical_facts(#[case] mode: &str) {
    let (record, metadata) = Fixture::new().row(1, "original").into_parts();
    let original = metadata.logical_identity().unwrap().clone();
    let mut references = original.references().clone();
    let subject = match mode {
        "missing" => {
            references.remove("/class_id");
            original.subject()
        }
        "family" => {
            references.insert(
                "/class_id".into(),
                StorageCapturedIdentity::Resource {
                    family: StorageIdentityResourceKind::Collection,
                    reference: IdentityReference::Known {
                        identity: ResourceIdentity::generate(),
                    },
                },
            );
            original.subject()
        }
        "unknown" => Some(StorageIdentitySubject::Resource {
            identity: IdentityReference::Known {
                identity: original
                    .subject()
                    .unwrap()
                    .record_identity()
                    .unwrap()
                    .resource(),
            },
            version: derived(),
        }),
        "open" | "bounded_tombstone" | "empty" => original.subject(),
        _ => unreachable!(),
    };
    let evidence = StorageIdentityEvidence::try_new(subject, references).unwrap();
    let metadata = StorageHistoryMetadata::try_new(
        if mode == "bounded_tombstone" {
            StorageHistoryOperation::Delete
        } else {
            StorageHistoryOperation::Update
        },
        at(100),
        (mode != "open").then_some(if mode == "empty" { at(100) } else { at(150) }),
        HistoryRecordId::new(1).unwrap(),
        ResourceRevision::INITIAL,
    )
    .unwrap()
    .with_logical_identity(evidence);
    assert!(
        HistoryObjectAuthorizationFacts::capture(
            StorageObjectHistoryRecord::try_new(record, metadata).unwrap()
        )
        .is_err()
    );
}

#[test]
fn known_restored_facts_preserve_original_sources_and_name() {
    let (record, metadata) = Fixture::new()
        .row(1, "original_before_restore")
        .into_parts();
    let source = metadata
        .logical_identity()
        .unwrap()
        .subject()
        .unwrap()
        .record_identity()
        .unwrap();
    let evidence = metadata
        .logical_identity()
        .unwrap()
        .clone()
        .at_restore_baseline(derived())
        .unwrap();
    let facts = HistoryObjectAuthorizationFacts::capture(
        StorageObjectHistoryRecord::try_new(record, metadata.with_logical_identity(evidence))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(facts.target().resource(), source.resource());
    assert_ne!(facts.target().version(), source.version());
    assert!(facts.source_versions().contains(&source.version()));
    assert_eq!(facts.name(), "original_before_restore");
    assert_eq!(
        serde_json::from_slice::<HistoryObjectAuthorizationFacts>(
            &serde_json::to_vec(&facts).unwrap()
        )
        .unwrap(),
        facts
    );
}

#[test]
fn captured_facts_cannot_retarget_or_replace_an_original_selection() {
    let facts =
        HistoryObjectAuthorizationFacts::capture(Fixture::new().row(1, "original_name")).unwrap();
    let selected = candidate(&facts)
        .with_object_authorization(facts.clone())
        .unwrap();
    let mut replacement = serde_json::to_value(&facts).unwrap();
    replacement["name"] = json!("current_name");
    let replacement =
        serde_json::from_value::<HistoryObjectAuthorizationFacts>(replacement).unwrap();
    assert!(
        selected
            .clone()
            .with_object_authorization(replacement)
            .is_err()
    );
    assert!(
        selected
            .with_read_span(HistoryExpiryReadSpan::closed(at(99), at(150)).unwrap())
            .is_err()
    );
    let unrelated =
        HistoryObjectAuthorizationFacts::capture(Fixture::new().row(1, "original_name")).unwrap();
    assert!(
        candidate(&facts)
            .with_object_authorization(unrelated)
            .is_err()
    );
}

fn filled_manifest(
    fixture: &Fixture,
    snapshot: &HistoryLifecycleSnapshot,
    offset: i32,
) -> HistoryExpiryManifest {
    let mut selection = HistoryExpirySelection::new(
        Uuid::new_v4(),
        HistoryExpiryOrigin::Manual {
            principal: PrincipalIdentity::generate(),
        },
        HistoryLifecycleScan::new(key(), 1000).unwrap(),
        snapshot,
    )
    .unwrap();
    let name = "n".repeat(32 * 1024);
    for index in 1..=1000 {
        let facts =
            HistoryObjectAuthorizationFacts::capture(fixture.row(offset + index, &name)).unwrap();
        let selected = candidate(&facts).with_object_authorization(facts).unwrap();
        // Pure builder budget input; this is not a native row measurement.
        match selection
            .consider(
                selected,
                HistoryExpiryInputBytes::new(64 * 1024).unwrap(),
                &[],
            )
            .unwrap()
        {
            HistoryExpirySelectionOutcome::Selected => {}
            HistoryExpirySelectionOutcome::BudgetReached => break,
            HistoryExpirySelectionOutcome::Blocked(_) => panic!("Fixture must be eligible"),
        }
    }
    let manifest = selection.finish().unwrap();
    assert!(
        manifest.members().len() < 256,
        "Retained bytes must bind before the native-input ceiling"
    );
    assert_eq!(
        manifest.input_bytes() as usize,
        manifest.members().len() * 64 * 1024
    );
    assert!(serde_json::to_vec(&manifest).unwrap().len() <= HistoryExpiryManifest::MAX_BYTES);
    manifest
}

#[test]
fn retained_facts_count_toward_manifest_journal_and_completion_capacity() {
    let fixture = Fixture::new();
    let snapshot = fixture.snapshot();
    let manifests = (0..5)
        .map(|index| filled_manifest(&fixture, &snapshot, index * 1000))
        .collect::<Vec<_>>();
    let intents = manifests
        .iter()
        .cloned()
        .map(HistoryExpiryIntent::new)
        .collect::<Vec<_>>();
    assert!(HistoryExpiryJournal::new(intents).is_err());
    let legacy = manifests
        .into_iter()
        .map(|manifest| {
            let mut value = serde_json::to_value(manifest).unwrap();
            for member in value["members"].as_array_mut().unwrap() {
                member
                    .as_object_mut()
                    .unwrap()
                    .remove("object_authorization");
            }
            HistoryExpiryIntent::new(
                serde_json::from_value::<HistoryExpiryManifest>(value).unwrap(),
            )
        })
        .collect();
    assert!(HistoryExpiryJournal::new(legacy).is_ok());
}

#[test]
fn recovery_cannot_strip_or_backfill_retained_authorization() {
    let fixture = Fixture::new();
    let snapshot = fixture.snapshot();
    let manifest = filled_manifest(&fixture, &snapshot, 0);
    let mut value = serde_json::to_value(&manifest).unwrap();
    for member in value["members"].as_array_mut().unwrap() {
        member
            .as_object_mut()
            .unwrap()
            .remove("object_authorization");
    }
    let legacy = HistoryExpiryIntent::new(serde_json::from_value(value).unwrap());
    let retained = HistoryExpiryIntent::new(manifest);
    assert!(retained.merge(&legacy).is_err());
    assert!(legacy.merge(&retained).is_err());
}

fn population_gap_fixture() -> (
    HistoryExpiryIntent,
    StorageRecordIdentity,
    crate::StorageClassObjectsAsOfQuery,
    HistoryReadEpoch,
) {
    let fixture = Fixture::new();
    let snapshot = fixture.snapshot();
    let facts =
        HistoryObjectAuthorizationFacts::capture(fixture.row(1, "original_gap_name")).unwrap();
    let target = facts.target();
    let mut selection = HistoryExpirySelection::new(
        Uuid::new_v4(),
        HistoryExpiryOrigin::Manual {
            principal: PrincipalIdentity::generate(),
        },
        HistoryLifecycleScan::new(key(), 1).unwrap(),
        &snapshot,
    )
    .unwrap();
    assert!(matches!(
        selection
            .consider(
                candidate(&facts).with_object_authorization(facts).unwrap(),
                HistoryExpiryInputBytes::new(1024).unwrap(),
                &[],
            )
            .unwrap(),
        HistoryExpirySelectionOutcome::Selected
    ));
    let query = crate::StorageClassObjectsAsOfQuery::try_new(
        ClassId::new(1).unwrap(),
        at(125),
        hubuum_query::QueryOptions::new(vec![], vec![], Some(2), None, false).unwrap(),
    )
    .unwrap()
    .for_resource(fixture.scope.class().unwrap());
    let runtime = crate::RuntimeRegistration::generate_history_aware();
    let epoch = HistoryReadEpoch::new(
        crate::StorageExecutionPermit::new(
            Some(runtime.owner()),
            crate::ApplicationFence::new(snapshot.fence().incarnation(), 0).unwrap(),
        ),
        snapshot.fence(),
        HistoryPublicationGeneration::new(0).unwrap(),
    )
    .unwrap();
    (
        HistoryExpiryIntent::new(selection.finish().unwrap()),
        target,
        query,
        epoch,
    )
}

#[test]
fn population_gap_preserves_epoch_and_original_facts_without_authorizing_absence() {
    let (intent, target, query, epoch) = population_gap_fixture();
    let gap =
        HistoryObjectPopulationGap::from_retained_member(&intent, target, &query, epoch.clone())
            .unwrap();
    assert!(gap.matches_query(&query));
    assert_eq!(gap.epoch(), &epoch);
    assert_eq!(gap.facts().name(), "original_gap_name");
    assert_eq!(gap.intent(), intent.identity());
    let error = crate::StorageError::history_population_gap(gap);
    assert_eq!(error.kind(), crate::StorageErrorKind::Unavailable);
    assert!(!format!("{error:?}").contains("original_gap_name"));
    assert!(error.history_population_gap_evidence().is_some());
}

#[rstest]
#[case::missing_original("missing_original")]
#[case::different_original("different_original")]
#[case::different_integer("different_integer")]
#[case::before_interval("before_interval")]
#[case::closed_end("closed_end")]
#[case::hidden_collection("hidden_collection")]
fn population_gap_rejects_changed_original_selection(#[case] change: &str) {
    let (intent, target, original, epoch) = population_gap_fixture();
    let query = crate::StorageClassObjectsAsOfQuery::try_new(
        ClassId::new(if change == "different_integer" { 2 } else { 1 }).unwrap(),
        match change {
            "before_interval" => at(99),
            "closed_end" => at(150),
            _ => at(125),
        },
        hubuum_query::QueryOptions::new(vec![], vec![], Some(2), None, false).unwrap(),
    )
    .unwrap();
    let query = match change {
        "missing_original" => query,
        "different_original" => query.for_resource(ResourceIdentity::generate()),
        _ => query.for_resource(original.expected_resource().unwrap()),
    };
    let query = if change == "hidden_collection" {
        query.visibility(crate::StorageHistoryCollectionScope::Visible(vec![]), false)
    } else {
        query
    };
    assert!(
        HistoryObjectPopulationGap::from_retained_member(&intent, target, &query, epoch).is_err()
    );
}

#[test]
fn population_gap_does_not_backfill_missing_retained_facts() {
    let (intent, target, query, epoch) = population_gap_fixture();
    let mut stored = serde_json::to_value(intent).unwrap();
    stored["manifest"]["members"][0]
        .as_object_mut()
        .unwrap()
        .remove("object_authorization");
    let old: HistoryExpiryIntent = serde_json::from_value(stored).unwrap();
    assert!(
        HistoryObjectPopulationGap::from_retained_member(&old, target, &query, epoch.clone())
            .is_err()
    );
    let journal = HistoryExpiryJournal::new(vec![old]).unwrap();
    assert!(HistoryObjectPopulationGap::for_population(&journal, &query, epoch).is_err());
}

fn filled_small_population_manifest(
    fixture: &Fixture,
    snapshot: &HistoryLifecycleSnapshot,
    offset: i32,
) -> HistoryExpiryManifest {
    let mut selection = HistoryExpirySelection::new(
        Uuid::new_v4(),
        HistoryExpiryOrigin::Manual {
            principal: PrincipalIdentity::generate(),
        },
        HistoryLifecycleScan::new(key(), 1000).unwrap(),
        snapshot,
    )
    .unwrap();
    for index in 1..=1000 {
        let facts =
            HistoryObjectAuthorizationFacts::capture(fixture.row(offset + index, "n")).unwrap();
        // Logical budget fixture only; no native full-row measurement claim.
        match selection
            .consider(
                candidate(&facts).with_object_authorization(facts).unwrap(),
                HistoryExpiryInputBytes::new(1).unwrap(),
                &[],
            )
            .unwrap()
        {
            HistoryExpirySelectionOutcome::Selected => {}
            HistoryExpirySelectionOutcome::BudgetReached => break,
            HistoryExpirySelectionOutcome::Blocked(_) => panic!("Fixture must be eligible"),
        }
    }
    selection.finish().unwrap()
}

#[rstest]
#[case::fact_bytes(true)]
#[case::candidate_rows(false)]
fn population_gap_refuses_coverage_beyond_retained_read_bounds(#[case] large_names: bool) {
    let fixture = Fixture::new();
    let snapshot = fixture.snapshot();
    let manifests = (0..2)
        .map(|index| {
            if large_names {
                filled_manifest(&fixture, &snapshot, index * 1000)
            } else {
                filled_small_population_manifest(&fixture, &snapshot, index * 1000)
            }
        })
        .map(HistoryExpiryIntent::new)
        .collect::<Vec<_>>();
    let query = crate::StorageClassObjectsAsOfQuery::try_new(
        ClassId::new(1).unwrap(),
        at(125),
        hubuum_query::QueryOptions::new(vec![], vec![], Some(2), None, false).unwrap(),
    )
    .unwrap()
    .for_resource(fixture.scope.class().unwrap());
    let registration = crate::RuntimeRegistration::generate_history_aware();
    let epoch = HistoryReadEpoch::new(
        crate::StorageExecutionPermit::new(
            Some(registration.owner()),
            crate::ApplicationFence::new(snapshot.fence().incarnation(), 0).unwrap(),
        ),
        snapshot.fence(),
        HistoryPublicationGeneration::new(0).unwrap(),
    )
    .unwrap();
    let within = HistoryExpiryJournal::new(vec![manifests[0].clone()]).unwrap();
    assert_eq!(
        HistoryObjectPopulationGap::for_population(&within, &query, epoch.clone())
            .unwrap()
            .len(),
        manifests[0].manifest().members().len()
    );
    let exceeded = HistoryExpiryJournal::new(manifests).unwrap();
    assert!(HistoryObjectPopulationGap::for_population(&exceeded, &query, epoch).is_err());
}

#[rstest]
#[case::valid("valid", true)]
#[case::object_uuid("object", false)]
#[case::collection_uuid("collection", false)]
#[case::class_uuid("class", false)]
#[case::object_collection("object_collection", false)]
#[case::object_class("object_class", false)]
#[case::class_collection("class_collection", false)]
#[case::coordinates("coordinates", false)]
#[case::unknown_provenance("unknown", false)]
fn remapping_requires_original_uuid_tuple_and_relationships(
    #[case] change: &str,
    #[case] accepted: bool,
) {
    let fixture = Fixture::new();
    let facts = HistoryObjectAuthorizationFacts::capture(fixture.row(7, "protected_original_name"))
        .unwrap();
    let collection = facts.attribution().collection();
    let class = facts.attribution().class().unwrap();
    let reference = |kind, identity| StorageCapturedIdentity::Resource {
        family: kind,
        reference: IdentityReference::Known { identity },
    };
    let evidence = |identity, references| {
        StorageIdentityEvidence::try_new(
            Some(StorageIdentitySubject::Resource {
                identity: IdentityReference::Known { identity },
                version: VersionIdentity::generate(),
            }),
            references,
        )
        .unwrap()
    };
    let mut object_evidence = evidence(
        if change == "object" {
            ResourceIdentity::generate()
        } else {
            facts.target().resource()
        },
        BTreeMap::from([
            (
                "/collection_id".into(),
                reference(
                    StorageIdentityResourceKind::Collection,
                    if change == "object_collection" {
                        ResourceIdentity::generate()
                    } else {
                        collection
                    },
                ),
            ),
            (
                "/class_id".into(),
                reference(
                    StorageIdentityResourceKind::Class,
                    if change == "object_class" {
                        ResourceIdentity::generate()
                    } else {
                        class
                    },
                ),
            ),
        ]),
    );
    if change == "unknown" {
        let mut value = serde_json::to_value(object_evidence).unwrap();
        value["subject"]["version"] = json!(derived());
        object_evidence = serde_json::from_value(value).unwrap();
    }
    let epoch = population_gap_fixture().3;
    let result = HistoryObjectAuthorizationRemapping::new(
        &facts,
        epoch.clone(),
        HistoryAuthorizationResource::object(
            ObjectId::new(70).unwrap(),
            CollectionId::new(80).unwrap(),
            ClassId::new(if change == "coordinates" { 91 } else { 90 }).unwrap(),
            object_evidence,
        ),
        HistoryAuthorizationResource::collection(
            CollectionId::new(80).unwrap(),
            evidence(
                if change == "collection" {
                    ResourceIdentity::generate()
                } else {
                    collection
                },
                BTreeMap::new(),
            ),
        ),
        HistoryAuthorizationResource::class(
            ClassId::new(90).unwrap(),
            CollectionId::new(80).unwrap(),
            evidence(
                if change == "class" {
                    ResourceIdentity::generate()
                } else {
                    class
                },
                BTreeMap::from([(
                    "/collection_id".into(),
                    reference(
                        StorageIdentityResourceKind::Collection,
                        if change == "class_collection" {
                            ResourceIdentity::generate()
                        } else {
                            collection
                        },
                    ),
                )]),
            ),
        ),
    );
    assert_eq!(result.is_ok(), accepted);
    if let Ok(proof) = result {
        assert_eq!(proof.object_id(), ObjectId::new(70).unwrap());
        assert!(proof.matches(&facts, &epoch));
        let mut changed = serde_json::to_value(&facts).unwrap();
        changed["name"] = json!("different_original_name");
        let changed = serde_json::from_value(changed).unwrap();
        assert!(!proof.matches(&changed, &epoch));
        assert!(!format!("{proof:?}").contains("protected_original_name"));
    }
}
