use super::*;
use crate::StorageRecordIdentity;
use hubuum_domain::VersionIdentity;
use rstest::rstest;
use uuid::Uuid;

fn at(seconds: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(seconds, 0).unwrap()
}
fn fixture(
    family: StorageIdentityHistoryKind,
) -> (HistoryLifecycleSnapshot, HistoryPolicyReference) {
    let key = HistoryPolicyKey::new(family, HistoryPolicyScope::Deployment).unwrap();
    let authority = HistoryLifecycleAuthority::empty()
        .apply(
            HistoryPolicyChange::new(
                key,
                HistoryPolicyRevision::new(0).unwrap(),
                HistoryPolicyBinding::Expire {
                    trigger: HistoryExpiryTrigger::LogicalSize {
                        upper_logical_bytes: HistoryLogicalBytes::new(100).unwrap(),
                        lower_logical_bytes: HistoryLogicalBytes::new(50).unwrap(),
                    },
                    minimum_age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
                },
                HistoryPolicyApplication::Prospective,
            )
            .unwrap(),
            PrincipalIdentity::generate(),
            at(100),
        )
        .unwrap();
    let reference = HistoryPolicyReference::from(&authority.policies().records()[0]);
    let control = HistoryLifecycleControl::decode(
        &serde_json::to_vec(&serde_json::json!({"authority":authority,"previews":[]})).unwrap(),
    )
    .unwrap();
    (
        HistoryLifecycleSnapshot::new(
            control,
            Uuid::new_v4(),
            HistoryDeletionRevision::new(0).unwrap(),
            at(200),
        )
        .unwrap(),
        reference,
    )
}
fn row(
    family: StorageIdentityHistoryKind,
    anchor: Option<HistoryRetentionAnchor>,
) -> HistoryExpiryCandidate {
    let attribution = HistoryScopeAttribution::new(
        ResourceIdentity::generate(),
        Some(ResourceIdentity::generate()),
    );
    let attrs = if matches!(
        family,
        StorageIdentityHistoryKind::ClassRelation | StorageIdentityHistoryKind::ObjectRelation
    ) {
        vec![attribution, attribution]
    } else {
        vec![attribution]
    };
    HistoryExpiryCandidate::new(
        family,
        StorageRecordIdentity::new(ResourceIdentity::generate(), VersionIdentity::generate()),
        anchor,
        attrs,
        true,
    )
    .unwrap()
}
fn measure(
    snapshot: &HistoryLifecycleSnapshot,
    policy: &HistoryPolicyReference,
    bytes: u64,
) -> HistorySizeAccounting {
    let mut census = HistorySizeCensus::new(snapshot.clone(), policy).unwrap();
    if bytes > 0 {
        census
            .observe(
                Some(&row(policy.key().family(), None)),
                HistoryLogicalBytes::new(bytes).unwrap(),
                10,
            )
            .unwrap();
    }
    census.finish().unwrap()
}
fn apply(
    snapshot: &HistoryLifecycleSnapshot,
    accounting: &mut HistorySizeAccounting,
) -> HistoryLifecycleSnapshot {
    let change = snapshot
        .control()
        .observe_size(accounting, PrincipalIdentity::generate())
        .unwrap();
    let decoded = HistoryLifecycleControl::decode(&change.next().encode().unwrap()).unwrap();
    HistoryLifecycleSnapshot::new(
        decoded,
        snapshot.fence().incarnation(),
        snapshot.fence().deletion_revision(),
        snapshot.observed_at(),
    )
    .unwrap()
}
#[rstest]
#[case(StorageIdentityHistoryKind::Collection)]
#[case(StorageIdentityHistoryKind::Class)]
#[case(StorageIdentityHistoryKind::Object)]
#[case(StorageIdentityHistoryKind::ClassRelation)]
#[case(StorageIdentityHistoryKind::ObjectRelation)]
#[case(StorageIdentityHistoryKind::ExportTemplate)]
#[case(StorageIdentityHistoryKind::RemoteTarget)]
#[case(StorageIdentityHistoryKind::ComputedDefinitions)]
#[case(StorageIdentityHistoryKind::ClassSchema)]
fn every_family_counts_retained_versions_once_despite_protection(
    #[case] family: StorageIdentityHistoryKind,
) {
    let (snapshot, policy) = fixture(family);
    let mut census = HistorySizeCensus::new(snapshot, &policy).unwrap();
    let (first, second) = if family == StorageIdentityHistoryKind::ClassSchema {
        (
            Some(HistoryRetentionAnchor::SchemaEvent(at(140))),
            HistoryRetentionAnchor::SchemaEvent(at(150)),
        )
    } else {
        (None, HistoryRetentionAnchor::Closed(at(150)))
    };
    census
        .observe(
            Some(&row(family, first)),
            HistoryLogicalBytes::new(60).unwrap(),
            10,
        )
        .unwrap();
    census
        .observe(
            Some(&row(family, Some(second))),
            HistoryLogicalBytes::new(60).unwrap(),
            10,
        )
        .unwrap();
    let accounting = census.finish().unwrap();
    assert_eq!(accounting.governed_rows, "2");
    assert_eq!(accounting.pressure.measured_bytes().value(), 120);
}
#[rstest]
#[case(100, false)]
#[case(101, true)]
fn upper_threshold_is_strict(#[case] bytes: u64, #[case] reclaiming: bool) {
    let (snapshot, policy) = fixture(StorageIdentityHistoryKind::Object);
    assert_eq!(
        measure(&snapshot, &policy, bytes).pressure.reclaiming(),
        reclaiming
    );
}

#[rstest]
#[case::after_policy("2", true)]
#[case::before_policy("1", false)]
fn recovered_size_episode_requires_its_policy_at_the_observation_revision(
    #[case] revision: &str,
    #[case] valid: bool,
) {
    let (snapshot, policy) = fixture(StorageIdentityHistoryKind::Object);
    let started = apply(&snapshot, &mut measure(&snapshot, &policy, 101));
    let mut material = serde_json::to_value(started.control()).unwrap();
    material["authority"]["size_transitions"][0]["revision"] = revision.into();
    assert_eq!(
        serde_json::from_value::<HistoryLifecycleControl>(material).is_ok(),
        valid
    );
}

#[rstest]
#[case::current(false)]
#[case::old_receipt(true)]
fn pressure_replay_uses_the_recorded_policy_id_convention(#[case] legacy: bool) {
    let (first, reference) = fixture(StorageIdentityHistoryKind::Object);
    let initial = &first.control().authority().policies().records()[0];
    let replacement = HistoryPolicyChange::new(
        reference.key(),
        reference.revision(),
        initial.proposal().binding().clone(),
        HistoryPolicyApplication::Prospective,
    )
    .unwrap();
    let change = first
        .control()
        .apply(
            HistoryPolicyApplyRequest::new(replacement, None),
            initial.principal(),
            first.fence(),
            at(150),
        )
        .unwrap();
    let snapshot = HistoryLifecycleSnapshot::new(
        change.next().clone(),
        first.fence().incarnation(),
        first.fence().deletion_revision(),
        at(200),
    )
    .unwrap();
    let replacement = &snapshot.control().authority().policies().records()[1];
    let reference = HistoryPolicyReference::from(replacement);
    let started = apply(&snapshot, &mut measure(&snapshot, &reference, 101));
    let ended = apply(&started, &mut measure(&started, &reference, 0));
    let mut material = serde_json::to_value(ended.control()).unwrap();
    if legacy {
        for receipt in material["authority"]["size_transitions"]
            .as_array_mut()
            .unwrap()
        {
            receipt.as_object_mut().unwrap().remove("source");
        }
    }
    let replay: HistoryLifecycleControl = serde_json::from_value(material.clone()).unwrap();
    assert_eq!(serde_json::to_value(&replay).unwrap(), material);
    let events = replay.audit_events_since(snapshot.control()).unwrap();
    assert_eq!(events.len(), 2);
    for event in events {
        let authority = &event.metadata()["lifecycle"]["authority"];
        assert_eq!(
            authority["policy_id"],
            serde_json::json!(if legacy {
                replacement.operation()
            } else {
                initial.operation()
            })
        );
        assert_eq!(
            authority["policy_revision"],
            serde_json::json!(reference.revision())
        );
        assert_eq!(event.actor_kind(), hubuum_events_core::ActorKind::User);
    }
}
#[rstest]
#[case(100, true)]
#[case(51, true)]
#[case(50, false)]
#[case(0, false)]
fn retained_hysteresis_survives_round_trip_until_lower_target(
    #[case] bytes: u64,
    #[case] reclaiming: bool,
) {
    let (snapshot, policy) = fixture(StorageIdentityHistoryKind::Object);
    let mut first = measure(&snapshot, &policy, 101);
    let started = apply(&snapshot, &mut first);
    let mut next = measure(&started, &policy, bytes);
    assert_eq!(next.pressure.reclaiming(), reclaiming);
    let ended = apply(&started, &mut next);
    assert_eq!(
        ended.control().authority().size_transitions().len(),
        if reclaiming { 1 } else { 2 }
    );
    assert_eq!(
        ended
            .control()
            .authority()
            .audit_events_since(started.control().authority())
            .unwrap()
            .len(),
        usize::from(!reclaiming)
    );
    let recovered =
        HistoryLifecycleRecovery::new(snapshot.clone(), ended.control().authority().clone())
            .unwrap();
    assert!(
        recovered
            .recovered()
            .authority()
            .covers(ended.control().authority())
    );
    assert_eq!(
        recovered
            .recovered()
            .authority()
            .size_pressure(&policy)
            .unwrap()
            .reclaiming(),
        reclaiming
    );
}
#[rstest]
#[case("duplicate")]
#[case("unresolved")]
#[case("metadata")]
#[case("rows")]
#[case("deadline")]
fn incomplete_or_ambiguous_census_fails_closed(#[case] mode: &str) {
    let (snapshot, policy) = fixture(StorageIdentityHistoryKind::Object);
    let mut census = HistorySizeCensus::new(snapshot, &policy).unwrap();
    let row = row(policy.key().family(), None);
    if mode == "deadline" {
        census.started = std::time::Instant::now() - std::time::Duration::from_secs(6);
    }
    if mode == "rows" {
        census.seen = (0..HistorySizeCensus::MAX_ROWS)
            .map(|_| {
                (
                    (ResourceIdentity::generate(), VersionIdentity::generate()),
                    HistoryLogicalBytes::new(0).unwrap(),
                )
            })
            .collect();
    }
    if mode == "duplicate" {
        census
            .observe(Some(&row), HistoryLogicalBytes::new(1).unwrap(), 1)
            .unwrap();
    }
    let result = census.observe(
        if mode == "unresolved" {
            None
        } else {
            Some(&row)
        },
        HistoryLogicalBytes::new(1).unwrap(),
        if mode == "metadata" {
            HistorySizeCensus::MAX_METADATA_BYTES
        } else {
            1
        },
    );
    assert!(result.is_err());
    assert!(census.finish().is_err());
}

#[rstest]
#[case(false)]
#[case(true)]
fn complete_row_limit_is_inclusive_and_the_next_row_invalidates_the_census(#[case] overflow: bool) {
    let (snapshot, policy) = fixture(StorageIdentityHistoryKind::Object);
    let mut census = HistorySizeCensus::new(snapshot, &policy).unwrap();
    for _ in 0..HistorySizeCensus::MAX_ROWS {
        census
            .observe(
                Some(&row(policy.key().family(), None)),
                HistoryLogicalBytes::new(1).unwrap(),
                1,
            )
            .unwrap();
    }
    if overflow {
        assert!(
            census
                .observe(
                    Some(&row(policy.key().family(), None)),
                    HistoryLogicalBytes::new(1).unwrap(),
                    1,
                )
                .is_err()
        );
        assert!(census.finish().is_err());
    } else {
        let accounting = census.finish().unwrap();
        assert_eq!(accounting.scanned_rows, "100000");
        assert_eq!(accounting.pressure.measured_bytes().value(), 100_000);
    }
}

#[rstest]
#[case(false)]
#[case(true)]
fn complete_metadata_limit_is_inclusive_and_the_next_byte_invalidates_the_census(
    #[case] overflow: bool,
) {
    let (snapshot, policy) = fixture(StorageIdentityHistoryKind::Object);
    let mut census = HistorySizeCensus::new(snapshot, &policy).unwrap();
    for _ in 0..64 {
        census
            .observe(
                Some(&row(policy.key().family(), None)),
                HistoryLogicalBytes::new(1).unwrap(),
                1024 * 1024,
            )
            .unwrap();
    }
    if overflow {
        assert!(
            census
                .observe(
                    Some(&row(policy.key().family(), None)),
                    HistoryLogicalBytes::new(1).unwrap(),
                    1,
                )
                .is_err()
        );
        assert!(census.finish().is_err());
    } else {
        assert_eq!(census.finish().unwrap().scanned_rows, "64");
    }
}
#[test]
fn retrospective_attribution_does_not_charge_old_versions_to_a_new_prospective_rule() {
    let (snapshot, policy) = fixture(StorageIdentityHistoryKind::Object);
    let mut census = HistorySizeCensus::new(snapshot, &policy).unwrap();
    census
        .observe(
            Some(&row(
                policy.key().family(),
                Some(HistoryRetentionAnchor::Closed(at(50))),
            )),
            HistoryLogicalBytes::new(1000).unwrap(),
            1,
        )
        .unwrap();
    assert_eq!(
        census.finish().unwrap().pressure.measured_bytes().value(),
        0
    );
}
#[test]
fn a_more_specific_override_is_excluded_from_the_broader_governed_set() {
    let (snapshot, policy) = fixture(StorageIdentityHistoryKind::Object);
    let row = row(policy.key().family(), None);
    let key = HistoryPolicyKey::new(
        policy.key().family(),
        HistoryPolicyScope::Class {
            identity: row.attributions()[0].class().unwrap(),
        },
    )
    .unwrap();
    let authority = snapshot
        .control()
        .authority()
        .apply(
            HistoryPolicyChange::new(
                key,
                HistoryPolicyRevision::new(0).unwrap(),
                HistoryPolicyBinding::Indefinite,
                HistoryPolicyApplication::Prospective,
            )
            .unwrap(),
            PrincipalIdentity::generate(),
            at(150),
        )
        .unwrap();
    let control = HistoryLifecycleControl::decode(
        &serde_json::to_vec(&serde_json::json!({"authority":authority,"previews":[]})).unwrap(),
    )
    .unwrap();
    let mut census = HistorySizeCensus::new(
        HistoryLifecycleSnapshot::new(
            control,
            snapshot.fence().incarnation(),
            snapshot.fence().deletion_revision(),
            at(200),
        )
        .unwrap(),
        &policy,
    )
    .unwrap();
    census
        .observe(Some(&row), HistoryLogicalBytes::new(1000).unwrap(), 1)
        .unwrap();
    assert_eq!(
        census.finish().unwrap().pressure.measured_bytes().value(),
        0
    );
}
#[test]
fn recovery_rejects_rewritten_pressure_receipts() {
    let (snapshot, policy) = fixture(StorageIdentityHistoryKind::Object);
    let mut first = measure(&snapshot, &policy, 101);
    let started = apply(&snapshot, &mut first);
    let mut fork = started.control().authority().clone();
    fork.size_transitions[0].operation = Uuid::new_v4();
    assert!(HistoryLifecycleRecovery::new(started, fork).is_err());
}
#[test]
fn capacity_reserves_space_for_the_active_episode_to_end() {
    let (mut snapshot, policy) = fixture(StorageIdentityHistoryKind::Object);
    for _ in 0..HistoryLifecycleAuthority::MAX_SIZE_TRANSITIONS / 2 {
        let mut high = measure(&snapshot, &policy, 101);
        snapshot = apply(&snapshot, &mut high);
        let mut low = measure(&snapshot, &policy, 50);
        snapshot = apply(&snapshot, &mut low);
    }
    let mut high = measure(&snapshot, &policy, 101);
    assert!(
        snapshot
            .control()
            .observe_size(&mut high, PrincipalIdentity::generate())
            .is_err()
    );
}
