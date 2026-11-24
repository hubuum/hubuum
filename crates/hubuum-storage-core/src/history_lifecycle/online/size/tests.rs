use super::*;
use rstest::rstest;

fn at(seconds: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(seconds, 0).unwrap()
}
fn fixture() -> (HistoryLifecycleSnapshot, Vec<HistoryExpiryCandidate>) {
    let key = HistoryPolicyKey::new(
        StorageIdentityHistoryKind::ClassSchema,
        HistoryPolicyScope::Deployment,
    )
    .unwrap();
    let original = HistoryLifecycleSnapshot::new(
        HistoryLifecycleControl::empty(),
        Uuid::new_v4(),
        HistoryDeletionRevision::new(0).unwrap(),
        at(1),
    )
    .unwrap();
    let proposal = HistoryPolicyChange::new(
        key,
        HistoryPolicyRevision::new(0).unwrap(),
        HistoryPolicyBinding::Expire {
            trigger: HistoryExpiryTrigger::LogicalSize {
                upper_logical_bytes: HistoryLogicalBytes::new(250).unwrap(),
                lower_logical_bytes: HistoryLogicalBytes::new(240).unwrap(),
            },
            minimum_age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
        },
        HistoryPolicyApplication::Prospective,
    )
    .unwrap();
    let change = original
        .control()
        .apply(
            HistoryPolicyApplyRequest::new(proposal, None),
            PrincipalIdentity::generate(),
            original.fence(),
            at(1),
        )
        .unwrap();
    let snapshot = HistoryLifecycleSnapshot::new(
        change.next().clone(),
        original.fence().incarnation(),
        original.fence().deletion_revision(),
        at(100),
    )
    .unwrap();
    let rows = [30, 20, 40]
        .into_iter()
        .map(|time| {
            let target = crate::StorageRecordIdentity::new(
                ResourceIdentity::generate(),
                hubuum_domain::VersionIdentity::generate(),
            );
            HistoryExpiryCandidate::new(
                key.family(),
                target,
                Some(HistoryRetentionAnchor::SchemaEvent(at(time))),
                vec![HistoryScopeAttribution::new(
                    ResourceIdentity::generate(),
                    Some(target.resource()),
                )],
                false,
            )
            .unwrap()
            .with_native_interval(at(time), None)
            .unwrap()
        })
        .collect();
    (snapshot, rows)
}
fn census(
    snapshot: &HistoryLifecycleSnapshot,
    rows: &[HistoryExpiryCandidate],
) -> HistoryFamilySizeAccounting {
    let family = StorageIdentityHistoryKind::ClassSchema;
    let mut census = HistorySizeCensus::for_family(snapshot.clone(), family).unwrap();
    for row in rows {
        census
            .observe(Some(row), HistoryLogicalBytes::new(100).unwrap(), 10)
            .unwrap();
    }
    census.finish_family().unwrap()
}
fn selection(
    snapshot: &HistoryLifecycleSnapshot,
    rows: &[HistoryExpiryCandidate],
) -> HistoryExpirySizeSelection {
    HistoryExpirySizeSelection::new(request(snapshot), census(snapshot, rows)).unwrap()
}
fn request(snapshot: &HistoryLifecycleSnapshot) -> HistoryExpiryAdmissionRequest {
    HistoryExpiryAdmissionRequest::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        HistoryExpiryOrigin::Manual {
            principal: PrincipalIdentity::generate(),
        },
        HistoryLifecycleScan::new(
            HistoryPolicyKey::new(
                StorageIdentityHistoryKind::ClassSchema,
                HistoryPolicyScope::Deployment,
            )
            .unwrap(),
            1000,
        )
        .unwrap(),
        snapshot.fence(),
    )
    .unwrap()
}
fn plan(
    snapshot: &HistoryLifecycleSnapshot,
    rows: &[HistoryExpiryCandidate],
) -> (HistoryLifecycleControlChange, HistoryExpiryAdmissionResult) {
    let mut selection = selection(snapshot, rows);
    for row in rows.iter().rev() {
        let visible = !snapshot
            .control()
            .authority()
            .expiry()
            .excludes(row.family(), row.target());
        selection.observe(row.clone(), 100, 10, visible).unwrap();
    }
    selection.finish().unwrap().into_parts()
}
fn admitted(
    snapshot: &HistoryLifecycleSnapshot,
    rows: &[HistoryExpiryCandidate],
) -> (HistoryLifecycleSnapshot, HistoryExpiryManifest) {
    let (pressure, result) = plan(snapshot, rows);
    let manifest = result.manifest().unwrap().clone();
    let change = pressure
        .next()
        .admit_expiry(manifest.clone(), manifest.fence())
        .unwrap();
    let snapshot = HistoryLifecycleSnapshot::new(
        change.next().clone(),
        snapshot.fence().incarnation(),
        snapshot.fence().deletion_revision(),
        at(100),
    )
    .unwrap();
    (snapshot, manifest)
}

#[test]
fn complete_selection_orders_original_clocks_and_stops_at_lower_target() {
    let (snapshot, rows) = fixture();
    let (pressure, result) = plan(&snapshot, &rows);
    let manifest = result.manifest().unwrap();
    assert_eq!(
        manifest
            .members()
            .iter()
            .map(|member| member.anchor().at())
            .collect::<Vec<_>>(),
        [at(20)]
    );
    assert_eq!(result.scanned(), 6);
    assert!(
        pressure.next().authority().size_transitions()[0]
            .pressure()
            .reclaiming()
    );
}

#[rstest]
#[case::scheduled(true, "scheduled")]
#[case::manual(false, "manual")]
fn worker_pressure_audit_preserves_initiation_after_recovery(
    #[case] scheduled: bool,
    #[case] trigger: &str,
) {
    let (snapshot, rows) = fixture();
    let policy = &snapshot.control().authority().policies().records()[0];
    let principal = policy.principal();
    let mut request = request(&snapshot);
    request.origin = if scheduled {
        HistoryExpiryOrigin::Scheduled {
            initiator: Some(principal),
            policy: Some(HistoryPolicyReference::from(policy)),
        }
    } else {
        HistoryExpiryOrigin::Manual { principal }
    };
    let selection = HistoryExpirySizeSelection::new(request, census(&snapshot, &rows)).unwrap();
    let recovered =
        HistoryLifecycleControl::decode(&selection.pressure_change.next().encode().unwrap())
            .unwrap();
    let events = recovered.audit_events_since(snapshot.control()).unwrap();
    assert_eq!(
        events[0].actor_kind(),
        hubuum_events_core::ActorKind::Worker
    );
    assert_eq!(
        events[0].metadata()["lifecycle"]["attribution"],
        serde_json::json!({
            "actor_principal_uuid": null, "initiator_principal_uuid": principal, "trigger": trigger
        })
    );
}

#[test]
fn pending_exact_membership_reserves_the_remaining_size_budget() {
    let (snapshot, rows) = fixture();
    let (snapshot, _) = admitted(&snapshot, &rows);
    let (_, result) = plan(&snapshot, &rows);
    assert!(result.manifest().is_none());
}

#[test]
fn absent_pending_copy_reserves_no_bytes_in_the_current_census() {
    let (snapshot, mut rows) = fixture();
    let (snapshot, manifest) = admitted(&snapshot, &rows);
    rows.retain(|row| row.target() != manifest.members()[0].target());
    rows.extend(fixture().1);
    // Five present 100-byte copies require three removals to reach 240 bytes.
    let (_, result) = plan(&snapshot, &rows);
    assert_eq!(result.manifest().unwrap().members().len(), 3);
}

#[rstest]
#[case::class_override(true)]
#[case::replacement_deployment_revision(false)]
fn pending_bytes_follow_the_current_policy(#[case] class_override: bool) {
    let (snapshot, mut rows) = fixture();
    let (snapshot, manifest) = admitted(&snapshot, &rows);
    let member = &manifest.members()[0];
    let policy = &snapshot.control().authority().policies().records()[0];
    let (scope, binding) = if class_override {
        (
            HistoryPolicyScope::Class {
                identity: member.target().resource(),
            },
            HistoryPolicyBinding::Indefinite,
        )
    } else {
        (
            HistoryPolicyScope::Deployment,
            policy.proposal().binding().clone(),
        )
    };
    let key = HistoryPolicyKey::new(member.family(), scope).unwrap();
    let change = HistoryPolicyChange::new(
        key,
        snapshot
            .control()
            .authority()
            .policies()
            .current_revision(key),
        binding,
        HistoryPolicyApplication::Retrospective {
            closed_through: at(100),
        },
    )
    .unwrap();
    let actor = PrincipalIdentity::generate();
    let fence = snapshot.fence();
    let preview = HistoryPolicyPreview::issue(
        actor,
        change.clone(),
        fence,
        at(101),
        HistoryPreviewImpact::new(HistoryPreviewCoverage::Bounded, 0, 0, vec![]).unwrap(),
    )
    .unwrap();
    let issued = snapshot
        .control()
        .issue_preview(preview.clone(), fence)
        .unwrap();
    let applied = issued
        .next()
        .apply(
            HistoryPolicyApplyRequest::new(change, Some(preview.reference().clone())),
            actor,
            fence,
            at(101),
        )
        .unwrap();
    let updated = HistoryLifecycleSnapshot::new(
        applied.next().clone(),
        fence.incarnation(),
        fence.deletion_revision(),
        at(102),
    )
    .unwrap();
    // Both cases have four copies. A class override removes one from the deployment
    // census. A replacement revision counts all four but reserves the pending copy.
    rows.push(fixture().1.pop().unwrap());
    let (_, result) = plan(&updated, &rows);
    assert_eq!(result.manifest().unwrap().members().len(), 1);
}

#[test]
fn intent_identity_cannot_reuse_a_pressure_operation() {
    let (snapshot, rows) = fixture();
    let (pressure, result) = plan(&snapshot, &rows);
    let mut manifest = result.manifest().unwrap().clone();
    manifest.identity = pressure.next().authority().size_transitions()[0].operation();
    assert!(
        pressure
            .next()
            .admit_expiry(manifest.clone(), manifest.fence())
            .is_err()
    );
}

#[rstest]
#[case::pressure_ended(false)]
#[case::new_hold(true)]
fn cleanup_preserves_original_size_cause_but_rechecks_holds(#[case] held: bool) {
    let (snapshot, rows) = fixture();
    let (snapshot, manifest) = admitted(&snapshot, &rows);
    let member = &manifest.members()[0];
    let row = rows
        .iter()
        .find(|row| row.target() == member.target())
        .unwrap();
    let reference =
        HistoryPolicyReference::from(&snapshot.control().authority().policies().records()[0]);
    let mut census = HistorySizeCensus::new(snapshot.clone(), &reference).unwrap();
    census
        .observe(Some(row), HistoryLogicalBytes::new(100).unwrap(), 10)
        .unwrap();
    let mut accounting = census.finish().unwrap();
    let change = snapshot
        .control()
        .observe_size(&mut accounting, PrincipalIdentity::generate())
        .unwrap();
    let mut control = change.next().clone();
    if held {
        let fence = HistoryReviewFence::new(
            snapshot.fence().incarnation(),
            control.authority().revision(),
            snapshot.fence().deletion_revision(),
        )
        .unwrap();
        let hold = HistoryRetentionHold::new(
            Uuid::new_v4(),
            manifest.key(),
            at(100),
            None,
            PrincipalIdentity::generate(),
        )
        .unwrap();
        control = control.create_hold(hold, fence).unwrap().next().clone();
    }
    let recovered = HistoryLifecycleControl::decode(&control.encode().unwrap()).unwrap();
    assert_eq!(
        member
            .validate_removal(row, recovered.authority(), at(101))
            .is_err(),
        held
    );
}

#[rstest]
#[case::missing("missing")]
#[case::foreign_episode("episode")]
#[case::over_reserved("reserved")]
fn recovery_rejects_lost_or_forged_size_causes(#[case] mode: &str) {
    let (snapshot, rows) = fixture();
    let (snapshot, _) = admitted(&snapshot, &rows);
    let mut value = serde_json::to_value(snapshot.control()).unwrap();
    let authorizations =
        &mut value["authority"]["expiry"][0]["manifest"]["members"][0]["size_authorizations"];
    match mode {
        "missing" => *authorizations = serde_json::json!([]),
        "episode" => authorizations[0]["episode"] = serde_json::json!(Uuid::new_v4()),
        "reserved" => authorizations[0]["reserved_bytes"] = serde_json::json!("300"),
        _ => unreachable!(),
    }
    assert!(serde_json::from_value::<HistoryLifecycleControl>(value).is_err());
}

#[test]
fn a_truncated_second_pass_cannot_issue_any_membership() {
    let (snapshot, rows) = fixture();
    let mut selection = selection(&snapshot, &rows);
    selection.observe(rows[0].clone(), 100, 10, true).unwrap();
    assert!(selection.finish().is_err());
}

#[test]
fn complete_accounting_cannot_authorize_another_family() {
    let (snapshot, rows) = fixture();
    let request = selection(&snapshot, &rows).request;
    let census = HistorySizeCensus::for_family(snapshot, StorageIdentityHistoryKind::Object)
        .unwrap()
        .finish_family()
        .unwrap();
    assert!(HistoryExpirySizeSelection::new(request, census).is_err());
}

#[test]
fn a_foreign_second_pass_row_invalidates_complete_selection() {
    let (snapshot, rows) = fixture();
    let mut selection = selection(&snapshot, &rows);
    let foreign = HistoryExpiryCandidate::new(
        StorageIdentityHistoryKind::Object,
        rows[0].target(),
        Some(HistoryRetentionAnchor::Closed(at(30))),
        rows[0].attributions().to_vec(),
        false,
    )
    .unwrap();
    assert!(selection.observe(foreign, 100, 10, true).is_err());
    assert!(selection.finish().is_err());
}

#[rstest]
#[case(32, true)]
#[case(33, false)]
fn two_passes_share_the_actual_metadata_budget(#[case] count: usize, #[case] complete: bool) {
    let (snapshot, examples) = fixture();
    let family = StorageIdentityHistoryKind::ClassSchema;
    let rows = (0..count)
        .map(|_| {
            HistoryExpiryCandidate::new(
                family,
                crate::StorageRecordIdentity::new(
                    ResourceIdentity::generate(),
                    hubuum_domain::VersionIdentity::generate(),
                ),
                Some(HistoryRetentionAnchor::SchemaEvent(at(20))),
                examples[0].attributions().to_vec(),
                false,
            )
            .unwrap()
            .with_native_interval(at(20), None)
            .unwrap()
        })
        .collect::<Vec<_>>();
    let mut census = HistorySizeCensus::for_family(snapshot.clone(), family).unwrap();
    for row in &rows {
        census
            .observe(
                Some(row),
                HistoryLogicalBytes::new(100).unwrap(),
                1024 * 1024,
            )
            .unwrap();
    }
    let mut selection =
        HistoryExpirySizeSelection::new(request(&snapshot), census.finish_family().unwrap())
            .unwrap();
    let result = rows
        .into_iter()
        .try_for_each(|row| selection.observe(row, 100, 1024 * 1024, true));
    assert_eq!(result.is_ok(), complete);
    assert_eq!(selection.finish().is_ok(), complete);
}

#[test]
fn multiple_sources_commit_original_pressure_receipts_one_at_a_time() {
    let (snapshot, rows) = fixture();
    let key = HistoryPolicyKey::new(
        StorageIdentityHistoryKind::ClassSchema,
        HistoryPolicyScope::Collection {
            identity: rows[0].attributions()[0].collection(),
        },
    )
    .unwrap();
    let proposal = HistoryPolicyChange::new(
        key,
        HistoryPolicyRevision::new(0).unwrap(),
        HistoryPolicyBinding::Expire {
            trigger: HistoryExpiryTrigger::LogicalSize {
                upper_logical_bytes: HistoryLogicalBytes::new(1).unwrap(),
                lower_logical_bytes: HistoryLogicalBytes::new(0).unwrap(),
            },
            minimum_age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
        },
        HistoryPolicyApplication::Prospective,
    )
    .unwrap();
    let applied = snapshot
        .control()
        .apply(
            HistoryPolicyApplyRequest::new(proposal, None),
            PrincipalIdentity::generate(),
            snapshot.fence(),
            at(2),
        )
        .unwrap();
    let snapshot = HistoryLifecycleSnapshot::new(
        applied.next().clone(),
        snapshot.fence().incarnation(),
        snapshot.fence().deletion_revision(),
        at(100),
    )
    .unwrap();
    let mut census = HistorySizeCensus::for_family(snapshot.clone(), key.family()).unwrap();
    for row in rows {
        census
            .observe(Some(&row), HistoryLogicalBytes::new(300).unwrap(), 10)
            .unwrap();
    }
    let (_, change) = census
        .finish_family()
        .unwrap()
        .retain(
            PrincipalIdentity::generate(),
            HistorySizeObservationSource::ManualRun,
        )
        .unwrap();
    let expected = change.next().clone();
    let mut current = snapshot.control().clone();
    let mut remaining = Some(change);
    let mut count = 0;
    while let Some(change) = remaining {
        let (first, rest) = change.split_first_size_transition().unwrap();
        assert_eq!(first.expected(), &current);
        assert_eq!(
            first.next().authority().revision().value(),
            current.authority().revision().value() + 1
        );
        current = first.next().clone();
        remaining = rest;
        count += 1;
    }
    assert_eq!(count, 2);
    assert_eq!(current, expected);
}
