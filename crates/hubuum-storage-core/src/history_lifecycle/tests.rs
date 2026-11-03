use super::*;
use crate::StorageRecordIdentity;
use hubuum_domain::VersionIdentity;
use rstest::rstest;
use uuid::Uuid;

fn at(seconds: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(seconds, 0).unwrap()
}
fn resource() -> ResourceIdentity {
    ResourceIdentity::generate()
}
fn age(seconds: u64) -> HistoryPolicyBinding {
    HistoryPolicyBinding::Expire {
        trigger: HistoryExpiryTrigger::Age {
            age_seconds: HistoryRetentionSeconds::new(seconds).unwrap(),
            deleted_age_seconds: None,
        },
        minimum_age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
    }
}
fn apply(
    catalog: &HistoryPolicyCatalog,
    scope: HistoryPolicyScope,
    binding: HistoryPolicyBinding,
    application: HistoryPolicyApplication,
    now: i64,
) -> HistoryPolicyCatalog {
    let key = HistoryPolicyKey::new(StorageIdentityHistoryKind::Object, scope).unwrap();
    let proposal =
        HistoryPolicyChange::new(key, catalog.current_revision(key), binding, application).unwrap();
    catalog
        .append(proposal, PrincipalIdentity::generate(), at(now))
        .unwrap()
}
fn candidate(
    attribution: Vec<HistoryScopeAttribution>,
    anchor: Option<HistoryRetentionAnchor>,
) -> HistoryExpiryCandidate {
    HistoryExpiryCandidate::new(
        StorageIdentityHistoryKind::Object,
        StorageRecordIdentity::new(resource(), VersionIdentity::generate()),
        anchor,
        attribution,
        false,
    )
    .unwrap()
}
#[rstest]
#[case(50, 100, false)]
#[case(100, 100, false)]
#[case(101, 100, true)]
fn prospective_rules_apply_only_to_later_anchors(
    #[case] closure: i64,
    #[case] applied: i64,
    #[case] eligible: bool,
) {
    let policies = apply(
        &HistoryPolicyCatalog::default(),
        HistoryPolicyScope::Deployment,
        age(0),
        HistoryPolicyApplication::Prospective,
        applied,
    );
    let row = candidate(
        vec![HistoryScopeAttribution::new(resource(), None)],
        Some(HistoryRetentionAnchor::Closed(at(closure))),
    );
    assert_eq!(
        row.inspect(&policies, at(200), false, &[])
            .unwrap()
            .eligible(),
        eligible
    );
}
#[test]
fn retrospective_policy_covers_a_late_committing_old_closure() {
    let policies = apply(
        &HistoryPolicyCatalog::default(),
        HistoryPolicyScope::Deployment,
        age(10),
        HistoryPolicyApplication::Retrospective {
            closed_through: at(80),
        },
        100,
    );
    let row = candidate(
        vec![HistoryScopeAttribution::new(resource(), None)],
        Some(HistoryRetentionAnchor::Closed(at(50))),
    );
    assert_eq!(
        row.inspect(&policies, at(500), false, &[])
            .unwrap()
            .endpoints()[0]
            .deadline(),
        Some(at(60))
    );
}
#[test]
fn prospective_inherit_preserves_existing_override() {
    let collection = resource();
    let attribution = HistoryScopeAttribution::new(collection, None);
    let policies = apply(
        &HistoryPolicyCatalog::default(),
        HistoryPolicyScope::Deployment,
        age(0),
        HistoryPolicyApplication::Retrospective {
            closed_through: at(1),
        },
        1,
    );
    let policies = apply(
        &policies,
        HistoryPolicyScope::Collection {
            identity: collection,
        },
        HistoryPolicyBinding::Indefinite,
        HistoryPolicyApplication::Prospective,
        2,
    );
    let policies = apply(
        &policies,
        HistoryPolicyScope::Collection {
            identity: collection,
        },
        HistoryPolicyBinding::Inherit,
        HistoryPolicyApplication::Prospective,
        100,
    );
    let row = candidate(
        vec![attribution],
        Some(HistoryRetentionAnchor::Closed(at(50))),
    );
    assert_eq!(
        row.inspect(&policies, at(200), false, &[])
            .unwrap()
            .blocker(),
        Some(HistoryExpiryBlocker::IndefiniteRetention)
    );
}
#[test]
fn retrospective_cutoff_leaves_unreviewed_gap_unchanged() {
    let policies = apply(
        &HistoryPolicyCatalog::default(),
        HistoryPolicyScope::Deployment,
        age(0),
        HistoryPolicyApplication::Retrospective {
            closed_through: at(50),
        },
        100,
    );
    let row = candidate(
        vec![HistoryScopeAttribution::new(resource(), None)],
        Some(HistoryRetentionAnchor::Closed(at(75))),
    );
    assert_eq!(
        row.inspect(&policies, at(200), false, &[])
            .unwrap()
            .blocker(),
        Some(HistoryExpiryBlocker::ExpiryDisabled)
    );
}
#[test]
fn both_relation_endpoints_must_permit_expiry() {
    let allowed = resource();
    let protected = resource();
    let key = HistoryPolicyKey::new(
        StorageIdentityHistoryKind::ObjectRelation,
        HistoryPolicyScope::Deployment,
    )
    .unwrap();
    let proposal = HistoryPolicyChange::new(
        key,
        HistoryPolicyRevision::new(0).unwrap(),
        age(0),
        HistoryPolicyApplication::Prospective,
    )
    .unwrap();
    let policies = HistoryPolicyCatalog::default()
        .append(proposal, PrincipalIdentity::generate(), at(1))
        .unwrap();
    let key = HistoryPolicyKey::new(
        StorageIdentityHistoryKind::ObjectRelation,
        HistoryPolicyScope::Collection {
            identity: protected,
        },
    )
    .unwrap();
    let proposal = HistoryPolicyChange::new(
        key,
        HistoryPolicyRevision::new(0).unwrap(),
        HistoryPolicyBinding::Indefinite,
        HistoryPolicyApplication::Prospective,
    )
    .unwrap();
    let policies = policies
        .append(proposal, PrincipalIdentity::generate(), at(2))
        .unwrap();
    let row = HistoryExpiryCandidate::new(
        StorageIdentityHistoryKind::ObjectRelation,
        StorageRecordIdentity::new(resource(), VersionIdentity::generate()),
        Some(HistoryRetentionAnchor::Closed(at(10))),
        vec![
            HistoryScopeAttribution::new(allowed, None),
            HistoryScopeAttribution::new(protected, None),
        ],
        false,
    )
    .unwrap();
    assert_eq!(
        row.inspect(&policies, at(100), false, &[])
            .unwrap()
            .blocker(),
        Some(HistoryExpiryBlocker::IndefiniteRetention)
    );
}
#[rstest]
#[case(None, false, HistoryExpiryBlocker::OpenVersion)]
#[case(
    Some(HistoryRetentionAnchor::Closed(at(10))),
    true,
    HistoryExpiryBlocker::Hold
)]
fn protection_blocks_otherwise_due_history(
    #[case] anchor: Option<HistoryRetentionAnchor>,
    #[case] held: bool,
    #[case] blocker: HistoryExpiryBlocker,
) {
    let policies = apply(
        &HistoryPolicyCatalog::default(),
        HistoryPolicyScope::Deployment,
        age(0),
        HistoryPolicyApplication::Prospective,
        1,
    );
    let row = candidate(vec![HistoryScopeAttribution::new(resource(), None)], anchor);
    assert_eq!(
        row.inspect(&policies, at(100), held, &[])
            .unwrap()
            .blocker(),
        Some(blocker)
    );
}
#[test]
fn size_hysteresis_continues_until_lower_target() {
    let binding = HistoryPolicyBinding::Expire {
        trigger: HistoryExpiryTrigger::LogicalSize {
            upper_logical_bytes: HistoryLogicalBytes::new(100).unwrap(),
            lower_logical_bytes: HistoryLogicalBytes::new(50).unwrap(),
        },
        minimum_age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
    };
    let policies = apply(
        &HistoryPolicyCatalog::default(),
        HistoryPolicyScope::Deployment,
        binding,
        HistoryPolicyApplication::Prospective,
        1,
    );
    let policy = policies.resolve(
        StorageIdentityHistoryKind::Object,
        HistoryScopeAttribution::new(resource(), None),
        HistoryRetentionAnchor::Closed(at(10)),
    );
    let first = HistorySizePressure::measure(&policy, HistoryLogicalBytes::new(101).unwrap(), None)
        .unwrap();
    let second =
        HistorySizePressure::measure(&policy, HistoryLogicalBytes::new(75).unwrap(), Some(&first))
            .unwrap();
    let stopped = HistorySizePressure::measure(
        &policy,
        HistoryLogicalBytes::new(50).unwrap(),
        Some(&second),
    )
    .unwrap();
    assert!(!stopped.reclaiming());
}
#[rstest]
#[case("01")]
#[case("-1")]
#[case("18446744073709551615")]
fn revision_rejects_noncanonical_or_out_of_range_strings(#[case] value: &str) {
    assert!(HistoryPolicyRevision::try_from(value.to_owned()).is_err());
}
#[test]
fn size_threshold_semantics_are_validated_on_decode() {
    let value = serde_json::json!({"action":"replace_policy","family":"object","scope":{"kind":"deployment"},"expected_policy_revision":"0","binding":{"kind":"expire","trigger":{"kind":"logical_size","upper_logical_bytes":"5","lower_logical_bytes":"5"},"minimum_age_seconds":"0"},"application":{"kind":"prospective"}});
    assert!(serde_json::from_value::<HistoryPolicyChange>(value).is_err());
}
#[test]
fn future_retrospective_cutoff_is_rejected() {
    let proposal = HistoryPolicyChange::new(
        HistoryPolicyKey::new(
            StorageIdentityHistoryKind::Object,
            HistoryPolicyScope::Deployment,
        )
        .unwrap(),
        HistoryPolicyRevision::new(0).unwrap(),
        age(0),
        HistoryPolicyApplication::Retrospective {
            closed_through: at(101),
        },
    )
    .unwrap();
    assert!(
        HistoryPolicyCatalog::default()
            .append(proposal, PrincipalIdentity::generate(), at(100))
            .is_err()
    );
}

fn preview_fixture() -> (HistoryPolicyPreview, PrincipalIdentity, HistoryReviewFence) {
    let principal = PrincipalIdentity::generate();
    let fence = HistoryReviewFence::new(
        uuid::Uuid::new_v4(),
        HistoryAuthorityRevision::new(0).unwrap(),
        HistoryDeletionRevision::new(0).unwrap(),
    )
    .unwrap();
    let proposal = HistoryPolicyChange::new(
        HistoryPolicyKey::new(
            StorageIdentityHistoryKind::Object,
            HistoryPolicyScope::Deployment,
        )
        .unwrap(),
        HistoryPolicyRevision::new(0).unwrap(),
        age(10),
        HistoryPolicyApplication::Retrospective {
            closed_through: at(50),
        },
    )
    .unwrap();
    let impact =
        HistoryPreviewImpact::new(HistoryPreviewCoverage::Bounded, 0, 0, Vec::new()).unwrap();
    (
        HistoryPolicyPreview::issue(principal, proposal, fence, at(100), impact).unwrap(),
        principal,
        fence,
    )
}
#[rstest]
#[case(399, true)]
#[case(400, false)]
#[case(401, false)]
fn preview_lifetime_is_fixed_on_issue(#[case] now: i64, #[case] accepted: bool) {
    let (preview, principal, fence) = preview_fixture();
    assert_eq!(
        preview
            .validate_apply(
                principal,
                preview.proposal(),
                preview.reference(),
                fence,
                at(now)
            )
            .is_ok(),
        accepted
    );
}
#[test]
fn changed_hold_authority_invalidates_review() {
    let (preview, principal, old_fence) = preview_fixture();
    let fence = HistoryReviewFence::new(
        old_fence.incarnation(),
        HistoryAuthorityRevision::new(1).unwrap(),
        HistoryDeletionRevision::new(0).unwrap(),
    )
    .unwrap();
    assert_eq!(
        preview.validate_apply(
            principal,
            preview.proposal(),
            preview.reference(),
            fence,
            at(101)
        ),
        Err(HistoryPolicyReviewConflict::StalePreview)
    );
}
#[test]
fn restore_incarnation_invalidates_review() {
    let (preview, principal, _) = preview_fixture();
    let fence = HistoryReviewFence::new(
        uuid::Uuid::new_v4(),
        HistoryAuthorityRevision::new(0).unwrap(),
        HistoryDeletionRevision::new(0).unwrap(),
    )
    .unwrap();
    assert_eq!(
        preview.validate_apply(
            principal,
            preview.proposal(),
            preview.reference(),
            fence,
            at(101)
        ),
        Err(HistoryPolicyReviewConflict::StalePreview)
    );
}
#[test]
fn another_principal_cannot_apply_review() {
    let (preview, _, fence) = preview_fixture();
    assert_eq!(
        preview.validate_apply(
            PrincipalIdentity::generate(),
            preview.proposal(),
            preview.reference(),
            fence,
            at(101)
        ),
        Err(HistoryPolicyReviewConflict::InaccessiblePreview)
    );
}
#[test]
fn changed_proposal_cannot_reuse_issued_review() {
    let (preview, principal, fence) = preview_fixture();
    let proposal = HistoryPolicyChange::new(
        preview.proposal().key(),
        preview.proposal().expected_policy_revision(),
        age(0),
        preview.proposal().application().clone(),
    )
    .unwrap();
    assert_eq!(
        preview.validate_apply(principal, &proposal, preview.reference(), fence, at(101)),
        Err(HistoryPolicyReviewConflict::PreviewMismatch)
    );
}
#[test]
fn persisted_review_does_not_accept_renewed_expiry() {
    let (preview, _, _) = preview_fixture();
    let mut value = serde_json::to_value(preview).unwrap();
    value["expires_at"] = serde_json::json!(at(500));
    assert!(serde_json::from_value::<HistoryPolicyPreview>(value).is_err());
}
#[test]
fn persisted_authority_roundtrip_preserves_original_hold_clocks() {
    let hold = HistoryRetentionHold::new(
        uuid::Uuid::new_v4(),
        HistoryPolicyKey::new(
            StorageIdentityHistoryKind::Object,
            HistoryPolicyScope::Deployment,
        )
        .unwrap(),
        at(100),
        Some(at(200)),
        PrincipalIdentity::generate(),
    )
    .unwrap();
    let authority = HistoryLifecycleAuthority::empty()
        .create_hold(hold.clone())
        .unwrap()
        .release_hold(hold.identity(), PrincipalIdentity::generate(), at(150))
        .unwrap();
    let recovered: HistoryLifecycleAuthority =
        serde_json::from_slice(&serde_json::to_vec(&authority).unwrap()).unwrap();
    assert_eq!(recovered, authority);
}
#[test]
fn retried_hold_creation_cannot_undo_release() {
    let hold = HistoryRetentionHold::new(
        uuid::Uuid::new_v4(),
        HistoryPolicyKey::new(
            StorageIdentityHistoryKind::Object,
            HistoryPolicyScope::Deployment,
        )
        .unwrap(),
        at(100),
        Some(at(200)),
        PrincipalIdentity::generate(),
    )
    .unwrap();
    let authority = HistoryLifecycleAuthority::empty()
        .create_hold(hold.clone())
        .unwrap()
        .release_hold(hold.identity(), PrincipalIdentity::generate(), at(150))
        .unwrap();
    let retried = authority.create_hold(hold).unwrap();
    assert!(retried.current_holds().all(|hold| !hold.is_active(at(175))));
}
#[test]
fn hold_on_either_relation_endpoint_protects_whole_copy() {
    let left = resource();
    let right = resource();
    let hold = HistoryRetentionHold::new(
        uuid::Uuid::new_v4(),
        HistoryPolicyKey::new(
            StorageIdentityHistoryKind::ObjectRelation,
            HistoryPolicyScope::Collection { identity: right },
        )
        .unwrap(),
        at(1),
        None,
        PrincipalIdentity::generate(),
    )
    .unwrap();
    let row = HistoryExpiryCandidate::new(
        StorageIdentityHistoryKind::ObjectRelation,
        StorageRecordIdentity::new(resource(), VersionIdentity::generate()),
        Some(HistoryRetentionAnchor::Closed(at(2))),
        vec![
            HistoryScopeAttribution::new(left, None),
            HistoryScopeAttribution::new(right, None),
        ],
        false,
    )
    .unwrap();
    assert!(
        HistoryLifecycleAuthority::empty()
            .create_hold(hold)
            .unwrap()
            .holds(&row, at(10))
    );
}

#[test]
fn expiring_hold_invalidates_review_without_a_concurrent_write() {
    let principal = PrincipalIdentity::generate();
    let key = HistoryPolicyKey::new(
        StorageIdentityHistoryKind::Object,
        HistoryPolicyScope::Deployment,
    )
    .unwrap();
    let hold =
        HistoryRetentionHold::new(uuid::Uuid::new_v4(), key, at(1), Some(at(150)), principal)
            .unwrap();
    let initial = HistoryLifecycleControl::empty();
    let fence = HistoryReviewFence::new(
        uuid::Uuid::new_v4(),
        initial.authority().revision(),
        HistoryDeletionRevision::new(0).unwrap(),
    )
    .unwrap();
    let control = initial.create_hold(hold, fence).unwrap().next().clone();
    let fence = HistoryReviewFence::new(
        fence.incarnation(),
        control.authority().revision(),
        HistoryDeletionRevision::new(0).unwrap(),
    )
    .unwrap();
    let proposal = HistoryPolicyChange::new(
        key,
        HistoryPolicyRevision::new(0).unwrap(),
        age(0),
        HistoryPolicyApplication::Retrospective {
            closed_through: at(100),
        },
    )
    .unwrap();
    let preview = HistoryPolicyPreview::issue(
        principal,
        proposal.clone(),
        fence,
        at(101),
        HistoryPreviewImpact::new(HistoryPreviewCoverage::Bounded, 0, 0, Vec::new()).unwrap(),
    )
    .unwrap();
    let issued = control
        .issue_preview(preview.clone(), fence)
        .unwrap()
        .next()
        .clone();
    assert!(matches!(
        issued.apply(
            HistoryPolicyApplyRequest::new(proposal, Some(preview.reference().clone())),
            principal,
            fence,
            at(151)
        ),
        Err(HistoryPolicyApplyError::Conflict(
            HistoryPolicyReviewConflict::StalePreview
        ))
    ));
}
#[rstest]
#[case(serde_json::json!(null))]
#[case(serde_json::json!(10))]
fn explicit_deleted_age_must_be_a_decimal_string(#[case] value: serde_json::Value) {
    let proposal = serde_json::json!({"action":"replace_policy","family":"object","scope":{"kind":"deployment"},"expected_policy_revision":"0","binding":{"kind":"expire","trigger":{"kind":"age","age_seconds":"20","deleted_age_seconds":value},"minimum_age_seconds":"0"},"application":{"kind":"prospective"}});
    assert!(serde_json::from_value::<HistoryPolicyChange>(proposal).is_err());
}
#[rstest]
#[case("2026-11-03T10:00:00+01:00")]
#[case("2026-02-30T10:00:00Z")]
fn retrospective_cutoff_requires_a_real_utc_timestamp(#[case] value: &str) {
    assert!(
        serde_json::from_value::<HistoryPolicyApplication>(
            serde_json::json!({"kind":"retrospective","closed_through":value})
        )
        .is_err()
    );
}
#[test]
fn policy_audit_replay_retains_identity_and_original_occurrence() {
    let policies = apply(
        &HistoryPolicyCatalog::default(),
        HistoryPolicyScope::Deployment,
        age(10),
        HistoryPolicyApplication::Prospective,
        100,
    );
    let authority = HistoryLifecycleAuthority::empty()
        .apply(
            policies.records()[0].proposal().clone(),
            policies.records()[0].principal(),
            at(100),
        )
        .unwrap();
    let recovered: HistoryLifecycleAuthority =
        serde_json::from_slice(&serde_json::to_vec(&authority).unwrap()).unwrap();
    let original = authority
        .audit_events_since(&HistoryLifecycleAuthority::empty())
        .unwrap();
    let replay = recovered
        .audit_events_since(&HistoryLifecycleAuthority::empty())
        .unwrap();
    assert_eq!(original[0].event_id(), replay[0].event_id());
    assert_eq!(replay[0].occurred_at(), Some(at(100)));
}
#[test]
fn unchanged_hold_creation_has_no_additional_audit_fact() {
    let hold = HistoryRetentionHold::new(
        uuid::Uuid::new_v4(),
        HistoryPolicyKey::new(
            StorageIdentityHistoryKind::Object,
            HistoryPolicyScope::Deployment,
        )
        .unwrap(),
        at(1),
        None,
        PrincipalIdentity::generate(),
    )
    .unwrap();
    let authority = HistoryLifecycleAuthority::empty()
        .create_hold(hold.clone())
        .unwrap();
    assert!(
        authority
            .create_hold(hold)
            .unwrap()
            .audit_events_since(&authority)
            .unwrap()
            .is_empty()
    );
}
#[rstest]
#[case(false, 100, 101, true)]
#[case(false, 100, 50, false)]
#[case(true, 20, 101, true)]
#[case(true, 20, 50, false)]
#[case(true, 100, 101, false)]
fn size_combinations_follow_explicit_boolean_rule(
    #[case] and: bool,
    #[case] age_seconds: u64,
    #[case] measured: u64,
    #[case] eligible: bool,
) {
    let age_rule = HistoryAgeRule::new(HistoryRetentionSeconds::new(age_seconds).unwrap(), None);
    let size = HistorySizeRule::new(
        HistoryLogicalBytes::new(100).unwrap(),
        HistoryLogicalBytes::new(40).unwrap(),
    )
    .unwrap();
    let trigger = if and {
        HistoryExpiryTrigger::AgeAndSize {
            age: age_rule,
            size,
        }
    } else {
        HistoryExpiryTrigger::AgeOrSize {
            age: age_rule,
            size,
        }
    };
    let policies = apply(
        &HistoryPolicyCatalog::default(),
        HistoryPolicyScope::Deployment,
        HistoryPolicyBinding::Expire {
            trigger,
            minimum_age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
        },
        HistoryPolicyApplication::Prospective,
        1,
    );
    let row = candidate(
        vec![HistoryScopeAttribution::new(resource(), None)],
        Some(HistoryRetentionAnchor::Closed(at(10))),
    );
    let policy = policies.resolve(row.family(), row.attributions()[0], row.anchor().unwrap());
    let pressure =
        HistorySizePressure::measure(&policy, HistoryLogicalBytes::new(measured).unwrap(), None)
            .unwrap();
    assert_eq!(
        row.inspect(&policies, at(50), false, &[pressure])
            .unwrap()
            .eligible(),
        eligible
    );
}

#[test]
fn hold_expiry_settlement_records_original_deadline_once() {
    let hold = HistoryRetentionHold::new(
        uuid::Uuid::new_v4(),
        HistoryPolicyKey::new(
            StorageIdentityHistoryKind::Object,
            HistoryPolicyScope::Deployment,
        )
        .unwrap(),
        at(1),
        Some(at(100)),
        PrincipalIdentity::generate(),
    )
    .unwrap();
    let created = HistoryLifecycleAuthority::empty()
        .create_hold(hold.clone())
        .unwrap();
    let expired = created
        .settle_expired_hold(hold.identity(), at(300))
        .unwrap();
    let events = expired.audit_events_since(&created).unwrap();
    assert_eq!(events[0].occurred_at(), Some(at(100)));
    assert!(
        expired
            .settle_expired_hold(hold.identity(), at(400))
            .unwrap()
            .audit_events_since(&expired)
            .unwrap()
            .is_empty()
    );
}

#[rstest]
#[case(false, 20, true)]
#[case(true, 100, false)]
fn conclusive_age_conditions_do_not_require_size_accounting(
    #[case] and: bool,
    #[case] seconds: u64,
    #[case] eligible: bool,
) {
    let age = HistoryAgeRule::new(HistoryRetentionSeconds::new(seconds).unwrap(), None);
    let size = HistorySizeRule::new(
        HistoryLogicalBytes::new(100).unwrap(),
        HistoryLogicalBytes::new(40).unwrap(),
    )
    .unwrap();
    let trigger = if and {
        HistoryExpiryTrigger::AgeAndSize { age, size }
    } else {
        HistoryExpiryTrigger::AgeOrSize { age, size }
    };
    let policies = apply(
        &HistoryPolicyCatalog::default(),
        HistoryPolicyScope::Deployment,
        HistoryPolicyBinding::Expire {
            trigger,
            minimum_age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
        },
        HistoryPolicyApplication::Prospective,
        1,
    );
    let row = candidate(
        vec![HistoryScopeAttribution::new(resource(), None)],
        Some(HistoryRetentionAnchor::Closed(at(10))),
    );
    assert_eq!(
        row.inspect(&policies, at(50), false, &[])
            .unwrap()
            .eligible(),
        eligible
    );
}

#[test]
fn applied_review_audit_is_rebuilt_after_transient_preview_retirement() {
    let (preview, principal, fence) = preview_fixture();
    let initial = HistoryLifecycleControl::empty();
    let issued = initial
        .issue_preview(preview.clone(), fence)
        .unwrap()
        .next()
        .clone();
    let applied = issued
        .apply(
            HistoryPolicyApplyRequest::new(
                preview.proposal().clone(),
                Some(preview.reference().clone()),
            ),
            principal,
            fence,
            at(101),
        )
        .unwrap()
        .next()
        .clone();
    let original = issued.audit_events_since(&initial).unwrap();
    let replay = applied
        .authority()
        .audit_events_since(&HistoryLifecycleAuthority::empty())
        .unwrap();
    let review = replay
        .iter()
        .find(|event| event.event_id() == original[0].event_id())
        .unwrap();
    assert_eq!(review.metadata(), original[0].metadata());
    assert_eq!(review.occurred_at(), original[0].occurred_at());
}

#[test]
fn decoded_authority_reserves_a_terminal_revision_for_every_created_hold() {
    let hold = HistoryRetentionHold::new(
        Uuid::new_v4(),
        HistoryPolicyKey::new(
            StorageIdentityHistoryKind::Object,
            HistoryPolicyScope::Deployment,
        )
        .unwrap(),
        at(100),
        None,
        PrincipalIdentity::generate(),
    )
    .unwrap();
    let authority = HistoryLifecycleAuthority::empty()
        .create_hold(hold)
        .unwrap();
    let mut encoded = serde_json::to_value(authority).unwrap();
    let source = encoded["holds"][0].clone();
    let count = HistoryLifecycleAuthority::MAX_HOLD_REVISIONS / 2 + 1;
    encoded["holds"] = serde_json::Value::Array(
        (0..count)
            .map(|index| {
                let mut entry = source.clone();
                entry["operation"] = serde_json::json!(Uuid::new_v4());
                entry["revision"] = serde_json::json!((index + 1).to_string());
                entry["hold"]["identity"] = serde_json::json!(Uuid::new_v4());
                entry
            })
            .collect(),
    );
    encoded["revision"] = serde_json::json!(count.to_string());
    assert!(serde_json::from_value::<HistoryLifecycleAuthority>(encoded).is_err());
}
