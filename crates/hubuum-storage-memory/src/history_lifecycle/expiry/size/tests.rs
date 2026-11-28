use super::*;

async fn fixture() -> MemoryStorage {
    let storage = MemoryStorage::new();
    for name in ["size_oldest", "size_newer"] {
        let class = storage
            .create_class(
                StorageClassCreate::builder(name, CollectionId::new(1).unwrap(), "").build(),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
        let resolved = storage
            .resolve_class(StorageClassSelector::Id(class.id()))
            .await
            .unwrap();
        storage
            .delete_class(&resolved, &EventContext::system())
            .await
            .unwrap()
            .into_value();
    }
    storage
}
fn install(
    state: &mut MemoryState,
    family: StorageIdentityHistoryKind,
    trigger: HistoryExpiryTrigger,
) -> HistoryPolicyKey {
    let snapshot = state.history_lifecycle_snapshot().unwrap();
    let key = HistoryPolicyKey::new(family, HistoryPolicyScope::Deployment).unwrap();
    let proposal = HistoryPolicyChange::new(
        key,
        HistoryPolicyRevision::new(0).unwrap(),
        HistoryPolicyBinding::Expire {
            trigger,
            minimum_age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
        },
        HistoryPolicyApplication::Prospective,
    )
    .unwrap();
    state.history_lifecycle = snapshot
        .control()
        .apply(
            HistoryPolicyApplyRequest::new(proposal, None),
            PrincipalIdentity::generate(),
            snapshot.fence(),
            snapshot.observed_at() - Duration::seconds(60),
        )
        .unwrap()
        .next()
        .clone();
    key
}
fn request(state: &MemoryState, key: HistoryPolicyKey) -> HistoryExpiryAdmissionRequest {
    HistoryExpiryAdmissionRequest::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        HistoryExpiryOrigin::Manual {
            principal: PrincipalIdentity::generate(),
        },
        HistoryLifecycleScan::new(key, 1000).unwrap(),
        state.history_lifecycle_snapshot().unwrap().fence(),
    )
    .unwrap()
}

#[tokio::test]
async fn complete_native_measurement_selects_the_oldest_copy_once() {
    let storage = fixture().await;
    let mut state = storage.state.write_control().await;
    let oldest = state
        .schema_history
        .iter()
        .min_by_key(|row| {
            serde_json::from_value::<DateTime<Utc>>(row.get("occurred_at").unwrap().clone())
                .unwrap()
        })
        .unwrap()
        .get("logical_identity")
        .unwrap()
        .clone();
    let expected: StorageIdentityEvidence = serde_json::from_value(oldest).unwrap();
    let total = state
        .schema_history
        .iter()
        .map(|row| {
            HistoryLogicalBytes::measure_fields_json_v1(row.fields())
                .unwrap()
                .value()
        })
        .sum::<u64>();
    let key = install(
        &mut state,
        StorageIdentityHistoryKind::ClassSchema,
        HistoryExpiryTrigger::LogicalSize {
            upper_logical_bytes: HistoryLogicalBytes::new(total - 1).unwrap(),
            lower_logical_bytes: HistoryLogicalBytes::new(total - 2).unwrap(),
        },
    );
    state.schema_history.reverse();
    let admission = request(&state, key);
    let page = super::super::admit_on(&mut state, admission).unwrap();
    assert!(!page.accounting_unavailable());
    let manifest = page.manifest().unwrap();
    assert_eq!(
        manifest
            .members()
            .iter()
            .map(|member| member.target())
            .collect::<Vec<_>>(),
        [expected.subject().unwrap().record_identity().unwrap()]
    );
    let admission = request(&state, key);
    assert!(
        super::super::admit_on(&mut state, admission)
            .unwrap()
            .manifest()
            .is_none()
    );
}

#[tokio::test]
async fn an_unavailable_complete_census_preserves_age_or_size_short_circuit() {
    let storage = fixture().await;
    let mut state = storage.state.write_control().await;
    let key = install(
        &mut state,
        StorageIdentityHistoryKind::ClassSchema,
        HistoryExpiryTrigger::AgeOrSize {
            age: HistoryAgeRule::new(HistoryRetentionSeconds::new(0).unwrap(), None),
            size: HistorySizeRule::new(
                HistoryLogicalBytes::new(1).unwrap(),
                HistoryLogicalBytes::new(0).unwrap(),
            )
            .unwrap(),
        },
    );
    let mut fields = state.schema_history[0].fields().clone();
    fields.insert("logical_identity".into(), Value::Null);
    state.schema_history[0] = StorageBackupRow::try_from_value(Value::Object(fields)).unwrap();
    let admission = request(&state, key);
    let page = super::super::admit_on(&mut state, admission).unwrap();
    assert!(page.accounting_unavailable());
    assert!(page.manifest().is_some());
    assert!(
        state
            .history_lifecycle
            .authority()
            .size_transitions()
            .is_empty()
    );
}

#[tokio::test]
async fn size_required_audit_keeps_both_pressure_and_intent_atomic() {
    let storage = fixture().await;
    let mut state = storage.state.write_control().await;
    let key = install(
        &mut state,
        StorageIdentityHistoryKind::ClassSchema,
        HistoryExpiryTrigger::LogicalSize {
            upper_logical_bytes: HistoryLogicalBytes::new(1).unwrap(),
            lower_logical_bytes: HistoryLogicalBytes::new(0).unwrap(),
        },
    );
    let before = state.history_lifecycle.clone();
    let events = state.events.len();
    // Permit the first event's sequence, then fail before the second can commit.
    state.event_sequences.observe(i64::MAX - 1);
    let admission = request(&state, key);
    assert!(super::super::admit_on(&mut state, admission).is_err());
    assert_eq!(state.history_lifecycle, before);
    assert_eq!(state.events.len(), events);
}

#[rstest::rstest]
#[case::foreign_scope(false)]
#[case::deployment(true)]
#[tokio::test]
async fn oversized_object_authorization_preserves_other_eligible_rows(#[case] deployment: bool) {
    use super::super::progress_tests::seed;
    let storage = MemoryStorage::new();
    let family = StorageIdentityHistoryKind::Object;
    let key = {
        let mut state = storage.state.write_control().await;
        install(
            &mut state,
            family,
            HistoryExpiryTrigger::LogicalSize {
                upper_logical_bytes: HistoryLogicalBytes::new(1).unwrap(),
                lower_logical_bytes: HistoryLogicalBytes::new(0).unwrap(),
            },
        )
    };
    seed(
        &storage,
        family,
        &"x".repeat(HistoryAuthorizationBytes::MAX_BYTES),
        "",
    )
    .await;
    let small = seed(&storage, family, "ordinary object", "").await;
    let state = storage.state.read_control().await;
    let plan = plan(
        &state,
        request(&state, if deployment { key } else { small }),
    )
    .unwrap();
    let (_, result) = plan.into_parts();
    assert_eq!(result.oversized_source_rows() > 0, deployment);
    let members = result.manifest().unwrap().members();
    assert!(members.iter().all(|member| {
        member
            .attributions()
            .iter()
            .any(|scope| scope.matches(small.scope()))
    }));
}
