use super::*;
use crate::StorageRecordIdentity;
use rstest::rstest;

fn request() -> HotObjectHistoryDeletion {
    HotObjectHistoryDeletion::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        StorageRecordIdentity::new(ResourceIdentity::generate(), VersionIdentity::generate()),
    )
    .unwrap()
}

fn empty() -> DeletionControlState {
    DeletionControlState::empty(Uuid::new_v4(), Uuid::new_v4()).unwrap()
}

#[test]
fn existing_v1_material_roundtrips_without_changing_signed_bytes() {
    let original = br#"{"format":1,"lineage":"00000000-0000-0000-0000-000000000001","epoch":"00000000-0000-0000-0000-000000000002","revision":1,"intents":{"00000000-0000-0000-0000-000000000003":{"subject":{"kind":"version","identity":"00000000-0000-4000-8000-000000000004"},"generation":1,"copies":{"00000000-0000-0000-0000-000000000005":"removed"}}}}"#;
    assert_eq!(
        DeletionControlState::decode(original)
            .unwrap()
            .encode()
            .unwrap(),
        original
    );
}

#[rstest]
#[case::pending(false)]
#[case::removed(true)]
fn v2_managed_scope_and_receipt_roundtrip(#[case] removed: bool) {
    let request = request();
    let mut state = empty()
        .record_hot_object_history_intent(&request)
        .unwrap()
        .next()
        .clone();
    if removed {
        let cleanup = HotObjectHistoryCleanup::new(
            state.clone(),
            request,
            DeletionCheckpointIdentity::new("a".repeat(64)).unwrap(),
        )
        .unwrap();
        state = state
            .record_hot_object_history_removed(&cleanup)
            .unwrap()
            .next()
            .clone();
    }
    let encoded = state.encode().unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&encoded).unwrap()["format"],
        2
    );
    assert_eq!(DeletionControlState::decode(&encoded).unwrap(), state);
}

#[test]
fn existing_legacy_copy_cannot_acquire_a_managed_binding() {
    let request = request();
    let legacy = empty()
        .record_intent(
            request.intent(),
            DeletionSubject::Version {
                identity: request.target().version(),
            },
            vec![request.copy()],
        )
        .unwrap();
    assert!(
        legacy
            .next()
            .record_hot_object_history_intent(&request)
            .is_err()
    );
}

#[rstest]
#[case(DeletionCopyState::Pending)]
#[case(DeletionCopyState::Blocked)]
#[case(DeletionCopyState::Removed)]
fn bound_copy_rejects_raw_outcome_assertions(#[case] outcome: DeletionCopyState) {
    let request = request();
    let state = empty().record_hot_object_history_intent(&request).unwrap();
    assert!(
        state
            .next()
            .record_copy(request.intent(), request.copy(), outcome)
            .is_err()
    );
}

#[test]
fn cleanup_receipt_binds_protecting_checkpoint_and_cannot_be_rewritten() {
    let request = request();
    let state = empty()
        .record_hot_object_history_intent(&request)
        .unwrap()
        .next()
        .clone();
    let checkpoint = DeletionCheckpointIdentity::new("a".repeat(64)).unwrap();
    let cleanup =
        HotObjectHistoryCleanup::new(state.clone(), request.clone(), checkpoint.clone()).unwrap();
    let completed = state.record_hot_object_history_removed(&cleanup).unwrap();
    let retry = HotObjectHistoryCleanup::new(
        completed.next().clone(),
        request.clone(),
        DeletionCheckpointIdentity::new("b".repeat(64)).unwrap(),
    )
    .unwrap();
    let retried = completed
        .next()
        .record_hot_object_history_removed(&retry)
        .unwrap();
    assert_eq!(
        retried.next().intents()[&request.intent()].receipts()[&request.copy()].checkpoint(),
        &checkpoint
    );
}

#[test]
fn recovered_state_cannot_rebind_a_copy_to_another_resource() {
    let request = request();
    let state = empty()
        .record_hot_object_history_intent(&request)
        .unwrap()
        .next()
        .clone();
    let mut changed = serde_json::to_value(&state).unwrap();
    changed["intents"][request.intent().to_string()]["bindings"][request.copy().to_string()]["target"]
        ["resource"] = serde_json::to_value(ResourceIdentity::generate()).unwrap();
    let changed: DeletionControlState = serde_json::from_value(changed).unwrap();
    assert!(!changed.covers(&state));
}

#[rstest]
#[case::v1_binding("format")]
#[case::removed_without_receipt("receipt")]
fn malformed_managed_authority_fails_closed(#[case] mutation: &str) {
    let request = request();
    let state = empty().record_hot_object_history_intent(&request).unwrap();
    let mut raw = serde_json::to_value(state.next()).unwrap();
    if mutation == "format" {
        raw["format"] = serde_json::json!(1);
    } else {
        raw["intents"][request.intent().to_string()]["copies"][request.copy().to_string()] =
            serde_json::json!("removed");
    }
    assert!(serde_json::from_value::<DeletionControlState>(raw).is_err());
}

#[test]
fn registration_debug_does_not_disclose_ownership() {
    let registration = RuntimeRegistration::generate();
    assert!(!format!("{registration:?}").contains(&registration.ownership_token().to_string()));
}
