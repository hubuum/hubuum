use chrono::Utc;
use rstest::rstest;

use super::*;

fn prepared() -> (DeletionControlState, HotObjectHistoryDeletion) {
    let request = HotObjectHistoryDeletion::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        crate::StorageRecordIdentity::new(
            ResourceIdentity::generate(),
            VersionIdentity::generate(),
        ),
    )
    .unwrap();
    let state = DeletionControlState::empty(Uuid::new_v4(), Uuid::new_v4())
        .unwrap()
        .record_hot_object_history_intent(&request)
        .unwrap()
        .with_hot_history_audit(&request, ResourceIdentity::generate(), Utc::now())
        .unwrap()
        .next()
        .clone();
    (state, request)
}

fn checkpoint() -> DeletionCheckpointIdentity {
    DeletionCheckpointIdentity::new("a".repeat(64)).unwrap()
}

fn complete() -> (DeletionControlState, HotObjectHistoryDeletion) {
    let (state, request) = prepared();
    let protected = state
        .record_audit_protection(checkpoint(), Utc::now(), DeletionAuditProtection::Intent)
        .unwrap();
    let cleanup =
        HotObjectHistoryCleanup::new(protected.next().clone(), request.clone(), checkpoint())
            .unwrap();
    let removed = protected
        .next()
        .record_hot_object_history_removed(&cleanup, Utc::now())
        .unwrap();
    (
        removed
            .next()
            .record_audit_protection(
                checkpoint(),
                Utc::now(),
                DeletionAuditProtection::Completion,
            )
            .unwrap()
            .next()
            .clone(),
        request,
    )
}

#[test]
fn copy_removal_requires_the_committed_recovery_observation() {
    let (state, request) = prepared();
    let cleanup = HotObjectHistoryCleanup::new(state.clone(), request, checkpoint()).unwrap();
    assert!(
        state
            .record_hot_object_history_removed(&cleanup, Utc::now())
            .is_err()
    );
}

#[test]
fn repeated_completion_preserves_the_original_evidence() {
    let (state, _) = complete();
    let retried = state
        .record_audit_protection(
            checkpoint(),
            Utc::now(),
            DeletionAuditProtection::Completion,
        )
        .unwrap();
    assert_eq!(retried.next(), &state);
}

#[test]
fn completion_evidence_preserves_the_existing_physical_reapplication_proof() {
    let (completed, request) = complete();
    let mut previous = serde_json::to_value(&completed).unwrap();
    previous["intents"][request.intent().to_string()]["audit"]["events"]
        .as_array_mut()
        .unwrap()
        .pop();
    previous["revision"] = serde_json::json!(completed.revision() - 1);
    let previous: DeletionControlState = serde_json::from_value(previous).unwrap();
    let fence = ApplicationFence::new(Uuid::new_v4(), previous.revision()).unwrap();
    let application = ApplicationControlState::quarantined(fence)
        .record_reapplication(ApplicationReapplication::new(fence))
        .unwrap();
    let change = previous
        .record_audit_protection(
            checkpoint(),
            Utc::now(),
            DeletionAuditProtection::Completion,
        )
        .unwrap();
    let updated = application.with_completed_audit(&change).unwrap().unwrap();
    assert_eq!(
        (
            updated.fence().incarnation(),
            updated.reapplication().unwrap().revision()
        ),
        (fence.incarnation(), change.next().revision())
    );
}

#[test]
fn an_intent_observation_cannot_replace_completion_evidence() {
    let (state, _) = prepared();
    let change = state
        .record_audit_protection(checkpoint(), Utc::now(), DeletionAuditProtection::Intent)
        .unwrap();
    let fence = ApplicationFence::new(Uuid::new_v4(), state.revision()).unwrap();
    let application = ApplicationControlState::quarantined(fence)
        .record_reapplication(ApplicationReapplication::new(fence))
        .unwrap();
    assert!(application.with_completed_audit(&change).is_err());
}

#[test]
fn protected_audit_roundtrip_preserves_exact_authority() {
    let (state, _) = complete();
    assert_eq!(
        DeletionControlState::decode(&state.encode().unwrap()).unwrap(),
        state
    );
}

#[test]
fn higher_revision_without_required_audit_facts_does_not_cover_the_checkpoint() {
    let (state, request) = complete();
    let mut modified = serde_json::to_value(&state).unwrap();
    modified["revision"] = serde_json::json!(state.revision() + 1);
    modified["intents"][request.intent().to_string()]
        .as_object_mut()
        .unwrap()
        .remove("audit");
    let modified: DeletionControlState = serde_json::from_value(modified).unwrap();
    assert!(!modified.covers(&state));
}

#[rstest]
#[case::requested(0, DeletionAuditTransition::Requested)]
#[case::restricted(1, DeletionAuditTransition::AccessRestricted)]
#[case::protected(2, DeletionAuditTransition::RecoveryProtected)]
#[case::removed(3, DeletionAuditTransition::CopyRemoved)]
#[case::completed(4, DeletionAuditTransition::Completed)]
fn recovered_canonical_projection_keeps_original_identity_and_time(
    #[case] index: usize,
    #[case] transition: DeletionAuditTransition,
) {
    let (state, request) = complete();
    let intent = &state.intents()[&request.intent()];
    let audit = intent.audit().unwrap();
    let fact = &audit.events()[index];
    let event = audit.event(request.intent(), intent, fact).unwrap();
    assert_eq!(
        (
            fact.transition(),
            event.event_id().as_uuid(),
            event.occurred_at()
        ),
        (transition, fact.identity(), Some(fact.occurred_at()))
    );
}

#[rstest]
#[case::nil_identity("event_id", serde_json::json!(Uuid::nil()))]
#[case::zero_revision("control_revision", serde_json::json!("0"))]
#[case::wrong_order("transition", serde_json::json!("completed"))]
fn malformed_protected_audit_fails_at_decode(
    #[case] field: &str,
    #[case] value: serde_json::Value,
) {
    let (state, request) = complete();
    let mut encoded = serde_json::to_value(state).unwrap();
    encoded["intents"][request.intent().to_string()]["audit"]["events"][0][field] = value;
    assert!(serde_json::from_value::<DeletionControlState>(encoded).is_err());
}

#[test]
fn protected_membership_identity_cannot_be_reused_for_another_operation() {
    let (state, request) = complete();
    let mut encoded = serde_json::to_value(state).unwrap();
    let mut intent = encoded["intents"][request.intent().to_string()].clone();
    for fact in intent["audit"]["events"].as_array_mut().unwrap() {
        fact["event_id"] = serde_json::json!(Uuid::new_v4());
    }
    encoded["intents"][Uuid::new_v4().to_string()] = intent;
    assert!(serde_json::from_value::<DeletionControlState>(encoded).is_err());
}

#[test]
fn protected_operation_identity_must_be_an_rfc_uuid() {
    let (state, request) = complete();
    let mut encoded = serde_json::to_value(state).unwrap();
    let intents = encoded["intents"].as_object_mut().unwrap();
    let intent = intents.remove(&request.intent().to_string()).unwrap();
    intents.insert("00000000-0000-f000-0000-000000000001".into(), intent);
    assert!(serde_json::from_value::<DeletionControlState>(encoded).is_err());
}
