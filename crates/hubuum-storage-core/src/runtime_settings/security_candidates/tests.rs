use super::*;
use crate::{StorageAuthorizationBackend, StorageRuntimeSettingsValue};
use hubuum_events_core::EventContext;
use rstest::rstest;

fn digest(byte: char) -> StorageCredentialFingerprint {
    StorageCredentialFingerprint::new(byte.to_string().repeat(64)).unwrap()
}
fn checks(status: StorageSecurityCheckStatus) -> Vec<StorageSecurityCheck> {
    [
        StorageSecurityCheckKind::Configuration,
        StorageSecurityCheckKind::RecoveryAdministrator,
    ]
    .into_iter()
    .map(|kind| StorageSecurityCheck::try_new(kind, status, None, "checked").unwrap())
    .collect()
}
fn candidate() -> StorageSecurityCandidate {
    let owner =
        StorageSecurityCandidateOwner::new(PrincipalId::new(1).unwrap(), TokenId::new(2).unwrap());
    let update = StorageRuntimeSettingsUpdate::new(
        StorageRuntimeSettingsAssignment::Managed(StorageRuntimeSettingsValue::Authorization(
            StorageAuthorizationBackend::Local,
        )),
        ResourceRevision::INITIAL,
        EventContext::user(owner.actor_id(), None, None),
    );
    StorageSecurityCandidate::try_new(
        Uuid::new_v4(),
        owner,
        &update,
        StorageSecurityCandidateBinding::new(digest('a'), digest('b'), digest('c')),
    )
    .unwrap()
    .with_checks(checks(StorageSecurityCheckStatus::Passed))
    .unwrap()
}
fn activation(
    candidate: &StorageSecurityCandidate,
    receipt: StorageCredentialFingerprint,
) -> StorageSecurityCandidateActivation {
    let owner = candidate.owner();
    let claim = StorageCredentialClaim::new(
        owner.actor_id(),
        owner.token_id(),
        StorageCredentialOperation::ActivateSecurityCandidate,
        digest('d'),
        digest('e'),
        Utc::now() - Duration::hours(24),
    )
    .security_candidate(candidate.id());
    let update = StorageRuntimeSettingsUpdate::new(
        candidate.assignment().clone(),
        candidate.base_revision(),
        EventContext::user(owner.actor_id(), None, None),
    );
    StorageSecurityCandidateActivation::try_new(candidate, receipt, update, claim, "admin".into())
        .unwrap()
}

#[rstest]
#[case(StorageSecurityCheckStatus::NotRun)]
#[case(StorageSecurityCheckStatus::Failed)]
fn incomplete_checks_cannot_mint_a_receipt(#[case] status: StorageSecurityCheckStatus) {
    assert!(
        candidate()
            .with_checks(checks(status))
            .unwrap()
            .verify(digest('f'))
            .is_err()
    );
}
#[test]
fn candidate_receipt_is_consumed_once() {
    let mut candidate = candidate().verify(digest('f')).unwrap();
    let request = activation(&candidate, digest('f'));
    candidate.activate(&request, Utc::now()).unwrap();
    assert!(candidate.activate(&request, Utc::now()).is_err());
}
#[test]
fn receipt_cannot_activate_another_candidate() {
    let mut candidate = candidate().verify(digest('f')).unwrap();
    let request = activation(&candidate, digest('0'));
    assert!(candidate.activate(&request, Utc::now()).is_err());
}
#[test]
fn expiration_revokes_verified_authority() {
    let mut candidate = candidate().verify(digest('f')).unwrap();
    let request = activation(&candidate, digest('f'));
    assert!(
        candidate
            .activate(&request, candidate.expires_at())
            .is_err()
    );
}
#[test]
fn concurrent_verification_cannot_lose_a_completed_check() {
    let mut stored = candidate();
    let first = stored
        .clone()
        .with_checks(checks(StorageSecurityCheckStatus::Passed))
        .unwrap();
    let stale = first.clone();
    stored.accept_verification(first, Utc::now()).unwrap();
    assert!(stored.accept_verification(stale, Utc::now()).is_err());
}
#[test]
fn release_retains_the_verified_selection_in_persistence() {
    let candidate = candidate()
        .release_ownership()
        .unwrap()
        .verify(digest('f'))
        .unwrap();
    let restored = StorageSecurityCandidate::from_document(candidate.to_document()).unwrap();
    assert_eq!(restored, candidate);
    assert!(restored.assignment().value().is_none());
    assert!(restored.verification_assignment().value().is_some());
}
#[test]
fn persistence_rejects_extended_candidate_lifetime() {
    let candidate = candidate();
    let mut document = candidate.to_document();
    document["expires_at"] = json!(candidate.created_at() + Duration::seconds(301));
    assert!(StorageSecurityCandidate::from_document(document).is_err());
}

#[rstest]
#[case("definition")]
#[case("secrets")]
#[case("deployment")]
fn changed_verification_binding_invalidates_authority(#[case] field: &str) {
    let mut stored = candidate().verify(digest('f')).unwrap();
    let mut changed = stored.to_document();
    changed[field] = json!("0".repeat(64));
    let changed = StorageSecurityCandidate::from_document(changed).unwrap();
    let request = activation(&changed, digest('f'));
    assert!(stored.activate(&request, Utc::now()).is_err());
}
