use super::*;
use crate::{
    InteractionAuthority, InteractionClientId, InteractionId, InteractionProviderId,
    ResourceRevision,
};
use rstest::rstest;

fn at(seconds: i64) -> DateTime<Utc> {
    DateTime::UNIX_EPOCH + Duration::seconds(seconds)
}
fn id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}
fn browser(cookie: char, csrf: char) -> InteractionBrowserBinding {
    InteractionBrowserBinding::try_new(
        InteractionDigest::new(cookie.to_string().repeat(64)).unwrap(),
        InteractionDigest::new(csrf.to_string().repeat(64)).unwrap(),
    )
    .unwrap()
}
fn interaction(mode: InteractionMode) -> InteractionLifecycle {
    InteractionLifecycle::try_new(
        InteractionBinding::try_new(
            InteractionId::new(id(1)).unwrap(),
            InteractionProviderId::external(id(2)).unwrap(),
            InteractionClientId::new("hubuum-cli").unwrap(),
            ResourceRevision::new(1).unwrap(),
            InteractionAuthority::login(),
        )
        .unwrap(),
        mode,
        at(0),
        at(300),
    )
    .unwrap()
}
fn ready() -> InteractionLifecycle {
    let mut value = interaction(InteractionMode::Credentials);
    let claim = value.claim_authentication(id(3), None, at(0)).unwrap();
    let proof = VerifiedInteractionSubject::new(value.binding().clone(), "subject").unwrap();
    value
        .finish_authentication(&claim, proof, None, at(1))
        .unwrap();
    value
}
fn awaiting_approval() -> InteractionLifecycle {
    let mut value = interaction(InteractionMode::Browser);
    let initial = browser('a', 'b');
    value.bind_browser(initial.clone(), at(0)).unwrap();
    let claim = value
        .claim_authentication(id(3), Some(&initial), at(1))
        .unwrap();
    let proof = VerifiedInteractionSubject::new(value.binding().clone(), "subject").unwrap();
    value
        .finish_authentication(&claim, proof, Some(browser('a', 'c')), at(2))
        .unwrap();
    value
}

#[rstest]
#[case(-1)]
#[case(0)]
#[case(601)]
fn lifetime_cannot_exceed_the_deployment_ceiling(#[case] seconds: i64) {
    let value = interaction(InteractionMode::Credentials);
    assert_eq!(
        InteractionLifecycle::try_new(
            value.binding().clone(),
            InteractionMode::Credentials,
            at(0),
            at(seconds)
        ),
        Err(InteractionTransitionError::InvalidLifetime)
    );
}

#[rstest]
#[case(-1)]
#[case(300)]
fn authentication_fails_closed_outside_the_time_window(#[case] seconds: i64) {
    let mut value = interaction(InteractionMode::Credentials);
    assert_eq!(
        value.claim_authentication(id(3), None, at(seconds)),
        Err(InteractionTransitionError::Expired)
    );
}

#[test]
fn early_polling_persists_increasing_backoff() {
    let mut value = interaction(InteractionMode::Credentials);
    let first = value.poll(at(1));
    let second = value.poll(at(2));
    assert_eq!(
        (first, second),
        (
            Err(InteractionTransitionError::SlowDown {
                retry_after_seconds: 10
            }),
            Err(InteractionTransitionError::SlowDown {
                retry_after_seconds: 15
            }),
        )
    );
}

#[test]
fn on_time_polling_preserves_the_interval() {
    let mut value = interaction(InteractionMode::Credentials);
    assert_eq!(value.poll(at(5)), Ok(InteractionState::Pending));
}

#[test]
fn visible_user_code_cannot_replace_the_first_browser() {
    let mut value = interaction(InteractionMode::Browser);
    value.bind_browser(browser('a', 'b'), at(0)).unwrap();
    assert_eq!(
        value.bind_browser(browser('c', 'd'), at(1)),
        Err(InteractionTransitionError::Invalid)
    );
}

#[test]
fn browser_claim_requires_the_bound_cookie_and_csrf() {
    let mut value = interaction(InteractionMode::Browser);
    value.bind_browser(browser('a', 'b'), at(0)).unwrap();
    assert_eq!(
        value.claim_authentication(id(3), Some(&browser('a', 'c')), at(1)),
        Err(InteractionTransitionError::Invalid)
    );
}

#[test]
fn only_one_authentication_claim_can_be_outstanding() {
    let mut value = interaction(InteractionMode::Credentials);
    value.claim_authentication(id(3), None, at(0)).unwrap();
    assert_eq!(
        value.claim_authentication(id(4), None, at(1)),
        Err(InteractionTransitionError::Invalid)
    );
}

#[test]
fn old_claim_identifier_cannot_be_reused_after_failure() {
    let mut value = interaction(InteractionMode::Credentials);
    let claim = value.claim_authentication(id(3), None, at(0)).unwrap();
    value.reject_authentication(&claim, at(1)).unwrap();
    assert_eq!(
        value.claim_authentication(id(3), None, at(2)),
        Err(InteractionTransitionError::Invalid)
    );
}

#[test]
fn expired_provider_exchange_claim_cannot_restart() {
    let mut value = interaction(InteractionMode::Browser);
    let browser = browser('a', 'b');
    value.bind_browser(browser.clone(), at(0)).unwrap();
    value
        .claim_authentication(id(3), Some(&browser), at(0))
        .unwrap();
    assert_eq!(
        value.claim_authentication(id(4), Some(&browser), at(30)),
        Err(InteractionTransitionError::Failed)
    );
}

#[test]
fn callback_requires_explicit_browser_approval() {
    let mut value = awaiting_approval();
    assert_eq!(
        value.consume(InteractionIssuedResult::Session(id(4)), at(3)),
        Err(InteractionTransitionError::Pending)
    );
}

#[test]
fn callback_csrf_cannot_approve_the_completed_identity() {
    let mut value = awaiting_approval();
    assert_eq!(
        value.decide_browser(&browser('a', 'b'), true, at(3)),
        Err(InteractionTransitionError::Invalid)
    );
}

#[rstest]
#[case(true, InteractionState::Ready)]
#[case(false, InteractionState::Denied)]
fn browser_decision_is_explicit(#[case] approve: bool, #[case] expected: InteractionState) {
    let mut value = awaiting_approval();
    value
        .decide_browser(&browser('a', 'c'), approve, at(3))
        .unwrap();
    assert_eq!(value.state(at(4)), expected);
}

#[test]
fn browser_decision_cannot_be_replayed() {
    let mut value = awaiting_approval();
    value
        .decide_browser(&browser('a', 'c'), true, at(3))
        .unwrap();
    assert_eq!(
        value.decide_browser(&browser('a', 'c'), true, at(4)),
        Err(InteractionTransitionError::Invalid)
    );
}

#[test]
fn login_cannot_complete_with_a_privileged_result() {
    let mut value = ready();
    assert_eq!(
        value.consume(InteractionIssuedResult::Approval(1), at(2)),
        Err(InteractionTransitionError::Invalid)
    );
}

#[test]
fn consumed_interaction_never_issues_again_after_response_loss() {
    let mut value = ready();
    value
        .consume(InteractionIssuedResult::Session(id(4)), at(2))
        .unwrap();
    assert_eq!(
        value.consume(InteractionIssuedResult::Session(id(5)), at(3)),
        Err(InteractionTransitionError::Consumed)
    );
}

#[test]
fn consumed_status_retains_orphan_cleanup_metadata_after_login_expiry() {
    let mut value = ready();
    value
        .consume(InteractionIssuedResult::Session(id(4)), at(2))
        .unwrap();
    assert_eq!(
        (value.poll(at(301)), value.issued_result()),
        (
            Ok(InteractionState::Consumed),
            Some(InteractionIssuedResult::Session(id(4)))
        )
    );
}

#[test]
fn cancellation_cannot_undo_a_successful_consumption() {
    let mut value = ready();
    value
        .consume(InteractionIssuedResult::Session(id(4)), at(2))
        .unwrap();
    assert_eq!(
        value.cancel(at(3)),
        Err(InteractionTransitionError::Consumed)
    );
}

#[test]
fn cancelled_interaction_cannot_be_consumed() {
    let mut value = ready();
    value.cancel(at(2)).unwrap();
    assert_eq!(
        value.consume(InteractionIssuedResult::Session(id(4)), at(3)),
        Err(InteractionTransitionError::Cancelled)
    );
}

#[test]
fn cancellation_is_idempotent() {
    let mut value = ready();
    value.cancel(at(2)).unwrap();
    assert_eq!(value.cancel(at(3)), Ok(()));
}

#[test]
fn deadline_arithmetic_is_bounded_near_the_timestamp_limit() {
    let expires = DateTime::<Utc>::MAX_UTC;
    let now = expires - Duration::seconds(1);
    let binding = interaction(InteractionMode::Credentials).binding().clone();
    let mut value =
        InteractionLifecycle::try_new(binding, InteractionMode::Credentials, now, expires).unwrap();
    let claim = value.claim_authentication(id(3), None, now).unwrap();
    assert_eq!(claim.deadline(), expires);
}

#[rstest]
#[case(InteractionPurpose::CredentialApproval)]
#[case(InteractionPurpose::ProviderVerification)]
fn privileged_purposes_cannot_consume_a_session_result(#[case] purpose: InteractionPurpose) {
    let actor = crate::InteractionActor::new(
        crate::PrincipalId::new(1).unwrap(),
        crate::TokenId::new(1).unwrap(),
    );
    let digest = InteractionDigest::new("a".repeat(64)).unwrap();
    let authority = match purpose {
        InteractionPurpose::CredentialApproval => InteractionAuthority::approval(actor, digest),
        InteractionPurpose::ProviderVerification => {
            InteractionAuthority::verification(actor, id(8), digest).unwrap()
        }
        InteractionPurpose::Login => unreachable!(),
    };
    let base = interaction(InteractionMode::Credentials);
    let binding = InteractionBinding::try_new(
        base.binding().id(),
        base.binding().provider(),
        base.binding().client().clone(),
        base.binding().revision(),
        authority,
    )
    .unwrap();
    let mut value =
        InteractionLifecycle::try_new(binding, InteractionMode::Credentials, at(0), at(300))
            .unwrap();
    let claim = value.claim_authentication(id(3), None, at(0)).unwrap();
    let proof = VerifiedInteractionSubject::new(value.binding().clone(), "subject").unwrap();
    value
        .finish_authentication(&claim, proof, None, at(1))
        .unwrap();
    assert_eq!(
        value.consume(InteractionIssuedResult::Session(id(4)), at(2)),
        Err(InteractionTransitionError::Invalid)
    );
}
