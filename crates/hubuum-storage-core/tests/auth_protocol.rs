//! Shared protocol persistence rules exercised independently of a backend.
use chrono::{Duration, Utc};
use hubuum_domain::{
    InteractionAuthority, InteractionBinding, InteractionBrowserBinding, InteractionClientId,
    InteractionDigest, InteractionId, InteractionLifecycle, InteractionMode, InteractionProviderId,
    ResourceRevision,
};
use hubuum_storage_core::*;
use rstest::rstest;
use uuid::Uuid;

fn digest(value: char) -> InteractionDigest {
    InteractionDigest::new(value.to_string().repeat(64)).unwrap()
}
fn fixture() -> (StorageInteractionRecord, StorageInteractionAuthorization) {
    let provider = StorageProviderIdentityBinding::try_new(
        "company".into(),
        Uuid::new_v4(),
        StorageAuthenticationProviderKind::Oidc,
        "https://id.example/realm".into(),
    )
    .unwrap();
    let id = InteractionId::new(Uuid::new_v4()).unwrap();
    let binding = InteractionBinding::try_new(
        id,
        InteractionProviderId::external(provider.id()).unwrap(),
        InteractionClientId::new("fixture").unwrap(),
        ResourceRevision::INITIAL,
        InteractionAuthority::login(),
    )
    .unwrap();
    let now = Utc::now();
    let lifecycle = InteractionLifecycle::try_new(
        binding,
        InteractionMode::Browser,
        now,
        now + Duration::minutes(5),
    )
    .unwrap();
    let mut record = StorageInteractionCreate::builder(lifecycle, digest('a'))
        .user_code(digest('b'))
        .provider(provider)
        .try_build()
        .unwrap()
        .into_record();
    let browser = InteractionBrowserBinding::try_new(digest('c'), digest('d')).unwrap();
    record.bind_browser(browser.clone(), now).unwrap();
    let authorization = StorageInteractionAuthorization::Browser {
        id,
        binding: browser,
    };
    let protocol =
        StorageProtectedAuthenticationState::new(digest('e'), "encrypted-callback-secrets".into())
            .unwrap();
    assert!(matches!(
        record.apply(
            StorageInteractionCommand::new(
                authorization.clone(),
                StorageInteractionAction::PrepareProtocol(protocol)
            ),
            now
        ),
        StorageInteractionOutcome::View(_)
    ));
    (record, authorization)
}
fn claim(
    authorization: StorageInteractionAuthorization,
    state: InteractionDigest,
) -> StorageInteractionCommand {
    StorageInteractionCommand::new(
        authorization,
        StorageInteractionAction::ClaimProtocol {
            attempt: Uuid::new_v4(),
            state,
        },
    )
}

#[test]
fn callback_claim_survives_restart_without_reusable_protocol_state() {
    let (record, authorization) = fixture();
    let mut restarted =
        StorageInteractionRecord::from_document(record.to_document().unwrap()).unwrap();
    assert!(matches!(
        restarted.apply(claim(authorization.clone(), digest('e')), Utc::now()),
        StorageInteractionOutcome::ProtocolClaim(_)
    ));
    let mut restarted =
        StorageInteractionRecord::from_document(restarted.to_document().unwrap()).unwrap();
    assert!(matches!(
        restarted.apply(claim(authorization, digest('e')), Utc::now()),
        StorageInteractionOutcome::Rejected(_)
    ));
}

#[rstest]
#[case::state(false)]
#[case::cookie(true)]
fn unauthenticated_callback_cannot_consume_protocol_state(#[case] wrong_cookie: bool) {
    let (mut record, authorization) = fixture();
    let mut wrong = authorization.clone();
    if wrong_cookie {
        let StorageInteractionAuthorization::Browser { binding, .. } = &mut wrong else {
            unreachable!()
        };
        *binding = InteractionBrowserBinding::try_new(digest('f'), digest('d')).unwrap();
    }
    assert!(matches!(
        record.apply(
            claim(wrong, digest(if wrong_cookie { 'e' } else { 'f' })),
            Utc::now()
        ),
        StorageInteractionOutcome::Rejected(_)
    ));
    assert!(matches!(
        record.apply(claim(authorization, digest('e')), Utc::now()),
        StorageInteractionOutcome::ProtocolClaim(_)
    ));
}

#[test]
fn prepared_browser_protocol_cannot_fall_back_to_password_claim() {
    let (mut record, authorization) = fixture();
    assert!(matches!(
        record.apply(
            StorageInteractionCommand::new(
                authorization,
                StorageInteractionAction::Claim(Uuid::new_v4())
            ),
            Utc::now()
        ),
        StorageInteractionOutcome::Rejected(_)
    ));
}

#[test]
fn expired_interaction_discards_encrypted_callback_material() {
    let (mut record, authorization) = fixture();
    assert!(matches!(
        record.apply(
            claim(authorization, digest('e')),
            Utc::now() + Duration::minutes(6)
        ),
        StorageInteractionOutcome::Rejected(_)
    ));
    assert!(record.to_document().unwrap()["protocol"].is_null());
}

#[test]
fn failed_callback_is_terminal_after_restart() {
    let (mut record, authorization) = fixture();
    let StorageInteractionOutcome::ProtocolClaim(claimed) =
        record.apply(claim(authorization.clone(), digest('e')), Utc::now())
    else {
        panic!("claim")
    };
    record.apply(
        StorageInteractionCommand::new(
            StorageInteractionAuthorization::Claim(claimed.claim().clone()),
            StorageInteractionAction::Fail,
        ),
        Utc::now(),
    );
    let mut record =
        StorageInteractionRecord::from_document(record.to_document().unwrap()).unwrap();
    assert!(matches!(
        record.apply(claim(authorization, digest('e')), Utc::now()),
        StorageInteractionOutcome::Rejected(_)
    ));
}
