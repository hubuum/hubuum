use chrono::Utc;
use hubuum_domain::{
    ExternalAuthenticationContext, ExternalIdentityProfile, ExternalSubject,
    FreshExternalAuthentication, FreshExternalAuthenticationRequest, IdentityScopeName,
    InteractionActor, InteractionAuthority, InteractionBinding, InteractionClientId,
    InteractionDigest, InteractionId, InteractionProviderId, MembershipClaims,
    OidcProviderDefinition, PrincipalId, ResourceRevision, TokenId,
};
use hubuum_storage_core::{
    AuthSessionId, StorageRetainedAuthentication, StorageUpstreamApproval, StorageUpstreamSession,
};
use rstest::rstest;
use serde_json::json;
use uuid::Uuid;

fn fresh(session: &StorageUpstreamSession) -> FreshExternalAuthentication {
    let binding = InteractionBinding::try_new(
        InteractionId::new(Uuid::new_v4()).unwrap(),
        session.binding().provider(),
        InteractionClientId::new("fresh-test").unwrap(),
        session.binding().revision(),
        InteractionAuthority::approval(
            InteractionActor::new(session.principal(), TokenId::new(2).unwrap()),
            InteractionDigest::new("a".repeat(64)).unwrap(),
        ),
    )
    .unwrap();
    let definition: OidcProviderDefinition = serde_json::from_value(json!({
        "issuer":"https://identity.example/realm", "client_id":"hubuum", "client_authentication":"client_secret_basic",
        "scopes":["openid"], "signing_algorithms":["RS256"], "admission":{"mode":"existing_only"},
        "membership":{"mode":"local_only","refresh_interval_seconds":300,"max_stale_seconds":900}
    })).unwrap();
    let context = ExternalAuthenticationContext::oidc(
        binding,
        IdentityScopeName::new("company").unwrap(),
        &definition,
    )
    .unwrap();
    let now = Utc::now();
    let proof = context
        .verified_oidc(
            definition.issuer().as_str(),
            ExternalIdentityProfile::new(ExternalSubject::new("123").unwrap(), None, None).unwrap(),
            MembershipClaims::Absent,
            now,
        )
        .unwrap();
    FreshExternalAuthenticationRequest::new(context, proof.identity().clone(), now, 0)
        .unwrap()
        .verify(proof, Some(now), now)
        .unwrap()
}

#[rstest]
#[case::principal("principal")]
#[case::provider("provider")]
#[case::revision("revision")]
#[case::scope("scope")]
#[case::namespace("namespace")]
#[case::subject("subject")]
fn fresh_retention_rejects_an_unrelated_existing_session(#[case] changed: &str) {
    let binding = InteractionBinding::try_new(
        InteractionId::new(Uuid::new_v4()).unwrap(),
        InteractionProviderId::external(Uuid::new_v4()).unwrap(),
        InteractionClientId::new("test").unwrap(),
        ResourceRevision::INITIAL,
        InteractionAuthority::login(),
    )
    .unwrap();
    let retained: StorageRetainedAuthentication = serde_json::from_value(json!({
        "binding":binding, "scope":"company", "namespace":"https://identity.example/realm", "subject":"123",
        "credential":"encrypted-credential"
    })).unwrap();
    let session = StorageUpstreamSession::try_new(
        AuthSessionId::new(Uuid::new_v4()),
        PrincipalId::new(1).unwrap(),
        retained,
    )
    .unwrap();
    let proof = fresh(&session);
    let mut document = serde_json::to_value(&session).unwrap();
    match changed {
        "principal" => document["principal"] = json!(2),
        "provider" | "revision" => {
            let old = session.binding();
            document["binding"] = serde_json::to_value(
                InteractionBinding::try_new(
                    old.id(),
                    if changed == "provider" {
                        InteractionProviderId::external(Uuid::new_v4()).unwrap()
                    } else {
                        old.provider()
                    },
                    old.client().clone(),
                    if changed == "revision" {
                        ResourceRevision::new(2).unwrap()
                    } else {
                        old.revision()
                    },
                    old.authority().clone(),
                )
                .unwrap(),
            )
            .unwrap();
        }
        "scope" => document["scope"] = json!("another"),
        "namespace" => document["namespace"] = json!("https://other.example/realm"),
        "subject" => document["subject"] = json!("456"),
        _ => unreachable!(),
    }
    let other: StorageUpstreamSession = serde_json::from_value(document).unwrap();
    assert!(StorageUpstreamApproval::new(&other, proof).is_err());
}

#[rstest]
#[case::oidc("https://identity.example/realm")]
#[case::oauth(r#"["https://identity.example/user","/id","integer"]"#)]
fn retained_sessions_round_trip_both_namespace_formats(#[case] namespace: &str) {
    let binding = InteractionBinding::try_new(
        InteractionId::new(Uuid::new_v4()).unwrap(),
        InteractionProviderId::external(Uuid::new_v4()).unwrap(),
        InteractionClientId::new("test").unwrap(),
        ResourceRevision::INITIAL,
        InteractionAuthority::login(),
    )
    .unwrap();
    let retained: StorageRetainedAuthentication = serde_json::from_value(json!({
        "binding":binding, "scope":"company", "namespace":namespace, "subject":"123",
        "credential":"encrypted-credential"
    }))
    .unwrap();
    let session = StorageUpstreamSession::try_new(
        AuthSessionId::new(Uuid::new_v4()),
        PrincipalId::new(1).unwrap(),
        retained,
    )
    .unwrap();
    let document = serde_json::to_value(session).unwrap();
    assert_eq!(document["namespace"], namespace);
    let restored: StorageUpstreamSession = serde_json::from_value(document).unwrap();
    assert_eq!(restored.namespace(), namespace);
}
