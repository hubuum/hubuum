use hubuum_domain::{
    InteractionAuthority, InteractionBinding, InteractionClientId, InteractionId,
    InteractionProviderId, ResourceRevision,
};
use hubuum_storage_core::{AuthSessionId, StorageRetainedAuthentication, StorageUpstreamSession};
use rstest::rstest;
use serde_json::json;
use uuid::Uuid;

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
