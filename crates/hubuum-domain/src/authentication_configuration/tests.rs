use super::*;
use rstest::rstest;
use serde_json::{Value, json};

fn oidc() -> Value {
    json!({"issuer":"https://identity.example/tenant", "client_id":"hubuum", "client_authentication":"client_secret_basic", "scopes":["openid"], "signing_algorithms":["RS256"], "membership":{"mode":"local_only","refresh_interval_seconds":300,"max_stale_seconds":900}, "admission":{"mode":"existing_only"}})
}

#[rstest]
#[case("https://identity.example/tenant", true)]
#[case("http://identity.example", false)]
#[case("https://user:secret@identity.example", false)]
#[case("https://identity.example?secret=x", false)]
#[case("https://identity.example#fragment", false)]
#[case(" https://identity.example", false)]
#[case("file:///etc/passwd", false)]
fn endpoints_require_unambiguous_verified_https(#[case] value: &str, #[case] valid: bool) {
    assert_eq!(
        AuthenticationEndpoint::try_from(value.to_owned()).is_ok(),
        valid
    );
}

#[test]
fn oidc_definition_round_trips_without_changing_issuer() {
    let value = oidc();
    let definition: OidcProviderDefinition = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(definition).unwrap(), value);
}

#[rstest]
#[case("client_id", json!(""))]
#[case("client_authentication", json!("client_secret_post"))]
#[case("scopes", json!(["email"]))]
#[case("scopes", json!(["openid", "openid"]))]
#[case("signing_algorithms", json!(["none"]))]
#[case("signing_algorithms", json!(["RS256", "RS256"]))]
#[case("client_secret", json!("must-not-persist"))]
#[case("membership", json!({"mode":"local_only","refresh_interval_seconds":301,"max_stale_seconds":900}))]
#[case("membership", json!({"mode":"local_only","refresh_interval_seconds":300,"max_stale_seconds":299}))]
#[case("membership", json!({"mode":"provider_groups","refresh_interval_seconds":300,"max_stale_seconds":900,"claim_source":"profile","claim_pointer":"/groups","key_mode":"opaque"}))]
fn invalid_oidc_configuration_cannot_cross_the_domain_boundary(
    #[case] field: &str,
    #[case] replacement: Value,
) {
    let mut value = oidc();
    value[field] = replacement;
    assert!(serde_json::from_value::<OidcProviderDefinition>(value).is_err());
}

#[rstest]
#[case(100, 1000, true)]
#[case(0, 1000, false)]
#[case(1001, 1000, false)]
#[case(100, 60001, false)]
fn remote_authorization_timeouts_are_bounded(
    #[case] connect: u64,
    #[case] request: u64,
    #[case] valid: bool,
) {
    let value = json!({"url":"https://policy.example", "connect_timeout_ms":connect, "request_timeout_ms":request, "trust_profile_id":"system"});
    assert_eq!(
        serde_json::from_value::<AuthorizationConnection>(value).is_ok(),
        valid
    );
}

#[rstest]
#[case("/groups")]
#[case("/namespace~1groups")]
#[case("/namespace~0groups")]
fn membership_pointer_accepts_rfc6901_escapes(#[case] pointer: &str) {
    assert!(validate_pointer(pointer).is_ok());
}
#[rstest]
#[case("groups")]
#[case("/groups~")]
#[case("/groups~2")]
fn membership_pointer_rejects_ambiguous_paths(#[case] pointer: &str) {
    assert!(validate_pointer(pointer).is_err());
}
