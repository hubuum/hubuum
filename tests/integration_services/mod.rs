use hubuum_auth_core::{AuthProviderError, ExternalIdentityProvider, ExternalUserRefreshRequest};
use hubuum_auth_ldap::{LdapIdentityProvider, LdapScopeConfig};
use hubuum_event_sink_email::EmailSink;

use super::*;

fn ldap(endpoint: &str) -> LdapIdentityProvider {
    let config: LdapScopeConfig = serde_json::from_value(json!({
        "scope": "contract", "url": fixture(endpoint),
        "bind_dn": "cn=admin,dc=example,dc=test", "bind_password": fixture("PASSWORD"),
        "connect_timeout_seconds": 2, "operation_timeout_seconds": 2,
        "user_base_dn": "dc=example,dc=test", "user_filter": "(uid={username})",
        "subject_attribute": "employeeNumber", "display_name_attribute": "cn",
        "email_attribute": "mail", "group_attributes": ["employeeType"],
        "group_rules": [{"pattern": "^(.+)$", "name": "$1", "key": "$1"}]
    }))
    .unwrap();
    LdapIdentityProvider::new(config).unwrap()
}

#[rstest]
#[case::ldaps("LDAP_URI")]
#[case::starttls("LDAP_STARTTLS_URI")]
#[tokio::test]
#[ignore = "requires the pinned OpenLDAP fixture"]
async fn ldap_authenticates_over_verified_tls(#[case] endpoint: &str) {
    let user = bounded(ldap(endpoint).authenticate("human", &fixture("PASSWORD")))
        .await
        .unwrap();
    assert_eq!(user.profile.subject, "stable-1");
    assert_eq!(user.profile.name, "human");
    assert_eq!(user.groups[0].name, "readers");
}

#[rstest]
#[case::wrong_password("human", "wrong-password")]
#[case::filter_injection("*)(uid=*)", "wrong-password")]
#[case::unknown_user("missing", "wrong-password")]
#[tokio::test]
#[ignore = "requires the pinned OpenLDAP fixture"]
async fn ldap_rejects_invalid_credentials(#[case] username: &str, #[case] password: &str) {
    let error = bounded(ldap("LDAP_URI").authenticate(username, password))
        .await
        .unwrap_err();
    assert!(matches!(error, AuthProviderError::AuthenticationFailed));
    assert!(!format!("{error:?}").contains(password));
}

#[rstest]
#[case::ldaps("LDAP_UNTRUSTED_URI")]
#[case::starttls("LDAP_UNTRUSTED_STARTTLS_URI")]
#[tokio::test]
#[ignore = "requires the pinned OpenLDAP fixture"]
async fn ldap_rejects_an_untrusted_ca(#[case] endpoint: &str) {
    let error = bounded(ldap(endpoint).authenticate("human", &fixture("PASSWORD")))
        .await
        .unwrap_err();
    assert!(matches!(error, AuthProviderError::Unavailable(_)));
    assert!(!format!("{error:?}").contains(&fixture("PASSWORD")));
}

#[rstest]
#[case::same_subject("stable-1", true)]
#[case::replaced_identity("other-subject", false)]
#[tokio::test]
#[ignore = "requires the pinned OpenLDAP fixture"]
async fn ldap_refresh_binds_the_original_subject(#[case] subject: &str, #[case] accepted: bool) {
    let request = ExternalUserRefreshRequest::new("human", subject).unwrap();
    let result = bounded(ldap("LDAP_URI").refresh_user(&request)).await;
    assert_eq!(result.is_ok(), accepted);
}

#[tokio::test]
#[ignore = "requires the pinned OpenLDAP fixture"]
async fn ldap_provider_recovers_after_restart() {
    let provider = ldap("LDAP_URI");
    let request = ExternalUserRefreshRequest::new("human", "stable-1").unwrap();
    bounded(provider.refresh_user(&request)).await.unwrap();
    restart_fixture("LDAP").await;
    timeout(Duration::from_secs(30), async {
        loop {
            if provider.refresh_user(&request).await.is_ok() {
                break;
            }
            sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("LDAP refresh must recover after service restart");
}

fn email_config(endpoint: &str, event: &EventEnvelope) -> Value {
    json!({"uri": fixture(endpoint), "from": "sender@example.test",
        "subject_template": event.event_id().to_string(),
        "body_template": "Contract notification"})
}

#[tokio::test]
#[ignore = "requires the pinned Mailpit fixture"]
async fn smtp_delivers_the_expected_message_over_authenticated_tls() {
    let event = envelope();
    bounded(EmailSink::default().deliver(
        &event,
        SinkDelivery::new(
            &email_config("SMTP_URI", &event),
            &json!({"recipients":["human@example.test"]}),
            Some(&secret()),
        ),
    ))
    .await
    .unwrap();
    let messages: Value = http()
        .get(format!("{}/api/v1/messages", fixture("SMTP_MANAGEMENT")))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let message = messages["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["Subject"] == event.event_id().to_string())
        .unwrap();
    assert_eq!(message["To"][0]["Address"], "human@example.test");
    let body: Value = http()
        .get(format!(
            "{}/api/v1/message/{}",
            fixture("SMTP_MANAGEMENT"),
            message["ID"].as_str().unwrap()
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        body["Text"].as_str().unwrap().trim(),
        "Contract notification"
    );
}

#[rstest]
#[case::temporary_recipient_failure("SMTP_TEMPORARY_URI", false)]
#[case::permanent_recipient_failure("SMTP_PERMANENT_URI", false)]
#[case::untrusted_ca("SMTP_UNTRUSTED_URI", false)]
#[case::wrong_password("SMTP_URI", true)]
#[tokio::test]
#[ignore = "requires the pinned Mailpit fixture"]
async fn smtp_reports_failed_delivery_without_credentials(
    #[case] endpoint: &str,
    #[case] wrong_password: bool,
) {
    let event = envelope();
    let secret = if wrong_password {
        SecretValue::new(b"invalid-fixture-password".to_vec()).unwrap()
    } else {
        secret()
    };
    let error = bounded(EmailSink::default().deliver(
        &event,
        SinkDelivery::new(
            &email_config(endpoint, &event),
            &json!({"recipients":["human@example.test"]}),
            Some(&secret),
        ),
    ))
    .await
    .unwrap_err();
    assert_eq!(error.to_string(), "Email SMTP delivery failed");
    assert!(!format!("{error:?}").contains(&fixture("PASSWORD")));
}

#[tokio::test]
#[ignore = "requires the pinned Mailpit fixture"]
async fn smtp_cached_transport_recovers_after_restart() {
    let sink = EmailSink::default();
    let event = envelope();
    let config = email_config("SMTP_URI", &event);
    let routing = json!({"recipients":["human@example.test"]});
    let secret = secret();
    bounded(sink.deliver(&event, SinkDelivery::new(&config, &routing, Some(&secret))))
        .await
        .unwrap();
    restart_fixture("SMTP").await;
    timeout(Duration::from_secs(30), async {
        loop {
            if sink
                .deliver(&event, SinkDelivery::new(&config, &routing, Some(&secret)))
                .await
                .is_ok()
            {
                break;
            }
            sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("cached SMTP transport must recover after service restart");
}
