use std::path::PathBuf;

use super::*;
use hubuum_auth_ldap::{LdapScopeConfig, LdapSearchScope};
use rstest::rstest;
use uuid::Uuid;

const CONFIG_SECRET: &str = "auth-config-secret";

struct TestAuthConfigFile {
    path: PathBuf,
}

impl TestAuthConfigFile {
    fn new(contents: &[u8]) -> Self {
        let path = std::env::temp_dir().join(format!("hubuum-auth-config-{}.toml", Uuid::new_v4()));
        std::fs::write(&path, contents).expect("test auth config should be written");
        Self { path }
    }

    fn with_len(len: u64) -> Self {
        let path = std::env::temp_dir().join(format!("hubuum-auth-config-{}.toml", Uuid::new_v4()));
        File::create(&path)
            .and_then(|file| file.set_len(len))
            .expect("sized test auth config should be created");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestAuthConfigFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn ldap_scope(scope: &str) -> ConfiguredLdapScope {
    ConfiguredLdapScope {
        ldap: LdapScopeConfig {
            scope: scope.to_string(),
            url: "ldap://ldap.example.org".to_string(),
            bind_dn: None,
            bind_password: None,
            bind_password_ref: None,
            connect_timeout_seconds: 5,
            operation_timeout_seconds: 10,
            user_base_dn: "ou=people,dc=example,dc=org".to_string(),
            user_filter: "(uid={username})".to_string(),
            user_scope: LdapSearchScope::Subtree,
            username_attribute: "uid".to_string(),
            subject_attribute: "dn".to_string(),
            display_name_attribute: Some("cn".to_string()),
            email_attribute: Some("mail".to_string()),
            group_attributes: Vec::new(),
            group_filters: Vec::new(),
            group_rules: Vec::new(),
        },
        refresh_ttl_seconds: Some(300),
        max_stale_seconds: Some(3600),
    }
}

#[rstest]
#[case::syntax(concat!("[[ldap]]\n", "bind_password = \"auth-config-secret\n"))]
#[case::type_mismatch(concat!(
    "[[ldap]]\n",
    "connect_timeout_seconds = \"auth-config-secret\"\n"
))]
fn auth_config_parse_errors_do_not_disclose_source(#[case] source: &str) {
    let error = parse_auth_config_source(Path::new("auth.toml"), source)
        .expect_err("test auth config should be rejected");
    let message = error.to_string();

    assert!(!message.contains(CONFIG_SECRET));
    assert!(message.contains(" at line "));
    assert!(message.contains(", column "));
    assert!(message.contains("source content omitted"));
}

#[test]
fn auth_config_source_positions_count_unicode_characters() {
    let source = "øx = true";
    let x_offset = source.find('x').unwrap();

    assert_eq!(source_position(source, x_offset), (1, 2));
}

#[test]
fn auth_config_reader_rejects_oversized_files() {
    let file = TestAuthConfigFile::with_len(MAX_AUTH_CONFIG_BYTES as u64 + 1);

    let error =
        read_auth_config_source(file.path()).expect_err("oversized auth config should be rejected");

    assert_eq!(error, auth_config_size_error(file.path()));
}

#[test]
fn auth_config_reader_rejects_invalid_utf8() {
    let file = TestAuthConfigFile::new(&[0xff]);

    let error =
        read_auth_config_source(file.path()).expect_err("non-UTF-8 auth config should be rejected");

    assert_eq!(
        error,
        ApiError::InternalServerError(format!(
            "Auth config {:?} must contain valid UTF-8",
            file.path()
        ))
    );
}

#[cfg(unix)]
#[test]
fn auth_config_reader_rejects_non_regular_files() {
    let path = Path::new("/dev/null");

    let error =
        read_auth_config_source(path).expect_err("non-regular auth config should be rejected");

    assert_eq!(
        error,
        ApiError::InternalServerError(
            "Auth config \"/dev/null\" must be a regular file".to_string()
        )
    );
}

fn timestamp() -> NaiveDateTime {
    chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap()
}

fn refresh_ttl(seconds: i64) -> RefreshTtl {
    RefreshTtl::new(seconds).unwrap()
}

fn refresh_policy() -> RefreshPolicy {
    RefreshPolicy::new(300, 3600).unwrap()
}

#[test]
fn refresh_lock_registry_reuses_live_principal_locks() {
    let registry = RefreshLockRegistry::default();
    let first = registry.lock_for(42);
    let second = registry.lock_for(42);

    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(registry.len(), 1);
}

#[test]
fn refresh_lock_registry_reclaims_dropped_principal_locks() {
    let registry = RefreshLockRegistry::default();
    let dropped = registry.lock_for(42);
    drop(dropped);

    let live = registry.lock_for(7);

    assert_eq!(registry.len(), 1);
    assert!(Arc::ptr_eq(&live, &registry.lock_for(7)));
}

#[test]
fn provider_names_include_local_and_sorted_external_scopes() {
    let registry = AuthProviderRegistry::from_config(AuthProvidersConfig {
        ldap: vec![ldap_scope("zeta"), ldap_scope("alpha")],
    })
    .unwrap();

    assert_eq!(registry.provider_names(), vec!["local", "alpha", "zeta"]);
}

#[test]
fn registry_accepts_referenced_ldap_bind_password_without_resolving_it_at_startup() {
    let mut configured = ldap_scope("directory");
    configured.ldap.bind_dn = Some("cn=service,dc=example,dc=org".to_string());
    configured.ldap.bind_password_ref = Some("directory_bind".to_string());

    let registry = AuthProviderRegistry::from_config(AuthProvidersConfig {
        ldap: vec![configured],
    })
    .unwrap();

    assert_eq!(registry.provider_names(), vec!["local", "directory"]);
}

#[test]
fn recent_success_keeps_external_state_fresh() {
    let current = timestamp();
    let success = current - chrono::Duration::seconds(299);

    assert_eq!(
        refresh_status_at(current, Some(success), Some(success), refresh_ttl(300)),
        RefreshStatus::Fresh
    );
}

#[test]
fn failed_refresh_enters_retry_backoff() {
    let current = timestamp();
    let success = current - chrono::Duration::seconds(600);
    let failed_attempt = current - chrono::Duration::seconds(1);

    assert_eq!(
        refresh_status_at(
            current,
            Some(success),
            Some(failed_attempt),
            refresh_ttl(300)
        ),
        RefreshStatus::Backoff
    );
}

#[test]
fn refresh_is_due_after_retry_backoff_expires() {
    let current = timestamp();
    let success = current - chrono::Duration::seconds(900);
    let failed_attempt = current - chrono::Duration::seconds(300);

    assert_eq!(
        refresh_status_at(
            current,
            Some(success),
            Some(failed_attempt),
            refresh_ttl(300)
        ),
        RefreshStatus::Due
    );
}

#[test]
fn successful_attempt_timestamp_does_not_trigger_backoff() {
    let current = timestamp();
    let success = current - chrono::Duration::seconds(300);

    assert_eq!(
        refresh_status_at(current, Some(success), Some(success), refresh_ttl(300)),
        RefreshStatus::Due
    );
}

#[test]
fn future_success_timestamp_requires_external_refresh() {
    let current = timestamp();
    let future_success = current + chrono::Duration::seconds(1);

    assert_eq!(
        refresh_status_at(current, Some(future_success), None, refresh_ttl(300)),
        RefreshStatus::Due
    );
}

#[test]
fn future_attempt_timestamp_does_not_create_retry_backoff() {
    let current = timestamp();
    let future_attempt = current + chrono::Duration::seconds(1);

    assert_eq!(
        refresh_status_at(current, None, Some(future_attempt), refresh_ttl(300)),
        RefreshStatus::Due
    );
}

#[test]
fn retry_backoff_rejects_cache_beyond_max_stale() {
    let current = timestamp();
    let state = ExternalUserState {
        identity_scope: "directory".to_string(),
        refresh_request: ExternalUserRefreshRequest::new("alice", "subject").unwrap(),
        last_sync_attempted_at: Some(current - chrono::Duration::seconds(1)),
        last_sync_success_at: Some(current - chrono::Duration::seconds(3600)),
        refresh_policy: refresh_policy(),
    };

    assert!(matches!(
        cached_external_state_result_at(current, &state),
        Err(ApiError::ServiceUnavailable(_))
    ));
}

#[test]
fn refresh_policy_rejects_a_non_positive_ttl() {
    let error = RefreshPolicy::new(0, 3600).err().unwrap();

    assert!(matches!(error, ApiError::BadRequest(_)));
    assert_eq!(
        error.to_string(),
        "ldap refresh_ttl_seconds must be positive"
    );
}

#[test]
fn refresh_policy_rejects_a_non_positive_max_stale_window() {
    let error = RefreshPolicy::new(300, 0).err().unwrap();

    assert!(matches!(error, ApiError::BadRequest(_)));
    assert_eq!(error.to_string(), "ldap max_stale_seconds must be positive");
}

#[rstest::rstest]
#[case::refresh_ttl(i64::MAX, 3600, "ldap refresh_ttl_seconds is too large")]
#[case::max_stale(300, i64::MAX, "ldap max_stale_seconds is too large")]
fn refresh_policy_rejects_unrepresentable_durations(
    #[case] refresh_ttl_seconds: i64,
    #[case] max_stale_seconds: i64,
    #[case] expected: &str,
) {
    let error = RefreshPolicy::new(refresh_ttl_seconds, max_stale_seconds)
        .err()
        .unwrap();

    assert!(matches!(error, ApiError::BadRequest(_)));
    assert_eq!(error.to_string(), expected);
}

#[test]
fn refresh_policy_rejects_a_stale_window_below_the_refresh_ttl() {
    let error = RefreshPolicy::new(300, 299).err().unwrap();

    assert!(matches!(error, ApiError::BadRequest(_)));
    assert_eq!(
        error.to_string(),
        "ldap max_stale_seconds must be greater than or equal to refresh_ttl_seconds"
    );
}

#[test]
fn external_user_state_rejects_an_empty_provider_subject() {
    let state = ExternalPrincipalState {
        identity_scope: "directory".to_string(),
        username: "alice".to_string(),
        external_subject: String::new(),
        last_sync_attempted_at: None,
        last_sync_success_at: None,
    };

    let error = ExternalUserState::new(state, refresh_policy())
        .err()
        .unwrap();

    assert!(matches!(error, ApiError::InternalServerError(_)));
    assert_eq!(
        error.to_string(),
        "Invalid external identity state: external subject must not be empty"
    );
}

fn runtime_request(
    configuration: serde_json::Value,
    enabled: bool,
    adopt_deployment: bool,
) -> RuntimeAuthenticationProviderRequest {
    RuntimeAuthenticationProviderRequest::Runtime {
        name: "corporate".to_string(),
        kind: crate::models::runtime_settings::RuntimeAuthenticationProviderKind::Ldap,
        enabled,
        configuration,
        adopt_deployment,
    }
}

fn existing_runtime_provider() -> StorageAuthenticationProviders {
    StorageAuthenticationProviders::try_new(vec![
        StorageAuthenticationProvider::try_new(
            "corporate",
            StorageAuthenticationProviderKind::Ldap,
            serde_json::json!({
                "scope": "corporate",
                "runtime_bind_password_envelope": {
                    "ciphertext": "old-envelope"
                }
            }),
        )
        .unwrap(),
    ])
    .unwrap()
}

#[rstest]
#[case::dotted_scope("scope", serde_json::json!("corp.example.org"), true)]
#[case::unicode_scope("scope", serde_json::json!("Bedrift Øst"), true)]
#[case::long_scope("scope", serde_json::json!("x".repeat(129)), true)]
#[case::empty_scope("scope", serde_json::json!(""), false)]
#[case::padded_scope("scope", serde_json::json!(" corporate "), false)]
#[case::reserved_scope("scope", serde_json::json!("local"), false)]
#[case::url_scheme("url", serde_json::json!("https://ldap.example.org"), false)]
#[case::embedded_credentials("url", serde_json::json!("ldap://user:pass@ldap.example.org"), false)]
#[case::empty_filter("user_filter", serde_json::json!(""), false)]
#[case::filter_placeholder("user_filter", serde_json::json!("(uid=alice)"), false)]
#[case::connect_timeout("connect_timeout_seconds", serde_json::json!(0), false)]
#[case::operation_timeout("operation_timeout_seconds", serde_json::json!(0), false)]
#[case::incomplete_bind("bind_dn", serde_json::json!("cn=service,dc=example,dc=org"), false)]
#[case::refresh_ttl("refresh_ttl_seconds", serde_json::json!(0), false)]
#[case::stale_before_refresh("max_stale_seconds", serde_json::json!(1), false)]
#[tokio::test]
async fn deployment_and_runtime_share_provider_validation(
    #[case] field: &str,
    #[case] value: serde_json::Value,
    #[case] accepted: bool,
) {
    let mut configuration = serde_json::to_value(ldap_scope("corporate")).unwrap();
    configuration[field] = value;
    let configured: ConfiguredLdapScope = serde_json::from_value(configuration.clone()).unwrap();
    let name = configured.ldap.scope.clone();
    let deployment = AuthProvidersConfig {
        ldap: vec![configured],
    };
    let deployment_accepted = AuthProviderRegistry::from_config(deployment.clone()).is_ok();
    let runtime_accepted = protect_runtime_authentication_settings(
        vec![RuntimeAuthenticationProviderRequest::Runtime {
            name,
            kind: crate::models::runtime_settings::RuntimeAuthenticationProviderKind::Ldap,
            enabled: false,
            configuration,
            adopt_deployment: true,
        }],
        None,
        &deployment,
    )
    .await
    .is_ok();

    assert_eq!([deployment_accepted, runtime_accepted], [accepted; 2]);
}

#[rstest]
#[case::enabled(true)]
#[case::disabled(false)]
#[tokio::test]
async fn deployment_activation_survives_reload(
    #[case] enabled: bool,
    #[values("corporate", "corp.example.org", "Bedrift Øst")] name: &str,
) {
    let configured = StorageAuthenticationProviders::try_new(vec![
        StorageAuthenticationProvider::deployment(
            name,
            StorageAuthenticationProviderKind::Ldap,
            enabled,
        )
        .unwrap(),
    ])
    .unwrap();
    let value = hubuum_storage_core::StorageRuntimeSettingsValue::Authentication(configured);
    let reloaded = hubuum_storage_core::StorageRuntimeSettingsValue::from_document(
        hubuum_storage_core::StorageRuntimeSettingsGroup::Authentication,
        &value.to_document(),
    )
    .unwrap();
    let hubuum_storage_core::StorageRuntimeSettingsValue::Authentication(reloaded) = reloaded
    else {
        panic!("expected authentication settings");
    };
    let registry = AuthProviderRegistry::from_runtime_providers(
        &reloaded,
        &AuthProvidersConfig {
            ldap: vec![ldap_scope(name)],
        },
    )
    .await
    .unwrap();
    assert_eq!(registry.provider(name).is_ok(), enabled);
}

#[rstest]
#[case::implicit(false)]
#[case::explicit(true)]
#[tokio::test]
async fn deployment_definition_requires_explicit_adoption(
    #[case] adopt: bool,
    #[values("corporate", "corp.example.org", "Bedrift Øst")] name: &str,
) {
    let deployment = AuthProvidersConfig {
        ldap: vec![ldap_scope(name)],
    };
    let result = protect_runtime_authentication_settings(
        vec![RuntimeAuthenticationProviderRequest::Runtime {
            name: name.to_string(),
            kind: crate::models::runtime_settings::RuntimeAuthenticationProviderKind::Ldap,
            enabled: false,
            configuration: serde_json::to_value(&deployment.ldap[0]).unwrap(),
            adopt_deployment: adopt,
        }],
        None,
        &deployment,
    )
    .await;
    assert_eq!(result.is_ok(), adopt);
}

#[tokio::test]
async fn omitted_deployment_provider_stays_disabled_under_runtime_ownership() {
    let registry = AuthProviderRegistry::from_runtime_providers(
        &StorageAuthenticationProviders::try_new(Vec::new()).unwrap(),
        &AuthProvidersConfig {
            ldap: vec![ldap_scope("corporate")],
        },
    )
    .await
    .unwrap();
    assert_eq!(registry.provider_names(), vec![LOCAL_IDENTITY_SCOPE]);
    assert!(
        registry
            .provider_statuses()
            .iter()
            .any(|provider| provider.name == "corporate"
                && !provider.enabled
                && !provider.definition_mutable)
    );
}

#[tokio::test]
async fn disabled_provider_rejects_even_fresh_cached_external_identity() {
    let registry = AuthProviderRegistry::from_runtime_providers(
        &StorageAuthenticationProviders::try_new(vec![
            StorageAuthenticationProvider::deployment(
                "corporate",
                StorageAuthenticationProviderKind::Ldap,
                false,
            )
            .unwrap(),
        ])
        .unwrap(),
        &AuthProvidersConfig {
            ldap: vec![ldap_scope("corporate")],
        },
    )
    .await
    .unwrap();
    let state = ExternalPrincipalState {
        identity_scope: "corporate".to_string(),
        username: "alice".to_string(),
        external_subject: "stable-subject".to_string(),
        last_sync_attempted_at: Some(now()),
        last_sync_success_at: Some(now()),
    };
    assert!(matches!(
        ExternalUserState::from_registry(state, &registry),
        Err(ApiError::Unauthorized(_))
    ));
}

#[tokio::test]
async fn disabled_runtime_provider_does_not_require_its_encryption_key() {
    let configured = StorageAuthenticationProviders::try_new(
        existing_runtime_provider()
            .providers()
            .iter()
            .cloned()
            .map(|provider| provider.with_enabled(false))
            .collect(),
    )
    .unwrap();
    let registry = AuthProviderRegistry::from_runtime_providers(
        &configured,
        &AuthProvidersConfig { ldap: Vec::new() },
    )
    .await
    .unwrap();
    registry.preflight().await.unwrap();
    assert_eq!(registry.provider_names(), vec![LOCAL_IDENTITY_SCOPE]);
}

#[rstest]
#[case::top_level(serde_json::json!({"group_filter": "admins"}))]
#[case::nested(serde_json::json!({
    "group_rules": [{"pattern": "admins", "name": "admins", "unexpected": true}]
}))]
#[tokio::test]
async fn runtime_ldap_rejects_unknown_fields(#[case] extra: serde_json::Value) {
    let mut configuration = serde_json::json!({
        "scope": "corporate",
        "url": "ldap://ldap.example.org",
        "user_base_dn": "dc=example,dc=org",
        "user_filter": "(uid={username})"
    });
    configuration
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    let provider = StorageAuthenticationProvider::try_new(
        "corporate",
        StorageAuthenticationProviderKind::Ldap,
        configuration,
    )
    .unwrap();

    let error = runtime_provider_configuration(&provider).await.unwrap_err();
    assert!(matches!(error, ApiError::BadRequest(_)));
    assert!(error.to_string().contains("unrecognized fields"));
}

#[tokio::test]
async fn runtime_ldap_accepts_known_flattened_and_nested_fields() {
    let provider = StorageAuthenticationProvider::try_new(
        "corporate",
        StorageAuthenticationProviderKind::Ldap,
        serde_json::json!({
            "scope": "corporate",
            "url": "ldap://ldap.example.org",
            "user_base_dn": "dc=example,dc=org",
            "user_filter": "(uid={username})",
            "group_filters": ["admins"],
            "group_rules": [{"pattern": "admins", "name": "admins"}],
            "refresh_ttl_seconds": 30
        }),
    )
    .unwrap();

    let configured = runtime_provider_configuration(&provider).await.unwrap();
    assert_eq!(configured.ldap.group_filters, ["admins"]);
    assert_eq!(configured.ldap.group_rules[0].name, "admins");
    assert_eq!(configured.refresh_ttl_seconds, Some(30));
}

#[tokio::test]
async fn omitted_runtime_password_preserves_the_existing_envelope() {
    let current = existing_runtime_provider();
    let providers = protect_runtime_authentication_settings(
        vec![runtime_request(
            serde_json::json!({"scope": "corporate"}),
            true,
            false,
        )],
        Some(&current),
        &AuthProvidersConfig { ldap: Vec::new() },
    )
    .await
    .unwrap();
    assert_eq!(
        providers.providers()[0].configuration().and_then(
            |configuration| configuration.get(RUNTIME_AUTHENTICATION_PASSWORD_ENVELOPE_FIELD)
        ),
        current.providers()[0].configuration().and_then(
            |configuration| configuration.get(RUNTIME_AUTHENTICATION_PASSWORD_ENVELOPE_FIELD)
        )
    );
}

#[tokio::test]
async fn explicit_null_runtime_password_clears_the_existing_envelope() {
    let current = existing_runtime_provider();
    let providers = protect_runtime_authentication_settings(
        vec![runtime_request(
            serde_json::json!({"scope": "corporate", "bind_password": null}),
            true,
            false,
        )],
        Some(&current),
        &AuthProvidersConfig { ldap: Vec::new() },
    )
    .await
    .unwrap();
    assert!(
        providers.providers()[0]
            .configuration()
            .and_then(
                |configuration| configuration.get(RUNTIME_AUTHENTICATION_PASSWORD_ENVELOPE_FIELD)
            )
            .is_none()
    );
}
