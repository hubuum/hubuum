use std::sync::OnceLock;
use std::time::Instant;

use hubuum_secrets::{
    DEFAULT_MAX_SECRET_BYTES, EnvironmentProvider, FileProvider, FileSymlinkPolicy, ResolvedSecret,
    ResolvedSecretGroup, SecretError, SecretErrorKind, SecretName, SecretProviderKind, SecretRef,
    SecretResolver, SecretVersion, SecretVersionSelector,
};

use crate::config::{CommandLineDatabaseUrl, SecretSourceOptions, SecretSourceSettings};

static APPLICATION_SECRETS: OnceLock<Result<ApplicationSecrets, SecretError>> = OnceLock::new();

struct ConsumerSecrets {
    provider_kind: SecretProviderKind,
    provider_label: &'static str,
    consumer_label: &'static str,
    resolver: SecretResolver,
}

impl ConsumerSecrets {
    async fn resolve(&self, alias: &str) -> Result<ResolvedSecret, SecretError> {
        self.resolve_version(alias, None).await
    }

    async fn resolve_version(
        &self,
        alias: &str,
        version: Option<&SecretVersion>,
    ) -> Result<ResolvedSecret, SecretError> {
        let started = Instant::now();
        crate::observability::metrics::secret_source_identity(self.provider_label);
        let result = match SecretName::new(alias) {
            Ok(name) => {
                let selector = version.map_or(SecretVersionSelector::Latest, |version| {
                    SecretVersionSelector::Exact(version.clone())
                });
                let reference = SecretRef::new(self.provider_kind, name).with_version(selector);
                self.resolver.resolve(&reference).await
            }
            Err(error) => Err(error),
        };
        crate::observability::metrics::secret_resolution_finished(
            self.provider_label,
            self.consumer_label,
            result
                .as_ref()
                .map(|_| "ok")
                .unwrap_or_else(|error| error_outcome(error.kind())),
            started.elapsed(),
        );
        result
    }

    async fn resolve_group(&self, aliases: &[String]) -> Result<ResolvedSecretGroup, SecretError> {
        let started = Instant::now();
        crate::observability::metrics::secret_source_identity(self.provider_label);
        let result = aliases
            .iter()
            .map(|alias| {
                SecretName::new(alias).map(|name| SecretRef::new(self.provider_kind, name))
            })
            .collect::<Result<Vec<_>, _>>();
        let result = match result {
            Ok(references) => self.resolver.resolve_group(&references).await,
            Err(error) => Err(error),
        };
        crate::observability::metrics::secret_resolution_finished(
            self.provider_label,
            self.consumer_label,
            result
                .as_ref()
                .map(|_| "ok")
                .unwrap_or_else(|error| error_outcome(error.kind())),
            started.elapsed(),
        );
        result
    }
}

struct ApplicationSecrets {
    settings: SecretSourceSettings,
    database: ConsumerSecrets,
    migration_database: ConsumerSecrets,
    event_sink: ConsumerSecrets,
    remote: ConsumerSecrets,
    ldap: ConsumerSecrets,
    oidc: ConsumerSecrets,
    oauth: ConsumerSecrets,
    runtime_settings: ConsumerSecrets,
    token: ConsumerSecrets,
}

impl ApplicationSecrets {
    async fn invalidate_security_candidates(&self) {
        for consumer in [&self.ldap, &self.oidc, &self.oauth, &self.runtime_settings] {
            consumer
                .resolver
                .invalidate_provider(&consumer.provider_kind)
                .await;
        }
    }

    fn from_environment() -> Result<Self, SecretError> {
        Self::new(SecretSourceOptions::from_environment()?.settings()?)
    }

    fn new(settings: SecretSourceSettings) -> Result<Self, SecretError> {
        Ok(Self {
            database: consumer_resolver(
                &settings,
                "",
                Some(("url", "HUBUUM_DATABASE_URL")),
                "database",
                "database",
            )?,
            migration_database: consumer_resolver(
                &settings,
                "",
                Some(("migration-url", "HUBUUM_MIGRATION_DATABASE_URL")),
                "database",
                "database",
            )?,
            event_sink: consumer_resolver(
                &settings,
                "HUBUUM_EVENT_SINK_SECRET_",
                None,
                "event-sink",
                "event_sink",
            )?,
            remote: consumer_resolver(
                &settings,
                "HUBUUM_REMOTE_SECRET_",
                None,
                "remote",
                "remote_target",
            )?,
            ldap: consumer_resolver(&settings, "HUBUUM_LDAP_SECRET_", None, "ldap", "ldap")?,
            oidc: consumer_resolver(&settings, "HUBUUM_OIDC_SECRET_", None, "oidc", "oidc")?,
            oauth: consumer_resolver(&settings, "HUBUUM_OAUTH_SECRET_", None, "oauth", "oauth")?,
            runtime_settings: consumer_resolver(
                &settings,
                "HUBUUM_RUNTIME_SETTINGS_KEY_",
                None,
                "runtime-settings",
                "runtime_settings",
            )?,
            token: consumer_resolver(
                &settings,
                "HUBUUM_TOKEN_HASH_KEY_",
                Some(("key", "HUBUUM_TOKEN_HASH_KEY")),
                "token",
                "token_hash",
            )?,
            settings,
        })
    }
}

fn consumer_resolver(
    settings: &SecretSourceSettings,
    environment_prefix: &str,
    exact_environment_key: Option<(&str, &str)>,
    file_prefix: &str,
    consumer_label: &'static str,
) -> Result<ConsumerSecrets, SecretError> {
    let provider_kind = match settings {
        SecretSourceSettings::Environment => SecretProviderKind::environment(),
        SecretSourceSettings::File(_) => SecretProviderKind::file(),
    };
    let builder = SecretResolver::builder();
    let resolver = match settings {
        SecretSourceSettings::Environment => {
            let mut provider = EnvironmentProvider::new(environment_prefix)?;
            if let Some((alias, environment_name)) = exact_environment_key {
                provider = provider.mapping(SecretName::new(alias)?, environment_name)?;
            }
            builder.provider(provider)?.build()
        }
        SecretSourceSettings::File(root) => builder
            .provider(
                FileProvider::builder(root)
                    .path_prefix(file_prefix)
                    .symlink_policy(FileSymlinkPolicy::AllowWithinRoot)
                    .build()?,
            )?
            .build(),
    };
    Ok(ConsumerSecrets {
        provider_kind,
        provider_label: match settings {
            SecretSourceSettings::Environment => "environment",
            SecretSourceSettings::File(_) => "file",
        },
        consumer_label,
        resolver,
    })
}

fn error_outcome(kind: SecretErrorKind) -> &'static str {
    match kind {
        SecretErrorKind::NotFound => "not_found",
        SecretErrorKind::PermissionDenied => "permission_denied",
        SecretErrorKind::TooLarge => "too_large",
        SecretErrorKind::UnsafePath => "unsafe_path",
        SecretErrorKind::ChangedDuringRead => "changed_during_read",
        SecretErrorKind::InvalidReference
        | SecretErrorKind::InvalidProviderConfiguration
        | SecretErrorKind::InvalidValue
        | SecretErrorKind::UnsupportedVersion => "invalid",
        SecretErrorKind::ProviderNotConfigured
        | SecretErrorKind::Timeout
        | SecretErrorKind::Unavailable
        | SecretErrorKind::Internal => "unavailable",
    }
}

fn configured() -> Result<&'static ApplicationSecrets, SecretError> {
    APPLICATION_SECRETS
        .get_or_init(ApplicationSecrets::from_environment)
        .as_ref()
        .map_err(Clone::clone)
}

pub(crate) fn initialize(options: &SecretSourceOptions) -> Result<(), SecretError> {
    let settings = options.settings()?;
    let configured = APPLICATION_SECRETS
        .get_or_init(|| ApplicationSecrets::new(settings.clone()))
        .as_ref()
        .map_err(Clone::clone)?;
    if configured.settings != settings {
        return Err(SecretError::new(
            SecretErrorKind::InvalidProviderConfiguration,
            "Secret sources were already initialized with different settings",
        ));
    }
    Ok(())
}

pub(crate) async fn resolve_event_sink_secret(alias: &str) -> Result<ResolvedSecret, SecretError> {
    configured()?.event_sink.resolve(alias).await
}

#[derive(Clone, Copy)]
pub(crate) enum DatabaseCredential {
    Runtime,
    Migration,
}

impl ApplicationSecrets {
    async fn database_url(
        &self,
        credential: DatabaseCredential,
        configured_url: Option<&str>,
        command_line: Option<&CommandLineDatabaseUrl>,
    ) -> Result<Option<String>, SecretError> {
        if command_line.is_some() || self.settings == SecretSourceSettings::Environment {
            let started = Instant::now();
            let result = if let Some(value) = command_line {
                if value.as_str().trim().is_empty() {
                    Ok(None)
                } else {
                    database_url_value(value.as_str()).map(Some)
                }
            } else {
                configured_url
                    .filter(|value| !value.trim().is_empty())
                    .map(database_url_value)
                    .transpose()
            };
            crate::observability::metrics::secret_source_identity(self.database.provider_label);
            crate::observability::metrics::secret_resolution_finished(
                self.database.provider_label,
                "database",
                match &result {
                    Ok(Some(_)) => "ok",
                    Ok(None) => "not_found",
                    Err(error) => error_outcome(error.kind()),
                },
                started.elapsed(),
            );
            return result;
        }
        let result = match credential {
            DatabaseCredential::Runtime => self.database.resolve("url").await,
            DatabaseCredential::Migration => self.migration_database.resolve("migration-url").await,
        };
        match result {
            Ok(secret) => database_url_value(secret.value().expose_utf8()?).map(Some),
            Err(error) if error.kind() == SecretErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }
}

fn database_url_value(value: &str) -> Result<String, SecretError> {
    if value.trim().is_empty() {
        return Err(SecretError::new(
            SecretErrorKind::InvalidValue,
            "Database URL must not be empty",
        ));
    }
    if value.len() > DEFAULT_MAX_SECRET_BYTES {
        return Err(SecretError::new(
            SecretErrorKind::TooLarge,
            "Database URL exceeds the secret size limit",
        ));
    }
    Ok(value.to_owned())
}

pub(crate) async fn resolve_database_url(
    credential: DatabaseCredential,
    configured_url: Option<&str>,
    command_line: Option<&CommandLineDatabaseUrl>,
) -> Result<Option<String>, SecretError> {
    configured()?
        .database_url(credential, configured_url, command_line)
        .await
}

pub(crate) async fn resolve_remote_secret(alias: &str) -> Result<ResolvedSecret, SecretError> {
    configured()?.remote.resolve(alias).await
}

pub(crate) async fn resolve_ldap_secret(alias: &str) -> Result<ResolvedSecret, SecretError> {
    configured()?.ldap.resolve(alias).await
}

pub(crate) async fn resolve_runtime_settings_key(
    alias: &str,
    version: Option<&SecretVersion>,
) -> Result<ResolvedSecret, SecretError> {
    configured()?
        .runtime_settings
        .resolve_version(alias, version)
        .await
}

pub(crate) fn resolve_token_hash_key() -> Result<Vec<u8>, SecretError> {
    let secrets = configured()?;
    let resolved = futures::executor::block_on(secrets.token.resolve("key"))?;
    if secrets.settings == SecretSourceSettings::Environment {
        let trimmed = resolved.value().expose_utf8()?.trim();
        if trimmed.is_empty() {
            return Err(SecretError::new(
                SecretErrorKind::InvalidValue,
                "token hash key must not be empty or whitespace",
            ));
        }
        return Ok(trimmed.as_bytes().to_vec());
    }
    Ok(resolved.value().expose().to_vec())
}

pub(crate) fn resolve_token_hash_key_group(
    aliases: &[String],
) -> Result<Vec<ResolvedSecret>, SecretError> {
    let secrets = configured()?;
    let resolved = futures::executor::block_on(secrets.token.resolve_group(aliases))?;
    Ok(resolved.values().to_vec())
}

pub(crate) fn token_hash_secrets_are_text() -> Result<bool, SecretError> {
    Ok(configured()?.settings == SecretSourceSettings::Environment)
}

pub(crate) async fn resolve_browser_provider_secret(
    kind: hubuum_storage_core::StorageAuthenticationProviderKind,
    alias: &str,
) -> Result<ResolvedSecret, SecretError> {
    match kind {
        hubuum_storage_core::StorageAuthenticationProviderKind::Oidc => {
            configured()?.oidc.resolve(alias).await
        }
        hubuum_storage_core::StorageAuthenticationProviderKind::Oauth => {
            configured()?.oauth.resolve(alias).await
        }
        hubuum_storage_core::StorageAuthenticationProviderKind::Ldap => {
            configured()?.ldap.resolve(alias).await
        }
    }
}

/// Candidate approval/activation must observe source rotation rather than accept
/// an otherwise valid receipt against a still-cached superseded secret.
pub(crate) async fn refresh_security_candidate_secrets() -> Result<(), SecretError> {
    configured()?.invalidate_security_candidates().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "hubuum-application-secrets-{}-{}",
                std::process::id(),
                NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[rstest::rstest]
    #[case("ldap")]
    #[case("oidc")]
    #[case("oauth")]
    #[case("runtime-settings")]
    #[actix_rt::test]
    async fn security_candidate_checks_observe_rotation_before_cache_expiry(
        #[case] namespace: &str,
    ) {
        let directory = TestDirectory::new();
        fs::create_dir(directory.0.join(namespace)).unwrap();
        let path = directory.0.join(namespace).join("candidate-secret");
        fs::write(&path, b"first-value").unwrap();
        let secrets =
            ApplicationSecrets::new(SecretSourceSettings::File(directory.0.clone())).unwrap();
        let consumer = match namespace {
            "ldap" => &secrets.ldap,
            "oidc" => &secrets.oidc,
            "oauth" => &secrets.oauth,
            _ => &secrets.runtime_settings,
        };
        consumer.resolve("candidate-secret").await.unwrap();
        fs::write(&path, b"rotated-value").unwrap();
        secrets.invalidate_security_candidates().await;
        assert_eq!(
            consumer
                .resolve("candidate-secret")
                .await
                .unwrap()
                .value()
                .expose(),
            b"rotated-value"
        );
    }

    #[test]
    fn unavailable_authentication_provider_is_retried_at_same_revision() {
        use crate::permissions::{AppContext, LocalPermissionBackend};
        use crate::storage::StorageHandle;
        use hubuum_domain::ResourceRevision;
        use std::sync::Arc;

        const CHILD: &str = "RUNTIME_SETTINGS_RETRY_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let root = std::env::temp_dir().join(format!(
                "hubuum-runtime-auth-retry-{}",
                uuid::Uuid::new_v4()
            ));
            std::fs::create_dir(&root).unwrap();
            let run = |phase| {
                std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "secrets::tests::unavailable_authentication_provider_is_retried_at_same_revision",
                        "--nocapture",
                    ])
                    .env(CHILD, phase)
                    .env("HUBUUM_SECRET_SOURCE", "file")
                    .env("HUBUUM_SECRET_FILE_ROOT", &root)
                    .env("HUBUUM_RUNTIME_SETTINGS_ENCRYPTION_KEY_REF", "retry-key")
                    .status()
                    .unwrap()
            };
            let writer = run("writer");
            let reader = if writer.success() {
                std::fs::remove_file(root.join("runtime-settings").join("retry-key")).unwrap();
                Some(run("reader"))
            } else {
                None
            };
            std::fs::remove_dir_all(root).unwrap();
            assert!(writer.success());
            assert!(reader.unwrap().success());
            return;
        }

        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                use hubuum_storage_core::{
                    RuntimeSettingsStorage, StorageAuthenticationProvider,
                    StorageAuthenticationProviderKind, StorageAuthenticationProviders,
                    StorageRuntimeSettingsAssignment, StorageRuntimeSettingsUpdate,
                    StorageRuntimeSettingsValue,
                };
                use serde_json::json;

                let root =
                    std::path::PathBuf::from(std::env::var_os("HUBUUM_SECRET_FILE_ROOT").unwrap());
                let key_path = root.join("runtime-settings").join("retry-key");
                std::fs::create_dir_all(key_path.parent().unwrap()).unwrap();
                let key = [b'k'; 32];
                let config_path = root.join("provider.json");
                if std::env::var(CHILD).unwrap() == "writer" {
                    crate::observability::metrics::init().unwrap();
                    std::fs::write(&key_path, key).unwrap();
                    let config = json!({
                        "scope": "directory",
                        "url": "ldap://localhost",
                        "bind_dn": "cn=service,dc=example,dc=org",
                        "bind_password": "bind-password",
                        "user_base_dn": "dc=example,dc=org",
                        "user_filter": "(uid={username})"
                    });
                    let providers = crate::auth::protect_runtime_authentication_settings(
                        vec![crate::models::runtime_settings::RuntimeAuthenticationProviderRequest::Runtime {
                            provider_id: None,
                            name: "directory".to_string(),
                            kind: crate::models::runtime_settings::RuntimeAuthenticationProviderKind::Ldap,
                            configuration: config,
                            enabled: true,
                            adopt_deployment: false,
                        }],
                        None,
                        &crate::auth::AuthProvidersConfig { ldap: Vec::new() },
                    )
                    .await
                    .unwrap();
                    std::fs::write(
                        config_path,
                        serde_json::to_vec(providers.providers()[0].configuration().unwrap()).unwrap(),
                    )
                    .unwrap();
                    return;
                }
                let config = serde_json::from_slice(&std::fs::read(config_path).unwrap()).unwrap();
                let providers = StorageAuthenticationProviders::try_new(vec![
                    StorageAuthenticationProvider::try_new(
                        "directory",
                        StorageAuthenticationProviderKind::Ldap,
                        config,
                    )
                    .unwrap(),
                ])
                .unwrap();
                let storage = StorageHandle::memory();
                let revision = storage
                    .update_runtime_settings(StorageRuntimeSettingsUpdate::new(
                        StorageRuntimeSettingsAssignment::Managed(
                            StorageRuntimeSettingsValue::Authentication(providers),
                        ),
                        ResourceRevision::INITIAL,
                        crate::events::EventContext::system(),
                    ))
                    .await
                    .unwrap()
                    .record()
                    .revision();
                let context = AppContext::new(
                    storage.clone(),
                    Arc::new(LocalPermissionBackend::new(storage, "admin".to_string())),
                );
                context.refresh_runtime_settings().await.unwrap();
                assert_eq!(context.applied_authentication_revision(), None);
                assert!(
                    crate::auth::auth_provider_statuses()
                        .unwrap()
                        .iter().any(|provider| provider.name == "directory" && !provider.available)
                );

                std::fs::write(key_path, key).unwrap();
                context.refresh_runtime_settings().await.unwrap();
                assert_eq!(context.applied_authentication_revision(), Some(revision));
                assert!(
                    crate::auth::auth_provider_statuses()
                        .unwrap()
                        .iter().any(|provider| provider.name == "directory" && provider.available)
                );
            });
    }

    #[test]
    fn startup_uses_authoritative_authentication_settings_before_provider_scopes() {
        const CHILD: &str = "RUNTIME_AUTH_STARTUP_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let missing = std::env::temp_dir().join(format!(
                "hubuum-missing-auth-config-{}.toml",
                uuid::Uuid::new_v4()
            ));
            for ownership in ["managed", "deployment"] {
                let status = std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "secrets::tests::startup_uses_authoritative_authentication_settings_before_provider_scopes",
                        "--nocapture",
                    ])
                    .env(CHILD, ownership)
                    .env("HUBUUM_AUTH_CONFIG_PATH", &missing)
                    .status()
                    .unwrap();
                assert!(status.success(), "{ownership} child failed");
            }
            return;
        }

        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                use crate::permissions::{AppContext, LocalPermissionBackend};
                use crate::storage::StorageHandle;
                use hubuum_domain::ResourceRevision;
                use hubuum_storage_core::{
                    RuntimeSettingsStorage, StorageAuthenticationProvider,
                    StorageAuthenticationProviderKind, StorageAuthenticationProviders,
                    StorageRuntimeSettingsAssignment, StorageRuntimeSettingsUpdate,
                    StorageRuntimeSettingsValue,
                };
                use std::sync::Arc;

                let managed = std::env::var(CHILD).unwrap() == "managed";
                let storage = StorageHandle::memory();
                if managed {
                    let provider = StorageAuthenticationProvider::try_new(
                        "directory",
                        StorageAuthenticationProviderKind::Ldap,
                        serde_json::json!({
                            "scope": "directory",
                            "url": "ldap://localhost",
                            "user_base_dn": "dc=example,dc=org",
                            "user_filter": "(uid={username})"
                        }),
                    )
                    .unwrap();
                    let providers =
                        StorageAuthenticationProviders::try_new(vec![provider]).unwrap();
                    storage
                        .update_runtime_settings(StorageRuntimeSettingsUpdate::new(
                            StorageRuntimeSettingsAssignment::Managed(
                                StorageRuntimeSettingsValue::Authentication(providers),
                            ),
                            ResourceRevision::INITIAL,
                            crate::events::EventContext::system(),
                        ))
                        .await
                        .unwrap();
                }

                let settings =
                    crate::utilities::init::InitializationSettings::new("admin").unwrap();
                crate::utilities::init::init(&storage, &settings)
                    .await
                    .unwrap();
                let context = AppContext::new(
                    storage.clone(),
                    Arc::new(LocalPermissionBackend::new(storage, "admin".to_string())),
                );
                context.refresh_runtime_settings().await.unwrap();
                let result =
                    crate::auth::ensure_configured_identity_scopes(context.backend()).await;
                assert_eq!(result.is_ok(), managed);
                if managed {
                    assert!(
                        crate::auth::auth_provider_statuses()
                            .unwrap()
                            .iter()
                            .any(|provider| provider.name == "directory" && provider.available)
                    );
                }
            });
    }

    #[test]
    fn failed_authentication_release_discards_previous_runtime_providers() {
        use hubuum_domain::ResourceRevision;

        const CHILD: &str = "RUNTIME_AUTH_RELEASE_FAILURE_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let missing = std::env::temp_dir().join(format!(
                "hubuum-missing-auth-release-{}.toml",
                uuid::Uuid::new_v4(),
            ));
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "secrets::tests::failed_authentication_release_discards_previous_runtime_providers"])
                .env(CHILD, "1")
                .env("HUBUUM_AUTH_CONFIG_PATH", missing)
                .status().unwrap();
            assert!(status.success());
            return;
        }
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                use hubuum_storage_core::{
                    StorageAuthenticationProvider, StorageAuthenticationProviderKind,
                    StorageAuthenticationProviders,
                };
                let providers =
                    StorageAuthenticationProviders::try_new(
                        vec![StorageAuthenticationProvider::try_new(
                "directory", StorageAuthenticationProviderKind::Ldap,
                serde_json::json!({
                    "scope": "directory", "url": "ldap://localhost",
                    "user_base_dn": "dc=example,dc=org", "user_filter": "(uid={username})"
                }),
            ).unwrap()],
                    )
                    .unwrap();
                crate::auth::apply_runtime_authentication_settings(
                    Some(&providers),
                    ResourceRevision::INITIAL,
                )
                .await
                .unwrap();
                assert!(
                    crate::auth::apply_runtime_authentication_settings(
                        None,
                        ResourceRevision::INITIAL.checked_advance().unwrap()
                    )
                    .await
                    .is_err()
                );
                assert!(crate::auth::auth_provider_names().is_err());
            });
    }

    #[actix_rt::test]
    async fn file_source_keeps_consumer_namespaces_separate() {
        let directory = TestDirectory::new();
        for namespace in ["event-sink", "remote", "ldap", "token"] {
            fs::create_dir(directory.0.join(namespace)).unwrap();
        }
        fs::write(directory.0.join("event-sink/shared"), b"sink-value").unwrap();
        fs::write(directory.0.join("remote/shared"), b"remote-value").unwrap();
        fs::write(directory.0.join("ldap/shared"), b"ldap-value").unwrap();
        fs::write(directory.0.join("token/key"), b"token-value").unwrap();
        let secrets =
            ApplicationSecrets::new(SecretSourceSettings::File(directory.0.clone())).unwrap();

        assert_eq!(
            secrets
                .event_sink
                .resolve("shared")
                .await
                .unwrap()
                .value()
                .expose(),
            b"sink-value"
        );
        assert_eq!(
            secrets
                .remote
                .resolve("shared")
                .await
                .unwrap()
                .value()
                .expose(),
            b"remote-value"
        );
        assert_eq!(
            secrets
                .ldap
                .resolve("shared")
                .await
                .unwrap()
                .value()
                .expose(),
            b"ldap-value"
        );
    }

    #[actix_rt::test]
    async fn aliases_cannot_select_paths_or_providers() {
        let directory = TestDirectory::new();
        for namespace in ["event-sink", "remote", "ldap", "token"] {
            fs::create_dir(directory.0.join(namespace)).unwrap();
        }
        let secrets =
            ApplicationSecrets::new(SecretSourceSettings::File(directory.0.clone())).unwrap();

        for alias in ["../token/key", "file:token", "/etc/passwd"] {
            assert_eq!(
                secrets.remote.resolve(alias).await.unwrap_err().kind(),
                SecretErrorKind::InvalidReference
            );
        }
    }
    #[rstest::rstest]
    #[case(DatabaseCredential::Runtime, "url")]
    #[case(DatabaseCredential::Migration, "migration-url")]
    #[actix_rt::test]
    async fn file_database_credentials_ignore_environment_values(
        #[case] credential: DatabaseCredential,
        #[case] filename: &str,
    ) {
        let directory = TestDirectory::new();
        fs::create_dir(directory.0.join("database")).unwrap();
        fs::write(
            directory.0.join("database").join(filename),
            b"postgres://file/db",
        )
        .unwrap();
        let secrets =
            ApplicationSecrets::new(SecretSourceSettings::File(directory.0.clone())).unwrap();
        let url = secrets
            .database_url(credential, Some("postgres://environment/db"), None)
            .await
            .unwrap();
        assert_eq!(url.as_deref(), Some("postgres://file/db"));
    }

    #[rstest::rstest]
    #[case(DatabaseCredential::Runtime)]
    #[case(DatabaseCredential::Migration)]
    #[actix_rt::test]
    async fn missing_file_database_credentials_do_not_fall_back_to_environment(
        #[case] credential: DatabaseCredential,
    ) {
        let directory = TestDirectory::new();
        let secrets =
            ApplicationSecrets::new(SecretSourceSettings::File(directory.0.clone())).unwrap();
        let url = secrets
            .database_url(credential, Some("postgres://environment/db"), None)
            .await
            .unwrap();
        assert!(url.is_none());
    }

    #[rstest::rstest]
    #[case(DatabaseCredential::Runtime)]
    #[case(DatabaseCredential::Migration)]
    #[actix_rt::test]
    async fn explicit_database_url_overrides_file_source(#[case] credential: DatabaseCredential) {
        use clap::{Arg, Command};
        let matches = Command::new("test")
            .arg(Arg::new("url").long("url"))
            .try_get_matches_from(["test", "--url", "postgres://cli/db"])
            .unwrap();
        let explicit = CommandLineDatabaseUrl::from_matches(&matches, "url").unwrap();
        let directory = TestDirectory::new();
        let secrets =
            ApplicationSecrets::new(SecretSourceSettings::File(directory.0.clone())).unwrap();
        let url = secrets
            .database_url(
                credential,
                Some("postgres://environment/db"),
                Some(&explicit),
            )
            .await
            .unwrap();
        assert_eq!(url.as_deref(), Some("postgres://cli/db"));
    }

    #[actix_rt::test]
    async fn invalid_migration_file_does_not_become_an_absent_override() {
        let directory = TestDirectory::new();
        fs::create_dir(directory.0.join("database")).unwrap();
        fs::write(directory.0.join("database/migration-url"), []).unwrap();
        let secrets =
            ApplicationSecrets::new(SecretSourceSettings::File(directory.0.clone())).unwrap();
        let error = secrets
            .database_url(DatabaseCredential::Migration, None, None)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), SecretErrorKind::InvalidValue);
    }
}
