use std::sync::OnceLock;
use std::time::Instant;

use hubuum_secrets::{
    DEFAULT_MAX_SECRET_BYTES, EnvironmentProvider, FileProvider, FileSymlinkPolicy, ResolvedSecret,
    ResolvedSecretGroup, SecretError, SecretErrorKind, SecretName, SecretProviderKind, SecretRef,
    SecretResolver,
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
        let started = Instant::now();
        crate::observability::metrics::secret_source_identity(self.provider_label);
        let result = match SecretName::new(alias) {
            Ok(name) => {
                let reference = SecretRef::new(self.provider_kind, name);
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
    token: ConsumerSecrets,
}

impl ApplicationSecrets {
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
