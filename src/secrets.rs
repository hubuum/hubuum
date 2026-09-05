use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::time::Instant;

use hubuum_secrets::{
    EnvironmentProvider, FileProvider, FileSymlinkPolicy, ResolvedSecret, ResolvedSecretGroup,
    SecretError, SecretErrorKind, SecretName, SecretProviderKind, SecretRef, SecretResolver,
};

use crate::config::environment::constraints;

const SOURCE_ENVIRONMENT: &str = "HUBUUM_SECRET_SOURCE";
const FILE_ROOT_ENVIRONMENT: &str = "HUBUUM_SECRET_FILE_ROOT";

static APPLICATION_SECRETS: LazyLock<Result<ApplicationSecrets, SecretError>> =
    LazyLock::new(ApplicationSecrets::from_environment);

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
    source: SecretSource,
    database: ConsumerSecrets,
    event_sink: ConsumerSecrets,
    remote: ConsumerSecrets,
    ldap: ConsumerSecrets,
    token: ConsumerSecrets,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SecretSource {
    Environment,
    File,
}

impl SecretSource {
    fn from_environment() -> Result<Self, SecretError> {
        match std::env::var(SOURCE_ENVIRONMENT) {
            Ok(value) if value.eq_ignore_ascii_case("environment") => Ok(Self::Environment),
            Ok(value) if value.eq_ignore_ascii_case("file") => Ok(Self::File),
            Ok(_) => Err(SecretError::new(
                SecretErrorKind::InvalidReference,
                "HUBUUM_SECRET_SOURCE must be 'environment' or 'file'",
            )),
            Err(std::env::VarError::NotPresent) => Ok(Self::Environment),
            Err(std::env::VarError::NotUnicode(_)) => Err(SecretError::new(
                SecretErrorKind::InvalidReference,
                "HUBUUM_SECRET_SOURCE must contain valid Unicode",
            )),
        }
    }

    fn as_label(self) -> &'static str {
        match self {
            Self::Environment => "environment",
            Self::File => "file",
        }
    }
}

impl ApplicationSecrets {
    fn from_environment() -> Result<Self, SecretError> {
        let source = SecretSource::from_environment()?;
        let root = match source {
            SecretSource::Environment => None,
            SecretSource::File => Some(PathBuf::from(
                std::env::var_os(FILE_ROOT_ENVIRONMENT).ok_or_else(|| {
                    SecretError::new(
                        SecretErrorKind::InvalidReference,
                        "HUBUUM_SECRET_FILE_ROOT is required when HUBUUM_SECRET_SOURCE=file",
                    )
                })?,
            )),
        };
        Self::new(source, root.as_deref())
    }

    fn new(source: SecretSource, root: Option<&Path>) -> Result<Self, SecretError> {
        crate::observability::metrics::secret_source_identity(source.as_label());
        Ok(Self {
            source,
            database: consumer_resolver(
                source,
                root,
                "",
                Some(("url", "HUBUUM_DATABASE_URL")),
                "database",
                "database",
            )?,
            event_sink: consumer_resolver(
                source,
                root,
                "HUBUUM_EVENT_SINK_SECRET_",
                None,
                "event-sink",
                "event_sink",
            )?,
            remote: consumer_resolver(
                source,
                root,
                "HUBUUM_REMOTE_SECRET_",
                None,
                "remote",
                "remote_target",
            )?,
            ldap: consumer_resolver(source, root, "HUBUUM_LDAP_SECRET_", None, "ldap", "ldap")?,
            token: consumer_resolver(
                source,
                root,
                "HUBUUM_TOKEN_HASH_KEY_",
                Some(("key", "HUBUUM_TOKEN_HASH_KEY")),
                "token",
                "token_hash",
            )?,
        })
    }
}

fn consumer_resolver(
    source: SecretSource,
    root: Option<&Path>,
    environment_prefix: &str,
    exact_environment_key: Option<(&str, &str)>,
    file_prefix: &str,
    consumer_label: &'static str,
) -> Result<ConsumerSecrets, SecretError> {
    let provider_kind = match source {
        SecretSource::Environment => SecretProviderKind::environment(),
        SecretSource::File => SecretProviderKind::file(),
    };
    let builder = SecretResolver::builder();
    let resolver = match source {
        SecretSource::Environment => {
            let mut provider = EnvironmentProvider::new(environment_prefix)?;
            if let Some((alias, environment_name)) = exact_environment_key {
                provider = provider.mapping(SecretName::new(alias)?, environment_name)?;
            }
            builder.provider(provider)?.build()
        }
        SecretSource::File => {
            let root = constraints::SECRET_FILE_ROOT.require(root).map_err(|_| {
                SecretError::new(
                    SecretErrorKind::InvalidReference,
                    "file secret source requires a configured root",
                )
            })?;
            builder
                .provider(
                    FileProvider::builder(root)
                        .path_prefix(file_prefix)
                        .symlink_policy(FileSymlinkPolicy::AllowWithinRoot)
                        .build()?,
                )?
                .build()
        }
    };
    Ok(ConsumerSecrets {
        provider_kind,
        provider_label: source.as_label(),
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
    APPLICATION_SECRETS.as_ref().map_err(Clone::clone)
}

pub(crate) fn validate_configuration() -> Result<(), SecretError> {
    configured().map(|_| ())
}

pub(crate) async fn resolve_event_sink_secret(alias: &str) -> Result<ResolvedSecret, SecretError> {
    configured()?.event_sink.resolve(alias).await
}

pub(crate) async fn resolve_database_url(fallback: &str) -> Result<String, SecretError> {
    let secrets = configured()?;
    match secrets.database.resolve("url").await {
        Ok(resolved) => Ok(resolved.value().expose_utf8()?.to_string()),
        Err(error)
            if secrets.source == SecretSource::Environment
                && error.kind() == SecretErrorKind::NotFound =>
        {
            Ok(fallback.to_string())
        }
        Err(error) => Err(error),
    }
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
    if secrets.source == SecretSource::Environment {
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
    Ok(configured()?.source == SecretSource::Environment)
}

pub(crate) fn running_source_configuration() -> (&'static str, bool) {
    let provider = SecretSource::from_environment()
        .map(SecretSource::as_label)
        .unwrap_or("invalid");
    (provider, std::env::var_os(FILE_ROOT_ENVIRONMENT).is_some())
}

#[cfg(test)]
mod tests {
    use std::fs;
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
        let secrets = ApplicationSecrets::new(SecretSource::File, Some(&directory.0)).unwrap();

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
        let secrets = ApplicationSecrets::new(SecretSource::File, Some(&directory.0)).unwrap();

        for alias in ["../token/key", "file:token", "/etc/passwd"] {
            assert_eq!(
                secrets.remote.resolve(alias).await.unwrap_err().kind(),
                SecretErrorKind::InvalidReference
            );
        }
    }
}
