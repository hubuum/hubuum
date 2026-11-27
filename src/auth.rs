use chrono::NaiveDateTime;
#[cfg(feature = "integration-test-support")]
use hubuum_auth_core::AuthenticatedExternalUser;
use hubuum_auth_core::{AuthProviderError, ExternalIdentityProvider, ExternalUserRefreshRequest};
use hubuum_auth_ldap::{LdapIdentityProvider, LdapScopeConfig, LdapSecretSource};
use hubuum_domain::{PrincipalId, ResourceRevision};
use hubuum_secrets::{SecretValue, SecretVersion};
use hubuum_storage_core::{
    ExternalIdentityStorage, RUNTIME_AUTHENTICATION_PASSWORD_ENVELOPE_FIELD,
    StorageAuthenticationProvider, StorageAuthenticationProviderKind,
    StorageAuthenticationProviders, StorageExternalSyncFailure, StorageUser,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::future::Future;
use std::io::Read;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Mutex as StdMutex, RwLock as StdRwLock, Weak};
use tokio::sync::Mutex;
use tracing::{Instrument, field, info_span};

use crate::errors::ApiError;
use crate::models::runtime_settings::{
    RuntimeAuthenticationProviderRequest, RuntimeAuthenticationProviderStatus,
    RuntimeSettingsSource,
};
use crate::models::user::{User, ValidatedLogin, auth_failure};
use crate::models::{LDAP_PROVIDER_KIND, LOCAL_IDENTITY_SCOPE, LOCAL_PROVIDER_KIND};
use crate::services::identity::ensure_identity_scope;
use crate::services::identity::{
    ExternalPrincipalState, get_external_principal_state, record_external_sync_failure,
    sync_external_credential as sync_external_user_from_backend,
};

const DEFAULT_REFRESH_TTL_SECONDS: i64 = 300;
const DEFAULT_MAX_STALE_SECONDS: i64 = 3600;
const MAX_AUTH_CONFIG_BYTES: usize = 1024 * 1024;

static REFRESH_LOCKS: LazyLock<RefreshLockRegistry> = LazyLock::new(RefreshLockRegistry::default);
static AUTH_PROVIDER_REGISTRY: LazyLock<StdRwLock<Option<Arc<AuthProviderRegistry>>>> =
    LazyLock::new(|| StdRwLock::new(None));

#[derive(Default)]
struct RefreshLockRegistry {
    locks: StdMutex<HashMap<i32, Weak<Mutex<()>>>>,
}

impl RefreshLockRegistry {
    fn lock_for(&self, principal_id: i32) -> Arc<Mutex<()>> {
        let mut locks = self
            .locks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        locks.retain(|_, lock| lock.strong_count() > 0);

        if let Some(lock) = locks.get(&principal_id).and_then(Weak::upgrade) {
            return lock;
        }

        let lock = Arc::new(Mutex::new(()));
        locks.insert(principal_id, Arc::downgrade(&lock));
        lock
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.locks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthProvidersConfig {
    #[serde(default)]
    pub ldap: Vec<ConfiguredLdapScope>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ConfiguredLdapScope {
    #[serde(flatten)]
    pub ldap: LdapScopeConfig,
    pub refresh_ttl_seconds: Option<i64>,
    pub max_stale_seconds: Option<i64>,
}

impl ConfiguredLdapScope {
    pub(crate) fn refresh_policy(&self) -> Result<RefreshPolicy, ApiError> {
        RefreshPolicy::new(
            self.refresh_ttl_seconds
                .unwrap_or(DEFAULT_REFRESH_TTL_SECONDS),
            self.max_stale_seconds.unwrap_or(DEFAULT_MAX_STALE_SECONDS),
        )
    }
}

struct AuthProviderRegistry {
    revision: Option<ResourceRevision>,
    providers: HashMap<String, RegisteredAuthProvider>,
    unavailable: HashMap<String, bool>,
    statuses: HashMap<String, RuntimeAuthenticationProviderStatus>,
}

struct ApplicationLdapSecretSource;

#[async_trait::async_trait]
impl LdapSecretSource for ApplicationLdapSecretSource {
    async fn resolve(
        &self,
        alias: &str,
    ) -> Result<hubuum_secrets::ResolvedSecret, hubuum_secrets::SecretError> {
        crate::secrets::resolve_ldap_secret(alias).await
    }
}

pub(crate) async fn protect_runtime_authentication_settings(
    providers: Vec<RuntimeAuthenticationProviderRequest>,
    current: Option<&StorageAuthenticationProviders>,
    deployment: &AuthProvidersConfig,
) -> Result<StorageAuthenticationProviders, ApiError> {
    let key_ref = crate::config::get_config()?
        .runtime_settings_encryption_key_ref
        .clone();
    let mut protected = Vec::with_capacity(providers.len());
    for request in providers {
        let (provider_id, name, kind, enabled, mut configuration) = match request {
            RuntimeAuthenticationProviderRequest::Deployment {
                provider_id,
                name,
                kind,
                enabled,
            } => {
                if enabled
                    && !deployment
                        .ldap
                        .iter()
                        .any(|provider| provider.ldap.scope == name)
                {
                    return Err(ApiError::BadRequest(format!(
                        "Deployment authentication provider '{name}' is not configured on this replica"
                    )));
                }
                let provider =
                    StorageAuthenticationProvider::deployment(name, kind.into(), enabled)
                        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
                protected.push(match provider_id {
                    Some(id) => provider
                        .with_id(id)
                        .map_err(|error| ApiError::BadRequest(error.to_string()))?,
                    None => provider,
                });
                continue;
            }
            RuntimeAuthenticationProviderRequest::Runtime {
                provider_id,
                name,
                kind,
                enabled,
                configuration,
                adopt_deployment,
            } => {
                let already_managed = current.is_some_and(|providers| {
                    providers.providers().iter().any(|provider| {
                        provider.name() == name && provider.configuration().is_some()
                    })
                });
                if !already_managed
                    && !adopt_deployment
                    && deployment
                        .ldap
                        .iter()
                        .any(|provider| provider.ldap.scope == name)
                {
                    return Err(ApiError::BadRequest(format!(
                        "Provider '{name}' is deployment-owned; use a deployment activation entry or explicitly set adopt_deployment to true"
                    )));
                }
                (
                    provider_id,
                    name,
                    StorageAuthenticationProviderKind::from(kind),
                    enabled,
                    configuration,
                )
            }
        };
        let Some(config) = configuration.as_object_mut() else {
            return Err(ApiError::BadRequest(
                "Authentication provider configuration must be an object".to_string(),
            ));
        };
        if config.contains_key(RUNTIME_AUTHENTICATION_PASSWORD_ENVELOPE_FIELD) {
            return Err(ApiError::BadRequest(
                "Authentication provider configuration contains a reserved field".to_string(),
            ));
        }
        if config.get("scope").and_then(serde_json::Value::as_str) != Some(name.as_str()) {
            return Err(ApiError::BadRequest(format!(
                "Authentication provider '{name}' must use the same LDAP scope"
            )));
        }
        let previous_configuration = current
            .and_then(|current| {
                current
                    .providers()
                    .iter()
                    .find(|provider| provider.name() == name && provider.kind() == kind)
            })
            .and_then(StorageAuthenticationProvider::configuration);
        let previous_envelope = previous_configuration
            .and_then(|config| config.get(RUNTIME_AUTHENTICATION_PASSWORD_ENVELOPE_FIELD))
            .cloned();
        if !config.contains_key("bind_password")
            && !config.contains_key("bind_password_ref")
            && let Some(reference) =
                previous_configuration.and_then(|config| config.get("bind_password_ref"))
        {
            config.insert("bind_password_ref".to_string(), reference.clone());
        }
        match config.remove("bind_password") {
            Some(serde_json::Value::String(password)) => {
                if password.is_empty() {
                    return Err(ApiError::BadRequest(
                        "LDAP bind_password must not be empty; use null to clear it".to_string(),
                    ));
                }
                let plaintext = SecretValue::new(password.into_bytes()).map_err(|_| {
                    ApiError::BadRequest("LDAP bind_password is invalid".to_string())
                })?;
                let key_ref = key_ref.as_ref().ok_or_else(|| {
                    ApiError::BadRequest(
                        "HUBUUM_RUNTIME_SETTINGS_ENCRYPTION_KEY_REF is required to store LDAP passwords"
                            .to_string(),
                    )
                })?;
                let wrapping_key = crate::secrets::resolve_runtime_settings_key(key_ref, None)
                    .await
                    .map_err(|_| {
                        ApiError::ServiceUnavailable(
                            "Runtime settings encryption key is unavailable".to_string(),
                        )
                    })?;
                let envelope = crate::runtime_settings_secrets::encrypt(
                    key_ref.clone(),
                    crate::runtime_settings_secrets::ALIAS_KEY_VERSION.to_string(),
                    wrapping_key.value(),
                    &format!("authentication:{name}:bind_password"),
                    &plaintext,
                )
                .map_err(|_| {
                    ApiError::InternalServerError(
                        "LDAP bind_password could not be encrypted".to_string(),
                    )
                })?;
                config.insert(
                    RUNTIME_AUTHENTICATION_PASSWORD_ENVELOPE_FIELD.to_string(),
                    serde_json::to_value(envelope).map_err(|_| {
                        ApiError::InternalServerError(
                            "LDAP bind_password envelope could not be serialized".to_string(),
                        )
                    })?,
                );
            }
            Some(serde_json::Value::Null) => {}
            Some(_) => {
                return Err(ApiError::BadRequest(
                    "LDAP bind_password must be a string or null".to_string(),
                ));
            }
            None => {
                if let Some(envelope) = previous_envelope {
                    config.insert(
                        RUNTIME_AUTHENTICATION_PASSWORD_ENVELOPE_FIELD.to_string(),
                        envelope,
                    );
                }
            }
        }
        if !enabled {
            // Validate the complete definition without resolving an unavailable
            // existing key. The placeholder is used only for structural checks.
            let mut validation = configuration.clone();
            if let Some(config) = validation.as_object_mut()
                && config
                    .remove(RUNTIME_AUTHENTICATION_PASSWORD_ENVELOPE_FIELD)
                    .is_some()
            {
                config.insert(
                    "bind_password".to_string(),
                    serde_json::Value::String("validation-only".to_string()),
                );
            }
            AuthProviderRegistry::from_config(AuthProvidersConfig {
                ldap: vec![parse_runtime_provider_configuration(&name, validation)?],
            })?;
        }
        let provider = StorageAuthenticationProvider::try_new(name, kind, configuration)
            .map_err(|error| ApiError::BadRequest(error.message().to_string()))?
            .with_enabled(enabled);
        protected.push(match provider_id {
            Some(id) => provider
                .with_id(id)
                .map_err(|error| ApiError::BadRequest(error.to_string()))?,
            None => provider,
        });
    }
    StorageAuthenticationProviders::try_new(protected)
        .map_err(|error| ApiError::BadRequest(error.message().to_string()))
}

pub(crate) async fn runtime_provider_configuration(
    provider: &StorageAuthenticationProvider,
) -> Result<ConfiguredLdapScope, ApiError> {
    let mut configuration = provider.configuration().cloned().ok_or_else(|| {
        ApiError::BadRequest("Provider definition belongs to deployment configuration".to_string())
    })?;
    let Some(config) = configuration.as_object_mut() else {
        return Err(ApiError::BadRequest(
            "Authentication provider configuration must be an object".to_string(),
        ));
    };
    if let Some(envelope) = config.remove(RUNTIME_AUTHENTICATION_PASSWORD_ENVELOPE_FIELD) {
        let envelope: crate::runtime_settings_secrets::EncryptedSecret =
            serde_json::from_value(envelope).map_err(|_| {
                ApiError::ServiceUnavailable(
                    "Stored LDAP bind_password envelope is invalid".to_string(),
                )
            })?;
        let version =
            if envelope.key_version() == crate::runtime_settings_secrets::ALIAS_KEY_VERSION {
                None
            } else {
                Some(
                    SecretVersion::new(envelope.key_version().to_string()).map_err(|_| {
                        ApiError::ServiceUnavailable(
                            "Stored LDAP bind_password key version is invalid".to_string(),
                        )
                    })?,
                )
            };
        let wrapping_key =
            crate::secrets::resolve_runtime_settings_key(envelope.key_ref(), version.as_ref())
                .await
                .map_err(|_| {
                    ApiError::ServiceUnavailable(
                        "Stored LDAP bind_password encryption key is unavailable".to_string(),
                    )
                })?;
        let plaintext = crate::runtime_settings_secrets::decrypt(
            &envelope,
            wrapping_key.value(),
            &format!("authentication:{}:bind_password", provider.name()),
        )
        .map_err(|_| {
            ApiError::ServiceUnavailable(
                "Stored LDAP bind_password could not be decrypted".to_string(),
            )
        })?;
        let password = plaintext.expose_utf8().map_err(|_| {
            ApiError::ServiceUnavailable("Stored LDAP bind_password is not valid UTF-8".to_string())
        })?;
        config.insert(
            "bind_password".to_string(),
            serde_json::Value::String(password.to_string()),
        );
    }
    parse_runtime_provider_configuration(provider.name(), configuration)
}

fn parse_runtime_provider_configuration(
    name: &str,
    configuration: serde_json::Value,
) -> Result<ConfiguredLdapScope, ApiError> {
    let configured: ConfiguredLdapScope =
        serde_json::from_value(configuration.clone()).map_err(|_| {
            ApiError::BadRequest(format!(
                "Authentication provider '{name}' configuration is invalid"
            ))
        })?;
    let known = serde_json::to_value(&configured).map_err(|_| {
        ApiError::InternalServerError(
            "Authentication provider configuration could not be validated".to_string(),
        )
    })?;
    if contains_unknown_fields(&configuration, &known) {
        return Err(ApiError::BadRequest(format!(
            "Authentication provider '{name}' configuration contains unrecognized fields"
        )));
    }
    Ok(configured)
}

fn contains_unknown_fields(input: &serde_json::Value, known: &serde_json::Value) -> bool {
    match (input, known) {
        (serde_json::Value::Object(input), serde_json::Value::Object(known)) => {
            input.iter().any(|(key, value)| {
                known
                    .get(key)
                    .is_none_or(|known| contains_unknown_fields(value, known))
            })
        }
        (serde_json::Value::Array(input), serde_json::Value::Array(known)) => input
            .iter()
            .zip(known)
            .any(|(value, known)| contains_unknown_fields(value, known)),
        _ => false,
    }
}

type AuthProviderFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ApiError>> + Send + 'a>>;
type AuthProviderRefreshFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), AuthProviderRefreshError>> + Send + 'a>>;

enum AuthProviderRefreshError {
    Provider(AuthProviderError),
    Internal(ApiError),
}

trait AuthProviderBackend: Send + Sync {
    fn authenticate<'a>(
        &'a self,
        storage: &'a crate::storage::StorageHandle,
        revision: ResourceRevision,
        login: ValidatedLogin,
    ) -> AuthProviderFuture<'a, StorageUser>;

    fn refresh<'a>(
        &'a self,
        storage: &'a crate::storage::StorageHandle,
        revision: ResourceRevision,
        state: &'a ExternalUserState,
    ) -> AuthProviderRefreshFuture<'a>;
}

struct RegisteredAuthProvider {
    name: String,
    kind: String,
    display_order: u16,
    refresh_policy: Option<RefreshPolicy>,
    backend: Box<dyn AuthProviderBackend>,
}

#[derive(Clone, Copy)]
pub(crate) struct RefreshPolicy {
    refresh_ttl: RefreshTtl,
    max_stale: MaxStale,
}

impl RefreshPolicy {
    fn new(refresh_ttl_seconds: i64, max_stale_seconds: i64) -> Result<Self, ApiError> {
        let refresh_ttl = RefreshTtl::new(refresh_ttl_seconds)?;
        let max_stale = MaxStale::new(max_stale_seconds)?;
        if max_stale.duration() < refresh_ttl.duration() {
            return Err(ApiError::BadRequest(
                "ldap max_stale_seconds must be greater than or equal to refresh_ttl_seconds"
                    .to_string(),
            ));
        }
        Ok(Self {
            refresh_ttl,
            max_stale,
        })
    }
}

#[derive(Clone, Copy)]
struct RefreshDuration(chrono::Duration);

impl RefreshDuration {
    fn new(seconds: i64, field: &str) -> Result<Self, ApiError> {
        if seconds <= 0 {
            return Err(ApiError::BadRequest(format!(
                "ldap {field} must be positive"
            )));
        }
        chrono::Duration::try_seconds(seconds)
            .map(Self)
            .ok_or_else(|| ApiError::BadRequest(format!("ldap {field} is too large")))
    }

    fn contains(self, elapsed: chrono::Duration) -> bool {
        elapsed >= chrono::Duration::zero() && elapsed < self.0
    }
}

#[derive(Clone, Copy)]
struct RefreshTtl(RefreshDuration);

impl RefreshTtl {
    fn new(seconds: i64) -> Result<Self, ApiError> {
        RefreshDuration::new(seconds, "refresh_ttl_seconds").map(Self)
    }

    fn contains(self, elapsed: chrono::Duration) -> bool {
        self.0.contains(elapsed)
    }

    fn duration(self) -> chrono::Duration {
        self.0.0
    }
}

#[derive(Clone, Copy)]
struct MaxStale(RefreshDuration);

impl MaxStale {
    fn new(seconds: i64) -> Result<Self, ApiError> {
        RefreshDuration::new(seconds, "max_stale_seconds").map(Self)
    }

    fn contains(self, elapsed: chrono::Duration) -> bool {
        self.0.contains(elapsed)
    }

    fn duration(self) -> chrono::Duration {
        self.0.0
    }
}

struct LocalAuthProvider;

struct LdapAuthProvider {
    scope: String,
    provider: LdapIdentityProvider,
}

impl AuthProviderRegistry {
    fn from_config(config: AuthProvidersConfig) -> Result<Self, ApiError> {
        let mut registry = Self {
            revision: None,
            providers: HashMap::new(),
            unavailable: HashMap::new(),
            statuses: HashMap::new(),
        };
        LocalAuthProvider::register(&mut registry)?;
        for configured in config.ldap {
            LdapAuthProvider::register(configured, &mut registry)?;
        }
        Ok(registry)
    }

    async fn from_runtime_providers(
        configured: &StorageAuthenticationProviders,
        deployment: &AuthProvidersConfig,
    ) -> Result<Self, ApiError> {
        let mut registry = Self::from_config(AuthProvidersConfig { ldap: Vec::new() })?;
        for provider in &deployment.ldap {
            if provider.ldap.scope == LOCAL_IDENTITY_SCOPE {
                continue;
            }
            registry.statuses.insert(
                provider.ldap.scope.clone(),
                RuntimeAuthenticationProviderStatus {
                    provider_id: None,
                    kind: LDAP_PROVIDER_KIND.into(),
                    name: provider.ldap.scope.clone(),
                    source: RuntimeSettingsSource::Deployment,
                    enabled: false,
                    definition_mutable: false,
                    runtime_definition: None,
                    credential_configured: false,
                    available: false,
                },
            );
        }
        for definition in configured.providers() {
            let source = if definition.configuration().is_some() {
                RuntimeSettingsSource::Runtime
            } else {
                RuntimeSettingsSource::Deployment
            };
            let mut status = RuntimeAuthenticationProviderStatus {
                provider_id: definition.id(),
                kind: definition.kind().as_str().into(),
                name: definition.name().to_string(),
                source,
                enabled: definition.enabled(),
                definition_mutable: source == RuntimeSettingsSource::Runtime,
                runtime_definition: None,
                credential_configured: false,
                available: false,
            };
            if !definition.enabled() {
                registry
                    .statuses
                    .insert(definition.name().to_string(), status);
                continue;
            }
            if definition.kind() != StorageAuthenticationProviderKind::Ldap {
                if definition.browser_definition().is_none() {
                    return Err(ApiError::ServiceUnavailable(
                        "Validated browser provider definition required".into(),
                    ));
                }
                status.credential_configured =
                    definition.configuration().is_some_and(|configuration| {
                        configuration.get("protected_credential").is_some()
                    });
                status.available = matches!(
                    crate::services::security_candidates::providers::browser_secret(definition)
                        .await,
                    Ok(Some(_))
                );
                if !status.available {
                    registry.unavailable.insert(definition.name().into(), true);
                }
                registry
                    .statuses
                    .insert(definition.name().to_owned(), status);
                continue;
            }
            let configuration = if definition.configuration().is_some() {
                runtime_provider_configuration(definition).await
            } else {
                let mut matches = deployment
                    .ldap
                    .iter()
                    .filter(|provider| provider.ldap.scope == definition.name());
                let candidate = matches.next();
                candidate
                    .filter(|_| matches.next().is_none())
                    .cloned()
                    .ok_or_else(|| {
                        ApiError::BadRequest(
                            "Deployment authentication provider must have exactly one definition"
                                .to_string(),
                        )
                    })
            };
            let result = match configuration {
                Ok(provider) if provider.ldap.scope == definition.name() => {
                    LdapAuthProvider::register(provider, &mut registry)
                }
                Ok(_) => Err(ApiError::BadRequest(
                    "LDAP provider scope does not match its name".to_string(),
                )),
                Err(error) => Err(error),
            };
            if let Err(error) = result {
                registry.unavailable.insert(
                    definition.name().to_string(),
                    matches!(error, ApiError::ServiceUnavailable(_)),
                );
                tracing::warn!(
                    provider = definition.name(),
                    "Authentication provider could not be initialized"
                );
            } else {
                status.available = true;
            }
            registry
                .statuses
                .insert(definition.name().to_string(), status);
        }
        Ok(registry)
    }

    fn all_providers_available(&self) -> bool {
        self.unavailable.is_empty()
    }

    fn register(&mut self, provider: RegisteredAuthProvider) -> Result<(), ApiError> {
        let name = provider.name.clone();
        if self.providers.contains_key(&name) {
            return Err(ApiError::BadRequest(format!(
                "duplicate auth provider name '{name}'"
            )));
        }
        let kind = provider.kind.clone();
        self.providers.insert(name.clone(), provider);
        self.statuses.insert(
            name.clone(),
            RuntimeAuthenticationProviderStatus {
                provider_id: None,
                kind,
                name,
                source: RuntimeSettingsSource::Deployment,
                enabled: true,
                definition_mutable: false,
                runtime_definition: None,
                credential_configured: false,
                available: true,
            },
        );
        Ok(())
    }

    fn provider_names(&self) -> Vec<String> {
        let mut providers = self.providers.values().collect::<Vec<_>>();
        providers.sort_unstable_by(|left, right| {
            left.display_order
                .cmp(&right.display_order)
                .then_with(|| left.name.cmp(&right.name))
        });
        let mut names = providers
            .into_iter()
            .map(|provider| provider.name.clone())
            .collect::<Vec<_>>();
        let mut unavailable = self
            .unavailable
            .keys()
            .filter(|name| {
                self.statuses
                    .get(*name)
                    .is_some_and(|status| status.kind == LDAP_PROVIDER_KIND)
            })
            .cloned()
            .collect::<Vec<_>>();
        unavailable.sort_unstable();
        names.extend(unavailable);
        names
    }

    fn provider_statuses(&self) -> Vec<RuntimeAuthenticationProviderStatus> {
        let mut statuses = self.statuses.values().cloned().collect::<Vec<_>>();
        statuses.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        statuses
    }

    fn provider(&self, name: &str) -> Result<&RegisteredAuthProvider, ApiError> {
        self.providers
            .get(name)
            .ok_or_else(|| ApiError::Unauthorized("Authentication failure".to_string()))
    }

    fn providers(&self) -> impl Iterator<Item = &RegisteredAuthProvider> {
        self.providers.values()
    }
}

/// Resolve password-capable names from one immutable, reconciled registry.
pub(crate) fn interaction_password_providers(
    revision: ResourceRevision,
) -> Result<Vec<(String, String)>, ApiError> {
    let registry = auth_provider_registry()?;
    if registry
        .providers()
        .any(|provider| provider.name != LOCAL_IDENTITY_SCOPE)
        && registry.revision != Some(revision)
    {
        return Err(crate::errors::InteractionError::ProviderUnavailable.into());
    }
    let mut providers = registry
        .providers()
        .map(|provider| (provider.name.clone(), provider.kind.clone()))
        .collect::<Vec<_>>();
    providers.sort_by(|left, right| {
        (left.0 != LOCAL_IDENTITY_SCOPE, &left.0).cmp(&(right.0 != LOCAL_IDENTITY_SCOPE, &right.0))
    });
    Ok(providers)
}

pub fn auth_provider_names() -> Result<Vec<String>, ApiError> {
    Ok(auth_provider_registry()?.provider_names())
}

pub(crate) fn auth_provider_statuses() -> Result<Vec<RuntimeAuthenticationProviderStatus>, ApiError>
{
    Ok(auth_provider_registry()?.provider_statuses())
}

impl LocalAuthProvider {
    fn register(registry: &mut AuthProviderRegistry) -> Result<(), ApiError> {
        registry.register(RegisteredAuthProvider {
            name: LOCAL_IDENTITY_SCOPE.to_string(),
            kind: LOCAL_PROVIDER_KIND.to_string(),
            display_order: 0,
            refresh_policy: None,
            backend: Box::new(Self),
        })
    }
}

impl AuthProviderBackend for LocalAuthProvider {
    fn authenticate<'a>(
        &'a self,
        storage: &'a crate::storage::StorageHandle,
        _revision: ResourceRevision,
        login: ValidatedLogin,
    ) -> AuthProviderFuture<'a, StorageUser> {
        Box::pin(async move {
            crate::services::authentication::authenticate_local(storage, login).await
        })
    }

    fn refresh<'a>(
        &'a self,
        _storage: &'a crate::storage::StorageHandle,
        _revision: ResourceRevision,
        _state: &'a ExternalUserState,
    ) -> AuthProviderRefreshFuture<'a> {
        Box::pin(async {
            Err(AuthProviderRefreshError::Internal(
                ApiError::InternalServerError(
                    "Local authentication provider does not support external refresh".to_string(),
                ),
            ))
        })
    }
}

impl LdapAuthProvider {
    fn register(
        configured: ConfiguredLdapScope,
        registry: &mut AuthProviderRegistry,
    ) -> Result<(), ApiError> {
        let refresh_policy = configured.refresh_policy()?;
        let scope = configured.ldap.scope.clone();
        let provider = LdapIdentityProvider::with_secret_source(
            configured.ldap,
            Arc::new(ApplicationLdapSecretSource),
        )
        .map_err(provider_config_error)?
        .with_transport(Arc::new(ApplicationLdapTransport));
        registry.register(RegisteredAuthProvider {
            name: scope.clone(),
            kind: LDAP_PROVIDER_KIND.to_string(),
            display_order: 100,
            refresh_policy: Some(refresh_policy),
            backend: Box::new(Self { scope, provider }),
        })
    }
}

impl AuthProviderBackend for LdapAuthProvider {
    fn authenticate<'a>(
        &'a self,
        storage: &'a crate::storage::StorageHandle,
        revision: ResourceRevision,
        login: ValidatedLogin,
    ) -> AuthProviderFuture<'a, StorageUser> {
        Box::pin(async move {
            let authenticated = self
                .provider
                .authenticate(login.name(), login.password().as_str())
                .await
                .map_err(login_provider_error)?;
            sync_external_user_from_backend(
                storage,
                revision,
                &self.scope,
                LDAP_PROVIDER_KIND,
                authenticated,
            )
            .await
        })
    }

    fn refresh<'a>(
        &'a self,
        storage: &'a crate::storage::StorageHandle,
        revision: ResourceRevision,
        state: &'a ExternalUserState,
    ) -> AuthProviderRefreshFuture<'a> {
        Box::pin(async move {
            let refreshed = self
                .provider
                .refresh_user(&state.refresh_request)
                .await
                .map_err(AuthProviderRefreshError::Provider)?;
            sync_external_user_from_backend(
                storage,
                revision,
                &self.scope,
                LDAP_PROVIDER_KIND,
                refreshed,
            )
            .await
            .map(|_| ())
            .map_err(AuthProviderRefreshError::Internal)
        })
    }
}

/// Proof that a configured provider authenticated and synchronized this user.
/// Credential reauthentication may inspect the user without issuing a login token.
pub struct AuthenticatedLogin {
    user: User,
    credential: StorageUser,
    identity_scope: String,
    provider_kind: String,
    authentication_revision: Option<ResourceRevision>,
}

impl AuthenticatedLogin {
    pub(crate) const fn authentication_revision(&self) -> Option<ResourceRevision> {
        self.authentication_revision
    }
    pub(crate) fn credential(&self) -> &StorageUser {
        &self.credential
    }

    pub fn user(&self) -> &User {
        &self.user
    }

    pub(crate) fn identity_scope(&self) -> &str {
        &self.identity_scope
    }

    pub(crate) fn provider_kind(&self) -> &str {
        &self.provider_kind
    }
}

pub async fn login(
    storage: &impl crate::storage::StorageContext,
    login: ValidatedLogin,
) -> Result<AuthenticatedLogin, ApiError> {
    let applied = crate::storage::storage_handle(storage).applied_runtime_settings();
    applied.ensure_security_current()?;
    let registry = auth_provider_registry()?;
    if login.identity_scope() != LOCAL_IDENTITY_SCOPE {
        let revision = applied.snapshot().map(|snapshot| {
            snapshot
                .record(hubuum_storage_core::StorageRuntimeSettingsGroup::Authentication)
                .revision()
        });
        if revision.is_none() || registry.revision != revision {
            return Err(ApiError::ServiceUnavailable(
                "Authentication configuration is not reconciled".into(),
            ));
        }
    }
    login_with_registry(storage, login, &registry).await
}

async fn login_with_registry(
    storage: &impl crate::storage::StorageContext,
    login: ValidatedLogin,
    registry: &AuthProviderRegistry,
) -> Result<AuthenticatedLogin, ApiError> {
    let storage = crate::storage::storage_handle(storage);
    let scope = login.identity_scope().to_owned();
    // Pin the proof to the same immutable registry that authenticates the user.
    // The shared settings snapshot can advance while this login is in flight.
    let authentication_revision = if scope == LOCAL_IDENTITY_SCOPE {
        None
    } else {
        Some(registry.revision.ok_or_else(|| {
            ApiError::ServiceUnavailable("Authentication configuration is not reconciled".into())
        })?)
    };
    let provider = registry.provider(&scope)?;
    let span = info_span!(
        "auth.provider",
        auth.provider.kind = bounded_provider_kind(&provider.kind),
        auth.operation = "login",
        auth.result = field::Empty,
    );
    let result = provider
        .backend
        .authenticate(
            &storage,
            authentication_revision.unwrap_or(ResourceRevision::INITIAL),
            login,
        )
        .instrument(span.clone())
        .await;
    span.record(
        "auth.result",
        if result.is_ok() {
            "authenticated"
        } else {
            "rejected"
        },
    );
    result.map(|credential| AuthenticatedLogin {
        user: crate::services::identity::user_from_storage(credential.clone()),
        credential,
        identity_scope: scope,
        provider_kind: provider.kind.clone(),
        authentication_revision,
    })
}

fn bounded_provider_kind(kind: &str) -> &'static str {
    match kind {
        LOCAL_PROVIDER_KIND => "local",
        LDAP_PROVIDER_KIND => "ldap",
        _ => "unknown",
    }
}

pub async fn refresh_principal_if_needed(
    storage: &impl crate::storage::StorageContext,
    principal_id: i32,
) -> Result<(), ApiError> {
    let storage = crate::storage::storage_handle(storage);
    storage
        .applied_runtime_settings()
        .ensure_security_current()?;
    let id = PrincipalId::new(principal_id)?;
    let Some(state) = storage.get_external_principal_state(id).await? else {
        return Ok(());
    };
    let registry = auth_provider_registry()?;
    let password_provider = registry
        .provider(state.identity_scope())
        .is_ok_and(|provider| provider.kind == LDAP_PROVIDER_KIND);
    if !password_provider
        && crate::services::auth_interactions::external::check_existing_authority(
            &storage, id, &state,
        )
        .await?
    {
        return Ok(());
    }
    refresh_principal_with_registry(
        &storage,
        principal_id,
        crate::services::identity::external_state_from_storage(state),
        &registry,
    )
    .await
}

async fn refresh_principal_with_registry(
    storage: &crate::storage::StorageHandle,
    principal_id: i32,
    state: ExternalPrincipalState,
    registry: &AuthProviderRegistry,
) -> Result<(), ApiError> {
    let state = ExternalUserState::from_registry(state, registry)?;

    match refresh_status(&state) {
        RefreshStatus::Fresh => return Ok(()),
        RefreshStatus::Backoff => return cached_external_state_result(&state),
        RefreshStatus::Due => {}
    }

    let lock = REFRESH_LOCKS.lock_for(principal_id);
    {
        let _guard = lock.lock().await;

        match external_user_state(storage, principal_id, registry).await {
            Err(err) => Err(err),
            Ok(None) => Ok(()),
            Ok(Some(state)) => match refresh_status(&state) {
                RefreshStatus::Fresh => Ok(()),
                RefreshStatus::Backoff => cached_external_state_result(&state),
                RefreshStatus::Due => match registry.provider(&state.identity_scope) {
                    Err(err) => Err(err),
                    Ok(configured) => {
                        let span = info_span!(
                            "auth.identity_refresh",
                            auth.provider.kind = bounded_provider_kind(&configured.kind),
                            auth.operation = "identity_refresh",
                            auth.result = field::Empty,
                        );
                        let revision = registry.revision.ok_or_else(|| {
                            ApiError::ServiceUnavailable(
                                "Authentication configuration is not reconciled".into(),
                            )
                        })?;
                        let refresh_result = configured
                            .backend
                            .refresh(storage, revision, &state)
                            .instrument(span.clone())
                            .await;
                        span.record(
                            "auth.result",
                            match &refresh_result {
                                Ok(()) => "refreshed",
                                Err(AuthProviderRefreshError::Provider(_)) => "provider_error",
                                Err(AuthProviderRefreshError::Internal(_)) => "internal_error",
                            },
                        );
                        match refresh_result {
                            Ok(()) => Ok(()),
                            Err(AuthProviderRefreshError::Internal(err)) => Err(err),
                            Err(AuthProviderRefreshError::Provider(
                                error @ (AuthProviderError::AuthenticationFailed
                                | AuthProviderError::Config(_)
                                | AuthProviderError::Protocol(_)),
                            )) => {
                                record_external_sync_failure(
                                    storage,
                                    revision,
                                    principal_id,
                                    StorageExternalSyncFailure::Rejected,
                                )
                                .await?;
                                Err(provider_error(error))
                            }
                            Err(AuthProviderRefreshError::Provider(
                                AuthProviderError::Unavailable(err),
                            )) => {
                                match record_external_sync_failure(
                                    storage,
                                    revision,
                                    principal_id,
                                    StorageExternalSyncFailure::Unavailable,
                                )
                                .await
                                {
                                    Err(mark_err) => Err(mark_err),
                                    Ok(()) => {
                                        if within_max_stale(
                                            state.last_sync_success_at,
                                            state.refresh_policy.max_stale,
                                        ) {
                                            tracing::warn!(
                                                principal_id,
                                                identity_scope = state.identity_scope,
                                                error = %err,
                                                "External identity refresh failed; using cached memberships inside max-stale window"
                                            );
                                            Ok(())
                                        } else {
                                            tracing::error!(
                                                principal_id,
                                                identity_scope = state.identity_scope,
                                                error = %err,
                                                "External identity refresh failed; cached memberships exceed max-stale window"
                                            );
                                            stale_external_state_error()
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
            },
        }
    }
}

pub async fn ensure_configured_identity_scopes(
    pool: &impl crate::storage::StorageContext,
) -> Result<(), ApiError> {
    let registry = auth_provider_registry()?;
    for provider in registry.providers() {
        ensure_identity_scope(pool, &provider.name, &provider.kind).await?;
    }
    Ok(())
}

pub(crate) async fn apply_runtime_authentication_settings(
    providers: Option<&StorageAuthenticationProviders>,
    revision: ResourceRevision,
) -> Result<bool, ApiError> {
    let registry = match providers {
        Some(providers) => {
            AuthProviderRegistry::from_runtime_providers(providers, &runtime_deployment_config())
                .await
        }
        None => build_auth_provider_registry(),
    };
    let mut registry = match registry {
        Ok(registry) => registry,
        Err(error) => {
            // Never continue using a removed runtime provider after a failed
            // release to this replica's deployment configuration.
            *AUTH_PROVIDER_REGISTRY
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
            return Err(error);
        }
    };
    registry.revision = Some(revision);
    let complete = registry.all_providers_available();
    *AUTH_PROVIDER_REGISTRY
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::new(registry));
    Ok(complete)
}

pub(crate) fn authentication_is_ready() -> bool {
    auth_provider_registry().is_ok()
}

fn auth_provider_registry() -> Result<Arc<AuthProviderRegistry>, ApiError> {
    if let Some(registry) = AUTH_PROVIDER_REGISTRY
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
    {
        return Ok(registry);
    }
    let registry = Arc::new(build_auth_provider_registry()?);
    let mut current = AUTH_PROVIDER_REGISTRY
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    Ok(current.get_or_insert(registry).clone())
}

fn build_auth_provider_registry() -> Result<AuthProviderRegistry, ApiError> {
    AuthProviderRegistry::from_config(load_auth_config()?)
}

pub(crate) fn load_auth_config() -> Result<AuthProvidersConfig, ApiError> {
    let config = crate::config::get_config()?;
    let Some(path) = config.auth_config_path.as_deref() else {
        return Ok(AuthProvidersConfig { ldap: Vec::new() });
    };
    let path = Path::new(path);
    let source = read_auth_config_source(path)?;
    parse_auth_config_source(path, &source)
}

/// Runtime definitions remain authoritative if the old deployment file is gone.
/// Enabled deployment references will be reported unavailable and fail preflight.
pub(crate) fn runtime_deployment_config() -> AuthProvidersConfig {
    load_auth_config().unwrap_or_else(|_| {
        tracing::warn!("Deployment authentication configuration is unavailable; runtime-owned definitions remain authoritative");
        AuthProvidersConfig { ldap: Vec::new() }
    })
}

fn read_auth_config_source(path: &Path) -> Result<String, ApiError> {
    let file = File::open(path).map_err(|error| {
        ApiError::InternalServerError(format!("Failed to open auth config {path:?}: {error}"))
    })?;
    let metadata = file.metadata().map_err(|error| {
        ApiError::InternalServerError(format!("Failed to inspect auth config {path:?}: {error}"))
    })?;
    if !metadata.is_file() {
        return Err(ApiError::InternalServerError(format!(
            "Auth config {path:?} must be a regular file"
        )));
    }
    if metadata.len() > MAX_AUTH_CONFIG_BYTES as u64 {
        return Err(auth_config_size_error(path));
    }

    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_AUTH_CONFIG_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| {
            ApiError::InternalServerError(format!("Failed to read auth config {path:?}: {error}"))
        })?;
    if bytes.len() > MAX_AUTH_CONFIG_BYTES {
        return Err(auth_config_size_error(path));
    }
    String::from_utf8(bytes).map_err(|_| {
        ApiError::InternalServerError(format!("Auth config {path:?} must contain valid UTF-8"))
    })
}

fn auth_config_size_error(path: &Path) -> ApiError {
    ApiError::InternalServerError(format!(
        "Auth config {path:?} exceeds the {MAX_AUTH_CONFIG_BYTES}-byte limit"
    ))
}

fn parse_auth_config_source(path: &Path, source: &str) -> Result<AuthProvidersConfig, ApiError> {
    toml::from_str::<AuthProvidersConfig>(source).map_err(|error| {
        let location = error.span().map(|span| {
            let (line, column) = source_position(source, span.start);
            format!(" at line {line}, column {column}")
        });
        ApiError::InternalServerError(format!(
            "Failed to parse auth config {path:?}{}; source content omitted because it may contain credentials",
            location.as_deref().unwrap_or_default()
        ))
    })
}

fn source_position(source: &str, byte_offset: usize) -> (usize, usize) {
    let bytes = source.as_bytes();
    let offset = byte_offset.min(bytes.len());
    let line = bytes[..offset]
        .iter()
        .filter(|byte| **byte == b'\n')
        .count()
        + 1;
    let line_start = bytes[..offset]
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |newline| newline + 1);
    let column = source
        .get(line_start..offset)
        .map_or(offset - line_start + 1, |prefix| prefix.chars().count() + 1);
    (line, column)
}

fn provider_error(err: AuthProviderError) -> ApiError {
    match err {
        AuthProviderError::AuthenticationFailed => auth_failure(),
        AuthProviderError::Unavailable(message) => ApiError::ServiceUnavailable(message),
        AuthProviderError::Config(message) | AuthProviderError::Protocol(message) => {
            ApiError::InternalServerError(message)
        }
    }
}

fn provider_config_error(err: AuthProviderError) -> ApiError {
    match err {
        AuthProviderError::Config(message) => ApiError::BadRequest(message),
        other => provider_error(other),
    }
}

fn login_provider_error(err: AuthProviderError) -> ApiError {
    match err {
        AuthProviderError::AuthenticationFailed => auth_failure(),
        other => provider_error(other),
    }
}

fn now() -> NaiveDateTime {
    chrono::Utc::now().naive_utc()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RefreshStatus {
    Fresh,
    Backoff,
    Due,
}

fn refresh_status(state: &ExternalUserState) -> RefreshStatus {
    refresh_status_at(
        now(),
        state.last_sync_success_at,
        state.last_sync_attempted_at,
        state.refresh_policy.refresh_ttl,
    )
}

fn refresh_status_at(
    current: NaiveDateTime,
    last_success: Option<NaiveDateTime>,
    last_attempt: Option<NaiveDateTime>,
    refresh_ttl: RefreshTtl,
) -> RefreshStatus {
    if last_success.is_some_and(|success| refresh_ttl.contains(current - success)) {
        return RefreshStatus::Fresh;
    }

    let failed_attempt = match (last_attempt, last_success) {
        (Some(attempt), Some(success)) => (attempt > success).then_some(attempt),
        (Some(attempt), None) => Some(attempt),
        _ => None,
    };
    if failed_attempt.is_some_and(|attempt| refresh_ttl.contains(current - attempt)) {
        RefreshStatus::Backoff
    } else {
        RefreshStatus::Due
    }
}

fn within_max_stale(last_success: Option<NaiveDateTime>, max_stale: MaxStale) -> bool {
    within_max_stale_at(now(), last_success, max_stale)
}

fn within_max_stale_at(
    current: NaiveDateTime,
    last_success: Option<NaiveDateTime>,
    max_stale: MaxStale,
) -> bool {
    let Some(last_success) = last_success else {
        return false;
    };
    max_stale.contains(current - last_success)
}

fn cached_external_state_result(state: &ExternalUserState) -> Result<(), ApiError> {
    cached_external_state_result_at(now(), state)
}

fn cached_external_state_result_at(
    current: NaiveDateTime,
    state: &ExternalUserState,
) -> Result<(), ApiError> {
    if within_max_stale_at(
        current,
        state.last_sync_success_at,
        state.refresh_policy.max_stale,
    ) {
        tracing::debug!(
            identity_scope = state.identity_scope,
            "External identity refresh is in retry backoff; using cached memberships"
        );
        Ok(())
    } else {
        tracing::warn!(
            identity_scope = state.identity_scope,
            last_sync_attempted_at = ?state.last_sync_attempted_at,
            last_sync_success_at = ?state.last_sync_success_at,
            "External identity refresh is in retry backoff; cached memberships exceed max-stale window"
        );
        stale_external_state_error()
    }
}

fn stale_external_state_error() -> Result<(), ApiError> {
    Err(ApiError::ServiceUnavailable(
        "External identity provider is unavailable and cached memberships are stale".to_string(),
    ))
}

struct ExternalUserState {
    identity_scope: String,
    refresh_request: ExternalUserRefreshRequest,
    last_sync_attempted_at: Option<NaiveDateTime>,
    last_sync_success_at: Option<NaiveDateTime>,
    refresh_policy: RefreshPolicy,
}

impl ExternalUserState {
    fn from_registry(
        state: ExternalPrincipalState,
        registry: &AuthProviderRegistry,
    ) -> Result<Self, ApiError> {
        let configured = registry.provider(&state.identity_scope)?;
        let refresh_policy = configured.refresh_policy.ok_or_else(|| {
            ApiError::InternalServerError(format!(
                "Authentication provider '{}' does not support external refresh",
                configured.name
            ))
        })?;
        Self::new(state, refresh_policy)
    }

    fn new(state: ExternalPrincipalState, refresh_policy: RefreshPolicy) -> Result<Self, ApiError> {
        let refresh_request =
            match ExternalUserRefreshRequest::new(state.username, state.external_subject) {
                Ok(request) => request,
                Err(error) => {
                    return Err(ApiError::InternalServerError(format!(
                        "Invalid external identity state: {error}"
                    )));
                }
            };
        Ok(Self {
            identity_scope: state.identity_scope,
            refresh_request,
            last_sync_attempted_at: state.last_sync_attempted_at,
            last_sync_success_at: state.last_sync_success_at,
            refresh_policy,
        })
    }
}

async fn external_user_state(
    pool: &impl crate::storage::StorageContext,
    principal_id_value: i32,
    registry: &AuthProviderRegistry,
) -> Result<Option<ExternalUserState>, ApiError> {
    let Some(state) = get_external_principal_state(pool, principal_id_value).await? else {
        return Ok(None);
    };
    ExternalUserState::from_registry(state, registry).map(Some)
}

#[cfg(feature = "integration-test-support")]
pub(crate) async fn sync_external_user(
    pool: &impl crate::storage::StorageContext,
    configured: &ConfiguredLdapScope,
    authenticated: AuthenticatedExternalUser,
) -> Result<User, ApiError> {
    use hubuum_storage_core::RuntimeSettingsStorage;
    sync_external_user_from_backend(
        pool,
        crate::storage::storage_handle(pool)
            .load_runtime_settings()
            .await?
            .record(hubuum_storage_core::StorageRuntimeSettingsGroup::Authentication)
            .revision(),
        &configured.ldap.scope,
        LDAP_PROVIDER_KIND,
        authenticated,
    )
    .await
    .map(crate::services::identity::user_from_storage)
}

/// Screen DNS on every LDAP connection and connect only to the screened socket.
/// TLS remains owned by the LDAP adapter and validates the configured hostname.
pub(crate) struct ApplicationLdapTransport;

#[async_trait::async_trait]
impl hubuum_auth_ldap::LdapTransport for ApplicationLdapTransport {
    async fn connect(
        &self,
        host: &str,
        port: u16,
        timeout: std::time::Duration,
    ) -> Result<std::net::TcpStream, hubuum_auth_core::AuthProviderError> {
        use hubuum_auth_core::AuthProviderError;
        let allow_private = crate::config::get_config()
            .map_err(|_| {
                AuthProviderError::Config("Authentication egress policy unavailable".into())
            })?
            .auth_runtime_allow_private_targets;
        tokio::time::timeout(timeout, async {
            let addresses = hubuum_outbound_http::screen_host(
                host.trim_matches(['[', ']']),
                port,
                allow_private,
                false,
            )
            .await
            .map_err(|error| {
                if error.is_dns_failure() {
                    AuthProviderError::Unavailable("LDAP DNS resolution failed".into())
                } else {
                    AuthProviderError::Config("LDAP destination is prohibited".into())
                }
            })?;
            let stream = tokio::net::TcpStream::connect(addresses.as_slice())
                .await
                .map_err(|_| AuthProviderError::Unavailable("LDAP connection failed".into()))?;
            stream
                .into_std()
                .map_err(|_| AuthProviderError::Unavailable("LDAP connection unavailable".into()))
        })
        .await
        .map_err(|_| AuthProviderError::Unavailable("LDAP connection timed out".into()))?
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod login_events;

#[cfg(test)]
mod security_tests;
