use chrono::NaiveDateTime;
#[cfg(feature = "integration-test-support")]
use hubuum_auth_core::AuthenticatedExternalUser;
use hubuum_auth_core::{AuthProviderError, ExternalIdentityProvider, ExternalUserRefreshRequest};
use hubuum_auth_ldap::{LdapIdentityProvider, LdapScopeConfig, LdapSecretSource};
use hubuum_secrets::{SecretValue, SecretVersion};
use hubuum_storage_core::{
    RUNTIME_AUTHENTICATION_PASSWORD_ENVELOPE_FIELD, StorageAuthenticationProvider,
    StorageAuthenticationProviderKind, StorageAuthenticationProviders, StorageExternalSyncFailure,
    StorageUser,
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
    fn refresh_policy(&self) -> Result<RefreshPolicy, ApiError> {
        RefreshPolicy::new(
            self.refresh_ttl_seconds
                .unwrap_or(DEFAULT_REFRESH_TTL_SECONDS),
            self.max_stale_seconds.unwrap_or(DEFAULT_MAX_STALE_SECONDS),
        )
    }
}

struct AuthProviderRegistry {
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
        let (name, kind, enabled, mut configuration) = match request {
            RuntimeAuthenticationProviderRequest::Deployment {
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
                protected.push(
                    StorageAuthenticationProvider::deployment(name, kind.into(), enabled)
                        .map_err(|error| ApiError::BadRequest(error.to_string()))?,
                );
                continue;
            }
            RuntimeAuthenticationProviderRequest::Runtime {
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
        let previous_envelope = current
            .and_then(|current| {
                current
                    .providers()
                    .iter()
                    .find(|provider| provider.name() == name && provider.kind() == kind)
            })
            .and_then(|provider| {
                provider
                    .configuration()
                    .and_then(|config| config.get(RUNTIME_AUTHENTICATION_PASSWORD_ENVELOPE_FIELD))
            })
            .cloned();
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
        protected.push(
            StorageAuthenticationProvider::try_new(name, kind, configuration)
                .map_err(|error| ApiError::BadRequest(error.message().to_string()))?
                .with_enabled(enabled),
        );
    }
    StorageAuthenticationProviders::try_new(protected)
        .map_err(|error| ApiError::BadRequest(error.message().to_string()))
}

async fn runtime_provider_configuration(
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
        login: ValidatedLogin,
    ) -> AuthProviderFuture<'a, StorageUser>;

    fn refresh<'a>(
        &'a self,
        storage: &'a crate::storage::StorageHandle,
        state: &'a ExternalUserState,
    ) -> AuthProviderRefreshFuture<'a>;

    fn preflight(&self) -> AuthProviderFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

struct RegisteredAuthProvider {
    name: String,
    kind: String,
    display_order: u16,
    refresh_policy: Option<RefreshPolicy>,
    backend: Box<dyn AuthProviderBackend>,
}

#[derive(Clone, Copy)]
struct RefreshPolicy {
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
                    name: provider.ldap.scope.clone(),
                    source: RuntimeSettingsSource::Deployment,
                    enabled: false,
                    definition_mutable: false,
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
                name: definition.name().to_string(),
                source,
                enabled: definition.enabled(),
                definition_mutable: source == RuntimeSettingsSource::Runtime,
                available: false,
            };
            if !definition.enabled() {
                registry
                    .statuses
                    .insert(definition.name().to_string(), status);
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

    async fn preflight(&self) -> Result<(), ApiError> {
        if self.unavailable.values().any(|unavailable| *unavailable) {
            return Err(ApiError::ServiceUnavailable(
                "A runtime authentication secret is unavailable".to_string(),
            ));
        }
        if !self.unavailable.is_empty() {
            return Err(ApiError::BadRequest(
                "One or more authentication providers are invalid".to_string(),
            ));
        }
        for provider in self.providers.values() {
            provider.backend.preflight().await?;
        }
        Ok(())
    }

    fn register(&mut self, provider: RegisteredAuthProvider) -> Result<(), ApiError> {
        let name = provider.name.clone();
        if self.providers.contains_key(&name) {
            return Err(ApiError::BadRequest(format!(
                "duplicate auth provider name '{name}'"
            )));
        }
        self.providers.insert(name.clone(), provider);
        self.statuses.insert(
            name.clone(),
            RuntimeAuthenticationProviderStatus {
                name,
                source: RuntimeSettingsSource::Deployment,
                enabled: true,
                definition_mutable: false,
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
        let mut unavailable = self.unavailable.keys().cloned().collect::<Vec<_>>();
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
        login: ValidatedLogin,
    ) -> AuthProviderFuture<'a, StorageUser> {
        Box::pin(async move {
            crate::services::authentication::authenticate_local(storage, login).await
        })
    }

    fn refresh<'a>(
        &'a self,
        _storage: &'a crate::storage::StorageHandle,
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

    fn preflight(&self) -> AuthProviderFuture<'_, ()> {
        Box::pin(async { Ok(()) })
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
        .map_err(provider_config_error)?;
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
        login: ValidatedLogin,
    ) -> AuthProviderFuture<'a, StorageUser> {
        Box::pin(async move {
            let authenticated = self
                .provider
                .authenticate(login.name(), login.password().as_str())
                .await
                .map_err(login_provider_error)?;
            sync_external_user_from_backend(storage, &self.scope, LDAP_PROVIDER_KIND, authenticated)
                .await
        })
    }

    fn refresh<'a>(
        &'a self,
        storage: &'a crate::storage::StorageHandle,
        state: &'a ExternalUserState,
    ) -> AuthProviderRefreshFuture<'a> {
        Box::pin(async move {
            let refreshed = self
                .provider
                .refresh_user(&state.refresh_request)
                .await
                .map_err(AuthProviderRefreshError::Provider)?;
            sync_external_user_from_backend(storage, &self.scope, LDAP_PROVIDER_KIND, refreshed)
                .await
                .map(|_| ())
                .map_err(AuthProviderRefreshError::Internal)
        })
    }

    fn preflight(&self) -> AuthProviderFuture<'_, ()> {
        Box::pin(async move {
            self.provider
                .test_connection()
                .await
                .map_err(|_| ApiError::BadRequest("LDAP connectivity preflight failed".to_string()))
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
}

impl AuthenticatedLogin {
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
    crate::storage::storage_handle(storage)
        .applied_runtime_settings()
        .ensure_security_current()?;
    let registry = auth_provider_registry()?;
    login_with_registry(storage, login, &registry).await
}

async fn login_with_registry(
    storage: &impl crate::storage::StorageContext,
    login: ValidatedLogin,
    registry: &AuthProviderRegistry,
) -> Result<AuthenticatedLogin, ApiError> {
    let storage = crate::storage::storage_handle(storage);
    let scope = login.identity_scope().to_owned();
    let provider = registry.provider(&scope)?;
    let span = info_span!(
        "auth.provider",
        auth.provider.kind = bounded_provider_kind(&provider.kind),
        auth.operation = "login",
        auth.result = field::Empty,
    );
    let result = provider
        .backend
        .authenticate(&storage, login)
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
    let Some(state) = get_external_principal_state(&storage, principal_id).await? else {
        return Ok(());
    };
    let registry = auth_provider_registry()?;
    refresh_principal_with_registry(&storage, principal_id, state, &registry).await
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
                        let refresh_result = configured
                            .backend
                            .refresh(storage, &state)
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
) -> Result<bool, ApiError> {
    let registry = match providers {
        Some(providers) => {
            AuthProviderRegistry::from_runtime_providers(providers, &runtime_deployment_config())
                .await
        }
        None => build_auth_provider_registry(),
    };
    let registry = match registry {
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
    let complete = registry.unavailable.is_empty();
    *AUTH_PROVIDER_REGISTRY
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::new(registry));
    Ok(complete)
}

pub(crate) fn authentication_is_ready() -> bool {
    auth_provider_registry().is_ok()
}

pub(crate) async fn preflight_runtime_authentication_settings(
    providers: &StorageAuthenticationProviders,
    storage: &crate::storage::StorageHandle,
    deployment: &AuthProvidersConfig,
) -> Result<(), ApiError> {
    let registry = AuthProviderRegistry::from_runtime_providers(providers, deployment).await?;
    registry.preflight().await?;
    for provider in providers.providers() {
        ensure_identity_scope(storage, provider.name(), LDAP_PROVIDER_KIND).await?;
    }
    Ok(())
}

pub(crate) async fn preflight_deployment_authentication_settings(
    storage: &crate::storage::StorageHandle,
) -> Result<(), ApiError> {
    let registry = build_auth_provider_registry()?;
    registry.preflight().await?;
    for provider in registry.providers() {
        ensure_identity_scope(storage, &provider.name, &provider.kind).await?;
    }
    Ok(())
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
    sync_external_user_from_backend(
        pool,
        &configured.ldap.scope,
        LDAP_PROVIDER_KIND,
        authenticated,
    )
    .await
    .map(crate::services::identity::user_from_storage)
}

#[cfg(test)]
mod tests {
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
            let path =
                std::env::temp_dir().join(format!("hubuum-auth-config-{}.toml", Uuid::new_v4()));
            std::fs::write(&path, contents).expect("test auth config should be written");
            Self { path }
        }

        fn with_len(len: u64) -> Self {
            let path =
                std::env::temp_dir().join(format!("hubuum-auth-config-{}.toml", Uuid::new_v4()));
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

        let error = read_auth_config_source(file.path())
            .expect_err("oversized auth config should be rejected");

        assert_eq!(error, auth_config_size_error(file.path()));
    }

    #[test]
    fn auth_config_reader_rejects_invalid_utf8() {
        let file = TestAuthConfigFile::new(&[0xff]);

        let error = read_auth_config_source(file.path())
            .expect_err("non-UTF-8 auth config should be rejected");

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
            providers.providers()[0]
                .configuration()
                .and_then(|configuration| configuration
                    .get(RUNTIME_AUTHENTICATION_PASSWORD_ENVELOPE_FIELD)),
            current.providers()[0]
                .configuration()
                .and_then(|configuration| configuration
                    .get(RUNTIME_AUTHENTICATION_PASSWORD_ENVELOPE_FIELD))
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
                .and_then(|configuration| configuration
                    .get(RUNTIME_AUTHENTICATION_PASSWORD_ENVELOPE_FIELD))
                .is_none()
        );
    }
}

#[cfg(test)]
mod login_events;

#[cfg(test)]
mod security_tests;
