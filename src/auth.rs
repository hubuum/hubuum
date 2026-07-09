use chrono::NaiveDateTime;
use hubuum_auth_core::{AuthProviderError, AuthenticatedExternalUser, ExternalIdentityProvider};
use hubuum_auth_ldap::{LdapIdentityProvider, LdapScopeConfig};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, LazyLock, OnceLock};
use tokio::sync::Mutex;

use crate::db::DbPool;
use crate::db::traits::external_identity::{
    external_principal_state, mark_external_sync_attempted,
    sync_external_user as sync_external_user_from_backend,
};
use crate::db::traits::identity::ensure_identity_scope;
use crate::errors::ApiError;
use crate::models::user::{LoginUser, User, auth_failure};
use crate::models::{LDAP_PROVIDER_KIND, LOCAL_IDENTITY_SCOPE, LOCAL_PROVIDER_KIND};

const DEFAULT_REFRESH_TTL_SECONDS: i64 = 300;
const DEFAULT_MAX_STALE_SECONDS: i64 = 3600;

static REFRESH_LOCKS: LazyLock<Mutex<HashMap<i32, Arc<Mutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static AUTH_PROVIDER_REGISTRY: OnceLock<Result<AuthProviderRegistry, AuthProviderRegistryError>> =
    OnceLock::new();

#[derive(Debug, Clone)]
enum AuthProviderRegistryError {
    Config(String),
    Internal(String),
}

impl AuthProviderRegistryError {
    fn to_api_error(&self) -> ApiError {
        match self {
            Self::Config(message) => ApiError::InternalServerError(format!(
                "Auth provider configuration error: {message}"
            )),
            Self::Internal(message) => ApiError::InternalServerError(message.clone()),
        }
    }
}

impl From<ApiError> for AuthProviderRegistryError {
    fn from(value: ApiError) -> Self {
        match value {
            ApiError::BadRequest(message)
            | ApiError::ValidationError(message)
            | ApiError::OperatorMismatch(message)
            | ApiError::InvalidIntegerRange(message) => Self::Config(message),
            other => Self::Internal(other.to_string()),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthProvidersConfig {
    #[serde(default)]
    pub ldap: Vec<ConfiguredLdapScope>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConfiguredLdapScope {
    #[serde(flatten)]
    pub ldap: LdapScopeConfig,
    pub refresh_ttl_seconds: Option<i64>,
    pub max_stale_seconds: Option<i64>,
}

impl ConfiguredLdapScope {
    pub fn refresh_ttl_seconds(&self) -> i64 {
        self.refresh_ttl_seconds
            .unwrap_or(DEFAULT_REFRESH_TTL_SECONDS)
    }

    pub fn max_stale_seconds(&self) -> i64 {
        self.max_stale_seconds.unwrap_or(DEFAULT_MAX_STALE_SECONDS)
    }
}

struct AuthProviderRegistry {
    ldap: HashMap<String, ConfiguredLdapProvider>,
}

struct ConfiguredLdapProvider {
    scope: String,
    refresh_ttl_seconds: i64,
    max_stale_seconds: i64,
    provider: LdapIdentityProvider,
}

impl AuthProviderRegistry {
    fn from_config(config: AuthProvidersConfig) -> Result<Self, ApiError> {
        let mut ldap = HashMap::new();
        for configured in config.ldap {
            let provider = ConfiguredLdapProvider::new(configured)?;
            if provider.scope == LOCAL_IDENTITY_SCOPE {
                return Err(ApiError::BadRequest(
                    "external auth provider scope must not be 'local'".to_string(),
                ));
            }
            if ldap.insert(provider.scope.clone(), provider).is_some() {
                return Err(ApiError::BadRequest(
                    "duplicate external auth provider scope".to_string(),
                ));
            }
        }
        Ok(Self { ldap })
    }

    fn ldap_scope(&self, scope: &str) -> Result<&ConfiguredLdapProvider, ApiError> {
        self.ldap
            .get(scope)
            .ok_or_else(|| ApiError::Unauthorized("Authentication failure".to_string()))
    }

    fn scopes(&self) -> impl Iterator<Item = &ConfiguredLdapProvider> {
        self.ldap.values()
    }
}

impl ConfiguredLdapProvider {
    fn new(configured: ConfiguredLdapScope) -> Result<Self, ApiError> {
        let refresh_ttl_seconds = configured.refresh_ttl_seconds();
        let max_stale_seconds = configured.max_stale_seconds();
        if refresh_ttl_seconds <= 0 {
            return Err(ApiError::BadRequest(
                "ldap refresh_ttl_seconds must be positive".to_string(),
            ));
        }
        if max_stale_seconds <= 0 {
            return Err(ApiError::BadRequest(
                "ldap max_stale_seconds must be positive".to_string(),
            ));
        }
        let scope = configured.ldap.scope.clone();
        let provider = LdapIdentityProvider::new(configured.ldap).map_err(provider_config_error)?;
        Ok(Self {
            scope,
            refresh_ttl_seconds,
            max_stale_seconds,
            provider,
        })
    }
}

pub async fn login(pool: &DbPool, login: LoginUser) -> Result<User, ApiError> {
    let scope = login
        .identity_scope
        .as_deref()
        .unwrap_or(LOCAL_IDENTITY_SCOPE)
        .to_string();
    if scope == LOCAL_IDENTITY_SCOPE {
        return login.login(pool).await;
    }

    let configured = auth_provider_registry()?.ldap_scope(&scope)?;
    let authenticated = configured
        .provider
        .authenticate(&login.name, &login.password)
        .await
        .map_err(login_provider_error)?;
    sync_external_user_from_configured_provider(pool, configured, authenticated).await
}

pub async fn refresh_principal_if_needed(pool: &DbPool, principal_id: i32) -> Result<(), ApiError> {
    let Some(state) = external_user_state(pool, principal_id).await? else {
        return Ok(());
    };

    if !refresh_due(state.last_sync_success_at, state.refresh_ttl_seconds) {
        return Ok(());
    }

    let lock = {
        let mut locks = REFRESH_LOCKS.lock().await;
        locks
            .entry(principal_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    };
    let refresh_result = {
        let _guard = lock.lock().await;

        match external_user_state(pool, principal_id).await {
            Err(err) => Err(err),
            Ok(None) => Ok(()),
            Ok(Some(state))
                if !refresh_due(state.last_sync_success_at, state.refresh_ttl_seconds) =>
            {
                Ok(())
            }
            Ok(Some(state)) => match auth_provider_registry()
                .and_then(|registry| registry.ldap_scope(&state.identity_scope))
            {
                Err(err) => Err(err),
                Ok(configured) => match configured
                    .provider
                    .refresh_user(&state.external_subject)
                    .await
                {
                    Ok(refreshed) => {
                        sync_external_user_from_configured_provider(pool, configured, refreshed)
                            .await
                            .map(|_| ())
                    }
                    Err(err) => match mark_external_sync_attempted(pool, principal_id) {
                        Err(mark_err) => Err(mark_err),
                        Ok(()) => {
                            if within_max_stale(state.last_sync_success_at, state.max_stale_seconds)
                            {
                                tracing::warn!(
                                    principal_id,
                                    identity_scope = state.identity_scope,
                                    error = %err,
                                    "External identity refresh failed; using cached memberships inside max-stale window"
                                );
                                Ok(())
                            } else {
                                Err(ApiError::ServiceUnavailable(
                                    "External identity provider is unavailable and cached memberships are stale"
                                        .to_string(),
                                ))
                            }
                        }
                    },
                },
            },
        }
    };

    {
        let mut locks = REFRESH_LOCKS.lock().await;
        if Arc::strong_count(&lock) <= 2 {
            locks.remove(&principal_id);
        }
    }

    refresh_result
}

pub async fn ensure_configured_identity_scopes(pool: &DbPool) -> Result<(), ApiError> {
    ensure_identity_scope(pool, LOCAL_IDENTITY_SCOPE, LOCAL_PROVIDER_KIND).await?;
    for scope in auth_provider_registry()?.scopes() {
        ensure_identity_scope(pool, &scope.scope, LDAP_PROVIDER_KIND).await?;
    }
    Ok(())
}

fn auth_provider_registry() -> Result<&'static AuthProviderRegistry, ApiError> {
    match AUTH_PROVIDER_REGISTRY
        .get_or_init(|| build_auth_provider_registry().map_err(AuthProviderRegistryError::from))
    {
        Ok(registry) => Ok(registry),
        Err(err) => Err(err.to_api_error()),
    }
}

fn build_auth_provider_registry() -> Result<AuthProviderRegistry, ApiError> {
    AuthProviderRegistry::from_config(load_auth_config()?)
}

fn load_auth_config() -> Result<AuthProvidersConfig, ApiError> {
    let Some(path) = crate::config::get_config()?.auth_config_path.clone() else {
        return Ok(AuthProvidersConfig { ldap: Vec::new() });
    };
    let raw = std::fs::read_to_string(Path::new(&path)).map_err(|e| {
        ApiError::InternalServerError(format!("Failed to read auth config '{path}': {e}"))
    })?;
    toml::from_str::<AuthProvidersConfig>(&raw).map_err(|e| {
        ApiError::InternalServerError(format!("Failed to parse auth config '{path}': {e}"))
    })
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

fn refresh_due(last_success: Option<NaiveDateTime>, ttl_seconds: i64) -> bool {
    let Some(last_success) = last_success else {
        return true;
    };
    now() - last_success >= chrono::Duration::seconds(ttl_seconds)
}

fn within_max_stale(last_success: Option<NaiveDateTime>, max_stale_seconds: i64) -> bool {
    let Some(last_success) = last_success else {
        return false;
    };
    now() - last_success < chrono::Duration::seconds(max_stale_seconds)
}

struct ExternalUserState {
    identity_scope: String,
    external_subject: String,
    last_sync_success_at: Option<NaiveDateTime>,
    refresh_ttl_seconds: i64,
    max_stale_seconds: i64,
}

async fn external_user_state(
    pool: &DbPool,
    principal_id_value: i32,
) -> Result<Option<ExternalUserState>, ApiError> {
    let Some(state) = external_principal_state(pool, principal_id_value).await? else {
        return Ok(None);
    };
    let configured = auth_provider_registry()?.ldap_scope(&state.identity_scope)?;
    Ok(Some(ExternalUserState {
        identity_scope: state.identity_scope,
        external_subject: state.external_subject,
        last_sync_success_at: state.last_sync_success_at,
        refresh_ttl_seconds: configured.refresh_ttl_seconds,
        max_stale_seconds: configured.max_stale_seconds,
    }))
}

async fn sync_external_user_from_configured_provider(
    pool: &DbPool,
    configured: &ConfiguredLdapProvider,
    authenticated: AuthenticatedExternalUser,
) -> Result<User, ApiError> {
    sync_external_user_from_backend(pool, &configured.scope, LDAP_PROVIDER_KIND, authenticated)
        .await
}

#[cfg(test)]
pub(crate) async fn sync_external_user(
    pool: &DbPool,
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
}
