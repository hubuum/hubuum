use chrono::NaiveDateTime;
#[cfg(test)]
use hubuum_auth_core::AuthenticatedExternalUser;
use hubuum_auth_core::{AuthProviderError, ExternalIdentityProvider};
use hubuum_auth_ldap::{LdapIdentityProvider, LdapScopeConfig};
use serde::Deserialize;
use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
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
    providers: HashMap<String, RegisteredAuthProvider>,
}

type AuthProviderFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ApiError>> + Send + 'a>>;
type AuthProviderRefreshFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), AuthProviderRefreshError>> + Send + 'a>>;

enum AuthProviderRefreshError {
    Provider(ApiError),
    Internal(ApiError),
}

trait AuthProviderBackend: Send + Sync {
    fn authenticate<'a>(
        &'a self,
        pool: &'a DbPool,
        login: LoginUser,
    ) -> AuthProviderFuture<'a, User>;

    fn refresh<'a>(
        &'a self,
        pool: &'a DbPool,
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
struct RefreshPolicy {
    refresh_ttl_seconds: i64,
    max_stale_seconds: i64,
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
        };
        LocalAuthProvider::register(&mut registry)?;
        for configured in config.ldap {
            LdapAuthProvider::register(configured, &mut registry)?;
        }
        Ok(registry)
    }

    fn register(&mut self, provider: RegisteredAuthProvider) -> Result<(), ApiError> {
        let name = provider.name.clone();
        if self.providers.insert(name.clone(), provider).is_some() {
            return Err(ApiError::BadRequest(format!(
                "duplicate auth provider name '{name}'"
            )));
        }
        Ok(())
    }

    fn provider_names(&self) -> Vec<String> {
        let mut providers = self.providers.values().collect::<Vec<_>>();
        providers.sort_unstable_by(|left, right| {
            left.display_order
                .cmp(&right.display_order)
                .then_with(|| left.name.cmp(&right.name))
        });
        providers
            .into_iter()
            .map(|provider| provider.name.clone())
            .collect()
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
        pool: &'a DbPool,
        login: LoginUser,
    ) -> AuthProviderFuture<'a, User> {
        Box::pin(async move { login.login(pool).await })
    }

    fn refresh<'a>(
        &'a self,
        _pool: &'a DbPool,
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
        registry.register(RegisteredAuthProvider {
            name: scope.clone(),
            kind: LDAP_PROVIDER_KIND.to_string(),
            display_order: 100,
            refresh_policy: Some(RefreshPolicy {
                refresh_ttl_seconds,
                max_stale_seconds,
            }),
            backend: Box::new(Self { scope, provider }),
        })
    }
}

impl AuthProviderBackend for LdapAuthProvider {
    fn authenticate<'a>(
        &'a self,
        pool: &'a DbPool,
        login: LoginUser,
    ) -> AuthProviderFuture<'a, User> {
        Box::pin(async move {
            let authenticated = self
                .provider
                .authenticate(&login.name, &login.password)
                .await
                .map_err(login_provider_error)?;
            sync_external_user_from_backend(pool, &self.scope, LDAP_PROVIDER_KIND, authenticated)
                .await
        })
    }

    fn refresh<'a>(
        &'a self,
        pool: &'a DbPool,
        state: &'a ExternalUserState,
    ) -> AuthProviderRefreshFuture<'a> {
        Box::pin(async move {
            let refreshed = self
                .provider
                .refresh_user(&state.external_subject)
                .await
                .map_err(|error| AuthProviderRefreshError::Provider(provider_error(error)))?;
            sync_external_user_from_backend(pool, &self.scope, LDAP_PROVIDER_KIND, refreshed)
                .await
                .map(|_| ())
                .map_err(AuthProviderRefreshError::Internal)
        })
    }
}

pub async fn login(pool: &DbPool, login: LoginUser) -> Result<User, ApiError> {
    let scope = login
        .identity_scope
        .as_deref()
        .unwrap_or(LOCAL_IDENTITY_SCOPE)
        .to_string();
    auth_provider_registry()?
        .provider(&scope)?
        .backend
        .authenticate(pool, login)
        .await
}

pub async fn refresh_principal_if_needed(pool: &DbPool, principal_id: i32) -> Result<(), ApiError> {
    let Some(state) = external_user_state(pool, principal_id).await? else {
        return Ok(());
    };

    match refresh_status(&state) {
        RefreshStatus::Fresh => return Ok(()),
        RefreshStatus::Backoff => return cached_external_state_result(&state),
        RefreshStatus::Due => {}
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
            Ok(Some(state)) => match refresh_status(&state) {
                RefreshStatus::Fresh => Ok(()),
                RefreshStatus::Backoff => cached_external_state_result(&state),
                RefreshStatus::Due => match auth_provider_registry()
                    .and_then(|registry| registry.provider(&state.identity_scope))
                {
                    Err(err) => Err(err),
                    Ok(configured) => match configured.backend.refresh(pool, &state).await {
                        Ok(()) => Ok(()),
                        Err(AuthProviderRefreshError::Internal(err)) => Err(err),
                        Err(AuthProviderRefreshError::Provider(err)) => {
                            match mark_external_sync_attempted(pool, principal_id) {
                                Err(mark_err) => Err(mark_err),
                                Ok(()) => {
                                    if within_max_stale(
                                        state.last_sync_success_at,
                                        state.max_stale_seconds,
                                    ) {
                                        tracing::warn!(
                                            principal_id,
                                            identity_scope = state.identity_scope,
                                            error = %err,
                                            "External identity refresh failed; using cached memberships inside max-stale window"
                                        );
                                        Ok(())
                                    } else {
                                        stale_external_state_error()
                                    }
                                }
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
    for provider in auth_provider_registry()?.providers() {
        ensure_identity_scope(pool, &provider.name, &provider.kind).await?;
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
        state.refresh_ttl_seconds,
    )
}

fn refresh_status_at(
    current: NaiveDateTime,
    last_success: Option<NaiveDateTime>,
    last_attempt: Option<NaiveDateTime>,
    ttl_seconds: i64,
) -> RefreshStatus {
    let ttl = chrono::Duration::seconds(ttl_seconds);
    if last_success.is_some_and(|success| current - success < ttl) {
        return RefreshStatus::Fresh;
    }

    let failed_attempt = match (last_attempt, last_success) {
        (Some(attempt), Some(success)) => (attempt > success).then_some(attempt),
        (Some(attempt), None) => Some(attempt),
        _ => None,
    };
    if failed_attempt.is_some_and(|attempt| current - attempt < ttl) {
        RefreshStatus::Backoff
    } else {
        RefreshStatus::Due
    }
}

fn within_max_stale(last_success: Option<NaiveDateTime>, max_stale_seconds: i64) -> bool {
    within_max_stale_at(now(), last_success, max_stale_seconds)
}

fn within_max_stale_at(
    current: NaiveDateTime,
    last_success: Option<NaiveDateTime>,
    max_stale_seconds: i64,
) -> bool {
    let Some(last_success) = last_success else {
        return false;
    };
    current - last_success < chrono::Duration::seconds(max_stale_seconds)
}

fn cached_external_state_result(state: &ExternalUserState) -> Result<(), ApiError> {
    cached_external_state_result_at(now(), state)
}

fn cached_external_state_result_at(
    current: NaiveDateTime,
    state: &ExternalUserState,
) -> Result<(), ApiError> {
    if within_max_stale_at(current, state.last_sync_success_at, state.max_stale_seconds) {
        tracing::debug!(
            identity_scope = state.identity_scope,
            "External identity refresh is in retry backoff; using cached memberships"
        );
        Ok(())
    } else {
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
    external_subject: String,
    last_sync_attempted_at: Option<NaiveDateTime>,
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
    let configured = auth_provider_registry()?.provider(&state.identity_scope)?;
    let refresh_policy = configured.refresh_policy.ok_or_else(|| {
        ApiError::InternalServerError(format!(
            "Authentication provider '{}' does not support external refresh",
            configured.name
        ))
    })?;
    Ok(Some(ExternalUserState {
        identity_scope: state.identity_scope,
        external_subject: state.external_subject,
        last_sync_attempted_at: state.last_sync_attempted_at,
        last_sync_success_at: state.last_sync_success_at,
        refresh_ttl_seconds: refresh_policy.refresh_ttl_seconds,
        max_stale_seconds: refresh_policy.max_stale_seconds,
    }))
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

#[cfg(test)]
mod tests {
    use super::*;
    use hubuum_auth_ldap::{LdapScopeConfig, LdapSearchScope};

    fn ldap_scope(scope: &str) -> ConfiguredLdapScope {
        ConfiguredLdapScope {
            ldap: LdapScopeConfig {
                scope: scope.to_string(),
                url: "ldap://ldap.example.org".to_string(),
                bind_dn: None,
                bind_password: None,
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

    fn timestamp() -> NaiveDateTime {
        chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
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
    fn recent_success_keeps_external_state_fresh() {
        let current = timestamp();
        let success = current - chrono::Duration::seconds(299);

        assert_eq!(
            refresh_status_at(current, Some(success), Some(success), 300),
            RefreshStatus::Fresh
        );
    }

    #[test]
    fn failed_refresh_enters_retry_backoff() {
        let current = timestamp();
        let success = current - chrono::Duration::seconds(600);
        let failed_attempt = current - chrono::Duration::seconds(1);

        assert_eq!(
            refresh_status_at(current, Some(success), Some(failed_attempt), 300),
            RefreshStatus::Backoff
        );
    }

    #[test]
    fn refresh_is_due_after_retry_backoff_expires() {
        let current = timestamp();
        let success = current - chrono::Duration::seconds(900);
        let failed_attempt = current - chrono::Duration::seconds(300);

        assert_eq!(
            refresh_status_at(current, Some(success), Some(failed_attempt), 300),
            RefreshStatus::Due
        );
    }

    #[test]
    fn successful_attempt_timestamp_does_not_trigger_backoff() {
        let current = timestamp();
        let success = current - chrono::Duration::seconds(300);

        assert_eq!(
            refresh_status_at(current, Some(success), Some(success), 300),
            RefreshStatus::Due
        );
    }

    #[test]
    fn retry_backoff_rejects_cache_beyond_max_stale() {
        let current = timestamp();
        let state = ExternalUserState {
            identity_scope: "directory".to_string(),
            external_subject: "subject".to_string(),
            last_sync_attempted_at: Some(current - chrono::Duration::seconds(1)),
            last_sync_success_at: Some(current - chrono::Duration::seconds(3600)),
            refresh_ttl_seconds: 300,
            max_stale_seconds: 3600,
        };

        assert!(matches!(
            cached_external_state_result_at(current, &state),
            Err(ApiError::ServiceUnavailable(_))
        ));
    }
}
