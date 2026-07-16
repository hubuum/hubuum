use actix_web::{HttpRequest, web};
use ipnet::{Ipv4Net, Ipv6Net};

use crate::config::{LoginRateLimitConfig, login_rate_limit_config};
use crate::errors::ApiError;
use crate::middlewares::client_allowlist::{ProxyTrust, extract_client_ip_from_http_request};

use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
#[cfg(feature = "login-rate-limit-valkey")]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{info, warn};
#[cfg(feature = "login-rate-limit-valkey")]
use uuid::Uuid;

#[cfg(feature = "login-rate-limit-valkey")]
mod valkey;

/// Failure bookkeeping for a single rate-limit scope (a user+IP pair, an IP, or a
/// subnet). A sliding window of recent failures triggers a lockout once it reaches the
/// scope threshold; repeated lockouts back off exponentially.
#[derive(Default)]
struct ScopeState {
    attempts: VecDeque<Instant>,
    locked_until: Option<Instant>,
    lockout_level: u32,
    in_flight: usize,
}

impl ScopeState {
    fn prune(&mut self, now: Instant, window: Duration) {
        while let Some(first) = self.attempts.front() {
            if now.duration_since(*first) > window {
                self.attempts.pop_front();
            } else {
                break;
            }
        }
    }

    fn is_locked(&self, now: Instant) -> bool {
        self.locked_until.is_some_and(|until| now < until)
    }

    fn trigger_lockout(&mut self, now: Instant, cfg: &LoginRateLimitConfig) {
        self.lockout_level = self.lockout_level.saturating_add(1);
        let duration = lockout_duration(
            self.lockout_level,
            cfg.backoff_base_seconds,
            cfg.backoff_max_seconds,
        );
        self.locked_until = Some(now + duration);
        // The window is consumed by the lockout; fresh failures accrue afterwards.
        self.attempts.clear();
    }

    /// Forget escalation once a prior lockout has expired and the scope has then stayed
    /// quiet for a full cool-off (the window). Backoff therefore reflects only sustained,
    /// recent abuse: an attacker resuming immediately after a lockout keeps escalating,
    /// while a scope that goes idle resets to the base lockout. The expired lock marker is
    /// retained until then so the cool-off can be measured.
    fn reset_escalation_if_cooled_off(&mut self, now: Instant, window: Duration) {
        if let Some(until) = self.locked_until
            && now >= until
            && now.duration_since(until) >= window
        {
            self.locked_until = None;
            self.lockout_level = 0;
        }
    }

    /// Record a failed attempt: prune the window, reset escalation after a genuine
    /// cool-off, append the attempt, and lock out if the threshold is reached.
    fn register_failure(
        &mut self,
        now: Instant,
        threshold: usize,
        cfg: &LoginRateLimitConfig,
    ) -> bool {
        let window = Duration::from_secs(cfg.window_seconds);
        self.prune(now, window);
        self.reset_escalation_if_cooled_off(now, window);
        self.attempts.push_back(now);
        if self.attempts.len() >= threshold {
            self.trigger_lockout(now, cfg);
            true
        } else {
            false
        }
    }

    /// Whether this entry still carries useful state and must not be pruned. Besides live
    /// attempts and active lockouts, an *expired* lockout is kept until its cool-off window
    /// elapses, so pruning does not discard escalation that `reset_escalation_if_cooled_off`
    /// would otherwise preserve (and reset only after a genuine cool-off).
    fn is_active(&self, now: Instant, window: Duration) -> bool {
        if !self.attempts.is_empty() || self.is_locked(now) || self.in_flight > 0 {
            return true;
        }
        match self.locked_until {
            Some(until) => now.duration_since(until) < window,
            None => false,
        }
    }
}

static LOGIN_ATTEMPTS: LazyLock<Mutex<HashMap<String, ScopeState>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

const MAX_LOGIN_ATTEMPT_KEYS: usize = 10_000;

#[derive(Clone, Debug)]
pub struct LoginRateLimitStoreSettings {
    backend: LoginRateLimitStoreBackend,
}

#[derive(Clone, Debug)]
enum LoginRateLimitStoreBackend {
    Memory,
    #[cfg(feature = "login-rate-limit-valkey")]
    Valkey {
        url: String,
        prefix: String,
        io_timeout: Duration,
    },
}

impl LoginRateLimitStoreSettings {
    pub fn in_memory() -> Self {
        Self {
            backend: LoginRateLimitStoreBackend::Memory,
        }
    }

    #[cfg(feature = "login-rate-limit-valkey")]
    pub fn valkey(
        url: impl Into<String>,
        prefix: impl Into<String>,
        io_timeout: Duration,
    ) -> Result<Self, String> {
        let url = url.into();
        let prefix = prefix.into();
        if url.trim().is_empty() {
            return Err("login rate-limit Valkey URL must not be empty".to_string());
        }
        if prefix.trim().is_empty() || prefix.contains(['{', '}']) {
            return Err(
                "login rate-limit Valkey prefix must not be empty or contain braces".to_string(),
            );
        }
        if io_timeout.is_zero() {
            return Err(
                "login rate-limit Valkey I/O timeout must be greater than zero".to_string(),
            );
        }
        Ok(Self {
            backend: LoginRateLimitStoreBackend::Valkey {
                url,
                prefix,
                io_timeout,
            },
        })
    }
}

trait LoginRateLimitStore {
    async fn begin(
        &self,
        permit: &LoginAttemptPermit,
        config: &LoginRateLimitConfig,
    ) -> Result<bool, ApiError>;

    async fn finish(
        &self,
        permit: &LoginAttemptPermit,
        outcome: LoginAttemptOutcome,
        config: &LoginRateLimitConfig,
    ) -> Result<Vec<String>, ApiError>;

    async fn snapshot(&self, config: &LoginRateLimitConfig)
    -> Result<Vec<ScopeSnapshot>, ApiError>;

    async fn release_entry(&self, key: &str) -> Result<bool, ApiError>;

    async fn clear_all(&self) -> Result<usize, ApiError>;
}

struct MemoryLoginRateLimitStore;

#[cfg(feature = "login-rate-limit-valkey")]
struct SharedLoginRateLimitStore {
    local: MemoryLoginRateLimitStore,
    valkey: valkey::ValkeyLoginRateLimitStore,
    degraded: AtomicBool,
}

#[cfg(feature = "login-rate-limit-valkey")]
impl SharedLoginRateLimitStore {
    fn new(valkey: valkey::ValkeyLoginRateLimitStore) -> Self {
        Self {
            local: MemoryLoginRateLimitStore,
            valkey,
            degraded: AtomicBool::new(false),
        }
    }

    fn record_failure(&self, operation: &'static str, error: &ApiError) {
        crate::observability::metrics::login_limiter_backend_failure(operation);
        if !self.degraded.swap(true, Ordering::AcqRel) {
            warn!(
                message = "Shared login limiter unavailable; enforcing per-instance limits",
                backend = "valkey",
                operation,
                error = %error,
            );
        }
    }

    fn record_success(&self) {
        if self.degraded.swap(false, Ordering::AcqRel) {
            info!(
                message = "Shared login limiter recovered; resuming cross-instance enforcement",
                backend = "valkey",
            );
        }
    }
}

enum ActiveLoginRateLimitStore {
    Memory(MemoryLoginRateLimitStore),
    #[cfg(feature = "login-rate-limit-valkey")]
    Shared(Box<SharedLoginRateLimitStore>),
}

impl LoginRateLimitStore for ActiveLoginRateLimitStore {
    async fn begin(
        &self,
        permit: &LoginAttemptPermit,
        config: &LoginRateLimitConfig,
    ) -> Result<bool, ApiError> {
        match self {
            Self::Memory(store) => store.begin(permit, config).await,
            #[cfg(feature = "login-rate-limit-valkey")]
            Self::Shared(store) => {
                let locally_available = store.local.begin(permit, config).await?;
                match store.valkey.begin(permit, config).await {
                    Ok(available) => {
                        store.record_success();
                        if !available && locally_available {
                            store
                                .local
                                .finish(permit, LoginAttemptOutcome::Aborted, config)
                                .await?;
                        }
                        // The shared store is authoritative while it is healthy. A local
                        // lockout can be stale when another replica handled an administrative
                        // release, so it must not reject a request that Valkey accepts.
                        Ok(available)
                    }
                    Err(error) => {
                        store.record_failure("begin", &error);
                        Ok(locally_available)
                    }
                }
            }
        }
    }

    async fn finish(
        &self,
        permit: &LoginAttemptPermit,
        outcome: LoginAttemptOutcome,
        config: &LoginRateLimitConfig,
    ) -> Result<Vec<String>, ApiError> {
        match self {
            Self::Memory(store) => store.finish(permit, outcome, config).await,
            #[cfg(feature = "login-rate-limit-valkey")]
            Self::Shared(store) => {
                let local_lockouts = store.local.finish(permit, outcome, config).await?;
                match store.valkey.finish(permit, outcome, config).await {
                    Ok(shared_lockouts) => {
                        store.record_success();
                        Ok(shared_lockouts)
                    }
                    Err(error) => {
                        store.record_failure("finish", &error);
                        Ok(local_lockouts)
                    }
                }
            }
        }
    }

    async fn snapshot(
        &self,
        config: &LoginRateLimitConfig,
    ) -> Result<Vec<ScopeSnapshot>, ApiError> {
        match self {
            Self::Memory(store) => store.snapshot(config).await,
            #[cfg(feature = "login-rate-limit-valkey")]
            Self::Shared(store) => match store.valkey.snapshot(config).await {
                Ok(snapshot) => {
                    store.record_success();
                    Ok(snapshot)
                }
                Err(error) => {
                    store.record_failure("snapshot", &error);
                    Err(error)
                }
            },
        }
    }

    async fn release_entry(&self, key: &str) -> Result<bool, ApiError> {
        match self {
            Self::Memory(store) => store.release_entry(key).await,
            #[cfg(feature = "login-rate-limit-valkey")]
            Self::Shared(store) => match store.valkey.release_entry(key).await {
                Ok(removed) => {
                    store.record_success();
                    store.local.release_entry(key).await?;
                    Ok(removed)
                }
                Err(error) => {
                    store.record_failure("release", &error);
                    Err(error)
                }
            },
        }
    }

    async fn clear_all(&self) -> Result<usize, ApiError> {
        match self {
            Self::Memory(store) => store.clear_all().await,
            #[cfg(feature = "login-rate-limit-valkey")]
            Self::Shared(store) => match store.valkey.clear_all().await {
                Ok(removed) => {
                    store.record_success();
                    store.local.clear_all().await?;
                    Ok(removed)
                }
                Err(error) => {
                    store.record_failure("clear", &error);
                    Err(error)
                }
            },
        }
    }
}

static LOGIN_RATE_LIMIT_STORE: OnceLock<ActiveLoginRateLimitStore> = OnceLock::new();

fn active_store() -> &'static ActiveLoginRateLimitStore {
    LOGIN_RATE_LIMIT_STORE
        .get_or_init(|| ActiveLoginRateLimitStore::Memory(MemoryLoginRateLimitStore))
}

pub async fn initialize_login_rate_limit_store(
    settings: LoginRateLimitStoreSettings,
) -> Result<(), ApiError> {
    let (backend_name, store) = match settings.backend {
        LoginRateLimitStoreBackend::Memory => (
            "memory",
            ActiveLoginRateLimitStore::Memory(MemoryLoginRateLimitStore),
        ),
        #[cfg(feature = "login-rate-limit-valkey")]
        LoginRateLimitStoreBackend::Valkey {
            url,
            prefix,
            io_timeout,
        } => (
            "valkey",
            ActiveLoginRateLimitStore::Shared(Box::new(SharedLoginRateLimitStore::new(
                valkey::ValkeyLoginRateLimitStore::connect(url, prefix, io_timeout).await?,
            ))),
        ),
    };
    LOGIN_RATE_LIMIT_STORE.set(store).map_err(|_| {
        ApiError::InternalServerError("Login rate-limit store was already initialized".to_string())
    })?;
    info!(
        message = "Login rate-limit store initialized",
        backend = backend_name,
    );
    Ok(())
}

/// Exponential backoff lockout duration for a given lockout level, saturating at the
/// configured maximum and immune to shift/multiply overflow.
fn lockout_duration(level: u32, base_seconds: u64, max_seconds: u64) -> Duration {
    let shift = level.saturating_sub(1).min(63);
    let factor = 1u64.checked_shl(shift).unwrap_or(u64::MAX);
    let seconds = base_seconds.saturating_mul(factor).min(max_seconds);
    Duration::from_secs(seconds)
}

fn principal_label(identity_scope: &str, username: &str) -> String {
    format!(
        "{}/{}",
        identity_scope.trim().to_ascii_lowercase(),
        username.trim().to_ascii_lowercase()
    )
}

fn user_ip_key(identity_scope: &str, username: &str, ip_label: &str) -> String {
    format!(
        "u:{}|{}",
        principal_label(identity_scope, username),
        ip_label
    )
}

fn ip_label(client_ip: Option<IpAddr>) -> String {
    client_ip
        .map(|ip| ip.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Aggregate an IP into its subnet label using the configured prefix lengths.
fn subnet_label(ip: IpAddr, cfg: &LoginRateLimitConfig) -> String {
    match ip {
        IpAddr::V4(addr) => Ipv4Net::new(addr, cfg.subnet_prefix_v4)
            .map(|net| net.trunc().to_string())
            .unwrap_or_else(|_| ip.to_string()),
        IpAddr::V6(addr) => Ipv6Net::new(addr, cfg.subnet_prefix_v6)
            .map(|net| net.trunc().to_string())
            .unwrap_or_else(|_| ip.to_string()),
    }
}

fn scope_kind(key: &str) -> &'static str {
    match key.as_bytes().first() {
        Some(b'u') => "principal_ip",
        Some(b'i') => "ip",
        Some(b's') => "subnet",
        _ => "unknown",
    }
}

/// The set of `(key, threshold)` scopes that apply to a login attempt. The user+IP scope
/// always applies; the per-IP and per-subnet scopes apply only when a client IP is known
/// and their thresholds are non-zero (a zero threshold disables that scope).
fn scopes_for(
    identity_scope: &str,
    username: &str,
    client_ip: Option<IpAddr>,
    cfg: &LoginRateLimitConfig,
) -> Vec<(String, usize)> {
    let mut scopes = vec![(
        user_ip_key(identity_scope, username, &ip_label(client_ip)),
        cfg.max_attempts,
    )];

    if let Some(ip) = client_ip {
        if cfg.max_attempts_per_ip > 0 {
            scopes.push((format!("i:{ip}"), cfg.max_attempts_per_ip));
        }
        if cfg.max_attempts_per_subnet > 0 {
            scopes.push((
                format!("s:{}", subnet_label(ip, cfg)),
                cfg.max_attempts_per_subnet,
            ));
        }
    }

    scopes
}

fn prune_login_attempts_map(
    attempts_by_key: &mut HashMap<String, ScopeState>,
    now: Instant,
    window: Duration,
) {
    attempts_by_key.retain(|_, state| {
        state.prune(now, window);
        state.is_active(now, window)
    });
}

/// Reservation for a login attempt. Applicable scope budgets are reserved before
/// password verification starts so concurrent requests cannot all pass the limiter
/// check before any of them records a failure.
pub(crate) struct LoginAttemptPermit {
    scopes: Vec<(String, usize)>,
    user_ip_key: String,
    #[cfg(feature = "login-rate-limit-valkey")]
    reservation_id: Uuid,
    enabled: bool,
}

#[derive(Clone, Copy)]
pub(crate) enum LoginAttemptOutcome {
    Succeeded,
    Failed,
    Aborted,
}

/// Atomically check every applicable scope and reserve capacity for one login.
/// Returns `None` when a scope is locked or has enough failures and concurrent
/// verifications to reach its threshold.
pub(crate) async fn begin_login_attempt(
    identity_scope: &str,
    username: &str,
    client_ip: Option<IpAddr>,
) -> Result<Option<LoginAttemptPermit>, ApiError> {
    let cfg = login_rate_limit_config();
    let scopes = scopes_for(identity_scope, username, client_ip, &cfg);
    let user_ip_key = user_ip_key(identity_scope, username, &ip_label(client_ip));
    let permit = LoginAttemptPermit {
        scopes,
        user_ip_key,
        #[cfg(feature = "login-rate-limit-valkey")]
        reservation_id: Uuid::new_v4(),
        enabled: cfg.enabled,
    };
    if !cfg.enabled {
        return Ok(Some(permit));
    }

    active_store()
        .begin(&permit, &cfg)
        .await
        .map(|available| available.then_some(permit))
}

/// Release a login reservation and update all applicable scope budgets in the same
/// critical section. Internal authentication errors release capacity without counting
/// as credential failures.
pub(crate) async fn finish_login_attempt(
    permit: LoginAttemptPermit,
    outcome: LoginAttemptOutcome,
) -> Result<(), ApiError> {
    if !permit.enabled {
        return Ok(());
    }

    let cfg = login_rate_limit_config();
    for key in active_store().finish(&permit, outcome, &cfg).await? {
        crate::observability::metrics::login_lockout(scope_kind(&key));
    }
    Ok(())
}

impl MemoryLoginRateLimitStore {
    async fn begin_with_max_keys(
        &self,
        permit: &LoginAttemptPermit,
        config: &LoginRateLimitConfig,
        max_keys: usize,
    ) -> Result<bool, ApiError> {
        let now = Instant::now();
        let window = Duration::from_secs(config.window_seconds);
        let mut guard = LOGIN_ATTEMPTS.lock().await;
        prune_login_attempts_map(&mut guard, now, window);

        let unavailable = permit.scopes.iter().any(|(key, threshold)| {
            guard.get(key).is_some_and(|state| {
                state.is_locked(now)
                    || state.attempts.len().saturating_add(state.in_flight) >= *threshold
            })
        });
        if unavailable {
            return Ok(false);
        }

        let missing_scopes = permit
            .scopes
            .iter()
            .filter(|(key, _)| !guard.contains_key(key))
            .count();
        if guard.len().saturating_add(missing_scopes) > max_keys {
            // Every inactive entry was removed by `prune_login_attempts_map`.
            // Reject new high-cardinality scopes instead of discarding live
            // failures, lockouts, cool-off state, or reservations.
            return Ok(false);
        }

        for (key, _) in &permit.scopes {
            let state = guard.entry(key.clone()).or_default();
            state.in_flight = state.in_flight.saturating_add(1);
        }
        Ok(true)
    }
}

impl LoginRateLimitStore for MemoryLoginRateLimitStore {
    async fn begin(
        &self,
        permit: &LoginAttemptPermit,
        config: &LoginRateLimitConfig,
    ) -> Result<bool, ApiError> {
        self.begin_with_max_keys(permit, config, MAX_LOGIN_ATTEMPT_KEYS)
            .await
    }

    async fn finish(
        &self,
        permit: &LoginAttemptPermit,
        outcome: LoginAttemptOutcome,
        config: &LoginRateLimitConfig,
    ) -> Result<Vec<String>, ApiError> {
        let now = Instant::now();
        let window = Duration::from_secs(config.window_seconds);
        let mut guard = LOGIN_ATTEMPTS.lock().await;
        let mut lockouts = Vec::new();

        for (key, threshold) in &permit.scopes {
            if let Some(state) = guard.get_mut(key) {
                state.in_flight = state.in_flight.saturating_sub(1);
                if matches!(outcome, LoginAttemptOutcome::Failed)
                    && state.register_failure(now, *threshold, config)
                {
                    lockouts.push(key.clone());
                }
            }
        }

        if matches!(outcome, LoginAttemptOutcome::Succeeded) {
            let should_remove = if let Some(state) = guard.get_mut(&permit.user_ip_key) {
                state.attempts.clear();
                state.locked_until = None;
                state.lockout_level = 0;
                state.in_flight == 0
            } else {
                false
            };
            if should_remove {
                guard.remove(&permit.user_ip_key);
            }
        }

        prune_login_attempts_map(&mut guard, now, window);
        Ok(lockouts)
    }

    async fn snapshot(
        &self,
        config: &LoginRateLimitConfig,
    ) -> Result<Vec<ScopeSnapshot>, ApiError> {
        let window = Duration::from_secs(config.window_seconds);
        let now = Instant::now();
        let guard = LOGIN_ATTEMPTS.lock().await;

        Ok(guard
            .iter()
            .map(|(key, state)| {
                let attempts = state
                    .attempts
                    .iter()
                    .filter(|at| now.duration_since(**at) <= window)
                    .count();
                let locked_for = state
                    .locked_until
                    .filter(|until| now < *until)
                    .map(|until| until.duration_since(now));
                ScopeSnapshot {
                    key: key.clone(),
                    attempts,
                    locked: locked_for.is_some(),
                    locked_for,
                    lockout_level: state.lockout_level,
                }
            })
            .collect())
    }

    async fn release_entry(&self, key: &str) -> Result<bool, ApiError> {
        Ok(LOGIN_ATTEMPTS.lock().await.remove(key).is_some())
    }

    async fn clear_all(&self) -> Result<usize, ApiError> {
        let mut guard = LOGIN_ATTEMPTS.lock().await;
        let removed = guard.len();
        guard.clear();
        Ok(removed)
    }
}

/// Resolve the trustworthy client IP for a login request, honoring the configured proxy
/// trust policy. Returns `None` when no address can be determined.
pub(crate) fn client_ip_for_request(req: &HttpRequest) -> Option<IpAddr> {
    match req.app_data::<web::Data<ProxyTrust>>() {
        Some(policy) => extract_client_ip_from_http_request(req, policy.get_ref()),
        None => extract_client_ip_from_http_request(req, &ProxyTrust::default()),
    }
}

/// Record a failed login attempt across all applicable scopes, applying lockouts with
/// exponential backoff when a scope crosses its threshold.
#[cfg(feature = "integration-test-support")]
pub(crate) async fn record_login_failure(
    identity_scope: &str,
    username: &str,
    client_ip: Option<IpAddr>,
) {
    let cfg = login_rate_limit_config();
    if !cfg.enabled {
        return;
    }

    let now = Instant::now();
    let window = Duration::from_secs(cfg.window_seconds);
    let mut guard = LOGIN_ATTEMPTS.lock().await;

    for (key, threshold) in scopes_for(identity_scope, username, client_ip, &cfg) {
        if !guard.contains_key(&key) && guard.len() >= MAX_LOGIN_ATTEMPT_KEYS {
            prune_login_attempts_map(&mut guard, now, window);
            if guard.len() >= MAX_LOGIN_ATTEMPT_KEYS {
                continue;
            }
        }

        let scope = scope_kind(&key);
        let state = guard.entry(key).or_default();
        if state.register_failure(now, threshold, &cfg) {
            crate::observability::metrics::login_lockout(scope);
        }
    }
}

/// A point-in-time view of one tracked rate-limit scope, for the admin observability API.
pub(crate) struct ScopeSnapshot {
    /// Raw internal map key (e.g. `u:alice|1.2.3.4`, `i:1.2.3.4`, `s:1.2.3.0/24`).
    pub key: String,
    /// Failed attempts currently inside the sliding window.
    pub attempts: usize,
    /// Whether the scope is locked out right now.
    pub locked: bool,
    /// Remaining lockout time, if currently locked.
    pub locked_for: Option<Duration>,
    /// Current exponential-backoff level.
    pub lockout_level: u32,
}

/// Snapshot every tracked scope for the admin API. Read-only: the live window count is
/// computed without mutating state, and remaining lockout time is derived from the
/// monotonic clock.
pub(crate) async fn snapshot() -> Result<Vec<ScopeSnapshot>, ApiError> {
    let cfg = login_rate_limit_config();
    active_store().snapshot(&cfg).await
}

/// Release a single tracked scope by its raw key. Returns whether an entry was removed.
pub(crate) async fn release_entry(key: &str) -> Result<bool, ApiError> {
    active_store().release_entry(key).await
}

/// Clear all tracked scopes. Returns the number of entries removed.
pub(crate) async fn clear_all() -> Result<usize, ApiError> {
    active_store().clear_all().await
}

/// Serializes tests that touch the process-global limiter state (auth login tests and the
/// admin `/meta/login-rate-limit` tests) so they do not observe each other's failures.
#[cfg(any(test, feature = "integration-test-support"))]
pub static LOGIN_RATE_LIMIT_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[cfg(any(test, feature = "integration-test-support"))]
pub(crate) async fn reset_login_rate_limit_for_tests() {
    LOGIN_ATTEMPTS.lock().await.clear();
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use std::net::Ipv4Addr;

    #[rstest]
    #[case("u:local/alice|192.0.2.1", "principal_ip")]
    #[case("i:192.0.2.1", "ip")]
    #[case("s:192.0.2.0/24", "subnet")]
    #[case("x:unexpected", "unknown")]
    fn limiter_keys_have_bounded_metric_scope_kinds(
        #[case] key: &str,
        #[case] expected: &'static str,
    ) {
        assert_eq!(scope_kind(key), expected);
    }

    fn cfg() -> LoginRateLimitConfig {
        LoginRateLimitConfig {
            enabled: true,
            max_attempts: 5,
            max_attempts_per_ip: 20,
            max_attempts_per_subnet: 100,
            window_seconds: 300,
            backoff_base_seconds: 300,
            backoff_max_seconds: 86_400,
            subnet_prefix_v4: 24,
            subnet_prefix_v6: 64,
        }
    }

    fn permit_for(username: &str, config: &LoginRateLimitConfig) -> LoginAttemptPermit {
        LoginAttemptPermit {
            scopes: scopes_for("local", username, None, config),
            user_ip_key: user_ip_key("local", username, "unknown"),
            #[cfg(feature = "login-rate-limit-valkey")]
            reservation_id: Uuid::new_v4(),
            enabled: true,
        }
    }

    #[cfg(feature = "login-rate-limit-valkey")]
    async fn shared_local_snapshot(
        store: &ActiveLoginRateLimitStore,
        config: &LoginRateLimitConfig,
    ) -> Vec<ScopeSnapshot> {
        match store {
            ActiveLoginRateLimitStore::Shared(store) => store.local.snapshot(config).await.unwrap(),
            ActiveLoginRateLimitStore::Memory(_) => unreachable!(),
        }
    }

    async fn assert_store_contract(store: &impl LoginRateLimitStore, username: &str) {
        let mut config = cfg();
        config.max_attempts = 2;
        config.max_attempts_per_ip = 0;
        config.max_attempts_per_subnet = 0;
        store.clear_all().await.unwrap();

        for _ in 0..config.max_attempts {
            let permit = permit_for(username, &config);
            assert!(store.begin(&permit, &config).await.unwrap());
            store
                .finish(&permit, LoginAttemptOutcome::Failed, &config)
                .await
                .unwrap();
        }

        let blocked = permit_for(username, &config);
        assert!(!store.begin(&blocked, &config).await.unwrap());
        let snapshots = store.snapshot(&config).await.unwrap();
        assert_eq!(snapshots.len(), 1);
        assert!(snapshots[0].locked);
        assert!(store.release_entry(&blocked.user_ip_key).await.unwrap());

        let released = permit_for(username, &config);
        assert!(store.begin(&released, &config).await.unwrap());
        store
            .finish(&released, LoginAttemptOutcome::Aborted, &config)
            .await
            .unwrap();
        assert_eq!(store.clear_all().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn memory_store_satisfies_limiter_contract() {
        let _guard = LOGIN_RATE_LIMIT_TEST_LOCK.lock().await;
        assert_store_contract(&MemoryLoginRateLimitStore, "memory-contract").await;
    }

    #[cfg(feature = "login-rate-limit-valkey")]
    #[tokio::test]
    #[ignore = "requires Valkey or Redis at redis://127.0.0.1:6379/"]
    async fn valkey_store_satisfies_limiter_contract() {
        let store = valkey::ValkeyLoginRateLimitStore::connect(
            "redis://127.0.0.1:6379/".to_string(),
            format!("hubuum:test-contract:{}", Uuid::new_v4()),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_store_contract(&store, "valkey-contract").await;
    }

    #[cfg(feature = "login-rate-limit-valkey")]
    #[tokio::test]
    #[ignore = "requires Valkey or Redis at redis://127.0.0.1:6379/"]
    async fn valkey_capacity_does_not_evict_in_flight_reservations() {
        let mut config = cfg();
        config.max_attempts_per_ip = 0;
        config.max_attempts_per_subnet = 0;
        let store = valkey::ValkeyLoginRateLimitStore::connect(
            "redis://127.0.0.1:6379/".to_string(),
            format!("hubuum:test-capacity-inflight:{}", Uuid::new_v4()),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        let protected = permit_for("protected-inflight", &config);
        let newcomer = permit_for("newcomer", &config);

        assert!(
            store
                .begin_with_max_keys(&protected, &config, 1)
                .await
                .unwrap()
        );
        assert!(
            !store
                .begin_with_max_keys(&newcomer, &config, 1)
                .await
                .unwrap()
        );
        let snapshots = store.snapshot(&config).await.unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].key, protected.user_ip_key);

        store.clear_all().await.unwrap();
    }

    #[cfg(feature = "login-rate-limit-valkey")]
    #[tokio::test]
    #[ignore = "requires Valkey or Redis at redis://127.0.0.1:6379/"]
    async fn valkey_capacity_does_not_evict_active_lockouts() {
        let mut config = cfg();
        config.max_attempts = 1;
        config.max_attempts_per_ip = 0;
        config.max_attempts_per_subnet = 0;
        let store = valkey::ValkeyLoginRateLimitStore::connect(
            "redis://127.0.0.1:6379/".to_string(),
            format!("hubuum:test-capacity-lock:{}", Uuid::new_v4()),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        let protected = permit_for("protected-lock", &config);
        let newcomer = permit_for("newcomer", &config);

        assert!(
            store
                .begin_with_max_keys(&protected, &config, 1)
                .await
                .unwrap()
        );
        store
            .finish(&protected, LoginAttemptOutcome::Failed, &config)
            .await
            .unwrap();
        assert!(
            !store
                .begin_with_max_keys(&newcomer, &config, 1)
                .await
                .unwrap()
        );
        let snapshots = store.snapshot(&config).await.unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].key, protected.user_ip_key);
        assert!(snapshots[0].locked);

        store.clear_all().await.unwrap();
    }

    #[cfg(feature = "login-rate-limit-valkey")]
    #[tokio::test]
    #[ignore = "requires Valkey or Redis at redis://127.0.0.1:6379/"]
    async fn shared_store_accepts_remote_admin_release_despite_stale_local_lockout() {
        let _guard = LOGIN_RATE_LIMIT_TEST_LOCK.lock().await;
        reset_login_rate_limit_for_tests().await;
        let mut config = cfg();
        config.max_attempts = 2;
        config.max_attempts_per_ip = 0;
        config.max_attempts_per_subnet = 0;
        let prefix = format!("hubuum:test-remote-release:{}", Uuid::new_v4());
        let shared = ActiveLoginRateLimitStore::Shared(Box::new(SharedLoginRateLimitStore::new(
            valkey::ValkeyLoginRateLimitStore::connect(
                "redis://127.0.0.1:6379/".to_string(),
                prefix.clone(),
                Duration::from_secs(1),
            )
            .await
            .unwrap(),
        )));
        let remote_admin = valkey::ValkeyLoginRateLimitStore::connect(
            "redis://127.0.0.1:6379/".to_string(),
            prefix,
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        for _ in 0..config.max_attempts {
            let permit = permit_for("remote-release", &config);
            assert!(shared.begin(&permit, &config).await.unwrap());
            shared
                .finish(&permit, LoginAttemptOutcome::Failed, &config)
                .await
                .unwrap();
        }

        let blocked = permit_for("remote-release", &config);
        assert!(!shared.begin(&blocked, &config).await.unwrap());
        assert!(
            remote_admin
                .release_entry(&blocked.user_ip_key)
                .await
                .unwrap()
        );
        assert!(shared_local_snapshot(&shared, &config).await[0].locked);

        let released = permit_for("remote-release", &config);
        assert!(shared.begin(&released, &config).await.unwrap());
        shared
            .finish(&released, LoginAttemptOutcome::Aborted, &config)
            .await
            .unwrap();

        remote_admin.clear_all().await.unwrap();
        reset_login_rate_limit_for_tests().await;
    }

    #[test]
    fn lockout_duration_doubles_and_caps() {
        assert_eq!(lockout_duration(1, 300, 86_400), Duration::from_secs(300));
        assert_eq!(lockout_duration(2, 300, 86_400), Duration::from_secs(600));
        assert_eq!(lockout_duration(3, 300, 86_400), Duration::from_secs(1200));
        // Caps at the maximum rather than overflowing.
        assert_eq!(
            lockout_duration(64, 300, 86_400),
            Duration::from_secs(86_400)
        );
        assert_eq!(
            lockout_duration(1_000_000, 300, 86_400),
            Duration::from_secs(86_400)
        );
    }

    #[test]
    fn scope_locks_after_reaching_threshold() {
        let now = Instant::now();
        let window = Duration::from_secs(300);
        let mut state = ScopeState::default();
        let config = cfg();

        for _ in 0..config.max_attempts {
            state.prune(now, window);
            state.attempts.push_back(now);
            if state.attempts.len() >= config.max_attempts {
                state.trigger_lockout(now, &config);
            }
        }

        assert!(state.is_locked(now));
        // Lockout clears the window so the level (not the count) drives further backoff.
        assert!(state.attempts.is_empty());
        assert_eq!(state.lockout_level, 1);
    }

    #[test]
    fn repeated_lockouts_increase_backoff_level() {
        let now = Instant::now();
        let config = cfg();
        let mut state = ScopeState::default();

        state.trigger_lockout(now, &config);
        let first = state.locked_until.unwrap();
        state.trigger_lockout(now, &config);
        let second = state.locked_until.unwrap();

        assert_eq!(state.lockout_level, 2);
        assert!(second > first);
    }

    #[test]
    fn backoff_escalates_when_sustained_but_resets_after_cooloff() {
        let config = cfg();
        let window = Duration::from_secs(config.window_seconds);
        let t0 = Instant::now();
        let mut state = ScopeState::default();

        // First episode: reach the threshold -> lockout level 1.
        for _ in 0..config.max_attempts {
            state.register_failure(t0, config.max_attempts, &config);
        }
        assert_eq!(state.lockout_level, 1);

        // Sustained: resume just after the lockout expires (within the cool-off) -> the
        // escalation persists and the next lockout is level 2.
        let resume = state.locked_until.unwrap() + Duration::from_secs(1);
        for _ in 0..config.max_attempts {
            state.register_failure(resume, config.max_attempts, &config);
        }
        assert_eq!(
            state.lockout_level, 2,
            "sustained abuse must keep escalating"
        );

        // Cool-off: stay quiet past expiry + a full window -> escalation resets.
        let cooled = state.locked_until.unwrap() + window + Duration::from_secs(1);
        state.register_failure(cooled, config.max_attempts, &config);
        assert_eq!(state.lockout_level, 0, "a genuine cool-off resets backoff");
        assert!(state.locked_until.is_none());
        assert_eq!(state.attempts.len(), 1);
    }

    #[test]
    fn subnet_label_aggregates_by_prefix() {
        let config = cfg();
        let a = subnet_label(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 10)), &config);
        let b = subnet_label(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 250)), &config);
        assert_eq!(a, b);
        assert_eq!(a, "198.51.100.0/24");
    }

    #[test]
    fn scopes_skip_disabled_and_unknown_ip() {
        let mut config = cfg();
        config.max_attempts_per_ip = 0;
        config.max_attempts_per_subnet = 0;
        let only_user = scopes_for(
            "local",
            "alice",
            Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            &config,
        );
        assert_eq!(only_user.len(), 1);

        let no_ip = scopes_for("local", "alice", None, &cfg());
        assert_eq!(no_ip.len(), 1, "no IP means only the user+IP scope applies");
    }

    #[test]
    fn scopes_cover_user_ip_and_subnet_when_enabled() {
        let scopes = scopes_for(
            "Directory",
            "Alice",
            Some(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 7))),
            &cfg(),
        );
        let keys: Vec<&str> = scopes.iter().map(|(k, _)| k.as_str()).collect();
        assert!(keys.iter().any(|k| k.starts_with("u:directory/alice|")));
        assert!(keys.contains(&"i:198.51.100.7"));
        assert!(keys.contains(&"s:198.51.100.0/24"));
    }

    #[tokio::test]
    async fn memory_capacity_rejects_new_scope_without_evicting_active_lockout() {
        let _guard = LOGIN_RATE_LIMIT_TEST_LOCK.lock().await;
        reset_login_rate_limit_for_tests().await;
        let mut config = cfg();
        config.max_attempts = 1;
        config.max_attempts_per_ip = 0;
        config.max_attempts_per_subnet = 0;
        let store = MemoryLoginRateLimitStore;
        let protected = permit_for("protected-memory-lock", &config);
        let newcomer = permit_for("newcomer-memory-lock", &config);

        assert!(
            store
                .begin_with_max_keys(&protected, &config, 1)
                .await
                .unwrap()
        );
        store
            .finish(&protected, LoginAttemptOutcome::Failed, &config)
            .await
            .unwrap();
        assert!(
            !store
                .begin_with_max_keys(&newcomer, &config, 1)
                .await
                .unwrap()
        );

        let snapshots = store.snapshot(&config).await.unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].key, protected.user_ip_key);
        assert!(snapshots[0].locked);
        reset_login_rate_limit_for_tests().await;
    }

    #[test]
    fn prune_map_keeps_locked_but_drops_expired() {
        let now = Instant::now();
        let window = Duration::from_secs(300);
        let config = cfg();
        let mut map: HashMap<String, ScopeState> = HashMap::new();

        let mut expired = ScopeState::default();
        expired.attempts.push_back(now - Duration::from_secs(600));
        map.insert("expired".to_string(), expired);

        let mut locked = ScopeState::default();
        locked.trigger_lockout(now, &config);
        map.insert("locked".to_string(), locked);

        prune_login_attempts_map(&mut map, now, window);

        assert!(!map.contains_key("expired"));
        assert!(map.contains_key("locked"));
    }

    #[test]
    fn prune_map_retains_expired_lockouts_within_cooloff() {
        let now = Instant::now();
        let window = Duration::from_secs(300);
        let mut map: HashMap<String, ScopeState> = HashMap::new();

        // Lockout expired 10s ago: still inside the cool-off window, so its escalation
        // level must survive pruning.
        let mut cooling = ScopeState {
            locked_until: Some(now - Duration::from_secs(10)),
            lockout_level: 2,
            ..ScopeState::default()
        };
        cooling.attempts.clear();
        map.insert("cooling".to_string(), cooling);

        // Lockout expired 400s ago: past the cool-off, eligible for pruning.
        let cooled = ScopeState {
            locked_until: Some(now - Duration::from_secs(400)),
            lockout_level: 3,
            ..ScopeState::default()
        };
        map.insert("cooled".to_string(), cooled);

        prune_login_attempts_map(&mut map, now, window);

        assert!(map.contains_key("cooling"));
        assert_eq!(map["cooling"].lockout_level, 2);
        assert!(!map.contains_key("cooled"));
    }

    #[tokio::test]
    async fn concurrent_attempts_reserve_capacity_atomically() {
        let _guard = LOGIN_RATE_LIMIT_TEST_LOCK.lock().await;
        reset_login_rate_limit_for_tests().await;
        let config = login_rate_limit_config();
        assert!(config.enabled);

        let mut permits = Vec::new();
        for _ in 0..config.max_attempts {
            permits.push(
                begin_login_attempt("local", "atomic-limit-user", None)
                    .await
                    .expect("limiter store should be available")
                    .expect("capacity below the threshold should be reserved"),
            );
        }
        assert!(
            begin_login_attempt("local", "atomic-limit-user", None)
                .await
                .expect("limiter store should be available")
                .is_none()
        );

        for permit in permits {
            finish_login_attempt(permit, LoginAttemptOutcome::Aborted)
                .await
                .unwrap();
        }
        assert!(
            begin_login_attempt("local", "atomic-limit-user", None)
                .await
                .expect("limiter store should be available")
                .is_some()
        );
        reset_login_rate_limit_for_tests().await;
    }

    #[cfg(feature = "login-rate-limit-valkey")]
    #[tokio::test]
    async fn unavailable_valkey_falls_back_to_local_enforcement() {
        let _guard = LOGIN_RATE_LIMIT_TEST_LOCK.lock().await;
        reset_login_rate_limit_for_tests().await;
        let mut config = cfg();
        config.max_attempts = 2;
        config.max_attempts_per_ip = 0;
        config.max_attempts_per_subnet = 0;
        let shared = ActiveLoginRateLimitStore::Shared(Box::new(SharedLoginRateLimitStore::new(
            valkey::ValkeyLoginRateLimitStore::connect(
                "redis://127.0.0.1:1/".to_string(),
                "hubuum:test-unavailable".to_string(),
                Duration::from_millis(25),
            )
            .await
            .unwrap(),
        )));

        for _ in 0..config.max_attempts {
            let permit = LoginAttemptPermit {
                scopes: scopes_for("local", "fallback-user", None, &config),
                user_ip_key: user_ip_key("local", "fallback-user", "unknown"),
                reservation_id: Uuid::new_v4(),
                enabled: true,
            };
            assert!(shared.begin(&permit, &config).await.unwrap());
            shared
                .finish(&permit, LoginAttemptOutcome::Failed, &config)
                .await
                .unwrap();
        }

        let blocked = LoginAttemptPermit {
            scopes: scopes_for("local", "fallback-user", None, &config),
            user_ip_key: user_ip_key("local", "fallback-user", "unknown"),
            reservation_id: Uuid::new_v4(),
            enabled: true,
        };
        assert!(!shared.begin(&blocked, &config).await.unwrap());

        assert_eq!(shared_local_snapshot(&shared, &config).await.len(), 1);
        assert!(shared.release_entry(&blocked.user_ip_key).await.is_err());
        assert_eq!(shared_local_snapshot(&shared, &config).await.len(), 1);
        assert!(shared.clear_all().await.is_err());
        assert_eq!(shared_local_snapshot(&shared, &config).await.len(), 1);
        reset_login_rate_limit_for_tests().await;
    }
}
