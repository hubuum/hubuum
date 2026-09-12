use actix_web::{HttpRequest, web};
use ipnet::{Ipv4Net, Ipv6Net};

use crate::config::{LoginRateLimitConfig, login_rate_limit_config};
use crate::errors::ApiError;
use crate::middlewares::client_allowlist::{ProxyTrust, extract_client_ip_from_http_request};
#[cfg(feature = "login-rate-limit-valkey")]
use crate::models::REDACTED_DEBUG_VALUE;

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::net::IpAddr;
#[cfg(feature = "login-rate-limit-valkey")]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::info;
#[cfg(feature = "login-rate-limit-valkey")]
use tracing::warn;
#[cfg(all(test, feature = "login-rate-limit-valkey"))]
use uuid::Uuid;

mod reservation;
use reservation::{Reservation, ReservationId, ReservationLease};

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
    in_flight: HashMap<ReservationId, ReservationLease>,
}

impl ScopeState {
    fn prune(&mut self, now: Instant, window: Duration) {
        self.in_flight.retain(|_, lease| lease.is_active(now));
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
        if !self.attempts.is_empty() || self.is_locked(now) || !self.in_flight.is_empty() {
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

#[derive(Clone)]
enum LoginRateLimitStoreBackend {
    Memory,
    #[cfg(feature = "login-rate-limit-valkey")]
    Valkey {
        url: String,
        prefix: String,
        io_timeout: Duration,
    },
}

impl fmt::Debug for LoginRateLimitStoreBackend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory => formatter.write_str("Memory"),
            #[cfg(feature = "login-rate-limit-valkey")]
            Self::Valkey {
                prefix, io_timeout, ..
            } => formatter
                .debug_struct("Valkey")
                .field("url", &REDACTED_DEBUG_VALUE)
                .field("prefix", prefix)
                .field("io_timeout", io_timeout)
                .finish(),
        }
    }
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
struct SharedLoginRateLimitStore<S = valkey::ValkeyLoginRateLimitStore> {
    local: MemoryLoginRateLimitStore,
    valkey: S,
    degraded: AtomicBool,
}

#[cfg(feature = "login-rate-limit-valkey")]
impl<S> SharedLoginRateLimitStore<S> {
    fn new(valkey: S) -> Self {
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
            Self::Shared(store) => store.begin(permit, config).await,
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
            Self::Shared(store) => store.finish(permit, outcome, config).await,
        }
    }
    async fn snapshot(
        &self,
        config: &LoginRateLimitConfig,
    ) -> Result<Vec<ScopeSnapshot>, ApiError> {
        match self {
            Self::Memory(store) => store.snapshot(config).await,
            #[cfg(feature = "login-rate-limit-valkey")]
            Self::Shared(store) => store.snapshot(config).await,
        }
    }
    async fn release_entry(&self, key: &str) -> Result<bool, ApiError> {
        match self {
            Self::Memory(store) => store.release_entry(key).await,
            #[cfg(feature = "login-rate-limit-valkey")]
            Self::Shared(store) => store.release_entry(key).await,
        }
    }
    async fn clear_all(&self) -> Result<usize, ApiError> {
        match self {
            Self::Memory(store) => store.clear_all().await,
            #[cfg(feature = "login-rate-limit-valkey")]
            Self::Shared(store) => store.clear_all().await,
        }
    }
}

#[cfg(feature = "login-rate-limit-valkey")]
impl<S: LoginRateLimitStore> LoginRateLimitStore for SharedLoginRateLimitStore<S> {
    async fn begin(
        &self,
        permit: &LoginAttemptPermit,
        config: &LoginRateLimitConfig,
    ) -> Result<bool, ApiError> {
        let locally_available = self.local.begin(permit, config).await?;
        match self.valkey.begin(permit, config).await {
            Ok(available) => {
                self.record_success();
                if !available && locally_available {
                    self.local
                        .finish(permit, LoginAttemptOutcome::Aborted, config)
                        .await?;
                } else if available && !locally_available {
                    self.local.observe_shared_admission(permit, config).await;
                }
                // The shared store is authoritative while it is healthy. A local
                // lockout can be stale when another replica handled an administrative
                // release, so it must not reject a request that Valkey accepts.
                Ok(available)
            }
            Err(error) => {
                self.record_failure("begin", &error);
                Ok(locally_available)
            }
        }
    }
    async fn finish(
        &self,
        permit: &LoginAttemptPermit,
        outcome: LoginAttemptOutcome,
        config: &LoginRateLimitConfig,
    ) -> Result<Vec<String>, ApiError> {
        let local_lockouts = self.local.finish(permit, outcome, config).await?;
        match self.valkey.finish(permit, outcome, config).await {
            Ok(shared_lockouts) => {
                self.record_success();
                Ok(shared_lockouts)
            }
            Err(error) => {
                self.record_failure("finish", &error);
                Ok(local_lockouts)
            }
        }
    }
    async fn snapshot(
        &self,
        config: &LoginRateLimitConfig,
    ) -> Result<Vec<ScopeSnapshot>, ApiError> {
        match self.valkey.snapshot(config).await {
            Ok(snapshot) => {
                self.record_success();
                Ok(snapshot)
            }
            Err(error) => {
                self.record_failure("snapshot", &error);
                Err(error)
            }
        }
    }
    async fn release_entry(&self, key: &str) -> Result<bool, ApiError> {
        match self.valkey.release_entry(key).await {
            Ok(removed) => {
                self.record_success();
                self.local.release_entry(key).await?;
                Ok(removed)
            }
            Err(error) => {
                self.record_failure("release", &error);
                Err(error)
            }
        }
    }
    async fn clear_all(&self) -> Result<usize, ApiError> {
        match self.valkey.clear_all().await {
            Ok(removed) => {
                self.record_success();
                self.local.clear_all().await?;
                Ok(removed)
            }
            Err(error) => {
                self.record_failure("clear", &error);
                Err(error)
            }
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
    reservation: Reservation,
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
        reservation: Reservation::new(Duration::from_secs(cfg.window_seconds)),
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
    /// A healthy shared admission overrides stale local budgets. Mirror its own
    /// reservation so its result cannot consume another local request's capacity.
    /// Retain the local key bound even when the shared store accepts a new scope.
    #[cfg(feature = "login-rate-limit-valkey")]
    async fn observe_shared_admission(
        &self,
        permit: &LoginAttemptPermit,
        config: &LoginRateLimitConfig,
    ) {
        let mut guard = LOGIN_ATTEMPTS.lock().await;
        prune_login_attempts_map(
            &mut guard,
            Instant::now(),
            Duration::from_secs(config.window_seconds),
        );
        for (key, _) in &permit.scopes {
            if guard.contains_key(key) || guard.len() < MAX_LOGIN_ATTEMPT_KEYS {
                guard
                    .entry(key.clone())
                    .or_default()
                    .in_flight
                    .insert(permit.reservation.id(), permit.reservation.lease());
            }
        }
    }

    async fn begin_with_max_keys(
        &self,
        permit: &LoginAttemptPermit,
        config: &LoginRateLimitConfig,
        max_keys: usize,
    ) -> Result<bool, ApiError> {
        let window = Duration::from_secs(config.window_seconds);
        let mut guard = LOGIN_ATTEMPTS.lock().await;
        let now = Instant::now();
        prune_login_attempts_map(&mut guard, now, window);

        let unavailable = permit.scopes.iter().any(|(key, threshold)| {
            guard.get(key).is_some_and(|state| {
                state.is_locked(now)
                    || state.attempts.len().saturating_add(state.in_flight.len()) >= *threshold
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
            state
                .in_flight
                .insert(permit.reservation.id(), permit.reservation.lease());
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
        let window = Duration::from_secs(config.window_seconds);
        let mut guard = LOGIN_ATTEMPTS.lock().await;
        let now = Instant::now();
        let mut lockouts = Vec::new();

        for (key, threshold) in &permit.scopes {
            let Some(state) = guard.get_mut(key) else {
                continue;
            };
            // Matching ownership makes completion idempotent and fences old
            // requests from state recreated by an administrative release.
            let Some(lease) = state.in_flight.remove(&permit.reservation.id()) else {
                continue;
            };
            if !lease.is_active(now) {
                continue;
            }
            match outcome {
                LoginAttemptOutcome::Failed => {
                    if state.register_failure(now, *threshold, config) {
                        lockouts.push(key.clone());
                    }
                }
                LoginAttemptOutcome::Succeeded if key == &permit.user_ip_key => {
                    state.attempts.clear();
                    state.locked_until = None;
                    state.lockout_level = 0;
                }
                LoginAttemptOutcome::Succeeded | LoginAttemptOutcome::Aborted => {}
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
        let mut guard = LOGIN_ATTEMPTS.lock().await;
        let now = Instant::now();
        prune_login_attempts_map(&mut guard, now, window);

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

/// Snapshot live scopes for the admin API, pruning canceled and expired reservations.
/// Remaining lockout time is derived from the monotonic clock.
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
mod tests;
