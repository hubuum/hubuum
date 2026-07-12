use actix_web::{HttpRequest, web};
use ipnet::{Ipv4Net, Ipv6Net};

use crate::config::{AppConfig, LoginRateLimitConfig, login_rate_limit_config};
use crate::middlewares::client_allowlist::{ProxyTrust, extract_client_ip_from_http_request};

#[cfg(test)]
use crate::tests::{TestMutex, test_mutex};
use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::warn;

/// Failure bookkeeping for a single rate-limit scope (a user+IP pair, an IP, or a
/// subnet). A sliding window of recent failures triggers a lockout once it reaches the
/// scope threshold; repeated lockouts back off exponentially.
#[derive(Default)]
struct ScopeState {
    attempts: VecDeque<Instant>,
    locked_until: Option<Instant>,
    lockout_level: u32,
    in_flight: usize,
    last_activity_at: Option<Instant>,
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
        self.last_activity_at = Some(now);
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

    /// Most recent point of activity, counting an active lockout's expiry. Used to pick
    /// the stalest entry for eviction; locked entries sort as "freshest" (their expiry is
    /// in the future) and are therefore protected from premature eviction.
    fn last_activity(&self) -> Option<Instant> {
        [
            self.attempts.back().copied(),
            self.locked_until,
            self.last_activity_at,
        ]
        .into_iter()
        .flatten()
        .max()
    }
}

static LOGIN_ATTEMPTS: LazyLock<Mutex<HashMap<String, ScopeState>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

const MAX_LOGIN_ATTEMPT_KEYS: usize = 10_000;

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

fn evict_stalest_login_attempt_key(attempts_by_key: &mut HashMap<String, ScopeState>) -> bool {
    let stalest_key = attempts_by_key
        .iter()
        .filter(|(_, state)| state.in_flight == 0)
        .filter_map(|(key, state)| state.last_activity().map(|last| (key.clone(), last)))
        .min_by_key(|(_, last)| *last)
        .map(|(key, _)| key);

    if let Some(stalest_key) = stalest_key {
        attempts_by_key.remove(&stalest_key);
        warn!(
            message = "Evicted stalest login limiter entry to enforce key cap",
            max_tracked_keys = MAX_LOGIN_ATTEMPT_KEYS
        );
        true
    } else {
        false
    }
}

/// Reservation for a login attempt. Applicable scope budgets are reserved before
/// password verification starts so concurrent requests cannot all pass the limiter
/// check before any of them records a failure.
pub(crate) struct LoginAttemptPermit {
    scopes: Vec<(String, usize)>,
    user_ip_key: String,
    enabled: bool,
}

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
) -> Option<LoginAttemptPermit> {
    let cfg = login_rate_limit_config();
    let scopes = scopes_for(identity_scope, username, client_ip, &cfg);
    let user_ip_key = user_ip_key(identity_scope, username, &ip_label(client_ip));
    if !cfg.enabled {
        return Some(LoginAttemptPermit {
            scopes,
            user_ip_key,
            enabled: false,
        });
    }

    let now = Instant::now();
    let window = Duration::from_secs(cfg.window_seconds);
    let mut guard = LOGIN_ATTEMPTS.lock().await;
    prune_login_attempts_map(&mut guard, now, window);

    let unavailable = scopes.iter().any(|(key, threshold)| {
        guard.get(key).is_some_and(|state| {
            state.is_locked(now)
                || state.attempts.len().saturating_add(state.in_flight) >= *threshold
        })
    });
    if unavailable {
        return None;
    }

    let missing_scopes = scopes
        .iter()
        .filter(|(key, _)| !guard.contains_key(key))
        .count();
    while guard.len().saturating_add(missing_scopes) > MAX_LOGIN_ATTEMPT_KEYS {
        if !evict_stalest_login_attempt_key(&mut guard) {
            return None;
        }
    }

    for (key, _) in &scopes {
        let state = guard.entry(key.clone()).or_default();
        state.in_flight = state.in_flight.saturating_add(1);
        state.last_activity_at = Some(now);
    }

    Some(LoginAttemptPermit {
        scopes,
        user_ip_key,
        enabled: true,
    })
}

/// Release a login reservation and update all applicable scope budgets in the same
/// critical section. Internal authentication errors release capacity without counting
/// as credential failures.
pub(crate) async fn finish_login_attempt(permit: LoginAttemptPermit, outcome: LoginAttemptOutcome) {
    if !permit.enabled {
        return;
    }

    let cfg = login_rate_limit_config();
    let now = Instant::now();
    let window = Duration::from_secs(cfg.window_seconds);
    let mut guard = LOGIN_ATTEMPTS.lock().await;

    for (key, threshold) in &permit.scopes {
        if let Some(state) = guard.get_mut(key) {
            state.in_flight = state.in_flight.saturating_sub(1);
            state.last_activity_at = Some(now);
            if matches!(outcome, LoginAttemptOutcome::Failed)
                && state.register_failure(now, *threshold, &cfg)
            {
                crate::observability::metrics::login_lockout(scope_kind(key));
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
}

/// Resolve the trustworthy client IP for a login request, honoring the configured proxy
/// trust policy. Returns `None` when no address can be determined.
pub(crate) fn client_ip_for_request(req: &HttpRequest) -> Option<IpAddr> {
    let policy = req
        .app_data::<web::Data<AppConfig>>()
        .map(|config| ProxyTrust::from_config(config))
        .unwrap_or_default();

    extract_client_ip_from_http_request(req, &policy)
}

/// Record a failed login attempt across all applicable scopes, applying lockouts with
/// exponential backoff when a scope crosses its threshold.
#[cfg(test)]
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
                let _ = evict_stalest_login_attempt_key(&mut guard);
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
pub(crate) async fn snapshot() -> Vec<ScopeSnapshot> {
    let cfg = login_rate_limit_config();
    let window = Duration::from_secs(cfg.window_seconds);
    let now = Instant::now();
    let guard = LOGIN_ATTEMPTS.lock().await;

    guard
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
        .collect()
}

/// Release a single tracked scope by its raw key. Returns whether an entry was removed.
pub(crate) async fn release_entry(key: &str) -> bool {
    LOGIN_ATTEMPTS.lock().await.remove(key).is_some()
}

/// Clear all tracked scopes. Returns the number of entries removed.
pub(crate) async fn clear_all() -> usize {
    let mut guard = LOGIN_ATTEMPTS.lock().await;
    let removed = guard.len();
    guard.clear();
    removed
}

/// Serializes tests that touch the process-global limiter state (auth login tests and the
/// admin `/meta/login-rate-limit` tests) so they do not observe each other's failures.
#[cfg(test)]
pub(crate) static LOGIN_RATE_LIMIT_TEST_LOCK: TestMutex = test_mutex();

#[cfg(test)]
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

    #[test]
    fn evict_stalest_protects_locked_entries() {
        let now = Instant::now();
        let config = cfg();
        let mut map: HashMap<String, ScopeState> = HashMap::new();

        let mut stale = ScopeState::default();
        stale.attempts.push_back(now - Duration::from_secs(120));
        map.insert("stale".to_string(), stale);

        let mut locked = ScopeState::default();
        locked.trigger_lockout(now, &config);
        map.insert("locked".to_string(), locked);

        assert!(evict_stalest_login_attempt_key(&mut map));

        assert!(!map.contains_key("stale"));
        assert!(map.contains_key("locked"));
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
                    .expect("capacity below the threshold should be reserved"),
            );
        }
        assert!(
            begin_login_attempt("local", "atomic-limit-user", None)
                .await
                .is_none()
        );

        for permit in permits {
            finish_login_attempt(permit, LoginAttemptOutcome::Aborted).await;
        }
        assert!(
            begin_login_attempt("local", "atomic-limit-user", None)
                .await
                .is_some()
        );
        reset_login_rate_limit_for_tests().await;
    }
}
