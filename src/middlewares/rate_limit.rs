use actix_web::{HttpRequest, web};
use ipnet::{Ipv4Net, Ipv6Net};

use crate::config::{AppConfig, LoginRateLimitConfig, login_rate_limit_config};
use crate::middlewares::client_allowlist::{ProxyTrust, extract_client_ip_from_http_request};

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

    /// Whether this entry still carries useful state and must not be evicted/pruned.
    fn is_active(&self, now: Instant) -> bool {
        !self.attempts.is_empty() || self.is_locked(now)
    }

    /// Most recent point of activity, counting an active lockout's expiry. Used to pick
    /// the stalest entry for eviction; locked entries sort as "freshest" (their expiry is
    /// in the future) and are therefore protected from premature eviction.
    fn last_activity(&self) -> Option<Instant> {
        match (self.attempts.back().copied(), self.locked_until) {
            (Some(attempt), Some(lock)) => Some(attempt.max(lock)),
            (Some(attempt), None) => Some(attempt),
            (None, lock) => lock,
        }
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

fn user_ip_key(username: &str, ip_label: &str) -> String {
    format!("u:{}|{}", username.trim().to_ascii_lowercase(), ip_label)
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

/// The set of `(key, threshold)` scopes that apply to a login attempt. The user+IP scope
/// always applies; the per-IP and per-subnet scopes apply only when a client IP is known
/// and their thresholds are non-zero (a zero threshold disables that scope).
fn scopes_for(
    username: &str,
    client_ip: Option<IpAddr>,
    cfg: &LoginRateLimitConfig,
) -> Vec<(String, usize)> {
    let mut scopes = vec![(
        user_ip_key(username, &ip_label(client_ip)),
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
        state.is_active(now)
    });
}

fn evict_stalest_login_attempt_key(attempts_by_key: &mut HashMap<String, ScopeState>) {
    let stalest_key = attempts_by_key
        .iter()
        .filter_map(|(key, state)| state.last_activity().map(|last| (key.clone(), last)))
        .min_by_key(|(_, last)| *last)
        .map(|(key, _)| key);

    if let Some(stalest_key) = stalest_key {
        attempts_by_key.remove(&stalest_key);
        warn!(
            message = "Evicted stalest login limiter entry to enforce key cap",
            max_tracked_keys = MAX_LOGIN_ATTEMPT_KEYS
        );
    }
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

/// Whether the login request is currently throttled by any applicable scope.
pub(crate) async fn login_is_rate_limited(username: &str, client_ip: Option<IpAddr>) -> bool {
    let cfg = login_rate_limit_config();
    if !cfg.enabled {
        return false;
    }

    let now = Instant::now();
    let guard = LOGIN_ATTEMPTS.lock().await;

    scopes_for(username, client_ip, &cfg)
        .iter()
        .any(|(key, _)| guard.get(key).is_some_and(|state| state.is_locked(now)))
}

/// Record a failed login attempt across all applicable scopes, applying lockouts with
/// exponential backoff when a scope crosses its threshold.
pub(crate) async fn record_login_failure(username: &str, client_ip: Option<IpAddr>) {
    let cfg = login_rate_limit_config();
    if !cfg.enabled {
        return;
    }

    let now = Instant::now();
    let window = Duration::from_secs(cfg.window_seconds);
    let mut guard = LOGIN_ATTEMPTS.lock().await;

    for (key, threshold) in scopes_for(username, client_ip, &cfg) {
        if !guard.contains_key(&key) && guard.len() >= MAX_LOGIN_ATTEMPT_KEYS {
            prune_login_attempts_map(&mut guard, now, window);
            if guard.len() >= MAX_LOGIN_ATTEMPT_KEYS {
                evict_stalest_login_attempt_key(&mut guard);
            }
        }

        let state = guard.entry(key).or_default();
        state.prune(now, window);
        state.attempts.push_back(now);
        if state.attempts.len() >= threshold {
            state.trigger_lockout(now, &cfg);
        }
    }
}

/// Clear the per-(user, IP) failure state after a successful login. The per-IP and
/// per-subnet budgets are intentionally left intact: one user's success must not reset
/// the spray/distributed counters for the whole host or network.
pub(crate) async fn clear_login_failures(username: &str, client_ip: Option<IpAddr>) {
    let key = user_ip_key(username, &ip_label(client_ip));
    LOGIN_ATTEMPTS.lock().await.remove(&key);
}

#[cfg(test)]
pub(crate) async fn reset_login_rate_limit_for_tests() {
    LOGIN_ATTEMPTS.lock().await.clear();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

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
        let only_user = scopes_for("alice", Some(IpAddr::V4(Ipv4Addr::LOCALHOST)), &config);
        assert_eq!(only_user.len(), 1);

        let no_ip = scopes_for("alice", None, &cfg());
        assert_eq!(no_ip.len(), 1, "no IP means only the user+IP scope applies");
    }

    #[test]
    fn scopes_cover_user_ip_and_subnet_when_enabled() {
        let scopes = scopes_for(
            "Alice",
            Some(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 7))),
            &cfg(),
        );
        let keys: Vec<&str> = scopes.iter().map(|(k, _)| k.as_str()).collect();
        assert!(keys.iter().any(|k| k.starts_with("u:alice|")));
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

        evict_stalest_login_attempt_key(&mut map);

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
}
