use actix_web::{HttpRequest, web};

use crate::config::{AppConfig, login_rate_limit_max_attempts, login_rate_limit_window_seconds};
use crate::middlewares::client_allowlist::extract_client_ip_from_http_request;

use std::collections::{HashMap, VecDeque};
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::warn;

static LOGIN_ATTEMPTS: LazyLock<Mutex<HashMap<String, VecDeque<Instant>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

const MAX_LOGIN_ATTEMPT_KEYS: usize = 10_000;

fn login_rate_limit_key(username: &str, client_ip: &str) -> String {
    format!("{}|{}", username.trim().to_ascii_lowercase(), client_ip)
}

fn current_login_window() -> Duration {
    Duration::from_secs(login_rate_limit_window_seconds())
}

fn prune_attempts(attempts: &mut VecDeque<Instant>, now: Instant) {
    let window = current_login_window();
    while let Some(first) = attempts.front() {
        if now.duration_since(*first) > window {
            attempts.pop_front();
        } else {
            break;
        }
    }
}

fn prune_login_attempts_map(
    attempts_by_key: &mut HashMap<String, VecDeque<Instant>>,
    now: Instant,
) {
    attempts_by_key.retain(|_, attempts| {
        prune_attempts(attempts, now);
        !attempts.is_empty()
    });
}

fn evict_stalest_login_attempt_key(attempts_by_key: &mut HashMap<String, VecDeque<Instant>>) {
    let stalest_key = attempts_by_key
        .iter()
        .filter_map(|(key, attempts)| {
            attempts
                .back()
                .copied()
                .map(|last_attempt| (key.clone(), last_attempt))
        })
        .min_by_key(|(_, last_attempt)| *last_attempt)
        .map(|(key, _)| key);

    if let Some(stalest_key) = stalest_key {
        attempts_by_key.remove(&stalest_key);
        warn!(
            message = "Evicted stalest login limiter entry to enforce key cap",
            max_tracked_keys = MAX_LOGIN_ATTEMPT_KEYS
        );
    }
}

pub(crate) fn client_ip_for_request(req: &HttpRequest) -> String {
    let trust_ip_headers = req
        .app_data::<web::Data<AppConfig>>()
        .map(|config| config.trust_ip_headers)
        .unwrap_or(false);

    extract_client_ip_from_http_request(req, trust_ip_headers)
        .map(|ip| ip.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

pub(crate) async fn login_is_rate_limited(username: &str, client_ip: &str) -> bool {
    let key = login_rate_limit_key(username, client_ip);
    let now = Instant::now();
    let mut guard = LOGIN_ATTEMPTS.lock().await;

    if let Some(attempts) = guard.get_mut(&key) {
        prune_attempts(attempts, now);
        attempts.len() >= login_rate_limit_max_attempts()
    } else {
        false
    }
}

pub(crate) async fn record_login_failure(username: &str, client_ip: &str) {
    let key = login_rate_limit_key(username, client_ip);
    let now = Instant::now();
    let mut guard = LOGIN_ATTEMPTS.lock().await;

    if let Some(attempts) = guard.get_mut(&key) {
        prune_attempts(attempts, now);
    }

    if !guard.contains_key(&key) && guard.len() >= MAX_LOGIN_ATTEMPT_KEYS {
        prune_login_attempts_map(&mut guard, now);
        if guard.len() >= MAX_LOGIN_ATTEMPT_KEYS {
            evict_stalest_login_attempt_key(&mut guard);
        }
    }

    let attempts = guard.entry(key).or_default();
    attempts.push_back(now);
}

pub(crate) async fn clear_login_failures(username: &str, client_ip: &str) {
    let key = login_rate_limit_key(username, client_ip);
    LOGIN_ATTEMPTS.lock().await.remove(&key);
}

#[cfg(test)]
pub(crate) async fn reset_login_rate_limit_for_tests() {
    LOGIN_ATTEMPTS.lock().await.clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_attempts(times: &[Instant]) -> VecDeque<Instant> {
        times.iter().copied().collect()
    }

    #[test]
    fn evict_stalest_login_attempt_key_removes_entry_with_oldest_last_attempt() {
        let now = Instant::now();
        let mut map: HashMap<String, VecDeque<Instant>> = HashMap::new();
        map.insert(
            "fresh".to_string(),
            make_attempts(&[now - Duration::from_secs(10), now]),
        );
        map.insert(
            "stale".to_string(),
            make_attempts(&[
                now - Duration::from_secs(600),
                now - Duration::from_secs(500),
            ]),
        );
        map.insert(
            "middle".to_string(),
            make_attempts(&[now - Duration::from_secs(120)]),
        );

        evict_stalest_login_attempt_key(&mut map);

        assert!(!map.contains_key("stale"));
        assert!(map.contains_key("fresh"));
        assert!(map.contains_key("middle"));
    }

    #[test]
    fn evict_stalest_login_attempt_key_skips_entries_without_attempts() {
        let now = Instant::now();
        let mut map: HashMap<String, VecDeque<Instant>> = HashMap::new();
        map.insert("empty".to_string(), VecDeque::new());
        map.insert("only".to_string(), make_attempts(&[now]));

        evict_stalest_login_attempt_key(&mut map);

        assert!(map.contains_key("empty"));
        assert!(!map.contains_key("only"));
    }

    #[test]
    fn evict_stalest_login_attempt_key_is_noop_on_empty_map() {
        let mut map: HashMap<String, VecDeque<Instant>> = HashMap::new();
        evict_stalest_login_attempt_key(&mut map);
        assert!(map.is_empty());
    }
}
