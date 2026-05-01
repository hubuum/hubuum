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

pub fn client_ip_for_request(req: &HttpRequest) -> String {
    let trust_ip_headers = req
        .app_data::<web::Data<AppConfig>>()
        .map(|config| config.trust_ip_headers)
        .unwrap_or(false);

    extract_client_ip_from_http_request(req, trust_ip_headers)
        .map(|ip| ip.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

pub async fn login_is_rate_limited(username: &str, client_ip: &str) -> bool {
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

pub async fn record_login_failure(username: &str, client_ip: &str) {
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

pub async fn clear_login_failures(username: &str, client_ip: &str) {
    let key = login_rate_limit_key(username, client_ip);
    LOGIN_ATTEMPTS.lock().await.remove(&key);
}

#[cfg(test)]
pub async fn reset_login_rate_limit_for_tests() {
    LOGIN_ATTEMPTS.lock().await.clear();
}
