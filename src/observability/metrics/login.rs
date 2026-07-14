use opentelemetry::KeyValue;

use crate::middlewares::rate_limit;

use super::{Metrics, current};

pub fn login_attempt(outcome: &'static str) {
    if let Some(metrics) = current() {
        metrics
            .login_attempts
            .add(1, &[KeyValue::new("outcome", outcome)]);
    }
}

pub fn login_lockout(scope: &'static str) {
    if let Some(metrics) = current() {
        metrics
            .login_lockouts
            .add(1, &[KeyValue::new("scope", scope)]);
    }
}

#[cfg(feature = "login-rate-limit-valkey")]
pub fn login_limiter_backend_failure(operation: &'static str) {
    if let Some(metrics) = current() {
        metrics.login_limiter_backend_failures.add(
            1,
            &[
                KeyValue::new("backend", "valkey"),
                KeyValue::new("operation", operation),
            ],
        );
    }
}

pub(super) async fn refresh_login_limiter_gauges(metrics: &Metrics) {
    let Ok(snapshots) = rate_limit::snapshot().await else {
        return;
    };
    let locked = snapshots.iter().filter(|entry| entry.locked).count();
    metrics.login_limiter_entries.record(
        u64::try_from(snapshots.len()).unwrap_or(u64::MAX),
        &[KeyValue::new("state", "active")],
    );
    metrics.login_limiter_entries.record(
        u64::try_from(locked).unwrap_or(u64::MAX),
        &[KeyValue::new("state", "locked")],
    );
}
