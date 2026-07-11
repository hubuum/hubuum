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

pub(super) async fn refresh_login_limiter_gauges(metrics: &Metrics) {
    let snapshots = rate_limit::snapshot().await;
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
