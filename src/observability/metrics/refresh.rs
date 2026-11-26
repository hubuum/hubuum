use std::time::{Instant, SystemTime, UNIX_EPOCH};

use opentelemetry::KeyValue;

use super::Metrics;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RefreshOutcome {
    Succeeded,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RefreshSource {
    Database,
    Events,
    Inventory,
    LoginLimiter,
    Process,
    Tasks,
    TokenKeys,
}

impl RefreshSource {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Database => "database",
            Self::Events => "events",
            Self::Inventory => "inventory",
            Self::LoginLimiter => "login_limiter",
            Self::Process => "process",
            Self::Tasks => "tasks",
            Self::TokenKeys => "token_keys",
        }
    }
}

pub(super) fn record_refresh_attempt(
    metrics: &Metrics,
    source: RefreshSource,
    started_at: Instant,
    outcome: RefreshOutcome,
) {
    let attrs = [KeyValue::new("source", source.as_str())];
    metrics
        .refresh_duration
        .record(started_at.elapsed().as_secs_f64(), &attrs);
    match outcome {
        RefreshOutcome::Succeeded => {
            metrics.refresh_last_success.record(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64(),
                &attrs,
            );
        }
        RefreshOutcome::Failed => metrics.refresh_failures.add(1, &attrs),
    }
}

pub(super) fn record_concurrent_refresh_skip(metrics: &Metrics) {
    metrics.refresh_skipped.add(
        1,
        &[
            KeyValue::new("source", RefreshSource::Database.as_str()),
            KeyValue::new("reason", "concurrent"),
        ],
    );
}

#[cfg(test)]
mod tests {
    use super::RefreshSource;

    #[test]
    fn refresh_sources_have_stable_bounded_labels() {
        assert_eq!(
            [
                RefreshSource::Database,
                RefreshSource::Events,
                RefreshSource::Inventory,
                RefreshSource::LoginLimiter,
                RefreshSource::Process,
                RefreshSource::Tasks,
                RefreshSource::TokenKeys,
            ]
            .map(RefreshSource::as_str),
            [
                "database",
                "events",
                "inventory",
                "login_limiter",
                "process",
                "tasks",
                "token_keys",
            ]
        );
    }
}
