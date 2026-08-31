use opentelemetry::KeyValue;

use super::current;
use super::scrape::{RefreshOutcome, RefreshSource, record_refresh_attempt};
use crate::storage::TokenStorage;

pub(crate) fn token_hash_key_ring(
    mode: &'static str,
    active_key_id: &str,
    ring_identity: &str,
    active: u64,
    previous: u64,
) {
    if let Some(metrics) = current() {
        metrics.token_hash_key_info.record(
            1,
            &[
                KeyValue::new("mode", mode),
                KeyValue::new("active_key_id", active_key_id.to_string()),
                KeyValue::new("ring_identity", ring_identity.to_string()),
            ],
        );
        metrics
            .token_hash_keys
            .record(active, &[KeyValue::new("state", "active")]);
        metrics
            .token_hash_keys
            .record(previous, &[KeyValue::new("state", "previous")]);
    }
}

pub(crate) fn token_authentication(
    format: &'static str,
    key_state: &'static str,
    outcome: &'static str,
) {
    if let Some(metrics) = current() {
        metrics.token_authentications.add(
            1,
            &[
                KeyValue::new("format", format),
                KeyValue::new("key_state", key_state),
                KeyValue::new("outcome", outcome),
            ],
        );
    }
}

pub(super) async fn refresh_token_key_gauges(
    metrics: &super::Metrics,
    backend: &crate::storage::StorageHandle,
) {
    let started_at = std::time::Instant::now();
    let result = async {
        let observed_at = chrono::Utc::now();
        let legacy_valid_after = crate::models::configured_token_lifetime()?
            .cutoff_from(observed_at.naive_utc())?
            .and_utc();
        let observation =
            crate::storage::StorageTokenObservation::try_new(observed_at, legacy_valid_after)
                .map_err(|error| crate::errors::ApiError::InternalServerError(error.to_string()))?;
        let usage = backend.token_key_usage(observation).await?;
        let ring = crate::config::token_hash_key_ring()
            .map_err(|error| crate::errors::ApiError::InternalServerError(error.to_string()))?;
        let mut counts = [[0_i64; 3]; 4];
        for item in usage {
            let key_state = match item.key_id() {
                Some(id) if id == ring.active_key_id() => 0,
                Some(id) if ring.previous_key_ids().any(|previous| previous == id) => 1,
                Some(_) => 3,
                None => 2,
            };
            counts[key_state][0] += item.active();
            counts[key_state][1] += item.revoked();
            counts[key_state][2] += item.expired();
        }
        for (key_state, state_counts) in ["active", "previous", "legacy", "unconfigured"]
            .into_iter()
            .zip(counts)
        {
            for (lifecycle, count) in ["active", "revoked", "expired"]
                .into_iter()
                .zip(state_counts)
            {
                metrics.token_hash_stored.record(
                    count,
                    &[
                        KeyValue::new("key_state", key_state),
                        KeyValue::new("lifecycle", lifecycle),
                    ],
                );
            }
        }
        Ok::<(), crate::errors::ApiError>(())
    }
    .await;
    record_refresh_attempt(
        metrics,
        RefreshSource::TokenKeys,
        started_at,
        if result.is_ok() {
            RefreshOutcome::Succeeded
        } else {
            RefreshOutcome::Failed
        },
    );
}
