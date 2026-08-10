use std::time::{Instant, SystemTime, UNIX_EPOCH};

use actix_web::{HttpResponse, Responder, http::header};
use opentelemetry::KeyValue;
use prometheus::{Encoder, TextEncoder};

use crate::errors::ApiError;
use crate::permissions::AppContext;
use crate::storage::capabilities::{StorageCallSite, with_storage_call_site};

use super::Metrics;
use super::{db, event, get, inventory, login, task};

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
        }
    }
}

pub async fn scrape(context: AppContext) -> Result<impl Responder, ApiError> {
    let metrics = get()?;
    let process_refresh_started_at = Instant::now();
    let process_refresh_outcome = if metrics.process_metrics.refresh() {
        RefreshOutcome::Succeeded
    } else {
        RefreshOutcome::Failed
    };
    record_refresh_attempt(
        metrics,
        RefreshSource::Process,
        process_refresh_started_at,
        process_refresh_outcome,
    );
    with_storage_call_site(
        StorageCallSite::MetricsRefresh,
        refresh_scrape_gauges(metrics, &context),
    )
    .await;

    let encoder = TextEncoder::new();
    let metric_families = metrics.registry.gather();
    let mut body = Vec::new();
    encoder
        .encode(&metric_families, &mut body)
        .map_err(|error| {
            ApiError::InternalServerError(format!("Failed to encode metrics: {error}"))
        })?;

    Ok(HttpResponse::Ok()
        .insert_header((header::CONTENT_TYPE, encoder.format_type()))
        .body(body))
}

async fn refresh_scrape_gauges(metrics: &Metrics, backend: &impl crate::storage::StorageContext) {
    db::refresh_pool_gauges(metrics, backend);
    login::refresh_login_limiter_gauges(metrics).await;
    if let Ok(_refresh_guard) = metrics.db_refresh_lock.try_lock() {
        inventory::refresh_inventory_gauges(metrics, backend).await;
        task::refresh_task_gauges(metrics, backend).await;
        event::refresh_event_gauges(metrics, backend).await;
    } else {
        metrics.refresh_skipped.add(
            1,
            &[
                KeyValue::new("source", RefreshSource::Database.as_str()),
                KeyValue::new("reason", "concurrent"),
            ],
        );
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
            ]
            .map(RefreshSource::as_str),
            [
                "database",
                "events",
                "inventory",
                "login_limiter",
                "process",
                "tasks",
            ]
        );
    }
}
