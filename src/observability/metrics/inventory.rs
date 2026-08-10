use std::time::Instant;

use opentelemetry::KeyValue;

use crate::storage::postgres::operations::metrics::{
    InventoryGaugeSnapshot, MetricsRefreshBackend,
};

use super::Metrics;
use super::scrape::{RefreshOutcome, RefreshSource, record_refresh_attempt};

pub(super) async fn refresh_inventory_gauges(
    metrics: &Metrics,
    backend: &impl crate::storage::StorageContext,
) {
    if let Some(row) = cached_inventory_snapshot(metrics) {
        record_inventory_snapshot(metrics, &row);
        return;
    }

    let refresh_started_at = Instant::now();
    match backend.metrics_inventory_gauge_snapshot().await {
        Ok(row) => {
            record_refresh_attempt(
                metrics,
                RefreshSource::Inventory,
                refresh_started_at,
                RefreshOutcome::Succeeded,
            );
            record_inventory_snapshot(metrics, &row);
            store_inventory_snapshot(metrics, row);
        }
        Err(_) => {
            record_refresh_attempt(
                metrics,
                RefreshSource::Inventory,
                refresh_started_at,
                RefreshOutcome::Failed,
            );
            if let Some(row) = stale_inventory_snapshot(metrics) {
                record_inventory_snapshot(metrics, &row);
            }
        }
    }
}

fn cached_inventory_snapshot(metrics: &Metrics) -> Option<InventoryGaugeSnapshot> {
    let now = Instant::now();
    metrics
        .scrape_cache
        .lock()
        .ok()
        .and_then(|cache| cache.inventory.fresh_value(now))
}

fn stale_inventory_snapshot(metrics: &Metrics) -> Option<InventoryGaugeSnapshot> {
    metrics
        .scrape_cache
        .lock()
        .ok()
        .and_then(|cache| cache.inventory.cached_value())
}

fn store_inventory_snapshot(metrics: &Metrics, snapshot: InventoryGaugeSnapshot) {
    if let Ok(mut cache) = metrics.scrape_cache.lock() {
        cache.inventory.store(snapshot, Instant::now());
    }
}

fn record_inventory_snapshot(metrics: &Metrics, snapshot: &InventoryGaugeSnapshot) {
    let counts = &snapshot.counts;
    record_inventory(metrics, "collections", counts.collections);
    record_inventory(metrics, "classes", counts.classes);
    record_inventory(metrics, "objects", counts.objects);
    record_inventory(metrics, "users", counts.users);
    record_inventory(metrics, "groups", counts.groups);
    record_inventory(metrics, "service_accounts", counts.service_accounts);
    record_inventory(metrics, "remote_targets", counts.remote_targets);

    metrics.export_template_info.reset();
    for template in &snapshot.export_templates {
        metrics
            .export_template_info
            .with_label_values(&[&template.id.id().to_string(), &template.name])
            .set(1);
    }
}

fn record_inventory(metrics: &Metrics, entity_type: &'static str, count: i64) {
    metrics
        .inventory_entities
        .record(count, &[KeyValue::new("entity_type", entity_type)]);
}
