use std::time::Instant;

use opentelemetry::KeyValue;

use crate::db::DbPool;
use crate::db::traits::metrics::{InventoryMetricsSnapshot, MetricsBackend};

use super::Metrics;

pub(super) async fn refresh_inventory_gauges(metrics: &Metrics, pool: &DbPool) {
    if let Some(row) = cached_inventory_snapshot(metrics) {
        record_inventory_snapshot(metrics, &row);
        return;
    }

    match pool.metrics_inventory_snapshot().await {
        Ok(row) => {
            store_inventory_snapshot(metrics, row);
            record_inventory_snapshot(metrics, &row);
        }
        Err(_) => {
            metrics
                .refresh_failures
                .add(1, &[KeyValue::new("source", "inventory")]);
            if let Some(row) = stale_inventory_snapshot(metrics) {
                record_inventory_snapshot(metrics, &row);
            }
        }
    }
}

fn cached_inventory_snapshot(metrics: &Metrics) -> Option<InventoryMetricsSnapshot> {
    let now = Instant::now();
    metrics
        .scrape_cache
        .lock()
        .ok()
        .and_then(|cache| cache.inventory.fresh_value(now))
}

fn stale_inventory_snapshot(metrics: &Metrics) -> Option<InventoryMetricsSnapshot> {
    metrics
        .scrape_cache
        .lock()
        .ok()
        .and_then(|cache| cache.inventory.cached_value())
}

fn store_inventory_snapshot(metrics: &Metrics, snapshot: InventoryMetricsSnapshot) {
    if let Ok(mut cache) = metrics.scrape_cache.lock() {
        cache.inventory.store(snapshot, Instant::now());
    }
}

fn record_inventory_snapshot(metrics: &Metrics, row: &InventoryMetricsSnapshot) {
    record_inventory(metrics, "collections", row.collections);
    record_inventory(metrics, "classes", row.classes);
    record_inventory(metrics, "objects", row.objects);
    record_inventory(metrics, "users", row.users);
    record_inventory(metrics, "groups", row.groups);
    record_inventory(metrics, "service_accounts", row.service_accounts);
    record_inventory(metrics, "remote_targets", row.remote_targets);
}

fn record_inventory(metrics: &Metrics, entity_type: &'static str, count: i64) {
    metrics
        .inventory_entities
        .record(count, &[KeyValue::new("entity_type", entity_type)]);
}
