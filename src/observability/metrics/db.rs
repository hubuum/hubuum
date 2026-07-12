use std::time::Duration;

use opentelemetry::KeyValue;

use crate::db::DbPool;

use super::{Metrics, current};

pub enum ResultKind {
    Ok,
    Error(&'static str),
}

impl ResultKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error(class) => class,
        }
    }
}

pub fn db_connection_acquired(duration: Duration) {
    if let Some(metrics) = current() {
        metrics
            .db_connection_acquire_duration
            .record(duration.as_secs_f64(), &[]);
    }
}

pub fn db_connection_acquire_failed(duration: Duration) {
    if let Some(metrics) = current() {
        metrics
            .db_connection_acquire_duration
            .record(duration.as_secs_f64(), &[]);
        metrics.db_connection_acquire_failures.add(1, &[]);
    }
}

pub fn db_operation_finished(operation: &'static str, duration: Duration, result: &ResultKind) {
    if let Some(metrics) = current() {
        let attrs = [
            KeyValue::new("operation", operation),
            KeyValue::new("result", result.as_str()),
        ];
        metrics
            .db_operation_duration
            .record(duration.as_secs_f64(), &attrs);
        if matches!(result, ResultKind::Error(_)) {
            metrics.db_operation_errors.add(1, &attrs);
        }
    }
}

pub(super) fn refresh_pool_gauges(metrics: &Metrics, pool: &DbPool) {
    let state = pool.state();
    metrics.db_pool_connections.record(
        u64::from(pool.config().max_size),
        &[KeyValue::new("state", "configured")],
    );
    metrics.db_pool_connections.record(
        u64::from(state.connections),
        &[KeyValue::new("state", "open")],
    );
    metrics.db_pool_connections.record(
        u64::from(state.idle_connections),
        &[KeyValue::new("state", "idle")],
    );
    let checked_out = state.connections.saturating_sub(state.idle_connections);
    metrics.db_pool_connections.record(
        u64::from(checked_out),
        &[KeyValue::new("state", "checked_out")],
    );
}
