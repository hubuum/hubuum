use std::time::Duration;

use opentelemetry::KeyValue;

use crate::storage::StorageHandle;

use super::{Metrics, current};

pub(crate) enum ResultKind {
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

pub(crate) fn db_connection_acquired(call_site: &'static str, duration: Duration) {
    if let Some(metrics) = current() {
        metrics.db_connection_acquire_duration.record(
            duration.as_secs_f64(),
            &[KeyValue::new("caller", call_site)],
        );
    }
}

pub(crate) fn db_connection_acquire_failed(call_site: &'static str, duration: Duration) {
    if let Some(metrics) = current() {
        metrics.db_connection_acquire_duration.record(
            duration.as_secs_f64(),
            &[KeyValue::new("caller", call_site)],
        );
        metrics
            .db_connection_acquire_failures
            .add(1, &[KeyValue::new("caller", call_site)]);
    }
}

pub(crate) fn db_operation_finished(
    call_site: &'static str,
    operation: &'static str,
    duration: Duration,
    result: &ResultKind,
) {
    if let Some(metrics) = current() {
        let attrs = [
            KeyValue::new("caller", call_site),
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

pub(super) fn refresh_pool_gauges(metrics: &Metrics, backend: &StorageHandle) {
    let Some(state) = backend.database_pool_state() else {
        return;
    };
    let capacity = state.capacity();
    metrics.db_pool_connections.record(
        u64::from(capacity.max_connections()),
        &[KeyValue::new("state", "configured")],
    );
    metrics.db_pool_connections.record(
        u64::from(capacity.total_connections()),
        &[KeyValue::new("state", "open")],
    );
    metrics.db_pool_connections.record(
        u64::from(capacity.idle_connections()),
        &[KeyValue::new("state", "idle")],
    );
    metrics.db_pool_connections.record(
        u64::from(capacity.in_use_connections()),
        &[KeyValue::new("state", "checked_out")],
    );
}
