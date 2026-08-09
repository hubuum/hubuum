use std::time::Duration;

use opentelemetry::KeyValue;

use super::current;

/// Publish the one complete storage backend selected for this process.
pub fn storage_backend_identity(backend: &'static str, contract_version: u16) {
    if let Some(metrics) = current() {
        metrics.storage_backend_info.record(
            1,
            &[
                KeyValue::new("backend", backend),
                KeyValue::new("contract_version", i64::from(contract_version)),
            ],
        );
    }
}

/// Record one backend-neutral lifecycle storage operation.
pub fn storage_operation_finished(
    backend: &'static str,
    capability: &'static str,
    operation: &'static str,
    result: &'static str,
    duration: Duration,
) {
    if let Some(metrics) = current() {
        let attributes = [
            KeyValue::new("backend", backend),
            KeyValue::new("capability", capability),
            KeyValue::new("operation", operation),
            KeyValue::new("result", result),
        ];
        metrics
            .storage_operation_duration
            .record(duration.as_secs_f64(), &attributes);
        if result != "ok" {
            metrics.storage_operation_errors.add(1, &attributes);
        }
    }
}
