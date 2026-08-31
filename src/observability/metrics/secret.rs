use std::time::Duration;

use opentelemetry::KeyValue;

use super::current;

pub(crate) fn secret_source_identity(provider: &'static str) {
    if let Some(metrics) = current() {
        metrics
            .secret_source_info
            .record(1, &[KeyValue::new("provider", provider)]);
    }
}

pub(crate) fn secret_resolution_finished(
    provider: &'static str,
    consumer: &'static str,
    outcome: &'static str,
    duration: Duration,
) {
    if let Some(metrics) = current() {
        let attributes = [
            KeyValue::new("provider", provider),
            KeyValue::new("consumer", consumer),
            KeyValue::new("outcome", outcome),
        ];
        metrics
            .secret_resolution_duration
            .record(duration.as_secs_f64(), &attributes);
        metrics.secret_resolutions.add(1, &attributes);
    }
}
