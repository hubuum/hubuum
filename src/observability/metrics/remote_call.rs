use std::time::Duration;

use opentelemetry::KeyValue;

use super::current;

pub fn remote_call_finished(
    method: &str,
    status_family: &'static str,
    outcome: &'static str,
    duration: Duration,
) {
    if let Some(metrics) = current() {
        let attrs = [
            KeyValue::new("method", method.to_string()),
            KeyValue::new("status_family", status_family),
            KeyValue::new("outcome", outcome),
        ];
        metrics
            .remote_call_duration
            .record(duration.as_secs_f64(), &attrs);
        metrics.remote_call_results.add(1, &attrs);
    }
}
