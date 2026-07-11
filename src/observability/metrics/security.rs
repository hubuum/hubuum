use opentelemetry::KeyValue;

use super::current;

pub fn client_allowlist_rejected(reason: &'static str) {
    if let Some(metrics) = current() {
        metrics
            .client_allowlist_rejections
            .add(1, &[KeyValue::new("reason", reason)]);
    }
}
