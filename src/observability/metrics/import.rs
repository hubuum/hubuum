use std::time::Duration;

use opentelemetry::KeyValue;

use super::current;

pub fn import_phase_duration(phase: &'static str, duration: Duration) {
    if let Some(metrics) = current() {
        metrics
            .import_duration
            .record(duration.as_secs_f64(), &[KeyValue::new("phase", phase)]);
    }
}

pub fn import_items(processed: i32, succeeded: i32, failed: i32) {
    if let Some(metrics) = current() {
        metrics
            .import_processed_items
            .add(u64::try_from(processed).unwrap_or(0), &[]);
        metrics
            .import_succeeded_items
            .add(u64::try_from(succeeded).unwrap_or(0), &[]);
        metrics
            .import_failed_items
            .add(u64::try_from(failed).unwrap_or(0), &[]);
    }
}
