use std::time::Duration;

use opentelemetry::KeyValue;

use super::current;

fn export_attrs(scope: &'static str, content_type: &'static str) -> [KeyValue; 2] {
    [
        KeyValue::new("scope", scope),
        KeyValue::new("content_type", content_type),
    ]
}

pub fn export_output_cleanup_run() {
    if let Some(metrics) = current() {
        metrics.export_output_cleanup_runs.add(1, &[]);
    }
}

pub fn export_output_cleanup_failed() {
    if let Some(metrics) = current() {
        metrics.export_output_cleanup_failures.add(1, &[]);
    }
}

pub fn export_output_cleanup_deleted(count: usize) {
    if let Some(metrics) = current() {
        metrics
            .export_output_cleanup_deleted
            .add(u64::try_from(count).unwrap_or(u64::MAX), &[]);
    }
}

pub fn export_phase_duration(phase: &'static str, duration: Duration) {
    if let Some(metrics) = current() {
        metrics
            .export_duration
            .record(duration.as_secs_f64(), &[KeyValue::new("phase", phase)]);
    }
}

pub fn export_completed(scope: &'static str, content_type: &'static str) {
    if let Some(metrics) = current() {
        metrics
            .export_completions
            .add(1, &export_attrs(scope, content_type));
    }
}

pub fn export_truncated(scope: &'static str, content_type: &'static str) {
    if let Some(metrics) = current() {
        metrics
            .export_truncations
            .add(1, &export_attrs(scope, content_type));
    }
}

pub fn export_warnings(scope: &'static str, content_type: &'static str, count: usize) {
    if let Some(metrics) = current() {
        metrics.export_warnings.add(
            u64::try_from(count).unwrap_or(u64::MAX),
            &export_attrs(scope, content_type),
        );
    }
}
