use std::time::Duration;

use opentelemetry::KeyValue;

use super::current;

pub fn report_output_cleanup_run() {
    if let Some(metrics) = current() {
        metrics.report_output_cleanup_runs.add(1, &[]);
    }
}

pub fn report_output_cleanup_failed() {
    if let Some(metrics) = current() {
        metrics.report_output_cleanup_failures.add(1, &[]);
    }
}

pub fn report_output_cleanup_deleted(count: usize) {
    if let Some(metrics) = current() {
        metrics
            .report_output_cleanup_deleted
            .add(u64::try_from(count).unwrap_or(u64::MAX), &[]);
    }
}

pub fn report_phase_duration(phase: &'static str, duration: Duration) {
    if let Some(metrics) = current() {
        metrics
            .report_duration
            .record(duration.as_secs_f64(), &[KeyValue::new("phase", phase)]);
    }
}

pub fn report_result(scope: &'static str, content_type: &'static str, outcome: &'static str) {
    if let Some(metrics) = current() {
        metrics.report_results.add(
            1,
            &[
                KeyValue::new("scope", scope),
                KeyValue::new("content_type", content_type),
                KeyValue::new("outcome", outcome),
            ],
        );
    }
}
