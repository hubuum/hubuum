use std::time::Duration;

use hubuum_computed_fields::EvaluationResult;
use opentelemetry::KeyValue;

use super::current;

pub fn computed_evaluation(scope: &'static str, result: &EvaluationResult) {
    if let Some(metrics) = current() {
        let outcome = if result.errors.is_empty() {
            "success"
        } else {
            "field_error"
        };
        metrics.computed_evaluations.add(
            1,
            &[
                KeyValue::new("scope", scope),
                KeyValue::new("outcome", outcome),
            ],
        );
        for error in result.errors.values() {
            metrics.computed_evaluator_errors.add(
                1,
                &[
                    KeyValue::new("scope", scope),
                    KeyValue::new("code", error.code.as_str()),
                ],
            );
        }
    }
}

pub fn computed_live_fallback() {
    if let Some(metrics) = current() {
        metrics.computed_live_fallbacks.add(1, &[]);
    }
}

pub fn computed_read_repair(outcome: &'static str) {
    if let Some(metrics) = current() {
        metrics
            .computed_read_repairs
            .add(1, &[KeyValue::new("outcome", outcome)]);
    }
}

pub fn computed_rebuild_batch(items: usize) {
    if let Some(metrics) = current() {
        metrics.computed_rebuild_batches.add(
            1,
            &[KeyValue::new(
                "items",
                if items == 0 { "empty" } else { "non_empty" },
            )],
        );
    }
}

pub fn computed_rebuild_finished(status: &'static str, duration: Duration) {
    if let Some(metrics) = current() {
        let attrs = [KeyValue::new("status", status)];
        metrics.computed_rebuild_completions.add(1, &attrs);
        metrics
            .computed_rebuild_duration
            .record(duration.as_secs_f64(), &attrs);
    }
}
