use std::time::{Duration, Instant};

use opentelemetry::KeyValue;

use super::current;

fn export_attrs(scope: &'static str, content_type: &'static str) -> [KeyValue; 2] {
    [
        KeyValue::new("scope", scope),
        KeyValue::new("content_type", content_type),
    ]
}

fn export_phase_attrs(phase: &'static str, outcome: &'static str) -> [KeyValue; 2] {
    [
        KeyValue::new("phase", phase),
        KeyValue::new("outcome", outcome),
    ]
}

fn template_duration_attrs(template_id: Option<i32>, outcome: &'static str) -> [KeyValue; 2] {
    [
        KeyValue::new(
            "template_id",
            template_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string()),
        ),
        KeyValue::new("outcome", outcome),
    ]
}

fn record_export_phase_duration(
    phase: &'static str,
    outcome: &'static str,
    duration: Duration,
    template_id: Option<i32>,
) {
    if let Some(metrics) = current() {
        metrics
            .export_phase_duration
            .record(duration.as_secs_f64(), &export_phase_attrs(phase, outcome));
        if phase == "total" {
            metrics.export_template_duration.record(
                duration.as_secs_f64(),
                &template_duration_attrs(template_id, outcome),
            );
        }
    }
}

pub fn export_phase_duration(phase: &'static str, duration: Duration) {
    record_export_phase_duration(phase, "success", duration, None);
}

pub fn export_output_cleanup_run() {
    super::task::task_output_cleanup_run("export");
}

pub fn export_output_cleanup_failed() {
    super::task::task_output_cleanup_failed("export");
}

pub fn export_output_cleanup_deleted(count: usize) {
    super::task::task_output_cleanup_deleted("export", count);
}

#[must_use = "an export phase timer must be finished or dropped to record its outcome"]
pub struct ExportPhaseTimer {
    phase: &'static str,
    template_id: Option<i32>,
    started_at: Instant,
    finished: bool,
}

impl ExportPhaseTimer {
    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    pub fn finish(mut self, outcome: &'static str) -> Duration {
        let elapsed = self.started_at.elapsed();
        record_export_phase_duration(self.phase, outcome, elapsed, self.template_id);
        self.finished = true;
        elapsed
    }
}

impl Drop for ExportPhaseTimer {
    fn drop(&mut self) {
        if !self.finished {
            record_export_phase_duration(
                self.phase,
                "error",
                self.started_at.elapsed(),
                self.template_id,
            );
        }
    }
}

pub fn export_phase_timer(phase: &'static str, template_id: Option<i32>) -> ExportPhaseTimer {
    ExportPhaseTimer {
        phase,
        template_id,
        started_at: Instant::now(),
        finished: false,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_phase_attributes_are_bounded() {
        assert_eq!(
            export_phase_attrs("render", "success"),
            [
                KeyValue::new("phase", "render"),
                KeyValue::new("outcome", "success"),
            ]
        );
    }

    #[test]
    fn template_duration_attributes_use_stable_template_id() {
        assert_eq!(
            template_duration_attrs(Some(42), "success"),
            [
                KeyValue::new("template_id", "42"),
                KeyValue::new("outcome", "success"),
            ]
        );
    }

    #[test]
    fn untemplated_duration_attributes_use_none_identity() {
        assert_eq!(
            template_duration_attrs(None, "error"),
            [
                KeyValue::new("template_id", "none"),
                KeyValue::new("outcome", "error"),
            ]
        );
    }
}
