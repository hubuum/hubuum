use std::time::Duration;

use opentelemetry::KeyValue;

use crate::models::ExportTemplateID;

use super::current;
use super::timer::OutcomeTimer;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExportMetricPhase {
    Total,
    Query,
    Hydration,
    Render,
}

impl ExportMetricPhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Total => "total",
            Self::Query => "query",
            Self::Hydration => "hydration",
            Self::Render => "render",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExportMetricOutcome {
    Success,
    Error,
    Timeout,
}

impl ExportMetricOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
            Self::Timeout => "timeout",
        }
    }
}

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

fn template_duration_attrs(
    template_id: Option<ExportTemplateID>,
    outcome: &'static str,
) -> [KeyValue; 2] {
    [
        KeyValue::new(
            "template_id",
            template_id
                .map(|value| value.id().to_string())
                .unwrap_or_else(|| "none".to_string()),
        ),
        KeyValue::new("outcome", outcome),
    ]
}

fn record_export_phase_duration(
    phase: &'static str,
    outcome: &'static str,
    duration: Duration,
    template_id: Option<ExportTemplateID>,
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

pub fn export_phase_duration(phase: ExportMetricPhase, duration: Duration) {
    record_export_phase_duration(
        phase.as_str(),
        ExportMetricOutcome::Success.as_str(),
        duration,
        None,
    );
}

pub fn export_output_cleanup_run() {
    super::task::task_output_cleanup_run(super::task::TaskOutputKind::Export);
}

pub fn export_output_cleanup_failed() {
    super::task::task_output_cleanup_failed(super::task::TaskOutputKind::Export);
}

pub fn export_output_cleanup_deleted(count: usize) {
    super::task::task_output_cleanup_deleted(super::task::TaskOutputKind::Export, count);
}

#[must_use = "an export phase timer must be finished or dropped to record its outcome"]
pub struct ExportPhaseTimer {
    phase: ExportMetricPhase,
    template_id: Option<ExportTemplateID>,
    timer: OutcomeTimer,
}

impl ExportPhaseTimer {
    pub fn elapsed(&self) -> Duration {
        self.timer.elapsed()
    }

    pub fn finish(mut self, outcome: ExportMetricOutcome) -> Duration {
        let elapsed = self.timer.finish();
        record_export_phase_duration(
            self.phase.as_str(),
            outcome.as_str(),
            elapsed,
            self.template_id,
        );
        elapsed
    }
}

impl Drop for ExportPhaseTimer {
    fn drop(&mut self) {
        if let Some(elapsed) = self.timer.unfinished_elapsed() {
            record_export_phase_duration(
                self.phase.as_str(),
                ExportMetricOutcome::Error.as_str(),
                elapsed,
                self.template_id,
            );
        }
    }
}

pub fn export_phase_timer(
    phase: ExportMetricPhase,
    template_id: Option<ExportTemplateID>,
) -> ExportPhaseTimer {
    ExportPhaseTimer {
        phase,
        template_id,
        timer: OutcomeTimer::start(),
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
    fn export_timer_labels_are_stable_and_bounded() {
        assert_eq!(
            [
                ExportMetricPhase::Total,
                ExportMetricPhase::Query,
                ExportMetricPhase::Hydration,
                ExportMetricPhase::Render,
            ]
            .map(ExportMetricPhase::as_str),
            ["total", "query", "hydration", "render"]
        );
        assert_eq!(
            [
                ExportMetricOutcome::Success,
                ExportMetricOutcome::Error,
                ExportMetricOutcome::Timeout,
            ]
            .map(ExportMetricOutcome::as_str),
            ["success", "error", "timeout"]
        );
    }

    #[test]
    fn template_duration_attributes_use_stable_template_id() {
        assert_eq!(
            template_duration_attrs(Some(ExportTemplateID::new(42).unwrap()), "success"),
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
