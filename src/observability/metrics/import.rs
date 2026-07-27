use std::time::Duration;

use opentelemetry::KeyValue;

use super::current;
use super::timer::OutcomeTimer;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImportMetricPhase {
    Total,
    Planning,
    Execution,
}

impl ImportMetricPhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Total => "total",
            Self::Planning => "planning",
            Self::Execution => "execution",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImportMetricOutcome {
    Success,
    Failed,
    PartiallySucceeded,
    Error,
}

impl ImportMetricOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failed => "failed",
            Self::PartiallySucceeded => "partially_succeeded",
            Self::Error => "error",
        }
    }
}

fn import_phase_attrs(phase: &'static str, outcome: &'static str) -> [KeyValue; 2] {
    [
        KeyValue::new("phase", phase),
        KeyValue::new("outcome", outcome),
    ]
}

fn record_import_phase_duration(phase: &'static str, outcome: &'static str, duration: Duration) {
    if let Some(metrics) = current() {
        metrics
            .import_phase_duration
            .record(duration.as_secs_f64(), &import_phase_attrs(phase, outcome));
    }
}

/// Record a successful import phase using the legacy direct-observation API.
///
/// New task instrumentation should prefer [`import_phase_timer`] so failures
/// and unfinished phases are recorded automatically.
pub fn import_phase_duration(phase: &'static str, duration: Duration) {
    record_import_phase_duration(phase, ImportMetricOutcome::Success.as_str(), duration);
}

#[must_use = "an import phase timer must be finished or dropped to record its outcome"]
pub struct ImportPhaseTimer {
    phase: ImportMetricPhase,
    timer: OutcomeTimer,
}

impl ImportPhaseTimer {
    pub fn elapsed(&self) -> Duration {
        self.timer.elapsed()
    }

    pub fn finish(mut self, outcome: ImportMetricOutcome) -> Duration {
        let elapsed = self.timer.finish();
        record_import_phase_duration(self.phase.as_str(), outcome.as_str(), elapsed);
        elapsed
    }
}

impl Drop for ImportPhaseTimer {
    fn drop(&mut self) {
        if let Some(elapsed) = self.timer.unfinished_elapsed() {
            record_import_phase_duration(
                self.phase.as_str(),
                ImportMetricOutcome::Error.as_str(),
                elapsed,
            );
        }
    }
}

pub fn import_phase_timer(phase: ImportMetricPhase) -> ImportPhaseTimer {
    ImportPhaseTimer {
        phase,
        timer: OutcomeTimer::start(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn import_phase_attributes_are_bounded() {
        assert_eq!(
            import_phase_attrs("execution", "partially_succeeded"),
            [
                KeyValue::new("phase", "execution"),
                KeyValue::new("outcome", "partially_succeeded"),
            ]
        );
    }

    #[test]
    fn import_timer_labels_are_stable_and_bounded() {
        assert_eq!(
            [
                ImportMetricPhase::Total,
                ImportMetricPhase::Planning,
                ImportMetricPhase::Execution,
            ]
            .map(ImportMetricPhase::as_str),
            ["total", "planning", "execution"]
        );
        assert_eq!(
            [
                ImportMetricOutcome::Success,
                ImportMetricOutcome::Failed,
                ImportMetricOutcome::PartiallySucceeded,
                ImportMetricOutcome::Error,
            ]
            .map(ImportMetricOutcome::as_str),
            ["success", "failed", "partially_succeeded", "error",]
        );
    }
}
