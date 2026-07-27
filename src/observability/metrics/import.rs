use std::time::{Duration, Instant};

use opentelemetry::KeyValue;

use super::current;

fn import_phase_attrs(phase: &'static str, outcome: &'static str) -> [KeyValue; 2] {
    [
        KeyValue::new("phase", phase),
        KeyValue::new("outcome", outcome),
    ]
}

fn record_import_phase_duration(phase: &'static str, outcome: &'static str, duration: Duration) {
    if let Some(metrics) = current() {
        metrics
            .import_duration
            .record(duration.as_secs_f64(), &import_phase_attrs(phase, outcome));
    }
}

pub fn import_phase_duration(phase: &'static str, duration: Duration) {
    record_import_phase_duration(phase, "success", duration);
}

#[must_use = "an import phase timer must be finished or dropped to record its outcome"]
pub struct ImportPhaseTimer {
    phase: &'static str,
    started_at: Instant,
    finished: bool,
}

impl ImportPhaseTimer {
    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    pub fn finish(mut self, outcome: &'static str) -> Duration {
        let elapsed = self.started_at.elapsed();
        record_import_phase_duration(self.phase, outcome, elapsed);
        self.finished = true;
        elapsed
    }
}

impl Drop for ImportPhaseTimer {
    fn drop(&mut self) {
        if !self.finished {
            record_import_phase_duration(self.phase, "error", self.started_at.elapsed());
        }
    }
}

pub fn import_phase_timer(phase: &'static str) -> ImportPhaseTimer {
    ImportPhaseTimer {
        phase,
        started_at: Instant::now(),
        finished: false,
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
}
