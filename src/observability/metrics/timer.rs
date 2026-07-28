use std::time::{Duration, Instant};

/// Shared lifecycle state for metric timers that record an explicit outcome.
pub(super) struct OutcomeTimer {
    started_at: Instant,
    finished: bool,
}

impl OutcomeTimer {
    pub(super) fn start() -> Self {
        Self {
            started_at: Instant::now(),
            finished: false,
        }
    }

    pub(super) fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    pub(super) fn finish(&mut self) -> Duration {
        self.finished = true;
        self.elapsed()
    }

    pub(super) fn unfinished_elapsed(&self) -> Option<Duration> {
        (!self.finished).then(|| self.elapsed())
    }
}

#[cfg(test)]
mod tests {
    use super::OutcomeTimer;

    #[test]
    fn finished_timer_is_not_reported_as_unfinished() {
        let mut timer = OutcomeTimer::start();

        timer.finish();

        assert!(timer.unfinished_elapsed().is_none());
    }
}
