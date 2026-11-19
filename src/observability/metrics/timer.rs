use std::time::{Duration, Instant};

/// Shared lifecycle state for metric timers that record an explicit outcome.
pub(super) struct OutcomeTimer {
    started_at: Option<Instant>,
    accumulated: Duration,
    finished: bool,
}

impl OutcomeTimer {
    pub(super) fn start() -> Self {
        Self {
            started_at: Some(Instant::now()),
            accumulated: Duration::ZERO,
            finished: false,
        }
    }

    pub(super) fn elapsed(&self) -> Duration {
        self.accumulated
            + self
                .started_at
                .map_or(Duration::ZERO, |start| start.elapsed())
    }

    pub(super) fn pause(&mut self) {
        if let Some(start) = self.started_at.take() {
            self.accumulated += start.elapsed();
        }
    }

    pub(super) fn resume(&mut self) {
        self.started_at.get_or_insert_with(Instant::now);
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
    fn paused_phases_exclude_other_work_and_accumulate_across_pages() {
        use std::time::{Duration, Instant};

        let mut timer = OutcomeTimer::start();
        timer.started_at = Some(Instant::now() - Duration::from_secs(2));
        timer.pause();
        let first = timer.elapsed();
        assert_eq!(timer.elapsed(), first);
        timer.resume();
        timer.started_at = Some(Instant::now() - Duration::from_secs(3));
        assert!(timer.finish() >= first + Duration::from_secs(3));
    }

    #[test]
    fn finished_timer_is_not_reported_as_unfinished() {
        let mut timer = OutcomeTimer::start();

        timer.finish();

        assert!(timer.unfinished_elapsed().is_none());
    }
}
