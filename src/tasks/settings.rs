use std::time::{Duration, Instant};

use chrono::Utc;

/// Validated settings for task execution, lease renewal, and maintenance work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TaskWorkerSettings {
    worker_count: usize,
    poll_interval: Duration,
    lease_duration: TaskLeaseDuration,
    heartbeat_interval: Duration,
    recovery_interval: Duration,
    export_output_cleanup_interval: Duration,
}

impl TaskWorkerSettings {
    pub fn builder() -> TaskWorkerSettingsBuilder {
        TaskWorkerSettingsBuilder::default()
    }

    pub const fn worker_count(self) -> usize {
        self.worker_count
    }

    pub const fn poll_interval(self) -> Duration {
        self.poll_interval
    }

    pub const fn lease_duration(self) -> Duration {
        self.lease_duration.duration()
    }

    pub const fn heartbeat_interval(self) -> Duration {
        self.heartbeat_interval
    }

    pub const fn recovery_interval(self) -> Duration {
        self.recovery_interval
    }

    pub const fn export_output_cleanup_interval(self) -> Duration {
        self.export_output_cleanup_interval
    }
}

/// Builder for the multi-field task-worker policy.
#[derive(Debug, Default)]
pub struct TaskWorkerSettingsBuilder {
    worker_count: Option<usize>,
    poll_interval: Option<Duration>,
    lease_duration: Option<Duration>,
    heartbeat_interval: Option<Duration>,
    recovery_interval: Option<Duration>,
    export_output_cleanup_interval: Option<Duration>,
}

impl TaskWorkerSettingsBuilder {
    pub fn worker_count(mut self, value: usize) -> Self {
        self.worker_count = Some(value);
        self
    }

    pub fn poll_interval(mut self, value: Duration) -> Self {
        self.poll_interval = Some(value);
        self
    }

    pub fn lease_duration(mut self, value: Duration) -> Self {
        self.lease_duration = Some(value);
        self
    }

    pub fn heartbeat_interval(mut self, value: Duration) -> Self {
        self.heartbeat_interval = Some(value);
        self
    }

    pub fn recovery_interval(mut self, value: Duration) -> Self {
        self.recovery_interval = Some(value);
        self
    }

    pub fn export_output_cleanup_interval(mut self, value: Duration) -> Self {
        self.export_output_cleanup_interval = Some(value);
        self
    }

    pub fn build(self) -> Result<TaskWorkerSettings, String> {
        let worker_count = self
            .worker_count
            .ok_or_else(|| "task worker count is required".to_string())?;
        let poll_interval = self
            .poll_interval
            .ok_or_else(|| "task worker poll interval is required".to_string())?;
        let lease_duration = self
            .lease_duration
            .ok_or_else(|| "task worker lease duration is required".to_string())?;
        let heartbeat_interval = self
            .heartbeat_interval
            .ok_or_else(|| "task worker heartbeat interval is required".to_string())?;
        let recovery_interval = self
            .recovery_interval
            .ok_or_else(|| "task recovery interval is required".to_string())?;
        let export_output_cleanup_interval = self
            .export_output_cleanup_interval
            .ok_or_else(|| "export output cleanup interval is required".to_string())?;

        if poll_interval.is_zero() {
            return Err("task worker poll interval must be greater than zero".to_string());
        }
        let lease_duration = TaskLeaseDuration::new(lease_duration)?;
        if heartbeat_interval.is_zero() || heartbeat_interval >= lease_duration.duration() {
            return Err(
                "task worker heartbeat interval must be greater than zero and shorter than the lease"
                    .to_string(),
            );
        }
        if recovery_interval.is_zero() {
            return Err("task recovery interval must be greater than zero".to_string());
        }
        if export_output_cleanup_interval.is_zero() {
            return Err("export output cleanup interval must be greater than zero".to_string());
        }

        Ok(TaskWorkerSettings {
            worker_count,
            poll_interval,
            lease_duration,
            heartbeat_interval,
            recovery_interval,
            export_output_cleanup_interval,
        })
    }
}

/// A task lease duration that is safe for both worker clocks and PostgreSQL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TaskLeaseDuration {
    duration: Duration,
    database_milliseconds: i64,
}

impl TaskLeaseDuration {
    pub(crate) fn new(duration: Duration) -> Result<Self, String> {
        if duration.is_zero() {
            return Err("task worker lease duration must be greater than zero".to_string());
        }
        let database_milliseconds = i64::try_from(duration.as_millis()).map_err(|_| {
            "task worker lease duration is too large for database timestamps".to_string()
        })?;
        let chrono_duration = chrono::Duration::from_std(duration).map_err(|_| {
            "task worker lease duration is too large for database timestamps".to_string()
        })?;
        if Utc::now()
            .naive_utc()
            .checked_add_signed(chrono_duration)
            .is_none()
        {
            return Err(
                "task worker lease duration is too large for database timestamps".to_string(),
            );
        }
        if Instant::now().checked_add(duration).is_none() {
            return Err("task worker lease duration is too large for worker clocks".to_string());
        }
        Ok(Self {
            duration,
            database_milliseconds,
        })
    }

    pub(crate) const fn duration(self) -> Duration {
        self.duration
    }

    pub(crate) const fn database_milliseconds(self) -> i64 {
        self.database_milliseconds
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_builder() -> TaskWorkerSettingsBuilder {
        TaskWorkerSettings::builder()
            .worker_count(2)
            .poll_interval(Duration::from_millis(250))
            .lease_duration(Duration::from_secs(60))
            .heartbeat_interval(Duration::from_secs(10))
            .recovery_interval(Duration::from_secs(30))
            .export_output_cleanup_interval(Duration::from_secs(300))
    }

    #[test]
    fn task_worker_settings_preserve_validated_values() {
        let settings = valid_builder().build().unwrap();

        assert_eq!(settings.worker_count(), 2);
        assert_eq!(settings.poll_interval(), Duration::from_millis(250));
        assert_eq!(settings.lease_duration(), Duration::from_secs(60));
        assert_eq!(settings.heartbeat_interval(), Duration::from_secs(10));
        assert_eq!(settings.recovery_interval(), Duration::from_secs(30));
        assert_eq!(
            settings.export_output_cleanup_interval(),
            Duration::from_secs(300)
        );
        assert_eq!(
            TaskLeaseDuration::new(settings.lease_duration())
                .unwrap()
                .database_milliseconds(),
            60_000
        );
    }

    #[test]
    fn task_worker_settings_require_every_builder_field() {
        assert_eq!(
            TaskWorkerSettings::builder().build().unwrap_err(),
            "task worker count is required"
        );
    }

    #[rstest::rstest]
    #[case::poll("poll")]
    #[case::lease("lease")]
    #[case::heartbeat("heartbeat")]
    #[case::recovery("recovery")]
    #[case::cleanup("cleanup")]
    fn task_worker_settings_reject_zero_durations(#[case] zero_field: &str) {
        let mut builder = valid_builder();
        builder = match zero_field {
            "poll" => builder.poll_interval(Duration::ZERO),
            "lease" => builder.lease_duration(Duration::ZERO),
            "heartbeat" => builder.heartbeat_interval(Duration::ZERO),
            "recovery" => builder.recovery_interval(Duration::ZERO),
            "cleanup" => builder.export_output_cleanup_interval(Duration::ZERO),
            _ => unreachable!(),
        };

        assert!(builder.build().is_err());
    }

    #[test]
    fn task_worker_settings_reject_heartbeat_at_or_beyond_lease() {
        let error = valid_builder()
            .heartbeat_interval(Duration::from_secs(60))
            .build()
            .unwrap_err();

        assert!(error.contains("shorter than the lease"));
    }

    #[test]
    fn task_worker_settings_reject_unrepresentable_lease_duration() {
        let error = valid_builder()
            .lease_duration(Duration::from_secs(u64::MAX))
            .build()
            .unwrap_err();

        assert_eq!(
            error,
            "task worker lease duration is too large for database timestamps"
        );
    }
}
