use std::num::{NonZeroI32, NonZeroUsize};
use std::time::Duration as StdDuration;

use chrono::{Duration, NaiveDateTime, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EventWorkerBatchSize {
    value: NonZeroUsize,
    database_limit: i64,
}

impl EventWorkerBatchSize {
    fn new(value: usize, field: &str) -> Result<Self, String> {
        let value =
            NonZeroUsize::new(value).ok_or_else(|| format!("{field} must be greater than 0"))?;
        let database_limit = i64::try_from(value.get())
            .map_err(|_| format!("{field} is too large for database queries"))?;
        Ok(Self {
            value,
            database_limit,
        })
    }

    const fn get(self) -> usize {
        self.value.get()
    }

    const fn database_limit(self) -> i64 {
        self.database_limit
    }
}

fn database_timeout(milliseconds: u64, field: &str) -> Result<Duration, String> {
    if milliseconds == 0 {
        return Err(format!("{field} must be greater than 0"));
    }
    let milliseconds = i64::try_from(milliseconds)
        .map_err(|_| format!("{field} is too large for database timestamps"))?;
    let duration = Duration::try_milliseconds(milliseconds)
        .ok_or_else(|| format!("{field} is too large for database timestamps"))?;
    if Utc::now()
        .naive_utc()
        .checked_add_signed(duration)
        .is_none()
    {
        return Err(format!("{field} is too large for database timestamps"));
    }
    Ok(duration)
}

fn retention_duration(days: i64, field: &str) -> Result<Duration, String> {
    if days <= 0 {
        return Err(format!("{field} must be greater than 0"));
    }
    let duration = Duration::try_days(days)
        .ok_or_else(|| format!("{field} is too large for database timestamps"))?;
    if Utc::now()
        .naive_utc()
        .checked_sub_signed(duration)
        .is_none()
    {
        return Err(format!("{field} is too large for database timestamps"));
    }
    Ok(duration)
}

/// Validated policy used by event-delivery database claims and transports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EventDeliverySettings {
    batch_size: EventWorkerBatchSize,
    lock_timeout: Duration,
    lock_timeout_ms: u64,
    transport_timeout: StdDuration,
    transport_timeout_ms: u64,
    retry_backoff_base_ms: u64,
    retry_backoff_max_ms: u64,
    max_attempts: NonZeroI32,
}

impl EventDeliverySettings {
    pub(crate) fn builder() -> EventDeliverySettingsBuilder {
        EventDeliverySettingsBuilder::default()
    }

    pub(crate) const fn batch_size(self) -> usize {
        self.batch_size.get()
    }

    pub(crate) const fn database_batch_size(self) -> i64 {
        self.batch_size.database_limit()
    }

    pub(crate) const fn lock_deadline(self, now: NaiveDateTime) -> Option<NaiveDateTime> {
        now.checked_add_signed(self.lock_timeout)
    }

    pub(crate) const fn lock_timeout_ms(self) -> u64 {
        self.lock_timeout_ms
    }

    pub(crate) const fn transport_timeout(self) -> StdDuration {
        self.transport_timeout
    }

    pub(crate) const fn transport_timeout_ms(self) -> u64 {
        self.transport_timeout_ms
    }

    pub(crate) const fn retry_backoff_base_ms(self) -> u64 {
        self.retry_backoff_base_ms
    }

    pub(crate) const fn retry_backoff_max_ms(self) -> u64 {
        self.retry_backoff_max_ms
    }

    pub(crate) fn retry_deadline(self, now: NaiveDateTime, attempts: i32) -> Option<NaiveDateTime> {
        let exponent = attempts.saturating_sub(1).min(31) as u32;
        let delay_ms = self
            .retry_backoff_base_ms
            .saturating_mul(2_u64.saturating_pow(exponent))
            .min(self.retry_backoff_max_ms);
        let delay_ms = i64::try_from(delay_ms).ok()?;
        let delay = Duration::try_milliseconds(delay_ms)?;
        now.checked_add_signed(delay)
    }

    pub(crate) const fn max_attempts(self) -> i32 {
        self.max_attempts.get()
    }
}

/// Builder for the multi-field event-delivery policy.
#[derive(Debug, Default)]
pub(crate) struct EventDeliverySettingsBuilder {
    batch_size: Option<usize>,
    lock_timeout_ms: Option<u64>,
    transport_timeout_ms: Option<u64>,
    retry_backoff_base_ms: Option<u64>,
    retry_backoff_max_ms: Option<u64>,
    max_attempts: Option<i32>,
}

impl EventDeliverySettingsBuilder {
    pub(crate) fn batch_size(mut self, value: usize) -> Self {
        self.batch_size = Some(value);
        self
    }

    pub(crate) fn lock_timeout_ms(mut self, value: u64) -> Self {
        self.lock_timeout_ms = Some(value);
        self
    }

    pub(crate) fn transport_timeout_ms(mut self, value: u64) -> Self {
        self.transport_timeout_ms = Some(value);
        self
    }

    pub(crate) fn retry_backoff_base_ms(mut self, value: u64) -> Self {
        self.retry_backoff_base_ms = Some(value);
        self
    }

    pub(crate) fn retry_backoff_max_ms(mut self, value: u64) -> Self {
        self.retry_backoff_max_ms = Some(value);
        self
    }

    pub(crate) fn max_attempts(mut self, value: i32) -> Self {
        self.max_attempts = Some(value);
        self
    }

    pub(crate) fn build(self) -> Result<EventDeliverySettings, String> {
        let batch_size = self
            .batch_size
            .ok_or_else(|| "event_delivery_batch_size is required".to_string())?;
        let lock_timeout_ms = self
            .lock_timeout_ms
            .ok_or_else(|| "event_delivery_lock_timeout_ms is required".to_string())?;
        let transport_timeout_ms = self
            .transport_timeout_ms
            .ok_or_else(|| "event_delivery_transport_timeout_ms is required".to_string())?;
        let retry_backoff_base_ms = self
            .retry_backoff_base_ms
            .ok_or_else(|| "event_delivery_retry_backoff_base_ms is required".to_string())?;
        let retry_backoff_max_ms = self
            .retry_backoff_max_ms
            .ok_or_else(|| "event_delivery_retry_backoff_max_ms is required".to_string())?;
        let max_attempts = self
            .max_attempts
            .ok_or_else(|| "event_delivery_max_attempts is required".to_string())?;

        let batch_size = EventWorkerBatchSize::new(batch_size, "event_delivery_batch_size")?;
        let lock_timeout = database_timeout(lock_timeout_ms, "event_delivery_lock_timeout_ms")?;
        if transport_timeout_ms == 0 {
            return Err("event_delivery_transport_timeout_ms must be greater than 0".to_string());
        }
        if transport_timeout_ms >= lock_timeout_ms {
            return Err(format!(
                "event_delivery_transport_timeout_ms ({transport_timeout_ms}) must be less than event_delivery_lock_timeout_ms ({lock_timeout_ms})"
            ));
        }
        if retry_backoff_base_ms == 0 {
            return Err("event_delivery_retry_backoff_base_ms must be greater than 0".to_string());
        }
        if retry_backoff_max_ms == 0 {
            return Err("event_delivery_retry_backoff_max_ms must be greater than 0".to_string());
        }
        if retry_backoff_base_ms > retry_backoff_max_ms {
            return Err(format!(
                "event_delivery_retry_backoff_base_ms ({retry_backoff_base_ms}) must be less than or equal to event_delivery_retry_backoff_max_ms ({retry_backoff_max_ms})"
            ));
        }
        database_timeout(retry_backoff_max_ms, "event_delivery_retry_backoff_max_ms")?;
        let max_attempts = NonZeroI32::new(max_attempts)
            .filter(|value| value.get() > 0)
            .ok_or_else(|| "event_delivery_max_attempts must be greater than 0".to_string())?;

        Ok(EventDeliverySettings {
            batch_size,
            lock_timeout,
            lock_timeout_ms,
            transport_timeout: StdDuration::from_millis(transport_timeout_ms),
            transport_timeout_ms,
            retry_backoff_base_ms,
            retry_backoff_max_ms,
            max_attempts,
        })
    }
}

/// Validated policy used when claiming events for subscription fan-out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EventFanoutSettings {
    batch_size: EventWorkerBatchSize,
    lock_timeout: Duration,
    lock_timeout_ms: u64,
}

impl EventFanoutSettings {
    pub(crate) fn new(batch_size: usize, lock_timeout_ms: u64) -> Result<Self, String> {
        Ok(Self {
            batch_size: EventWorkerBatchSize::new(batch_size, "event_fanout_batch_size")?,
            lock_timeout: database_timeout(lock_timeout_ms, "event_fanout_lock_timeout_ms")?,
            lock_timeout_ms,
        })
    }

    pub(crate) const fn batch_size(self) -> usize {
        self.batch_size.get()
    }

    pub(crate) const fn database_batch_size(self) -> i64 {
        self.batch_size.database_limit()
    }

    pub(crate) const fn lock_deadline(self, now: NaiveDateTime) -> Option<NaiveDateTime> {
        now.checked_add_signed(self.lock_timeout)
    }

    pub(crate) const fn lock_timeout_ms(self) -> u64 {
        self.lock_timeout_ms
    }
}

/// Validated policy used by event and terminal-delivery retention queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EventRetentionSettings {
    event_retention: Duration,
    delivery_retention: Duration,
    batch_size: EventWorkerBatchSize,
}

impl EventRetentionSettings {
    pub(crate) fn new(
        event_retention_days: i64,
        delivery_retention_days: i64,
        batch_size: usize,
    ) -> Result<Self, String> {
        Ok(Self {
            event_retention: retention_duration(event_retention_days, "event_retention_days")?,
            delivery_retention: retention_duration(
                delivery_retention_days,
                "event_delivery_retention_days",
            )?,
            batch_size: EventWorkerBatchSize::new(batch_size, "event_retention_purge_batch_size")?,
        })
    }

    pub(crate) const fn event_cutoff(self, now: NaiveDateTime) -> Option<NaiveDateTime> {
        now.checked_sub_signed(self.event_retention)
    }

    pub(crate) const fn delivery_cutoff(self, now: NaiveDateTime) -> Option<NaiveDateTime> {
        now.checked_sub_signed(self.delivery_retention)
    }

    pub(crate) const fn event_retention_days(self) -> i64 {
        self.event_retention.num_days()
    }

    pub(crate) const fn delivery_retention_days(self) -> i64 {
        self.delivery_retention.num_days()
    }

    pub(crate) const fn batch_size(self) -> usize {
        self.batch_size.get()
    }

    pub(crate) const fn database_batch_size(self) -> i64 {
        self.batch_size.database_limit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_delivery_settings() -> EventDeliverySettings {
        EventDeliverySettings::builder()
            .batch_size(100)
            .lock_timeout_ms(30_000)
            .transport_timeout_ms(10_000)
            .retry_backoff_base_ms(100)
            .retry_backoff_max_ms(5_000)
            .max_attempts(5)
            .build()
            .unwrap()
    }

    #[test]
    fn delivery_settings_preserve_validated_values() {
        let settings = valid_delivery_settings();

        assert_eq!(settings.batch_size(), 100);
        assert_eq!(settings.database_batch_size(), 100);
        assert_eq!(settings.lock_timeout_ms(), 30_000);
        assert_eq!(settings.transport_timeout(), StdDuration::from_secs(10));
        assert_eq!(settings.retry_backoff_base_ms(), 100);
        assert_eq!(settings.retry_backoff_max_ms(), 5_000);
        assert_eq!(settings.max_attempts(), 5);

        let now = chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .unwrap()
            .naive_utc();
        assert_eq!(
            settings.retry_deadline(now, 3),
            now.checked_add_signed(Duration::milliseconds(400))
        );
    }

    #[test]
    fn delivery_settings_require_every_builder_field() {
        let error = EventDeliverySettings::builder().build().unwrap_err();

        assert_eq!(error, "event_delivery_batch_size is required");
    }

    #[rstest::rstest]
    #[case::batch_size(0, 30_000, 10_000, 100, 5_000, 5, "event_delivery_batch_size")]
    #[case::lock_timeout(100, 0, 0, 100, 5_000, 5, "event_delivery_lock_timeout_ms")]
    #[case::transport_timeout(100, 30_000, 0, 100, 5_000, 5, "event_delivery_transport_timeout_ms")]
    #[case::backoff_base(
        100,
        30_000,
        10_000,
        0,
        5_000,
        5,
        "event_delivery_retry_backoff_base_ms"
    )]
    #[case::backoff_max(100, 30_000, 10_000, 100, 0, 5, "event_delivery_retry_backoff_max_ms")]
    #[case::attempts(100, 30_000, 10_000, 100, 5_000, 0, "event_delivery_max_attempts")]
    #[case::negative_attempts(100, 30_000, 10_000, 100, 5_000, -1, "event_delivery_max_attempts")]
    fn delivery_settings_reject_non_positive_values(
        #[case] batch_size: usize,
        #[case] lock_timeout_ms: u64,
        #[case] transport_timeout_ms: u64,
        #[case] retry_backoff_base_ms: u64,
        #[case] retry_backoff_max_ms: u64,
        #[case] max_attempts: i32,
        #[case] field: &str,
    ) {
        let error = EventDeliverySettings::builder()
            .batch_size(batch_size)
            .lock_timeout_ms(lock_timeout_ms)
            .transport_timeout_ms(transport_timeout_ms)
            .retry_backoff_base_ms(retry_backoff_base_ms)
            .retry_backoff_max_ms(retry_backoff_max_ms)
            .max_attempts(max_attempts)
            .build()
            .unwrap_err();

        assert!(error.contains(field));
    }

    #[test]
    fn delivery_settings_reject_cross_field_inversions() {
        let timeout_error = EventDeliverySettings::builder()
            .batch_size(100)
            .lock_timeout_ms(10_000)
            .transport_timeout_ms(10_000)
            .retry_backoff_base_ms(100)
            .retry_backoff_max_ms(5_000)
            .max_attempts(5)
            .build()
            .unwrap_err();
        assert!(timeout_error.contains("must be less than"));

        let backoff_error = EventDeliverySettings::builder()
            .batch_size(100)
            .lock_timeout_ms(30_000)
            .transport_timeout_ms(10_000)
            .retry_backoff_base_ms(5_001)
            .retry_backoff_max_ms(5_000)
            .max_attempts(5)
            .build()
            .unwrap_err();
        assert!(backoff_error.contains("less than or equal"));
    }

    #[test]
    fn database_timeouts_reject_unrepresentable_values() {
        let delivery_error = EventDeliverySettings::builder()
            .batch_size(100)
            .lock_timeout_ms(u64::MAX)
            .transport_timeout_ms(10_000)
            .retry_backoff_base_ms(100)
            .retry_backoff_max_ms(5_000)
            .max_attempts(5)
            .build()
            .unwrap_err();
        assert!(delivery_error.contains("too large for database timestamps"));

        let fanout_error = EventFanoutSettings::new(100, u64::MAX).unwrap_err();
        assert!(fanout_error.contains("too large for database timestamps"));

        let retry_error = EventDeliverySettings::builder()
            .batch_size(100)
            .lock_timeout_ms(30_000)
            .transport_timeout_ms(10_000)
            .retry_backoff_base_ms(100)
            .retry_backoff_max_ms(u64::MAX)
            .max_attempts(5)
            .build()
            .unwrap_err();
        assert!(retry_error.contains("too large for database timestamps"));
    }

    #[test]
    fn retention_settings_reject_invalid_or_unrepresentable_values() {
        assert!(EventRetentionSettings::new(0, 30, 100).is_err());
        assert!(EventRetentionSettings::new(30, 0, 100).is_err());
        assert!(EventRetentionSettings::new(30, 30, 0).is_err());

        let error = EventRetentionSettings::new(i64::MAX, 30, 100).unwrap_err();
        assert!(error.contains("too large for database timestamps"));
    }
}
