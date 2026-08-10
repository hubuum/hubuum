use std::fmt;
use std::num::{NonZeroI32, NonZeroUsize};
use std::time::Duration as StdDuration;

use chrono::{Duration, NaiveDateTime, Utc};

/// Validation failure for an event worker or retention policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventPolicyError(String);

impl EventPolicyError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for EventPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for EventPolicyError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EventWorkerBatchSize {
    value: NonZeroUsize,
    query_limit: i64,
}

impl EventWorkerBatchSize {
    fn new(value: usize, field: &str) -> Result<Self, EventPolicyError> {
        let value = NonZeroUsize::new(value)
            .ok_or_else(|| EventPolicyError::new(format!("{field} must be greater than 0")))?;
        let query_limit = i64::try_from(value.get())
            .map_err(|_| EventPolicyError::new(format!("{field} is too large for queries")))?;
        Ok(Self { value, query_limit })
    }

    const fn get(self) -> usize {
        self.value.get()
    }

    const fn query_limit(self) -> i64 {
        self.query_limit
    }
}

fn validated_timeout(milliseconds: u64, field: &str) -> Result<Duration, EventPolicyError> {
    if milliseconds == 0 {
        return Err(EventPolicyError::new(format!(
            "{field} must be greater than 0"
        )));
    }
    let milliseconds = i64::try_from(milliseconds)
        .map_err(|_| EventPolicyError::new(format!("{field} is too large for timestamps")))?;
    let duration = Duration::try_milliseconds(milliseconds)
        .ok_or_else(|| EventPolicyError::new(format!("{field} is too large for timestamps")))?;
    if Utc::now()
        .naive_utc()
        .checked_add_signed(duration)
        .is_none()
    {
        return Err(EventPolicyError::new(format!(
            "{field} is too large for timestamps"
        )));
    }
    Ok(duration)
}

fn retention_duration(days: i64, field: &str) -> Result<Duration, EventPolicyError> {
    if days <= 0 {
        return Err(EventPolicyError::new(format!(
            "{field} must be greater than 0"
        )));
    }
    let duration = Duration::try_days(days)
        .ok_or_else(|| EventPolicyError::new(format!("{field} is too large for timestamps")))?;
    if Utc::now()
        .naive_utc()
        .checked_sub_signed(duration)
        .is_none()
    {
        return Err(EventPolicyError::new(format!(
            "{field} is too large for timestamps"
        )));
    }
    Ok(duration)
}

/// Validated policy used by event-delivery claims and transports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventDeliverySettings {
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
    pub fn builder() -> EventDeliverySettingsBuilder {
        EventDeliverySettingsBuilder::default()
    }

    pub const fn batch_size(self) -> usize {
        self.batch_size.get()
    }

    pub const fn query_batch_size(self) -> i64 {
        self.batch_size.query_limit()
    }

    pub const fn lock_deadline(self, now: NaiveDateTime) -> Option<NaiveDateTime> {
        now.checked_add_signed(self.lock_timeout)
    }

    pub const fn lock_timeout_ms(self) -> u64 {
        self.lock_timeout_ms
    }

    pub const fn transport_timeout(self) -> StdDuration {
        self.transport_timeout
    }

    pub const fn transport_timeout_ms(self) -> u64 {
        self.transport_timeout_ms
    }

    pub const fn retry_backoff_base_ms(self) -> u64 {
        self.retry_backoff_base_ms
    }

    pub const fn retry_backoff_max_ms(self) -> u64 {
        self.retry_backoff_max_ms
    }

    pub fn retry_deadline(self, now: NaiveDateTime, attempts: i32) -> Option<NaiveDateTime> {
        let exponent = attempts.saturating_sub(1).min(31) as u32;
        let delay_ms = self
            .retry_backoff_base_ms
            .saturating_mul(2_u64.saturating_pow(exponent))
            .min(self.retry_backoff_max_ms);
        let delay_ms = i64::try_from(delay_ms).ok()?;
        let delay = Duration::try_milliseconds(delay_ms)?;
        now.checked_add_signed(delay)
    }

    pub const fn max_attempts(self) -> i32 {
        self.max_attempts.get()
    }
}

/// Builder for the multi-field event-delivery policy.
#[derive(Debug, Default)]
pub struct EventDeliverySettingsBuilder {
    batch_size: Option<usize>,
    lock_timeout_ms: Option<u64>,
    transport_timeout_ms: Option<u64>,
    retry_backoff_base_ms: Option<u64>,
    retry_backoff_max_ms: Option<u64>,
    max_attempts: Option<i32>,
}

impl EventDeliverySettingsBuilder {
    pub fn batch_size(mut self, value: usize) -> Self {
        self.batch_size = Some(value);
        self
    }

    pub fn lock_timeout_ms(mut self, value: u64) -> Self {
        self.lock_timeout_ms = Some(value);
        self
    }

    pub fn transport_timeout_ms(mut self, value: u64) -> Self {
        self.transport_timeout_ms = Some(value);
        self
    }

    pub fn retry_backoff_base_ms(mut self, value: u64) -> Self {
        self.retry_backoff_base_ms = Some(value);
        self
    }

    pub fn retry_backoff_max_ms(mut self, value: u64) -> Self {
        self.retry_backoff_max_ms = Some(value);
        self
    }

    pub fn max_attempts(mut self, value: i32) -> Self {
        self.max_attempts = Some(value);
        self
    }

    pub fn build(self) -> Result<EventDeliverySettings, EventPolicyError> {
        let batch_size = self
            .batch_size
            .ok_or_else(|| EventPolicyError::new("event_delivery_batch_size is required"))?;
        let lock_timeout_ms = self
            .lock_timeout_ms
            .ok_or_else(|| EventPolicyError::new("event_delivery_lock_timeout_ms is required"))?;
        let transport_timeout_ms = self.transport_timeout_ms.ok_or_else(|| {
            EventPolicyError::new("event_delivery_transport_timeout_ms is required")
        })?;
        let retry_backoff_base_ms = self.retry_backoff_base_ms.ok_or_else(|| {
            EventPolicyError::new("event_delivery_retry_backoff_base_ms is required")
        })?;
        let retry_backoff_max_ms = self.retry_backoff_max_ms.ok_or_else(|| {
            EventPolicyError::new("event_delivery_retry_backoff_max_ms is required")
        })?;
        let max_attempts = self
            .max_attempts
            .ok_or_else(|| EventPolicyError::new("event_delivery_max_attempts is required"))?;

        let batch_size = EventWorkerBatchSize::new(batch_size, "event_delivery_batch_size")?;
        let lock_timeout = validated_timeout(lock_timeout_ms, "event_delivery_lock_timeout_ms")?;
        if transport_timeout_ms == 0 {
            return Err(EventPolicyError::new(
                "event_delivery_transport_timeout_ms must be greater than 0",
            ));
        }
        if transport_timeout_ms >= lock_timeout_ms {
            return Err(EventPolicyError::new(format!(
                "event_delivery_transport_timeout_ms ({transport_timeout_ms}) must be less than event_delivery_lock_timeout_ms ({lock_timeout_ms})"
            )));
        }
        if retry_backoff_base_ms == 0 {
            return Err(EventPolicyError::new(
                "event_delivery_retry_backoff_base_ms must be greater than 0",
            ));
        }
        if retry_backoff_max_ms == 0 {
            return Err(EventPolicyError::new(
                "event_delivery_retry_backoff_max_ms must be greater than 0",
            ));
        }
        if retry_backoff_base_ms > retry_backoff_max_ms {
            return Err(EventPolicyError::new(format!(
                "event_delivery_retry_backoff_base_ms ({retry_backoff_base_ms}) must be less than or equal to event_delivery_retry_backoff_max_ms ({retry_backoff_max_ms})"
            )));
        }
        validated_timeout(retry_backoff_max_ms, "event_delivery_retry_backoff_max_ms")?;
        let max_attempts = NonZeroI32::new(max_attempts)
            .filter(|value| value.get() > 0)
            .ok_or_else(|| {
                EventPolicyError::new("event_delivery_max_attempts must be greater than 0")
            })?;

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
pub struct EventFanoutSettings {
    batch_size: EventWorkerBatchSize,
    lock_timeout: Duration,
    lock_timeout_ms: u64,
}

impl EventFanoutSettings {
    pub fn new(batch_size: usize, lock_timeout_ms: u64) -> Result<Self, EventPolicyError> {
        Ok(Self {
            batch_size: EventWorkerBatchSize::new(batch_size, "event_fanout_batch_size")?,
            lock_timeout: validated_timeout(lock_timeout_ms, "event_fanout_lock_timeout_ms")?,
            lock_timeout_ms,
        })
    }

    pub const fn batch_size(self) -> usize {
        self.batch_size.get()
    }

    pub const fn query_batch_size(self) -> i64 {
        self.batch_size.query_limit()
    }

    pub const fn lock_deadline(self, now: NaiveDateTime) -> Option<NaiveDateTime> {
        now.checked_add_signed(self.lock_timeout)
    }

    pub const fn lock_timeout_ms(self) -> u64 {
        self.lock_timeout_ms
    }
}

/// Validated policy used by event and terminal-delivery retention queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventRetentionSettings {
    event_retention: Duration,
    delivery_retention: Duration,
    batch_size: EventWorkerBatchSize,
}

impl EventRetentionSettings {
    pub fn new(
        event_retention_days: i64,
        delivery_retention_days: i64,
        batch_size: usize,
    ) -> Result<Self, EventPolicyError> {
        Ok(Self {
            event_retention: retention_duration(event_retention_days, "event_retention_days")?,
            delivery_retention: retention_duration(
                delivery_retention_days,
                "event_delivery_retention_days",
            )?,
            batch_size: EventWorkerBatchSize::new(batch_size, "event_retention_purge_batch_size")?,
        })
    }

    pub const fn event_cutoff(self, now: NaiveDateTime) -> Option<NaiveDateTime> {
        now.checked_sub_signed(self.event_retention)
    }

    pub const fn delivery_cutoff(self, now: NaiveDateTime) -> Option<NaiveDateTime> {
        now.checked_sub_signed(self.delivery_retention)
    }

    pub const fn event_retention_days(self) -> i64 {
        self.event_retention.num_days()
    }

    pub const fn delivery_retention_days(self) -> i64 {
        self.delivery_retention.num_days()
    }

    pub const fn batch_size(self) -> usize {
        self.batch_size.get()
    }

    pub const fn query_batch_size(self) -> i64 {
        self.batch_size.query_limit()
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
        assert_eq!(settings.query_batch_size(), 100);
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

        assert_eq!(error.to_string(), "event_delivery_batch_size is required");
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

        assert!(error.to_string().contains(field));
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
        assert!(timeout_error.to_string().contains("must be less than"));

        let backoff_error = EventDeliverySettings::builder()
            .batch_size(100)
            .lock_timeout_ms(30_000)
            .transport_timeout_ms(10_000)
            .retry_backoff_base_ms(5_001)
            .retry_backoff_max_ms(5_000)
            .max_attempts(5)
            .build()
            .unwrap_err();
        assert!(backoff_error.to_string().contains("less than or equal"));
    }

    #[test]
    fn timestamp_durations_reject_unrepresentable_values() {
        let delivery_error = EventDeliverySettings::builder()
            .batch_size(100)
            .lock_timeout_ms(u64::MAX)
            .transport_timeout_ms(10_000)
            .retry_backoff_base_ms(100)
            .retry_backoff_max_ms(5_000)
            .max_attempts(5)
            .build()
            .unwrap_err();
        assert!(
            delivery_error
                .to_string()
                .contains("too large for timestamps")
        );

        let fanout_error = EventFanoutSettings::new(100, u64::MAX).unwrap_err();
        assert!(
            fanout_error
                .to_string()
                .contains("too large for timestamps")
        );

        let retry_error = EventDeliverySettings::builder()
            .batch_size(100)
            .lock_timeout_ms(30_000)
            .transport_timeout_ms(10_000)
            .retry_backoff_base_ms(100)
            .retry_backoff_max_ms(u64::MAX)
            .max_attempts(5)
            .build()
            .unwrap_err();
        assert!(retry_error.to_string().contains("too large for timestamps"));
    }

    #[test]
    fn retention_settings_reject_invalid_or_unrepresentable_values() {
        assert!(EventRetentionSettings::new(0, 30, 100).is_err());
        assert!(EventRetentionSettings::new(30, 0, 100).is_err());
        assert!(EventRetentionSettings::new(30, 30, 0).is_err());

        let error = EventRetentionSettings::new(i64::MAX, 30, 100).unwrap_err();
        assert!(error.to_string().contains("too large for timestamps"));
    }
}
