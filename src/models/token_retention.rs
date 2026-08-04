use chrono::{Duration, NaiveDateTime};

use crate::errors::ApiError;

pub const MIN_TOKEN_RETENTION_PURGE_BATCH_SIZE: usize = 10;

/// Validated post-terminal period for retaining token metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenRetentionPeriod(Duration);

impl TokenRetentionPeriod {
    pub fn from_days(days: i64) -> Result<Self, ApiError> {
        if days <= 0 {
            return Err(ApiError::BadRequest(
                "token_retention_days must be greater than 0".to_string(),
            ));
        }
        Duration::try_days(days).map(Self).ok_or_else(|| {
            ApiError::BadRequest("token_retention_days is outside the supported range".to_string())
        })
    }

    pub fn days(self) -> i64 {
        self.0.num_days()
    }
}

/// A validated token lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenLifetime(Duration);

impl TokenLifetime {
    pub fn from_hours(hours: i64) -> Result<Self, ApiError> {
        Self::from_hours_for("token_lifetime_hours", hours)
    }

    fn from_hours_for(setting_name: &str, hours: i64) -> Result<Self, ApiError> {
        if hours <= 0 {
            return Err(ApiError::BadRequest(format!(
                "{setting_name} must be greater than 0"
            )));
        }
        if hours > i64::from(i32::MAX) {
            return Err(ApiError::BadRequest(format!(
                "{setting_name} must not exceed {}",
                i32::MAX
            )));
        }
        Duration::try_hours(hours).map(Self).ok_or_else(|| {
            ApiError::BadRequest(format!("{setting_name} is outside the supported range"))
        })
    }

    pub fn hours(self) -> i64 {
        self.0.num_hours()
    }

    pub fn cutoff_from(self, now: NaiveDateTime) -> Result<NaiveDateTime, ApiError> {
        now.checked_sub_signed(self.0).ok_or_else(|| {
            ApiError::BadRequest("token lifetime cutoff is outside the supported range".to_string())
        })
    }

    fn expiry_from(self, issued_at: NaiveDateTime) -> Result<NaiveDateTime, ApiError> {
        issued_at.checked_add_signed(self.0).ok_or_else(|| {
            ApiError::BadRequest("token expiry is outside the supported range".to_string())
        })
    }
}

/// Validated policy for materializing and bounding newly issued token expiry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TokenIssuancePolicy {
    default_lifetime: TokenLifetime,
    maximum_lifetime: TokenLifetime,
}

impl TokenIssuancePolicy {
    pub(crate) fn from_hours(
        default_lifetime_hours: i64,
        maximum_lifetime_hours: i64,
    ) -> Result<Self, ApiError> {
        let default_lifetime = TokenLifetime::from_hours(default_lifetime_hours)?;
        let maximum_lifetime =
            TokenLifetime::from_hours_for("max_token_lifetime_hours", maximum_lifetime_hours)?;
        if default_lifetime.hours() > maximum_lifetime.hours() {
            return Err(ApiError::BadRequest(
                "token_lifetime_hours must not exceed max_token_lifetime_hours".to_string(),
            ));
        }
        Ok(Self {
            default_lifetime,
            maximum_lifetime,
        })
    }

    pub(crate) fn default_lifetime(self) -> TokenLifetime {
        self.default_lifetime
    }

    pub(crate) fn resolve_expiry(
        self,
        issued_at: NaiveDateTime,
        requested_expiry: Option<NaiveDateTime>,
    ) -> Result<NaiveDateTime, ApiError> {
        let expires_at = match requested_expiry {
            Some(expires_at) => expires_at,
            None => self.default_lifetime.expiry_from(issued_at)?,
        };
        // PostgreSQL timestamps have microsecond precision. Reject a value
        // that is only later at Chrono's finer nanosecond precision because
        // persisting it would truncate it back to the issuance timestamp.
        let earliest_persisted_expiry = issued_at
            .checked_add_signed(Duration::microseconds(1))
            .ok_or_else(|| {
                ApiError::BadRequest("token expiry is outside the supported range".to_string())
            })?;
        if expires_at < earliest_persisted_expiry {
            return Err(ApiError::BadRequest(
                "expires_at must be later than the token issuance time".to_string(),
            ));
        }
        if expires_at > self.maximum_lifetime.expiry_from(issued_at)? {
            return Err(ApiError::BadRequest(format!(
                "expires_at must not exceed the configured maximum token lifetime of {} hours",
                self.maximum_lifetime.hours()
            )));
        }
        Ok(expires_at)
    }
}

/// Validated upper bound for rows deleted by one retention transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenRetentionBatchSize(usize);

impl TokenRetentionBatchSize {
    pub fn new(batch_size: usize) -> Result<Self, ApiError> {
        if batch_size < MIN_TOKEN_RETENTION_PURGE_BATCH_SIZE {
            return Err(ApiError::BadRequest(format!(
                "token_retention_purge_batch_size must be at least \
                 {MIN_TOKEN_RETENTION_PURGE_BATCH_SIZE}"
            )));
        }
        i64::try_from(batch_size).map_err(|_| {
            ApiError::BadRequest("token_retention_purge_batch_size is too large".to_string())
        })?;
        Ok(Self(batch_size))
    }

    pub fn get(self) -> usize {
        self.0
    }

    pub(crate) fn as_i64(self) -> i64 {
        self.0 as i64
    }
}

/// Complete, validated settings required by the destructive purge operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenRetentionSettings {
    retention_period: TokenRetentionPeriod,
    token_lifetime: TokenLifetime,
    batch_size: TokenRetentionBatchSize,
}

/// Database cutoffs derived together from one wall-clock timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenRetentionCutoffs {
    explicit_expiry: NaiveDateTime,
    implicit_issue: NaiveDateTime,
}

impl TokenRetentionCutoffs {
    pub fn explicit_expiry(self) -> NaiveDateTime {
        self.explicit_expiry
    }

    pub fn implicit_issue(self) -> NaiveDateTime {
        self.implicit_issue
    }
}

impl TokenRetentionSettings {
    pub fn builder() -> TokenRetentionSettingsBuilder {
        TokenRetentionSettingsBuilder::default()
    }

    pub fn retention_period(self) -> TokenRetentionPeriod {
        self.retention_period
    }

    pub fn token_lifetime(self) -> TokenLifetime {
        self.token_lifetime
    }

    pub fn batch_size(self) -> TokenRetentionBatchSize {
        self.batch_size
    }

    pub fn cutoffs(self, now: NaiveDateTime) -> Result<TokenRetentionCutoffs, ApiError> {
        let explicit_expiry = now
            .checked_sub_signed(self.retention_period.0)
            .ok_or_else(|| {
                ApiError::BadRequest(
                    "token retention cutoff is outside the supported range".to_string(),
                )
            })?;
        let implicit_issue = explicit_expiry
            .checked_sub_signed(self.token_lifetime.0)
            .ok_or_else(|| {
                ApiError::BadRequest(
                    "implicit token retention cutoff is outside the supported range".to_string(),
                )
            })?;

        Ok(TokenRetentionCutoffs {
            explicit_expiry,
            implicit_issue,
        })
    }
}

/// Builder that validates every retention setting before creating purge state.
#[derive(Debug, Default)]
pub struct TokenRetentionSettingsBuilder {
    retention_days: Option<i64>,
    token_lifetime_hours: Option<i64>,
    batch_size: Option<usize>,
}

impl TokenRetentionSettingsBuilder {
    pub fn retention_days(mut self, retention_days: i64) -> Self {
        self.retention_days = Some(retention_days);
        self
    }

    pub fn token_lifetime_hours(mut self, token_lifetime_hours: i64) -> Self {
        self.token_lifetime_hours = Some(token_lifetime_hours);
        self
    }

    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    pub fn build(self) -> Result<TokenRetentionSettings, ApiError> {
        let retention_days = self
            .retention_days
            .ok_or_else(|| ApiError::BadRequest("token_retention_days is required".to_string()))?;
        let token_lifetime_hours = self
            .token_lifetime_hours
            .ok_or_else(|| ApiError::BadRequest("token_lifetime_hours is required".to_string()))?;
        let batch_size = self.batch_size.ok_or_else(|| {
            ApiError::BadRequest("token_retention_purge_batch_size is required".to_string())
        })?;

        Ok(TokenRetentionSettings {
            retention_period: TokenRetentionPeriod::from_days(retention_days)?,
            token_lifetime: TokenLifetime::from_hours(token_lifetime_hours)?,
            batch_size: TokenRetentionBatchSize::new(batch_size)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use rstest::rstest;

    use super::*;

    fn settings() -> TokenRetentionSettings {
        TokenRetentionSettings::builder()
            .retention_days(30)
            .token_lifetime_hours(24)
            .batch_size(1_000)
            .build()
            .unwrap()
    }

    #[rstest]
    #[case(-1)]
    #[case(0)]
    fn retention_period_rejects_non_positive_days(#[case] days: i64) {
        let error = TokenRetentionPeriod::from_days(days).unwrap_err();

        assert_eq!(
            error.to_string(),
            "token_retention_days must be greater than 0"
        );
    }

    #[test]
    fn retention_period_rejects_values_outside_the_supported_range() {
        let error = TokenRetentionPeriod::from_days(i64::MAX).unwrap_err();

        assert_eq!(
            error.to_string(),
            "token_retention_days is outside the supported range"
        );
    }

    #[rstest]
    #[case(-1)]
    #[case(0)]
    fn token_lifetime_rejects_non_positive_hours(#[case] hours: i64) {
        let error = TokenLifetime::from_hours(hours).unwrap_err();

        assert_eq!(
            error.to_string(),
            "token_lifetime_hours must be greater than 0"
        );
    }

    #[test]
    fn token_lifetime_rejects_values_above_the_supported_range() {
        let error = TokenLifetime::from_hours(i64::from(i32::MAX) + 1).unwrap_err();

        assert_eq!(
            error.to_string(),
            "token_lifetime_hours must not exceed 2147483647"
        );
    }

    #[test]
    fn token_issuance_policy_requires_maximum_to_cover_default() {
        let error = TokenIssuancePolicy::from_hours(48, 24).unwrap_err();

        assert_eq!(
            error.to_string(),
            "token_lifetime_hours must not exceed max_token_lifetime_hours"
        );
    }

    #[test]
    fn token_issuance_policy_materializes_the_default_expiry() {
        let issued_at = NaiveDate::from_ymd_opt(2026, 8, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let policy = TokenIssuancePolicy::from_hours(24, 168).unwrap();

        assert_eq!(
            policy.resolve_expiry(issued_at, None).unwrap(),
            issued_at + Duration::hours(24)
        );
    }

    #[rstest]
    #[case::equal(Duration::zero())]
    #[case::below_database_precision(Duration::nanoseconds(1))]
    #[case::past(Duration::hours(-1))]
    fn token_issuance_policy_rejects_non_future_expiry(#[case] offset: Duration) {
        let issued_at = NaiveDate::from_ymd_opt(2026, 8, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let policy = TokenIssuancePolicy::from_hours(24, 168).unwrap();

        let error = policy
            .resolve_expiry(issued_at, Some(issued_at + offset))
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "expires_at must be later than the token issuance time"
        );
    }

    #[test]
    fn token_issuance_policy_rejects_expiry_beyond_maximum() {
        let issued_at = NaiveDate::from_ymd_opt(2026, 8, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let policy = TokenIssuancePolicy::from_hours(24, 168).unwrap();

        let error = policy
            .resolve_expiry(issued_at, Some(issued_at + Duration::hours(169)))
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "expires_at must not exceed the configured maximum token lifetime of 168 hours"
        );
    }

    #[test]
    fn token_issuance_policy_accepts_expiry_at_maximum() {
        let issued_at = NaiveDate::from_ymd_opt(2026, 8, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let policy = TokenIssuancePolicy::from_hours(24, 168).unwrap();
        let maximum = issued_at + Duration::hours(168);

        assert_eq!(
            policy.resolve_expiry(issued_at, Some(maximum)).unwrap(),
            maximum
        );
    }

    #[rstest]
    #[case(0)]
    #[case(MIN_TOKEN_RETENTION_PURGE_BATCH_SIZE - 1)]
    fn retention_batch_size_rejects_values_below_the_minimum(#[case] batch_size: usize) {
        let error = TokenRetentionBatchSize::new(batch_size).unwrap_err();

        assert_eq!(
            error.to_string(),
            "token_retention_purge_batch_size must be at least 10"
        );
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn retention_batch_size_rejects_values_above_the_database_limit() {
        let error = TokenRetentionBatchSize::new(usize::MAX).unwrap_err();

        assert_eq!(
            error.to_string(),
            "token_retention_purge_batch_size is too large"
        );
    }

    #[test]
    fn retention_settings_compute_both_cutoffs_from_typed_durations() {
        let now = NaiveDate::from_ymd_opt(2026, 7, 25)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let settings = settings();
        let cutoffs = settings.cutoffs(now).unwrap();

        assert_eq!(cutoffs.explicit_expiry(), now - Duration::days(30));
        assert_eq!(
            cutoffs.implicit_issue(),
            now - Duration::days(30) - Duration::hours(24)
        );
    }
}
