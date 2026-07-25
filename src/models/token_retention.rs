use chrono::{Duration, NaiveDateTime};

use crate::errors::ApiError;

pub const MIN_TOKEN_RETENTION_PURGE_BATCH_SIZE: usize = 10;

/// Validated post-expiry period for retaining token metadata.
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

/// Validated lifetime applied to tokens that omit an explicit expiry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenLifetime(Duration);

impl TokenLifetime {
    pub fn from_hours(hours: i64) -> Result<Self, ApiError> {
        if hours <= 0 {
            return Err(ApiError::BadRequest(
                "token_lifetime_hours must be greater than 0".to_string(),
            ));
        }
        if hours > i64::from(i32::MAX) {
            return Err(ApiError::BadRequest(format!(
                "token_lifetime_hours must not exceed {}",
                i32::MAX
            )));
        }
        Duration::try_hours(hours).map(Self).ok_or_else(|| {
            ApiError::BadRequest("token_lifetime_hours is outside the supported range".to_string())
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
