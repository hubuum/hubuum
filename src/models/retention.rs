use chrono::{Duration, NaiveDateTime};

/// A positive, representable duration used to place an artifact expiry in the future.
///
/// Construction validates the configured unit without binding the value to one wall-clock
/// sample. Expiry calculation remains checked because even a representable duration can exceed
/// `NaiveDateTime` when added to a timestamp near its upper bound.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FutureRetention {
    duration: Duration,
    field: &'static str,
}

impl FutureRetention {
    pub(crate) fn from_hours(value: i64, field: &'static str) -> Result<Self, String> {
        Self::new(value, field, Duration::try_hours)
    }

    pub(crate) fn from_minutes(value: i64, field: &'static str) -> Result<Self, String> {
        Self::new(value, field, Duration::try_minutes)
    }

    fn new(
        value: i64,
        field: &'static str,
        convert: fn(i64) -> Option<Duration>,
    ) -> Result<Self, String> {
        if value <= 0 {
            return Err(format!("{field} must be greater than 0"));
        }
        let duration = convert(value)
            .ok_or_else(|| format!("{field} is outside the supported duration range"))?;
        Ok(Self { duration, field })
    }

    pub(crate) fn expires_at(self, now: NaiveDateTime) -> Result<NaiveDateTime, String> {
        now.checked_add_signed(self.duration).ok_or_else(|| {
            format!(
                "{0} produces a timestamp outside the supported range",
                self.field
            )
        })
    }

    pub(crate) fn hours(self) -> i64 {
        self.duration.num_hours()
    }

    pub(crate) fn minutes(self) -> i64 {
        self.duration.num_minutes()
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::hours(0, FutureRetention::from_hours)]
    #[case::minutes(-1, FutureRetention::from_minutes)]
    fn rejects_non_positive_values(
        #[case] value: i64,
        #[case] constructor: fn(i64, &'static str) -> Result<FutureRetention, String>,
    ) {
        let error = constructor(value, "artifact_retention").unwrap_err();

        assert_eq!(error, "artifact_retention must be greater than 0");
    }

    #[rstest]
    #[case::hours(FutureRetention::from_hours)]
    #[case::minutes(FutureRetention::from_minutes)]
    fn rejects_unrepresentable_durations(
        #[case] constructor: fn(i64, &'static str) -> Result<FutureRetention, String>,
    ) {
        let error = constructor(i64::MAX, "artifact_retention").unwrap_err();

        assert_eq!(
            error,
            "artifact_retention is outside the supported duration range"
        );
    }

    #[test]
    fn computes_expiry_from_the_supplied_clock() {
        let now = NaiveDate::from_ymd_opt(2026, 8, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let retention = FutureRetention::from_hours(24, "artifact_retention").unwrap();

        assert_eq!(
            retention.expires_at(now).unwrap(),
            NaiveDate::from_ymd_opt(2026, 8, 2)
                .unwrap()
                .and_hms_opt(12, 0, 0)
                .unwrap()
        );
        assert_eq!(retention.hours(), 24);
        assert_eq!(retention.minutes(), 24 * 60);
    }

    #[test]
    fn rejects_an_expiry_outside_the_timestamp_range() {
        let retention = FutureRetention::from_hours(1, "artifact_retention").unwrap();

        let error = retention.expires_at(NaiveDateTime::MAX).unwrap_err();

        assert_eq!(
            error,
            "artifact_retention produces a timestamp outside the supported range"
        );
    }
}
