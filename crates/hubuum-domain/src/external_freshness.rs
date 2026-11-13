use chrono::{DateTime, Duration, Utc};

use crate::{ExternalAuthenticationError, ProviderMembership};

/// Eligibility and membership age, independent of Hubuum token rotation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalFreshness {
    Current,
    RefreshRequired,
    Expired,
}

/// Browser-provider freshness bounds. LDAP retains its existing configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExternalFreshnessPolicy {
    refresh_interval: Duration,
    max_stale: Duration,
}

impl ExternalFreshnessPolicy {
    pub fn new(
        refresh_interval_seconds: u32,
        max_stale_seconds: u32,
    ) -> Result<Self, ExternalAuthenticationError> {
        if refresh_interval_seconds == 0
            || refresh_interval_seconds > 300
            || max_stale_seconds < refresh_interval_seconds
            || max_stale_seconds > 900
        {
            return Err(ExternalAuthenticationError::InvalidFreshnessPolicy);
        }
        Ok(Self {
            refresh_interval: Duration::seconds(i64::from(refresh_interval_seconds)),
            max_stale: Duration::seconds(i64::from(max_stale_seconds)),
        })
    }

    /// Only a successful same-subject upstream eligibility/membership check may
    /// supply `last_verified_at`. Session issuance/rotation is not such a check.
    /// Future timestamps fail closed, including after wall-clock rollback.
    pub fn evaluate(
        self,
        last_verified_at: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> ExternalFreshness {
        let Some(verified) = last_verified_at else {
            return ExternalFreshness::Expired;
        };
        let age = now - verified;
        if age < Duration::zero() || age >= self.max_stale {
            ExternalFreshness::Expired
        } else if age >= self.refresh_interval {
            ExternalFreshness::RefreshRequired
        } else {
            ExternalFreshness::Current
        }
    }

    /// Temporary outage tolerance applies only to existing authority. Callers
    /// must revoke on definitive rejection, invalid grants or subject mismatch;
    /// this method must never admit a new login or a credential approval.
    pub fn allows_existing_authority_during_outage(
        self,
        last_verified_at: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> bool {
        self.evaluate(last_verified_at, now) != ExternalFreshness::Expired
    }
}

impl Default for ExternalFreshnessPolicy {
    fn default() -> Self {
        Self {
            refresh_interval: Duration::seconds(300),
            max_stale: Duration::seconds(900),
        }
    }
}

#[cfg(test)]
mod tests;

impl TryFrom<&ProviderMembership> for ExternalFreshnessPolicy {
    type Error = ExternalAuthenticationError;
    fn try_from(membership: &ProviderMembership) -> Result<Self, Self::Error> {
        let (refresh, stale) = match membership {
            ProviderMembership::LocalOnly {
                refresh_interval_seconds,
                max_stale_seconds,
            }
            | ProviderMembership::ProviderGroups {
                refresh_interval_seconds,
                max_stale_seconds,
                ..
            } => (*refresh_interval_seconds, *max_stale_seconds),
        };
        Self::new(refresh, stale)
    }
}
