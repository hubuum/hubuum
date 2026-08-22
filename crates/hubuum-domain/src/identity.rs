use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[cfg(feature = "openapi")]
use utoipa::ToSchema;

/// Canonical identity scope for locally managed principals.
pub const LOCAL_IDENTITY_SCOPE: &str = "local";

/// Provider marker for locally managed identity records.
pub const LOCAL_PROVIDER_KIND: &str = "local";

/// Provider marker for LDAP-managed identity records.
pub const LDAP_PROVIDER_KIND: &str = "ldap";

/// Membership-source marker for application-managed group assignments.
pub const MANUAL_MEMBERSHIP_SOURCE: &str = "manual";

/// Membership-source marker for directory-managed group assignments.
pub const EXTERNAL_MEMBERSHIP_SOURCE: &str = "external";

/// Closed set of principal kinds understood by every Hubuum layer.
///
/// Adapters must parse persisted values into this type before returning a
/// principal so database corruption cannot cross the storage boundary as an
/// arbitrary string.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    Human,
    ServiceAccount,
}

impl PrincipalKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Human => "human",
            Self::ServiceAccount => "service_account",
        }
    }

    #[must_use]
    pub const fn is_human(self) -> bool {
        matches!(self, Self::Human)
    }

    #[must_use]
    pub const fn is_service_account(self) -> bool {
        matches!(self, Self::ServiceAccount)
    }
}

impl fmt::Display for PrincipalKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for PrincipalKind {
    type Err = PrincipalKindParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "human" => Ok(Self::Human),
            "service_account" => Ok(Self::ServiceAccount),
            other => Err(PrincipalKindParseError(other.to_string())),
        }
    }
}

/// Failure to parse a persisted principal kind.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrincipalKindParseError(String);

impl PrincipalKindParseError {
    #[must_use]
    pub fn value(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PrincipalKindParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Unknown principal kind '{}'", self.0)
    }
}

impl std::error::Error for PrincipalKindParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn principal_kind_round_trips_its_persisted_name() {
        for kind in [PrincipalKind::Human, PrincipalKind::ServiceAccount] {
            assert_eq!(kind.as_str().parse(), Ok(kind));
        }
    }

    #[test]
    fn principal_kind_rejects_unknown_persisted_values() {
        assert_eq!(
            "robot".parse::<PrincipalKind>().unwrap_err().value(),
            "robot"
        );
    }
}
