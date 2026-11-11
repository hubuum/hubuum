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

/// Nonempty identity namespace whose exact spelling is preserved across
/// deployment configuration, runtime settings, and persisted identities.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct IdentityScopeName(String);

impl IdentityScopeName {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentityScopeNameError> {
        let value = value.into();
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(IdentityScopeNameError::Empty);
        }
        if trimmed != value {
            return Err(IdentityScopeNameError::SurroundingWhitespace);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for IdentityScopeName {
    type Error = IdentityScopeNameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl fmt::Display for IdentityScopeName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityScopeNameError {
    Empty,
    SurroundingWhitespace,
}

impl fmt::Display for IdentityScopeNameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "identity scope name must not be empty",
            Self::SurroundingWhitespace => {
                "identity scope name must not contain leading or trailing whitespace"
            }
        })
    }
}

impl std::error::Error for IdentityScopeNameError {}

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
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::local("local".to_string())]
    #[case::dotted("corp.example.org".to_string())]
    #[case::unicode_with_space("Bedrift Øst".to_string())]
    #[case::long("x".repeat(129))]
    fn scope_name_preserves_valid_names(#[case] value: String) {
        let scope = IdentityScopeName::new(value.clone()).unwrap();
        let serialized = serde_json::to_string(&scope).unwrap();
        let reloaded: IdentityScopeName = serde_json::from_str(&serialized).unwrap();

        assert_eq!(reloaded.as_str(), value);
    }

    #[rstest]
    #[case::empty("", IdentityScopeNameError::Empty)]
    #[case::whitespace(" \t", IdentityScopeNameError::Empty)]
    #[case::leading(" ldap", IdentityScopeNameError::SurroundingWhitespace)]
    #[case::trailing("ldap ", IdentityScopeNameError::SurroundingWhitespace)]
    #[case::unicode("\u{2003}ldap", IdentityScopeNameError::SurroundingWhitespace)]
    fn scope_name_rejects_invalid_names(
        #[case] value: &str,
        #[case] expected: IdentityScopeNameError,
    ) {
        assert_eq!(IdentityScopeName::new(value), Err(expected));
    }

    #[rstest]
    #[case::empty("")]
    #[case::whitespace(" \t")]
    #[case::leading(" ldap")]
    #[case::trailing("ldap ")]
    fn deserialization_rejects_invalid_scope_names(#[case] value: &str) {
        let serialized = serde_json::to_string(value).unwrap();

        assert!(serde_json::from_str::<IdentityScopeName>(&serialized).is_err());
    }

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
