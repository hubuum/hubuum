//! Destination authority for requests carrying server-owned credentials.
use std::fmt;

use crate::validate_outbound_url;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CredentialOrigin(String);

impl CredentialOrigin {
    pub fn new(origin: &str) -> Result<Self, CredentialDestinationError> {
        let parsed =
            validate_outbound_url(origin).map_err(|_| CredentialDestinationError::InvalidOrigin)?;
        if parsed.url.path() != "/"
            || parsed.url.query().is_some()
            || parsed.url.fragment().is_some()
        {
            return Err(CredentialDestinationError::InvalidOrigin);
        }
        Ok(Self(parsed.url.origin().ascii_serialization()))
    }
}

/// Proof that this exact URL belongs to one of the credential's permitted
/// HTTPS origins. Destination changes require a new authorization decision.
#[derive(Clone)]
pub struct AuthorizedDestination(String);

impl AuthorizedDestination {
    pub fn authorize(
        url: &str,
        origins: &[CredentialOrigin],
    ) -> Result<Self, CredentialDestinationError> {
        let parsed = validate_outbound_url(url)
            .map_err(|_| CredentialDestinationError::InvalidDestination)?;
        let origin = parsed.url.origin().ascii_serialization();
        if !origins.iter().any(|allowed| allowed.0 == origin) {
            return Err(CredentialDestinationError::DestinationDenied);
        }
        Ok(Self(parsed.url.to_string()))
    }

    pub fn url(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for AuthorizedDestination {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizedDestination")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialDestinationError {
    InvalidOrigin,
    InvalidDestination,
    DestinationDenied,
}

impl fmt::Display for CredentialDestinationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidOrigin => "credential origins must be HTTPS origins without paths, queries, fragments, or credentials",
            Self::InvalidDestination => "credential destination must be a valid HTTPS URL without embedded credentials",
            Self::DestinationDenied => "credential use is not permitted for this destination origin",
        })
    }
}
impl std::error::Error for CredentialDestinationError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_https_port_is_the_same_origin() {
        let origin = CredentialOrigin::new("https://example.com:443").unwrap();
        assert!(
            AuthorizedDestination::authorize("https://example.com/path?q=value", &[origin]).is_ok()
        );
    }

    #[test]
    fn different_origin_cannot_receive_credentials() {
        let origin = CredentialOrigin::new("https://example.com").unwrap();
        assert_eq!(
            AuthorizedDestination::authorize("https://example.com.attacker.invalid/", &[origin])
                .unwrap_err(),
            CredentialDestinationError::DestinationDenied
        );
    }

    #[test]
    fn different_port_cannot_receive_credentials() {
        let origin = CredentialOrigin::new("https://example.com").unwrap();
        assert!(AuthorizedDestination::authorize("https://example.com:8443/", &[origin]).is_err());
    }

    #[test]
    fn origin_cannot_contain_a_path() {
        assert!(CredentialOrigin::new("https://example.com/token").is_err());
    }
}
