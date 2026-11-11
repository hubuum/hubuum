#![allow(async_fn_in_trait)]

mod credential;
pub use credential::{BearerCredential, CredentialError, VersionedCredential, keyed_digest};

pub use hubuum_domain::IdentityScopeName;
use hubuum_domain::IdentityScopeNameError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalUserProfile {
    pub subject: String,
    pub name: String,
    pub proper_name: Option<String>,
    pub email: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalGroup {
    pub key: String,
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedExternalUser {
    pub profile: ExternalUserProfile,
    pub groups: Vec<ExternalGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ExternalUsername(String);

impl ExternalUsername {
    fn new(value: impl Into<String>) -> Result<Self, ExternalIdentityValueError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ExternalIdentityValueError::EmptyUsername);
        }
        Ok(Self(value))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ExternalSubject(String);

impl ExternalSubject {
    fn new(value: impl Into<String>) -> Result<Self, ExternalIdentityValueError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ExternalIdentityValueError::EmptySubject);
        }
        Ok(Self(value))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum ExternalIdentityValueError {
    #[error("external username must not be empty")]
    EmptyUsername,
    #[error("external subject must not be empty")]
    EmptySubject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalUserRefreshRequest {
    username: ExternalUsername,
    expected_subject: ExternalSubject,
}

impl ExternalUserRefreshRequest {
    pub fn new(
        username: impl Into<String>,
        expected_subject: impl Into<String>,
    ) -> Result<Self, ExternalIdentityValueError> {
        Ok(Self {
            username: ExternalUsername::new(username)?,
            expected_subject: ExternalSubject::new(expected_subject)?,
        })
    }

    pub fn username(&self) -> &str {
        self.username.as_str()
    }

    pub fn expected_subject(&self) -> &str {
        self.expected_subject.as_str()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum AuthProviderError {
    #[error("authentication failed")]
    AuthenticationFailed,
    #[error("provider unavailable: {0}")]
    Unavailable(String),
    #[error("provider configuration error: {0}")]
    Config(String),
    #[error("provider protocol error: {0}")]
    Protocol(String),
}

impl From<IdentityScopeNameError> for AuthProviderError {
    fn from(error: IdentityScopeNameError) -> Self {
        Self::Config(error.to_string())
    }
}

pub trait ExternalIdentityProvider: Send + Sync {
    fn scope_name(&self) -> &IdentityScopeName;

    async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> Result<AuthenticatedExternalUser, AuthProviderError>;

    async fn refresh_user(
        &self,
        request: &ExternalUserRefreshRequest,
    ) -> Result<AuthenticatedExternalUser, AuthProviderError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_name_validation_maps_to_configuration_errors() {
        let error = AuthProviderError::from(IdentityScopeName::new("").unwrap_err());

        assert!(matches!(error, AuthProviderError::Config(message)
            if message == "identity scope name must not be empty"));
    }

    #[test]
    fn refresh_request_rejects_an_empty_username() {
        let error = ExternalUserRefreshRequest::new("", "stable-subject").unwrap_err();

        assert_eq!(error, ExternalIdentityValueError::EmptyUsername);
    }

    #[test]
    fn refresh_request_rejects_an_empty_subject() {
        let error = ExternalUserRefreshRequest::new("alice", "").unwrap_err();

        assert_eq!(error, ExternalIdentityValueError::EmptySubject);
    }

    #[test]
    fn refresh_request_exposes_validated_identity_values() {
        let request = ExternalUserRefreshRequest::new("alice", "stable-subject").unwrap();

        assert_eq!(request.username(), "alice");
        assert_eq!(request.expected_subject(), "stable-subject");
    }
}
