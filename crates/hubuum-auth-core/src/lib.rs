#![allow(async_fn_in_trait)]

use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IdentityScopeName(String);

impl IdentityScopeName {
    pub fn new(value: impl Into<String>) -> Result<Self, AuthProviderError> {
        let value = value.into();
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(AuthProviderError::Config(
                "identity scope name must not be empty".to_string(),
            ));
        }
        if trimmed != value {
            return Err(AuthProviderError::Config(
                "identity scope name must not contain leading or trailing whitespace".to_string(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IdentityScopeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

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
