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
        subject: &str,
    ) -> Result<AuthenticatedExternalUser, AuthProviderError>;
}
