use chrono::{DateTime, Utc};
use hubuum_storage_core::{
    RefreshCredential, SessionClientId, SessionCredentials, StorageSessionIssued,
    StorageTokenFormat, StorageTokenHashKeyId,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    errors::ApiError,
    models::{LoginUser, Token},
    utilities::auth::generate_token,
};

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SessionLoginRequest {
    pub client_id: String,
    pub identity_scope: Option<String>,
    pub name: String,
    pub password: String,
}
impl SessionLoginRequest {
    pub fn into_login(self) -> Result<(SessionClientId, LoginUser), ApiError> {
        let client = SessionClientId::try_new(self.client_id)
            .map_err(|e| ApiError::BadRequest(e.to_string()))?;
        Ok((
            client,
            LoginUser {
                identity_scope: self.identity_scope,
                name: self.name,
                password: self.password,
            },
        ))
    }
}

#[derive(Clone, Deserialize, Serialize, ToSchema)]
#[serde(try_from = "String", into = "String")]
#[schema(value_type = String)]
pub struct RefreshToken(String);
impl fmt::Debug for RefreshToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RefreshToken(<redacted>)")
    }
}
impl TryFrom<String> for RefreshToken {
    type Error = ApiError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        let mut parts = value.split('.');
        if parts.next() != Some("hbr1")
            || StorageTokenHashKeyId::try_new(parts.next().unwrap_or_default()).is_err()
            || parts
                .next()
                .is_none_or(|s| !super::token::is_valid_token_secret(s))
            || parts.next().is_some()
        {
            return Err(authentication_failed());
        }
        Ok(Self(value))
    }
}
impl From<RefreshToken> for String {
    fn from(value: RefreshToken) -> Self {
        value.0
    }
}
impl RefreshToken {
    pub fn generate() -> Self {
        Self(generate_token().0.replacen("hbt1.", "hbr1.", 1))
    }
    pub fn credential(&self) -> Result<RefreshCredential, ApiError> {
        let key_id = StorageTokenHashKeyId::try_new(self.0.split('.').nth(1).unwrap_or_default())
            .map_err(|_| authentication_failed())?;
        let ring = crate::config::token_hash_key_ring().map_err(|_| {
            ApiError::InternalServerError("Authentication configuration unavailable".into())
        })?;
        let key = ring.key_bytes(&key_id).ok_or_else(authentication_failed)?;
        Ok(RefreshCredential::new(super::token::digest_with_key(
            &self.0,
            key,
            StorageTokenFormat::Version1,
            key_id,
        )?))
    }
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SessionRefreshRequest {
    pub client_id: String,
    pub refresh_token: RefreshToken,
}

#[derive(Serialize, ToSchema)]
pub struct SessionResponse {
    pub session_id: Uuid,
    pub token_type: String,
    pub access_token: String,
    pub access_expires_at: DateTime<Utc>,
    pub refresh_token: RefreshToken,
    pub refresh_expires_at: DateTime<Utc>,
    pub absolute_expires_at: DateTime<Utc>,
}
impl fmt::Debug for SessionResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionResponse")
            .field("session_id", &self.session_id)
            .finish_non_exhaustive()
    }
}
impl SessionResponse {
    pub fn new(access: Token, refresh: RefreshToken, metadata: StorageSessionIssued) -> Self {
        Self {
            session_id: metadata.session_id().value(),
            token_type: "Bearer".into(),
            access_token: access.0,
            access_expires_at: metadata.access_expires_at(),
            refresh_token: refresh,
            refresh_expires_at: metadata.refresh_expires_at(),
            absolute_expires_at: metadata.absolute_expires_at(),
        }
    }
}
pub fn session_credentials(
    access: &Token,
    refresh: &RefreshToken,
) -> Result<SessionCredentials, ApiError> {
    SessionCredentials::try_new(
        access.storage_digest()?,
        refresh.credential()?.digest().clone(),
    )
    .map_err(|_| authentication_failed())
}
pub fn authentication_failed() -> ApiError {
    ApiError::Unauthorized("Authentication failed".into())
}

impl fmt::Debug for SessionLoginRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SessionLoginRequest(<redacted>)")
    }
}
