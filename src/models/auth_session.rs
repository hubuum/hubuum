use chrono::{DateTime, Utc};
use hubuum_auth_core::VersionedCredential;
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
    utilities::auth::generate_token_secret,
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

#[derive(Clone, Deserialize, Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct RefreshToken {
    value: VersionedCredential,
    key_id: StorageTokenHashKeyId,
}
impl utoipa::PartialSchema for RefreshToken {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        <String as utoipa::PartialSchema>::schema()
    }
}
impl ToSchema for RefreshToken {}
impl fmt::Debug for RefreshToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RefreshToken(<redacted>)")
    }
}
impl TryFrom<String> for RefreshToken {
    type Error = ApiError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        let value =
            VersionedCredential::parse_refresh(value).map_err(|_| authentication_failed())?;
        let key_id =
            StorageTokenHashKeyId::try_new(value.key_id()).map_err(|_| authentication_failed())?;
        Ok(Self { value, key_id })
    }
}
impl From<RefreshToken> for String {
    fn from(value: RefreshToken) -> Self {
        value.value.as_str().to_owned()
    }
}
impl RefreshToken {
    pub fn generate() -> Self {
        let ring = crate::config::token_hash_key_ring()
            .expect("token hash key-ring configuration must be valid at startup");
        let value = VersionedCredential::issue_refresh(
            ring.active_key_id().as_str(),
            &generate_token_secret(),
        )
        .expect("generated refresh credential must have valid syntax");
        Self {
            value,
            key_id: ring.active_key_id().clone(),
        }
    }
    pub fn credential(&self) -> Result<RefreshCredential, ApiError> {
        let ring = crate::config::token_hash_key_ring().map_err(|_| {
            ApiError::InternalServerError("Authentication configuration unavailable".into())
        })?;
        let key = ring
            .key_bytes(&self.key_id)
            .ok_or_else(authentication_failed)?;
        Ok(RefreshCredential::new(super::token::digest_with_key(
            self.value.as_str(),
            key,
            StorageTokenFormat::Version1,
            self.key_id.clone(),
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
            access_token: access.get_token(),
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
