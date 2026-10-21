//! Interactive sessions are distinct from durable bearer credentials.
use chrono::{DateTime, Duration, Utc};
use hubuum_domain::{PrincipalId, TokenId};
use hubuum_events_core::EventContext;
use uuid::Uuid;

use crate::{
    StorageLogin, StorageTokenDigest, StorageTokenFormat, StorageTokenHashAlgorithm,
    StorageValidationError,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionClientId(String);
impl SessionClientId {
    pub fn try_new(value: impl Into<String>) -> Result<Self, StorageValidationError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-._:".contains(&b))
        {
            return Err(StorageValidationError::invalid(
                "client_id must contain 1-128 ASCII letters, digits, '-', '.', '_' or ':'",
            ));
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuthSessionId(Uuid);
impl AuthSessionId {
    pub fn new(value: Uuid) -> Self {
        Self(value)
    }
    pub fn value(self) -> Uuid {
        self.0
    }
}

/// Policy pinned at login. Refresh never changes the original lifetimes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionPolicy {
    access_seconds: i64,
    idle_seconds: i64,
    absolute_seconds: i64,
    retention_seconds: i64,
}
impl SessionPolicy {
    pub fn try_new(
        access_seconds: i64,
        idle_seconds: i64,
        absolute_seconds: i64,
        retention_seconds: i64,
    ) -> Result<Self, StorageValidationError> {
        if access_seconds < 1
            || access_seconds > idle_seconds
            || idle_seconds > absolute_seconds
            || absolute_seconds > 31_536_000
            || !(1..=31_536_000).contains(&retention_seconds)
        {
            return Err(StorageValidationError::invalid(
                "session lifetimes require 0 < access <= idle <= absolute <= 31536000 seconds and retention in 1..=31536000",
            ));
        }
        Ok(Self {
            access_seconds,
            idle_seconds,
            absolute_seconds,
            retention_seconds,
        })
    }
    pub fn access_seconds(self) -> i64 {
        self.access_seconds
    }
    pub fn idle_seconds(self) -> i64 {
        self.idle_seconds
    }
    pub fn absolute_seconds(self) -> i64 {
        self.absolute_seconds
    }
    pub fn retention_seconds(self) -> i64 {
        self.retention_seconds
    }
    pub fn deadlines(
        self,
        now: DateTime<Utc>,
        absolute: DateTime<Utc>,
    ) -> Result<(DateTime<Utc>, DateTime<Utc>), StorageValidationError> {
        let advance = |seconds| {
            now.checked_add_signed(Duration::seconds(seconds))
                .ok_or_else(|| StorageValidationError::invalid("session expiry is out of range"))
        };
        Ok((
            advance(self.access_seconds)?.min(absolute),
            advance(self.idle_seconds)?.min(absolute),
        ))
    }
}

#[derive(Clone, Debug)]
pub struct SessionCredentials {
    access: StorageTokenDigest,
    refresh: StorageTokenDigest,
}
impl SessionCredentials {
    pub fn try_new(
        access: StorageTokenDigest,
        refresh: StorageTokenDigest,
    ) -> Result<Self, StorageValidationError> {
        if access == refresh
            || [&access, &refresh].into_iter().any(|digest| {
                digest.format() != StorageTokenFormat::Version1
                    || digest.algorithm() != StorageTokenHashAlgorithm::HmacSha256V1
                    || digest.key_id().is_none()
            })
        {
            return Err(StorageValidationError::invalid(
                "Session credentials require distinct versioned keyed digests",
            ));
        }
        Ok(Self { access, refresh })
    }
    pub fn access(&self) -> &StorageTokenDigest {
        &self.access
    }
    pub fn refresh(&self) -> &StorageTokenDigest {
        &self.refresh
    }
}

#[derive(Clone, Debug)]
pub struct StorageSessionCreate {
    login: StorageLogin,
    client: SessionClientId,
    credentials: SessionCredentials,
    policy: SessionPolicy,
    context: EventContext,
}
impl StorageSessionCreate {
    pub fn new(
        login: StorageLogin,
        client: SessionClientId,
        credentials: SessionCredentials,
        policy: SessionPolicy,
        context: EventContext,
    ) -> Self {
        Self {
            login,
            client,
            credentials,
            policy,
            context,
        }
    }
    pub fn login(&self) -> &StorageLogin {
        &self.login
    }
    pub fn client(&self) -> &SessionClientId {
        &self.client
    }
    pub fn credentials(&self) -> &SessionCredentials {
        &self.credentials
    }
    pub fn policy(&self) -> SessionPolicy {
        self.policy
    }
    pub fn context(&self) -> &EventContext {
        &self.context
    }
}

/// Refresh digests can only be used in the session capability, never bearer lookup.
#[derive(Clone, Debug)]
pub struct RefreshCredential(StorageTokenDigest);
impl RefreshCredential {
    pub fn new(digest: StorageTokenDigest) -> Self {
        Self(digest)
    }
    pub fn digest(&self) -> &StorageTokenDigest {
        &self.0
    }
}

#[derive(Clone, Debug)]
pub struct StorageSessionRotate {
    presented: RefreshCredential,
    client: SessionClientId,
    credentials: SessionCredentials,
    context: EventContext,
}
impl StorageSessionRotate {
    pub fn new(
        presented: RefreshCredential,
        client: SessionClientId,
        credentials: SessionCredentials,
        context: EventContext,
    ) -> Self {
        Self {
            presented,
            client,
            credentials,
            context,
        }
    }
    pub fn presented(&self) -> &RefreshCredential {
        &self.presented
    }
    pub fn client(&self) -> &SessionClientId {
        &self.client
    }
    pub fn credentials(&self) -> &SessionCredentials {
        &self.credentials
    }
    pub fn context(&self) -> &EventContext {
        &self.context
    }
}

#[derive(Clone, Debug)]
pub struct StorageSessionIssued {
    session_id: AuthSessionId,
    principal_id: PrincipalId,
    access_id: TokenId,
    issued_at: DateTime<Utc>,
    access_expires_at: DateTime<Utc>,
    refresh_expires_at: DateTime<Utc>,
    absolute_expires_at: DateTime<Utc>,
}
impl StorageSessionIssued {
    pub fn new(
        session_id: AuthSessionId,
        principal_id: PrincipalId,
        access_id: TokenId,
        issued_at: DateTime<Utc>,
        access_expires_at: DateTime<Utc>,
        refresh_expires_at: DateTime<Utc>,
        absolute_expires_at: DateTime<Utc>,
    ) -> Self {
        Self {
            session_id,
            principal_id,
            access_id,
            issued_at,
            access_expires_at,
            refresh_expires_at,
            absolute_expires_at,
        }
    }
    pub fn session_id(&self) -> AuthSessionId {
        self.session_id
    }
    pub fn principal_id(&self) -> PrincipalId {
        self.principal_id
    }
    pub fn access_id(&self) -> TokenId {
        self.access_id
    }
    pub fn issued_at(&self) -> DateTime<Utc> {
        self.issued_at
    }
    pub fn access_expires_at(&self) -> DateTime<Utc> {
        self.access_expires_at
    }
    pub fn refresh_expires_at(&self) -> DateTime<Utc> {
        self.refresh_expires_at
    }
    pub fn absolute_expires_at(&self) -> DateTime<Utc> {
        self.absolute_expires_at
    }
}

/// Rejection is a committed outcome: replay revocation must not be rolled back.
#[derive(Clone, Debug)]
pub enum StorageSessionRotation {
    Issued(StorageSessionIssued),
    Rejected,
    Replayed,
}

#[derive(Clone, Copy, Debug)]
pub enum SessionRevocation {
    Current(TokenId),
    All(PrincipalId),
}
