//! Bounded, best-effort credential source observations, independent of revisions.
use crate::{AuthSessionId, StorageValidationError};
use chrono::{DateTime, Utc};
use hubuum_domain::{PrincipalId, TokenId};
use std::net::IpAddr;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ObservedSource {
    ConnectionPeer(IpAddr),
    TrustedForwarded(IpAddr),
    Unavailable,
}
impl ObservedSource {
    pub fn address(self) -> Option<IpAddr> {
        match self {
            Self::ConnectionPeer(ip) | Self::TrustedForwarded(ip) => Some(ip),
            Self::Unavailable => None,
        }
    }
    pub fn method(self) -> &'static str {
        match self {
            Self::ConnectionPeer(_) => "connection_peer",
            Self::TrustedForwarded(_) => "trusted_forwarded",
            Self::Unavailable => "unavailable",
        }
    }
    pub fn from_persisted(
        address: Option<&str>,
        method: &str,
    ) -> Result<Self, StorageValidationError> {
        let parsed = address
            .map(str::parse::<IpAddr>)
            .transpose()
            .map_err(|_| StorageValidationError::invalid("invalid persisted source address"))?;
        match (parsed, method) {
            (Some(ip), "connection_peer") => Ok(Self::ConnectionPeer(ip)),
            (Some(ip), "trusted_forwarded") => Ok(Self::TrustedForwarded(ip)),
            (None, "unavailable") => Ok(Self::Unavailable),
            _ => Err(StorageValidationError::invalid("invalid source provenance")),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CredentialIssuanceMethod {
    Login,
    Creation,
    Renewal,
    Refresh,
}
impl CredentialIssuanceMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Login => "login",
            Self::Creation => "creation",
            Self::Renewal => "renewal",
            Self::Refresh => "refresh",
        }
    }
    pub fn from_persisted(value: &str) -> Result<Self, StorageValidationError> {
        match value {
            "login" => Ok(Self::Login),
            "creation" => Ok(Self::Creation),
            "renewal" => Ok(Self::Renewal),
            "refresh" => Ok(Self::Refresh),
            _ => Err(StorageValidationError::invalid("invalid issuance method")),
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub struct CredentialActivityPolicy {
    enabled: bool,
    retention_days: i64,
    source_cap: usize,
}
impl CredentialActivityPolicy {
    pub fn try_new(
        enabled: bool,
        retention_days: i64,
        source_cap: usize,
    ) -> Result<Self, StorageValidationError> {
        if !(1..=365).contains(&retention_days) || !(1..=256).contains(&source_cap) {
            return Err(StorageValidationError::invalid(
                "credential activity retention must be 1-365 days and source cap 1-256",
            ));
        }
        Ok(Self {
            enabled,
            retention_days,
            source_cap,
        })
    }
    pub fn enabled(self) -> bool {
        self.enabled
    }
    pub fn retention_days(self) -> i64 {
        self.retention_days
    }
    pub fn source_cap(self) -> usize {
        self.source_cap
    }
}
#[derive(Clone, Debug)]
pub struct StorageCredentialCreation {
    issuer: Option<PrincipalId>,
    method: CredentialIssuanceMethod,
    source: ObservedSource,
    issued_at: DateTime<Utc>,
}
impl StorageCredentialCreation {
    pub fn new(
        issuer: Option<PrincipalId>,
        method: CredentialIssuanceMethod,
        source: ObservedSource,
        issued_at: DateTime<Utc>,
    ) -> Self {
        Self {
            issuer,
            method,
            source,
            issued_at,
        }
    }
    pub fn issuer(&self) -> Option<PrincipalId> {
        self.issuer
    }
    pub fn method(&self) -> CredentialIssuanceMethod {
        self.method
    }
    pub fn source(&self) -> ObservedSource {
        self.source
    }
    pub fn issued_at(&self) -> DateTime<Utc> {
        self.issued_at
    }
}
#[derive(Clone, Debug)]
pub struct StorageCredentialSource {
    source: ObservedSource,
    first_seen: DateTime<Utc>,
    last_seen: DateTime<Utc>,
}
impl StorageCredentialSource {
    pub fn new(
        source: ObservedSource,
        first_seen: DateTime<Utc>,
        last_seen: DateTime<Utc>,
    ) -> Self {
        Self {
            source,
            first_seen,
            last_seen,
        }
    }
    pub fn source(&self) -> ObservedSource {
        self.source
    }
    pub fn first_seen(&self) -> DateTime<Utc> {
        self.first_seen
    }
    pub fn last_seen(&self) -> DateTime<Utc> {
        self.last_seen
    }
}
#[derive(Clone, Copy, Debug)]
pub struct StorageActivityQuery {
    principal_id: PrincipalId,
    token_id: TokenId,
    offset: usize,
    limit: usize,
    policy: CredentialActivityPolicy,
}
impl StorageActivityQuery {
    pub fn try_new(
        principal_id: PrincipalId,
        token_id: TokenId,
        offset: usize,
        limit: usize,
        policy: CredentialActivityPolicy,
    ) -> Result<Self, StorageValidationError> {
        if offset > 256 || !(1..=100).contains(&limit) {
            return Err(StorageValidationError::invalid(
                "activity offset must be 0-256 and limit 1-100",
            ));
        }
        Ok(Self {
            principal_id,
            token_id,
            offset,
            limit,
            policy,
        })
    }
    pub fn principal_id(self) -> PrincipalId {
        self.principal_id
    }
    pub fn token_id(self) -> TokenId {
        self.token_id
    }
    pub fn offset(self) -> usize {
        self.offset
    }
    pub fn limit(self) -> usize {
        self.limit
    }
    pub fn policy(self) -> CredentialActivityPolicy {
        self.policy
    }
}
#[derive(Clone, Debug)]
pub struct StorageCredentialActivity {
    session_id: Option<AuthSessionId>,
    creation: Option<StorageCredentialCreation>,
    sources: Vec<StorageCredentialSource>,
    total: usize,
    overflow_since: Option<DateTime<Utc>>,
}
impl StorageCredentialActivity {
    pub fn new(
        session_id: Option<AuthSessionId>,
        creation: Option<StorageCredentialCreation>,
        sources: Vec<StorageCredentialSource>,
        total: usize,
        overflow_since: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            session_id,
            creation,
            sources,
            total,
            overflow_since,
        }
    }
    pub fn session_id(&self) -> Option<AuthSessionId> {
        self.session_id
    }
    pub fn creation(&self) -> Option<&StorageCredentialCreation> {
        self.creation.as_ref()
    }
    pub fn sources(&self) -> &[StorageCredentialSource] {
        &self.sources
    }
    pub fn total(&self) -> usize {
        self.total
    }
    pub fn overflow_since(&self) -> Option<DateTime<Utc>> {
        self.overflow_since
    }
}
