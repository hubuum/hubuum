//! Database-managed runtime settings shared by every process using one store.
//!
//! Each settings group is owned either by every process's deployment
//! configuration or by one persisted, revisioned value that is authoritative for
//! the whole deployment. Values cross this boundary as validated types; adapters
//! may persist them as JSON documents but never expose arbitrary keys.

mod client_network;
mod metrics_path;
mod policies;
pub use client_network::{ClientAllowlist, ClientNetworkParseError, TrustedProxies};
pub use metrics_path::MetricsPath;
pub use policies::*;

use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hubuum_domain::{IdentityScopeName, LOCAL_IDENTITY_SCOPE, PrincipalId, ResourceRevision};
use hubuum_events_core::{ActorKind, CorrelationId, EventContext};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{StorageError, StorageQueryObservationSettings, StorageValidationError};

/// Largest page accepted by [`StorageRuntimeSettingsHistoryQuery`].
pub const MAX_RUNTIME_SETTINGS_HISTORY_PAGE: u16 = 100;
pub const RUNTIME_AUTHENTICATION_PASSWORD_ENVELOPE_FIELD: &str = "runtime_bind_password_envelope";

/// Stable identifier for a group of correlated runtime settings.
///
/// Groups are updated atomically and revisioned independently.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StorageRuntimeSettingsGroup {
    Authentication,
    Authorization,
    QueryObservations,
    Pagination,
    Observability,
    Network,
    QueryLimits,
    Tasks,
    RemoteCalls,
    Tokens,
    LoginRateLimit,
    Events,
    Exports,
    Backups,
    Restores,
}

impl StorageRuntimeSettingsGroup {
    /// Every group a complete adapter must persist.
    pub const ALL: [Self; 15] = [
        Self::Authentication,
        Self::Authorization,
        Self::QueryObservations,
        Self::Pagination,
        Self::Observability,
        Self::Network,
        Self::QueryLimits,
        Self::Tasks,
        Self::RemoteCalls,
        Self::Tokens,
        Self::LoginRateLimit,
        Self::Events,
        Self::Exports,
        Self::Backups,
        Self::Restores,
    ];

    /// Bounded persistence and diagnostic key.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Authentication => "authentication",
            Self::Authorization => "authorization",
            Self::QueryObservations => "query_observations",
            Self::Pagination => "pagination",
            Self::Observability => "observability",
            Self::Network => "network",
            Self::QueryLimits => "query_limits",
            Self::Tasks => "tasks",
            Self::RemoteCalls => "remote_calls",
            Self::Tokens => "tokens",
            Self::LoginRateLimit => "login_rate_limit",
            Self::Events => "events",
            Self::Exports => "exports",
            Self::Backups => "backups",
            Self::Restores => "restores",
        }
    }

    /// Parse a persisted group key.
    pub fn parse(value: &str) -> Result<Self, StorageValidationError> {
        Self::ALL
            .into_iter()
            .find(|group| group.as_str() == value)
            .ok_or_else(|| {
                StorageValidationError::invalid(format!("Unknown runtime settings group '{value}'"))
            })
    }
}

/// Shared enablement and optional sampling/capacity overrides for
/// process-local query observation collection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageQueryObservationPolicy {
    enabled: bool,
    sample_every: Option<u32>,
    max_patterns: Option<usize>,
    max_patterns_per_class: Option<usize>,
    retention_seconds: Option<u32>,
    max_predicates_per_query: Option<usize>,
}

impl StorageQueryObservationPolicy {
    #[must_use]
    pub const fn new(enabled: bool) -> Self {
        Self {
            enabled,
            sample_every: None,
            max_patterns: None,
            max_patterns_per_class: None,
            retention_seconds: None,
            max_predicates_per_query: None,
        }
    }

    pub fn try_with_resource_limits(
        enabled: bool,
        sample_every: u32,
        max_patterns: usize,
        max_patterns_per_class: usize,
        retention_seconds: u32,
        max_predicates_per_query: usize,
    ) -> Result<Self, StorageValidationError> {
        StorageQueryObservationSettings::try_new(
            enabled,
            sample_every,
            max_patterns,
            max_patterns_per_class,
            retention_seconds,
            max_predicates_per_query,
        )?;
        Ok(Self {
            enabled,
            sample_every: Some(sample_every),
            max_patterns: Some(max_patterns),
            max_patterns_per_class: Some(max_patterns_per_class),
            retention_seconds: Some(retention_seconds),
            max_predicates_per_query: Some(max_predicates_per_query),
        })
    }

    #[must_use]
    pub const fn enabled(self) -> bool {
        self.enabled
    }

    pub const fn sample_every(self) -> Option<u32> {
        self.sample_every
    }

    pub const fn max_patterns(self) -> Option<usize> {
        self.max_patterns
    }

    pub const fn max_patterns_per_class(self) -> Option<usize> {
        self.max_patterns_per_class
    }

    pub const fn retention_seconds(self) -> Option<u32> {
        self.retention_seconds
    }

    pub const fn max_predicates_per_query(self) -> Option<usize> {
        self.max_predicates_per_query
    }
}

/// Shared default page size and optional maximum for all paginated requests.
///
/// Every process still clamps the value to its own deployment ceiling.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoragePaginationPolicy {
    default_page_limit: NonZeroU32,
    max_page_limit: Option<NonZeroU64>,
}

impl StoragePaginationPolicy {
    pub fn try_new(default_page_limit: u64) -> Result<Self, StorageValidationError> {
        u32::try_from(default_page_limit)
            .ok()
            .and_then(NonZeroU32::new)
            .map(|default_page_limit| Self {
                default_page_limit,
                max_page_limit: None,
            })
            .ok_or_else(|| {
                StorageValidationError::invalid(format!(
                    "default_page_limit must be between 1 and {}",
                    u32::MAX
                ))
            })
    }

    /// Validate the shared default against the configuring process's ceiling.
    pub fn try_new_with_ceiling(
        default_page_limit: u64,
        maximum: u64,
    ) -> Result<Self, StorageValidationError> {
        let policy = Self::try_new(default_page_limit)?;
        if default_page_limit > maximum {
            return Err(StorageValidationError::invalid(format!(
                "default_page_limit ({default_page_limit}) must be less than or equal to max_page_limit ({maximum})"
            )));
        }
        Ok(policy)
    }

    /// Set both limits atomically, preserving `1 <= default <= maximum`.
    pub fn try_with_maximum(
        default_page_limit: u64,
        maximum: u64,
    ) -> Result<Self, StorageValidationError> {
        let maximum = NonZeroU64::new(maximum).ok_or_else(|| {
            StorageValidationError::invalid("max_page_limit must be greater than 0")
        })?;
        let mut policy = Self::try_new_with_ceiling(default_page_limit, maximum.get())?;
        policy.max_page_limit = Some(maximum);
        Ok(policy)
    }

    /// `None` preserves legacy policies that inherit the deployment maximum.
    #[must_use]
    pub const fn max_page_limit(self) -> Option<u64> {
        match self.max_page_limit {
            Some(maximum) => Some(maximum.get()),
            None => None,
        }
    }

    #[must_use]
    pub const fn default_page_limit(self) -> u32 {
        self.default_page_limit.get()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageAuthorizationBackend {
    Local,
    Treetop,
}

impl StorageAuthorizationBackend {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Treetop => "treetop",
        }
    }

    fn parse(value: &str) -> Result<Self, StorageValidationError> {
        match value {
            "local" => Ok(Self::Local),
            "treetop" => Ok(Self::Treetop),
            _ => Err(StorageValidationError::invalid(format!(
                "Unknown authorization backend '{value}'"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageAuthenticationProviderKind {
    Ldap,
}

impl StorageAuthenticationProviderKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ldap => "ldap",
        }
    }

    fn parse(value: &str) -> Result<Self, StorageValidationError> {
        match value {
            "ldap" => Ok(Self::Ldap),
            _ => Err(StorageValidationError::invalid(format!(
                "Unknown authentication provider kind '{value}'"
            ))),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthenticationProvider {
    name: IdentityScopeName,
    kind: StorageAuthenticationProviderKind,
    enabled: bool,
    definition: AuthenticationProviderDefinition,
}

#[derive(Clone, PartialEq, Eq)]
enum AuthenticationProviderDefinition {
    Deployment,
    Runtime(Value),
}

impl StorageAuthenticationProvider {
    pub fn try_new(
        name: impl Into<String>,
        kind: StorageAuthenticationProviderKind,
        configuration: Value,
    ) -> Result<Self, StorageValidationError> {
        let name = Self::validate_name(name.into())?;
        if !configuration.is_object() || configuration.get("bind_password").is_some() {
            return Err(StorageValidationError::invalid(
                "Runtime authentication providers require object configuration without plaintext passwords",
            ));
        }
        Ok(Self {
            name,
            kind,
            enabled: true,
            definition: AuthenticationProviderDefinition::Runtime(configuration),
        })
    }

    pub fn deployment(
        name: impl Into<String>,
        kind: StorageAuthenticationProviderKind,
        enabled: bool,
    ) -> Result<Self, StorageValidationError> {
        Ok(Self {
            name: Self::validate_name(name.into())?,
            kind,
            enabled,
            definition: AuthenticationProviderDefinition::Deployment,
        })
    }

    fn validate_name(name: String) -> Result<IdentityScopeName, StorageValidationError> {
        let name = IdentityScopeName::new(name)
            .map_err(|error| StorageValidationError::invalid(error.to_string()))?;
        if name.as_str() == LOCAL_IDENTITY_SCOPE {
            return Err(StorageValidationError::invalid(
                "Local authentication cannot be replaced or disabled",
            ));
        }
        Ok(name)
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub const fn kind(&self) -> StorageAuthenticationProviderKind {
        self.kind
    }

    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn configuration(&self) -> Option<&Value> {
        match &self.definition {
            AuthenticationProviderDefinition::Deployment => None,
            AuthenticationProviderDefinition::Runtime(configuration) => Some(configuration),
        }
    }
}

impl std::fmt::Debug for StorageAuthenticationProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageAuthenticationProvider")
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("enabled", &self.enabled)
            .field("configuration", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageAuthenticationProviders(Vec<StorageAuthenticationProvider>);

impl StorageAuthenticationProviders {
    pub const MAX_PROVIDERS: usize = 64;

    pub fn try_new(
        providers: Vec<StorageAuthenticationProvider>,
    ) -> Result<Self, StorageValidationError> {
        if providers.len() > Self::MAX_PROVIDERS {
            return Err(StorageValidationError::invalid(format!(
                "Authentication providers must not exceed {} entries",
                Self::MAX_PROVIDERS
            )));
        }
        let mut names = std::collections::HashSet::with_capacity(providers.len());
        if providers
            .iter()
            .any(|provider| !names.insert(&provider.name))
        {
            return Err(StorageValidationError::invalid(
                "Authentication provider names must be unique",
            ));
        }
        Ok(Self(providers))
    }

    pub fn providers(&self) -> &[StorageAuthenticationProvider] {
        &self.0
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AuthenticationProviderDocument {
    name: String,
    kind: String,
    #[serde(default)]
    source: AuthenticationProviderSource,
    #[serde(default = "provider_enabled_by_default")]
    enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    configuration: Option<Value>,
}

#[derive(Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum AuthenticationProviderSource {
    Deployment,
    #[default]
    Runtime,
}

const fn provider_enabled_by_default() -> bool {
    true
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AuthenticationProvidersDocument {
    providers: Vec<AuthenticationProviderDocument>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AuthorizationDocument {
    backend: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct QueryObservationDocument {
    enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sample_every: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_patterns: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_patterns_per_class: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retention_seconds: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_predicates_per_query: Option<usize>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PaginationDocument {
    default_page_limit: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_page_limit: Option<u64>,
}

/// A validated value for exactly one group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageRuntimeSettingsValue {
    QueryObservations(StorageQueryObservationPolicy),
    Pagination(StoragePaginationPolicy),
    Authentication(StorageAuthenticationProviders),
    Authorization(StorageAuthorizationBackend),
    Observability(StorageObservabilityPolicy),
    Network(StorageNetworkPolicy),
    QueryLimits(StorageQueryLimitsPolicy),
    Tasks(StorageTasksPolicy),
    RemoteCalls(StorageRemoteCallsPolicy),
    Tokens(StorageTokensPolicy),
    LoginRateLimit(StorageLoginRateLimitPolicy),
    Events(StorageEventsPolicy),
    Exports(StorageExportsPolicy),
    Backups(StorageBackupsPolicy),
    Restores(StorageRestoresPolicy),
}

impl StorageRuntimeSettingsValue {
    #[must_use]
    pub const fn group(&self) -> StorageRuntimeSettingsGroup {
        match self {
            Self::QueryObservations(_) => StorageRuntimeSettingsGroup::QueryObservations,
            Self::Pagination(_) => StorageRuntimeSettingsGroup::Pagination,
            Self::Authentication(_) => StorageRuntimeSettingsGroup::Authentication,
            Self::Authorization(_) => StorageRuntimeSettingsGroup::Authorization,
            Self::Observability(_) => StorageRuntimeSettingsGroup::Observability,
            Self::Network(_) => StorageRuntimeSettingsGroup::Network,
            Self::QueryLimits(_) => StorageRuntimeSettingsGroup::QueryLimits,
            Self::Tasks(_) => StorageRuntimeSettingsGroup::Tasks,
            Self::RemoteCalls(_) => StorageRuntimeSettingsGroup::RemoteCalls,
            Self::Tokens(_) => StorageRuntimeSettingsGroup::Tokens,
            Self::LoginRateLimit(_) => StorageRuntimeSettingsGroup::LoginRateLimit,
            Self::Events(_) => StorageRuntimeSettingsGroup::Events,
            Self::Exports(_) => StorageRuntimeSettingsGroup::Exports,
            Self::Backups(_) => StorageRuntimeSettingsGroup::Backups,
            Self::Restores(_) => StorageRuntimeSettingsGroup::Restores,
        }
    }

    /// Stable persistence document. Adapters store it without interpretation.
    #[must_use]
    pub fn to_document(&self) -> Value {
        let document = match self {
            Self::Observability(policy) => serde_json::to_value(policy),
            Self::Network(policy) => serde_json::to_value(policy),
            Self::QueryLimits(policy) => serde_json::to_value(policy),
            Self::Tasks(policy) => serde_json::to_value(policy),
            Self::RemoteCalls(policy) => serde_json::to_value(policy),
            Self::Tokens(policy) => serde_json::to_value(policy),
            Self::LoginRateLimit(policy) => serde_json::to_value(policy),
            Self::Events(policy) => serde_json::to_value(policy),
            Self::Exports(policy) => serde_json::to_value(policy),
            Self::Backups(policy) => serde_json::to_value(policy),
            Self::Restores(policy) => serde_json::to_value(policy),

            Self::QueryObservations(policy) => serde_json::to_value(QueryObservationDocument {
                enabled: policy.enabled(),
                sample_every: policy.sample_every(),
                max_patterns: policy.max_patterns(),
                max_patterns_per_class: policy.max_patterns_per_class(),
                retention_seconds: policy.retention_seconds(),
                max_predicates_per_query: policy.max_predicates_per_query(),
            }),
            Self::Pagination(policy) => serde_json::to_value(PaginationDocument {
                default_page_limit: u64::from(policy.default_page_limit()),
                max_page_limit: policy.max_page_limit(),
            }),
            Self::Authentication(providers) => {
                serde_json::to_value(AuthenticationProvidersDocument {
                    providers: providers
                        .providers()
                        .iter()
                        .map(|provider| AuthenticationProviderDocument {
                            name: provider.name().to_string(),
                            kind: provider.kind().as_str().to_string(),
                            source: if provider.configuration().is_some() {
                                AuthenticationProviderSource::Runtime
                            } else {
                                AuthenticationProviderSource::Deployment
                            },
                            enabled: provider.enabled(),
                            configuration: provider.configuration().cloned(),
                        })
                        .collect(),
                })
            }
            Self::Authorization(backend) => serde_json::to_value(AuthorizationDocument {
                backend: backend.as_str().to_string(),
            }),
        };
        document.expect("runtime settings documents contain only scalar fields")
    }

    /// Validate a persisted document for `group`.
    pub fn from_document(
        group: StorageRuntimeSettingsGroup,
        document: &Value,
    ) -> Result<Self, StorageValidationError> {
        let invalid = |error: serde_json::Error| {
            StorageValidationError::invalid(format!(
                "Invalid {} runtime settings document: {error}",
                group.as_str()
            ))
        };
        match group {
            StorageRuntimeSettingsGroup::Observability => {
                StorageObservabilityPolicy::deserialize(document)
                    .map(Self::Observability)
                    .map_err(invalid)
            }
            StorageRuntimeSettingsGroup::Network => StorageNetworkPolicy::deserialize(document)
                .map(Self::Network)
                .map_err(invalid),
            StorageRuntimeSettingsGroup::QueryLimits => {
                StorageQueryLimitsPolicy::deserialize(document)
                    .map(Self::QueryLimits)
                    .map_err(invalid)
            }
            StorageRuntimeSettingsGroup::Tasks => StorageTasksPolicy::deserialize(document)
                .map(Self::Tasks)
                .map_err(invalid),
            StorageRuntimeSettingsGroup::RemoteCalls => {
                StorageRemoteCallsPolicy::deserialize(document)
                    .map(Self::RemoteCalls)
                    .map_err(invalid)
            }
            StorageRuntimeSettingsGroup::Tokens => StorageTokensPolicy::deserialize(document)
                .map(Self::Tokens)
                .map_err(invalid),
            StorageRuntimeSettingsGroup::LoginRateLimit => {
                StorageLoginRateLimitPolicy::deserialize(document)
                    .map(Self::LoginRateLimit)
                    .map_err(invalid)
            }
            StorageRuntimeSettingsGroup::Events => StorageEventsPolicy::deserialize(document)
                .map(Self::Events)
                .map_err(invalid),
            StorageRuntimeSettingsGroup::Exports => StorageExportsPolicy::deserialize(document)
                .map(Self::Exports)
                .map_err(invalid),
            StorageRuntimeSettingsGroup::Backups => StorageBackupsPolicy::deserialize(document)
                .map(Self::Backups)
                .map_err(invalid),
            StorageRuntimeSettingsGroup::Restores => StorageRestoresPolicy::deserialize(document)
                .map(Self::Restores)
                .map_err(invalid),
            StorageRuntimeSettingsGroup::Authentication => {
                let document =
                    AuthenticationProvidersDocument::deserialize(document).map_err(invalid)?;
                let providers = document
                    .providers
                    .into_iter()
                    .map(|provider| {
                        let kind = StorageAuthenticationProviderKind::parse(&provider.kind)?;
                        match (provider.source, provider.configuration) {
                            (AuthenticationProviderSource::Runtime, Some(configuration)) => {
                                StorageAuthenticationProvider::try_new(
                                    provider.name,
                                    kind,
                                    configuration,
                                )
                                .map(|value| value.with_enabled(provider.enabled))
                            }
                            (AuthenticationProviderSource::Deployment, None) => {
                                StorageAuthenticationProvider::deployment(
                                    provider.name,
                                    kind,
                                    provider.enabled,
                                )
                            }
                            _ => Err(StorageValidationError::invalid(
                                "Only runtime-owned providers may contain a configuration",
                            )),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Self::Authentication(
                    StorageAuthenticationProviders::try_new(providers)?,
                ))
            }
            StorageRuntimeSettingsGroup::Authorization => {
                let document = AuthorizationDocument::deserialize(document).map_err(invalid)?;
                Ok(Self::Authorization(StorageAuthorizationBackend::parse(
                    &document.backend,
                )?))
            }
            StorageRuntimeSettingsGroup::QueryObservations => {
                let document = QueryObservationDocument::deserialize(document).map_err(invalid)?;
                let values = [
                    document.sample_every.is_some(),
                    document.max_patterns.is_some(),
                    document.max_patterns_per_class.is_some(),
                    document.retention_seconds.is_some(),
                    document.max_predicates_per_query.is_some(),
                ];
                if values.iter().any(|present| *present) && values.iter().any(|present| !present) {
                    return Err(StorageValidationError::invalid(
                        "Query observation resource settings must be specified together",
                    ));
                }
                let policy = match (
                    document.sample_every,
                    document.max_patterns,
                    document.max_patterns_per_class,
                    document.retention_seconds,
                    document.max_predicates_per_query,
                ) {
                    (
                        Some(sample_every),
                        Some(max_patterns),
                        Some(max_patterns_per_class),
                        Some(retention_seconds),
                        Some(max_predicates_per_query),
                    ) => StorageQueryObservationPolicy::try_with_resource_limits(
                        document.enabled,
                        sample_every,
                        max_patterns,
                        max_patterns_per_class,
                        retention_seconds,
                        max_predicates_per_query,
                    )?,
                    (None, None, None, None, None) => {
                        StorageQueryObservationPolicy::new(document.enabled)
                    }
                    _ => unreachable!("query observation fields were checked as a group"),
                };
                Ok(Self::QueryObservations(policy))
            }
            StorageRuntimeSettingsGroup::Pagination => {
                let document = PaginationDocument::deserialize(document).map_err(invalid)?;
                let policy = match document.max_page_limit {
                    Some(maximum) => StoragePaginationPolicy::try_with_maximum(
                        document.default_page_limit,
                        maximum,
                    )?,
                    None => StoragePaginationPolicy::try_new(document.default_page_limit)?,
                };
                Ok(Self::Pagination(policy))
            }
        }
    }
}

/// Owner of a group's effective value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageRuntimeSettingsAssignment {
    /// Each process applies its own deployment configuration.
    Deployment(StorageRuntimeSettingsGroup),
    /// The persisted value is authoritative for every participating process.
    Managed(StorageRuntimeSettingsValue),
}

impl StorageRuntimeSettingsAssignment {
    #[must_use]
    pub const fn group(&self) -> StorageRuntimeSettingsGroup {
        match self {
            Self::Deployment(group) => *group,
            Self::Managed(value) => value.group(),
        }
    }

    #[must_use]
    pub const fn value(&self) -> Option<&StorageRuntimeSettingsValue> {
        match self {
            Self::Deployment(_) => None,
            Self::Managed(value) => Some(value),
        }
    }

    /// Persistence document; `None` records deployment ownership.
    #[must_use]
    pub fn to_document(&self) -> Option<Value> {
        self.value().map(StorageRuntimeSettingsValue::to_document)
    }

    /// Validate persisted ownership for `group`.
    pub fn from_document(
        group: StorageRuntimeSettingsGroup,
        document: Option<&Value>,
    ) -> Result<Self, StorageValidationError> {
        match document {
            None => Ok(Self::Deployment(group)),
            Some(document) => Ok(Self::Managed(StorageRuntimeSettingsValue::from_document(
                group, document,
            )?)),
        }
    }
}

/// Current persisted state of one group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageRuntimeSettingsRecord {
    assignment: StorageRuntimeSettingsAssignment,
    revision: ResourceRevision,
    changed_at: DateTime<Utc>,
    changed_by: Option<PrincipalId>,
}

impl StorageRuntimeSettingsRecord {
    #[must_use]
    pub const fn new(
        assignment: StorageRuntimeSettingsAssignment,
        revision: ResourceRevision,
        changed_at: DateTime<Utc>,
        changed_by: Option<PrincipalId>,
    ) -> Self {
        Self {
            assignment,
            revision,
            changed_at,
            changed_by,
        }
    }

    /// Initial deployment ownership before any administrator change.
    #[must_use]
    pub const fn initial(group: StorageRuntimeSettingsGroup, changed_at: DateTime<Utc>) -> Self {
        Self::new(
            StorageRuntimeSettingsAssignment::Deployment(group),
            ResourceRevision::INITIAL,
            changed_at,
            None,
        )
    }

    #[must_use]
    pub const fn group(&self) -> StorageRuntimeSettingsGroup {
        self.assignment.group()
    }

    #[must_use]
    pub const fn assignment(&self) -> &StorageRuntimeSettingsAssignment {
        &self.assignment
    }

    #[must_use]
    pub const fn revision(&self) -> ResourceRevision {
        self.revision
    }

    #[must_use]
    pub const fn changed_at(&self) -> DateTime<Utc> {
        self.changed_at
    }

    #[must_use]
    pub const fn changed_by(&self) -> Option<PrincipalId> {
        self.changed_by
    }
}

/// Complete persisted runtime settings: exactly one record for every group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageRuntimeSettingsSnapshot {
    records: Vec<StorageRuntimeSettingsRecord>,
}

impl StorageRuntimeSettingsSnapshot {
    /// Reject partial or duplicated snapshots so callers never guess defaults.
    pub fn try_new(
        mut records: Vec<StorageRuntimeSettingsRecord>,
    ) -> Result<Self, StorageValidationError> {
        records.sort_by_key(StorageRuntimeSettingsRecord::group);
        let groups = records
            .iter()
            .map(StorageRuntimeSettingsRecord::group)
            .collect::<Vec<_>>();
        if groups != StorageRuntimeSettingsGroup::ALL {
            return Err(StorageValidationError::invalid(
                "Runtime settings must contain exactly one record for every group",
            ));
        }
        Ok(Self { records })
    }

    /// Every group owned by deployment configuration at its initial revision.
    #[must_use]
    pub fn initial(changed_at: DateTime<Utc>) -> Self {
        Self {
            records: StorageRuntimeSettingsGroup::ALL
                .into_iter()
                .map(|group| StorageRuntimeSettingsRecord::initial(group, changed_at))
                .collect(),
        }
    }

    #[must_use]
    pub fn records(&self) -> &[StorageRuntimeSettingsRecord] {
        &self.records
    }

    #[must_use]
    pub fn record(&self, group: StorageRuntimeSettingsGroup) -> &StorageRuntimeSettingsRecord {
        self.records
            .iter()
            .find(|record| record.group() == group)
            .expect("snapshots contain every runtime settings group")
    }

    /// Managed query observation policy, if an administrator set one.
    #[must_use]
    pub fn query_observations(&self) -> Option<StorageQueryObservationPolicy> {
        match self
            .record(StorageRuntimeSettingsGroup::QueryObservations)
            .assignment()
            .value()
        {
            Some(StorageRuntimeSettingsValue::QueryObservations(policy)) => Some(*policy),
            _ => None,
        }
    }

    /// Managed pagination policy, if an administrator set one.
    #[must_use]
    pub fn pagination(&self) -> Option<StoragePaginationPolicy> {
        match self
            .record(StorageRuntimeSettingsGroup::Pagination)
            .assignment()
            .value()
        {
            Some(StorageRuntimeSettingsValue::Pagination(policy)) => Some(*policy),
            _ => None,
        }
    }

    #[must_use]
    pub fn authentication(&self) -> Option<&StorageAuthenticationProviders> {
        match self
            .record(StorageRuntimeSettingsGroup::Authentication)
            .assignment()
            .value()
        {
            Some(StorageRuntimeSettingsValue::Authentication(providers)) => Some(providers),
            _ => None,
        }
    }

    #[must_use]
    pub fn authorization(&self) -> Option<StorageAuthorizationBackend> {
        match self
            .record(StorageRuntimeSettingsGroup::Authorization)
            .assignment()
            .value()
        {
            Some(StorageRuntimeSettingsValue::Authorization(backend)) => Some(*backend),
            _ => None,
        }
    }
}

/// Revision-checked replacement of one group's ownership and value.
#[derive(Clone, Debug)]
pub struct StorageRuntimeSettingsUpdate {
    assignment: StorageRuntimeSettingsAssignment,
    expected_revision: ResourceRevision,
    context: EventContext,
}

impl StorageRuntimeSettingsUpdate {
    #[must_use]
    pub const fn new(
        assignment: StorageRuntimeSettingsAssignment,
        expected_revision: ResourceRevision,
        context: EventContext,
    ) -> Self {
        Self {
            assignment,
            expected_revision,
            context,
        }
    }

    #[must_use]
    pub const fn group(&self) -> StorageRuntimeSettingsGroup {
        self.assignment.group()
    }

    #[must_use]
    pub const fn assignment(&self) -> &StorageRuntimeSettingsAssignment {
        &self.assignment
    }

    #[must_use]
    pub const fn expected_revision(&self) -> ResourceRevision {
        self.expected_revision
    }

    #[must_use]
    pub const fn context(&self) -> &EventContext {
        &self.context
    }
}

/// Result of a revision-checked update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageRuntimeSettingsUpdateOutcome {
    /// A new revision and its audit record were committed together.
    Committed(StorageRuntimeSettingsRecord),
    /// The assignment was already current; no revision or audit record was written.
    Unchanged(StorageRuntimeSettingsRecord),
}

impl StorageRuntimeSettingsUpdateOutcome {
    #[must_use]
    pub const fn record(&self) -> &StorageRuntimeSettingsRecord {
        match self {
            Self::Committed(record) | Self::Unchanged(record) => record,
        }
    }

    #[must_use]
    pub const fn is_committed(&self) -> bool {
        matches!(self, Self::Committed(_))
    }
}

/// Durable audit record for one committed revision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageRuntimeSettingsChange {
    revision: ResourceRevision,
    previous: StorageRuntimeSettingsAssignment,
    current: StorageRuntimeSettingsAssignment,
    changed_at: DateTime<Utc>,
    actor_kind: ActorKind,
    actor: Option<PrincipalId>,
    request_id: Option<Uuid>,
    correlation_id: Option<CorrelationId>,
}

/// Provenance captured with one audit record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageRuntimeSettingsActor {
    kind: ActorKind,
    principal: Option<PrincipalId>,
    request_id: Option<Uuid>,
    correlation_id: Option<CorrelationId>,
}

impl StorageRuntimeSettingsActor {
    #[must_use]
    pub const fn new(
        kind: ActorKind,
        principal: Option<PrincipalId>,
        request_id: Option<Uuid>,
        correlation_id: Option<CorrelationId>,
    ) -> Self {
        Self {
            kind,
            principal,
            request_id,
            correlation_id,
        }
    }

    /// Project the provenance an adapter must persist for an update.
    #[must_use]
    pub fn from_context(context: &EventContext) -> Self {
        Self::new(
            context.actor_kind(),
            context.actor_user_id(),
            context.request_id(),
            context.correlation_id().cloned(),
        )
    }
}

impl StorageRuntimeSettingsChange {
    /// Validate one audit record. Revision 1 is the initial state and is never audited.
    pub fn try_new(
        revision: ResourceRevision,
        previous: StorageRuntimeSettingsAssignment,
        current: StorageRuntimeSettingsAssignment,
        changed_at: DateTime<Utc>,
        actor: StorageRuntimeSettingsActor,
    ) -> Result<Self, StorageValidationError> {
        if previous.group() != current.group() {
            return Err(StorageValidationError::invalid(
                "A runtime settings change must stay within one group",
            ));
        }
        if revision <= ResourceRevision::INITIAL {
            return Err(StorageValidationError::invalid(
                "Runtime settings changes start after the initial revision",
            ));
        }
        Ok(Self {
            revision,
            previous,
            current,
            changed_at,
            actor_kind: actor.kind,
            actor: actor.principal,
            request_id: actor.request_id,
            correlation_id: actor.correlation_id,
        })
    }

    #[must_use]
    pub const fn group(&self) -> StorageRuntimeSettingsGroup {
        self.current.group()
    }

    #[must_use]
    pub const fn revision(&self) -> ResourceRevision {
        self.revision
    }

    #[must_use]
    pub const fn previous(&self) -> &StorageRuntimeSettingsAssignment {
        &self.previous
    }

    #[must_use]
    pub const fn current(&self) -> &StorageRuntimeSettingsAssignment {
        &self.current
    }

    #[must_use]
    pub const fn changed_at(&self) -> DateTime<Utc> {
        self.changed_at
    }

    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }

    #[must_use]
    pub const fn actor(&self) -> Option<PrincipalId> {
        self.actor
    }

    #[must_use]
    pub const fn request_id(&self) -> Option<Uuid> {
        self.request_id
    }

    #[must_use]
    pub fn correlation_id(&self) -> Option<&CorrelationId> {
        self.correlation_id.as_ref()
    }
}

/// Newest-first page of one group's audit records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageRuntimeSettingsHistoryQuery {
    group: StorageRuntimeSettingsGroup,
    before: Option<ResourceRevision>,
    limit: NonZeroU16,
}

impl StorageRuntimeSettingsHistoryQuery {
    /// `before` selects records with strictly older revisions.
    pub fn try_new(
        group: StorageRuntimeSettingsGroup,
        before: Option<ResourceRevision>,
        limit: usize,
    ) -> Result<Self, StorageValidationError> {
        u16::try_from(limit)
            .ok()
            .filter(|limit| *limit <= MAX_RUNTIME_SETTINGS_HISTORY_PAGE)
            .and_then(NonZeroU16::new)
            .map(|limit| Self {
                group,
                before,
                limit,
            })
            .ok_or_else(|| {
                StorageValidationError::invalid(format!(
                    "limit must be between 1 and {MAX_RUNTIME_SETTINGS_HISTORY_PAGE}"
                ))
            })
    }

    #[must_use]
    pub const fn group(self) -> StorageRuntimeSettingsGroup {
        self.group
    }

    #[must_use]
    pub const fn before(self) -> Option<ResourceRevision> {
        self.before
    }

    #[must_use]
    pub const fn limit(self) -> usize {
        self.limit.get() as usize
    }
}

/// Shared, revisioned runtime configuration with a durable audit trail.
///
/// Values are deployment configuration rather than domain data: adapters keep
/// them, and their audit records, across logical backup restores and exclude
/// them from portable backups.
#[async_trait]
pub trait RuntimeSettingsStorage: Send + Sync {
    /// Load every group's current record. Never returns a partial snapshot.
    async fn load_runtime_settings(&self) -> Result<StorageRuntimeSettingsSnapshot, StorageError>;

    /// Replace one group's assignment when `expected_revision` is current.
    ///
    /// A committed change advances the group's revision and appends exactly one
    /// audit record in the same atomic operation. A stale revision returns
    /// `RevisionConflict` with the current revision. Requesting the current
    /// assignment returns `Unchanged` without a new revision or audit record.
    async fn update_runtime_settings(
        &self,
        request: StorageRuntimeSettingsUpdate,
    ) -> Result<StorageRuntimeSettingsUpdateOutcome, StorageError>;

    /// Newest-first audit records for one group, older than `before` when set.
    async fn list_runtime_settings_changes(
        &self,
        query: StorageRuntimeSettingsHistoryQuery,
    ) -> Result<Vec<StorageRuntimeSettingsChange>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use serde_json::json;

    #[rstest]
    #[case::query_observations(StorageRuntimeSettingsValue::QueryObservations(
        StorageQueryObservationPolicy::new(true)
    ))]
    #[case::query_observation_limits(StorageRuntimeSettingsValue::QueryObservations(
        StorageQueryObservationPolicy::try_with_resource_limits(true, 4, 128, 32, 3600, 8)
            .unwrap()
    ))]
    #[case::pagination(StorageRuntimeSettingsValue::Pagination(
        StoragePaginationPolicy::try_new(25).unwrap()
    ))]
    #[case::pagination_maximum(StorageRuntimeSettingsValue::Pagination(
        StoragePaginationPolicy::try_with_maximum(25, 100).unwrap()
    ))]
    #[case::authentication(StorageRuntimeSettingsValue::Authentication(
        StorageAuthenticationProviders::try_new(vec![
            StorageAuthenticationProvider::try_new(
                "corporate",
                StorageAuthenticationProviderKind::Ldap,
                json!({"url": "ldaps://directory.example"}),
            )
            .unwrap(),
        ])
        .unwrap()
    ))]
    #[case::authorization(StorageRuntimeSettingsValue::Authorization(
        StorageAuthorizationBackend::Treetop
    ))]
    fn documents_round_trip_through_validation(#[case] value: StorageRuntimeSettingsValue) {
        assert_eq!(
            StorageRuntimeSettingsValue::from_document(value.group(), &value.to_document())
                .unwrap(),
            value
        );
    }

    #[rstest]
    #[case::unknown_field(StorageRuntimeSettingsGroup::Pagination, json!({"default_page_limit": 5, "max": 9}))]
    #[case::zero_limit(StorageRuntimeSettingsGroup::Pagination, json!({"default_page_limit": 0}))]
    #[case::oversized_limit(StorageRuntimeSettingsGroup::Pagination, json!({"default_page_limit": u64::from(u32::MAX) + 1}))]
    #[case::negative_limit(StorageRuntimeSettingsGroup::Pagination, json!({"default_page_limit": -1}))]
    #[case::zero_maximum(StorageRuntimeSettingsGroup::Pagination, json!({"default_page_limit": 5, "max_page_limit": 0}))]
    #[case::negative_maximum(StorageRuntimeSettingsGroup::Pagination, json!({"default_page_limit": 5, "max_page_limit": -1}))]
    #[case::inverted_limits(StorageRuntimeSettingsGroup::Pagination, json!({"default_page_limit": 10, "max_page_limit": 5}))]
    #[case::unknown_authentication_kind(StorageRuntimeSettingsGroup::Authentication, json!({"providers": [{"name": "corporate", "kind": "oidc", "configuration": {}}]}))]
    #[case::unknown_authorization_backend(StorageRuntimeSettingsGroup::Authorization, json!({"backend": "remote"}))]
    #[case::wrong_group(StorageRuntimeSettingsGroup::QueryObservations, json!({"default_page_limit": 5}))]
    #[case::wrong_type(StorageRuntimeSettingsGroup::QueryObservations, json!({"enabled": "yes"}))]
    fn invalid_documents_are_rejected(
        #[case] group: StorageRuntimeSettingsGroup,
        #[case] document: Value,
    ) {
        assert!(StorageRuntimeSettingsValue::from_document(group, &document).is_err());
    }

    #[test]
    fn legacy_pagination_documents_keep_the_deployment_maximum() {
        let document = json!({"default_page_limit": 25});
        let value = StorageRuntimeSettingsValue::from_document(
            StorageRuntimeSettingsGroup::Pagination,
            &document,
        )
        .unwrap();
        assert_eq!(value.to_document(), document);
    }

    #[test]
    fn authentication_configuration_debug_output_redacts_credentials() {
        let providers = StorageAuthenticationProviders::try_new(vec![
            StorageAuthenticationProvider::try_new(
                "corporate",
                StorageAuthenticationProviderKind::Ldap,
                json!({"runtime_bind_password_envelope": "secret-value"}),
            )
            .unwrap(),
        ])
        .unwrap();
        assert!(!format!("{providers:?}").contains("secret-value"));
    }

    #[test]
    fn authentication_provider_names_must_be_unique() {
        let provider = || {
            StorageAuthenticationProvider::try_new(
                "corporate",
                StorageAuthenticationProviderKind::Ldap,
                json!({}),
            )
            .unwrap()
        };
        assert!(StorageAuthenticationProviders::try_new(vec![provider(), provider()]).is_err());
    }

    #[rstest]
    #[case::dotted("corp.example.org".to_string())]
    #[case::unicode("Bedrift Øst".to_string())]
    #[case::long("a".repeat(129))]
    fn authentication_provider_names_survive_persistence(
        #[case] name: String,
        #[values(true, false)] deployment_owned: bool,
    ) {
        let provider = if deployment_owned {
            StorageAuthenticationProvider::deployment(
                name,
                StorageAuthenticationProviderKind::Ldap,
                true,
            )
        } else {
            StorageAuthenticationProvider::try_new(
                name,
                StorageAuthenticationProviderKind::Ldap,
                json!({}),
            )
        }
        .unwrap();
        let value = StorageRuntimeSettingsValue::Authentication(
            StorageAuthenticationProviders::try_new(vec![provider]).unwrap(),
        );
        assert_eq!(
            StorageRuntimeSettingsValue::from_document(value.group(), &value.to_document())
                .unwrap(),
            value
        );
    }

    #[rstest]
    #[case::local("local")]
    #[case::empty("")]
    #[case::whitespace(" ")]
    #[case::leading_whitespace(" corporate")]
    #[case::trailing_whitespace("corporate ")]
    #[case::unicode_whitespace("\u{2003}corporate")]
    fn authentication_provider_names_reject_reserved_or_untrimmed_names(
        #[case] name: &str,
        #[values(true, false)] deployment_owned: bool,
    ) {
        let result = if deployment_owned {
            StorageAuthenticationProvider::deployment(
                name,
                StorageAuthenticationProviderKind::Ldap,
                false,
            )
        } else {
            StorageAuthenticationProvider::try_new(
                name,
                StorageAuthenticationProviderKind::Ldap,
                json!({}),
            )
        };
        assert!(result.is_err());
    }

    #[rstest]
    #[case::deployment(json!({"name": "local", "kind": "ldap", "source": "deployment", "enabled": false}))]
    #[case::runtime(json!({"name": "local", "kind": "ldap", "source": "runtime", "configuration": {}}))]
    #[case::deployment_configuration(json!({"name": "corporate", "kind": "ldap", "source": "deployment", "configuration": {}}))]
    #[case::missing_runtime_configuration(json!({"name": "corporate", "kind": "ldap", "source": "runtime"}))]
    fn authentication_documents_reject_invalid_ownership(#[case] provider: Value) {
        assert!(
            StorageRuntimeSettingsValue::from_document(
                StorageRuntimeSettingsGroup::Authentication,
                &json!({"providers": [provider]}),
            )
            .is_err()
        );
    }

    #[test]
    fn authentication_provider_documents_reject_plaintext_passwords() {
        assert!(
            StorageAuthenticationProvider::try_new(
                "corporate",
                StorageAuthenticationProviderKind::Ldap,
                json!({"bind_password": "must-not-persist"}),
            )
            .is_err()
        );
    }

    #[test]
    fn legacy_query_observation_documents_keep_deployment_resource_defaults() {
        let value = StorageRuntimeSettingsValue::from_document(
            StorageRuntimeSettingsGroup::QueryObservations,
            &json!({"enabled": true}),
        )
        .unwrap();
        let StorageRuntimeSettingsValue::QueryObservations(policy) = value else {
            panic!("expected query observation policy");
        };
        assert_eq!(
            (
                policy.enabled(),
                policy.sample_every(),
                policy.max_patterns(),
                policy.max_patterns_per_class(),
                policy.retention_seconds(),
                policy.max_predicates_per_query(),
            ),
            (true, None, None, None, None, None)
        );
    }

    #[test]
    fn query_observation_documents_reject_partial_resource_limits() {
        assert!(
            StorageRuntimeSettingsValue::from_document(
                StorageRuntimeSettingsGroup::QueryObservations,
                &json!({"enabled": true, "sample_every": 4}),
            )
            .is_err()
        );
    }

    #[rstest]
    #[case::missing_group(vec![StorageRuntimeSettingsRecord::initial(StorageRuntimeSettingsGroup::Pagination, Utc::now())])]
    #[case::duplicate_group(vec![
        StorageRuntimeSettingsRecord::initial(StorageRuntimeSettingsGroup::Pagination, Utc::now()),
        StorageRuntimeSettingsRecord::initial(StorageRuntimeSettingsGroup::Pagination, Utc::now()),
        StorageRuntimeSettingsRecord::initial(StorageRuntimeSettingsGroup::QueryObservations, Utc::now()),
    ])]
    fn snapshots_require_exactly_one_record_per_group(
        #[case] records: Vec<StorageRuntimeSettingsRecord>,
    ) {
        assert!(StorageRuntimeSettingsSnapshot::try_new(records).is_err());
    }

    #[test]
    fn changes_cannot_cross_groups() {
        let result = StorageRuntimeSettingsChange::try_new(
            ResourceRevision::new(2).unwrap(),
            StorageRuntimeSettingsAssignment::Deployment(StorageRuntimeSettingsGroup::Pagination),
            StorageRuntimeSettingsAssignment::Managed(
                StorageRuntimeSettingsValue::QueryObservations(StorageQueryObservationPolicy::new(
                    true,
                )),
            ),
            Utc::now(),
            StorageRuntimeSettingsActor::from_context(&EventContext::system()),
        );
        assert!(result.is_err());
    }

    #[rstest]
    #[case::zero(0)]
    #[case::too_large(usize::from(MAX_RUNTIME_SETTINGS_HISTORY_PAGE) + 1)]
    fn history_pages_are_bounded(#[case] limit: usize) {
        assert!(
            StorageRuntimeSettingsHistoryQuery::try_new(
                StorageRuntimeSettingsGroup::Pagination,
                None,
                limit
            )
            .is_err()
        );
    }

    #[test]
    fn group_keys_are_stable() {
        for group in StorageRuntimeSettingsGroup::ALL {
            assert_eq!(
                StorageRuntimeSettingsGroup::parse(group.as_str()),
                Ok(group)
            );
        }
        assert_eq!(
            StorageRuntimeSettingsGroup::ALL.map(StorageRuntimeSettingsGroup::as_str),
            [
                "authentication",
                "authorization",
                "query_observations",
                "pagination",
                "observability",
                "network",
                "query_limits",
                "tasks",
                "remote_calls",
                "tokens",
                "login_rate_limit",
                "events",
                "exports",
                "backups",
                "restores",
            ]
        );
    }
}
