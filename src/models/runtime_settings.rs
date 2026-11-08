//! API representation of database-managed runtime settings.
use chrono::{DateTime, Utc};
use hubuum_domain::{PrincipalId, ResourceRevision};
use hubuum_storage_core::{
    StorageAuthenticationProviderKind, StorageAuthorizationBackend, StoragePaginationPolicy,
    StorageQueryObservationPolicy, StorageQueryObservationSettings,
    StorageRuntimeSettingsAssignment, StorageRuntimeSettingsChange, StorageRuntimeSettingsGroup,
    StorageRuntimeSettingsRecord, StorageRuntimeSettingsUpdate, StorageRuntimeSettingsValue,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use crate::errors::ApiError;
use crate::events::EventContext;
use crate::pagination::PageLimits;
use crate::storage::runtime_settings::{
    AppliedRuntimeSettings, RUNTIME_SETTINGS_RECONCILE_INTERVAL, RuntimeSettingsApplication,
};

/// Default number of audit records returned by a history request.
const DEFAULT_HISTORY_LIMIT: usize = 20;

/// Stable key of a group of correlated runtime settings.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeSettingsGroup {
    Authentication,
    Authorization,
    QueryObservations,
    Pagination,
}

impl From<RuntimeSettingsGroup> for StorageRuntimeSettingsGroup {
    fn from(value: RuntimeSettingsGroup) -> Self {
        match value {
            RuntimeSettingsGroup::Authentication => Self::Authentication,
            RuntimeSettingsGroup::Authorization => Self::Authorization,
            RuntimeSettingsGroup::QueryObservations => Self::QueryObservations,
            RuntimeSettingsGroup::Pagination => Self::Pagination,
        }
    }
}

impl From<StorageRuntimeSettingsGroup> for RuntimeSettingsGroup {
    fn from(value: StorageRuntimeSettingsGroup) -> Self {
        match value {
            StorageRuntimeSettingsGroup::Authentication => Self::Authentication,
            StorageRuntimeSettingsGroup::Authorization => Self::Authorization,
            StorageRuntimeSettingsGroup::QueryObservations => Self::QueryObservations,
            StorageRuntimeSettingsGroup::Pagination => Self::Pagination,
        }
    }
}

/// Manage query observation enablement for every replica sharing storage.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct QueryObservationSettingsRequest {
    /// Current revision of the `query_observations` group.
    pub expected_revision: ResourceRevision,
    pub enabled: bool,
    /// Sample one in every N eligible queries. Smaller values sample more often.
    #[schema(minimum = 1, maximum = 1_000_000)]
    pub sample_every: Option<u32>,
    #[schema(minimum = 1, maximum = 16_384)]
    pub max_patterns: Option<usize>,
    #[schema(minimum = 1, maximum = 128)]
    pub max_patterns_per_class: Option<usize>,
    #[schema(minimum = 1, maximum = 604_800)]
    pub retention_seconds: Option<u32>,
    #[schema(minimum = 1, maximum = 32)]
    pub max_predicates_per_query: Option<usize>,
}

impl QueryObservationSettingsRequest {
    pub fn into_update(
        self,
        deployment: StorageQueryObservationSettings,
        context: EventContext,
    ) -> Result<StorageRuntimeSettingsUpdate, ApiError> {
        let policy = StorageQueryObservationPolicy::try_with_resource_limits(
            self.enabled,
            self.sample_every.unwrap_or(deployment.sample_every()),
            self.max_patterns.unwrap_or(deployment.max_patterns()),
            self.max_patterns_per_class
                .unwrap_or(deployment.max_patterns_per_class()),
            self.retention_seconds
                .unwrap_or(deployment.retention_seconds()),
            self.max_predicates_per_query
                .unwrap_or(deployment.max_predicates_per_query()),
        )
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
        if !deployment.policy_within_ceilings(policy) {
            return Err(ApiError::BadRequest(
                "query observation settings exceed this replica's deployment ceilings".to_string(),
            ));
        }
        Ok(StorageRuntimeSettingsUpdate::new(
            StorageRuntimeSettingsAssignment::Managed(
                StorageRuntimeSettingsValue::QueryObservations(policy),
            ),
            self.expected_revision,
            context,
        ))
    }
}

/// Manage the default page size for every replica sharing storage.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PaginationSettingsRequest {
    /// Current revision of the `pagination` group.
    pub expected_revision: ResourceRevision,
    /// Items returned when a request omits `limit`. Must not exceed the
    /// responding replica's deployment `max_page_limit`.
    #[schema(minimum = 1)]
    pub default_page_limit: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeAuthenticationProviderKind {
    Ldap,
}

impl From<RuntimeAuthenticationProviderKind> for StorageAuthenticationProviderKind {
    fn from(value: RuntimeAuthenticationProviderKind) -> Self {
        match value {
            RuntimeAuthenticationProviderKind::Ldap => Self::Ldap,
        }
    }
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RuntimeAuthenticationProviderRequest {
    pub name: String,
    pub kind: RuntimeAuthenticationProviderKind,
    /// Provider-specific configuration. LDAP passwords are accepted here but
    /// are encrypted before the document is persisted.
    #[schema(value_type = Object)]
    pub configuration: Value,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthenticationSettingsRequest {
    /// Current revision of the `authentication` group.
    pub expected_revision: ResourceRevision,
    pub providers: Vec<RuntimeAuthenticationProviderRequest>,
}

impl AuthenticationSettingsRequest {
    pub fn into_parts(
        self,
    ) -> (
        ResourceRevision,
        Vec<(String, StorageAuthenticationProviderKind, Value)>,
    ) {
        (
            self.expected_revision,
            self.providers
                .into_iter()
                .map(|provider| (provider.name, provider.kind.into(), provider.configuration))
                .collect(),
        )
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeAuthorizationBackend {
    Local,
    Treetop,
}

impl From<RuntimeAuthorizationBackend> for StorageAuthorizationBackend {
    fn from(value: RuntimeAuthorizationBackend) -> Self {
        match value {
            RuntimeAuthorizationBackend::Local => Self::Local,
            RuntimeAuthorizationBackend::Treetop => Self::Treetop,
        }
    }
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationSettingsRequest {
    /// Current revision of the `authorization` group.
    pub expected_revision: ResourceRevision,
    pub backend: RuntimeAuthorizationBackend,
}

impl AuthorizationSettingsRequest {
    pub fn into_update(self, context: EventContext) -> StorageRuntimeSettingsUpdate {
        StorageRuntimeSettingsUpdate::new(
            StorageRuntimeSettingsAssignment::Managed(StorageRuntimeSettingsValue::Authorization(
                self.backend.into(),
            )),
            self.expected_revision,
            context,
        )
    }
}

impl PaginationSettingsRequest {
    /// Validate against the responding replica's deployment ceiling. Replicas
    /// configured with a lower ceiling clamp the shared value when applying it.
    pub fn into_update(
        self,
        ceiling: PageLimits,
        context: EventContext,
    ) -> Result<StorageRuntimeSettingsUpdate, ApiError> {
        let maximum = ceiling.maximum_limit();
        if usize::try_from(self.default_page_limit).map_or(true, |limit| limit > maximum) {
            return Err(ApiError::BadRequest(format!(
                "default_page_limit ({}) must be less than or equal to the deployment max_page_limit ({maximum})",
                self.default_page_limit
            )));
        }
        let policy = StoragePaginationPolicy::try_new(self.default_page_limit)
            .map_err(|error| ApiError::BadRequest(error.message().to_string()))?;
        Ok(StorageRuntimeSettingsUpdate::new(
            StorageRuntimeSettingsAssignment::Managed(StorageRuntimeSettingsValue::Pagination(
                policy,
            )),
            self.expected_revision,
            context,
        ))
    }
}

/// Return a group to deployment configuration as a new, audited revision.
#[derive(Debug, Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub struct RuntimeSettingsReleaseParams {
    /// Current revision of the group.
    pub expected_revision: ResourceRevision,
}

impl RuntimeSettingsReleaseParams {
    pub fn into_update(
        self,
        group: RuntimeSettingsGroup,
        context: EventContext,
    ) -> StorageRuntimeSettingsUpdate {
        StorageRuntimeSettingsUpdate::new(
            StorageRuntimeSettingsAssignment::Deployment(group.into()),
            self.expected_revision,
            context,
        )
    }
}

#[derive(Debug, Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub struct RuntimeSettingsHistoryParams {
    /// Records to return, newest first. Defaults to 20; at most 100.
    #[param(minimum = 1, maximum = 100)]
    pub limit: Option<usize>,
    /// Return records with revisions older than this one.
    pub before_revision: Option<ResourceRevision>,
}

impl RuntimeSettingsHistoryParams {
    pub fn limit(&self) -> usize {
        self.limit.unwrap_or(DEFAULT_HISTORY_LIMIT)
    }
}

/// Owner of a group's effective value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeSettingsSource {
    /// Each process applies its own startup configuration.
    Deployment,
    /// The persisted value applies to every replica.
    Runtime,
}

/// When a changed value takes effect on a replica that has applied it.
#[derive(Clone, Copy, Debug, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeSettingsEffect {
    /// API requests resolve the value once, when they start.
    NextRequest,
    /// Sampling decisions read the value for each eligible query.
    NextQuery,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeSettingsApplicationStatus {
    /// This replica has not loaded shared settings; deployment values apply.
    NotLoaded,
    /// The latest refresh succeeded within the reconcile window.
    Current,
    /// The latest refresh failed or reconciliation is overdue. The last valid
    /// snapshot is retained except where a group documents a different policy.
    Stale,
}

impl From<RuntimeSettingsApplication> for RuntimeSettingsApplicationStatus {
    fn from(value: RuntimeSettingsApplication) -> Self {
        match value {
            RuntimeSettingsApplication::NotLoaded => Self::NotLoaded,
            RuntimeSettingsApplication::Current => Self::Current,
            RuntimeSettingsApplication::Stale => Self::Stale,
        }
    }
}

/// Application state of the replica that answered this request. Other
/// replicas apply committed changes independently.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct RuntimeSettingsReplicaStatus {
    pub status: RuntimeSettingsApplicationStatus,
    pub applied_at: Option<DateTime<Utc>>,
    pub last_refresh_attempt_at: Option<DateTime<Utc>>,
    pub last_refresh_failure_at: Option<DateTime<Utc>>,
    /// Interval of the reconcile poll that recovers missed notifications.
    pub reconcile_interval_seconds: u64,
}

/// Ownership and revision metadata shared by every group.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct RuntimeSettingsOwnership {
    pub source: RuntimeSettingsSource,
    /// Revision applied by the responding replica; null before it first loads
    /// shared settings.
    pub revision: Option<ResourceRevision>,
    /// Administrators can change this group without restarting processes.
    pub mutable: bool,
    pub restart_required: bool,
    pub takes_effect: RuntimeSettingsEffect,
    pub changed_at: Option<DateTime<Utc>>,
    pub changed_by: Option<PrincipalId>,
}

impl RuntimeSettingsOwnership {
    fn new(
        record: Option<&StorageRuntimeSettingsRecord>,
        takes_effect: RuntimeSettingsEffect,
    ) -> Self {
        let source = match record.map(StorageRuntimeSettingsRecord::assignment) {
            Some(StorageRuntimeSettingsAssignment::Managed(_)) => RuntimeSettingsSource::Runtime,
            _ => RuntimeSettingsSource::Deployment,
        };
        Self {
            source,
            revision: record.map(StorageRuntimeSettingsRecord::revision),
            mutable: true,
            restart_required: false,
            takes_effect,
            changed_at: record.map(StorageRuntimeSettingsRecord::changed_at),
            changed_by: record.and_then(StorageRuntimeSettingsRecord::changed_by),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, ToSchema)]
pub struct QueryObservationPolicyValue {
    pub enabled: bool,
    pub sample_every: u32,
    pub max_patterns: usize,
    pub max_patterns_per_class: usize,
    pub retention_seconds: u32,
    pub max_predicates_per_query: usize,
}

impl From<StorageQueryObservationSettings> for QueryObservationPolicyValue {
    fn from(value: StorageQueryObservationSettings) -> Self {
        Self {
            enabled: value.enabled(),
            sample_every: value.sample_every(),
            max_patterns: value.max_patterns(),
            max_patterns_per_class: value.max_patterns_per_class(),
            retention_seconds: value.retention_seconds(),
            max_predicates_per_query: value.max_predicates_per_query(),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, ToSchema)]
pub struct QueryObservationCeilingsValue {
    pub min_sample_every: u32,
    pub max_patterns: usize,
    pub max_patterns_per_class: usize,
    pub max_retention_seconds: u32,
    pub max_predicates_per_query: usize,
}

impl From<hubuum_storage_core::StorageQueryObservationCeilings> for QueryObservationCeilingsValue {
    fn from(value: hubuum_storage_core::StorageQueryObservationCeilings) -> Self {
        Self {
            min_sample_every: value.min_sample_every(),
            max_patterns: value.max_patterns(),
            max_patterns_per_class: value.max_patterns_per_class(),
            max_retention_seconds: value.max_retention_seconds(),
            max_predicates_per_query: value.max_predicates_per_query(),
        }
    }
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct QueryObservationSettingsReport {
    #[serde(flatten)]
    pub ownership: RuntimeSettingsOwnership,
    pub effective: QueryObservationPolicyValue,
    pub runtime: Option<QueryObservationPolicyValue>,
    pub deployment: QueryObservationPolicyValue,
    pub ceilings: QueryObservationCeilingsValue,
    pub constrained_by_ceiling: bool,
    /// Collection pauses while this replica's latest refresh has failed,
    /// rather than keep a possibly stale enablement.
    pub collection_paused: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, ToSchema)]
pub struct PaginationPolicyValue {
    #[schema(minimum = 1)]
    pub default_page_limit: usize,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct PaginationSettingsReport {
    #[serde(flatten)]
    pub ownership: RuntimeSettingsOwnership,
    pub effective: PaginationPolicyValue,
    pub runtime: Option<PaginationPolicyValue>,
    pub deployment: PaginationPolicyValue,
    /// Deployment resource ceiling of this replica. It is never runtime-managed.
    pub max_page_limit: usize,
    /// The runtime default exceeds this replica's ceiling and is clamped to it.
    pub constrained_by_ceiling: bool,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct RuntimeAuthenticationProviderStatus {
    pub name: String,
    pub available: bool,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct AuthenticationSettingsReport {
    #[serde(flatten)]
    pub ownership: RuntimeSettingsOwnership,
    pub providers: Vec<RuntimeAuthenticationProviderStatus>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct AuthorizationSettingsReport {
    #[serde(flatten)]
    pub ownership: RuntimeSettingsOwnership,
    pub effective_backend: String,
    pub runtime_backend: Option<String>,
    pub deployment_backend: String,
    pub available: bool,
}

/// Deployment values that runtime settings override on this replica.
#[derive(Clone, Copy, Debug)]
pub struct RuntimeSettingsDeployment {
    pub query_observations: StorageQueryObservationSettings,
    pub pagination: PageLimits,
    pub authorization_backend: StorageAuthorizationBackend,
}

/// Runtime-managed settings as applied by the responding replica.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct RuntimeSettingsReport {
    pub replica: RuntimeSettingsReplicaStatus,
    pub authentication: AuthenticationSettingsReport,
    pub authorization: AuthorizationSettingsReport,
    pub query_observations: QueryObservationSettingsReport,
    pub pagination: PaginationSettingsReport,
}

impl RuntimeSettingsReport {
    pub(crate) fn new(
        applied: &AppliedRuntimeSettings,
        deployment: RuntimeSettingsDeployment,
        now: DateTime<Utc>,
        authentication_providers: Vec<RuntimeAuthenticationProviderStatus>,
        authorization_backend: String,
        authorization_ready: bool,
    ) -> Self {
        let snapshot = applied.snapshot();
        let record = |group| snapshot.map(|snapshot| snapshot.record(group));
        let observation_runtime = snapshot.and_then(|snapshot| {
            snapshot.query_observations().map(|policy| {
                QueryObservationPolicyValue::from(
                    deployment.query_observations.with_runtime_policy(policy),
                )
            })
        });
        let observation_deployment =
            QueryObservationPolicyValue::from(deployment.query_observations);
        let observation_effective = observation_runtime.unwrap_or(observation_deployment);
        let observation_requested = snapshot
            .and_then(|snapshot| snapshot.query_observations())
            .map(|policy| {
                let mut value = observation_deployment;
                value.enabled = policy.enabled();
                if let Some(sample_every) = policy.sample_every() {
                    value.sample_every = sample_every;
                }
                if let Some(max_patterns) = policy.max_patterns() {
                    value.max_patterns = max_patterns;
                }
                if let Some(max_patterns_per_class) = policy.max_patterns_per_class() {
                    value.max_patterns_per_class = max_patterns_per_class;
                }
                if let Some(retention_seconds) = policy.retention_seconds() {
                    value.retention_seconds = retention_seconds;
                }
                if let Some(max_predicates_per_query) = policy.max_predicates_per_query() {
                    value.max_predicates_per_query = max_predicates_per_query;
                }
                value
            });
        let pagination_policy = snapshot.and_then(|snapshot| snapshot.pagination());
        let effective_pages = deployment
            .pagination
            .with_runtime_default(pagination_policy);
        Self {
            replica: RuntimeSettingsReplicaStatus {
                status: applied.application(now).into(),
                applied_at: applied.applied_at(),
                last_refresh_attempt_at: applied.last_attempt_at(),
                last_refresh_failure_at: applied.last_failure_at(),
                reconcile_interval_seconds: RUNTIME_SETTINGS_RECONCILE_INTERVAL.as_secs(),
            },
            authentication: AuthenticationSettingsReport {
                ownership: RuntimeSettingsOwnership::new(
                    record(StorageRuntimeSettingsGroup::Authentication),
                    RuntimeSettingsEffect::NextRequest,
                ),
                providers: authentication_providers,
            },
            authorization: AuthorizationSettingsReport {
                ownership: RuntimeSettingsOwnership::new(
                    record(StorageRuntimeSettingsGroup::Authorization),
                    RuntimeSettingsEffect::NextRequest,
                ),
                effective_backend: authorization_backend,
                runtime_backend: snapshot
                    .and_then(|snapshot| snapshot.authorization())
                    .map(|backend| backend.as_str().to_string()),
                deployment_backend: deployment.authorization_backend.as_str().to_string(),
                available: authorization_ready,
            },
            query_observations: QueryObservationSettingsReport {
                ownership: RuntimeSettingsOwnership::new(
                    record(StorageRuntimeSettingsGroup::QueryObservations),
                    RuntimeSettingsEffect::NextQuery,
                ),
                effective: observation_effective,
                runtime: observation_requested,
                deployment: observation_deployment,
                ceilings: deployment.query_observations.ceilings().into(),
                constrained_by_ceiling: observation_requested
                    .is_some_and(|requested| requested != observation_effective),
                collection_paused: applied.refresh_failed(),
            },
            pagination: PaginationSettingsReport {
                ownership: RuntimeSettingsOwnership::new(
                    record(StorageRuntimeSettingsGroup::Pagination),
                    RuntimeSettingsEffect::NextRequest,
                ),
                effective: PaginationPolicyValue {
                    default_page_limit: effective_pages.default_limit(),
                },
                runtime: pagination_policy.map(|policy| PaginationPolicyValue {
                    default_page_limit: policy.default_page_limit() as usize,
                }),
                deployment: PaginationPolicyValue {
                    default_page_limit: deployment.pagination.default_limit(),
                },
                max_page_limit: deployment.pagination.maximum_limit(),
                constrained_by_ceiling: pagination_policy.is_some_and(|policy| {
                    policy.default_page_limit() as usize > deployment.pagination.maximum_limit()
                }),
            },
        }
    }

    /// Revision of `group` applied by the responding replica.
    pub fn applied_revision(&self, group: RuntimeSettingsGroup) -> Option<ResourceRevision> {
        match group {
            RuntimeSettingsGroup::Authentication => self.authentication.ownership.revision,
            RuntimeSettingsGroup::Authorization => self.authorization.ownership.revision,
            RuntimeSettingsGroup::QueryObservations => self.query_observations.ownership.revision,
            RuntimeSettingsGroup::Pagination => self.pagination.ownership.revision,
        }
    }
}

/// A committed update is not a cluster-wide acknowledgement: other replicas
/// apply it on notification or within the reconcile interval.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct RuntimeSettingsUpdateResponse {
    pub group: RuntimeSettingsGroup,
    /// Revision now current in shared storage.
    pub committed_revision: ResourceRevision,
    /// False when the requested value was already current; no revision or
    /// audit record was written.
    pub changed: bool,
    /// Whether the responding replica applies at least `committed_revision`.
    pub applied_by_responding_replica: bool,
    pub settings: RuntimeSettingsReport,
}

/// One side of an audited change.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct RuntimeSettingsValueResponse {
    pub source: RuntimeSettingsSource,
    /// Managed value; null when deployment configuration owns the group.
    #[schema(value_type = Option<Object>)]
    pub value: Option<Value>,
}

impl From<&StorageRuntimeSettingsAssignment> for RuntimeSettingsValueResponse {
    fn from(value: &StorageRuntimeSettingsAssignment) -> Self {
        let document = value.to_document().map(|document| {
            if value.group() == StorageRuntimeSettingsGroup::Authentication {
                redact_authentication_document(document)
            } else {
                document
            }
        });
        Self {
            source: match value {
                StorageRuntimeSettingsAssignment::Deployment(_) => {
                    RuntimeSettingsSource::Deployment
                }
                StorageRuntimeSettingsAssignment::Managed(_) => RuntimeSettingsSource::Runtime,
            },
            value: document,
        }
    }
}

fn redact_authentication_document(mut document: Value) -> Value {
    let Some(providers) = document.get_mut("providers").and_then(Value::as_array_mut) else {
        return document;
    };
    for provider in providers {
        let Some(configuration) = provider
            .get_mut("configuration")
            .and_then(Value::as_object_mut)
        else {
            continue;
        };
        let envelope_configured = configuration
            .remove(hubuum_storage_core::RUNTIME_AUTHENTICATION_PASSWORD_ENVELOPE_FIELD)
            .is_some();
        let inline_configured = configuration.remove("bind_password").is_some();
        let bind_password_ref = configuration.remove("bind_password_ref");
        let secret_configured = envelope_configured
            || inline_configured
            || bind_password_ref
                .as_ref()
                .is_some_and(|value| !value.is_null());
        if bind_password_ref.is_some_and(|value| !value.is_null()) {
            configuration.insert(
                "bind_password_ref".to_string(),
                Value::String("<redacted>".to_string()),
            );
        }
        configuration.insert(
            "bind_password_configured".to_string(),
            Value::Bool(secret_configured),
        );
    }
    document
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct RuntimeSettingsChangeActor {
    /// `user`, `system` (for example a migration import), or `worker`.
    pub kind: String,
    pub principal_id: Option<PrincipalId>,
}

/// Durable audit record of one committed revision.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct RuntimeSettingsChangeResponse {
    pub group: RuntimeSettingsGroup,
    pub revision: ResourceRevision,
    pub previous: RuntimeSettingsValueResponse,
    pub current: RuntimeSettingsValueResponse,
    pub changed_at: DateTime<Utc>,
    pub actor: RuntimeSettingsChangeActor,
    pub request_id: Option<Uuid>,
    pub correlation_id: Option<String>,
}

impl From<StorageRuntimeSettingsChange> for RuntimeSettingsChangeResponse {
    fn from(value: StorageRuntimeSettingsChange) -> Self {
        Self {
            group: value.group().into(),
            revision: value.revision(),
            previous: value.previous().into(),
            current: value.current().into(),
            changed_at: value.changed_at(),
            actor: RuntimeSettingsChangeActor {
                kind: value.actor_kind().as_str().to_string(),
                principal_id: value.actor(),
            },
            request_id: value.request_id(),
            correlation_id: value
                .correlation_id()
                .map(|value| value.as_str().to_string()),
        }
    }
}

/// Newest-first audit records for one group.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct RuntimeSettingsHistoryResponse {
    pub changes: Vec<RuntimeSettingsChangeResponse>,
    /// Pass as `before_revision` to continue; null when no older records exist.
    pub next_before_revision: Option<ResourceRevision>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use hubuum_storage_core::{
        StorageAuthenticationProvider, StorageAuthenticationProviders,
        StorageQueryObservationCeilings,
    };
    use rstest::rstest;

    fn ceiling() -> PageLimits {
        PageLimits::new(100, 250).unwrap()
    }

    #[rstest]
    #[case::zero(0)]
    #[case::above_ceiling(251)]
    #[case::beyond_storage_range(u64::from(u32::MAX) + 1)]
    fn pagination_requests_outside_the_deployment_ceiling_are_rejected(#[case] limit: u64) {
        let request = PaginationSettingsRequest {
            expected_revision: ResourceRevision::INITIAL,
            default_page_limit: limit,
        };
        assert!(matches!(
            request.into_update(ceiling(), EventContext::system()),
            Err(ApiError::BadRequest(_))
        ));
    }

    #[rstest]
    #[case::minimum(1)]
    #[case::ceiling(250)]
    fn pagination_requests_within_the_ceiling_are_accepted(#[case] limit: u64) {
        let request = PaginationSettingsRequest {
            expected_revision: ResourceRevision::INITIAL,
            default_page_limit: limit,
        };
        let update = request
            .into_update(ceiling(), EventContext::system())
            .unwrap();
        assert_eq!(
            update.assignment().clone(),
            StorageRuntimeSettingsAssignment::Managed(StorageRuntimeSettingsValue::Pagination(
                StoragePaginationPolicy::try_new(limit).unwrap()
            ))
        );
    }

    #[test]
    fn authentication_audit_values_redact_envelopes_and_secret_references() {
        let providers = StorageAuthenticationProviders::try_new(vec![
            StorageAuthenticationProvider::try_new(
                "corporate",
                StorageAuthenticationProviderKind::Ldap,
                serde_json::json!({
                    "scope": "corporate",
                    "runtime_bind_password_envelope": {
                        "ciphertext": "ciphertext-secret"
                    },
                    "bind_password_ref": "secret-alias"
                }),
            )
            .unwrap(),
        ])
        .unwrap();
        let assignment = StorageRuntimeSettingsAssignment::Managed(
            StorageRuntimeSettingsValue::Authentication(providers),
        );
        let response = RuntimeSettingsValueResponse::from(&assignment);
        let json = serde_json::to_string(&response).unwrap();
        assert!(!json.contains("ciphertext-secret"));
        assert!(!json.contains("secret-alias"));
        assert!(json.contains("\"bind_password_configured\":true"));
    }

    #[test]
    fn query_observation_requests_manage_all_limits_and_fill_omitted_values() {
        let request = QueryObservationSettingsRequest {
            expected_revision: ResourceRevision::INITIAL,
            enabled: true,
            sample_every: Some(4),
            max_patterns: None,
            max_patterns_per_class: Some(8),
            retention_seconds: None,
            max_predicates_per_query: Some(6),
        };
        let update = request
            .into_update(
                StorageQueryObservationSettings::default(),
                EventContext::system(),
            )
            .unwrap();
        let Some(StorageRuntimeSettingsValue::QueryObservations(policy)) =
            update.assignment().value()
        else {
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
            (true, Some(4), Some(2048), Some(8), Some(86400), Some(6))
        );
    }

    #[test]
    fn query_observation_requests_must_fit_the_deployment_ceilings() {
        let ceilings = StorageQueryObservationCeilings::try_new(4, 64, 32, 3600, 16).unwrap();
        let deployment = StorageQueryObservationSettings::try_new_with_ceilings(
            false, 8, 32, 16, 1800, 8, ceilings,
        )
        .unwrap();
        let request = QueryObservationSettingsRequest {
            expected_revision: ResourceRevision::INITIAL,
            enabled: true,
            sample_every: Some(2),
            max_patterns: Some(128),
            max_patterns_per_class: Some(16),
            retention_seconds: None,
            max_predicates_per_query: None,
        };
        assert!(
            request
                .into_update(deployment, EventContext::system())
                .is_err()
        );
    }
}
