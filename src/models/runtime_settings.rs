//! API representation of database-managed runtime settings.
use chrono::{DateTime, Utc};
use hubuum_domain::{PrincipalId, ResourceRevision};
use hubuum_storage_core::{
    StoragePaginationPolicy, StorageQueryObservationPolicy, StorageQueryObservationSettings,
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
    QueryObservations,
    Pagination,
}

impl From<RuntimeSettingsGroup> for StorageRuntimeSettingsGroup {
    fn from(value: RuntimeSettingsGroup) -> Self {
        match value {
            RuntimeSettingsGroup::QueryObservations => Self::QueryObservations,
            RuntimeSettingsGroup::Pagination => Self::Pagination,
        }
    }
}

impl From<StorageRuntimeSettingsGroup> for RuntimeSettingsGroup {
    fn from(value: StorageRuntimeSettingsGroup) -> Self {
        match value {
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
}

impl QueryObservationSettingsRequest {
    pub fn into_update(self, context: EventContext) -> StorageRuntimeSettingsUpdate {
        StorageRuntimeSettingsUpdate::new(
            StorageRuntimeSettingsAssignment::Managed(
                StorageRuntimeSettingsValue::QueryObservations(StorageQueryObservationPolicy::new(
                    self.enabled,
                )),
            ),
            self.expected_revision,
            context,
        )
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
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct QueryObservationSettingsReport {
    #[serde(flatten)]
    pub ownership: RuntimeSettingsOwnership,
    pub effective: QueryObservationPolicyValue,
    pub runtime: Option<QueryObservationPolicyValue>,
    pub deployment: QueryObservationPolicyValue,
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

/// Deployment values that runtime settings override on this replica.
#[derive(Clone, Copy, Debug)]
pub struct RuntimeSettingsDeployment {
    pub query_observations: StorageQueryObservationSettings,
    pub pagination: PageLimits,
}

/// Runtime-managed settings as applied by the responding replica.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct RuntimeSettingsReport {
    pub replica: RuntimeSettingsReplicaStatus,
    pub query_observations: QueryObservationSettingsReport,
    pub pagination: PaginationSettingsReport,
}

impl RuntimeSettingsReport {
    pub(crate) fn new(
        applied: &AppliedRuntimeSettings,
        deployment: RuntimeSettingsDeployment,
        now: DateTime<Utc>,
    ) -> Self {
        let snapshot = applied.snapshot();
        let record = |group| snapshot.map(|snapshot| snapshot.record(group));
        let observation_runtime = snapshot
            .and_then(|snapshot| snapshot.query_observations())
            .map(|policy| QueryObservationPolicyValue {
                enabled: policy.enabled(),
            });
        let observation_deployment = QueryObservationPolicyValue {
            enabled: deployment.query_observations.enabled(),
        };
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
            query_observations: QueryObservationSettingsReport {
                ownership: RuntimeSettingsOwnership::new(
                    record(StorageRuntimeSettingsGroup::QueryObservations),
                    RuntimeSettingsEffect::NextQuery,
                ),
                effective: observation_runtime.unwrap_or(observation_deployment),
                runtime: observation_runtime,
                deployment: observation_deployment,
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

impl From<StorageRuntimeSettingsAssignment> for RuntimeSettingsValueResponse {
    fn from(value: StorageRuntimeSettingsAssignment) -> Self {
        Self {
            source: match value {
                StorageRuntimeSettingsAssignment::Deployment(_) => {
                    RuntimeSettingsSource::Deployment
                }
                StorageRuntimeSettingsAssignment::Managed(_) => RuntimeSettingsSource::Runtime,
            },
            value: value.to_document(),
        }
    }
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
            update.assignment(),
            StorageRuntimeSettingsAssignment::Managed(StorageRuntimeSettingsValue::Pagination(
                StoragePaginationPolicy::try_new(limit).unwrap()
            ))
        );
    }
}
