//! Admission for finite synchronous work, independent of durable task ownership.

use async_trait::async_trait;
use hubuum_domain::PrincipalId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    StorageError, StorageValidationError, TaskByteLimit, TaskCapacityLimit, TaskPolicyRevision,
};

/// A server-selected operation class. Resource IDs, paths and client supplied
/// priority values must never become admission classes or metric labels.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum ApiRequestClass {
    Aggregate,
    History,
    Audit,
    ImportSubmission,
    ExportSubmission,
}

impl ApiRequestClass {
    pub const ALL: [Self; 5] = [
        Self::Aggregate,
        Self::History,
        Self::Audit,
        Self::ImportSubmission,
        Self::ExportSubmission,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Aggregate => "aggregate",
            Self::History => "history",
            Self::Audit => "audit",
            Self::ImportSubmission => "import_submission",
            Self::ExportSubmission => "export_submission",
        }
    }
}

/// Finite whole-request execution allowance, rather than a renewed per-query
/// timeout. Expiry fences execution but does not establish producer termination.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "openapi", schema(value_type = u64, minimum = 1, maximum = 60000))]
pub struct ApiRequestDuration(u64);

impl ApiRequestDuration {
    pub const MAX_MILLISECONDS: u64 = 60_000;

    pub fn new(milliseconds: u64) -> Result<Self, StorageValidationError> {
        if milliseconds == 0 || milliseconds > Self::MAX_MILLISECONDS {
            return Err(StorageValidationError::invalid(
                "API request duration must be between 1 and 60000 milliseconds",
            ));
        }
        Ok(Self(milliseconds))
    }

    #[must_use]
    pub const fn milliseconds(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for ApiRequestDuration {
    type Error = StorageValidationError;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ApiRequestDuration> for u64 {
    fn from(value: ApiRequestDuration) -> Self {
        value.milliseconds()
    }
}

/// A persisted deployment policy shared by every replica. Operational requests
/// and task control do not use this capacity. Reductions drain existing owners.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ApiRequestPolicy {
    deployment: TaskCapacityLimit,
    principal: TaskCapacityLimit,
    duration: ApiRequestDuration,
    input_bytes: TaskByteLimit,
    output_bytes: TaskByteLimit,
}

/// Authoritative policy and its compare-and-replace token.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct StorageApiRequestPolicy {
    revision: TaskPolicyRevision,
    policy: ApiRequestPolicy,
}

impl StorageApiRequestPolicy {
    #[must_use]
    pub const fn new(revision: TaskPolicyRevision, policy: ApiRequestPolicy) -> Self {
        Self { revision, policy }
    }
    #[must_use]
    pub const fn revision(&self) -> TaskPolicyRevision {
        self.revision
    }
    #[must_use]
    pub const fn policy(&self) -> &ApiRequestPolicy {
        &self.policy
    }
}

/// Prospective replacement. Existing reservations retain their admitted limits.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct StorageApiRequestPolicyUpdate {
    expected_revision: TaskPolicyRevision,
    policy: ApiRequestPolicy,
}

impl StorageApiRequestPolicyUpdate {
    #[must_use]
    pub const fn new(expected_revision: TaskPolicyRevision, policy: ApiRequestPolicy) -> Self {
        Self {
            expected_revision,
            policy,
        }
    }
    #[must_use]
    pub const fn expected_revision(&self) -> TaskPolicyRevision {
        self.expected_revision
    }
    #[must_use]
    pub const fn policy(&self) -> &ApiRequestPolicy {
        &self.policy
    }
}

impl ApiRequestPolicy {
    #[must_use]
    pub fn with_deployment(mut self, value: TaskCapacityLimit) -> Self {
        self.deployment = value;
        self
    }
    #[must_use]
    pub fn with_principal(mut self, value: TaskCapacityLimit) -> Self {
        self.principal = value;
        self
    }
    #[must_use]
    pub fn with_duration(mut self, value: ApiRequestDuration) -> Self {
        self.duration = value;
        self
    }
    #[must_use]
    pub fn with_input_bytes(mut self, value: TaskByteLimit) -> Self {
        self.input_bytes = value;
        self
    }
    #[must_use]
    pub fn with_output_bytes(mut self, value: TaskByteLimit) -> Self {
        self.output_bytes = value;
        self
    }

    #[must_use]
    pub const fn deployment(&self) -> TaskCapacityLimit {
        self.deployment
    }
    #[must_use]
    pub const fn principal(&self) -> TaskCapacityLimit {
        self.principal
    }
    #[must_use]
    pub const fn duration(&self) -> ApiRequestDuration {
        self.duration
    }
    #[must_use]
    pub const fn input_bytes(&self) -> TaskByteLimit {
        self.input_bytes
    }
    #[must_use]
    pub const fn output_bytes(&self) -> TaskByteLimit {
        self.output_bytes
    }
}

impl Default for ApiRequestPolicy {
    fn default() -> Self {
        Self {
            deployment: TaskCapacityLimit::new(16).expect("bounded default capacity"),
            principal: TaskCapacityLimit::new(2).expect("bounded default capacity"),
            duration: ApiRequestDuration::new(15_000).expect("bounded default duration"),
            input_bytes: TaskByteLimit::new(16 * 1024 * 1024).expect("bounded default bytes"),
            output_bytes: TaskByteLimit::new(16 * 1024 * 1024).expect("bounded default bytes"),
        }
    }
}

/// Public outcomes carry no scope counts or other principals' workload.
/// A terminal request-size/work failure must not advertise Retry-After.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum ApiRequestRejection {
    #[serde(rename = "api_capacity_deployment")]
    Deployment,
    #[serde(rename = "api_capacity_principal")]
    Principal,
    #[serde(rename = "api_capacity_input_bytes")]
    InputBytes,
    #[serde(rename = "api_capacity_output_bytes")]
    OutputBytes,
    #[serde(rename = "api_capacity_deadline_exceeded")]
    DeadlineExceeded,
    #[serde(rename = "api_capacity_identity_unavailable")]
    IdentityUnavailable,
    #[serde(rename = "api_capacity_reconciliation_required")]
    ReconciliationRequired,
}

impl ApiRequestRejection {
    pub const ALL: [Self; 7] = [
        Self::Deployment,
        Self::Principal,
        Self::InputBytes,
        Self::OutputBytes,
        Self::DeadlineExceeded,
        Self::IdentityUnavailable,
        Self::ReconciliationRequired,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Deployment => "api_capacity_deployment",
            Self::Principal => "api_capacity_principal",
            Self::InputBytes => "api_capacity_input_bytes",
            Self::OutputBytes => "api_capacity_output_bytes",
            Self::DeadlineExceeded => "api_capacity_deadline_exceeded",
            Self::IdentityUnavailable => "api_capacity_identity_unavailable",
            Self::ReconciliationRequired => "api_capacity_reconciliation_required",
        }
    }

    #[must_use]
    pub fn from_persisted(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|reason| reason.as_str() == value)
    }

    #[must_use]
    pub const fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::Deployment
                | Self::Principal
                | Self::IdentityUnavailable
                | Self::ReconciliationRequired
        )
    }
}

/// Validated admission proof. Adapters create this only after atomically
/// charging shared capacity and capturing the principal's stable identity.
/// Every execution boundary must fence this exact owner before doing work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageApiRequestPermit {
    owner: Uuid,
    revision: TaskPolicyRevision,
    policy: ApiRequestPolicy,
}

impl StorageApiRequestPermit {
    pub fn try_new(
        owner: Uuid,
        revision: TaskPolicyRevision,
        policy: ApiRequestPolicy,
    ) -> Result<Self, StorageValidationError> {
        if owner.is_nil() {
            return Err(StorageValidationError::invalid(
                "API request owner must not be nil",
            ));
        }
        Ok(Self {
            owner,
            revision,
            policy,
        })
    }
    #[must_use]
    pub const fn owner(&self) -> Uuid {
        self.owner
    }
    #[must_use]
    pub const fn revision(&self) -> TaskPolicyRevision {
        self.revision
    }
    #[must_use]
    pub const fn policy(&self) -> &ApiRequestPolicy {
        &self.policy
    }
}

#[async_trait]
pub trait ApiRequestAdmissionStorage: Send + Sync {
    async fn api_request_policy(&self) -> Result<StorageApiRequestPolicy, StorageError>;
    async fn replace_api_request_policy(
        &self,
        update: StorageApiRequestPolicyUpdate,
    ) -> Result<StorageApiRequestPolicy, StorageError>;
    /// Charge each physical attempt, including repeated preparation for a
    /// recognized task duplicate. Task receipt accounting remains independent.
    async fn admit_api_request(
        &self,
        principal: PrincipalId,
        class: ApiRequestClass,
        input_bytes: u64,
    ) -> Result<StorageApiRequestPermit, StorageError>;

    /// First fence new work, then wait for issued work to settle before releasing
    /// occupancy. Repeated cleanup is idempotent and cannot retire another owner.
    async fn settle_api_request(&self, permit: StorageApiRequestPermit)
    -> Result<(), StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(0, false)]
    #[case(1, true)]
    #[case(60_000, true)]
    #[case(60_001, false)]
    #[case(u64::MAX, false)]
    fn duration_is_finite(#[case] milliseconds: u64, #[case] valid: bool) {
        assert_eq!(ApiRequestDuration::new(milliseconds).is_ok(), valid);
    }

    #[test]
    fn reason_codes_round_trip_without_accepting_unknown_values() {
        for reason in ApiRequestRejection::ALL {
            assert_eq!(
                ApiRequestRejection::from_persisted(reason.as_str()),
                Some(reason)
            );
        }
        assert_eq!(
            ApiRequestRejection::from_persisted("another_principal"),
            None
        );
    }

    #[rstest]
    #[case(ApiRequestRejection::InputBytes)]
    #[case(ApiRequestRejection::OutputBytes)]
    #[case(ApiRequestRejection::DeadlineExceeded)]
    fn terminal_work_failures_do_not_suggest_retry(#[case] reason: ApiRequestRejection) {
        assert!(!reason.is_retryable());
    }
}
