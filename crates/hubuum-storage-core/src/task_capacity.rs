//! Shared task policy and restore-isolated accounting. Adapters own atomic
//! enforcement; process-local configuration is only an initialization input.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    StorageError, StorageTaskKind, StorageValidationError, TaskSchedulingPolicy,
    TaskSchedulingSnapshot,
};

/// Positive capacity representable by every supported persistence adapter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "openapi", schema(value_type = u64, minimum = 1, maximum = 9223372036854775807_i64))]
pub struct TaskCapacityLimit(u64);

impl TaskCapacityLimit {
    pub fn new(value: u64) -> Result<Self, StorageValidationError> {
        if value == 0 || value > i64::MAX as u64 {
            return Err(StorageValidationError::invalid(
                "Task capacity must be between 1 and 9223372036854775807",
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for TaskCapacityLimit {
    type Error = StorageValidationError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<TaskCapacityLimit> for u64 {
    fn from(value: TaskCapacityLimit) -> Self {
        value.get()
    }
}

/// Positive byte ceiling, distinct from a number of tasks or execution slots.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "openapi", schema(value_type = u64, minimum = 1, maximum = 9223372036854775807_i64))]
pub struct TaskByteLimit(u64);

impl TaskByteLimit {
    pub fn new(bytes: u64) -> Result<Self, StorageValidationError> {
        if bytes == 0 || bytes > i64::MAX as u64 {
            return Err(StorageValidationError::invalid(
                "Task byte limit must be between 1 and 9223372036854775807",
            ));
        }
        Ok(Self(bytes))
    }

    #[must_use]
    pub const fn bytes(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for TaskByteLimit {
    type Error = StorageValidationError;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<TaskByteLimit> for u64 {
    fn from(value: TaskByteLimit) -> Self {
        value.bytes()
    }
}

/// Optimistic concurrency token for a committed shared policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "openapi", schema(value_type = u64, minimum = 1, maximum = 9223372036854775807_i64))]
pub struct TaskPolicyRevision(u64);

impl TaskPolicyRevision {
    pub fn new(value: u64) -> Result<Self, StorageValidationError> {
        TaskCapacityLimit::new(value).map(|value| Self(value.get()))
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn next(self) -> Result<Self, StorageValidationError> {
        Self::new(self.0 + 1)
    }
}

impl TryFrom<u64> for TaskPolicyRevision {
    type Error = StorageValidationError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<TaskPolicyRevision> for u64 {
    fn from(value: TaskPolicyRevision) -> Self {
        value.get()
    }
}

/// Independent ceilings for one admission scope. Reductions drain existing
/// work; a lower outstanding limit does not revoke existing execution claims.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct TaskScopeCapacity {
    outstanding: TaskCapacityLimit,
    executing: TaskCapacityLimit,
    resident_bytes: TaskByteLimit,
}

impl TaskScopeCapacity {
    #[must_use]
    pub const fn new(
        outstanding: TaskCapacityLimit,
        executing: TaskCapacityLimit,
        resident_bytes: TaskByteLimit,
    ) -> Self {
        Self {
            outstanding,
            executing,
            resident_bytes,
        }
    }

    #[must_use]
    pub const fn outstanding(self) -> TaskCapacityLimit {
        self.outstanding
    }

    #[must_use]
    pub const fn executing(self) -> TaskCapacityLimit {
        self.executing
    }

    #[must_use]
    pub const fn resident_bytes(self) -> TaskByteLimit {
        self.resident_bytes
    }
}

/// Kind-wide limits and the compatibility per-principal admission ceiling.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct TaskKindCapacity {
    kind: StorageTaskKind,
    capacity: TaskScopeCapacity,
    principal_outstanding: TaskCapacityLimit,
}

impl TaskKindCapacity {
    #[must_use]
    pub const fn new(
        kind: StorageTaskKind,
        capacity: TaskScopeCapacity,
        principal_outstanding: TaskCapacityLimit,
    ) -> Self {
        Self {
            kind,
            capacity,
            principal_outstanding,
        }
    }

    #[must_use]
    pub const fn kind(self) -> StorageTaskKind {
        self.kind
    }

    #[must_use]
    pub const fn capacity(self) -> TaskScopeCapacity {
        self.capacity
    }

    #[must_use]
    pub const fn principal_outstanding(self) -> TaskCapacityLimit {
        self.principal_outstanding
    }
}

/// Complete deployment policy. No member binds an integer database identity.
/// Task owners and collection membership are captured separately at admission.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "PolicyWire")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct TaskAdmissionPolicy {
    scheduling: TaskSchedulingPolicy,
    deployment: TaskScopeCapacity,
    principal: TaskScopeCapacity,
    collection: TaskScopeCapacity,
    internal: TaskScopeCapacity,
    kinds: [TaskKindCapacity; 7],
    cumulative_input_bytes_per_task: TaskByteLimit,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyWire {
    scheduling: TaskSchedulingPolicy,
    deployment: TaskScopeCapacity,
    principal: TaskScopeCapacity,
    collection: TaskScopeCapacity,
    internal: TaskScopeCapacity,
    kinds: [TaskKindCapacity; 7],
    cumulative_input_bytes_per_task: TaskByteLimit,
}

impl TryFrom<PolicyWire> for TaskAdmissionPolicy {
    type Error = StorageValidationError;

    fn try_from(wire: PolicyWire) -> Result<Self, Self::Error> {
        for kind in StorageTaskKind::ALL {
            if wire.kinds.iter().filter(|entry| entry.kind == kind).count() != 1 {
                return Err(StorageValidationError::invalid(
                    "Task policy must contain every task kind exactly once",
                ));
            }
        }
        Ok(Self {
            scheduling: wire.scheduling,
            deployment: wire.deployment,
            principal: wire.principal,
            collection: wire.collection,
            internal: wire.internal,
            kinds: wire.kinds,
            cumulative_input_bytes_per_task: wire.cumulative_input_bytes_per_task,
        })
    }
}

impl Default for TaskAdmissionPolicy {
    fn default() -> Self {
        fn limit(value: u64) -> TaskCapacityLimit {
            TaskCapacityLimit::new(value).expect("default capacity is positive and bounded")
        }
        fn scope(outstanding: u64, executing: u64, bytes: u64) -> TaskScopeCapacity {
            TaskScopeCapacity::new(
                limit(outstanding),
                limit(executing),
                TaskByteLimit::new(bytes).expect("valid default byte limit"),
            )
        }
        Self {
            scheduling: TaskSchedulingPolicy::default(),
            deployment: scope(10_000, 64, 4 * 1024 * 1024 * 1024),
            principal: scope(500, 16, 1024 * 1024 * 1024),
            collection: scope(1_000, 32, 2 * 1024 * 1024 * 1024),
            internal: scope(10_000, 64, 1024 * 1024 * 1024),
            kinds: StorageTaskKind::ALL.map(|kind| {
                TaskKindCapacity::new(
                    kind,
                    scope(5_000, 64, 4 * 1024 * 1024 * 1024),
                    limit(match kind {
                        StorageTaskKind::Backup => 1,
                        StorageTaskKind::SchemaProfiling => 2,
                        _ => 100,
                    }),
                )
            }),
            cumulative_input_bytes_per_task: TaskByteLimit::new(1024 * 1024 * 1024 * 1024)
                .expect("valid default byte limit"),
        }
    }
}

impl TaskAdmissionPolicy {
    #[must_use]
    pub const fn scheduling(&self) -> &TaskSchedulingPolicy {
        &self.scheduling
    }
    #[must_use]
    pub fn with_scheduling(mut self, policy: TaskSchedulingPolicy) -> Self {
        self.scheduling = policy;
        self
    }
    #[must_use]
    pub const fn deployment(&self) -> TaskScopeCapacity {
        self.deployment
    }
    #[must_use]
    pub const fn principal(&self) -> TaskScopeCapacity {
        self.principal
    }
    #[must_use]
    pub const fn collection(&self) -> TaskScopeCapacity {
        self.collection
    }
    #[must_use]
    pub const fn internal(&self) -> TaskScopeCapacity {
        self.internal
    }
    #[must_use]
    pub fn for_kind(&self, kind: StorageTaskKind) -> TaskKindCapacity {
        *self
            .kinds
            .iter()
            .find(|entry| entry.kind == kind)
            .expect("validated complete policy")
    }
    #[must_use]
    pub const fn cumulative_input_bytes_per_task(&self) -> TaskByteLimit {
        self.cumulative_input_bytes_per_task
    }
    #[must_use]
    pub fn with_deployment(mut self, capacity: TaskScopeCapacity) -> Self {
        self.deployment = capacity;
        self
    }
    #[must_use]
    pub fn with_principal(mut self, capacity: TaskScopeCapacity) -> Self {
        self.principal = capacity;
        self
    }
    #[must_use]
    pub fn with_collection(mut self, capacity: TaskScopeCapacity) -> Self {
        self.collection = capacity;
        self
    }
    #[must_use]
    pub fn with_internal(mut self, capacity: TaskScopeCapacity) -> Self {
        self.internal = capacity;
        self
    }
    #[must_use]
    pub fn with_kind(mut self, capacity: TaskKindCapacity) -> Self {
        *self
            .kinds
            .iter_mut()
            .find(|entry| entry.kind == capacity.kind)
            .expect("validated complete policy") = capacity;
        self
    }
    #[must_use]
    pub fn with_cumulative_input_bytes_per_task(mut self, limit: TaskByteLimit) -> Self {
        self.cumulative_input_bytes_per_task = limit;
        self
    }
}

/// Safe public reason codes. Scope counts and other owners never accompany a
/// rejection; administrator diagnostics are a separate capability operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskCapacityRejection {
    Deployment,
    Principal,
    Kind,
    PrincipalKind,
    Collection,
    Internal,
    ResidentBytes,
    CumulativeWork,
    IdentityUnavailable,
    ReconciliationRequired,
}

impl TaskCapacityRejection {
    pub const ALL: [Self; 10] = [
        Self::Deployment,
        Self::Principal,
        Self::Kind,
        Self::PrincipalKind,
        Self::Collection,
        Self::Internal,
        Self::ResidentBytes,
        Self::CumulativeWork,
        Self::IdentityUnavailable,
        Self::ReconciliationRequired,
    ];

    #[must_use]
    pub fn from_persisted(code: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|reason| reason.as_str() == code)
    }

    #[must_use]
    pub const fn is_unavailable(self) -> bool {
        matches!(
            self,
            Self::IdentityUnavailable | Self::ReconciliationRequired
        )
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Deployment => "task_capacity_deployment",
            Self::Principal => "task_capacity_principal",
            Self::Kind => "task_capacity_kind",
            Self::PrincipalKind => "task_capacity_principal_kind",
            Self::Collection => "task_capacity_collection",
            Self::Internal => "task_capacity_internal",
            Self::ResidentBytes => "task_capacity_resident_bytes",
            Self::CumulativeWork => "task_capacity_cumulative_work",
            Self::IdentityUnavailable => "task_capacity_identity_unavailable",
            Self::ReconciliationRequired => "task_capacity_reconciliation_required",
        }
    }
}

/// A committed policy, including its optimistic update token.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct StorageTaskPolicy {
    revision: TaskPolicyRevision,
    policy: TaskAdmissionPolicy,
}

impl StorageTaskPolicy {
    #[must_use]
    pub const fn new(revision: TaskPolicyRevision, policy: TaskAdmissionPolicy) -> Self {
        Self { revision, policy }
    }
    #[must_use]
    pub const fn revision(&self) -> TaskPolicyRevision {
        self.revision
    }
    #[must_use]
    pub const fn policy(&self) -> &TaskAdmissionPolicy {
        &self.policy
    }
}

/// Compare-and-replace request; a stale revision cannot silently overwrite a
/// concurrent administrator's policy change.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct StorageTaskPolicyUpdate {
    expected_revision: TaskPolicyRevision,
    policy: TaskAdmissionPolicy,
}

impl StorageTaskPolicyUpdate {
    #[must_use]
    pub const fn new(expected_revision: TaskPolicyRevision, policy: TaskAdmissionPolicy) -> Self {
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
    pub const fn policy(&self) -> &TaskAdmissionPolicy {
        &self.policy
    }
}

/// Aggregate administrator diagnostics. These values never appear in public
/// capacity rejection responses. Cumulative charges include detached owners.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct StorageTaskCapacityUsage {
    outstanding: u64,
    executing: u64,
    resident_bytes: u64,
    cumulative_input_bytes: u64,
    cumulative_output_bytes: u64,
    cumulative_items: u64,
}

impl StorageTaskCapacityUsage {
    #[must_use]
    pub const fn with_current(
        mut self,
        outstanding: u64,
        executing: u64,
        resident_bytes: u64,
    ) -> Self {
        self.outstanding = outstanding;
        self.executing = executing;
        self.resident_bytes = resident_bytes;
        self
    }
    #[must_use]
    pub const fn with_cumulative(
        mut self,
        input_bytes: u64,
        output_bytes: u64,
        items: u64,
    ) -> Self {
        self.cumulative_input_bytes = input_bytes;
        self.cumulative_output_bytes = output_bytes;
        self.cumulative_items = items;
        self
    }
    #[must_use]
    pub const fn outstanding(self) -> u64 {
        self.outstanding
    }
    #[must_use]
    pub const fn executing(self) -> u64 {
        self.executing
    }
    #[must_use]
    pub const fn resident_bytes(self) -> u64 {
        self.resident_bytes
    }
    #[must_use]
    pub const fn cumulative_input_bytes(self) -> u64 {
        self.cumulative_input_bytes
    }
    #[must_use]
    pub const fn cumulative_output_bytes(self) -> u64 {
        self.cumulative_output_bytes
    }
    #[must_use]
    pub const fn cumulative_items(self) -> u64 {
        self.cumulative_items
    }
}

/// One consistent view of the policy, maintenance generation and accounting.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct StorageTaskCapacitySnapshot {
    scheduling: TaskSchedulingSnapshot,
    policy: StorageTaskPolicy,
    generation: u64,
    ordinary: StorageTaskCapacityUsage,
    internal: StorageTaskCapacityUsage,
    blocked_owners: u64,
    work_budget_blocked: u64,
}

impl StorageTaskCapacitySnapshot {
    #[must_use]
    pub fn new(
        policy: StorageTaskPolicy,
        generation: u64,
        scheduling: TaskSchedulingSnapshot,
    ) -> Self {
        Self {
            scheduling,
            policy,
            generation,
            ordinary: StorageTaskCapacityUsage::default(),
            internal: StorageTaskCapacityUsage::default(),
            blocked_owners: 0,
            work_budget_blocked: 0,
        }
    }
    #[must_use]
    pub const fn scheduling(&self) -> &TaskSchedulingSnapshot {
        &self.scheduling
    }
    #[must_use]
    pub const fn with_usage(
        mut self,
        ordinary: StorageTaskCapacityUsage,
        internal: StorageTaskCapacityUsage,
    ) -> Self {
        self.ordinary = ordinary;
        self.internal = internal;
        self
    }
    #[must_use]
    pub const fn with_blocked_owners(mut self, count: u64) -> Self {
        self.blocked_owners = count;
        self
    }
    #[must_use]
    pub const fn with_work_budget_blocked(mut self, count: u64) -> Self {
        self.work_budget_blocked = count;
        self
    }
    #[must_use]
    pub const fn work_budget_blocked(&self) -> u64 {
        self.work_budget_blocked
    }
    #[must_use]
    pub const fn policy(&self) -> &StorageTaskPolicy {
        &self.policy
    }
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }
    #[must_use]
    pub const fn ordinary(&self) -> StorageTaskCapacityUsage {
        self.ordinary
    }
    #[must_use]
    pub const fn internal(&self) -> StorageTaskCapacityUsage {
        self.internal
    }
    #[must_use]
    pub const fn blocked_owners(&self) -> u64 {
        self.blocked_owners
    }
}

#[async_trait]
pub trait TaskCapacityStorage: Send + Sync {
    /// Initialize once. Concurrent replicas receive the winning persisted
    /// policy, even when they propose different configuration values.
    async fn initialize_task_policy(
        &self,
        policy: TaskAdmissionPolicy,
    ) -> Result<StorageTaskPolicy, StorageError>;
    async fn task_policy(&self) -> Result<StorageTaskPolicy, StorageError>;
    /// Administrator-only diagnostics; adapters return a consistent snapshot.
    async fn task_capacity_snapshot(&self) -> Result<StorageTaskCapacitySnapshot, StorageError>;
    /// A reduction is prospective: existing work drains without preemption.
    async fn replace_task_policy(
        &self,
        update: StorageTaskPolicyUpdate,
    ) -> Result<StorageTaskPolicy, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case::zero(0)]
    #[case::database_overflow(i64::MAX as u64 + 1)]
    #[case::unsigned_maximum(u64::MAX)]
    fn deserialization_cannot_bypass_capacity_validation(#[case] value: u64) {
        assert!(serde_json::from_value::<TaskCapacityLimit>(value.into()).is_err());
    }

    #[test]
    fn policy_rejects_a_duplicate_kind_even_with_the_right_number_of_entries() {
        let mut value = serde_json::to_value(TaskAdmissionPolicy::default()).unwrap();
        value["kinds"][1]["kind"] = value["kinds"][0]["kind"].clone();
        assert!(serde_json::from_value::<TaskAdmissionPolicy>(value).is_err());
    }

    #[test]
    fn policy_roundtrip_preserves_the_complete_contract() {
        let policy = TaskAdmissionPolicy::default();
        let value = serde_json::to_value(&policy).unwrap();
        assert_eq!(
            serde_json::from_value::<TaskAdmissionPolicy>(value).unwrap(),
            policy
        );
    }

    #[test]
    fn exhausted_policy_revision_fails_without_wrapping() {
        assert!(
            TaskPolicyRevision::new(i64::MAX as u64)
                .unwrap()
                .next()
                .is_err()
        );
    }
}
