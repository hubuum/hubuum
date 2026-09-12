use hubuum_domain::{ClassId, SchemaFailure, SchemaReference, SchemaRevision, TaskId};
use hubuum_storage_core::{StorageComplianceStatus, StorageSchemaActivationPolicy};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

#[derive(Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SchemaStageRequest {
    pub json_schema: Option<Value>,
    pub validate_schema: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SchemaActivationPolicy {
    RejectIncompatible,
    AllowPending,
}
impl From<SchemaActivationPolicy> for StorageSchemaActivationPolicy {
    fn from(value: SchemaActivationPolicy) -> Self {
        match value {
            SchemaActivationPolicy::RejectIncompatible => Self::RejectIncompatible,
            SchemaActivationPolicy::AllowPending => Self::AllowPending,
        }
    }
}

#[derive(Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SchemaActivationRequest {
    pub expected_active_revision: SchemaRevision,
    pub policy: SchemaActivationPolicy,
    pub impact_task_id: Option<TaskId>,
}

#[derive(Clone, Copy, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ComplianceStatus {
    Valid,
    Invalid,
    Pending,
    NotRequired,
}
impl From<ComplianceStatus> for StorageComplianceStatus {
    fn from(value: ComplianceStatus) -> Self {
        match value {
            ComplianceStatus::Valid => Self::Valid,
            ComplianceStatus::Invalid => Self::Invalid,
            ComplianceStatus::Pending => Self::Pending,
            ComplianceStatus::NotRequired => Self::NotRequired,
        }
    }
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SchemaPageRequest {
    #[serde(default)]
    pub after: i64,
    #[serde(default = "default_page_limit")]
    pub limit: usize,
    pub status: Option<ComplianceStatus>,
}
fn default_page_limit() -> usize {
    50
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SchemaRevisionStatus {
    Staged,
    Active,
    Retired,
    Abandoned,
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SchemaWorkKind {
    Impact,
    Revalidation,
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SchemaWorkStatus {
    Running,
    Failed,
    Complete,
    Cancelled,
    Superseded,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct SchemaRevisionResponse {
    pub class_id: ClassId,
    pub revision: SchemaRevision,
    pub json_schema: Option<Value>,
    pub validate_schema: bool,
    pub status: SchemaRevisionStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub created_by: Option<i32>,
    pub activated_at: Option<chrono::DateTime<chrono::Utc>>,
    pub activation_policy: Option<SchemaActivationPolicy>,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct SchemaComplianceCounts {
    pub valid: u64,
    pub invalid: u64,
    pub pending: u64,
    pub not_required: u64,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct ClassSchemaResponse {
    pub active: SchemaRevisionResponse,
    pub counts: SchemaComplianceCounts,
    pub object_epoch: u64,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct ObjectSchemaEvidence {
    pub schema: SchemaReference,
    pub object_revision: crate::models::ResourceRevision,
    pub valid: bool,
    pub validated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct ObjectComplianceResponse {
    pub object_id: i32,
    pub object_revision: crate::models::ResourceRevision,
    pub active_schema: SchemaReference,
    pub status: ComplianceStatus,
    pub evidence: Option<ObjectSchemaEvidence>,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct SchemaWorkResponse {
    pub task_id: TaskId,
    pub target: SchemaReference,
    pub kind: SchemaWorkKind,
    pub status: SchemaWorkStatus,
    pub start_epoch: u64,
    pub end_epoch: Option<u64>,
    pub upper_bound: i32,
    pub cursor: i32,
    pub examined: u64,
    pub valid: u64,
    pub invalid: u64,
    pub not_required: u64,
    pub uninspectable: u64,
    pub stale: u64,
    pub invalid_samples: Vec<i32>,
    pub elapsed_millis: u64,
    pub batches: u64,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub impact: Option<SchemaImpactResponse>,
    /// Recomputed when this report is read; activation rechecks the same boundaries.
    pub readiness: Option<SchemaImpactReadiness>,
    pub current_epoch: Option<u64>,
    pub current_active_schema: Option<SchemaReference>,
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SchemaImpactReadiness {
    Compatible,
    Incompatible,
    Inconclusive,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct SchemaImpactResponse {
    pub baseline: SchemaReference,
    pub counts: SchemaImpactCounts,
    /// At most 20 groups, counting the first failure per object.
    pub failures: Vec<SchemaFailureGroup>,
    /// Failures whose group did not fit the report limit.
    pub ungrouped_failures: u64,
}

/// Disjoint outcomes; counts sum to examined objects, including unknown comparisons.
#[derive(Serialize, Deserialize, ToSchema)]
pub struct SchemaImpactCounts {
    /// Previously valid or not required, now invalid.
    pub newly_invalid: u64,
    /// Previously invalid, now valid.
    pub newly_valid: u64,
    pub still_invalid: u64,
    pub still_valid: u64,
    /// Previously not required, now valid under enforced validation.
    pub newly_required_valid: u64,
    pub no_longer_required: u64,
    pub unchanged_not_required: u64,
    /// Either policy could not be inspected, or the object changed before commit.
    pub uninspectable: u64,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct SchemaFailureGroup {
    pub reason: SchemaFailure,
    pub objects: u64,
    /// At most five object IDs per group; values and instance paths are omitted.
    pub samples: Vec<i32>,
}

#[derive(Serialize, ToSchema)]
pub struct SchemaCompliancePage {
    pub items: Vec<ObjectComplianceResponse>,
    /// Resume after the last inspected candidate, including hidden candidates.
    pub next_after: Option<i64>,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct SchemaActivationResponse {
    pub active: SchemaRevisionResponse,
    pub task_id: Option<TaskId>,
    pub dependent_rebuild_task_id: Option<TaskId>,
}
