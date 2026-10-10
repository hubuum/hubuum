use chrono::{DateTime, Utc};
use hubuum_domain::{ClassId, PrincipalId, ResourceId, ResourceRevision};
use hubuum_storage_core::{StorageQueryUsageDeclaration, StorageQueryUsagePattern};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum QueryUsageOperation {
    Equals,
    Gt,
    Gte,
    Lt,
    Lte,
    Between,
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum QueryUsageValueType {
    String,
    Numeric,
    Boolean,
}

/// Schema for the validated storage pattern accepted at the HTTP boundary.
#[derive(Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct QueryUsagePatternSchema {
    /// Comma-separated JSON path; at most 512 bytes and 32 segments.
    pub path: String,
    pub value_type: QueryUsageValueType,
    /// One to six operations; string and boolean support equals only.
    pub operations: Vec<QueryUsageOperation>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct QueryUsageCreateRequest {
    #[schema(value_type = QueryUsagePatternSchema)]
    pub pattern: StorageQueryUsagePattern,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct QueryUsageReplaceRequest {
    #[schema(value_type = QueryUsagePatternSchema)]
    pub pattern: StorageQueryUsagePattern,
    pub expected_revision: ResourceRevision,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct QueryUsageDeleteRequest {
    pub expected_revision: ResourceRevision,
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum QueryUsageCompatibility {
    Compatible,
    Incompatible,
    Unknown,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct QueryUsageResponse {
    pub id: ResourceId,
    pub class_id: ClassId,
    pub pattern: QueryUsagePatternSchema,
    pub revision: ResourceRevision,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub created_by: Option<PrincipalId>,
    pub updated_by: Option<PrincipalId>,
    /// Advisory assessment against the current class schema. Never affects validation.
    pub schema_compatibility: QueryUsageCompatibility,
}

impl QueryUsageResponse {
    pub fn from_record(
        record: StorageQueryUsageDeclaration,
        schema: Option<&Value>,
    ) -> Result<Self, serde_json::Error> {
        let mut snapshot = record.snapshot();
        snapshot["schema_compatibility"] =
            serde_json::to_value(record.pattern().schema_compatibility(schema))?;
        serde_json::from_value(snapshot)
    }
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct QueryUsageAnalysisRequest {
    /// Up to 32 hypothetical patterns. Analysis never records or adopts them.
    #[serde(default)]
    #[schema(value_type = Vec<QueryUsagePatternSchema>, max_items = 32)]
    pub proposed: Vec<StorageQueryUsagePattern>,
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum QueryUsageAnalysisStatus {
    Unavailable,
    InsufficientEvidence,
    Complete,
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum QueryUsageResourceOwnership {
    IndependentlyManaged,
    DeclarationManaged,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct QueryUsageResource {
    pub reference: String,
    pub ownership: QueryUsageResourceOwnership,
    pub bytes: u64,
    pub native_scan_count: Option<u64>,
    pub declaration_owners: Option<u64>,
    pub cleanup_eligible_if_removed: bool,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct QueryUsageAssessment {
    pub declaration_id: Option<ResourceId>,
    pub pattern: QueryUsagePatternSchema,
    pub schema_compatibility: QueryUsageCompatibility,
    pub can_prepare: bool,
    pub resources: Vec<QueryUsageResource>,
    pub adapter_progress: Option<QueryUsageAdapterProgress>,
    pub rationale: String,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct QueryPatternObservation {
    pub pattern: QueryUsagePatternSchema,
    pub sampled_queries: u64,
    pub first_observed_at: DateTime<Utc>,
    pub last_observed_at: DateTime<Utc>,
    /// Whole storage request duration, not the cost of this predicate.
    pub whole_query_duration_micros: u64,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct QueryObservationSettings {
    pub enabled: bool,
    pub sample_every: u32,
    pub max_patterns: usize,
    pub max_patterns_per_class: usize,
    pub retention_seconds: u32,
    pub max_predicates_per_query: usize,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct QueryObservationSnapshot {
    pub source: String,
    pub settings: QueryObservationSettings,
    pub process_started_at: DateTime<Utc>,
    pub captured_at: DateTime<Utc>,
    pub patterns: Vec<QueryPatternObservation>,
    pub capacity_drops: u64,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct QueryUsageSuggestion {
    pub proposed: QueryUsagePatternSchema,
    pub observations: Vec<QueryPatternObservation>,
    pub rationale: String,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct QueryUsageAnalysisResponse {
    pub status: QueryUsageAnalysisStatus,
    pub assessed_at: DateTime<Utc>,
    pub observations: QueryObservationSnapshot,
    pub assessments: Vec<QueryUsageAssessment>,
    pub suggestions: Vec<QueryUsageSuggestion>,
    pub limitations: Vec<String>,
}

/// Backend-owned operational facts; state names are not portable promises.
#[derive(Serialize, Deserialize, ToSchema)]
pub struct QueryUsageAdapterProgress {
    pub reference: String,
    pub state: String,
    pub declaration_owners: u64,
    pub last_error: Option<String>,
    pub updated_at: DateTime<Utc>,
}
