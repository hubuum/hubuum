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
