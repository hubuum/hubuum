use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use crate::models::{ComputedFieldErrorResponse, ResourceRevision};

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum HistoricalComputationMode {
    HistoricalRecomputation,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum HistoricalPersonalFields {
    Unsupported,
}

/// Identity of the stored object input; this response does not return live caches.
#[derive(Debug, Serialize, ToSchema)]
pub struct HistoricalComputedObjectSource {
    pub object_id: i32,
    pub class_id: i32,
    pub history_id: i64,
    pub revision: ResourceRevision,
    pub valid_from: DateTime<Utc>,
    pub valid_to: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct HistoricalComputedValues {
    pub mode: HistoricalComputationMode,
    pub at: DateTime<Utc>,
    pub object: HistoricalComputedObjectSource,
    pub shared: HistoricalSharedComputedFields,
    pub personal: HistoricalPersonalFields,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum HistoricalSharedComputedFields {
    Available {
        definition_history_id: i64,
        valid_from: DateTime<Utc>,
        valid_to: Option<DateTime<Utc>>,
        evaluator_version: i16,
        fields: Vec<HistoricalComputedField>,
    },
    Unavailable {
        reason: HistoricalComputedUnavailableReason,
    },
}

#[derive(Debug, Serialize, ToSchema)]
pub struct HistoricalComputedField {
    pub definition_id: i32,
    pub revision: ResourceRevision,
    pub key: String,
    pub label: String,
    pub semantics_version: i16,
    #[serde(flatten)]
    pub result: HistoricalComputedFieldResult,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum HistoricalComputedFieldResult {
    Evaluated {
        value: Value,
    },
    Error {
        error: ComputedFieldErrorResponse,
    },
    Disabled,
    Unavailable {
        reason: HistoricalComputedUnavailableReason,
    },
}

#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum HistoricalComputedUnavailableReason {
    DefinitionHistoryUnavailable,
    ClassHistoryUnavailable,
    UnsupportedEvaluatorVersion,
    UnsupportedDefinitionSemantics,
}
