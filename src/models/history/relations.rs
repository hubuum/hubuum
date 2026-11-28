use chrono::{DateTime, Utc};
use hubuum_storage_core::StorageIdentityEvidence;
use serde::Serialize;
use utoipa::ToSchema;

use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};
use crate::models::{HubuumClassRelation, HubuumObjectRelation, ResourceRevision};
use crate::traits::{CursorPaginated, CursorValue};

#[derive(Clone, Serialize, ToSchema)]
#[serde(tag = "kind", content = "relation", rename_all = "snake_case")]
pub enum HistoricalRelationValue {
    Class(HubuumClassRelation),
    Object(HubuumObjectRelation),
}

/// IDs refer to historical membership; no current names or computed data are expanded.
#[derive(Clone, Serialize, ToSchema)]
pub struct HistoricalRelationEndpoint {
    pub(crate) id: i32,
    pub(crate) hubuum_class_id: Option<i32>,
    pub(crate) collection_id: i32,
    pub(crate) name: String,
    pub(crate) history_id: i64,
    pub(crate) revision: ResourceRevision,
    pub(crate) valid_from: DateTime<Utc>,
    pub(crate) valid_to: Option<DateTime<Utc>>,
}

#[derive(Clone, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum HistoricalEndpointUnavailableReason {
    HistoryUnavailable,
}

#[derive(Clone, Serialize, ToSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum HistoricalEndpointContext {
    Available {
        snapshot: HistoricalRelationEndpoint,
    },
    Unavailable {
        reason: HistoricalEndpointUnavailableReason,
    },
}

/// Relationship versions and endpoint context resolved at `context_at`.
#[derive(Clone, Serialize, ToSchema)]
pub struct HistoricalRelation {
    #[serde(skip)]
    #[schema(ignore)]
    pub(crate) logical_identity: Option<StorageIdentityEvidence>,
    #[serde(flatten)]
    pub(crate) value: HistoricalRelationValue,
    pub(crate) history_id: i64,
    pub(crate) revision: ResourceRevision,
    pub(crate) op: String,
    pub(crate) valid_from: DateTime<Utc>,
    pub(crate) valid_to: Option<DateTime<Utc>>,
    pub(crate) actor_id: Option<i32>,
    pub(crate) actor_kind: Option<String>,
    pub(crate) initiator_user_id: Option<i32>,
    pub(crate) task_id: Option<i32>,
    pub(crate) context_at: DateTime<Utc>,
    pub(crate) from: HistoricalEndpointContext,
    pub(crate) to: HistoricalEndpointContext,
}

impl CursorPaginated for HistoricalRelation {
    fn supports_sort(field: &FilterField) -> bool {
        *field == FilterField::HistoryId
    }
    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        if Self::supports_sort(field) {
            Ok(CursorValue::Integer(self.history_id))
        } else {
            Err(ApiError::BadRequest(
                "Relationship history supports only history_id ordering".into(),
            ))
        }
    }
    fn default_sort() -> Vec<SortParam> {
        vec![SortParam::new(FilterField::HistoryId, true)]
    }
    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}
