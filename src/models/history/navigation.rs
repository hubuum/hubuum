use chrono::{DateTime, Utc};
use hubuum_domain::HistoryRecordId;
use hubuum_storage_core::StorageIdentityEvidence;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::errors::ApiError;
use crate::models::ResourceRevision;
use crate::models::search::{FilterField, SortParam};
use crate::traits::{CursorPaginated, CursorValue};

/// Metadata used to navigate history without downloading object data or class schemas.
#[derive(Clone, Serialize, ToSchema)]
pub struct HistoryMarker {
    #[serde(skip)]
    #[schema(ignore)]
    pub(crate) logical_identity: Option<StorageIdentityEvidence>,
    pub(crate) id: i32,
    pub(crate) collection_id: i32,
    pub(crate) hubuum_class_id: Option<i32>,
    pub(crate) name: String,
    pub(crate) history_id: i64,
    pub(crate) revision: ResourceRevision,
    pub(crate) op: String,
    pub(crate) valid_from: DateTime<Utc>,
    pub(crate) valid_to: Option<DateTime<Utc>>,
    pub(crate) actor_id: Option<i32>,
    pub(crate) actor_kind: Option<String>,
    pub(crate) initiator_user_id: Option<i32>,
    pub(crate) task_id: Option<i32>,
}

#[derive(Clone, Copy, Default, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum HistoryDirection {
    #[default]
    Older,
    Newer,
}

impl HistoryDirection {
    pub(crate) fn sorts(self) -> Vec<SortParam> {
        let descending = matches!(self, Self::Older);
        [FilterField::ValidFrom, FilterField::HistoryId]
            .into_iter()
            .map(|field| SortParam { field, descending })
            .collect()
    }
}

/// Navigation uses entry creation times. `from` is inclusive; `until` is exclusive.
/// `at` includes timestamp ties on the older side. `anchor` excludes the entry itself.
#[derive(Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub struct HistoryTimelineParameters {
    pub(crate) from: Option<DateTime<Utc>>,
    pub(crate) until: Option<DateTime<Utc>>,
    /// Seek around this exact timestamp; mutually exclusive with anchor.
    pub(crate) at: Option<DateTime<Utc>>,
    /// Resource-scoped history entry; mutually exclusive with at.
    #[param(value_type = Option<i64>, minimum = 1)]
    pub(crate) anchor: Option<HistoryRecordId>,
    #[serde(default)]
    pub(crate) direction: HistoryDirection,
    #[param(minimum = 1)]
    pub(crate) limit: Option<usize>,
    pub(crate) cursor: Option<String>,
    /// Exact authorized count within the selected window and direction. Defaults to false.
    #[serde(default)]
    pub(crate) include_total: bool,
}

impl CursorPaginated for HistoryMarker {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(field, FilterField::ValidFrom | FilterField::HistoryId)
    }
    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        match field {
            FilterField::ValidFrom => Ok(CursorValue::DateTime(self.valid_from.naive_utc())),
            FilterField::HistoryId => Ok(CursorValue::Integer(self.history_id)),
            _ => Err(ApiError::BadRequest(
                "unsupported history navigation sort".into(),
            )),
        }
    }
    fn default_sort() -> Vec<SortParam> {
        HistoryDirection::Older.sorts()
    }
    fn tie_breaker_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::HistoryId,
            descending: true,
        }]
    }
}

impl From<&HistoryMarker> for super::HistoryAuthorizationSnapshot {
    fn from(marker: &HistoryMarker) -> Self {
        match marker.hubuum_class_id {
            Some(class_id) => Self::object(
                marker.id,
                marker.collection_id,
                class_id,
                marker.name.clone(),
            ),
            None => Self::class(marker.id, marker.collection_id, marker.name.clone()),
        }
    }
}
