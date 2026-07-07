use std::str::FromStr;

use chrono::NaiveDateTime;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};
use crate::pagination::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};
use crate::schema::event_deliveries;

/// Identifier wrapper for an event delivery.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, ToSchema)]
pub struct EventDeliveryID(i64);

impl EventDeliveryID {
    pub fn new(id: i64) -> Result<Self, ApiError> {
        if id <= 0 {
            return Err(ApiError::BadRequest(format!(
                "Invalid event delivery id '{id}': must be a positive integer"
            )));
        }
        Ok(Self(id))
    }

    pub fn id(self) -> i64 {
        self.0
    }
}

impl<'de> Deserialize<'de> for EventDeliveryID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let id = <i64 as Deserialize>::deserialize(deserializer)?;
        Self::new(id).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EventDeliveryStatus {
    Pending,
    InFlight,
    Succeeded,
    Failed,
    Dead,
}

impl EventDeliveryStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InFlight => "in_flight",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Dead => "dead",
        }
    }
}

impl FromStr for EventDeliveryStatus {
    type Err = ApiError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "in_flight" => Ok(Self::InFlight),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "dead" => Ok(Self::Dead),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported event delivery status: '{value}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, Queryable, Selectable, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[diesel(table_name = event_deliveries)]
pub struct EventDelivery {
    pub id: i64,
    pub event_id: i64,
    pub subscription_id: i32,
    pub status: String,
    pub attempts: i32,
    pub next_attempt_at: NaiveDateTime,
    pub last_error: Option<String>,
    pub locked_until: Option<NaiveDateTime>,
    pub claim_token: Option<Uuid>,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct EventDeliveryUpdateResponse {
    pub delivery: EventDelivery,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default, ToSchema)]
pub struct EventDeliveryStatusCounts {
    pub total: i64,
    pub pending: i64,
    pub in_flight: i64,
    pub succeeded: i64,
    pub failed: i64,
    pub dead: i64,
    pub retryable: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct EventWorkerWakeupStats {
    pub notifications_sent: u64,
    pub notification_wakeups: u64,
    pub poll_wakeups: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct EventWorkerHealth {
    pub workers_configured: usize,
    pub batch_size: usize,
    pub poll_interval_ms: u64,
    pub lock_timeout_ms: u64,
    pub wakeups: EventWorkerWakeupStats,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct EventFanoutHealth {
    pub pending_events: i64,
    pub in_flight_events: i64,
    pub stale_claims: i64,
    pub oldest_pending_age_seconds: Option<i64>,
    pub worker: EventWorkerHealth,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct EventDeliveryQueueHealth {
    pub counts: EventDeliveryStatusCounts,
    pub stale_claims: i64,
    pub oldest_due_age_seconds: Option<i64>,
    pub worker: EventWorkerHealth,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct EventSinkDeliveryHealth {
    pub sink_id: i32,
    pub sink_name: String,
    pub sink_kind: String,
    pub sink_enabled: bool,
    pub subscription_count: i64,
    pub counts: EventDeliveryStatusCounts,
    pub stale_claims: i64,
    pub oldest_due_age_seconds: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct EventSubscriptionDeliveryHealth {
    pub subscription_id: i32,
    pub subscription_name: String,
    pub collection_id: i32,
    pub sink_id: i32,
    pub sink_name: String,
    pub sink_kind: String,
    pub subscription_enabled: bool,
    pub sink_enabled: bool,
    pub counts: EventDeliveryStatusCounts,
    pub stale_claims: i64,
    pub oldest_due_age_seconds: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct EventDeliveryHealthResponse {
    pub fanout: EventFanoutHealth,
    pub delivery: EventDeliveryQueueHealth,
    pub sinks: Vec<EventSinkDeliveryHealth>,
    pub subscriptions: Vec<EventSubscriptionDeliveryHealth>,
}

impl CursorPaginated for EventDelivery {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Status
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
                | FilterField::NextAttemptAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        match field {
            FilterField::Id => Ok(CursorValue::Integer(self.id)),
            FilterField::Status => Ok(CursorValue::String(self.status.clone())),
            FilterField::CreatedAt => Ok(CursorValue::DateTime(self.created_at)),
            FilterField::UpdatedAt => Ok(CursorValue::DateTime(self.updated_at)),
            FilterField::NextAttemptAt => Ok(CursorValue::DateTime(self.next_attempt_at)),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported sort field '{}' for event deliveries",
                field
            ))),
        }
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }
}

impl CursorSqlMapping for EventDelivery {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "event_deliveries.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Status => CursorSqlField {
                column: "event_deliveries.status",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "event_deliveries.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "event_deliveries.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::NextAttemptAt => CursorSqlField {
                column: "event_deliveries.next_attempt_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for event deliveries",
                    field
                )));
            }
        })
    }
}
