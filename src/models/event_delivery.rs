use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};
use crate::pagination::{CursorPaginated, CursorValue};

pub use hubuum_domain::{EventDeliveryId as EventDeliveryID, EventDeliveryStatus};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct EventDeliveryResponse {
    pub id: i64,
    pub event_id: i64,
    pub subscription_id: i32,
    pub status: String,
    pub attempts: i32,
    pub next_attempt_at: NaiveDateTime,
    pub last_error: Option<String>,
    pub locked_until: Option<NaiveDateTime>,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

impl CursorPaginated for EventDeliveryResponse {
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct EventDeliveryUpdateResponse {
    pub delivery: EventDeliveryResponse,
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

impl EventDeliveryStatusCounts {
    pub(crate) fn from_storage(snapshot: crate::storage::EventDeliveryStatusSnapshot) -> Self {
        Self {
            total: snapshot.total(),
            pending: snapshot.pending(),
            in_flight: snapshot.in_flight(),
            succeeded: snapshot.succeeded(),
            failed: snapshot.failed(),
            dead: snapshot.dead(),
            retryable: snapshot.retryable(),
        }
    }
}

impl EventDeliveryHealthResponse {
    pub(crate) fn from_storage(
        snapshot: crate::storage::EventDeliveryHealthSnapshot,
        fanout_worker: EventWorkerHealth,
        delivery_worker: EventWorkerHealth,
    ) -> Self {
        let fanout = snapshot.fanout();
        let delivery = snapshot.delivery();
        Self {
            fanout: EventFanoutHealth {
                pending_events: fanout.pending_events(),
                in_flight_events: fanout.in_flight_events(),
                stale_claims: fanout.stale_claims(),
                oldest_pending_age_seconds: fanout.oldest_pending_age_seconds(),
                worker: fanout_worker,
            },
            delivery: EventDeliveryQueueHealth {
                counts: EventDeliveryStatusCounts::from_storage(delivery.counts()),
                stale_claims: delivery.stale_claims(),
                oldest_due_age_seconds: delivery.oldest_due_age_seconds(),
                worker: delivery_worker,
            },
            sinks: snapshot
                .sinks()
                .iter()
                .map(|snapshot| {
                    let sink = snapshot.sink();
                    let queue = snapshot.queue();
                    EventSinkDeliveryHealth {
                        sink_id: sink.id(),
                        sink_name: sink.name().to_string(),
                        sink_kind: sink.kind().to_string(),
                        sink_enabled: sink.enabled(),
                        subscription_count: snapshot.subscription_count(),
                        counts: EventDeliveryStatusCounts::from_storage(queue.counts()),
                        stale_claims: queue.stale_claims(),
                        oldest_due_age_seconds: queue.oldest_due_age_seconds(),
                    }
                })
                .collect(),
            subscriptions: snapshot
                .subscriptions()
                .iter()
                .map(|snapshot| {
                    let sink = snapshot.sink();
                    let queue = snapshot.queue();
                    EventSubscriptionDeliveryHealth {
                        subscription_id: snapshot.id(),
                        subscription_name: snapshot.name().to_string(),
                        collection_id: snapshot.collection_id(),
                        sink_id: sink.id(),
                        sink_name: sink.name().to_string(),
                        sink_kind: sink.kind().to_string(),
                        subscription_enabled: snapshot.enabled(),
                        sink_enabled: sink.enabled(),
                        counts: EventDeliveryStatusCounts::from_storage(queue.counts()),
                        stale_claims: queue.stale_claims(),
                        oldest_due_age_seconds: queue.oldest_due_age_seconds(),
                    }
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hubuum_storage_core::{
        EventDeliveryHealthSnapshot, EventDeliveryStatusSnapshot, EventFanoutSnapshot,
        EventQueueSnapshot, EventSinkHealthSnapshot, EventSinkSnapshot,
        EventSubscriptionHealthSnapshot,
    };

    fn worker_health(workers_configured: usize) -> EventWorkerHealth {
        EventWorkerHealth {
            workers_configured,
            batch_size: 25,
            poll_interval_ms: 500,
            lock_timeout_ms: 5_000,
            wakeups: EventWorkerWakeupStats {
                notifications_sent: 2,
                notification_wakeups: 3,
                poll_wakeups: 5,
            },
        }
    }

    #[test]
    fn storage_health_snapshot_projects_into_the_existing_api_shape() {
        let counts = EventDeliveryStatusSnapshot::new(28, 2, 3, 5, 7, 11, 13);
        let queue = EventQueueSnapshot::new(counts, 17, Some(19));
        let sink = EventSinkSnapshot::new(23, "primary".to_string(), "webhook".to_string(), true);
        let snapshot = EventDeliveryHealthSnapshot::new(
            EventFanoutSnapshot::new(29, 31, 37, Some(41)),
            queue,
            vec![EventSinkHealthSnapshot::new(sink.clone(), 43, queue)],
            vec![EventSubscriptionHealthSnapshot::new(
                47,
                "changes".to_string(),
                53,
                false,
                sink,
                queue,
            )],
        );

        let response =
            EventDeliveryHealthResponse::from_storage(snapshot, worker_health(2), worker_health(3));

        assert_eq!(
            response,
            EventDeliveryHealthResponse {
                fanout: EventFanoutHealth {
                    pending_events: 29,
                    in_flight_events: 31,
                    stale_claims: 37,
                    oldest_pending_age_seconds: Some(41),
                    worker: worker_health(2),
                },
                delivery: EventDeliveryQueueHealth {
                    counts: EventDeliveryStatusCounts {
                        total: 28,
                        pending: 2,
                        in_flight: 3,
                        succeeded: 5,
                        failed: 7,
                        dead: 11,
                        retryable: 13,
                    },
                    stale_claims: 17,
                    oldest_due_age_seconds: Some(19),
                    worker: worker_health(3),
                },
                sinks: vec![EventSinkDeliveryHealth {
                    sink_id: 23,
                    sink_name: "primary".to_string(),
                    sink_kind: "webhook".to_string(),
                    sink_enabled: true,
                    subscription_count: 43,
                    counts: EventDeliveryStatusCounts {
                        total: 28,
                        pending: 2,
                        in_flight: 3,
                        succeeded: 5,
                        failed: 7,
                        dead: 11,
                        retryable: 13,
                    },
                    stale_claims: 17,
                    oldest_due_age_seconds: Some(19),
                }],
                subscriptions: vec![EventSubscriptionDeliveryHealth {
                    subscription_id: 47,
                    subscription_name: "changes".to_string(),
                    collection_id: 53,
                    sink_id: 23,
                    sink_name: "primary".to_string(),
                    sink_kind: "webhook".to_string(),
                    subscription_enabled: false,
                    sink_enabled: true,
                    counts: EventDeliveryStatusCounts {
                        total: 28,
                        pending: 2,
                        in_flight: 3,
                        succeeded: 5,
                        failed: 7,
                        dead: 11,
                        retryable: 13,
                    },
                    stale_claims: 17,
                    oldest_due_age_seconds: Some(19),
                }],
            }
        );
    }
}
