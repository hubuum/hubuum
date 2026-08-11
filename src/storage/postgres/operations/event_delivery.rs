use std::collections::HashMap;
use std::fmt;
use std::time::Duration as StdDuration;

use crate::storage::postgres::operations::maintenance::maintenance_state_conn;
use crate::storage::postgres::prelude::*;
use chrono::{NaiveDateTime, Utc};
use diesel::sql_types::{Nullable, Timestamp};
use uuid::Uuid;

use crate::errors::ApiError;
use crate::events::{EntityType, Event, EventDeliverySettings};
#[cfg(feature = "integration-test-support")]
use crate::models::EventDeliveryResponse;
use crate::models::search::{FilterField, Operator, ParsedQueryParamExt, QueryOptions, SortParam};
use crate::models::{
    EventDeliveryID, EventDeliveryStatus, EventSink, EventSubscription, redacted_debug_option,
};
use crate::pagination::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};
use crate::storage::postgres::{with_connection, with_transaction};
use crate::storage::{
    EventDeliveryBatch, EventDeliveryClaim, EventDeliverySink, EventDeliverySubscription,
    EventDeliveryWorkItem,
};

use super::event_record::EventRow;
use super::event_subscription::{EventSinkRow, EventSubscriptionRow};

#[derive(Clone, Queryable, Selectable, PartialEq, Eq)]
#[diesel(table_name = crate::schema::event_deliveries)]
pub(crate) struct EventDeliveryRow {
    pub(crate) id: i64,
    pub(crate) event_id: i64,
    pub(crate) subscription_id: i32,
    pub(crate) status: String,
    pub(crate) attempts: i32,
    pub(crate) next_attempt_at: NaiveDateTime,
    pub(crate) last_error: Option<String>,
    pub(crate) locked_until: Option<NaiveDateTime>,
    pub(crate) claim_token: Option<Uuid>,
    pub(crate) created_at: NaiveDateTime,
    pub(crate) updated_at: NaiveDateTime,
}

impl fmt::Debug for EventDeliveryRow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventDeliveryRow")
            .field("id", &self.id)
            .field("event_id", &self.event_id)
            .field("subscription_id", &self.subscription_id)
            .field("status", &self.status)
            .field("attempts", &self.attempts)
            .field("next_attempt_at", &self.next_attempt_at)
            .field("last_error", &redacted_debug_option(&self.last_error))
            .field("locked_until", &self.locked_until)
            .field("claim_token", &redacted_debug_option(&self.claim_token))
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .finish()
    }
}

impl CursorPaginated for EventDeliveryRow {
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
        Self::default_sort()
    }
}

impl CursorSqlMapping for EventDeliveryRow {
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

#[derive(Debug, Clone)]
pub(crate) struct ClaimedEventDelivery {
    pub delivery: EventDeliveryRow,
    pub event: Event,
    pub subscription: EventSubscriptionRow,
    pub sink: EventSinkRow,
}

pub(crate) struct EventDeliveryClaimBatch {
    deliveries: Vec<ClaimedEventDelivery>,
    next_wakeup_in: Option<StdDuration>,
}

impl EventDeliveryClaimBatch {
    pub(crate) fn into_parts(self) -> (Vec<ClaimedEventDelivery>, Option<StdDuration>) {
        (self.deliveries, self.next_wakeup_in)
    }
}

#[derive(QueryableByName)]
struct ScheduledDeliveryWakeup {
    #[diesel(sql_type = Nullable<Timestamp>)]
    wakeup_at: Option<NaiveDateTime>,
}

async fn next_event_delivery_wakeup_in(
    conn: &mut crate::storage::postgres::PostgresConnection,
    now: NaiveDateTime,
) -> Result<Option<StdDuration>, diesel::result::Error> {
    let schedule = diesel::sql_query(
        "WITH scheduled AS ( \
             (SELECT next_attempt_at AS wakeup_at \
              FROM event_deliveries \
              WHERE status = 'failed' \
                AND next_attempt_at > $1 \
              ORDER BY next_attempt_at \
              LIMIT 1) \
             UNION ALL \
             (SELECT locked_until AS wakeup_at \
              FROM event_deliveries \
              WHERE status = 'in_flight' \
                AND locked_until > $1 \
              ORDER BY locked_until \
              LIMIT 1) \
         ) \
         SELECT MIN(scheduled.wakeup_at) AS wakeup_at \
         FROM scheduled",
    )
    .bind::<Timestamp, _>(now)
    .get_result::<ScheduledDeliveryWakeup>(conn)
    .await?;

    Ok(schedule.wakeup_at.map(|wakeup_at| {
        wakeup_at
            .signed_duration_since(now)
            .to_std()
            .unwrap_or_default()
    }))
}

#[cfg(test)]
pub(crate) async fn next_event_delivery_wakeup_in_db(
    pool: &impl crate::storage::StorageContext,
) -> Result<Option<StdDuration>, ApiError> {
    let now = Utc::now().naive_utc();
    with_connection(pool, async |conn| {
        next_event_delivery_wakeup_in(conn, now).await
    })
    .await
}

pub(crate) async fn claim_event_delivery_batch(
    pool: &impl crate::storage::StorageContext,
    settings: EventDeliverySettings,
) -> Result<EventDeliveryClaimBatch, ApiError> {
    use crate::schema::event_deliveries::dsl::{
        claim_token, event_deliveries, id, locked_until, next_attempt_at, status,
    };

    with_transaction(
        pool,
        async |conn| -> Result<EventDeliveryClaimBatch, ApiError> {
            if !maintenance_state_conn(conn).await?.is_normal() {
                return Ok(EventDeliveryClaimBatch {
                    deliveries: Vec::new(),
                    next_wakeup_in: None,
                });
            }

            let now = Utc::now().naive_utc();
            let delivery_ids = event_deliveries
                .filter(
                    status
                        .eq(EventDeliveryStatus::Pending.as_str())
                        .or(status
                            .eq(EventDeliveryStatus::Failed.as_str())
                            .and(next_attempt_at.le(now)))
                        .or(status
                            .eq(EventDeliveryStatus::InFlight.as_str())
                            .and(locked_until.lt(now))),
                )
                .order((next_attempt_at.asc(), id.asc()))
                .for_update()
                .skip_locked()
                .limit(settings.query_batch_size())
                .select(id)
                .load::<i64>(conn)
                .await?;

            if delivery_ids.is_empty() {
                return Ok(EventDeliveryClaimBatch {
                    deliveries: Vec::new(),
                    next_wakeup_in: next_event_delivery_wakeup_in(conn, now).await?,
                });
            }

            let now = Utc::now().naive_utc();
            let lock_deadline = settings.lock_deadline(now).ok_or_else(|| {
                ApiError::InternalServerError(
                    "event delivery lock timeout exceeds the database timestamp range".to_string(),
                )
            })?;
            let claim = Uuid::new_v4();
            let claimed_deliveries =
                diesel::update(event_deliveries.filter(id.eq_any(delivery_ids)))
                    .set((
                        status.eq(EventDeliveryStatus::InFlight.as_str()),
                        locked_until.eq(Some(lock_deadline)),
                        claim_token.eq(Some(claim)),
                    ))
                    .get_results::<EventDeliveryRow>(conn)
                    .await?;

            Ok(EventDeliveryClaimBatch {
                deliveries: load_claimed_delivery_contexts(conn, claimed_deliveries).await?,
                next_wakeup_in: None,
            })
        },
    )
    .await
}

/// Claim and fully enrich one delivery batch before crossing the storage edge.
///
/// PostgreSQL rows and claim columns remain adapter-private. The worker sees a
/// bounded work-item DTO containing only the envelope and transport settings
/// required to perform and acknowledge delivery.
pub(crate) async fn claim_event_delivery_batch_from_storage(
    pool: &crate::storage::postgres::PostgresPool,
    settings: EventDeliverySettings,
) -> Result<EventDeliveryBatch, ApiError> {
    let (deliveries, next_wakeup_in) = claim_event_delivery_batch(pool, settings)
        .await?
        .into_parts();
    let deliveries = claimed_delivery_work_items(pool, deliveries).await?;
    Ok(EventDeliveryBatch::new(deliveries, next_wakeup_in))
}

async fn claimed_delivery_work_items(
    pool: &crate::storage::postgres::PostgresPool,
    mut deliveries: Vec<ClaimedEventDelivery>,
) -> Result<Vec<EventDeliveryWorkItem>, ApiError> {
    let legacy_task_ids = deliveries
        .iter()
        .filter(|claimed| {
            claimed.event.entity_type == EntityType::Task.as_str()
                && claimed.event.initiator_user_id.is_none()
        })
        .filter_map(|claimed| claimed.event.entity_id)
        .collect::<Vec<_>>();
    let queued_initiators =
        super::events::load_queued_task_initiators(pool, &legacy_task_ids).await?;
    for claimed in &mut deliveries {
        if claimed.event.entity_type != EntityType::Task.as_str() {
            continue;
        }
        let Some(task_id) = claimed.event.entity_id else {
            continue;
        };
        claimed.event.task_id.get_or_insert(task_id);
        if claimed.event.initiator_user_id.is_none() {
            claimed.event.initiator_user_id = queued_initiators.get(&task_id).copied().flatten();
        }
    }

    let principal_ids = deliveries
        .iter()
        .flat_map(|claimed| [claimed.event.actor_user_id, claimed.event.initiator_user_id])
        .flatten()
        .collect();
    let principal_names = super::history::resolve_principal_names(pool, principal_ids).await?;

    deliveries
        .into_iter()
        .map(|claimed| {
            let claim_token = claimed.delivery.claim_token.ok_or_else(|| {
                ApiError::InternalServerError(
                    "claimed event delivery is missing claim_token".to_string(),
                )
            })?;
            let subscription = EventSubscription::try_from(claimed.subscription)?;
            let sink = EventSink::try_from(claimed.sink)?;
            Ok(EventDeliveryWorkItem::new(
                EventDeliveryClaim::new(
                    claimed.delivery.id,
                    claimed.delivery.attempts,
                    claim_token,
                ),
                claimed.event.into_envelope(&principal_names),
                EventDeliverySubscription::new(
                    subscription.id,
                    subscription.name,
                    subscription.routing,
                ),
                EventDeliverySink::new(
                    sink.id,
                    sink.name,
                    sink.kind.as_str(),
                    sink.config,
                    sink.secret_ref,
                ),
            ))
        })
        .collect()
}

#[cfg(test)]
pub(crate) async fn claimed_event_delivery_work_item(
    pool: &impl crate::storage::StorageContext,
    claimed: ClaimedEventDelivery,
) -> Result<EventDeliveryWorkItem, ApiError> {
    claimed_delivery_work_items(crate::storage::context::postgres_pool(pool), vec![claimed])
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| ApiError::NotFound("Event delivery work item not found".to_string()))
}

#[cfg(test)]
pub(crate) async fn claim_event_deliveries(
    pool: &impl crate::storage::StorageContext,
    settings: EventDeliverySettings,
) -> Result<Vec<ClaimedEventDelivery>, ApiError> {
    let (deliveries, _) = claim_event_delivery_batch(pool, settings)
        .await?
        .into_parts();
    Ok(deliveries)
}

#[cfg(test)]
pub(crate) async fn claim_event_delivery_by_id(
    pool: &impl crate::storage::StorageContext,
    delivery_id: i64,
    settings: EventDeliverySettings,
) -> Result<ClaimedEventDelivery, ApiError> {
    use crate::schema::event_deliveries::dsl::{
        claim_token, event_deliveries, id, locked_until, next_attempt_at, status,
    };

    with_transaction(
        pool,
        async |conn| -> Result<ClaimedEventDelivery, ApiError> {
            let now = Utc::now().naive_utc();
            let lock_deadline = settings.lock_deadline(now).ok_or_else(|| {
                ApiError::InternalServerError(
                    "event delivery lock timeout exceeds the database timestamp range".to_string(),
                )
            })?;
            let claim = Uuid::new_v4();
            let delivery = diesel::update(
                event_deliveries.filter(id.eq(delivery_id)).filter(
                    status
                        .eq(EventDeliveryStatus::Pending.as_str())
                        .or(status
                            .eq(EventDeliveryStatus::Failed.as_str())
                            .and(next_attempt_at.le(now)))
                        .or(status
                            .eq(EventDeliveryStatus::InFlight.as_str())
                            .and(locked_until.lt(now))),
                ),
            )
            .set((
                status.eq(EventDeliveryStatus::InFlight.as_str()),
                locked_until.eq(Some(lock_deadline)),
                claim_token.eq(Some(claim)),
            ))
            .get_result::<EventDeliveryRow>(conn)
            .await?;

            load_claimed_delivery_context(conn, delivery).await
        },
    )
    .await
}

#[cfg(test)]
async fn load_claimed_delivery_context(
    conn: &mut crate::storage::postgres::PostgresConnection,
    delivery: EventDeliveryRow,
) -> Result<ClaimedEventDelivery, ApiError> {
    load_claimed_delivery_contexts(conn, vec![delivery])
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| ApiError::NotFound("Event delivery not found".to_string()))
}

async fn load_claimed_delivery_contexts(
    conn: &mut crate::storage::postgres::PostgresConnection,
    deliveries: Vec<EventDeliveryRow>,
) -> Result<Vec<ClaimedEventDelivery>, ApiError> {
    use crate::schema::{event_sinks, event_subscriptions, events};

    if deliveries.is_empty() {
        return Ok(Vec::new());
    }

    let event_ids = deliveries
        .iter()
        .map(|delivery| delivery.event_id)
        .collect::<Vec<_>>();
    let subscription_ids = deliveries
        .iter()
        .map(|delivery| delivery.subscription_id)
        .collect::<Vec<_>>();

    let loaded_events = events::table
        .filter(events::id.eq_any(&event_ids))
        .load::<EventRow>(conn)
        .await?
        .into_iter()
        .map(Event::from)
        .map(|event| (event.id, event))
        .collect::<HashMap<_, _>>();
    let loaded_subscriptions = event_subscriptions::table
        .filter(event_subscriptions::id.eq_any(&subscription_ids))
        .load::<EventSubscriptionRow>(conn)
        .await?
        .into_iter()
        .map(|subscription| (subscription.id, subscription))
        .collect::<HashMap<_, _>>();
    let sink_ids = loaded_subscriptions
        .values()
        .map(|subscription| subscription.sink_id)
        .collect::<Vec<_>>();
    let loaded_sinks = event_sinks::table
        .filter(event_sinks::id.eq_any(&sink_ids))
        .load::<EventSinkRow>(conn)
        .await?
        .into_iter()
        .map(|sink| (sink.id, sink))
        .collect::<HashMap<_, _>>();

    deliveries
        .into_iter()
        .map(|delivery| {
            let event = loaded_events
                .get(&delivery.event_id)
                .cloned()
                .ok_or_else(|| ApiError::NotFound("Event for delivery not found".to_string()))?;
            let subscription = loaded_subscriptions
                .get(&delivery.subscription_id)
                .cloned()
                .ok_or_else(|| {
                    ApiError::NotFound("Event subscription for delivery not found".to_string())
                })?;
            let sink = loaded_sinks
                .get(&subscription.sink_id)
                .cloned()
                .ok_or_else(|| {
                    ApiError::NotFound("Event sink for delivery subscription not found".to_string())
                })?;
            Ok(ClaimedEventDelivery {
                delivery,
                event,
                subscription,
                sink,
            })
        })
        .collect()
}

pub(crate) async fn mark_event_delivery_succeeded(
    pool: &impl crate::storage::StorageContext,
    delivery_id_value: i64,
    claim_token_value: Uuid,
) -> Result<EventDeliveryRow, ApiError> {
    use crate::schema::event_deliveries::dsl::{
        claim_token, event_deliveries, id, last_error, locked_until, status,
    };

    with_connection(pool, async |conn| {
        diesel::update(
            event_deliveries
                .filter(id.eq(delivery_id_value))
                .filter(claim_token.eq(claim_token_value))
                .filter(status.eq(EventDeliveryStatus::InFlight.as_str())),
        )
        .set((
            status.eq(EventDeliveryStatus::Succeeded.as_str()),
            locked_until.eq::<Option<chrono::NaiveDateTime>>(None),
            claim_token.eq::<Option<Uuid>>(None),
            last_error.eq::<Option<String>>(None),
        ))
        .get_result::<EventDeliveryRow>(conn)
        .await
    })
    .await
}

pub(crate) async fn mark_event_delivery_claim_succeeded(
    pool: &crate::storage::postgres::PostgresPool,
    claim: &EventDeliveryClaim,
) -> Result<(), ApiError> {
    mark_event_delivery_succeeded(pool, claim.delivery_id(), claim.token())
        .await
        .map(|_| ())
}

#[cfg(test)]
pub(crate) async fn mark_event_delivery_failed(
    pool: &impl crate::storage::StorageContext,
    delivery: &EventDeliveryRow,
    settings: EventDeliverySettings,
    error: &str,
) -> Result<EventDeliveryRow, ApiError> {
    mark_event_delivery_failed_values(
        pool,
        delivery.id,
        delivery.claim_token,
        delivery.attempts,
        settings,
        error,
    )
    .await
}

pub(crate) async fn mark_event_delivery_claim_failed(
    pool: &crate::storage::postgres::PostgresPool,
    claim: &EventDeliveryClaim,
    settings: EventDeliverySettings,
    error: &str,
) -> Result<(), ApiError> {
    mark_event_delivery_failed_values(
        pool,
        claim.delivery_id(),
        Some(claim.token()),
        claim.attempts(),
        settings,
        error,
    )
    .await
    .map(|_| ())
}

async fn mark_event_delivery_failed_values(
    pool: &impl crate::storage::StorageContext,
    delivery_id: i64,
    delivery_claim_token: Option<Uuid>,
    delivery_attempts: i32,
    settings: EventDeliverySettings,
    error: &str,
) -> Result<EventDeliveryRow, ApiError> {
    use crate::schema::event_deliveries::dsl::{
        attempts, claim_token, event_deliveries, id, last_error, locked_until, next_attempt_at,
        status,
    };

    let next_attempts = delivery_attempts + 1;
    let next_status = if next_attempts >= settings.max_attempts() {
        EventDeliveryStatus::Dead
    } else {
        EventDeliveryStatus::Failed
    };
    let next_attempt = settings
        .retry_deadline(Utc::now().naive_utc(), next_attempts)
        .ok_or_else(|| {
            ApiError::InternalServerError(
                "event delivery retry backoff exceeds the database timestamp range".to_string(),
            )
        })?;
    let error = truncate_delivery_error(error);

    with_connection(pool, async |conn| {
        diesel::update(
            event_deliveries
                .filter(id.eq(delivery_id))
                .filter(claim_token.eq(delivery_claim_token))
                .filter(status.eq(EventDeliveryStatus::InFlight.as_str())),
        )
        .set((
            status.eq(next_status.as_str()),
            attempts.eq(next_attempts),
            next_attempt_at.eq(next_attempt),
            last_error.eq(Some(error)),
            locked_until.eq::<Option<chrono::NaiveDateTime>>(None),
            claim_token.eq::<Option<Uuid>>(None),
        ))
        .get_result::<EventDeliveryRow>(conn)
        .await
    })
    .await
}

fn truncate_delivery_error(error: &str) -> String {
    const MAX_ERROR_BYTES: usize = 4096;
    if error.len() <= MAX_ERROR_BYTES {
        return error.to_string();
    }

    let mut end = MAX_ERROR_BYTES;
    while !error.is_char_boundary(end) {
        end -= 1;
    }
    error[..end].to_string()
}

pub(crate) async fn load_event_delivery(
    pool: &impl crate::storage::StorageContext,
    delivery_id: EventDeliveryID,
) -> Result<EventDeliveryRow, ApiError> {
    use crate::schema::event_deliveries::dsl::{event_deliveries, id};

    with_connection(pool, async |conn| {
        event_deliveries
            .filter(id.eq(delivery_id.id()))
            .first::<EventDeliveryRow>(conn)
            .await
    })
    .await
}

#[cfg(feature = "integration-test-support")]
fn event_delivery_response(delivery: EventDeliveryRow) -> EventDeliveryResponse {
    EventDeliveryResponse {
        id: delivery.id,
        event_id: delivery.event_id,
        subscription_id: delivery.subscription_id,
        status: delivery.status,
        attempts: delivery.attempts,
        next_attempt_at: delivery.next_attempt_at,
        last_error: delivery.last_error,
        locked_until: delivery.locked_until,
        created_at: delivery.created_at,
        updated_at: delivery.updated_at,
    }
}

#[cfg(feature = "integration-test-support")]
pub(crate) async fn load_event_delivery_for_event(
    pool: &impl crate::storage::StorageContext,
    event_id_value: i64,
) -> Result<EventDeliveryResponse, ApiError> {
    use crate::schema::event_deliveries::dsl::{event_deliveries, event_id};

    with_connection(pool, async |conn| {
        event_deliveries
            .filter(event_id.eq(event_id_value))
            .first::<EventDeliveryRow>(conn)
            .await
    })
    .await
    .map(event_delivery_response)
}

#[cfg(feature = "integration-test-support")]
pub(crate) async fn set_event_delivery_status_for_test(
    pool: &impl crate::storage::StorageContext,
    delivery_id: i64,
    delivery_status: EventDeliveryStatus,
) -> Result<(), ApiError> {
    use crate::schema::event_deliveries::dsl::{event_deliveries, id, status};

    with_connection(pool, async |conn| {
        diesel::update(event_deliveries.filter(id.eq(delivery_id)))
            .set(status.eq(delivery_status.as_str()))
            .execute(conn)
            .await
    })
    .await?;
    Ok(())
}

#[cfg(feature = "integration-test-support")]
pub(crate) async fn set_event_delivery_claim_token_for_test(
    pool: &impl crate::storage::StorageContext,
    delivery_id: i64,
    delivery_claim_token: Uuid,
) -> Result<(), ApiError> {
    use crate::schema::event_deliveries::dsl::{claim_token, event_deliveries, id};

    with_connection(pool, async |conn| {
        diesel::update(event_deliveries.filter(id.eq(delivery_id)))
            .set(claim_token.eq(Some(delivery_claim_token)))
            .execute(conn)
            .await
    })
    .await?;
    Ok(())
}

pub(crate) async fn list_event_deliveries_with_total_count(
    pool: &impl crate::storage::StorageContext,
    subscription_id_filter: Option<i32>,
    query_options: &QueryOptions,
) -> Result<(Vec<EventDeliveryRow>, i64), ApiError> {
    let query = build_event_delivery_query(subscription_id_filter, query_options)?;
    let total_count = crate::pagination::exact_count_or_skipped(query_options, async || {
        with_connection(pool, async |conn| {
            query.count().get_result::<i64>(conn).await
        })
        .await
    })
    .await?;
    let mut query = build_event_delivery_query(subscription_id_filter, query_options)?;
    crate::apply_query_options!(query, query_options, EventDeliveryRow);
    let deliveries = with_connection(pool, async |conn| {
        query.load::<EventDeliveryRow>(conn).await
    })
    .await?;
    Ok((deliveries, total_count))
}

fn build_event_delivery_query(
    subscription_id_filter: Option<i32>,
    query_options: &QueryOptions,
) -> Result<crate::schema::event_deliveries::BoxedQuery<'static, diesel::pg::Pg>, ApiError> {
    use crate::schema::event_deliveries::dsl::{
        created_at, event_deliveries, id, next_attempt_at, status, subscription_id, updated_at,
    };

    let mut query = event_deliveries.into_boxed();
    if let Some(value) = subscription_id_filter {
        query = query.filter(subscription_id.eq(value));
    }
    for param in query_options.filters.clone() {
        let operator = param.operator.clone();
        match param.field {
            FilterField::Id => {
                let values = param
                    .value_as_integer()?
                    .into_iter()
                    .map(i64::from)
                    .collect::<Vec<_>>();
                let (op, negated) = operator.op_and_neg();
                match (op, negated) {
                    (Operator::Equals, false) | (Operator::In, false) => {
                        query = query.filter(id.eq_any(values))
                    }
                    (Operator::Equals, true) | (Operator::In, true) => {
                        query = query.filter(diesel::dsl::not(id.eq_any(values)))
                    }
                    _ => {
                        return Err(ApiError::OperatorMismatch(format!(
                            "Operator '{operator:?}' not implemented for field '{}' (type: bigint)",
                            param.field
                        )));
                    }
                }
            }
            FilterField::Status => crate::string_search!(query, param, operator, status),
            FilterField::CreatedAt => crate::date_search!(query, param, operator, created_at),
            FilterField::UpdatedAt => crate::date_search!(query, param, operator, updated_at),
            FilterField::NextAttemptAt => {
                crate::date_search!(query, param, operator, next_attempt_at)
            }
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not searchable for event deliveries",
                    param.field
                )));
            }
        }
    }
    Ok(query)
}

pub(crate) async fn release_event_delivery_for_retry(
    pool: &impl crate::storage::StorageContext,
    delivery_id: EventDeliveryID,
) -> Result<EventDeliveryRow, ApiError> {
    use crate::schema::event_deliveries::dsl::{
        claim_token, event_deliveries, id, last_error, locked_until, next_attempt_at, status,
    };

    with_connection(
        pool,
        async |conn| -> Result<EventDeliveryRow, diesel::result::Error> {
            let delivery = diesel::update(event_deliveries.filter(id.eq(delivery_id.id())).filter(
                status.eq_any([
                    EventDeliveryStatus::Failed.as_str(),
                    EventDeliveryStatus::Dead.as_str(),
                ]),
            ))
            .set((
                status.eq(EventDeliveryStatus::Pending.as_str()),
                next_attempt_at.eq(Utc::now().naive_utc()),
                locked_until.eq::<Option<chrono::NaiveDateTime>>(None),
                claim_token.eq::<Option<Uuid>>(None),
                last_error.eq::<Option<String>>(None),
            ))
            .get_result::<EventDeliveryRow>(conn)
            .await?;
            crate::events::notify_event_delivery(conn).await?;
            Ok(delivery)
        },
    )
    .await
}

pub(crate) async fn mark_event_delivery_dead(
    pool: &impl crate::storage::StorageContext,
    delivery_id: EventDeliveryID,
) -> Result<EventDeliveryRow, ApiError> {
    use crate::schema::event_deliveries::dsl::{
        claim_token, event_deliveries, id, last_error, locked_until, status,
    };

    with_connection(pool, async |conn| {
        diesel::update(
            event_deliveries
                .filter(id.eq(delivery_id.id()))
                .filter(status.ne(EventDeliveryStatus::Succeeded.as_str())),
        )
        .set((
            status.eq(EventDeliveryStatus::Dead.as_str()),
            locked_until.eq::<Option<chrono::NaiveDateTime>>(None),
            claim_token.eq::<Option<Uuid>>(None),
            last_error.eq(Some("marked dead by operator".to_string())),
        ))
        .get_result::<EventDeliveryRow>(conn)
        .await
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_delivery_row_debug_redacts_claim_token_and_error() {
        let timestamp = chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .unwrap()
            .naive_utc();
        let claim_token = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        let delivery = EventDeliveryRow {
            id: 1,
            event_id: 2,
            subscription_id: 3,
            status: EventDeliveryStatus::InFlight.as_str().to_string(),
            attempts: 1,
            next_attempt_at: timestamp,
            last_error: Some("delivery-error-secret".to_string()),
            locked_until: Some(timestamp),
            claim_token: Some(claim_token),
            created_at: timestamp,
            updated_at: timestamp,
        };

        let debug = format!("{delivery:?}");

        assert!(debug.contains(crate::models::REDACTED_DEBUG_VALUE));
        assert!(!debug.contains("delivery-error-secret"));
        assert!(!debug.contains(&claim_token.to_string()));
    }
}
