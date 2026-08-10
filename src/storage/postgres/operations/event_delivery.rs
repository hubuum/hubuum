use std::collections::HashMap;
use std::time::Duration as StdDuration;

use crate::storage::postgres::operations::maintenance::maintenance_state_conn;
use crate::storage::postgres::prelude::*;
use chrono::{NaiveDateTime, Utc};
use diesel::sql_types::{Nullable, Timestamp};
use uuid::Uuid;

use crate::errors::ApiError;
use crate::events::{Event, EventDeliverySettings};
use crate::models::event_subscription::{EventSinkRow, EventSubscriptionRow};
use crate::models::search::{FilterField, Operator, ParsedQueryParamExt, QueryOptions};
use crate::models::{EventDelivery, EventDeliveryID, EventDeliveryStatus};
use crate::storage::postgres::{with_connection, with_transaction};

#[derive(Debug, Clone)]
pub(crate) struct ClaimedEventDelivery {
    pub delivery: EventDelivery,
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
                .limit(settings.database_batch_size())
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
                    .get_results::<EventDelivery>(conn)
                    .await?;

            Ok(EventDeliveryClaimBatch {
                deliveries: load_claimed_delivery_contexts(conn, claimed_deliveries).await?,
                next_wakeup_in: None,
            })
        },
    )
    .await
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
            .get_result::<EventDelivery>(conn)
            .await?;

            load_claimed_delivery_context(conn, delivery).await
        },
    )
    .await
}

#[cfg(test)]
async fn load_claimed_delivery_context(
    conn: &mut crate::storage::postgres::PostgresConnection,
    delivery: EventDelivery,
) -> Result<ClaimedEventDelivery, ApiError> {
    load_claimed_delivery_contexts(conn, vec![delivery])
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| ApiError::NotFound("Event delivery not found".to_string()))
}

async fn load_claimed_delivery_contexts(
    conn: &mut crate::storage::postgres::PostgresConnection,
    deliveries: Vec<EventDelivery>,
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
        .load::<Event>(conn)
        .await?
        .into_iter()
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
) -> Result<EventDelivery, ApiError> {
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
        .get_result::<EventDelivery>(conn)
        .await
    })
    .await
}

pub(crate) async fn mark_event_delivery_failed(
    pool: &impl crate::storage::StorageContext,
    delivery: &EventDelivery,
    settings: EventDeliverySettings,
    error: &str,
) -> Result<EventDelivery, ApiError> {
    use crate::schema::event_deliveries::dsl::{
        attempts, claim_token, event_deliveries, id, last_error, locked_until, next_attempt_at,
        status,
    };

    let next_attempts = delivery.attempts + 1;
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
                .filter(id.eq(delivery.id))
                .filter(claim_token.eq(delivery.claim_token))
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
        .get_result::<EventDelivery>(conn)
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

pub async fn load_event_delivery(
    pool: &impl crate::storage::StorageContext,
    delivery_id: EventDeliveryID,
) -> Result<EventDelivery, ApiError> {
    use crate::schema::event_deliveries::dsl::{event_deliveries, id};

    with_connection(pool, async |conn| {
        event_deliveries
            .filter(id.eq(delivery_id.id()))
            .first::<EventDelivery>(conn)
            .await
    })
    .await
}

pub async fn list_event_deliveries_with_total_count(
    pool: &impl crate::storage::StorageContext,
    query_options: &QueryOptions,
) -> Result<(Vec<EventDelivery>, i64), ApiError> {
    let query = build_event_delivery_query(query_options)?;
    let total_count = crate::pagination::exact_count_or_skipped(query_options, async || {
        with_connection(pool, async |conn| {
            query.count().get_result::<i64>(conn).await
        })
        .await
    })
    .await?;
    let mut query = build_event_delivery_query(query_options)?;
    crate::apply_query_options!(query, query_options, EventDelivery);
    let deliveries =
        with_connection(pool, async |conn| query.load::<EventDelivery>(conn).await).await?;
    Ok((deliveries, total_count))
}

fn build_event_delivery_query(
    query_options: &QueryOptions,
) -> Result<crate::schema::event_deliveries::BoxedQuery<'static, diesel::pg::Pg>, ApiError> {
    use crate::schema::event_deliveries::dsl::{
        created_at, event_deliveries, id, next_attempt_at, status, updated_at,
    };

    let mut query = event_deliveries.into_boxed();
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

pub async fn release_event_delivery_for_retry(
    pool: &impl crate::storage::StorageContext,
    delivery_id: EventDeliveryID,
) -> Result<EventDelivery, ApiError> {
    use crate::schema::event_deliveries::dsl::{
        claim_token, event_deliveries, id, last_error, locked_until, next_attempt_at, status,
    };

    with_connection(
        pool,
        async |conn| -> Result<EventDelivery, diesel::result::Error> {
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
            .get_result::<EventDelivery>(conn)
            .await?;
            crate::events::notify_event_delivery(conn).await?;
            Ok(delivery)
        },
    )
    .await
}

pub async fn mark_event_delivery_dead(
    pool: &impl crate::storage::StorageContext,
    delivery_id: EventDeliveryID,
) -> Result<EventDelivery, ApiError> {
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
        .get_result::<EventDelivery>(conn)
        .await
    })
    .await
}
