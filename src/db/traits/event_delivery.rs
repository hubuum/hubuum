use chrono::{Duration, Utc};
use diesel::prelude::*;
use uuid::Uuid;

use crate::db::{DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::events::Event;
use crate::models::event_subscription::{EventSinkRow, EventSubscriptionRow};
use crate::models::search::QueryOptions;
use crate::models::{EventDelivery, EventDeliveryID, EventDeliveryStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventDeliverySettings {
    pub batch_size: usize,
    pub lock_timeout_ms: u64,
    pub retry_backoff_base_ms: u64,
    pub retry_backoff_max_ms: u64,
    pub max_attempts: i32,
}

#[derive(Debug, Clone)]
pub(crate) struct ClaimedEventDelivery {
    pub delivery: EventDelivery,
    pub event: Event,
    pub subscription: EventSubscriptionRow,
    pub sink: EventSinkRow,
}

pub(crate) async fn claim_event_deliveries(
    pool: &DbPool,
    settings: EventDeliverySettings,
) -> Result<Vec<ClaimedEventDelivery>, ApiError> {
    use crate::schema::event_deliveries::dsl::{
        claim_token, event_deliveries, id, locked_until, next_attempt_at, status,
    };

    if settings.batch_size == 0 {
        return Ok(Vec::new());
    }

    with_transaction(
        pool,
        |conn| -> Result<Vec<ClaimedEventDelivery>, ApiError> {
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
                .limit(settings.batch_size as i64)
                .select(id)
                .load::<i64>(conn)?;

            if delivery_ids.is_empty() {
                return Ok(Vec::new());
            }

            let now = Utc::now().naive_utc();
            let claim = Uuid::new_v4();
            let claimed_deliveries =
                diesel::update(event_deliveries.filter(id.eq_any(delivery_ids)))
                    .set((
                        status.eq(EventDeliveryStatus::InFlight.as_str()),
                        locked_until.eq(Some(
                            now + Duration::milliseconds(settings.lock_timeout_ms as i64),
                        )),
                        claim_token.eq(Some(claim)),
                    ))
                    .get_results::<EventDelivery>(conn)?;

            claimed_deliveries
                .into_iter()
                .map(|delivery| load_claimed_delivery_context(conn, delivery))
                .collect()
        },
    )
}

#[cfg(test)]
pub(crate) async fn claim_event_delivery_by_id(
    pool: &DbPool,
    delivery_id: i64,
    settings: EventDeliverySettings,
) -> Result<ClaimedEventDelivery, ApiError> {
    use crate::schema::event_deliveries::dsl::{
        claim_token, event_deliveries, id, locked_until, next_attempt_at, status,
    };

    with_transaction(pool, |conn| -> Result<ClaimedEventDelivery, ApiError> {
        let now = Utc::now().naive_utc();
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
            locked_until.eq(Some(
                now + Duration::milliseconds(settings.lock_timeout_ms as i64),
            )),
            claim_token.eq(Some(claim)),
        ))
        .get_result::<EventDelivery>(conn)?;

        load_claimed_delivery_context(conn, delivery)
    })
}

fn load_claimed_delivery_context(
    conn: &mut PgConnection,
    delivery: EventDelivery,
) -> Result<ClaimedEventDelivery, ApiError> {
    use crate::schema::{event_sinks, event_subscriptions, events};

    let event = events::table
        .filter(events::id.eq(delivery.event_id))
        .first::<Event>(conn)?;
    let subscription = event_subscriptions::table
        .filter(event_subscriptions::id.eq(delivery.subscription_id))
        .first::<EventSubscriptionRow>(conn)?;
    let sink = event_sinks::table
        .filter(event_sinks::id.eq(subscription.sink_id))
        .first::<EventSinkRow>(conn)?;

    Ok(ClaimedEventDelivery {
        delivery,
        event,
        subscription,
        sink,
    })
}

pub async fn mark_event_delivery_succeeded(
    pool: &DbPool,
    delivery_id_value: i64,
    claim_token_value: Uuid,
) -> Result<EventDelivery, ApiError> {
    use crate::schema::event_deliveries::dsl::{
        claim_token, event_deliveries, id, last_error, locked_until, status,
    };

    with_connection(pool, |conn| {
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
    })
}

pub async fn mark_event_delivery_failed(
    pool: &DbPool,
    delivery: &EventDelivery,
    settings: EventDeliverySettings,
    error: &str,
) -> Result<EventDelivery, ApiError> {
    use crate::schema::event_deliveries::dsl::{
        attempts, claim_token, event_deliveries, id, last_error, locked_until, next_attempt_at,
        status,
    };

    let next_attempts = delivery.attempts + 1;
    let next_status = if next_attempts >= settings.max_attempts {
        EventDeliveryStatus::Dead
    } else {
        EventDeliveryStatus::Failed
    };
    let next_attempt = Utc::now().naive_utc()
        + Duration::milliseconds(retry_backoff_ms(
            next_attempts,
            settings.retry_backoff_base_ms,
            settings.retry_backoff_max_ms,
        ) as i64);
    let error = truncate_delivery_error(error);

    with_connection(pool, |conn| {
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
    })
}

pub fn retry_backoff_ms(attempts: i32, base_ms: u64, max_ms: u64) -> u64 {
    let exponent = attempts.saturating_sub(1).min(31) as u32;
    base_ms
        .saturating_mul(2_u64.saturating_pow(exponent))
        .min(max_ms)
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
    pool: &DbPool,
    delivery_id: EventDeliveryID,
) -> Result<EventDelivery, ApiError> {
    use crate::schema::event_deliveries::dsl::{event_deliveries, id};

    with_connection(pool, |conn| {
        event_deliveries
            .filter(id.eq(delivery_id.id()))
            .first::<EventDelivery>(conn)
    })
}

pub async fn list_event_deliveries_with_total_count(
    pool: &DbPool,
    query_options: &QueryOptions,
) -> Result<(Vec<EventDelivery>, i64), ApiError> {
    use crate::schema::event_deliveries::dsl::event_deliveries;

    let mut query = event_deliveries.into_boxed();
    crate::apply_query_options!(query, query_options, EventDelivery);
    let deliveries = with_connection(pool, |conn| query.load::<EventDelivery>(conn))?;
    let total_count = with_connection(pool, |conn| {
        event_deliveries.count().get_result::<i64>(conn)
    })?;
    Ok((deliveries, total_count))
}

pub async fn release_event_delivery_for_retry(
    pool: &DbPool,
    delivery_id: EventDeliveryID,
) -> Result<EventDelivery, ApiError> {
    use crate::schema::event_deliveries::dsl::{
        claim_token, event_deliveries, id, last_error, locked_until, next_attempt_at, status,
    };

    with_connection(pool, |conn| {
        diesel::update(
            event_deliveries
                .filter(id.eq(delivery_id.id()))
                .filter(status.eq_any([
                    EventDeliveryStatus::Failed.as_str(),
                    EventDeliveryStatus::Dead.as_str(),
                ])),
        )
        .set((
            status.eq(EventDeliveryStatus::Pending.as_str()),
            next_attempt_at.eq(Utc::now().naive_utc()),
            locked_until.eq::<Option<chrono::NaiveDateTime>>(None),
            claim_token.eq::<Option<Uuid>>(None),
            last_error.eq::<Option<String>>(None),
        ))
        .get_result::<EventDelivery>(conn)
    })
}

pub async fn mark_event_delivery_dead(
    pool: &DbPool,
    delivery_id: EventDeliveryID,
) -> Result<EventDelivery, ApiError> {
    use crate::schema::event_deliveries::dsl::{
        claim_token, event_deliveries, id, last_error, locked_until, status,
    };

    with_connection(pool, |conn| {
        diesel::update(event_deliveries.filter(id.eq(delivery_id.id())))
            .set((
                status.eq(EventDeliveryStatus::Dead.as_str()),
                locked_until.eq::<Option<chrono::NaiveDateTime>>(None),
                claim_token.eq::<Option<Uuid>>(None),
                last_error.eq(Some("marked dead by operator".to_string())),
            ))
            .get_result::<EventDelivery>(conn)
    })
}
