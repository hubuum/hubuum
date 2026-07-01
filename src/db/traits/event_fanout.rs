use chrono::{Duration, Utc};
use diesel::prelude::*;
use uuid::Uuid;

#[cfg(test)]
use crate::db::with_connection;
use crate::db::{DbPool, with_transaction};
use crate::errors::ApiError;
use crate::events::{Event, EventEnvelope};
use crate::models::EventDeliveryStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventFanoutSettings {
    pub batch_size: usize,
    pub lock_timeout_ms: u64,
}

pub async fn process_event_fanout_batch(
    pool: &DbPool,
    settings: EventFanoutSettings,
) -> Result<usize, ApiError> {
    let events = claim_events_for_fanout(pool, settings).await?;
    let event_ids = events.iter().map(|event| event.id).collect::<Vec<_>>();
    fanout_events(pool, &event_ids).await
}

pub async fn claim_events_for_fanout(
    pool: &DbPool,
    settings: EventFanoutSettings,
) -> Result<Vec<Event>, ApiError> {
    use crate::schema::events::dsl::{
        dispatched_at, events, fanout_claim_token, fanout_locked_until, id, occurred_at,
    };

    if settings.batch_size == 0 {
        return Ok(Vec::new());
    }

    with_transaction(pool, |conn| -> Result<Vec<Event>, ApiError> {
        let now = Utc::now().naive_utc();
        let event_ids = events
            .filter(dispatched_at.is_null())
            .filter(
                fanout_locked_until
                    .is_null()
                    .or(fanout_locked_until.lt(now)),
            )
            .order(occurred_at.asc())
            .for_update()
            .skip_locked()
            .limit(settings.batch_size as i64)
            .select(id)
            .load::<i64>(conn)?;

        if event_ids.is_empty() {
            return Ok(Vec::new());
        }

        let lock_timeout = Duration::milliseconds(settings.lock_timeout_ms as i64);
        let claim_token = Uuid::new_v4();
        let claimed = diesel::update(events.filter(id.eq_any(event_ids)))
            .set((
                fanout_locked_until.eq(Some(now + lock_timeout)),
                fanout_claim_token.eq(Some(claim_token)),
            ))
            .get_results::<Event>(conn)?;

        Ok(claimed)
    })
}

#[cfg(test)]
pub async fn fanout_event(pool: &DbPool, event_id: i64) -> Result<usize, ApiError> {
    fanout_events(pool, &[event_id]).await
}

pub async fn fanout_events(pool: &DbPool, event_ids: &[i64]) -> Result<usize, ApiError> {
    use crate::schema::events::dsl::{
        dispatched_at, events, fanout_claim_token, fanout_locked_until, id,
    };

    if event_ids.is_empty() {
        return Ok(0);
    }

    with_transaction(pool, |conn| -> Result<usize, ApiError> {
        let claimed_events = events
            .filter(id.eq_any(event_ids))
            .filter(dispatched_at.is_null())
            .order(id.asc())
            .load::<Event>(conn)?;
        if claimed_events.is_empty() {
            return Ok(0);
        }

        let subscriptions = load_enabled_subscriptions(conn)?;
        let mut inserted = 0;
        let mut processed_event_ids = Vec::with_capacity(claimed_events.len());
        for event in &claimed_events {
            let subscription_ids = matching_subscription_ids(&subscriptions, event);
            inserted += insert_delivery_rows(conn, event.id, &subscription_ids)?;
            processed_event_ids.push(event.id);
        }
        let now = Utc::now().naive_utc();
        diesel::update(events.filter(id.eq_any(processed_event_ids)))
            .set((
                dispatched_at.eq(Some(now)),
                fanout_locked_until.eq::<Option<chrono::NaiveDateTime>>(None),
                fanout_claim_token.eq::<Option<Uuid>>(None),
            ))
            .execute(conn)?;

        Ok(inserted)
    })
}

fn load_enabled_subscriptions(
    conn: &mut PgConnection,
) -> Result<Vec<crate::models::event_subscription::EventSubscriptionRow>, ApiError> {
    use crate::schema::{event_sinks, event_subscriptions};

    event_subscriptions::table
        .inner_join(event_sinks::table.on(event_sinks::id.eq(event_subscriptions::sink_id)))
        .filter(event_subscriptions::enabled.eq(true))
        .filter(event_sinks::enabled.eq(true))
        .select(event_subscriptions::all_columns)
        .load::<crate::models::event_subscription::EventSubscriptionRow>(conn)
        .map_err(ApiError::from)
}

fn matching_subscription_ids(
    subscriptions: &[crate::models::event_subscription::EventSubscriptionRow],
    event: &Event,
) -> Vec<i32> {
    subscriptions
        .iter()
        .filter(|subscription| subscription_matches_event(subscription, event))
        .map(|subscription| subscription.id)
        .collect()
}

fn subscription_matches_event(
    subscription: &crate::models::event_subscription::EventSubscriptionRow,
    event: &Event,
) -> bool {
    let entity_types = serde_json::from_value::<Vec<String>>(subscription.entity_types.clone())
        .unwrap_or_default();
    let actions =
        serde_json::from_value::<Vec<String>>(subscription.actions.clone()).unwrap_or_default();

    if !(entity_types.iter().any(|value| value == &event.entity_type)
        && actions.iter().any(|value| value == &event.action)
        && subscription_namespace_matches_event(subscription.namespace_id, event))
    {
        return false;
    }

    let filter = serde_json::from_value::<hubuum_events_core::EventSubscriptionFilter>(
        subscription.filter.clone(),
    )
    .unwrap_or_default();
    let envelope = EventEnvelope::from(event.clone());
    filter.matches(&envelope)
}

fn subscription_namespace_matches_event(namespace_id: i32, event: &Event) -> bool {
    event.namespace_id == Some(namespace_id)
        || EventEnvelope::from(event.clone())
            .related_namespace_ids()
            .contains(&namespace_id)
}

fn insert_delivery_rows(
    conn: &mut PgConnection,
    event_id_value: i64,
    subscription_ids: &[i32],
) -> Result<usize, ApiError> {
    use crate::schema::event_deliveries::dsl::{
        event_deliveries, event_id, status, subscription_id,
    };

    if subscription_ids.is_empty() {
        return Ok(0);
    }

    let rows = subscription_ids
        .iter()
        .map(|subscription_id_value| {
            (
                event_id.eq(event_id_value),
                subscription_id.eq(*subscription_id_value),
                status.eq(EventDeliveryStatus::Pending.as_str()),
            )
        })
        .collect::<Vec<_>>();

    diesel::insert_into(event_deliveries)
        .values(rows)
        .on_conflict((event_id, subscription_id))
        .do_nothing()
        .execute(conn)
        .map_err(ApiError::from)
}

#[cfg(test)]
pub(crate) async fn count_event_deliveries_for_event(
    pool: &DbPool,
    event_id_value: i64,
) -> Result<i64, ApiError> {
    use crate::schema::event_deliveries::dsl::{event_deliveries, event_id};

    with_connection(pool, |conn| {
        event_deliveries
            .filter(event_id.eq(event_id_value))
            .count()
            .get_result::<i64>(conn)
    })
}
