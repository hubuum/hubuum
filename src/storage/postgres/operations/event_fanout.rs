use std::collections::HashSet;

use crate::storage::postgres::operations::maintenance::maintenance_state_conn;
use crate::storage::postgres::prelude::*;
use chrono::Utc;
use uuid::Uuid;

use crate::errors::ApiError;
use crate::events::{Event, EventEnvelope, EventFanoutSettings};
use crate::models::EventDeliveryStatus;
#[cfg(test)]
use crate::storage::postgres::with_connection;
use crate::storage::postgres::with_transaction;

use super::event_subscription::EventSubscriptionRow;

pub(crate) async fn process_event_fanout_batch(
    pool: &crate::storage::postgres::PostgresPool,
    settings: EventFanoutSettings,
) -> Result<usize, ApiError> {
    let events = claim_events_for_fanout(pool, settings).await?;
    let event_ids = events.iter().map(|event| event.id).collect::<Vec<_>>();
    fanout_events(pool, &event_ids).await
}

pub(crate) async fn claim_events_for_fanout(
    pool: &impl crate::storage::StorageContext,
    settings: EventFanoutSettings,
) -> Result<Vec<Event>, ApiError> {
    use crate::schema::events::dsl::{
        dispatched_at, events, fanout_claim_token, fanout_locked_until, id, occurred_at,
    };

    with_transaction(pool, async |conn| -> Result<Vec<Event>, ApiError> {
        if !maintenance_state_conn(conn).await?.is_normal() {
            return Ok(Vec::new());
        }

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
            .limit(settings.query_batch_size())
            .select(id)
            .load::<i64>(conn)
            .await?;

        if event_ids.is_empty() {
            return Ok(Vec::new());
        }

        let lock_deadline = settings.lock_deadline(now).ok_or_else(|| {
            ApiError::InternalServerError(
                "event fan-out lock timeout exceeds the database timestamp range".to_string(),
            )
        })?;
        let claim_token = Uuid::new_v4();
        let claimed = diesel::update(events.filter(id.eq_any(event_ids)))
            .set((
                fanout_locked_until.eq(Some(lock_deadline)),
                fanout_claim_token.eq(Some(claim_token)),
            ))
            .get_results::<Event>(conn)
            .await?;

        Ok(claimed)
    })
    .await
}

#[cfg(any(test, feature = "integration-test-support"))]
pub async fn fanout_event(
    pool: &impl crate::storage::StorageContext,
    event_id: i64,
) -> Result<usize, ApiError> {
    fanout_events(pool, &[event_id]).await
}

pub async fn fanout_events(
    pool: &impl crate::storage::StorageContext,
    event_ids: &[i64],
) -> Result<usize, ApiError> {
    use crate::schema::events::dsl::{
        dispatched_at, events, fanout_claim_token, fanout_locked_until, id,
    };

    if event_ids.is_empty() {
        return Ok(0);
    }

    with_transaction(pool, async |conn| -> Result<usize, ApiError> {
        let claimed_events = events
            .filter(id.eq_any(event_ids))
            .filter(dispatched_at.is_null())
            .order(id.asc())
            .load::<Event>(conn)
            .await?;
        if claimed_events.is_empty() {
            return Ok(0);
        }

        let candidate_collection_ids = candidate_subscription_collection_ids(&claimed_events);
        let subscriptions = load_enabled_subscriptions(conn, &candidate_collection_ids)
            .await?
            .into_iter()
            .map(CompiledEventSubscription::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let mut inserted = 0;
        let mut processed_event_ids = Vec::with_capacity(claimed_events.len());
        for event in &claimed_events {
            let envelope = EventEnvelope::from(event.clone());
            let subscription_ids = matching_subscription_ids(&subscriptions, event, &envelope);
            inserted += insert_delivery_rows(conn, event.id, &subscription_ids).await?;
            processed_event_ids.push(event.id);
        }
        let now = Utc::now().naive_utc();
        diesel::update(events.filter(id.eq_any(processed_event_ids)))
            .set((
                dispatched_at.eq(Some(now)),
                fanout_locked_until.eq::<Option<chrono::NaiveDateTime>>(None),
                fanout_claim_token.eq::<Option<Uuid>>(None),
            ))
            .execute(conn)
            .await?;
        if inserted > 0 {
            crate::events::notify_event_delivery(conn).await?;
        }

        Ok(inserted)
    })
    .await
}

async fn load_enabled_subscriptions(
    conn: &mut crate::storage::postgres::PostgresConnection,
    collection_ids: &[i32],
) -> Result<Vec<EventSubscriptionRow>, ApiError> {
    use crate::schema::{event_sinks, event_subscriptions};

    if collection_ids.is_empty() {
        return Ok(Vec::new());
    }

    event_subscriptions::table
        .inner_join(event_sinks::table.on(event_sinks::id.eq(event_subscriptions::sink_id)))
        .filter(event_subscriptions::enabled.eq(true))
        .filter(event_sinks::enabled.eq(true))
        .filter(event_subscriptions::collection_id.eq_any(collection_ids))
        .select(event_subscriptions::all_columns)
        .load::<EventSubscriptionRow>(conn)
        .await
        .map_err(ApiError::from)
}

fn candidate_subscription_collection_ids(events: &[Event]) -> Vec<i32> {
    let mut collection_ids = HashSet::new();
    for event in events {
        if let Some(collection_id) = event.collection_id {
            collection_ids.insert(collection_id);
        }
        let envelope = EventEnvelope::from(event.clone());
        collection_ids.extend(envelope.related_collection_ids());
    }
    collection_ids.into_iter().collect()
}

fn matching_subscription_ids(
    subscriptions: &[CompiledEventSubscription],
    event: &Event,
    envelope: &EventEnvelope,
) -> Vec<i32> {
    subscriptions
        .iter()
        .filter(|subscription| subscription.matches_event(event, envelope))
        .map(|subscription| subscription.id)
        .collect()
}

#[derive(Debug)]
struct CompiledEventSubscription {
    id: i32,
    collection_id: i32,
    entity_types: HashSet<String>,
    actions: HashSet<String>,
    filter: hubuum_events_core::EventSubscriptionFilter,
}

impl TryFrom<EventSubscriptionRow> for CompiledEventSubscription {
    type Error = ApiError;

    fn try_from(subscription: EventSubscriptionRow) -> Result<Self, Self::Error> {
        let entity_types = serde_json::from_value::<Vec<String>>(subscription.entity_types)
            .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
        let actions = serde_json::from_value::<Vec<String>>(subscription.actions)
            .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
        let filter = serde_json::from_value::<hubuum_events_core::EventSubscriptionFilter>(
            subscription.filter,
        )
        .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
        Ok(Self {
            id: subscription.id,
            collection_id: subscription.collection_id,
            entity_types: entity_types.into_iter().collect(),
            actions: actions.into_iter().collect(),
            filter,
        })
    }
}

impl CompiledEventSubscription {
    fn matches_event(&self, event: &Event, envelope: &EventEnvelope) -> bool {
        self.entity_types.contains(&event.entity_type)
            && self.actions.contains(&event.action)
            && subscription_collection_matches_event(self.collection_id, event, envelope)
            && self.filter.matches(envelope)
    }
}

fn subscription_collection_matches_event(
    collection_id: i32,
    event: &Event,
    envelope: &EventEnvelope,
) -> bool {
    event.collection_id == Some(collection_id)
        || envelope.related_collection_ids().contains(&collection_id)
}

async fn insert_delivery_rows(
    conn: &mut crate::storage::postgres::PostgresConnection,
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
        .await
        .map_err(ApiError::from)
}

#[cfg(test)]
pub(crate) async fn count_event_deliveries_for_event(
    pool: &impl crate::storage::StorageContext,
    event_id_value: i64,
) -> Result<i64, ApiError> {
    use crate::schema::event_deliveries::dsl::{event_deliveries, event_id};

    with_connection(pool, async |conn| {
        event_deliveries
            .filter(event_id.eq(event_id_value))
            .count()
            .get_result::<i64>(conn)
            .await
    })
    .await
}
