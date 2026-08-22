use std::collections::HashSet;

use chrono::{NaiveDateTime, Utc};
use diesel::prelude::{BoolExpressionMethods, ExpressionMethods, JoinOnDsl, QueryDsl};
use diesel::{QueryResult, Queryable};
use diesel_async::RunQueryDsl;
use hubuum_domain::{CollectionId, EventDeliveryStatus, EventFanoutSettings, PrincipalId, TaskId};
use hubuum_events_core::{
    EventEntityId, EventEnvelope, EventSequence, EventSubscriptionFilter, Provenance,
    ProvenanceActor, ProvenancePrincipal,
};
use serde_json::Value;
use uuid::Uuid;

use crate::operations::maintenance::maintenance_state_on_connection;
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

#[derive(Queryable)]
struct FanoutEventRow {
    id: i64,
    event_id: Uuid,
    occurred_at: NaiveDateTime,
    entity_type: String,
    entity_id: Option<i32>,
    entity_name: Option<String>,
    collection_id: Option<i32>,
    action: String,
    actor_user_id: Option<i32>,
    actor_kind: String,
    request_id: Option<Uuid>,
    correlation_id: Option<String>,
    summary: String,
    before: Option<Value>,
    after: Option<Value>,
    metadata: Value,
    schema_version: i32,
    initiator_user_id: Option<i32>,
    task_id: Option<i32>,
}

impl TryFrom<FanoutEventRow> for EventEnvelope {
    type Error = PostgresStorageError;

    fn try_from(row: FanoutEventRow) -> Result<Self, Self::Error> {
        let actor_user_id = row.actor_user_id.map(PrincipalId::new).transpose()?;
        let initiator_user_id = row.initiator_user_id.map(PrincipalId::new).transpose()?;
        let actor = ProvenanceActor {
            kind: Some(row.actor_kind.clone()),
            principal: actor_user_id.map(|principal_id| ProvenancePrincipal {
                principal_id,
                name: None,
            }),
        };
        let initiator = initiator_user_id.map(|principal_id| ProvenancePrincipal {
            principal_id,
            name: None,
        });
        Ok(Self {
            id: EventSequence::new(row.id)?,
            event_id: row.event_id,
            occurred_at: row.occurred_at,
            entity_type: row.entity_type,
            entity_id: row.entity_id.map(EventEntityId::new).transpose()?,
            entity_name: row.entity_name,
            collection_id: row.collection_id.map(CollectionId::new).transpose()?,
            action: row.action,
            actor_user_id,
            actor_kind: row.actor_kind,
            provenance: Provenance {
                actor,
                initiator,
                task_id: row.task_id.map(TaskId::new).transpose()?,
            },
            request_id: row.request_id,
            correlation_id: row.correlation_id,
            summary: row.summary,
            before: row.before,
            after: row.after,
            metadata: row.metadata,
            schema_version: row.schema_version,
        })
    }
}

#[derive(Queryable)]
struct FanoutSubscriptionRow {
    id: i32,
    collection_id: i32,
    entity_types: Value,
    actions: Value,
    filter: Value,
}

struct CompiledEventSubscription {
    id: i32,
    collection_id: CollectionId,
    entity_types: HashSet<String>,
    actions: HashSet<String>,
    filter: EventSubscriptionFilter,
}

impl TryFrom<FanoutSubscriptionRow> for CompiledEventSubscription {
    type Error = PostgresStorageError;

    fn try_from(subscription: FanoutSubscriptionRow) -> Result<Self, Self::Error> {
        let entity_types = decode_persisted_json(subscription.entity_types, "entity types")?;
        let actions = decode_persisted_json(subscription.actions, "actions")?;
        let filter = decode_persisted_json(subscription.filter, "filter")?;
        Ok(Self {
            id: subscription.id,
            collection_id: CollectionId::new(subscription.collection_id)?,
            entity_types,
            actions,
            filter,
        })
    }
}

impl CompiledEventSubscription {
    fn matches(&self, event: &EventEnvelope) -> bool {
        self.entity_types.contains(&event.entity_type)
            && self.actions.contains(&event.action)
            && (event.collection_id == Some(self.collection_id)
                || event.related_collection_ids().contains(&self.collection_id))
            && self.filter.matches(event)
    }
}

fn decode_persisted_json<T>(value: Value, field: &str) -> Result<T, PostgresStorageError>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(value).map_err(|error| {
        PostgresStorageError::database(format!(
            "Invalid persisted event subscription {field}: {error}"
        ))
    })
}

/// Claim and fan out one bounded batch of pending events.
pub async fn process_event_fanout_batch(
    runtime: &PostgresRuntime,
    settings: EventFanoutSettings,
) -> Result<usize, PostgresStorageError> {
    let event_ids = claim_event_ids(runtime, settings).await?;
    fanout_events(runtime, &event_ids).await
}

/// Claim pending event identifiers for fan-out.
///
/// This narrow entrypoint is public for the adapter's compatibility and
/// concurrency tests. Application code should use
/// [`process_event_fanout_batch`].
#[doc(hidden)]
pub async fn claim_event_ids(
    runtime: &PostgresRuntime,
    settings: EventFanoutSettings,
) -> Result<Vec<i64>, PostgresStorageError> {
    use crate::schema::events::dsl::{
        dispatched_at, events, fanout_claim_token, fanout_locked_until, id, occurred_at,
    };

    runtime
        .with_transaction(
            async |connection| -> Result<Vec<i64>, PostgresStorageError> {
                if !maintenance_state_on_connection(connection)
                    .await?
                    .is_normal()
                {
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
                    .load::<i64>(connection)
                    .await?;
                if event_ids.is_empty() {
                    return Ok(Vec::new());
                }

                let lock_deadline = settings.lock_deadline(now).ok_or_else(|| {
                    PostgresStorageError::database(
                        "Event fan-out lock timeout exceeds the PostgreSQL timestamp range",
                    )
                })?;
                let claim_token = Uuid::new_v4();
                diesel::update(events.filter(id.eq_any(&event_ids)))
                    .set((
                        fanout_locked_until.eq(Some(lock_deadline)),
                        fanout_claim_token.eq(Some(claim_token)),
                    ))
                    .returning(id)
                    .get_results::<i64>(connection)
                    .await
                    .map_err(PostgresStorageError::from)
            },
        )
        .await
}

/// Fan out one event. Intended for adapter integration tests and test support.
#[doc(hidden)]
#[cfg(feature = "integration-test-support")]
pub async fn fanout_event(
    runtime: &PostgresRuntime,
    event_id: i64,
) -> Result<usize, PostgresStorageError> {
    fanout_events(runtime, &[event_id]).await
}

/// Fan out selected pending events atomically.
#[doc(hidden)]
pub async fn fanout_events(
    runtime: &PostgresRuntime,
    event_ids: &[i64],
) -> Result<usize, PostgresStorageError> {
    use crate::schema::events::dsl::{
        action, actor_kind, actor_user_id, after, before, collection_id, correlation_id,
        dispatched_at, entity_id, entity_name, entity_type, event_id, events, fanout_claim_token,
        fanout_locked_until, id, initiator_user_id, metadata, occurred_at, request_id,
        schema_version, summary, task_id,
    };

    if event_ids.is_empty() {
        return Ok(0);
    }

    runtime
        .with_transaction(async |connection| -> Result<usize, PostgresStorageError> {
            let envelopes = events
                .filter(id.eq_any(event_ids))
                .filter(dispatched_at.is_null())
                .order(id.asc())
                .select((
                    id,
                    event_id,
                    occurred_at,
                    entity_type,
                    entity_id,
                    entity_name,
                    collection_id,
                    action,
                    actor_user_id,
                    actor_kind,
                    request_id,
                    correlation_id,
                    summary,
                    before,
                    after,
                    metadata,
                    schema_version,
                    initiator_user_id,
                    task_id,
                ))
                .load::<FanoutEventRow>(connection)
                .await?
                .into_iter()
                .map(EventEnvelope::try_from)
                .collect::<Result<Vec<_>, _>>()?;
            if envelopes.is_empty() {
                return Ok(0);
            }

            let candidate_collection_ids = candidate_subscription_collection_ids(&envelopes);
            let subscriptions = load_enabled_subscriptions(connection, &candidate_collection_ids)
                .await?
                .into_iter()
                .map(CompiledEventSubscription::try_from)
                .collect::<Result<Vec<_>, _>>()?;
            let mut inserted = 0;
            let mut processed_event_ids = Vec::with_capacity(envelopes.len());
            for envelope in &envelopes {
                let subscription_ids = subscriptions
                    .iter()
                    .filter(|subscription| subscription.matches(envelope))
                    .map(|subscription| subscription.id)
                    .collect::<Vec<_>>();
                inserted +=
                    insert_delivery_rows(connection, envelope.id.get(), &subscription_ids).await?;
                processed_event_ids.push(envelope.id.get());
            }

            diesel::update(events.filter(id.eq_any(processed_event_ids)))
                .set((
                    dispatched_at.eq(Some(Utc::now().naive_utc())),
                    fanout_locked_until.eq::<Option<NaiveDateTime>>(None),
                    fanout_claim_token.eq::<Option<Uuid>>(None),
                ))
                .execute(connection)
                .await?;
            if inserted > 0 {
                notify_event_delivery(connection).await?;
            }
            Ok(inserted)
        })
        .await
}

fn candidate_subscription_collection_ids(events: &[EventEnvelope]) -> Vec<i32> {
    let mut collection_ids = HashSet::new();
    for event in events {
        collection_ids.extend(event.collection_id);
        collection_ids.extend(event.related_collection_ids());
    }
    collection_ids.into_iter().map(CollectionId::id).collect()
}

async fn load_enabled_subscriptions(
    connection: &mut PostgresConnection,
    collection_ids: &[i32],
) -> Result<Vec<FanoutSubscriptionRow>, PostgresStorageError> {
    use crate::schema::{event_sinks, event_subscriptions};

    if collection_ids.is_empty() {
        return Ok(Vec::new());
    }

    event_subscriptions::table
        .inner_join(event_sinks::table.on(event_sinks::id.eq(event_subscriptions::sink_id)))
        .filter(event_subscriptions::enabled.eq(true))
        .filter(event_sinks::enabled.eq(true))
        .filter(event_subscriptions::collection_id.eq_any(collection_ids))
        .select((
            event_subscriptions::id,
            event_subscriptions::collection_id,
            event_subscriptions::entity_types,
            event_subscriptions::actions,
            event_subscriptions::filter,
        ))
        .load::<FanoutSubscriptionRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn insert_delivery_rows(
    connection: &mut PostgresConnection,
    event_id_value: i64,
    subscription_ids: &[i32],
) -> Result<usize, PostgresStorageError> {
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
        .execute(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn notify_event_delivery(connection: &mut PostgresConnection) -> QueryResult<usize> {
    diesel::sql_query("SELECT pg_notify($1, $2)")
        .bind::<diesel::sql_types::Text, _>("hubuum_event_delivery")
        .bind::<diesel::sql_types::Text, _>("")
        .execute(connection)
        .await
}
