use crate::PostgresRevision;
use hubuum_domain::CollectionId;
use hubuum_domain::{EventDeliveryPolicy, EventDeliveryPurpose};
use hubuum_events_core::EventSubscriptionScope;
use hubuum_storage_core::StorageEventDeliveryConfiguration;
use hubuum_storage_core::StorageEventDeliveryDisposition;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use chrono::{NaiveDateTime, Utc};
#[cfg(feature = "integration-test-support")]
use diesel::BoolExpressionMethods;
use diesel::prelude::{ExpressionMethods, QueryDsl};
use diesel::sql_types::{BigInt, Nullable, Timestamp};
use diesel::{OptionalExtension, Queryable, QueryableByName, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_domain::{
    EventDeliveryId, EventDeliverySettings, EventDeliveryStatus, EventSinkId, EventSubscriptionId,
};
use hubuum_events_core::EventSequence;
use hubuum_query::{FilterField, Operator, QueryOptions};
use hubuum_storage_core::{
    StorageEventDelivery, StorageEventDeliveryBatch, StorageEventDeliveryClaim,
    StorageEventDeliveryLease, StorageEventDeliveryListQuery, StorageEventDeliverySink,
    StorageEventDeliverySubscription, StorageEventDeliveryWorkItem, StoragePage,
};
use serde_json::Value;
use uuid::Uuid;

use crate::operations::maintenance::maintenance_state_on_connection;
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

use super::event_rows::{StoredEventProjection, enrich_stored_events};

#[derive(Queryable)]
struct DeliveryRow {
    id: i64,
    event_id: i64,
    subscription_id: i32,
    attempts: i32,
    claim_token: Option<Uuid>,
    purpose: String,
}

#[derive(Queryable)]
struct DeliverySubscriptionRow {
    id: i32,
    sink_id: i32,
    name: String,
    routing: Value,
    collection_id: Option<i32>,
    revision: PostgresRevision,
}

#[derive(Queryable)]
struct DeliverySinkRow {
    id: i32,
    name: String,
    kind: String,
    configuration: Value,
    secret_ref: Option<String>,
    revision: PostgresRevision,
}

fn invalid_delivery_value(
    projection: &'static str,
    error: impl std::fmt::Debug,
) -> PostgresStorageError {
    PostgresStorageError::invalid_persisted_value(projection, error)
}

fn delivery_subscription_value(
    row: &DeliverySubscriptionRow,
) -> Result<StorageEventDeliverySubscription, PostgresStorageError> {
    StorageEventDeliverySubscription::try_new(
        EventSubscriptionId::new(row.id)?,
        row.name.clone(),
        row.routing.clone(),
    )
    .map_err(|error| invalid_delivery_value("event delivery subscription", error))
}

fn delivery_sink_value(
    row: &DeliverySinkRow,
) -> Result<StorageEventDeliverySink, PostgresStorageError> {
    StorageEventDeliverySink::try_new(
        EventSinkId::new(row.id)?,
        row.name.clone(),
        row.kind.clone(),
        row.configuration.clone(),
        row.secret_ref.clone(),
    )
    .map_err(|error| invalid_delivery_value("event delivery sink", error))
}

#[derive(Queryable)]
pub(super) struct AdministrationDeliveryRow {
    id: i64,
    event_id: i64,
    subscription_id: i32,
    status: String,
    attempts: i32,
    next_attempt_at: NaiveDateTime,
    last_error: Option<String>,
    locked_until: Option<NaiveDateTime>,
    _claim_token: Option<Uuid>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    purpose: String,
    deferred_reason: Option<String>,
}

impl TryFrom<AdministrationDeliveryRow> for StorageEventDelivery {
    type Error = PostgresStorageError;

    fn try_from(row: AdministrationDeliveryRow) -> Result<Self, Self::Error> {
        let status = row.status.parse::<EventDeliveryStatus>().map_err(|error| {
            PostgresStorageError::invalid_persisted_value("event delivery status", error)
        })?;
        Self::builder(
            EventDeliveryId::new(row.id)?,
            EventSequence::new(row.event_id)?,
            EventSubscriptionId::new(row.subscription_id)?,
            status,
            row.next_attempt_at.and_utc(),
            row.created_at.and_utc(),
            row.updated_at.and_utc(),
        )
        .purpose(
            EventDeliveryPurpose::parse(&row.purpose)
                .ok_or_else(|| PostgresStorageError::invalid_input("Invalid delivery purpose"))?,
        )
        .deferred_reason(row.deferred_reason)
        .attempts(row.attempts)
        .last_error(row.last_error)
        .locked_until(row.locked_until.map(|timestamp| timestamp.and_utc()))
        .try_build()
        .map_err(|error| {
            PostgresStorageError::invalid_persisted_value("event delivery projection", error)
        })
    }
}

#[derive(QueryableByName)]
struct ScheduledDeliveryWakeup {
    #[diesel(sql_type = Nullable<Timestamp>)]
    wakeup_at: Option<NaiveDateTime>,
}

/// Atomically claim and fully enrich one bounded batch of due deliveries.
///
/// Selection, claim mutation, and all enrichment queries share one
/// transaction. If persisted event, subscription, sink, or provenance data
/// cannot be converted into the backend-neutral work item, the claim rolls
/// back instead of leaving an in-flight row for a worker that never received
/// it.
pub async fn claim_event_delivery_batch(
    runtime: &PostgresRuntime,
    settings: EventDeliverySettings,
) -> Result<StorageEventDeliveryBatch, PostgresStorageError> {
    runtime
        .with_transaction(
            async |connection| -> Result<StorageEventDeliveryBatch, PostgresStorageError> {
                if !maintenance_state_on_connection(connection)
                    .await?
                    .is_normal()
                {
                    return Ok(StorageEventDeliveryBatch::default());
                }

                let now = Utc::now().naive_utc();
                let delivery_ids = select_due_delivery_ids(connection, now, settings).await?;
                if delivery_ids.is_empty() {
                    let next_wakeup_in = next_wakeup_on_connection(connection, now).await?;
                    return Ok(StorageEventDeliveryBatch::new(Vec::new(), next_wakeup_in));
                }

                let deliveries =
                    claim_delivery_ids(connection, &delivery_ids, now, settings).await?;
                let work_items = load_work_items(connection, deliveries).await?;
                crate::reach_fault_point(
                    crate::PostgresFaultPoint::EventDeliveryAfterClaim,
                    Some(connection),
                )
                .await?;
                Ok(StorageEventDeliveryBatch::new(work_items, None))
            },
        )
        .await
}

#[cfg(feature = "integration-test-support")]
pub(crate) async fn load_event_delivery_for_event_for_test(
    runtime: &PostgresRuntime,
    event_sequence: EventSequence,
) -> Result<StorageEventDelivery, PostgresStorageError> {
    runtime
        .with_connection(async move |connection| {
            use crate::schema::event_deliveries::dsl::{event_deliveries, event_id};

            event_deliveries
                .filter(event_id.eq(event_sequence.get()))
                .first::<AdministrationDeliveryRow>(connection)
                .await?
                .try_into()
        })
        .await
}

#[cfg(feature = "integration-test-support")]
pub(crate) async fn set_event_delivery_status_for_test(
    runtime: &PostgresRuntime,
    delivery_id: EventDeliveryId,
    delivery_status: EventDeliveryStatus,
) -> Result<(), PostgresStorageError> {
    runtime
        .with_connection(async move |connection| {
            use crate::schema::event_deliveries::dsl::{
                claim_token, event_deliveries, id, last_error, locked_until, status,
            };

            let target = event_deliveries.filter(id.eq(delivery_id.id()));
            let updated = match delivery_status {
                EventDeliveryStatus::Pending | EventDeliveryStatus::Succeeded => {
                    diesel::update(target)
                        .set((
                            status.eq(delivery_status.as_str()),
                            claim_token.eq::<Option<Uuid>>(None),
                            locked_until.eq::<Option<NaiveDateTime>>(None),
                            last_error.eq::<Option<String>>(None),
                        ))
                        .execute(connection)
                        .await?
                }
                EventDeliveryStatus::Failed | EventDeliveryStatus::Dead => {
                    diesel::update(target)
                        .set((
                            status.eq(delivery_status.as_str()),
                            claim_token.eq::<Option<Uuid>>(None),
                            locked_until.eq::<Option<NaiveDateTime>>(None),
                            last_error.eq(Some("test delivery failure".to_string())),
                        ))
                        .execute(connection)
                        .await?
                }
                EventDeliveryStatus::InFlight => {
                    diesel::update(target)
                        .set((
                            status.eq(delivery_status.as_str()),
                            claim_token.eq(Some(Uuid::new_v4())),
                            locked_until
                                .eq(Some(Utc::now().naive_utc() + chrono::Duration::minutes(1))),
                            last_error.eq::<Option<String>>(None),
                        ))
                        .execute(connection)
                        .await?
                }
            };
            if updated == 1 {
                Ok(())
            } else {
                Err(PostgresStorageError::not_found("event delivery not found"))
            }
        })
        .await
}

#[cfg(feature = "integration-test-support")]
pub(crate) async fn set_event_delivery_claim_token_for_test(
    runtime: &PostgresRuntime,
    delivery_id: EventDeliveryId,
    delivery_claim_token: Uuid,
) -> Result<(), PostgresStorageError> {
    runtime
        .with_connection(async move |connection| {
            use crate::schema::event_deliveries::dsl::{claim_token, event_deliveries, id};

            let updated = diesel::update(event_deliveries.filter(id.eq(delivery_id.id())))
                .set(claim_token.eq(Some(delivery_claim_token)))
                .execute(connection)
                .await?;
            if updated == 1 {
                Ok(())
            } else {
                Err(PostgresStorageError::not_found("event delivery not found"))
            }
        })
        .await
}

async fn select_due_delivery_ids(
    connection: &mut PostgresConnection,
    now: NaiveDateTime,
    settings: EventDeliverySettings,
) -> Result<Vec<i64>, PostgresStorageError> {
    #[derive(QueryableByName)]
    struct Candidate {
        #[diesel(sql_type = BigInt)]
        id: i64,
    }
    // The materialized candidates use the statement snapshot. Recheck delivery
    // eligibility on the row being locked so PostgreSQL can exclude a claim or
    // deferral committed by another worker after that snapshot was taken.
    diesel::sql_query("WITH candidates AS MATERIALIZED (
        SELECT d.id, s.delivery_policy->>'min_interval_ms' IS NOT NULL AS spaced,
               row_number() OVER (PARTITION BY s.id ORDER BY d.next_attempt_at, d.id) AS position
        FROM event_deliveries d JOIN event_subscriptions sub ON sub.id=d.subscription_id JOIN event_sinks s ON s.id=sub.sink_id
        LEFT JOIN event_sink_delivery_state schedule ON schedule.sink_id=s.id
        WHERE ((d.status IN ('pending','failed') AND d.next_attempt_at <= $1) OR (d.status='in_flight' AND d.locked_until < $1))
          AND greatest(schedule.blocked_until, CASE WHEN s.delivery_policy->>'min_interval_ms' IS NOT NULL THEN schedule.next_allowed_at END, $1) <= $1
    ) SELECT d.id FROM candidates c JOIN event_deliveries d ON d.id=c.id
      WHERE (NOT c.spaced OR c.position=1)
        AND ((d.status IN ('pending','failed') AND d.next_attempt_at <= $1) OR (d.status='in_flight' AND d.locked_until < $1))
      ORDER BY d.next_attempt_at, d.id
      LIMIT $2 FOR UPDATE OF d SKIP LOCKED")
        .bind::<Timestamp,_>(now).bind::<BigInt,_>(settings.query_batch_size())
        .load::<Candidate>(connection).await.map(|rows| rows.into_iter().map(|row| row.id).collect()).map_err(PostgresStorageError::from)
}

async fn claim_delivery_ids(
    connection: &mut PostgresConnection,
    delivery_ids: &[i64],
    now: NaiveDateTime,
    settings: EventDeliverySettings,
) -> Result<Vec<DeliveryRow>, PostgresStorageError> {
    use crate::schema::event_deliveries::dsl::{
        attempts, claim_token, event_deliveries, event_id, id, locked_until, status,
        subscription_id,
    };

    let lock_deadline = settings.lock_deadline(now).ok_or_else(|| {
        PostgresStorageError::database(
            "Event delivery lock timeout exceeds the PostgreSQL timestamp range",
        )
    })?;
    let claim = Uuid::new_v4();
    diesel::update(event_deliveries.filter(id.eq_any(delivery_ids)))
        .set((
            status.eq(EventDeliveryStatus::InFlight.as_str()),
            locked_until.eq(Some(lock_deadline)),
            claim_token.eq(Some(claim)),
        ))
        .returning((
            id,
            event_id,
            subscription_id,
            attempts,
            claim_token,
            crate::schema::event_deliveries::purpose,
        ))
        .get_results::<DeliveryRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn load_work_items(
    connection: &mut PostgresConnection,
    deliveries: Vec<DeliveryRow>,
) -> Result<Vec<StorageEventDeliveryWorkItem>, PostgresStorageError> {
    use crate::schema::{event_sinks, event_subscriptions, events};

    let event_ids = deliveries
        .iter()
        .map(|delivery| delivery.event_id)
        .collect::<Vec<_>>();
    let subscription_ids = deliveries
        .iter()
        .map(|delivery| delivery.subscription_id)
        .collect::<Vec<_>>();
    let mut event_rows = events::table
        .filter(events::id.eq_any(&event_ids))
        .select(StoredEventProjection::as_select())
        .load::<StoredEventProjection>(connection)
        .await?;
    let principal_names = enrich_stored_events(connection, &mut event_rows).await?;
    let loaded_events = event_rows
        .into_iter()
        .map(|event| (event.id, event))
        .collect::<HashMap<_, _>>();

    let loaded_subscriptions = event_subscriptions::table
        .filter(event_subscriptions::id.eq_any(&subscription_ids))
        .select((
            event_subscriptions::id,
            event_subscriptions::sink_id,
            event_subscriptions::name,
            event_subscriptions::routing,
            event_subscriptions::collection_id,
            event_subscriptions::revision,
        ))
        .load::<DeliverySubscriptionRow>(connection)
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
        .select((
            event_sinks::id,
            event_sinks::name,
            event_sinks::kind,
            event_sinks::config,
            event_sinks::secret_ref,
            event_sinks::revision,
        ))
        .load::<DeliverySinkRow>(connection)
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
                .ok_or_else(|| PostgresStorageError::not_found("Event for delivery not found"))?;
            let subscription = loaded_subscriptions
                .get(&delivery.subscription_id)
                .ok_or_else(|| {
                    PostgresStorageError::not_found("Event subscription for delivery not found")
                })?;
            let sink = loaded_sinks.get(&subscription.sink_id).ok_or_else(|| {
                PostgresStorageError::not_found("Event sink for delivery subscription not found")
            })?;
            let claim_token = delivery.claim_token.ok_or_else(|| {
                PostgresStorageError::database(
                    "Claimed event delivery is missing its PostgreSQL claim token",
                )
            })?;

            let claim = StorageEventDeliveryClaim::try_new(
                EventDeliveryId::new(delivery.id)?,
                delivery.attempts,
                claim_token,
            )
            .map_err(|error| invalid_delivery_value("event delivery claim", error))?
            .with_configuration(StorageEventDeliveryConfiguration::new(
                EventSinkId::new(sink.id)?,
                sink.revision.into_domain(),
                subscription.revision.into_domain(),
            ));
            let scope = subscription
                .collection_id
                .map(CollectionId::new)
                .transpose()?
                .map_or(EventSubscriptionScope::System, EventSubscriptionScope::from);
            let envelope = event
                .into_envelope(&principal_names)?
                .for_subscription_scope(scope);
            let subscription =
                delivery_subscription_value(subscription)?.for_test(delivery.purpose == "test");
            let sink = delivery_sink_value(sink)?;

            Ok(StorageEventDeliveryWorkItem::new(
                claim,
                envelope,
                subscription,
                sink,
            ))
        })
        .collect()
}

async fn next_wakeup_on_connection(
    connection: &mut PostgresConnection,
    now: NaiveDateTime,
) -> Result<Option<Duration>, PostgresStorageError> {
    let schedule = diesel::sql_query(
        "WITH scheduled AS (
             SELECT greatest(
                 CASE WHEN d.status = 'in_flight' THEN d.locked_until ELSE d.next_attempt_at END,
                 schedule.blocked_until,
                 CASE WHEN s.delivery_policy->>'min_interval_ms' IS NOT NULL THEN schedule.next_allowed_at END
             ) AS wakeup_at
             FROM event_deliveries d
             JOIN event_subscriptions sub ON sub.id=d.subscription_id
             JOIN event_sinks s ON s.id=sub.sink_id
             LEFT JOIN event_sink_delivery_state schedule ON schedule.sink_id=s.id
             WHERE d.status IN ('pending','failed','in_flight')
         ) SELECT MIN(wakeup_at) AS wakeup_at FROM scheduled WHERE wakeup_at > $1",
    )
    .bind::<Timestamp, _>(now)
    .get_result::<ScheduledDeliveryWakeup>(connection)
    .await?;

    Ok(schedule.wakeup_at.map(|wakeup_at| {
        wakeup_at
            .signed_duration_since(now)
            .to_std()
            .unwrap_or_default()
    }))
}

/// Check ownership using the database clock without extending an expired lease.
pub async fn begin_event_delivery(
    runtime: &PostgresRuntime,
    claim: &StorageEventDeliveryClaim,
) -> Result<Option<StorageEventDeliveryLease>, PostgresStorageError> {
    #[derive(QueryableByName)]
    struct RemainingLease {
        #[diesel(sql_type = BigInt)]
        remaining_micros: i64,
    }

    let check_started = Instant::now();
    let result = runtime
        .with_transaction(async |connection| {
            if !admit_sink_delivery(connection, claim).await? {
                return Ok(None);
            }
            let remaining = diesel::sql_query(
                "SELECT floor(extract(epoch FROM
                    (locked_until - (clock_timestamp() AT TIME ZONE 'UTC'))) * 1000000)::bigint
                    AS remaining_micros
                 FROM event_deliveries
                 WHERE id = $1 AND claim_token = $2 AND status = 'in_flight'
                   AND locked_until > (clock_timestamp() AT TIME ZONE 'UTC')",
            )
            .bind::<BigInt, _>(claim.delivery_id().id())
            .bind::<diesel::sql_types::Uuid, _>(claim.token())
            .get_result::<RemainingLease>(connection)
            .await
            .optional()?;
            remaining
                .filter(|value| value.remaining_micros > 0)
                .map(|value| {
                    StorageEventDeliveryLease::try_new(
                        claim.clone(),
                        check_started,
                        Duration::from_micros(value.remaining_micros as u64),
                    )
                    .map_err(|error| invalid_delivery_value("event delivery lease", error))
                })
                .transpose()
        })
        .await?;
    crate::reach_fault_point(
        crate::PostgresFaultPoint::EventDeliveryAfterOwnershipCheck,
        None,
    )
    .await?;
    Ok(result)
}

/// Serialize admissions for a sink, without holding a database lock during HTTP.
async fn admit_sink_delivery(
    connection: &mut PostgresConnection,
    claim: &StorageEventDeliveryClaim,
) -> Result<bool, PostgresStorageError> {
    #[derive(QueryableByName)]
    struct Admission {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        sink_id: i32,
        #[diesel(sql_type = diesel::sql_types::Jsonb)]
        delivery_policy: Value,
        #[diesel(sql_type = Timestamp)]
        now: NaiveDateTime,
    }
    // Match policy updates' lock order: sink, delivery, schedule. A sink lock
    // also prevents admitting against a policy that changed while we waited.
    let row = diesel::sql_query("SELECT s.id AS sink_id, s.delivery_policy, clock_timestamp() AT TIME ZONE 'UTC' AS now FROM event_deliveries d JOIN event_subscriptions sub ON sub.id=d.subscription_id JOIN event_sinks s ON s.id=sub.sink_id WHERE d.id=$1 AND d.claim_token=$2 AND d.status='in_flight' AND d.locked_until > (clock_timestamp() AT TIME ZONE 'UTC') FOR NO KEY UPDATE OF s")
        .bind::<BigInt,_>(claim.delivery_id().id()).bind::<diesel::sql_types::Uuid,_>(claim.token())
        .get_result::<Admission>(connection).await.optional()?;
    let Some(row) = row else { return Ok(false) };
    let Some(configuration) = claim.configuration() else {
        return Ok(false);
    };
    #[derive(QueryableByName)]
    struct Authorization {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        allowed: bool,
        #[diesel(sql_type = diesel::sql_types::Bool)]
        unchanged: bool,
    }
    let authority = diesel::sql_query("SELECT
        ((d.purpose = 'test' OR (s.enabled AND sub.enabled)) AND (
            (sub.collection_id IS NULL AND s.collection_id IS NULL) OR
            s.collection_id = sub.collection_id OR
            (s.collection_id IS NULL AND EXISTS (SELECT 1 FROM event_sink_collection_grants g WHERE g.sink_id=s.id AND g.collection_id=sub.collection_id))
        )) IS TRUE AS allowed,
        (s.id=$3 AND s.revision=$4 AND sub.revision=$5) AS unchanged
        FROM event_deliveries d JOIN event_subscriptions sub ON sub.id=d.subscription_id
        JOIN event_sinks s ON s.id=sub.sink_id WHERE d.id=$1 AND d.claim_token=$2
        FOR SHARE OF sub")
        .bind::<BigInt,_>(claim.delivery_id().id()).bind::<diesel::sql_types::Uuid,_>(claim.token())
        .bind::<diesel::sql_types::Integer,_>(configuration.sink_id().id())
        .bind::<BigInt,_>(configuration.sink_revision().get())
        .bind::<BigInt,_>(configuration.subscription_revision().get())
        .get_result::<Authorization>(connection).await.optional()?;
    let Some(authority) = authority else {
        return Ok(false);
    };
    if !authority.allowed || !authority.unchanged {
        diesel::sql_query("UPDATE event_deliveries SET status=$3, claim_token=NULL, locked_until=NULL, last_error=$4, next_attempt_at=clock_timestamp() AT TIME ZONE 'UTC' WHERE id=$1 AND claim_token=$2 AND status='in_flight'")
            .bind::<BigInt,_>(claim.delivery_id().id()).bind::<diesel::sql_types::Uuid,_>(claim.token())
            .bind::<diesel::sql_types::Text,_>(if authority.allowed { "pending" } else { "dead" })
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>,_>((!authority.allowed).then_some("Sink use revoked or destination disabled"))
            .execute(connection).await?;
        return Ok(false);
    }
    let policy: EventDeliveryPolicy = serde_json::from_value(row.delivery_policy)
        .map_err(|error| invalid_delivery_value("delivery policy", error))?;
    #[derive(QueryableByName)]
    struct Clock {
        #[diesel(sql_type = Timestamp)]
        now: NaiveDateTime,
    }
    let owned = diesel::sql_query("SELECT clock_timestamp() AT TIME ZONE 'UTC' AS now FROM event_deliveries WHERE id=$1 AND claim_token=$2 AND status='in_flight' AND locked_until > (clock_timestamp() AT TIME ZONE 'UTC') FOR UPDATE")
        .bind::<BigInt,_>(claim.delivery_id().id()).bind::<diesel::sql_types::Uuid,_>(claim.token()).get_result::<Clock>(connection).await.optional()?;
    if owned.is_none() {
        return Ok(false);
    }
    diesel::sql_query(
        "INSERT INTO event_sink_delivery_state(sink_id) VALUES ($1) ON CONFLICT DO NOTHING",
    )
    .bind::<diesel::sql_types::Integer, _>(row.sink_id)
    .execute(connection)
    .await?;
    #[derive(QueryableByName)]
    struct Schedule {
        #[diesel(sql_type = Timestamp)]
        eligible: NaiveDateTime,
        #[diesel(sql_type = diesel::sql_types::Bool)]
        provider: bool,
    }
    let schedule = diesel::sql_query("SELECT greatest(CASE WHEN $3 THEN next_allowed_at END,blocked_until,$2) AS eligible, blocked_until > $2 AS provider FROM event_sink_delivery_state WHERE sink_id=$1 FOR UPDATE")
        .bind::<diesel::sql_types::Integer,_>(row.sink_id).bind::<Timestamp,_>(row.now)
        .bind::<diesel::sql_types::Bool,_>(policy.min_interval_ms().is_some()).get_result::<Schedule>(connection).await?;
    let clock = diesel::sql_query("SELECT clock_timestamp() AT TIME ZONE 'UTC' AS now FROM event_deliveries WHERE id=$1 AND claim_token=$2 AND locked_until > (clock_timestamp() AT TIME ZONE 'UTC')")
        .bind::<BigInt,_>(claim.delivery_id().id()).bind::<diesel::sql_types::Uuid,_>(claim.token()).get_result::<Clock>(connection).await.optional()?;
    let Some(clock) = clock else { return Ok(false) };
    if schedule.eligible > clock.now {
        diesel::sql_query("UPDATE event_deliveries SET status='pending', next_attempt_at=$2, deferred_reason=$3, claim_token=NULL, locked_until=NULL, last_error=NULL WHERE id=$1")
            .bind::<BigInt,_>(claim.delivery_id().id()).bind::<Timestamp,_>(schedule.eligible)
            .bind::<diesel::sql_types::Text,_>(if schedule.provider { "provider_rate" } else { "configured_rate" }).execute(connection).await?;
        return Ok(false);
    }
    let next =
        clock.now + chrono::Duration::milliseconds(policy.min_interval_ms().unwrap_or(0) as i64);
    diesel::sql_query("UPDATE event_sink_delivery_state SET next_allowed_at=$2 WHERE sink_id=$1")
        .bind::<diesel::sql_types::Integer, _>(row.sink_id)
        .bind::<Timestamp, _>(next)
        .execute(connection)
        .await?;
    diesel::sql_query("UPDATE event_deliveries SET deferred_reason=NULL WHERE id=$1")
        .bind::<BigInt, _>(claim.delivery_id().id())
        .execute(connection)
        .await?;
    Ok(true)
}

/// The caller holds the sink lock. Keep provider cooldowns and retry backoffs
/// intact while moving configured deadlines relative to the last admission.
pub(super) async fn reconcile_sink_delivery_policy(
    connection: &mut PostgresConnection,
    sink_id: EventSinkId,
    before: EventDeliveryPolicy,
    after: EventDeliveryPolicy,
) -> Result<(), PostgresStorageError> {
    use crate::schema::{event_deliveries, event_subscriptions};

    // Lock deliveries before the schedule, as completion also uses that order.
    // Include in-flight rows whose completion might establish a provider delay.
    event_deliveries::table
        .filter(
            event_deliveries::subscription_id.eq_any(
                event_subscriptions::table
                    .filter(event_subscriptions::sink_id.eq(sink_id.id()))
                    .select(event_subscriptions::id),
            ),
        )
        .filter(event_deliveries::status.eq_any(["pending", "failed", "in_flight"]))
        .select(event_deliveries::id)
        .for_update()
        .load::<i64>(connection)
        .await?;
    let now = Utc::now().naive_utc();
    diesel::sql_query(
        "UPDATE event_sink_delivery_state SET next_allowed_at = CASE
        WHEN $2::bigint IS NULL OR $3::bigint IS NULL THEN $4
        ELSE next_allowed_at + (($3 - $2) * interval '1 millisecond') END
        WHERE sink_id=$1",
    )
    .bind::<diesel::sql_types::Integer, _>(sink_id.id())
    .bind::<Nullable<BigInt>, _>(before.min_interval_ms().map(|value| value as i64))
    .bind::<Nullable<BigInt>, _>(after.min_interval_ms().map(|value| value as i64))
    .bind::<Timestamp, _>(now)
    .execute(connection)
    .await?;
    diesel::sql_query("UPDATE event_deliveries d SET
        next_attempt_at=greatest(schedule.next_allowed_at,schedule.blocked_until,$2),
        deferred_reason=CASE WHEN schedule.blocked_until > $2 THEN 'provider_rate'
                             WHEN schedule.next_allowed_at > $2 THEN 'configured_rate' END
        FROM event_subscriptions sub JOIN event_sink_delivery_state schedule ON schedule.sink_id=sub.sink_id
        WHERE d.subscription_id=sub.id AND sub.sink_id=$1 AND d.status='pending' AND d.deferred_reason IS NOT NULL")
        .bind::<diesel::sql_types::Integer, _>(sink_id.id())
        .bind::<Timestamp, _>(now)
        .execute(connection)
        .await?;
    Ok(())
}

pub async fn finish_event_delivery(
    runtime: &PostgresRuntime,
    claim: &StorageEventDeliveryClaim,
    disposition: StorageEventDeliveryDisposition,
) -> Result<(), PostgresStorageError> {
    runtime.with_transaction(async |connection| -> Result<(), PostgresStorageError> {
        #[derive(QueryableByName)]
        struct Owned {
            #[diesel(sql_type = diesel::sql_types::Integer)]
            sink_id: i32,
            #[diesel(sql_type = Timestamp)]
            now: NaiveDateTime,
        }
        // Policy/name updates and sink deletion lock the sink before its
        // deliveries. Use the same order before recording a provider cooldown.
        diesel::sql_query("SELECT s.id AS sink_id, clock_timestamp() AT TIME ZONE 'UTC' AS now FROM event_deliveries d JOIN event_subscriptions sub ON sub.id=d.subscription_id JOIN event_sinks s ON s.id=sub.sink_id WHERE d.id=$1 AND d.claim_token=$2 AND d.status='in_flight' FOR NO KEY UPDATE OF s")
            .bind::<BigInt,_>(claim.delivery_id().id()).bind::<diesel::sql_types::Uuid,_>(claim.token()).get_result::<Owned>(connection).await?;
        let owned = diesel::sql_query("SELECT sub.sink_id, clock_timestamp() AT TIME ZONE 'UTC' AS now FROM event_deliveries d JOIN event_subscriptions sub ON sub.id=d.subscription_id WHERE d.id=$1 AND d.claim_token=$2 AND d.status='in_flight' AND d.locked_until > (clock_timestamp() AT TIME ZONE 'UTC') FOR UPDATE OF d")
            .bind::<BigInt,_>(claim.delivery_id().id()).bind::<diesel::sql_types::Uuid,_>(claim.token()).get_result::<Owned>(connection).await?;
        match disposition {
            StorageEventDeliveryDisposition::Permanent(ref error) => {
                diesel::sql_query("UPDATE event_deliveries SET status='dead', attempts=attempts+1, last_error=$2, claim_token=NULL, locked_until=NULL, deferred_reason=NULL WHERE id=$1")
                    .bind::<BigInt,_>(claim.delivery_id().id()).bind::<diesel::sql_types::Text,_>(error).execute(connection).await?;
            }
            StorageEventDeliveryDisposition::RateLimited(delay) => {
                let delay = chrono::Duration::from_std(delay).map_err(|_| PostgresStorageError::invalid_input("Provider cooldown exceeds supported duration"))?;
                let until = owned.now.checked_add_signed(delay).ok_or_else(|| PostgresStorageError::invalid_input("Provider cooldown exceeds supported timestamp"))?;
                diesel::sql_query("INSERT INTO event_sink_delivery_state(sink_id,blocked_until) VALUES ($1,$2) ON CONFLICT (sink_id) DO UPDATE SET blocked_until=greatest(event_sink_delivery_state.blocked_until,EXCLUDED.blocked_until)")
                    .bind::<diesel::sql_types::Integer,_>(owned.sink_id).bind::<Timestamp,_>(until).execute(connection).await?;
                diesel::sql_query("UPDATE event_deliveries SET status='pending', next_attempt_at=$2, deferred_reason='provider_rate', last_error=NULL, claim_token=NULL, locked_until=NULL WHERE id=$1")
                    .bind::<BigInt,_>(claim.delivery_id().id()).bind::<Timestamp,_>(until).execute(connection).await?;
            }
        }
        Ok(())
    }).await
}

/// Mark an in-flight claim as successfully delivered.
pub async fn mark_event_delivery_succeeded(
    runtime: &PostgresRuntime,
    claim: &StorageEventDeliveryClaim,
) -> Result<(), PostgresStorageError> {
    use crate::schema::event_deliveries::dsl::{
        claim_token, event_deliveries, id, last_error, locked_until, status,
    };

    runtime
        .with_transaction(async |connection| -> Result<(), PostgresStorageError> {
            diesel::update(
                event_deliveries
                    .filter(id.eq(claim.delivery_id().id()))
                    .filter(claim_token.eq(claim.token()))
                    .filter(status.eq(EventDeliveryStatus::InFlight.as_str()))
                    .filter(locked_until.gt(diesel::dsl::sql::<Nullable<Timestamp>>(
                        "clock_timestamp() AT TIME ZONE 'UTC'",
                    ))),
            )
            .set((
                status.eq(EventDeliveryStatus::Succeeded.as_str()),
                locked_until.eq::<Option<NaiveDateTime>>(None),
                claim_token.eq::<Option<Uuid>>(None),
                last_error.eq::<Option<String>>(None),
            ))
            .returning(id)
            .get_result::<i64>(connection)
            .await?;
            crate::reach_fault_point(
                crate::PostgresFaultPoint::EventDeliveryBeforeAcknowledge,
                Some(connection),
            )
            .await?;
            Ok(())
        })
        .await
}

/// Record a failed delivery and schedule its next retry or terminal state.
pub async fn mark_event_delivery_failed(
    runtime: &PostgresRuntime,
    claim: &StorageEventDeliveryClaim,
    settings: EventDeliverySettings,
    error: &str,
) -> Result<(), PostgresStorageError> {
    use crate::schema::event_deliveries::dsl::{
        attempts, claim_token, event_deliveries, id, last_error, locked_until, next_attempt_at,
        status,
    };

    let next_attempts = claim.attempts() + 1;
    let next_status = if next_attempts >= settings.max_attempts() {
        EventDeliveryStatus::Dead
    } else {
        EventDeliveryStatus::Failed
    };
    let next_attempt = settings
        .retry_deadline(Utc::now().naive_utc(), next_attempts)
        .ok_or_else(|| {
            PostgresStorageError::database(
                "Event delivery retry backoff exceeds the PostgreSQL timestamp range",
            )
        })?;
    let error = truncate_delivery_error(error);

    runtime
        .with_connection(async |connection| {
            diesel::update(
                event_deliveries
                    .filter(id.eq(claim.delivery_id().id()))
                    .filter(claim_token.eq(claim.token()))
                    .filter(status.eq(EventDeliveryStatus::InFlight.as_str()))
                    .filter(locked_until.gt(diesel::dsl::sql::<Nullable<Timestamp>>(
                        "clock_timestamp() AT TIME ZONE 'UTC'",
                    ))),
            )
            .set((
                status.eq(next_status.as_str()),
                attempts.eq(next_attempts),
                next_attempt_at.eq(next_attempt),
                last_error.eq(Some(error)),
                locked_until.eq::<Option<NaiveDateTime>>(None),
                claim_token.eq::<Option<Uuid>>(None),
            ))
            .returning(id)
            .get_result::<i64>(connection)
            .await
            .map(|_| ())
        })
        .await
}

/// List administrator-safe delivery projections without exposing claim
/// tokens or PostgreSQL rows.
pub async fn list_event_deliveries(
    runtime: &PostgresRuntime,
    query: StorageEventDeliveryListQuery,
) -> Result<StoragePage<StorageEventDelivery>, PostgresStorageError> {
    let include_total = query.options().include_total();
    runtime
        .with_read_only_snapshot(
            async |connection| -> Result<StoragePage<StorageEventDelivery>, PostgresStorageError> {
                let total = if include_total {
                    Some(
                        build_administration_delivery_query(
                            query.subscription_id_value().map(EventSubscriptionId::id),
                            query.options(),
                        )?
                        .count()
                        .get_result::<i64>(connection)
                        .await?,
                    )
                } else {
                    None
                };
                let mut records = build_administration_delivery_query(
                    query.subscription_id_value().map(EventSubscriptionId::id),
                    query.options(),
                )?;
                let fields = query
                    .options()
                    .sort()
                    .iter()
                    .map(|sort| administration_delivery_cursor_field(&sort.field))
                    .collect::<Result<Vec<_>, _>>()?;
                crate::apply_query_options_with_fields!(
                    records,
                    query.options(),
                    fields,
                    crate::cursor::CursorTieBreaker::new(
                        FilterField::Id,
                        false,
                        administration_delivery_cursor_field(&FilterField::Id)?,
                    )
                );
                let rows = records
                    .load::<AdministrationDeliveryRow>(connection)
                    .await?
                    .into_iter()
                    .map(StorageEventDelivery::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                crate::persisted_page(rows, total)
            },
        )
        .await
}

/// Load one administrator-safe delivery projection.
pub async fn get_event_delivery(
    runtime: &PostgresRuntime,
    delivery_id: i64,
) -> Result<StorageEventDelivery, PostgresStorageError> {
    use crate::schema::event_deliveries::dsl::{event_deliveries, id};

    runtime
        .with_connection(async |connection| {
            event_deliveries
                .filter(id.eq(delivery_id))
                .first::<AdministrationDeliveryRow>(connection)
                .await
        })
        .await
        .and_then(StorageEventDelivery::try_from)
}

/// Release failed or dead work for immediate retry and notify workers in the
/// same database operation.
pub async fn release_event_delivery_for_retry(
    runtime: &PostgresRuntime,
    delivery_id: i64,
) -> Result<StorageEventDelivery, PostgresStorageError> {
    use crate::schema::event_deliveries::dsl::{
        claim_token, event_deliveries, id, last_error, locked_until, next_attempt_at, status,
    };

    runtime
        .with_transaction(
            async |connection| -> Result<StorageEventDelivery, PostgresStorageError> {
                let delivery = diesel::update(event_deliveries.filter(id.eq(delivery_id)).filter(
                    status.eq_any([
                        EventDeliveryStatus::Failed.as_str(),
                        EventDeliveryStatus::Dead.as_str(),
                    ]),
                ))
                .set((
                    status.eq(EventDeliveryStatus::Pending.as_str()),
                    next_attempt_at.eq(Utc::now().naive_utc()),
                    locked_until.eq::<Option<NaiveDateTime>>(None),
                    claim_token.eq::<Option<Uuid>>(None),
                    last_error.eq::<Option<String>>(None),
                ))
                .get_result::<AdministrationDeliveryRow>(connection)
                .await?;
                notify_event_delivery(connection).await?;
                StorageEventDelivery::try_from(delivery)
            },
        )
        .await
}

/// Mark any non-succeeded delivery terminal while clearing claim state.
pub async fn mark_event_delivery_dead(
    runtime: &PostgresRuntime,
    delivery_id: i64,
) -> Result<StorageEventDelivery, PostgresStorageError> {
    use crate::schema::event_deliveries::dsl::{
        claim_token, event_deliveries, id, last_error, locked_until, status,
    };

    runtime
        .with_connection(async |connection| {
            diesel::update(
                event_deliveries
                    .filter(id.eq(delivery_id))
                    .filter(status.ne(EventDeliveryStatus::Succeeded.as_str())),
            )
            .set((
                status.eq(EventDeliveryStatus::Dead.as_str()),
                locked_until.eq::<Option<NaiveDateTime>>(None),
                claim_token.eq::<Option<Uuid>>(None),
                last_error.eq(Some("marked dead by operator".to_string())),
            ))
            .get_result::<AdministrationDeliveryRow>(connection)
            .await
        })
        .await
        .and_then(StorageEventDelivery::try_from)
}

fn build_administration_delivery_query(
    subscription_filter: Option<i32>,
    options: &QueryOptions,
) -> Result<
    crate::schema::event_deliveries::BoxedQuery<'static, diesel::pg::Pg>,
    PostgresStorageError,
> {
    use crate::schema::event_deliveries::dsl::{
        created_at, event_deliveries, id, next_attempt_at, status, subscription_id, updated_at,
    };

    let mut query = event_deliveries.into_boxed();
    if let Some(subscription_filter) = subscription_filter {
        query = query.filter(subscription_id.eq(subscription_filter));
    }
    for parameter in options.filters() {
        match parameter.field {
            FilterField::Id => {
                let values = hubuum_query::parse_integer_list(&parameter.value)
                    .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?
                    .into_iter()
                    .map(i64::from)
                    .collect::<Vec<_>>();
                let (operator, negated) = parameter.operator.op_and_neg();
                match (operator, negated) {
                    (Operator::Equals | Operator::In, false) => {
                        query = query.filter(id.eq_any(values));
                    }
                    (Operator::Equals | Operator::In, true) => {
                        query = query.filter(diesel::dsl::not(id.eq_any(values)));
                    }
                    _ => {
                        return Err(PostgresStorageError::invalid_input(format!(
                            "Operator '{:?}' not implemented for field '{}' (type: bigint)",
                            parameter.operator, parameter.field
                        )));
                    }
                }
            }
            FilterField::Status => crate::postgres_string_filter!(query, parameter, status),
            FilterField::CreatedAt => {
                crate::postgres_datetime_filter!(query, parameter, created_at)
            }
            FilterField::UpdatedAt => {
                crate::postgres_datetime_filter!(query, parameter, updated_at)
            }
            FilterField::NextAttemptAt => {
                crate::postgres_datetime_filter!(query, parameter, next_attempt_at)
            }
            _ => {
                return Err(PostgresStorageError::invalid_input(format!(
                    "Field '{}' is not searchable for event deliveries",
                    parameter.field
                )));
            }
        }
    }
    Ok(query)
}

fn administration_delivery_cursor_field(
    field: &FilterField,
) -> Result<crate::cursor::CursorSqlField, PostgresStorageError> {
    use crate::cursor::{CursorSqlField, CursorSqlType};

    Ok(match field {
        FilterField::Id => CursorSqlField {
            column: "event_deliveries.id",
            sql_type: CursorSqlType::BigInt,
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
            return Err(PostgresStorageError::invalid_input(format!(
                "Field '{field}' is not orderable for event deliveries"
            )));
        }
    })
}

async fn notify_event_delivery(
    connection: &mut PostgresConnection,
) -> Result<(), PostgresStorageError> {
    diesel::sql_query("SELECT pg_notify($1, $2)")
        .bind::<diesel::sql_types::Text, _>("hubuum_event_delivery")
        .bind::<diesel::sql_types::Text, _>("")
        .execute(connection)
        .await?;
    Ok(())
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

/// Claim one known delivery for adapter compatibility tests.
#[doc(hidden)]
#[cfg(feature = "integration-test-support")]
pub async fn claim_event_delivery_by_id(
    runtime: &PostgresRuntime,
    delivery_id: i64,
    settings: EventDeliverySettings,
) -> Result<StorageEventDeliveryWorkItem, PostgresStorageError> {
    use crate::schema::event_deliveries::dsl::{
        attempts, claim_token, event_deliveries, event_id, id, locked_until, next_attempt_at,
        status, subscription_id,
    };

    runtime
        .with_transaction(
            async |connection| -> Result<StorageEventDeliveryWorkItem, PostgresStorageError> {
                let now = Utc::now().naive_utc();
                let lock_deadline = settings.lock_deadline(now).ok_or_else(|| {
                    PostgresStorageError::database(
                        "Event delivery lock timeout exceeds the PostgreSQL timestamp range",
                    )
                })?;
                let token = Uuid::new_v4();
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
                    claim_token.eq(Some(token)),
                ))
                .returning((
                    id,
                    event_id,
                    subscription_id,
                    attempts,
                    claim_token,
                    crate::schema::event_deliveries::purpose,
                ))
                .get_result::<DeliveryRow>(connection)
                .await?;

                let work_item = load_work_items(connection, vec![delivery])
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| {
                        PostgresStorageError::not_found("Event delivery work item not found")
                    })?;
                crate::reach_fault_point(
                    crate::PostgresFaultPoint::EventDeliveryAfterClaim,
                    Some(connection),
                )
                .await?;
                Ok(work_item)
            },
        )
        .await
}

#[cfg(test)]
mod tests {
    use crate::PostgresRevision;
    use hubuum_storage_core::StorageErrorKind;

    use super::{
        DeliverySinkRow, DeliverySubscriptionRow, delivery_sink_value, delivery_subscription_value,
        truncate_delivery_error,
    };

    #[test]
    fn delivery_error_truncation_preserves_utf8_boundaries() {
        let error = format!("{}é", "x".repeat(4095));

        let truncated = truncate_delivery_error(&error);

        assert_eq!(truncated.len(), 4095);
        assert!(truncated.is_char_boundary(truncated.len()));
    }

    #[test]
    fn corrupt_delivery_transport_values_are_backend_failures() {
        let subscription = DeliverySubscriptionRow {
            collection_id: None,
            revision: PostgresRevision::INITIAL,
            id: 1,
            sink_id: 2,
            name: "subscription".to_string(),
            routing: serde_json::json!([]),
        };
        let sink = DeliverySinkRow {
            revision: PostgresRevision::INITIAL,
            id: 2,
            name: "sink".to_string(),
            kind: "webhook".to_string(),
            configuration: serde_json::json!([]),
            secret_ref: None,
        };

        assert_eq!(
            delivery_subscription_value(&subscription)
                .unwrap_err()
                .kind(),
            StorageErrorKind::Backend
        );
        assert_eq!(
            delivery_sink_value(&sink).unwrap_err().kind(),
            StorageErrorKind::Backend
        );
    }
}

#[cfg(all(test, feature = "integration-test-support"))]
mod scheduling_tests {
    use super::*;
    use crate::test_support::{
        database_role_tests_enabled, integration_test_database_roles,
        integration_test_migration_pool, integration_test_pool,
    };
    use chrono::NaiveDate;
    use diesel::sql_types::{Bool, Integer, Text};
    use diesel_async::SimpleAsyncConnection;
    use rstest::rstest;
    use tokio::time::{sleep, timeout};

    #[derive(QueryableByName)]
    struct BackendPid {
        #[diesel(sql_type = Integer)]
        pid: i32,
    }

    #[derive(QueryableByName)]
    struct SelectionBarrier {
        #[diesel(sql_type = Bool)]
        blocked: bool,
    }

    #[rstest]
    #[case::claimed("pending", "in_flight")]
    #[case::completed("pending", "succeeded")]
    #[case::retry_backoff("failed", "failed")]
    #[case::deferred("pending", "pending")]
    #[case::renewed_lease("in_flight", "in_flight")]
    #[tokio::test]
    async fn selection_rechecks_deliveries_changed_after_its_snapshot(
        #[case] initial_status: &str,
        #[case] concurrent_status: &str,
    ) {
        let pool = if database_role_tests_enabled() {
            integration_test_migration_pool(2)
        } else {
            integration_test_pool(2)
        };
        let mut writer = pool.get().await.unwrap();
        let mut selector = pool.get().await.unwrap();
        if database_role_tests_enabled() {
            let roles = integration_test_database_roles();
            let role = format!("SET ROLE \"{}\"", roles.owner().as_str());
            writer.batch_execute(&role).await.unwrap();
            selector.batch_execute(&role).await.unwrap();
        }
        let writer_pid = diesel::sql_query("SELECT pg_backend_pid() AS pid")
            .get_result::<BackendPid>(&mut writer)
            .await
            .unwrap()
            .pid;
        let selector_pid = diesel::sql_query("SELECT pg_backend_pid() AS pid")
            .get_result::<BackendPid>(&mut selector)
            .await
            .unwrap()
            .pid;
        let schema = format!("delivery_selection_{}", Uuid::new_v4().simple());
        writer
            .batch_execute(&format!(
                "CREATE SCHEMA {schema};
             CREATE TABLE {schema}.event_subscriptions (id integer, sink_id integer);
             CREATE TABLE {schema}.event_deliveries (id bigint PRIMARY KEY, subscription_id integer,
                 status text, next_attempt_at timestamp, locked_until timestamp);
             CREATE TABLE {schema}.event_sink_delivery_state (sink_id integer,
                 next_allowed_at timestamp, blocked_until timestamp);
             CREATE FUNCTION {schema}.selection_barrier() RETURNS jsonb LANGUAGE sql VOLATILE AS
                 'SELECT ''{{}}''::jsonb FROM pg_advisory_xact_lock({writer_pid}::bigint)';
             CREATE VIEW {schema}.event_sinks AS
                 SELECT 1 AS id, {schema}.selection_barrier() AS delivery_policy
                 UNION ALL SELECT 2, '{{}}'::jsonb;
             INSERT INTO {schema}.event_subscriptions VALUES (1,1),(2,2);"
            ))
            .await
            .unwrap();
        let now = NaiveDate::from_ymd_opt(2026, 10, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let result = timeout(Duration::from_secs(10), async {
            diesel::sql_query(format!(
                "INSERT INTO {schema}.event_deliveries VALUES
                 (1,1,$1,$2,$2 - interval '1 second'),(2,2,'pending',$2,NULL)"
            ))
            .bind::<Text, _>(initial_status)
            .bind::<Timestamp, _>(now)
            .execute(&mut writer)
            .await?;
            writer
                .batch_execute(&format!(
                    "BEGIN; SELECT pg_advisory_xact_lock({writer_pid}::bigint);"
                ))
                .await?;
            selector
                .batch_execute(&format!("BEGIN; SET LOCAL search_path TO {schema};"))
                .await?;
            let settings = EventDeliverySettings::builder()
                .batch_size(10)
                .lock_timeout_ms(30_000)
                .transport_timeout_ms(15_000)
                .retry_backoff_base_ms(1_000)
                .retry_backoff_max_ms(60_000)
                .max_attempts(10)
                .build()
                .unwrap();
            // The view pauses the actual production query after it acquires
            // its snapshot, before it can lock deliveries. Wait for that
            // database barrier rather than relying on a timing delay.
            let (selected, changed) = tokio::join!(
                select_due_delivery_ids(&mut selector, now, settings),
                async {
                    loop {
                        let barrier =
                            diesel::sql_query("SELECT $1 = ANY(pg_blocking_pids($2)) AS blocked")
                                .bind::<Integer, _>(writer_pid)
                                .bind::<Integer, _>(selector_pid)
                                .get_result::<SelectionBarrier>(&mut writer)
                                .await?;
                        if barrier.blocked {
                            break;
                        }
                        sleep(Duration::from_millis(5)).await;
                    }
                    diesel::sql_query(format!(
                        "UPDATE {schema}.event_deliveries SET status=$1,
                         next_attempt_at=$2, locked_until=$2 WHERE id=1"
                    ))
                    .bind::<Text, _>(concurrent_status)
                    .bind::<Timestamp, _>(now + chrono::Duration::seconds(60))
                    .execute(&mut writer)
                    .await?;
                    writer.batch_execute("COMMIT").await?;
                    Ok::<_, PostgresStorageError>(())
                },
            );
            changed?;
            selected
        })
        .await;
        // Clean up both transactions and the isolated fixture before asserting.
        writer.batch_execute("ROLLBACK").await.unwrap();
        selector.batch_execute("ROLLBACK").await.unwrap();
        writer
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE; RESET ROLE"))
            .await
            .unwrap();
        selector.batch_execute("RESET ROLE").await.unwrap();
        let selected = result.expect("selection barrier must be released").unwrap();
        assert_eq!(
            selected,
            vec![2],
            "only the unchanged delivery remains eligible"
        );
    }

    #[rstest]
    #[case::configured(true, false, "pending", true)]
    #[case::provider(false, true, "pending", true)]
    #[case::failed(true, false, "failed", true)]
    #[case::expired_lease(false, true, "in_flight", true)]
    #[case::disabled_spacing(false, false, "pending", false)]
    #[tokio::test]
    async fn selection_and_wakeup_respect_sink_cooldowns(
        #[case] configured: bool,
        #[case] provider: bool,
        #[case] status: &str,
        #[case] cooling: bool,
    ) {
        let pool = integration_test_pool(1);
        crate::with_transaction(&pool, async |connection| -> Result<(), PostgresStorageError> {
            // Temporary tables isolate selection from other workers and leave
            // no persistent fixtures, including when an assertion fails.
            connection.batch_execute(
                "CREATE TEMP TABLE event_sinks (id integer, delivery_policy jsonb) ON COMMIT DROP;
                 CREATE TEMP TABLE event_subscriptions (id integer, sink_id integer) ON COMMIT DROP;
                 CREATE TEMP TABLE event_deliveries (id bigint, subscription_id integer, status text,
                     next_attempt_at timestamp, locked_until timestamp) ON COMMIT DROP;
                 CREATE TEMP TABLE event_sink_delivery_state (sink_id integer, next_allowed_at timestamp,
                     blocked_until timestamp) ON COMMIT DROP;
                 INSERT INTO event_subscriptions VALUES (1,1),(2,2);
                 INSERT INTO event_sinks VALUES (2,'{}');"
            ).await?;
            // Keep the clock deterministic and exactly representable by PostgreSQL.
            let now = NaiveDate::from_ymd_opt(2026, 10, 1)
                .unwrap()
                .and_hms_micro_opt(12, 0, 0, 123_456)
                .unwrap();
            let future = now + chrono::Duration::seconds(60);
            diesel::sql_query("INSERT INTO event_sinks VALUES (1,$1)")
                .bind::<diesel::sql_types::Jsonb,_>(serde_json::json!({"min_interval_ms": configured.then_some(60_000)}))
                .execute(connection).await?;
            diesel::sql_query("INSERT INTO event_sink_delivery_state VALUES (1,$1,$2)")
                .bind::<Timestamp,_>(future)
                .bind::<Timestamp,_>(if provider { future } else { now })
                .execute(connection).await?;
            diesel::sql_query("INSERT INTO event_deliveries SELECT n,1,$1,$2,$2 - interval '1 second' FROM generate_series(1,5) n")
                .bind::<diesel::sql_types::Text,_>(status).bind::<Timestamp,_>(now)
                .execute(connection).await?;
            diesel::sql_query("INSERT INTO event_deliveries VALUES (6,2,'pending',$1,NULL)")
                .bind::<Timestamp,_>(now).execute(connection).await?;
            let settings = EventDeliverySettings::builder()
                .batch_size(10)
                .lock_timeout_ms(30_000)
                .transport_timeout_ms(15_000)
                .retry_backoff_base_ms(1_000)
                .retry_backoff_max_ms(60_000)
                .max_attempts(10)
                .build().unwrap();

            let ids = select_due_delivery_ids(connection, now, settings).await?;
            let wakeup = next_wakeup_on_connection(connection, now).await?;

            if cooling {
                assert_eq!(ids, vec![6], "a healthy sink remains eligible while the backlog waits");
                assert_eq!(wakeup, Some(Duration::from_secs(60)));
            } else {
                assert_eq!(ids, vec![1,2,3,4,5,6]);
                assert_eq!(wakeup, None);
            }
            Ok(())
        }).await.unwrap();
    }
}
