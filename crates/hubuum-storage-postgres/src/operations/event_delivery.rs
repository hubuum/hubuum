use std::collections::HashMap;
use std::time::Duration;

use chrono::{NaiveDateTime, Utc};
use diesel::prelude::{BoolExpressionMethods, ExpressionMethods, QueryDsl};
use diesel::sql_types::{Nullable, Timestamp};
use diesel::{Queryable, QueryableByName, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_domain::{
    EventDeliveryId, EventDeliverySettings, EventDeliveryStatus, EventSinkId, EventSubscriptionId,
};
use hubuum_events_core::EventSequence;
use hubuum_query::{FilterField, Operator, QueryOptions};
use hubuum_storage_core::{
    StorageEventDelivery, StorageEventDeliveryBatch, StorageEventDeliveryClaim,
    StorageEventDeliveryListQuery, StorageEventDeliverySink, StorageEventDeliverySubscription,
    StorageEventDeliveryWorkItem, StoragePage,
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
}

#[derive(Queryable)]
struct DeliverySubscriptionRow {
    id: i32,
    sink_id: i32,
    name: String,
    routing: Value,
    collection_id: i32,
}

#[derive(Queryable)]
struct DeliverySinkRow {
    id: i32,
    name: String,
    kind: String,
    configuration: Value,
    secret_ref: Option<String>,
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
        hubuum_domain::CollectionId::new(row.collection_id)?,
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
struct AdministrationDeliveryRow {
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
    use crate::schema::event_deliveries::dsl::{
        event_deliveries, id, locked_until, next_attempt_at, status,
    };

    event_deliveries
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
        .load::<i64>(connection)
        .await
        .map_err(PostgresStorageError::from)
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
        .returning((id, event_id, subscription_id, attempts, claim_token))
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
            .map_err(|error| invalid_delivery_value("event delivery claim", error))?;
            let subscription = delivery_subscription_value(subscription)?;
            let sink = delivery_sink_value(sink)?;

            Ok(StorageEventDeliveryWorkItem::new(
                claim,
                event.into_envelope(&principal_names)?,
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
    .get_result::<ScheduledDeliveryWakeup>(connection)
    .await?;

    Ok(schedule.wakeup_at.map(|wakeup_at| {
        wakeup_at
            .signed_duration_since(now)
            .to_std()
            .unwrap_or_default()
    }))
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
                    .filter(status.eq(EventDeliveryStatus::InFlight.as_str())),
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
                    .filter(status.eq(EventDeliveryStatus::InFlight.as_str())),
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
                .returning((id, event_id, subscription_id, attempts, claim_token))
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
            id: 1,
            collection_id: 1,
            sink_id: 2,
            name: "subscription".to_string(),
            routing: serde_json::json!([]),
        };
        let sink = DeliverySinkRow {
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
