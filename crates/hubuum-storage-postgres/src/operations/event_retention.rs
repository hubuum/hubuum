use chrono::{NaiveDateTime, Utc};
use diesel::prelude::{ExpressionMethods, QueryDsl};
use diesel::sql_types::{Array, BigInt, Bool, Timestamp};
use diesel::{Queryable, QueryableByName};
use diesel_async::RunQueryDsl;
use hubuum_domain::EventRetentionSettings;
use hubuum_storage_core::{EventArchive, EventRetentionSummary, RetainedEvent, StorageError};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use crate::operations::maintenance::maintenance_state_on_connection;
use crate::{PostgresConnection, PostgresRevision, PostgresRuntime, PostgresStorageError};

const EVENT_RETENTION_LOCK_KEY: i64 = 4_850_188_191_125_218;

#[derive(QueryableByName)]
struct AdvisoryLockRow {
    #[diesel(sql_type = Bool)]
    locked: bool,
}

#[derive(QueryableByName)]
struct EventIdRow {
    #[diesel(sql_type = BigInt)]
    id: i64,
}

/// PostgreSQL's complete serialized archive representation of an event.
///
/// Coordination fields are retained in the archive for compatibility with
/// the existing JSONL format, but this row never crosses the storage boundary
/// directly.
#[derive(Queryable, Serialize)]
struct RetentionEventRow {
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
    dispatched_at: Option<NaiveDateTime>,
    fanout_locked_until: Option<NaiveDateTime>,
    fanout_claim_token: Option<Uuid>,
    initiator_user_id: Option<i32>,
    task_id: Option<i32>,
    before_revision: Option<PostgresRevision>,
    after_revision: Option<PostgresRevision>,
}

/// Try to become the event-retention coordinator for the current transaction.
///
/// This narrow entrypoint exists for adapter concurrency tests. Normal callers
/// use [`process_event_retention_batch`].
#[doc(hidden)]
pub async fn try_acquire_event_retention_lock(
    connection: &mut PostgresConnection,
) -> Result<bool, PostgresStorageError> {
    Ok(
        diesel::sql_query("SELECT pg_try_advisory_xact_lock($1) AS locked")
            .bind::<BigInt, _>(EVENT_RETENTION_LOCK_KEY)
            .get_result::<AdvisoryLockRow>(connection)
            .await?
            .locked,
    )
}

/// Select, optionally archive, and purge one retention batch atomically.
pub async fn process_event_retention_batch(
    runtime: &PostgresRuntime,
    settings: EventRetentionSettings,
    archive: &dyn EventArchive,
) -> Result<EventRetentionSummary, PostgresStorageError> {
    runtime
        .with_transaction(async |connection| -> Result<_, PostgresStorageError> {
            if !maintenance_state_on_connection(connection)
                .await?
                .is_normal()
            {
                return Ok(EventRetentionSummary::default());
            }
            if !try_acquire_event_retention_lock(connection).await? {
                return Ok(EventRetentionSummary::default());
            }

            let events = select_events_for_retention_purge(connection, settings).await?;
            let retained_events = events
                .into_iter()
                .map(|event| {
                    let id = event.id;
                    serde_json::to_string(&event)
                        .map(|json| RetainedEvent::new(id, json))
                        .map_err(|error| {
                            PostgresStorageError::database(format!(
                                "Failed to serialize retained PostgreSQL event: {error}"
                            ))
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            archive
                .archive(&retained_events)
                .map_err(archive_callback_error)?;
            let event_ids = retained_events
                .iter()
                .map(RetainedEvent::id)
                .collect::<Vec<_>>();
            purge_event_retention_batch(connection, settings, &event_ids).await
        })
        .await
}

fn archive_callback_error(error: StorageError) -> PostgresStorageError {
    // The archive is an application-owned callback invoked inside the adapter
    // transaction. Preserve its already-bounded classification so the caller
    // receives the original failure after PostgreSQL rolls the transaction
    // back; no driver or application error type crosses either boundary.
    let (kind, message, current_etag) = error.into_parts();
    PostgresStorageError::new(kind, message, current_etag)
}

async fn select_events_for_retention_purge(
    connection: &mut PostgresConnection,
    settings: EventRetentionSettings,
) -> Result<Vec<RetentionEventRow>, PostgresStorageError> {
    let cutoff = settings
        .event_cutoff(Utc::now().naive_utc())
        .ok_or_else(|| {
            PostgresStorageError::database("Event retention exceeds the PostgreSQL timestamp range")
        })?;
    let ids = select_event_ids_for_retention_purge(connection, cutoff, settings.query_batch_size())
        .await?;
    if ids.is_empty() {
        return Ok(Vec::new());
    }

    use crate::schema::events::dsl::{events, id};
    events
        .filter(id.eq_any(ids))
        .order(id.asc())
        .load::<RetentionEventRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn purge_event_retention_batch(
    connection: &mut PostgresConnection,
    settings: EventRetentionSettings,
    event_ids: &[i64],
) -> Result<EventRetentionSummary, PostgresStorageError> {
    let delivery_cutoff = settings
        .delivery_cutoff(Utc::now().naive_utc())
        .ok_or_else(|| {
            PostgresStorageError::database(
                "Event delivery retention exceeds the PostgreSQL timestamp range",
            )
        })?;
    let purged_terminal_deliveries =
        purge_terminal_event_deliveries(connection, delivery_cutoff, settings.query_batch_size())
            .await?;
    let purged_events = purge_events_by_id(connection, event_ids).await?;
    Ok(EventRetentionSummary::new(
        purged_events,
        purged_terminal_deliveries,
    ))
}

async fn select_event_ids_for_retention_purge(
    connection: &mut PostgresConnection,
    cutoff: NaiveDateTime,
    batch_size: i64,
) -> Result<Vec<i64>, PostgresStorageError> {
    diesel::sql_query(
        "SELECT e.id
         FROM events e
         WHERE e.occurred_at < $1
           AND e.dispatched_at IS NOT NULL
           AND NOT EXISTS (
             SELECT 1
             FROM event_deliveries d
             WHERE d.event_id = e.id
               AND d.status IN ('pending', 'failed', 'in_flight')
         )
         ORDER BY e.occurred_at ASC, e.id ASC
         LIMIT $2
         FOR UPDATE OF e SKIP LOCKED",
    )
    .bind::<Timestamp, _>(cutoff)
    .bind::<BigInt, _>(batch_size)
    .load::<EventIdRow>(connection)
    .await
    .map(|rows| rows.into_iter().map(|row| row.id).collect())
    .map_err(PostgresStorageError::from)
}

async fn purge_terminal_event_deliveries(
    connection: &mut PostgresConnection,
    cutoff: NaiveDateTime,
    batch_size: i64,
) -> Result<usize, PostgresStorageError> {
    diesel::sql_query(
        "WITH candidates AS (
             SELECT id
             FROM event_deliveries
             WHERE updated_at < $1
               AND status IN ('succeeded', 'dead')
             ORDER BY updated_at ASC, id ASC
             LIMIT $2
             FOR UPDATE SKIP LOCKED
         )
         DELETE FROM event_deliveries AS delivery
         USING candidates
         WHERE delivery.id = candidates.id",
    )
    .bind::<Timestamp, _>(cutoff)
    .bind::<BigInt, _>(batch_size)
    .execute(connection)
    .await
    .map_err(PostgresStorageError::from)
}

async fn purge_events_by_id(
    connection: &mut PostgresConnection,
    event_ids: &[i64],
) -> Result<usize, PostgresStorageError> {
    if event_ids.is_empty() {
        return Ok(0);
    }

    diesel::sql_query("SELECT set_config('events.allow_purge', 'on', true)")
        .execute(&mut *connection)
        .await?;
    diesel::sql_query(
        "DELETE FROM events e
         WHERE e.id = ANY($1)
           AND e.dispatched_at IS NOT NULL
           AND NOT EXISTS (
             SELECT 1
             FROM event_deliveries d
             WHERE d.event_id = e.id
               AND d.status IN ('pending', 'failed', 'in_flight')
           )",
    )
    .bind::<Array<BigInt>, _>(event_ids)
    .execute(connection)
    .await
    .map_err(PostgresStorageError::from)
}

/// Run retention without an external archive. Intended for adapter tests.
#[doc(hidden)]
pub async fn purge_without_archive(
    runtime: &PostgresRuntime,
    settings: EventRetentionSettings,
) -> Result<EventRetentionSummary, PostgresStorageError> {
    struct DiscardArchive;

    impl EventArchive for DiscardArchive {
        fn archive(&self, _events: &[RetainedEvent]) -> Result<(), StorageError> {
            Ok(())
        }
    }

    process_event_retention_batch(runtime, settings, &DiscardArchive).await
}
