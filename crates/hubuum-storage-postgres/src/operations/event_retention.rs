use chrono::{NaiveDateTime, Utc};
use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel::sql_types::{Array, BigInt, Bool, Jsonb, Nullable, Timestamp, Uuid as SqlUuid};
use diesel::{Queryable, QueryableByName};
use diesel_async::RunQueryDsl;
use hubuum_domain::EventRetentionSettings;
use hubuum_events_core::EventSequence;
use hubuum_storage_core::{
    StorageEventRetentionBatch, StorageEventRetentionBatchId, StorageEventRetentionSummary,
    StorageRetainedEvent,
};
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

#[derive(QueryableByName)]
struct PurgeSummaryRow {
    #[diesel(sql_type = BigInt)]
    purged_events: i64,
    #[diesel(sql_type = BigInt)]
    purged_terminal_deliveries: i64,
}

#[derive(QueryableByName)]
struct RetentionBatchRow {
    #[diesel(sql_type = SqlUuid)]
    claim_id: Uuid,
    #[diesel(sql_type = Array<BigInt>)]
    event_ids: Vec<i64>,
    #[diesel(sql_type = Jsonb)]
    event_documents: Value,
    #[diesel(sql_type = Nullable<Timestamp>)]
    completed_at: Option<NaiveDateTime>,
    #[diesel(sql_type = Nullable<BigInt>)]
    purged_events: Option<i64>,
    #[diesel(sql_type = Nullable<BigInt>)]
    purged_terminal_deliveries: Option<i64>,
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
/// use `hubuum_storage_core::execute_event_retention_batch`.
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

/// Durably claim one retention batch, or return the existing pending claim.
pub async fn claim_event_retention_batch(
    runtime: &PostgresRuntime,
    settings: EventRetentionSettings,
) -> Result<Option<StorageEventRetentionBatch>, PostgresStorageError> {
    runtime
        .with_transaction(async |connection| -> Result<_, PostgresStorageError> {
            if !maintenance_state_on_connection(connection)
                .await?
                .is_normal()
            {
                return Err(PostgresStorageError::unavailable(
                    "Event retention claiming is paused by maintenance",
                ));
            }
            if !try_acquire_event_retention_lock(connection).await? {
                return Ok(None);
            }

            delete_expired_completed_claims(connection).await?;
            if let Some(existing) = load_pending_claim(connection).await? {
                return retention_batch(existing).map(Some);
            }

            let now = Utc::now().naive_utc();
            let event_cutoff = settings.event_cutoff(now).ok_or_else(|| {
                PostgresStorageError::database(
                    "Event retention exceeds the PostgreSQL timestamp range",
                )
            })?;
            let delivery_cutoff = settings.delivery_cutoff(now).ok_or_else(|| {
                PostgresStorageError::database(
                    "Event delivery retention exceeds the PostgreSQL timestamp range",
                )
            })?;
            let events = select_events_for_retention_purge(
                connection,
                event_cutoff,
                settings.query_batch_size(),
            )
            .await?;
            let claim_id = Uuid::new_v4();
            let event_ids = events.iter().map(|event| event.id).collect::<Vec<_>>();
            let event_documents = serde_json::to_value(&events).map_err(|error| {
                PostgresStorageError::database(format!(
                    "Failed to serialize retained PostgreSQL events: {error}"
                ))
            })?;
            diesel::sql_query(
                "INSERT INTO event_retention_batches (
                     claim_id, event_ids, event_documents, delivery_cutoff, delivery_batch_size
                 ) VALUES ($1, $2, $3, $4, $5)",
            )
            .bind::<SqlUuid, _>(claim_id)
            .bind::<Array<BigInt>, _>(&event_ids)
            .bind::<Jsonb, _>(&event_documents)
            .bind::<Timestamp, _>(delivery_cutoff)
            .bind::<BigInt, _>(settings.query_batch_size())
            .execute(connection)
            .await?;
            retention_batch(RetentionBatchRow {
                claim_id,
                event_ids,
                event_documents,
                completed_at: None,
                purged_events: None,
                purged_terminal_deliveries: None,
            })
            .map(Some)
        })
        .await
}

/// Idempotently purge a previously archived retention claim.
pub async fn complete_event_retention_batch(
    runtime: &PostgresRuntime,
    batch_id: StorageEventRetentionBatchId,
) -> Result<StorageEventRetentionSummary, PostgresStorageError> {
    runtime
        .with_transaction(async |connection| -> Result<_, PostgresStorageError> {
            if !maintenance_state_on_connection(connection)
                .await?
                .is_normal()
            {
                return Err(PostgresStorageError::unavailable(
                    "Event retention completion is paused by maintenance",
                ));
            }
            if !try_acquire_event_retention_lock(connection).await? {
                return Err(PostgresStorageError::unavailable(
                    "Event retention completion is already in progress",
                ));
            }
            let claim = load_claim_for_update(connection, batch_id).await?;
            if claim.completed_at.is_some() {
                return completed_summary(&claim);
            }
            let summary = purge_event_retention_batch(connection, batch_id).await?;
            diesel::sql_query(
                "UPDATE event_retention_batches
                 SET completed_at = clock_timestamp() AT TIME ZONE 'UTC',
                     purged_events = $2,
                     purged_terminal_deliveries = $3
                 WHERE claim_id = $1 AND completed_at IS NULL",
            )
            .bind::<SqlUuid, _>(batch_id.as_uuid())
            .bind::<BigInt, _>(usize_to_i64(summary.purged_events())?)
            .bind::<BigInt, _>(usize_to_i64(summary.purged_terminal_deliveries())?)
            .execute(connection)
            .await?;
            Ok(summary)
        })
        .await
}

async fn delete_expired_completed_claims(
    connection: &mut PostgresConnection,
) -> Result<(), PostgresStorageError> {
    diesel::sql_query(
        "DELETE FROM event_retention_batches
         WHERE completed_at < (clock_timestamp() AT TIME ZONE 'UTC') - INTERVAL '30 days'",
    )
    .execute(connection)
    .await?;
    Ok(())
}

async fn load_pending_claim(
    connection: &mut PostgresConnection,
) -> Result<Option<RetentionBatchRow>, PostgresStorageError> {
    diesel::sql_query(
        "SELECT claim_id, event_ids, event_documents, completed_at, purged_events,
                purged_terminal_deliveries
         FROM event_retention_batches
         WHERE completed_at IS NULL
         ORDER BY created_at ASC
         LIMIT 1",
    )
    .get_result::<RetentionBatchRow>(connection)
    .await
    .optional()
    .map_err(PostgresStorageError::from)
}

async fn load_claim_for_update(
    connection: &mut PostgresConnection,
    batch_id: StorageEventRetentionBatchId,
) -> Result<RetentionBatchRow, PostgresStorageError> {
    diesel::sql_query(
        "SELECT claim_id, event_ids, event_documents, completed_at, purged_events,
                purged_terminal_deliveries
         FROM event_retention_batches
         WHERE claim_id = $1
         FOR UPDATE",
    )
    .bind::<SqlUuid, _>(batch_id.as_uuid())
    .get_result::<RetentionBatchRow>(connection)
    .await
    .map_err(PostgresStorageError::from)
}

fn retention_batch(
    row: RetentionBatchRow,
) -> Result<StorageEventRetentionBatch, PostgresStorageError> {
    let documents = row.event_documents.as_array().ok_or_else(|| {
        PostgresStorageError::database("Event retention claim documents are not a JSON array")
    })?;
    if documents.len() != row.event_ids.len() {
        return Err(PostgresStorageError::database(
            "Event retention claim has mismatched event ids and documents",
        ));
    }
    let events = row
        .event_ids
        .into_iter()
        .zip(documents)
        .map(|(id, document)| {
            if document.get("id").and_then(Value::as_i64) != Some(id) {
                return Err(PostgresStorageError::database(
                    "Event retention claim document does not match its event id",
                ));
            }
            let sequence = EventSequence::new(id)?;
            let json = serde_json::to_string(document).map_err(|error| {
                PostgresStorageError::database(format!(
                    "Failed to serialize claimed PostgreSQL event: {error}"
                ))
            })?;
            StorageRetainedEvent::try_new(sequence, json).map_err(|error| {
                PostgresStorageError::invalid_persisted_value("retained event", error)
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(StorageEventRetentionBatch::new(
        StorageEventRetentionBatchId::new(row.claim_id),
        events,
    ))
}

fn completed_summary(
    claim: &RetentionBatchRow,
) -> Result<StorageEventRetentionSummary, PostgresStorageError> {
    let purged_events = claim.purged_events.ok_or_else(|| {
        PostgresStorageError::database("Completed retention claim is missing its event count")
    })?;
    let purged_terminal_deliveries = claim.purged_terminal_deliveries.ok_or_else(|| {
        PostgresStorageError::database("Completed retention claim is missing its delivery count")
    })?;
    Ok(StorageEventRetentionSummary::new(
        usize::try_from(purged_events).map_err(|_| {
            PostgresStorageError::database("Retention event count does not fit usize")
        })?,
        usize::try_from(purged_terminal_deliveries).map_err(|_| {
            PostgresStorageError::database("Retention delivery count does not fit usize")
        })?,
    ))
}

fn usize_to_i64(value: usize) -> Result<i64, PostgresStorageError> {
    i64::try_from(value).map_err(|_| {
        PostgresStorageError::database("Retention count does not fit PostgreSQL bigint")
    })
}

async fn select_events_for_retention_purge(
    connection: &mut PostgresConnection,
    cutoff: NaiveDateTime,
    batch_size: i64,
) -> Result<Vec<RetentionEventRow>, PostgresStorageError> {
    let ids = select_event_ids_for_retention_purge(connection, cutoff, batch_size).await?;
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
    batch_id: StorageEventRetentionBatchId,
) -> Result<StorageEventRetentionSummary, PostgresStorageError> {
    let row = diesel::sql_query(
        "SELECT purged_events, purged_terminal_deliveries \
         FROM hubuum_complete_event_retention_purge($1)",
    )
    .bind::<SqlUuid, _>(batch_id.as_uuid())
    .get_result::<PurgeSummaryRow>(connection)
    .await?;
    Ok(StorageEventRetentionSummary::new(
        usize::try_from(row.purged_events).map_err(|_| {
            PostgresStorageError::database("Retention event count does not fit usize")
        })?,
        usize::try_from(row.purged_terminal_deliveries).map_err(|_| {
            PostgresStorageError::database("Retention delivery count does not fit usize")
        })?,
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
