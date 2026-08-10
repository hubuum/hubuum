use crate::storage::postgres::prelude::*;
use chrono::{NaiveDateTime, Utc};
use diesel::sql_types::{Array, BigInt, Bool, Timestamp};

use crate::errors::ApiError;
use crate::events::{Event, EventRetentionSettings};
use crate::storage::postgres::PostgresConnection;
#[cfg(test)]
use crate::storage::postgres::with_transaction;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EventRetentionPurgeSummary {
    pub purged_events: usize,
    pub purged_terminal_deliveries: usize,
}

const EVENT_RETENTION_LOCK_KEY: i64 = 4_850_188_191_125_218;

#[derive(Debug, QueryableByName)]
struct AdvisoryLockRow {
    #[diesel(sql_type = Bool)]
    locked: bool,
}

#[derive(Debug, QueryableByName)]
struct EventIdRow {
    #[diesel(sql_type = BigInt)]
    id: i64,
}

/// Try to become the one event-retention coordinator for this transaction.
///
/// Retention runs in every background-worker replica. A transaction-scoped
/// advisory lock ensures only one replica selects, archives, and purges a
/// batch at a time without making idle replicas wait for the active worker.
pub(crate) async fn try_acquire_event_retention_lock(
    conn: &mut PostgresConnection,
) -> Result<bool, ApiError> {
    Ok(
        diesel::sql_query("SELECT pg_try_advisory_xact_lock($1) AS locked")
            .bind::<BigInt, _>(EVENT_RETENTION_LOCK_KEY)
            .get_result::<AdvisoryLockRow>(conn)
            .await?
            .locked,
    )
}

pub(crate) async fn select_events_for_retention_purge_conn(
    conn: &mut PostgresConnection,
    settings: EventRetentionSettings,
) -> Result<Vec<Event>, ApiError> {
    let cutoff = settings
        .event_cutoff(Utc::now().naive_utc())
        .ok_or_else(|| {
            ApiError::InternalServerError(
                "event retention exceeds the database timestamp range".to_string(),
            )
        })?;
    let batch_size = settings.database_batch_size();
    let ids = select_event_ids_for_retention_purge(conn, cutoff, batch_size).await?;

    if ids.is_empty() {
        return Ok(Vec::new());
    }

    use crate::schema::events::dsl::{events, id};
    Ok(events
        .filter(id.eq_any(ids))
        .order(id.asc())
        .load::<Event>(conn)
        .await?)
}

pub(crate) async fn purge_event_retention_batch_conn(
    conn: &mut PostgresConnection,
    settings: EventRetentionSettings,
    event_ids: &[i64],
) -> Result<EventRetentionPurgeSummary, ApiError> {
    let delivery_cutoff = settings
        .delivery_cutoff(Utc::now().naive_utc())
        .ok_or_else(|| {
            ApiError::InternalServerError(
                "event delivery retention exceeds the database timestamp range".to_string(),
            )
        })?;
    let batch_size = settings.database_batch_size();
    let purged_terminal_deliveries =
        purge_terminal_event_deliveries(conn, delivery_cutoff, batch_size).await?;
    let purged_events = purge_events_by_id(conn, event_ids).await?;

    Ok(EventRetentionPurgeSummary {
        purged_events,
        purged_terminal_deliveries,
    })
}

#[cfg(test)]
pub(crate) async fn purge_event_retention_without_archive(
    pool: &impl crate::storage::StorageContext,
    settings: EventRetentionSettings,
) -> Result<EventRetentionPurgeSummary, ApiError> {
    with_transaction(pool, async |conn| -> Result<_, ApiError> {
        if !try_acquire_event_retention_lock(conn).await? {
            return Ok(EventRetentionPurgeSummary::default());
        }
        let events = select_events_for_retention_purge_conn(conn, settings).await?;
        let event_ids = events.iter().map(|event| event.id).collect::<Vec<_>>();
        purge_event_retention_batch_conn(conn, settings, &event_ids).await
    })
    .await
}

async fn select_event_ids_for_retention_purge(
    conn: &mut PostgresConnection,
    cutoff: NaiveDateTime,
    batch_size: i64,
) -> Result<Vec<i64>, diesel::result::Error> {
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
    .load::<EventIdRow>(conn)
    .await
    .map(|rows| rows.into_iter().map(|row| row.id).collect())
}

async fn purge_terminal_event_deliveries(
    conn: &mut PostgresConnection,
    cutoff: NaiveDateTime,
    batch_size: i64,
) -> Result<usize, diesel::result::Error> {
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
    .execute(conn)
    .await
}

async fn purge_events_by_id(
    conn: &mut PostgresConnection,
    event_ids: &[i64],
) -> Result<usize, diesel::result::Error> {
    if event_ids.is_empty() {
        return Ok(0);
    }

    diesel::sql_query("SELECT set_config('events.allow_purge', 'on', true)")
        .execute(conn)
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
    .execute(conn)
    .await
}
