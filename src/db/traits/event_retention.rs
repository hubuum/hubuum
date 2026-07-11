use crate::db::prelude::*;
use chrono::{Duration, NaiveDateTime, Utc};
use diesel::sql_types::{Array, BigInt, Timestamp};

use crate::db::{DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::events::Event;
use crate::models::EventDeliveryStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventRetentionSettings {
    pub event_retention_days: i64,
    pub delivery_retention_days: i64,
    pub batch_size: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EventRetentionPurgeSummary {
    pub purged_events: usize,
    pub purged_terminal_deliveries: usize,
}

#[derive(Debug, QueryableByName)]
struct EventIdRow {
    #[diesel(sql_type = BigInt)]
    id: i64,
}

pub async fn select_events_for_retention_purge(
    pool: &DbPool,
    settings: EventRetentionSettings,
) -> Result<Vec<Event>, ApiError> {
    if settings.batch_size == 0 {
        return Ok(Vec::new());
    }

    let cutoff = Utc::now().naive_utc() - Duration::days(settings.event_retention_days);
    let ids = with_connection(pool, async |conn| {
        select_event_ids_for_retention_purge(conn, cutoff, settings.batch_size).await
    })
    .await?;

    if ids.is_empty() {
        return Ok(Vec::new());
    }

    use crate::schema::events::dsl::{events, id};
    with_connection(pool, async |conn| {
        events
            .filter(id.eq_any(ids))
            .order(id.asc())
            .load::<Event>(conn)
            .await
    })
    .await
}

pub async fn purge_event_retention_batch(
    pool: &DbPool,
    settings: EventRetentionSettings,
    event_ids: &[i64],
) -> Result<EventRetentionPurgeSummary, ApiError> {
    with_transaction(
        pool,
        async |conn| -> Result<EventRetentionPurgeSummary, ApiError> {
            let delivery_cutoff =
                Utc::now().naive_utc() - Duration::days(settings.delivery_retention_days);
            let purged_terminal_deliveries =
                purge_terminal_event_deliveries(conn, delivery_cutoff).await?;
            let purged_events = purge_events_by_id(conn, event_ids).await?;

            Ok(EventRetentionPurgeSummary {
                purged_events,
                purged_terminal_deliveries,
            })
        },
    )
    .await
}

#[cfg(test)]
pub async fn purge_event_retention_without_archive(
    pool: &DbPool,
    settings: EventRetentionSettings,
) -> Result<EventRetentionPurgeSummary, ApiError> {
    let events = select_events_for_retention_purge(pool, settings).await?;
    let event_ids = events.iter().map(|event| event.id).collect::<Vec<_>>();
    purge_event_retention_batch(pool, settings, &event_ids).await
}

async fn select_event_ids_for_retention_purge(
    conn: &mut crate::db::DbConnection,
    cutoff: NaiveDateTime,
    batch_size: usize,
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
         LIMIT $2",
    )
    .bind::<Timestamp, _>(cutoff)
    .bind::<BigInt, _>(batch_size as i64)
    .load::<EventIdRow>(conn)
    .await
    .map(|rows| rows.into_iter().map(|row| row.id).collect())
}

async fn purge_terminal_event_deliveries(
    conn: &mut crate::db::DbConnection,
    cutoff: NaiveDateTime,
) -> Result<usize, diesel::result::Error> {
    use crate::schema::event_deliveries::dsl::{event_deliveries, status, updated_at};

    diesel::delete(
        event_deliveries
            .filter(updated_at.lt(cutoff))
            .filter(status.eq_any([
                EventDeliveryStatus::Succeeded.as_str(),
                EventDeliveryStatus::Dead.as_str(),
            ])),
    )
    .execute(conn)
    .await
}

async fn purge_events_by_id(
    conn: &mut crate::db::DbConnection,
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
