//! Transaction-aware event writer (#71).
//!
//! [`emit_event`] is the narrow producer API: it accepts the caller's
//! `&mut crate::db::DbConnection` (the same connection used inside `with_transaction`)
//! and appends exactly one row to `events`. It deliberately exposes nothing
//! about fan-out or delivery — mutation code depends only on this writer and
//! the [`NewEvent`](super::NewEvent) builder.
//!
//! Because the insert runs on the caller's transaction connection, the event
//! commits or rolls back together with the domain mutation, giving the
//! "recorded iff committed" guarantee.

use crate::db::prelude::*;
use diesel::result::Error as DieselError;

use crate::schema::events::dsl::events;

use super::{Event, NewEvent};

/// Append one event row on the caller's transaction connection.
///
/// Call this inside a `with_transaction(pool, |conn| { ...; emit_event(conn,
/// &event) })` block so the event and the mutation commit atomically.
pub async fn emit_event(
    conn: &mut crate::db::DbConnection,
    new_event: &NewEvent,
) -> Result<Event, DieselError> {
    let event = diesel::insert_into(events)
        .values(new_event)
        .get_result::<Event>(conn)
        .await?;
    if let (Ok(entity_type), Ok(action)) = (event.entity_type(), event.action()) {
        crate::logger::log_operation_mutation(
            entity_type,
            action,
            event.entity_id,
            event.actor_user_id,
            event.request_id,
            event.correlation_id.as_deref(),
        );
    }
    Ok(event)
}

/// Append a bounded batch of events in one statement.
///
/// Retention workers use this to make an immutable audit snapshot part of the
/// same transaction as each bounded deletion batch without issuing one insert
/// statement per row.
pub(crate) async fn emit_events(
    conn: &mut crate::db::DbConnection,
    new_events: &[NewEvent],
) -> Result<Vec<Event>, DieselError> {
    if new_events.is_empty() {
        return Ok(Vec::new());
    }

    let persisted = diesel::insert_into(events)
        .values(new_events)
        .get_results::<Event>(conn)
        .await?;
    for event in &persisted {
        if let (Ok(entity_type), Ok(action)) = (event.entity_type(), event.action()) {
            crate::logger::log_operation_mutation(
                entity_type,
                action,
                event.entity_id,
                event.actor_user_id,
                event.request_id,
                event.correlation_id.as_deref(),
            );
        }
    }
    Ok(persisted)
}
