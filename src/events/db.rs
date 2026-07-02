//! Transaction-aware event writer (#71).
//!
//! [`emit_event`] is the narrow producer API: it accepts the caller's
//! `&mut PgConnection` (the same connection used inside `with_transaction`)
//! and appends exactly one row to `events`. It deliberately exposes nothing
//! about fan-out or delivery — mutation code depends only on this writer and
//! the [`NewEvent`](super::NewEvent) builder.
//!
//! Because the insert runs on the caller's transaction connection, the event
//! commits or rolls back together with the domain mutation, giving the
//! "recorded iff committed" guarantee.

use diesel::prelude::*;
use diesel::result::Error as DieselError;

use crate::schema::events::dsl::events;

use super::{Event, NewEvent};

/// Append one event row on the caller's transaction connection.
///
/// Call this inside a `with_transaction(pool, |conn| { ...; emit_event(conn,
/// &event) })` block so the event and the mutation commit atomically.
pub fn emit_event(conn: &mut PgConnection, new_event: &NewEvent) -> Result<Event, DieselError> {
    let event = diesel::insert_into(events)
        .values(new_event)
        .get_result::<Event>(conn)?;
    super::notify_event_fanout(conn)?;
    Ok(event)
}
