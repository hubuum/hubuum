//! Unified event & audit stream — Diesel/Postgres layer (issue #70/#71).
//!
//! The canonical `events` table is the single source of truth for both the
//! internal audit log and external event delivery. A change is recorded iff
//! its database transaction commits: [`emit_event`] appends exactly one row
//! inside the caller's `with_transaction` block, so the event rolls back
//! together with the domain mutation on failure.
//!
//! Backend-agnostic catalog types (`EntityType` / `Action` / `ActorKind` and
//! the validity catalog) live in the [`hubuum_events_core`] crate, which is
//! free of Diesel/Actix/app concerns so the producer, the audit read API, and
//! the fan-out worker share one authoritative definition.

mod context;
mod db;
mod fanout;
mod model;

pub use context::RequestProvenance;
pub use db::emit_event;
pub use fanout::{ensure_event_fanout_worker_running, kick_event_fanout_worker};
pub use model::{Event, EventId, EventResponse, NewEvent};

pub use hubuum_events_core::{
    Action, ActorKind, EntityType, EventCatalogError, EventContext, is_valid_pair, valid_actions,
};

#[cfg(test)]
mod tests;
