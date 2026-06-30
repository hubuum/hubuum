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

#[cfg(feature = "amqp")]
mod amqp;
mod context;
mod db;
mod delivery;
#[cfg(feature = "email")]
mod email;
mod fanout;
mod model;
mod retention;
mod sink;
#[cfg(feature = "valkey")]
mod valkey;
mod webhook;

pub use context::RequestProvenance;
pub use db::emit_event;
pub use delivery::{
    ensure_event_delivery_worker_running, event_delivery_wakeup_stats, kick_event_delivery_worker,
};
pub use fanout::{
    ensure_event_fanout_worker_running, event_fanout_wakeup_stats, kick_event_fanout_worker,
};
pub use model::{Event, EventId, EventResponse, NewEvent};
pub use retention::ensure_event_retention_worker_running;
pub use sink::{
    DefaultSinkResolver, EventEnvelope, NoopSinkResolver, Sink, SinkError, SinkResolver,
};

pub use hubuum_events_core::{
    Action, ActorKind, EntityType, EventCatalogError, EventContext, is_valid_pair, valid_actions,
};

#[cfg(test)]
mod tests;
