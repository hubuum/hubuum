//! Unified event & audit stream — Diesel/Postgres layer (issue #70/#71).
//!
//! The canonical `events` table is the single source of truth for both the
//! internal audit log and external event delivery. A change is recorded iff
//! its storage transaction commits, so the event rolls back together with the
//! domain mutation on failure.
//!
//! Backend-agnostic catalog types (`EntityType` / `Action` / `ActorKind` and
//! the validity catalog) live in the [`hubuum_events_core`] crate, which is
//! free of Diesel/Actix/app concerns so the producer, the audit read API, and
//! the fan-out worker share one authoritative definition.

mod context;
mod delivery;
mod fanout;
mod model;
mod pg_notify;
mod retention;
mod settings;
mod sink;

pub use context::RequestProvenance;
pub(crate) use delivery::event_delivery_worker_health;
pub use delivery::{
    ensure_event_delivery_worker_running, event_delivery_wakeup_stats, kick_event_delivery_worker,
};
pub(crate) use fanout::event_fanout_worker_health;
pub use fanout::{
    ensure_event_fanout_worker_running, event_fanout_wakeup_stats, kick_event_fanout_worker,
};
pub use model::{Event, EventId, EventResponse, NewEvent};
pub(crate) use model::{PrincipalNames, StoredProvenance};
pub use retention::ensure_event_retention_worker_running;
pub(crate) use settings::{EventDeliverySettings, EventFanoutSettings, EventRetentionSettings};
pub use sink::{
    DefaultSinkResolver, EventEnvelope, NoopSinkResolver, Sink, SinkError, SinkResolver,
};

pub(crate) use pg_notify::{
    TASK_QUEUE_CHANNEL, notify_event_delivery, notify_task_queue,
    spawn_postgres_notification_listener,
};

pub use hubuum_events_core::{
    Action, ActorKind, EntityType, EventCatalogError, EventContext, MutationProvenance, Provenance,
    ProvenanceActor, ProvenancePrincipal, is_valid_pair, valid_actions,
};

#[cfg(test)]
mod tests;
