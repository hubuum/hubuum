//! Application-facing persistence capabilities.
//!
//! Consumers depend on operation-shaped contracts from this module and pass a
//! [`crate::storage::StorageContext`]. PostgreSQL adapters remain behind the
//! boundary in `crate::storage::postgres`.

/// Operation-shaped persistence capabilities still used directly by the
/// application layer.
///
/// Keep this list explicit: adding a database module wholesale would make the
/// boundary cosmetic and allow consumers to select arbitrary SQL adapters.
pub(crate) mod collection {
    pub(crate) use crate::storage::postgres::operations::collection::collection_permission_set_from_backend;
}

pub(crate) mod event_delivery {
    #[cfg(test)]
    pub(crate) use crate::storage::postgres::operations::event_delivery::{
        ClaimedEventDelivery, claimed_event_delivery_work_item,
    };
    pub(crate) use crate::storage::postgres::operations::event_delivery::{
        list_event_deliveries_with_total_count, load_event_delivery, mark_event_delivery_dead,
        release_event_delivery_for_retry,
    };
}

pub(crate) mod event_subscription {
    pub(crate) use crate::storage::postgres::operations::event_subscription::{
        DeleteEventSinkRecord, DeleteEventSubscriptionRecord, SaveEventSinkRecord,
        SaveEventSubscriptionRecord, UpdateEventSinkRecord, UpdateEventSubscriptionRecord,
        enabled_event_sink_count,
    };
}

pub(crate) mod events {
    pub(crate) use crate::storage::postgres::operations::events::{
        list_events_with_total_count, parse_event_filters,
    };
}

pub(crate) mod meta {
    pub(crate) use crate::storage::postgres::operations::meta::{
        load_database_state, load_task_queue_state,
    };
}

pub(crate) mod permissions {
    pub(crate) use crate::storage::postgres::operations::permissions::PermissionControllerBackend;
}
