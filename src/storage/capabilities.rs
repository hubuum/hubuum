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

#[cfg(test)]
pub(crate) mod event_delivery {
    pub(crate) use crate::storage::postgres::operations::event_delivery::{
        ClaimedEventDelivery, claimed_event_delivery_work_item,
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
