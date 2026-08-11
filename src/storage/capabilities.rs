//! Test-only access to backend worker fixtures.
//!
//! Production application consumers use mandatory backend-neutral storage
//! contracts and services. The remaining export builds an opaque worker claim
//! for event-delivery tests without exposing it to production code.

#[cfg(test)]
pub(crate) mod event_delivery {
    pub(crate) use crate::storage::postgres::operations::event_delivery::{
        ClaimedEventDelivery, claimed_event_delivery_work_item,
    };
}
