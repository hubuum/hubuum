use hubuum_domain::{CollectionId, EventSinkId};
use hubuum_events_core::EventContext;

use crate::{StorageError, StorageEventSink};

/// A sink resolved for one collection. Storage rechecks the grant under its
/// mutation/admission lock because authorization can change concurrently.
#[derive(Clone)]
pub struct StorageAuthorizedEventSink {
    collection_id: CollectionId,
    sink: StorageEventSink,
}

impl StorageAuthorizedEventSink {
    pub fn try_new(
        collection_id: CollectionId,
        sink: StorageEventSink,
        directly_granted: bool,
    ) -> Result<Self, StorageError> {
        if sink.collection_id() != Some(collection_id)
            && !(sink.collection_id().is_none() && directly_granted)
        {
            return Err(StorageError::permission_denied(
                "Sink is not available to this collection",
            ));
        }
        Ok(Self {
            collection_id,
            sink,
        })
    }

    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }
    pub const fn sink(&self) -> &StorageEventSink {
        &self.sink
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventSinkGrantAction {
    Grant,
    Revoke,
}

/// An explicit administrator change to a global sink's collection grants.
#[derive(Clone)]
pub struct StorageEventSinkGrantChange {
    sink_id: EventSinkId,
    collection_id: CollectionId,
    action: EventSinkGrantAction,
    event_context: EventContext,
}

impl StorageEventSinkGrantChange {
    pub const fn new(
        sink_id: EventSinkId,
        collection_id: CollectionId,
        action: EventSinkGrantAction,
        event_context: EventContext,
    ) -> Self {
        Self {
            sink_id,
            collection_id,
            action,
            event_context,
        }
    }
    pub const fn sink_id(&self) -> EventSinkId {
        self.sink_id
    }
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }
    pub const fn action(&self) -> EventSinkGrantAction {
        self.action
    }
    pub const fn event_context(&self) -> &EventContext {
        &self.event_context
    }
}
