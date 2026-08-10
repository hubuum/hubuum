pub(crate) mod capabilities;
mod class_relations;
mod classes;
mod collections;
mod context;
mod contract;
#[cfg(test)]
mod memory;
mod metrics;
mod object_relations;
mod objects;
mod observed;
mod operational;
#[doc(hidden)]
pub mod postgres;

pub(crate) use class_relations::ClassRelationStore;
pub(crate) use classes::ClassStore;
pub(crate) use collections::CollectionStore;
pub(crate) use context::{StorageContext, StorageHandle, storage_handle};
#[cfg(test)]
pub(crate) use contract::STORAGE_CONTRACT_VERSION;
pub(crate) use contract::{
    DynLifecycleStorage, LifecycleStorage, StorageBackend, StorageBackendDescriptor,
    StorageBackendKind, StorageIdentity,
};
pub(crate) use hubuum_storage_core::{StorageError, StorageErrorKind};
#[cfg(test)]
pub(crate) use memory::MemoryStorageModel;
pub(crate) use metrics::{
    EventMetricsSnapshot, ExportTemplateMetricIdentity, InventoryGaugeSnapshot,
    InventoryMetricsSnapshot, MetricsStorage, StoragePoolState, TaskGaugeAge, TaskGaugeCount,
    TaskGaugeLastTerminal, TaskGaugeSnapshot,
};
pub(crate) use object_relations::ObjectRelationStore;
pub(crate) use objects::ObjectStore;
pub(crate) use operational::{
    EventDeliveryHealthSnapshot, EventDeliveryStatusSnapshot, EventFanoutSnapshot,
    EventHealthStorage, EventQueueSnapshot, EventSinkHealthSnapshot, EventSinkSnapshot,
    EventSubscriptionHealthSnapshot, OperationalStateStorage, ReadinessSnapshot,
    TokenRetentionStorage,
};
pub(crate) use postgres::PostgresStorage;
