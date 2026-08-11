use async_trait::async_trait;

use crate::StorageError;

/// Number of persisted objects belonging to one class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageObjectsByClassCount {
    class_id: i32,
    count: i64,
}

impl StorageObjectsByClassCount {
    #[must_use]
    pub const fn new(class_id: i32, count: i64) -> Self {
        Self { class_id, count }
    }

    #[must_use]
    pub const fn class_id(self) -> i32 {
        self.class_id
    }

    #[must_use]
    pub const fn count(self) -> i64 {
        self.count
    }
}

/// Backend-neutral inventory counters used by administrative APIs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageInventoryCounts {
    total_objects: i64,
    total_classes: i64,
    total_collections: i64,
    objects_by_class: Vec<StorageObjectsByClassCount>,
}

impl StorageInventoryCounts {
    #[must_use]
    pub fn new(
        total_objects: i64,
        total_classes: i64,
        total_collections: i64,
        objects_by_class: Vec<StorageObjectsByClassCount>,
    ) -> Self {
        Self {
            total_objects,
            total_classes,
            total_collections,
            objects_by_class,
        }
    }

    #[must_use]
    pub const fn total_objects(&self) -> i64 {
        self.total_objects
    }

    #[must_use]
    pub const fn total_classes(&self) -> i64 {
        self.total_classes
    }

    #[must_use]
    pub const fn total_collections(&self) -> i64 {
        self.total_collections
    }

    #[must_use]
    pub fn objects_by_class(&self) -> &[StorageObjectsByClassCount] {
        &self.objects_by_class
    }

    #[must_use]
    pub fn into_objects_by_class(self) -> Vec<StorageObjectsByClassCount> {
        self.objects_by_class
    }
}

/// Administrative inventory queries every selectable backend must provide.
#[async_trait]
pub trait InventoryStorage: Send + Sync {
    async fn inventory_counts(&self) -> Result<StorageInventoryCounts, StorageError>;
}
