use async_trait::async_trait;
use hubuum_domain::ClassId;

use crate::{StorageError, StorageValidationError};

/// Number of persisted objects belonging to one class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageObjectCountByClass {
    class_id: ClassId,
    count: i64,
}

impl StorageObjectCountByClass {
    pub fn try_new(class_id: ClassId, count: i64) -> Result<Self, StorageValidationError> {
        if count < 0 {
            return Err(StorageValidationError::invalid(
                "objects-by-class count must not be negative",
            ));
        }
        Ok(Self { class_id, count })
    }

    #[must_use]
    pub const fn class_id(self) -> ClassId {
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
    objects_by_class: Vec<StorageObjectCountByClass>,
}

impl StorageInventoryCounts {
    pub fn try_new(
        total_objects: i64,
        total_classes: i64,
        total_collections: i64,
        objects_by_class: Vec<StorageObjectCountByClass>,
    ) -> Result<Self, StorageValidationError> {
        if total_objects < 0 || total_classes < 0 || total_collections < 0 {
            return Err(StorageValidationError::invalid(
                "inventory totals must not be negative",
            ));
        }
        let mut class_ids = std::collections::HashSet::with_capacity(objects_by_class.len());
        let mut object_sum = 0_i64;
        for count in &objects_by_class {
            if !class_ids.insert(count.class_id()) {
                return Err(StorageValidationError::invalid(
                    "inventory objects-by-class values must have unique class ids",
                ));
            }
            object_sum = object_sum.checked_add(count.count()).ok_or_else(|| {
                StorageValidationError::invalid("inventory objects-by-class count overflow")
            })?;
        }
        if object_sum != total_objects {
            return Err(StorageValidationError::invalid(
                "inventory objects-by-class counts must sum to total_objects",
            ));
        }
        let class_count = i64::try_from(objects_by_class.len()).map_err(|_| {
            StorageValidationError::invalid("inventory objects-by-class count does not fit i64")
        })?;
        if class_count > total_classes {
            return Err(StorageValidationError::invalid(
                "inventory contains more class counts than total_classes",
            ));
        }
        Ok(Self {
            total_objects,
            total_classes,
            total_collections,
            objects_by_class,
        })
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
    pub fn objects_by_class(&self) -> &[StorageObjectCountByClass] {
        &self.objects_by_class
    }

    #[must_use]
    pub fn into_objects_by_class(self) -> Vec<StorageObjectCountByClass> {
        self.objects_by_class
    }
}

/// Administrative inventory queries every selectable backend must provide.
#[async_trait]
pub trait InventoryStorage: Send + Sync {
    async fn get_inventory_counts(&self) -> Result<StorageInventoryCounts, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventory_rejects_negative_and_inconsistent_counts() {
        let class_id = ClassId::new(1).unwrap();
        assert!(StorageObjectCountByClass::try_new(class_id, -1).is_err());

        let per_class = StorageObjectCountByClass::try_new(class_id, 2).unwrap();
        assert!(StorageInventoryCounts::try_new(1, 1, 1, vec![per_class]).is_err());
        assert!(StorageInventoryCounts::try_new(-1, 1, 1, Vec::new()).is_err());
    }

    #[test]
    fn inventory_accepts_consistent_counts() {
        let per_class = StorageObjectCountByClass::try_new(ClassId::new(1).unwrap(), 2).unwrap();
        let counts = StorageInventoryCounts::try_new(2, 1, 1, vec![per_class]).unwrap();

        assert_eq!(counts.total_objects(), 2);
    }
}
