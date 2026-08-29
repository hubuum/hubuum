use crate::errors::ApiError;
use crate::models::ObjectsByClass;
use crate::storage::InventoryStorage;

pub(crate) struct InventoryCounts {
    pub(crate) total_objects: i64,
    pub(crate) total_classes: i64,
    pub(crate) total_collections: i64,
    pub(crate) objects_by_class: Vec<ObjectsByClass>,
}

pub(crate) async fn counts(storage: &impl InventoryStorage) -> Result<InventoryCounts, ApiError> {
    let counts = storage.get_inventory_counts().await?;
    let total_objects = counts.total_objects();
    let total_classes = counts.total_classes();
    let total_collections = counts.total_collections();
    let objects_by_class = counts
        .into_objects_by_class()
        .into_iter()
        .map(|row| ObjectsByClass {
            hubuum_class_id: row.class_id().id(),
            count: row.count(),
        })
        .collect();

    Ok(InventoryCounts {
        total_objects,
        total_classes,
        total_collections,
        objects_by_class,
    })
}
