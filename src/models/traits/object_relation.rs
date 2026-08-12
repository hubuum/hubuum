use crate::errors::ApiError;
use crate::events::EventContext;

use crate::models::{
    HubuumObjectRelation, HubuumObjectRelationID, HubuumObjectWithPath, NewHubuumObjectRelation,
    ObjectGraphRow, ObjectRelationSelector, RelatedObjectForRootRow, RelatedObjectGraphRow,
    RelatedObjectIncludeRow, ResolvedObjectRelationTarget,
};
use crate::storage::{StorageContext, storage_handle};
use crate::traits::accessors::{IdAccessor, InstanceAdapter};
use crate::traits::crud::{DeleteAdapter, SaveAdapter};

async fn resolve_object_relation(
    backend: &impl StorageContext,
    id: HubuumObjectRelationID,
) -> Result<ResolvedObjectRelationTarget, ApiError> {
    storage_handle(backend)
        .object_relation_store()
        .resolve_object_relation(ObjectRelationSelector::by_id(id))
        .await
        .map_err(ApiError::from)
}

impl IdAccessor for HubuumObjectRelationID {
    fn accessor_id(&self) -> i32 {
        // Deref to the owned (Copy) value on purpose: with a `&self` receiver, `self.id()`
        // binds to the `SelfAccessors::id` trait method, which calls back into `accessor_id`
        // and recurses. The inherent `id` is only selected on an owned receiver.
        (*self).id()
    }
}

impl InstanceAdapter<HubuumObjectRelation> for HubuumObjectRelationID {
    async fn instance_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumObjectRelation, ApiError> {
        Ok(*resolve_object_relation(pool, *self).await?.relation())
    }
}
impl IdAccessor for HubuumObjectRelation {
    fn accessor_id(&self) -> i32 {
        self.id
    }
}

impl InstanceAdapter<HubuumObjectRelation> for HubuumObjectRelation {
    async fn instance_adapter(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumObjectRelation, ApiError> {
        Ok(*self)
    }
}

impl DeleteAdapter for HubuumObjectRelation {
    async fn delete_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(), ApiError> {
        storage_handle(pool)
            .object_relation_store()
            .delete_object_relation_by_id(HubuumObjectRelationID::new(self.id)?, None)
            .await
            .map_err(ApiError::from)
    }

    async fn delete_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<(), ApiError> {
        storage_handle(pool)
            .object_relation_store()
            .delete_object_relation_by_id(HubuumObjectRelationID::new(self.id)?, Some(context))
            .await
            .map_err(ApiError::from)
    }
}

impl DeleteAdapter for HubuumObjectRelationID {
    async fn delete_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(), ApiError> {
        storage_handle(pool)
            .object_relation_store()
            .delete_object_relation_by_id(*self, None)
            .await
            .map_err(ApiError::from)
    }

    async fn delete_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<(), ApiError> {
        storage_handle(pool)
            .object_relation_store()
            .delete_object_relation_by_id(*self, Some(context))
            .await
            .map_err(ApiError::from)
    }
}

impl SaveAdapter for NewHubuumObjectRelation {
    type Output = HubuumObjectRelation;

    async fn save_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumObjectRelation, ApiError> {
        storage_handle(pool)
            .object_relation_store()
            .create_object_relation_from_command(self.clone(), None)
            .await
            .map_err(ApiError::from)
    }

    async fn save_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<HubuumObjectRelation, ApiError> {
        storage_handle(pool)
            .object_relation_store()
            .create_object_relation_from_command(self.clone(), Some(context))
            .await
            .map_err(ApiError::from)
    }
}

impl ObjectGraphRow {
    pub fn to_descendant_object_with_path(&self) -> HubuumObjectWithPath {
        HubuumObjectWithPath {
            id: self.descendant_object_id,
            name: self.descendant_name.clone(),
            collection_id: self.descendant_collection_id,
            hubuum_class_id: self.descendant_class_id,
            data: self.descendant_data.clone(),
            description: self.descendant_description.clone(),
            created_at: self.descendant_created_at,
            updated_at: self.descendant_updated_at,
            revision: self.descendant_revision,
            path: self.path.clone(),
        }
    }
}

impl RelatedObjectGraphRow {
    pub fn to_descendant_object_with_path(&self) -> HubuumObjectWithPath {
        HubuumObjectWithPath {
            id: self.descendant_object_id,
            name: self.descendant_name.clone(),
            collection_id: self.descendant_collection_id,
            hubuum_class_id: self.descendant_class_id,
            data: self.descendant_data.clone(),
            description: self.descendant_description.clone(),
            created_at: self.descendant_created_at,
            updated_at: self.descendant_updated_at,
            revision: self.descendant_revision,
            path: self.path.clone(),
        }
    }
}

impl RelatedObjectIncludeRow {
    pub fn to_descendant_object_with_path(&self) -> HubuumObjectWithPath {
        HubuumObjectWithPath {
            id: self.descendant_object_id,
            name: self.descendant_name.clone(),
            collection_id: self.descendant_collection_id,
            hubuum_class_id: self.descendant_class_id,
            data: self.descendant_data.clone(),
            description: self.descendant_description.clone(),
            created_at: self.descendant_created_at,
            updated_at: self.descendant_updated_at,
            revision: self.descendant_revision,
            path: self.path.clone(),
        }
    }
}

impl RelatedObjectForRootRow {
    pub fn to_descendant_object_with_path(&self) -> HubuumObjectWithPath {
        HubuumObjectWithPath {
            id: self.descendant_object_id,
            name: self.descendant_name.clone(),
            collection_id: self.descendant_collection_id,
            hubuum_class_id: self.descendant_class_id,
            data: self.descendant_data.clone(),
            description: self.descendant_description.clone(),
            created_at: self.descendant_created_at,
            updated_at: self.descendant_updated_at,
            revision: self.descendant_revision,
            path: self.path.clone(),
        }
    }
}

// Trait for converting graph rows to API-ready object payloads.
pub trait ToHubuumObjects {
    fn to_descendant_objects_with_path(self) -> Vec<HubuumObjectWithPath>;
}

impl ToHubuumObjects for Vec<ObjectGraphRow> {
    fn to_descendant_objects_with_path(self) -> Vec<HubuumObjectWithPath> {
        self.into_iter()
            .map(|ocv| ocv.to_descendant_object_with_path())
            .collect()
    }
}

impl ToHubuumObjects for Vec<RelatedObjectGraphRow> {
    fn to_descendant_objects_with_path(self) -> Vec<HubuumObjectWithPath> {
        self.into_iter()
            .map(|ocv| ocv.to_descendant_object_with_path())
            .collect()
    }
}
