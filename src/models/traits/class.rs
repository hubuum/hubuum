use crate::traits::accessors::{ClassAdapter, CollectionAdapter, IdAccessor, InstanceAdapter};
use crate::traits::{ClassAccessors, CollectionAccessors, PermissionController};

use crate::errors::ApiError;
use crate::events::EventContext;
use crate::storage::{ClassRecordStorage, storage_handle};
use crate::traits::crud::{DeleteAdapter, SaveAdapter, UpdateAdapter};

use crate::models::{
    ClassSelector, Collection, CollectionID, HubuumClass, HubuumClassID, NewHubuumClass,
    ResolvedClassTarget, UpdateHubuumClass,
};

pub trait ResolveClassTarget {
    async fn resolve_class_target<C>(&self, backend: &C) -> Result<ResolvedClassTarget, ApiError>
    where
        C: crate::storage::StorageContext;
}

impl ResolveClassTarget for ClassSelector {
    async fn resolve_class_target<C>(&self, backend: &C) -> Result<ResolvedClassTarget, ApiError>
    where
        C: crate::storage::StorageContext,
    {
        storage_handle(backend)
            .lifecycle_storage()
            .inner()
            .resolve_class(self.clone())
            .await
            .map_err(ApiError::from)
    }
}

pub trait UpdateResolvedClass {
    async fn update_resolved_class<C>(
        &self,
        backend: &C,
        target: &ResolvedClassTarget,
        context: &EventContext,
    ) -> Result<HubuumClass, ApiError>
    where
        C: crate::storage::StorageContext;
}

impl UpdateResolvedClass for UpdateHubuumClass {
    async fn update_resolved_class<C>(
        &self,
        backend: &C,
        target: &ResolvedClassTarget,
        context: &EventContext,
    ) -> Result<HubuumClass, ApiError>
    where
        C: crate::storage::StorageContext,
    {
        storage_handle(backend)
            .lifecycle_storage()
            .inner()
            .update_class(target, self.clone(), context)
            .await
            .map_err(ApiError::from)
    }
}

pub trait DeleteResolvedClass {
    async fn delete_resolved_class<C>(
        &self,
        backend: &C,
        context: &EventContext,
    ) -> Result<(), ApiError>
    where
        C: crate::storage::StorageContext;
}

impl DeleteResolvedClass for ResolvedClassTarget {
    async fn delete_resolved_class<C>(
        &self,
        backend: &C,
        context: &EventContext,
    ) -> Result<(), ApiError>
    where
        C: crate::storage::StorageContext,
    {
        storage_handle(backend)
            .lifecycle_storage()
            .inner()
            .delete_class(self, context)
            .await
            .map_err(ApiError::from)
    }
}

impl SaveAdapter for HubuumClass {
    type Output = HubuumClass;

    async fn save_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Self::Output, ApiError> {
        let update = UpdateHubuumClass {
            name: Some(self.name.clone()),
            collection_id: Some(self.collection_id),
            json_schema: self.json_schema.clone(),
            validate_schema: Some(self.validate_schema),
            description: Some(self.description.clone()),
        };

        storage_handle(pool)
            .update_class_record(&update, self.id, None)
            .await
            .map_err(ApiError::from)
    }

    async fn save_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<Self::Output, ApiError> {
        let update = UpdateHubuumClass {
            name: Some(self.name.clone()),
            collection_id: Some(self.collection_id),
            json_schema: self.json_schema.clone(),
            validate_schema: Some(self.validate_schema),
            description: Some(self.description.clone()),
        };

        storage_handle(pool)
            .update_class_record(&update, self.id, Some(context))
            .await
            .map_err(ApiError::from)
    }
}

impl DeleteAdapter for HubuumClass {
    async fn delete_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(), ApiError> {
        storage_handle(pool)
            .delete_class_record(self, None)
            .await
            .map_err(ApiError::from)
    }

    async fn delete_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<(), ApiError> {
        storage_handle(pool)
            .delete_class_record(self, Some(context))
            .await
            .map_err(ApiError::from)
    }
}

impl SaveAdapter for NewHubuumClass {
    type Output = HubuumClass;

    async fn save_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClass, ApiError> {
        storage_handle(pool)
            .create_class_record(self, None)
            .await
            .map_err(ApiError::from)
    }

    async fn save_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<HubuumClass, ApiError> {
        storage_handle(pool)
            .create_class_record(self, Some(context))
            .await
            .map_err(ApiError::from)
    }
}

impl UpdateAdapter for UpdateHubuumClass {
    type Output = HubuumClass;
    type Identifier = HubuumClassID;

    async fn update_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
        class_id: HubuumClassID,
    ) -> Result<HubuumClass, ApiError> {
        storage_handle(pool)
            .update_class_record(self, class_id.id(), None)
            .await
            .map_err(ApiError::from)
    }

    async fn update_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        class_id: HubuumClassID,
        context: &EventContext,
    ) -> Result<HubuumClass, ApiError> {
        storage_handle(pool)
            .update_class_record(self, class_id.id(), Some(context))
            .await
            .map_err(ApiError::from)
    }
}

impl IdAccessor for HubuumClass {
    fn accessor_id(&self) -> i32 {
        self.id
    }
}

impl InstanceAdapter<HubuumClass> for HubuumClass {
    async fn instance_adapter(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClass, ApiError> {
        Ok(self.clone())
    }
}

impl ClassAdapter for HubuumClass {
    async fn class_id_adapter(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClassID, ApiError> {
        HubuumClassID::new(self.id)
    }

    async fn class_adapter(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClass, ApiError> {
        Ok(self.clone())
    }
}

impl CollectionAdapter for HubuumClass {
    async fn collection_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Collection, ApiError> {
        storage_handle(pool)
            .class_collection(self.id)
            .await
            .map_err(ApiError::from)
    }

    async fn collection_id_adapter(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<CollectionID, ApiError> {
        CollectionID::new(self.collection_id)
    }
}

impl IdAccessor for HubuumClassID {
    fn accessor_id(&self) -> i32 {
        // Deref to the owned (Copy) value on purpose: with a `&self` receiver, `self.id()`
        // binds to the `SelfAccessors::id` trait method, which calls back into `accessor_id`
        // and recurses. The inherent `id` is only selected on an owned receiver.
        (*self).id()
    }
}

impl InstanceAdapter<HubuumClass> for HubuumClassID {
    async fn instance_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClass, ApiError> {
        self.class(pool).await
    }
}

impl ClassAdapter for HubuumClassID {
    async fn class_id_adapter(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClassID, ApiError> {
        Ok(*self)
    }

    async fn class_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClass, ApiError> {
        storage_handle(pool)
            .load_class_record(self.id())
            .await
            .map_err(ApiError::from)
    }
}

impl CollectionAdapter for HubuumClassID {
    async fn collection_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Collection, ApiError> {
        storage_handle(pool)
            .class_collection(self.id())
            .await
            .map_err(ApiError::from)
    }

    async fn collection_id_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<CollectionID, ApiError> {
        CollectionID::new(self.collection(pool).await?.id)
    }
}

impl PermissionController for HubuumClass {}
