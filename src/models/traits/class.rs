use crate::traits::accessors::{ClassAdapter, CollectionAdapter, IdAccessor, InstanceAdapter};
use crate::traits::{CanUpdate, ClassAccessors, CollectionAccessors, PermissionController};

use crate::errors::ApiError;
use crate::events::EventContext;
use crate::storage::postgres::operations::class::{
    ClassCollectionLookup, CreateClassRecord, DeleteClassRecord, DeleteResolvedClassRecord,
    LoadClassRecord, ResolveClassSelectorRecord, UpdateClassRecord, UpdateResolvedClassRecord,
};
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
        let class = self.resolve_class_selector_record(backend).await?;
        Ok(ResolvedClassTarget::new(self.clone(), class))
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
        self.update_resolved_class_record(backend, target, context)
            .await
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
        self.delete_resolved_class_record(backend, context).await
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

        update
            .update_without_events(pool, HubuumClassID::new(self.id)?)
            .await
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

        update
            .update_class_record(pool, self.id, Some(context))
            .await
    }
}

impl DeleteAdapter for HubuumClass {
    async fn delete_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(), ApiError> {
        self.delete_class_record_without_events(pool).await
    }

    async fn delete_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<(), ApiError> {
        self.delete_class_record(pool, Some(context)).await
    }
}

impl SaveAdapter for NewHubuumClass {
    type Output = HubuumClass;

    async fn save_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClass, ApiError> {
        self.validate_schema()?;
        self.create_class_record_without_events(pool).await
    }

    async fn save_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<HubuumClass, ApiError> {
        self.validate_schema()?;
        self.create_class_record(pool, Some(context)).await
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
        self.update_class_record_without_events(pool, class_id.id())
            .await
    }

    async fn update_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        class_id: HubuumClassID,
        context: &EventContext,
    ) -> Result<HubuumClass, ApiError> {
        self.update_class_record(pool, class_id.id(), Some(context))
            .await
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
        self.lookup_class_collection(pool).await
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
        self.load_class_record(pool).await
    }
}

impl CollectionAdapter for HubuumClassID {
    async fn collection_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Collection, ApiError> {
        self.lookup_class_collection(pool).await
    }

    async fn collection_id_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<CollectionID, ApiError> {
        CollectionID::new(self.collection(pool).await?.id)
    }
}

impl PermissionController for HubuumClass {}
