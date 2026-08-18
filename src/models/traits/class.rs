use crate::traits::accessors::{ClassAdapter, CollectionAdapter, IdAccessor, InstanceAdapter};
use crate::traits::{ClassAccessors, CollectionAccessors, PermissionController};

use crate::errors::ApiError;
use crate::events::EventContext;
use crate::services::storage_boundary::{
    class_create_to_storage, class_id_to_storage, class_record_from_storage,
    class_selector_to_storage, class_update_to_storage, collection_from_storage,
    collection_id_to_storage, resolved_class_from_storage, resolved_class_to_storage,
};
use crate::storage::{StorageClassSelector, storage_handle};
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
            .class_store()
            .resolve_class(class_selector_to_storage(self.clone()))
            .await
            .map_err(ApiError::from)
            .and_then(resolved_class_from_storage)
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
        let target = resolved_class_to_storage(target);
        storage_handle(backend)
            .class_store()
            .update_class(&target, class_update_to_storage(self.clone()), context)
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .and_then(class_record_from_storage)
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
        let target = resolved_class_to_storage(self);
        storage_handle(backend)
            .class_store()
            .delete_class(&target, context)
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
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

        let target = storage_handle(pool)
            .class_store()
            .resolve_class(StorageClassSelector::Id(class_id_to_storage(self.id)))
            .await
            .map_err(ApiError::from)?;
        storage_handle(pool)
            .class_store()
            .update_class(
                &target,
                class_update_to_storage(update),
                &EventContext::system(),
            )
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .and_then(class_record_from_storage)
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

        let target = storage_handle(pool)
            .class_store()
            .resolve_class(StorageClassSelector::Id(class_id_to_storage(self.id)))
            .await
            .map_err(ApiError::from)?;
        storage_handle(pool)
            .class_store()
            .update_class(&target, class_update_to_storage(update), context)
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .and_then(class_record_from_storage)
    }
}

impl DeleteAdapter for HubuumClass {
    async fn delete_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(), ApiError> {
        let target = storage_handle(pool)
            .class_store()
            .resolve_class(StorageClassSelector::Id(class_id_to_storage(self.id)))
            .await
            .map_err(ApiError::from)?;
        storage_handle(pool)
            .class_store()
            .delete_class(&target, &EventContext::system())
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
    }

    async fn delete_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<(), ApiError> {
        let target = storage_handle(pool)
            .class_store()
            .resolve_class(StorageClassSelector::Id(class_id_to_storage(self.id)))
            .await
            .map_err(ApiError::from)?;
        storage_handle(pool)
            .class_store()
            .delete_class(&target, context)
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
    }
}

impl SaveAdapter for NewHubuumClass {
    type Output = HubuumClass;

    async fn save_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClass, ApiError> {
        storage_handle(pool)
            .class_store()
            .create_class(
                class_create_to_storage(self.clone()),
                &EventContext::system(),
            )
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .and_then(class_record_from_storage)
    }

    async fn save_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<HubuumClass, ApiError> {
        storage_handle(pool)
            .class_store()
            .create_class(class_create_to_storage(self.clone()), context)
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .and_then(class_record_from_storage)
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
        let target = storage_handle(pool)
            .class_store()
            .resolve_class(StorageClassSelector::Id(class_id_to_storage(class_id.id())))
            .await
            .map_err(ApiError::from)?;
        storage_handle(pool)
            .class_store()
            .update_class(
                &target,
                class_update_to_storage(self.clone()),
                &EventContext::system(),
            )
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .and_then(class_record_from_storage)
    }

    async fn update_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        class_id: HubuumClassID,
        context: &EventContext,
    ) -> Result<HubuumClass, ApiError> {
        let target = storage_handle(pool)
            .class_store()
            .resolve_class(StorageClassSelector::Id(class_id_to_storage(class_id.id())))
            .await
            .map_err(ApiError::from)?;
        storage_handle(pool)
            .class_store()
            .update_class(&target, class_update_to_storage(self.clone()), context)
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .and_then(class_record_from_storage)
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
        Ok(HubuumClassID::new(self.id)?)
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
            .collection_store()
            .get_collection(collection_id_to_storage(self.collection_id))
            .await
            .map_err(ApiError::from)
            .and_then(collection_from_storage)
    }

    async fn collection_id_adapter(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<CollectionID, ApiError> {
        Ok(CollectionID::new(self.collection_id)?)
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
            .class_store()
            .resolve_class(StorageClassSelector::Id(class_id_to_storage(self.id())))
            .await
            .map_err(ApiError::from)
            .and_then(resolved_class_from_storage)
            .map(|target| target.class().clone())
    }
}

impl CollectionAdapter for HubuumClassID {
    async fn collection_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Collection, ApiError> {
        let class = storage_handle(pool)
            .class_store()
            .resolve_class(StorageClassSelector::Id(class_id_to_storage(self.id())))
            .await
            .map_err(ApiError::from)?;
        storage_handle(pool)
            .collection_store()
            .get_collection(class.class().collection_id())
            .await
            .map_err(ApiError::from)
            .and_then(collection_from_storage)
    }

    async fn collection_id_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<CollectionID, ApiError> {
        Ok(CollectionID::new(self.collection(pool).await?.id)?)
    }
}

impl PermissionController for HubuumClass {}
