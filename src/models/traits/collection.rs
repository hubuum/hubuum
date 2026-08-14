use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::collection::{
    Collection, CollectionID, NewCollection, NewCollectionWithAssignee, UpdateCollection,
};
use crate::models::group::GroupID;
use crate::models::search::{FilterField, SortParam};
use crate::services::storage_boundary::{
    collection_create_to_storage, collection_from_storage, collection_update_to_storage,
};
use crate::storage::{StorageContext, storage_handle};
use crate::traits::accessors::{CollectionAdapter, IdAccessor, InstanceAdapter};
use crate::traits::crud::{DeleteAdapter, SaveAdapter, UpdateAdapter};
use crate::traits::{CollectionAccessors, CursorPaginated, PermissionController};

impl SaveAdapter for Collection {
    type Output = Collection;

    async fn save_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Self::Output, ApiError> {
        let updated_collection = UpdateCollection {
            name: Some(self.name.clone()),
            description: Some(self.description.clone()),
        };
        storage_handle(pool)
            .collection_store()
            .update_collection(
                self.id,
                collection_update_to_storage(updated_collection),
                None,
            )
            .await
            .map_err(ApiError::from)
            .and_then(collection_from_storage)
    }

    async fn save_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<Self::Output, ApiError> {
        let updated_collection = UpdateCollection {
            name: Some(self.name.clone()),
            description: Some(self.description.clone()),
        };
        storage_handle(pool)
            .collection_store()
            .update_collection(
                self.id,
                collection_update_to_storage(updated_collection),
                Some(context),
            )
            .await
            .map_err(ApiError::from)
            .and_then(collection_from_storage)
    }
}

impl DeleteAdapter for Collection {
    async fn delete_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(), ApiError> {
        storage_handle(pool)
            .collection_store()
            .delete_collection(self.id, None)
            .await
            .map_err(ApiError::from)
    }

    async fn delete_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<(), ApiError> {
        storage_handle(pool)
            .collection_store()
            .delete_collection(self.id, Some(context))
            .await
            .map_err(ApiError::from)
    }
}

impl DeleteAdapter for CollectionID {
    async fn delete_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(), ApiError> {
        storage_handle(pool)
            .collection_store()
            .delete_collection(self.id(), None)
            .await
            .map_err(ApiError::from)
    }

    async fn delete_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<(), ApiError> {
        storage_handle(pool)
            .collection_store()
            .delete_collection(self.id(), Some(context))
            .await
            .map_err(ApiError::from)
    }
}

impl UpdateAdapter for UpdateCollection {
    type Output = Collection;
    type Identifier = CollectionID;

    async fn update_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
        target_collection_id: CollectionID,
    ) -> Result<Self::Output, ApiError> {
        storage_handle(pool)
            .collection_store()
            .update_collection(
                target_collection_id.id(),
                collection_update_to_storage(self.clone()),
                None,
            )
            .await
            .map_err(ApiError::from)
            .and_then(collection_from_storage)
    }

    async fn update_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        target_collection_id: CollectionID,
        context: &EventContext,
    ) -> Result<Self::Output, ApiError> {
        storage_handle(pool)
            .collection_store()
            .update_collection(
                target_collection_id.id(),
                collection_update_to_storage(self.clone()),
                Some(context),
            )
            .await
            .map_err(ApiError::from)
            .and_then(collection_from_storage)
    }
}

impl SaveAdapter for NewCollectionWithAssignee {
    type Output = Collection;

    async fn save_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Collection, ApiError> {
        storage_handle(pool)
            .collection_store()
            .create_collection(collection_create_to_storage(self.clone()), None)
            .await
            .map_err(ApiError::from)
            .and_then(collection_from_storage)
    }

    async fn save_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<Collection, ApiError> {
        storage_handle(pool)
            .collection_store()
            .create_collection(collection_create_to_storage(self.clone()), Some(context))
            .await
            .map_err(ApiError::from)
            .and_then(collection_from_storage)
    }
}

impl IdAccessor for Collection {
    fn accessor_id(&self) -> i32 {
        self.id
    }
}

impl InstanceAdapter<Collection> for Collection {
    async fn instance_adapter(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<Collection, ApiError> {
        Ok(self.clone())
    }
}

impl CollectionAdapter for Collection {
    async fn collection_adapter(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<Collection, ApiError> {
        Ok(self.clone())
    }

    async fn collection_id_adapter(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<CollectionID, ApiError> {
        CollectionID::new(self.id)
    }
}

impl IdAccessor for CollectionID {
    fn accessor_id(&self) -> i32 {
        // Deref to the owned (Copy) value on purpose: with a `&self` receiver, `self.id()`
        // binds to the `SelfAccessors::id` trait method, which calls back into `accessor_id`
        // and recurses. The inherent `id` is only selected on an owned receiver.
        (*self).id()
    }
}

impl InstanceAdapter<Collection> for CollectionID {
    async fn instance_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Collection, ApiError> {
        self.collection(pool).await
    }
}

impl CollectionAdapter for CollectionID {
    async fn collection_id_adapter(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<CollectionID, ApiError> {
        Ok(*self)
    }

    async fn collection_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Collection, ApiError> {
        storage_handle(pool)
            .collection_store()
            .get_collection(self.id())
            .await
            .map_err(ApiError::from)
            .and_then(collection_from_storage)
    }
}

impl NewCollection {
    /// Create a collection and grant the full collection permission set to the assignee group.
    ///
    /// This is a convenience wrapper around the backend transaction that creates the collection
    /// record and the corresponding permission record together.
    pub async fn save_and_grant_all_to<C>(
        self,
        backend: &C,
        assignee: GroupID,
    ) -> Result<Collection, ApiError>
    where
        C: StorageContext,
    {
        let command = NewCollectionWithAssignee {
            name: self.name,
            description: self.description,
            group_id: assignee,
            parent_collection_id: self
                .parent_collection_id
                .map(CollectionID::new)
                .transpose()?,
        };
        storage_handle(backend)
            .collection_store()
            .create_collection(collection_create_to_storage(command), None)
            .await
            .map_err(ApiError::from)
            .and_then(collection_from_storage)
    }

    /// Persist the collection and apply permissions using the assignee embedded in the supplied
    /// `NewCollectionWithAssignee`.
    ///
    /// This delegates into the same backend helper as [`Self::save_and_grant_all_to`], but takes
    /// the assignee from the provided wrapper value.
    pub async fn update_with_permissions<C>(
        self,
        backend: &C,
        collection_with_assignee: NewCollectionWithAssignee,
    ) -> Result<Collection, ApiError>
    where
        C: StorageContext,
    {
        let command = NewCollectionWithAssignee {
            name: self.name,
            description: self.description,
            group_id: collection_with_assignee.group_id,
            parent_collection_id: self
                .parent_collection_id
                .map(CollectionID::new)
                .transpose()?,
        };
        storage_handle(backend)
            .collection_store()
            .create_collection(collection_create_to_storage(command), None)
            .await
            .map_err(ApiError::from)
            .and_then(collection_from_storage)
    }
}

impl PermissionController for Collection {}
impl PermissionController for CollectionID {}

impl CursorPaginated for Collection {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::Description
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
                | FilterField::Revision
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<crate::traits::CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => crate::traits::CursorValue::Integer(self.id as i64),
            FilterField::Name => crate::traits::CursorValue::String(self.name.clone()),
            FilterField::Description => {
                crate::traits::CursorValue::String(self.description.clone())
            }
            FilterField::CreatedAt => crate::traits::CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => crate::traits::CursorValue::DateTime(self.updated_at),
            FilterField::Revision => crate::traits::CursorValue::Integer(self.revision.get()),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for collections",
                    field
                )));
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}
