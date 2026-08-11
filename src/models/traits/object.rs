use crate::errors::ApiError;
use crate::events::EventContext;

use crate::models::class::{HubuumClass, HubuumClassID, ResolvedClassTarget};
use crate::models::collection::{Collection, CollectionID};
use crate::models::object::{
    HubuumObject, HubuumObjectID, HubuumObjectWithPath, NewHubuumObject, ObjectSelector,
    ResolvedObjectTarget, UpdateHubuumObject,
};
use crate::models::object_data_patch::ObjectDataPatchDocument;
use crate::models::search::{FilterField, SortParam};
use crate::storage::{ObjectRecordStorage, StorageContext, storage_handle};
use crate::traits::accessors::{ClassAdapter, CollectionAdapter, IdAccessor, InstanceAdapter};
use crate::traits::crud::{DeleteAdapter, SaveAdapter, UpdateAdapter};
use crate::traits::{
    ClassAccessors, CollectionAccessors, CursorPaginated, CursorValue, PermissionController,
    Validate, ValidateAgainstSchema,
};
use tracing::debug;

pub async fn check_if_object_in_class<C, O>(
    pool: &impl crate::storage::StorageContext,
    class: &C,
    object: &O,
) -> Result<(), ApiError>
where
    C: crate::traits::SelfAccessors<HubuumClass>,
    O: crate::traits::SelfAccessors<HubuumObject> + ClassAccessors<HubuumClass>,
{
    let object_class_id = object.class_id(pool).await?.id();

    if object_class_id != class.id() {
        debug!(
            message = "Object class mismatch",
            class_id = class.id(),
            object_id = object.id(),
            object_class = object_class_id
        );
        return Err(ApiError::NotFound(format!(
            "Object {} is not of class {}",
            object.id(),
            class.id()
        )));
    }

    Ok(())
}

impl HubuumObject {
    /// Create a new HubuumObject merged with the update object.
    ///
    /// This method will take the current object and merge it with the provided update object,
    /// returning a new HubuumObject. If a field in the update object is `None`, the corresponding
    /// field in the current object will be used.
    ///
    /// ## Arguments
    ///
    /// * `update` - A reference to the `UpdateHubuumObject` containing the new values.
    ///
    /// ## Returns
    ///
    /// * A new `HubuumObject` with the merged values.
    pub fn merge_update(&self, update: &UpdateHubuumObject) -> Self {
        Self {
            name: update.name.clone().unwrap_or_else(|| self.name.clone()),
            collection_id: update.collection_id.unwrap_or(self.collection_id),
            hubuum_class_id: update.hubuum_class_id.unwrap_or(self.hubuum_class_id),
            data: update.data.clone().unwrap_or_else(|| self.data.clone()),
            description: update
                .description
                .clone()
                .unwrap_or_else(|| self.description.clone()),
            created_at: self.created_at,
            updated_at: chrono::Local::now().naive_local(),
            id: self.id,
            revision: self.revision,
        }
    }
}

impl Validate for HubuumObject {
    async fn validate<C>(&self, backend: &C) -> Result<(), ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .validate_object(self)
            .await
            .map_err(ApiError::from)
    }
}

impl ValidateAgainstSchema for HubuumObject {
    async fn validate_against_schema(&self, schema: &serde_json::Value) -> Result<(), ApiError> {
        crate::utilities::json_schema::validate_json_value(schema, &self.data)
    }
}

impl Validate for NewHubuumObject {
    async fn validate<C>(&self, backend: &C) -> Result<(), ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .validate_new_object(self)
            .await
            .map_err(ApiError::from)
    }
}

impl ValidateAgainstSchema for NewHubuumObject {
    async fn validate_against_schema(&self, schema: &serde_json::Value) -> Result<(), ApiError> {
        crate::utilities::json_schema::validate_json_value(schema, &self.data)
    }
}

impl Validate for (&UpdateHubuumObject, i32) {
    async fn validate<C>(&self, backend: &C) -> Result<(), ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .validate_object_update(self.0, self.1)
            .await
            .map_err(ApiError::from)
    }
}

//
// Save/Update/Delete
//
impl SaveAdapter for HubuumObject {
    type Output = HubuumObject;

    async fn save_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Self::Output, ApiError> {
        storage_handle(pool)
            .save_object_record(self, None)
            .await
            .map_err(ApiError::from)
    }

    async fn save_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<Self::Output, ApiError> {
        storage_handle(pool)
            .save_object_record(self, Some(context))
            .await
            .map_err(ApiError::from)
    }
}

impl SaveAdapter for NewHubuumObject {
    type Output = HubuumObject;

    async fn save_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Self::Output, ApiError> {
        storage_handle(pool)
            .create_object_record(self, None)
            .await
            .map_err(ApiError::from)
    }

    async fn save_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<Self::Output, ApiError> {
        storage_handle(pool)
            .create_object_record(self, Some(context))
            .await
            .map_err(ApiError::from)
    }
}

impl UpdateAdapter for UpdateHubuumObject {
    type Output = HubuumObject;
    type Identifier = HubuumObjectID;

    async fn update_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
        object_id: HubuumObjectID,
    ) -> Result<Self::Output, ApiError> {
        storage_handle(pool)
            .update_object_record(self, object_id.id(), None)
            .await
            .map_err(ApiError::from)
    }

    async fn update_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        object_id: HubuumObjectID,
        context: &EventContext,
    ) -> Result<Self::Output, ApiError> {
        storage_handle(pool)
            .update_object_record(self, object_id.id(), Some(context))
            .await
            .map_err(ApiError::from)
    }
}

pub trait PatchObjectData {
    async fn patch_object_data<C>(
        &self,
        backend: &C,
        target: &ResolvedObjectTarget,
        context: &EventContext,
    ) -> Result<HubuumObject, ApiError>
    where
        C: StorageContext;
}

impl PatchObjectData for ObjectDataPatchDocument {
    async fn patch_object_data<C>(
        &self,
        backend: &C,
        target: &ResolvedObjectTarget,
        context: &EventContext,
    ) -> Result<HubuumObject, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .lifecycle_storage()
            .inner()
            .patch_object_data(target, self.clone(), context)
            .await
            .map_err(ApiError::from)
    }
}

pub trait ResolveObjectTarget {
    async fn resolve_object_target<C>(&self, backend: &C) -> Result<ResolvedObjectTarget, ApiError>
    where
        C: StorageContext;
}

pub trait CreateObjectInResolvedClass {
    async fn create_object_in_resolved_class<C>(
        &self,
        backend: &C,
        target: &ResolvedClassTarget,
        context: &EventContext,
    ) -> Result<HubuumObject, ApiError>
    where
        C: StorageContext;
}

impl CreateObjectInResolvedClass for NewHubuumObject {
    async fn create_object_in_resolved_class<C>(
        &self,
        backend: &C,
        target: &ResolvedClassTarget,
        context: &EventContext,
    ) -> Result<HubuumObject, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .lifecycle_storage()
            .inner()
            .create_object(target, self.clone(), context)
            .await
            .map_err(ApiError::from)
    }
}

impl ResolveObjectTarget for ObjectSelector {
    async fn resolve_object_target<C>(&self, backend: &C) -> Result<ResolvedObjectTarget, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .lifecycle_storage()
            .inner()
            .resolve_object(self.clone())
            .await
            .map_err(ApiError::from)
    }
}

pub trait UpdateResolvedObject {
    async fn update_resolved_object<C>(
        &self,
        backend: &C,
        target: &ResolvedObjectTarget,
        context: &EventContext,
    ) -> Result<HubuumObject, ApiError>
    where
        C: StorageContext;
}

impl UpdateResolvedObject for UpdateHubuumObject {
    async fn update_resolved_object<C>(
        &self,
        backend: &C,
        target: &ResolvedObjectTarget,
        context: &EventContext,
    ) -> Result<HubuumObject, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .lifecycle_storage()
            .inner()
            .update_object(target, self.clone(), context)
            .await
            .map_err(ApiError::from)
    }
}

pub trait DeleteResolvedObject {
    async fn delete_resolved_object<C>(
        &self,
        backend: &C,
        context: &EventContext,
    ) -> Result<(), ApiError>
    where
        C: StorageContext;
}

impl DeleteResolvedObject for ResolvedObjectTarget {
    async fn delete_resolved_object<C>(
        &self,
        backend: &C,
        context: &EventContext,
    ) -> Result<(), ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .lifecycle_storage()
            .inner()
            .delete_object(self, context)
            .await
            .map_err(ApiError::from)
    }
}

impl DeleteAdapter for HubuumObject {
    async fn delete_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(), ApiError> {
        storage_handle(pool)
            .delete_object_record(self, None)
            .await
            .map_err(ApiError::from)
    }

    async fn delete_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<(), ApiError> {
        storage_handle(pool)
            .delete_object_record(self, Some(context))
            .await
            .map_err(ApiError::from)
    }
}

//
// Accessors
//
impl IdAccessor for HubuumObject {
    fn accessor_id(&self) -> i32 {
        self.id
    }
}

impl InstanceAdapter<HubuumObject> for HubuumObject {
    async fn instance_adapter(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumObject, ApiError> {
        Ok(self.clone())
    }
}

impl CollectionAdapter for HubuumObject {
    async fn collection_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Collection, ApiError> {
        storage_handle(pool)
            .object_collection(self.id)
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

impl ClassAdapter for HubuumObject {
    async fn class_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClass, ApiError> {
        storage_handle(pool)
            .object_class(self.id)
            .await
            .map_err(ApiError::from)
    }

    async fn class_id_adapter(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClassID, ApiError> {
        HubuumClassID::new(self.hubuum_class_id)
    }
}

impl IdAccessor for HubuumObjectID {
    fn accessor_id(&self) -> i32 {
        // Deref to the owned (Copy) value on purpose: with a `&self` receiver, `self.id()`
        // binds to the `SelfAccessors::id` trait method, which calls back into `accessor_id`
        // and recurses. The inherent `id` is only selected on an owned receiver.
        (*self).id()
    }
}

impl InstanceAdapter<HubuumObject> for HubuumObjectID {
    async fn instance_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumObject, ApiError> {
        storage_handle(pool)
            .load_object_record(self.id())
            .await
            .map_err(ApiError::from)
    }
}

impl CollectionAdapter for HubuumObjectID {
    async fn collection_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Collection, ApiError> {
        storage_handle(pool)
            .object_collection(self.id())
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

impl ClassAdapter for HubuumObjectID {
    async fn class_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClass, ApiError> {
        storage_handle(pool)
            .object_class(self.id())
            .await
            .map_err(ApiError::from)
    }

    async fn class_id_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClassID, ApiError> {
        HubuumClassID::new(self.class(pool).await?.id)
    }
}

impl PermissionController for HubuumObject {}
impl PermissionController for HubuumObjectID {}

impl CursorPaginated for HubuumObject {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::Description
                | FilterField::Collections
                | FilterField::CollectionId
                | FilterField::ClassId
                | FilterField::Classes
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
                | FilterField::Revision
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::Name => CursorValue::String(self.name.clone()),
            FilterField::Description => CursorValue::String(self.description.clone()),
            FilterField::Collections | FilterField::CollectionId => {
                CursorValue::Integer(self.collection_id as i64)
            }
            FilterField::ClassId | FilterField::Classes => {
                CursorValue::Integer(self.hubuum_class_id as i64)
            }
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            FilterField::Revision => CursorValue::Integer(self.revision.get()),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for objects",
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

impl CursorPaginated for HubuumObjectWithPath {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::Collections
                | FilterField::CollectionId
                | FilterField::ClassId
                | FilterField::Classes
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
                | FilterField::Path
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::Name => CursorValue::String(self.name.clone()),
            FilterField::Collections | FilterField::CollectionId => {
                CursorValue::Integer(self.collection_id as i64)
            }
            FilterField::ClassId | FilterField::Classes => {
                CursorValue::Integer(self.hubuum_class_id as i64)
            }
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            FilterField::Path => CursorValue::IntegerArray(self.path.clone()),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for related objects",
                    field
                )));
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![
            SortParam {
                field: FilterField::Path,
                descending: false,
            },
            SortParam {
                field: FilterField::Id,
                descending: false,
            },
        ]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}
