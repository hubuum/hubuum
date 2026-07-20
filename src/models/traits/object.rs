use crate::db::DbPool;
use crate::db::traits::object::{
    CreateObjectInResolvedClassRecord, DeleteObjectRecord, DeleteResolvedObjectRecord,
    LoadObjectRecord, ObjectClassLookup, ObjectCollectionLookup, PatchObjectDataRecord,
    ResolveObjectSelectorRecord, SaveObjectRecord, UpdateObjectRecord, UpdateResolvedObjectRecord,
    ValidateObjectRecord, ValidateObjectSchema,
};
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
use crate::traits::accessors::{ClassAdapter, CollectionAdapter, IdAccessor, InstanceAdapter};
use crate::traits::crud::{DeleteAdapter, SaveAdapter, UpdateAdapter};
use crate::traits::{
    BackendContext, ClassAccessors, CollectionAccessors, CursorPaginated, CursorSqlField,
    CursorSqlMapping, CursorSqlType, CursorValue, PermissionController, Validate,
    ValidateAgainstSchema,
};
use tracing::debug;

pub async fn check_if_object_in_class<C, O>(
    pool: &DbPool,
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
        }
    }
}

impl Validate for HubuumObject {
    async fn validate<C>(&self, backend: &C) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.validate_object_record(backend.db_pool()).await
    }
}

impl ValidateAgainstSchema for HubuumObject {
    async fn validate_against_schema(&self, schema: &serde_json::Value) -> Result<(), ApiError> {
        self.validate_object_schema(schema)
    }
}

impl Validate for NewHubuumObject {
    async fn validate<C>(&self, backend: &C) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.validate_object_record(backend.db_pool()).await
    }
}

impl ValidateAgainstSchema for NewHubuumObject {
    async fn validate_against_schema(&self, schema: &serde_json::Value) -> Result<(), ApiError> {
        self.validate_object_schema(schema)
    }
}

impl Validate for (&UpdateHubuumObject, i32) {
    async fn validate<C>(&self, backend: &C) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.validate_object_record(backend.db_pool()).await
    }
}

//
// Save/Update/Delete
//
impl SaveAdapter for HubuumObject {
    type Output = HubuumObject;

    async fn save_adapter_without_events(&self, pool: &DbPool) -> Result<Self::Output, ApiError> {
        self.save_object_record_without_events(pool).await
    }

    async fn save_adapter(
        &self,
        pool: &DbPool,
        context: &EventContext,
    ) -> Result<Self::Output, ApiError> {
        self.save_object_record(pool, Some(context)).await
    }
}

impl SaveAdapter for NewHubuumObject {
    type Output = HubuumObject;

    async fn save_adapter_without_events(&self, pool: &DbPool) -> Result<Self::Output, ApiError> {
        self.save_object_record_without_events(pool).await
    }

    async fn save_adapter(
        &self,
        pool: &DbPool,
        context: &EventContext,
    ) -> Result<Self::Output, ApiError> {
        self.save_object_record(pool, Some(context)).await
    }
}

impl UpdateAdapter for UpdateHubuumObject {
    type Output = HubuumObject;

    async fn update_adapter_without_events(
        &self,
        pool: &DbPool,
        object_id: i32,
    ) -> Result<Self::Output, ApiError> {
        self.update_object_record_without_events(pool, object_id)
            .await
    }

    async fn update_adapter(
        &self,
        pool: &DbPool,
        object_id: i32,
        context: &EventContext,
    ) -> Result<Self::Output, ApiError> {
        self.update_object_record(pool, object_id, Some(context))
            .await
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
        C: BackendContext + ?Sized;
}

impl PatchObjectData for ObjectDataPatchDocument {
    async fn patch_object_data<C>(
        &self,
        backend: &C,
        target: &ResolvedObjectTarget,
        context: &EventContext,
    ) -> Result<HubuumObject, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.patch_object_data_record(backend.db_pool(), target, context)
            .await
    }
}

pub trait ResolveObjectTarget {
    async fn resolve_object_target<C>(&self, backend: &C) -> Result<ResolvedObjectTarget, ApiError>
    where
        C: BackendContext + ?Sized;
}

pub trait CreateObjectInResolvedClass {
    async fn create_object_in_resolved_class<C>(
        &self,
        backend: &C,
        target: &ResolvedClassTarget,
        context: &EventContext,
    ) -> Result<HubuumObject, ApiError>
    where
        C: BackendContext + ?Sized;
}

impl CreateObjectInResolvedClass for NewHubuumObject {
    async fn create_object_in_resolved_class<C>(
        &self,
        backend: &C,
        target: &ResolvedClassTarget,
        context: &EventContext,
    ) -> Result<HubuumObject, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.create_object_in_resolved_class_record(backend.db_pool(), target, context)
            .await
    }
}

impl ResolveObjectTarget for ObjectSelector {
    async fn resolve_object_target<C>(&self, backend: &C) -> Result<ResolvedObjectTarget, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let (class, object) = self
            .resolve_object_selector_record(backend.db_pool())
            .await?;
        Ok(ResolvedObjectTarget::new(self.clone(), class, object))
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
        C: BackendContext + ?Sized;
}

impl UpdateResolvedObject for UpdateHubuumObject {
    async fn update_resolved_object<C>(
        &self,
        backend: &C,
        target: &ResolvedObjectTarget,
        context: &EventContext,
    ) -> Result<HubuumObject, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.update_resolved_object_record(backend.db_pool(), target, context)
            .await
    }
}

pub trait DeleteResolvedObject {
    async fn delete_resolved_object<C>(
        &self,
        backend: &C,
        context: &EventContext,
    ) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized;
}

impl DeleteResolvedObject for ResolvedObjectTarget {
    async fn delete_resolved_object<C>(
        &self,
        backend: &C,
        context: &EventContext,
    ) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.delete_resolved_object_record(backend.db_pool(), context)
            .await
    }
}

impl DeleteAdapter for HubuumObject {
    async fn delete_adapter_without_events(&self, pool: &DbPool) -> Result<(), ApiError> {
        self.delete_object_record_without_events(pool).await
    }

    async fn delete_adapter(&self, pool: &DbPool, context: &EventContext) -> Result<(), ApiError> {
        self.delete_object_record(pool, Some(context)).await
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
    async fn instance_adapter(&self, _pool: &DbPool) -> Result<HubuumObject, ApiError> {
        Ok(self.clone())
    }
}

impl CollectionAdapter for HubuumObject {
    async fn collection_adapter(&self, pool: &DbPool) -> Result<Collection, ApiError> {
        self.lookup_object_collection(pool).await
    }

    async fn collection_id_adapter(&self, _pool: &DbPool) -> Result<CollectionID, ApiError> {
        CollectionID::new(self.collection_id)
    }
}

impl ClassAdapter for HubuumObject {
    async fn class_adapter(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        self.lookup_object_class(pool).await
    }

    async fn class_id_adapter(&self, _pool: &DbPool) -> Result<HubuumClassID, ApiError> {
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
    async fn instance_adapter(&self, pool: &DbPool) -> Result<HubuumObject, ApiError> {
        self.load_object_record(pool).await
    }
}

impl CollectionAdapter for HubuumObjectID {
    async fn collection_adapter(&self, pool: &DbPool) -> Result<Collection, ApiError> {
        self.lookup_object_collection(pool).await
    }

    async fn collection_id_adapter(&self, pool: &DbPool) -> Result<CollectionID, ApiError> {
        CollectionID::new(self.collection(pool).await?.id)
    }
}

impl ClassAdapter for HubuumObjectID {
    async fn class_adapter(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        self.lookup_object_class(pool).await
    }

    async fn class_id_adapter(&self, pool: &DbPool) -> Result<HubuumClassID, ApiError> {
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

impl CursorSqlMapping for HubuumObject {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "hubuumobject.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "hubuumobject.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Description => CursorSqlField {
                column: "hubuumobject.description",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Collections | FilterField::CollectionId => CursorSqlField {
                column: "hubuumobject.collection_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ClassId | FilterField::Classes => CursorSqlField {
                column: "hubuumobject.hubuum_class_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "hubuumobject.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "hubuumobject.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for objects",
                    field
                )));
            }
        })
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

impl CursorSqlMapping for HubuumObjectWithPath {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "descendant_object_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "descendant_name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Collections | FilterField::CollectionId => CursorSqlField {
                column: "descendant_collection_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ClassId | FilterField::Classes => CursorSqlField {
                column: "descendant_class_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "descendant_created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "descendant_updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::Path => CursorSqlField {
                column: "path",
                sql_type: CursorSqlType::IntegerArray,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for related objects",
                    field
                )));
            }
        })
    }
}
