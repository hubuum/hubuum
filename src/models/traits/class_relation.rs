use crate::errors::ApiError;
use crate::events::EventContext;

use crate::models::search::{FilterField, SortParam};
use crate::models::{
    ClassGraphRow, Collection, CollectionID, HubuumClass, HubuumClassID, HubuumClassRelation,
    HubuumClassRelationID, HubuumClassRelationTransitive, HubuumClassWithPath, HubuumObject,
    HubuumObjectID, HubuumObjectRelation, HubuumObjectRelationID, NewHubuumClassRelation,
    NewHubuumObjectRelation, ObjectGraphRow, ObjectRelationCreateSelector, ObjectRelationSelector,
    PreparedClassRelation, PreparedObjectRelation, RelatedObjectGraphRow,
    ResolvedClassRelationTarget, ResolvedObjectRelationTarget,
};
use crate::storage::{StorageContext, storage_handle};
use crate::traits::accessors::{
    ClassAdapter, CollectionAdapter, IdAccessor, InstanceAdapter, ObjectAdapter,
};
use crate::traits::crud::{DeleteAdapter, SaveAdapter};
use crate::traits::{
    ClassAccessors, CollectionAccessors, CursorPaginated, CursorValue, ObjectAccessors,
    SelfAccessors,
};

async fn prepare_class_relation(
    backend: &impl StorageContext,
    command: &NewHubuumClassRelation,
) -> Result<PreparedClassRelation, ApiError> {
    storage_handle(backend)
        .lifecycle_storage()
        .inner()
        .prepare_class_relation(command.clone())
        .await
        .map_err(ApiError::from)
}

async fn resolve_class_relation(
    backend: &impl StorageContext,
    id: HubuumClassRelationID,
) -> Result<ResolvedClassRelationTarget, ApiError> {
    storage_handle(backend)
        .lifecycle_storage()
        .inner()
        .resolve_class_relation(id)
        .await
        .map_err(ApiError::from)
}

async fn prepare_object_relation(
    backend: &impl StorageContext,
    command: &NewHubuumObjectRelation,
) -> Result<PreparedObjectRelation, ApiError> {
    storage_handle(backend)
        .lifecycle_storage()
        .inner()
        .prepare_object_relation(ObjectRelationCreateSelector::explicit(command.clone()))
        .await
        .map_err(ApiError::from)
}

async fn resolve_object_relation(
    backend: &impl StorageContext,
    id: HubuumObjectRelationID,
) -> Result<ResolvedObjectRelationTarget, ApiError> {
    storage_handle(backend)
        .lifecycle_storage()
        .inner()
        .resolve_object_relation(ObjectRelationSelector::by_id(id))
        .await
        .map_err(ApiError::from)
}

async fn relation_collections(
    backend: &impl StorageContext,
    from_collection_id: i32,
    to_collection_id: i32,
) -> Result<(Collection, Collection), ApiError> {
    let storage = storage_handle(backend).lifecycle_storage();
    let from_collection = storage
        .inner()
        .get_collection(CollectionID::new(from_collection_id)?)
        .await
        .map_err(ApiError::from)?;
    let to_collection = storage
        .inner()
        .get_collection(CollectionID::new(to_collection_id)?)
        .await
        .map_err(ApiError::from)?;
    Ok((from_collection, to_collection))
}

impl IdAccessor for HubuumClassRelationID {
    fn accessor_id(&self) -> i32 {
        // Deref to the owned (Copy) value on purpose: with a `&self` receiver, `self.id()`
        // binds to the `SelfAccessors::id` trait method, which calls back into `accessor_id`
        // and recurses. The inherent `id` is only selected on an owned receiver.
        (*self).id()
    }
}

impl InstanceAdapter<HubuumClassRelation> for HubuumClassRelationID {
    async fn instance_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClassRelation, ApiError> {
        Ok(resolve_class_relation(pool, *self)
            .await?
            .relation()
            .clone())
    }
}
impl IdAccessor for HubuumClassRelation {
    fn accessor_id(&self) -> i32 {
        self.id
    }
}

impl InstanceAdapter<HubuumClassRelation> for HubuumClassRelation {
    async fn instance_adapter(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClassRelation, ApiError> {
        Ok(self.clone())
    }
}

impl DeleteAdapter for HubuumClassRelation {
    async fn delete_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(), ApiError> {
        storage_handle(pool)
            .lifecycle_storage()
            .inner()
            .delete_class_relation_by_id(HubuumClassRelationID::new(self.id)?, None)
            .await
            .map_err(ApiError::from)
    }

    async fn delete_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<(), ApiError> {
        storage_handle(pool)
            .lifecycle_storage()
            .inner()
            .delete_class_relation_by_id(HubuumClassRelationID::new(self.id)?, Some(context))
            .await
            .map_err(ApiError::from)
    }
}

impl SaveAdapter for NewHubuumClassRelation {
    type Output = HubuumClassRelation;

    async fn save_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClassRelation, ApiError> {
        storage_handle(pool)
            .lifecycle_storage()
            .inner()
            .create_class_relation_from_command(self.clone(), None)
            .await
            .map_err(ApiError::from)
    }

    async fn save_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<HubuumClassRelation, ApiError> {
        storage_handle(pool)
            .lifecycle_storage()
            .inner()
            .create_class_relation_from_command(self.clone(), Some(context))
            .await
            .map_err(ApiError::from)
    }
}

impl DeleteAdapter for HubuumClassRelationID {
    async fn delete_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(), ApiError> {
        storage_handle(pool)
            .lifecycle_storage()
            .inner()
            .delete_class_relation_by_id(*self, None)
            .await
            .map_err(ApiError::from)
    }

    async fn delete_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<(), ApiError> {
        storage_handle(pool)
            .lifecycle_storage()
            .inner()
            .delete_class_relation_by_id(*self, Some(context))
            .await
            .map_err(ApiError::from)
    }
}

impl CollectionAdapter<(Collection, Collection), (CollectionID, CollectionID)>
    for NewHubuumClassRelation
{
    async fn collection_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(Collection, Collection), ApiError> {
        let prepared = prepare_class_relation(pool, self).await?;
        relation_collections(
            pool,
            prepared.from_class().collection_id,
            prepared.to_class().collection_id,
        )
        .await
    }

    async fn collection_id_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(CollectionID, CollectionID), ApiError> {
        let (collection_one, collection_two) = self.collection(pool).await?;
        Ok((
            CollectionID::new(collection_one.id)?,
            CollectionID::new(collection_two.id)?,
        ))
    }
}

impl CollectionAdapter<(Collection, Collection), (CollectionID, CollectionID)>
    for NewHubuumObjectRelation
{
    async fn collection_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(Collection, Collection), ApiError> {
        let prepared = prepare_object_relation(pool, self).await?;
        relation_collections(
            pool,
            prepared.from_object().collection_id,
            prepared.to_object().collection_id,
        )
        .await
    }

    async fn collection_id_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(CollectionID, CollectionID), ApiError> {
        let (collection_one, collection_two) = self.collection(pool).await?;
        Ok((
            CollectionID::new(collection_one.id)?,
            CollectionID::new(collection_two.id)?,
        ))
    }
}

impl CollectionAdapter<(Collection, Collection), (CollectionID, CollectionID)>
    for HubuumObjectRelationID
{
    async fn collection_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(Collection, Collection), ApiError> {
        self.instance(pool).await?.collection(pool).await
    }

    async fn collection_id_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(CollectionID, CollectionID), ApiError> {
        self.instance(pool).await?.collection_id(pool).await
    }
}

impl CollectionAdapter<(Collection, Collection), (CollectionID, CollectionID)>
    for HubuumObjectRelation
{
    async fn collection_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(Collection, Collection), ApiError> {
        let target = resolve_object_relation(pool, HubuumObjectRelationID::new(self.id)?).await?;
        relation_collections(
            pool,
            target.from_object().collection_id,
            target.to_object().collection_id,
        )
        .await
    }

    async fn collection_id_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(CollectionID, CollectionID), ApiError> {
        let (collection_one, collection_two) = self.collection(pool).await?;
        Ok((
            CollectionID::new(collection_one.id)?,
            CollectionID::new(collection_two.id)?,
        ))
    }
}

impl CollectionAdapter<(Collection, Collection), (CollectionID, CollectionID)>
    for HubuumClassRelation
{
    async fn collection_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(Collection, Collection), ApiError> {
        let target = resolve_class_relation(pool, HubuumClassRelationID::new(self.id)?).await?;
        relation_collections(
            pool,
            target.from_class().collection_id,
            target.to_class().collection_id,
        )
        .await
    }

    async fn collection_id_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(CollectionID, CollectionID), ApiError> {
        let (collection_one, collection_two) = self.collection(pool).await?;
        Ok((
            CollectionID::new(collection_one.id)?,
            CollectionID::new(collection_two.id)?,
        ))
    }
}

impl ClassAdapter<(HubuumClass, HubuumClass), (HubuumClassID, HubuumClassID)>
    for HubuumClassRelation
{
    async fn class_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(HubuumClass, HubuumClass), ApiError> {
        let target = resolve_class_relation(pool, HubuumClassRelationID::new(self.id)?).await?;
        Ok((target.from_class().clone(), target.to_class().clone()))
    }

    async fn class_id_adapter(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<(HubuumClassID, HubuumClassID), ApiError> {
        Ok((
            HubuumClassID::new(self.from_hubuum_class_id)?,
            HubuumClassID::new(self.to_hubuum_class_id)?,
        ))
    }
}

impl CollectionAdapter<(Collection, Collection), (CollectionID, CollectionID)>
    for HubuumClassRelationID
{
    async fn collection_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(Collection, Collection), ApiError> {
        self.instance(pool).await?.collection(pool).await
    }

    async fn collection_id_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(CollectionID, CollectionID), ApiError> {
        self.instance(pool).await?.collection_id(pool).await
    }
}

impl ClassAdapter<(HubuumClass, HubuumClass), (HubuumClassID, HubuumClassID)>
    for HubuumClassRelationID
{
    async fn class_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(HubuumClass, HubuumClass), ApiError> {
        let target = resolve_class_relation(pool, *self).await?;
        Ok((target.from_class().clone(), target.to_class().clone()))
    }

    async fn class_id_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(HubuumClassID, HubuumClassID), ApiError> {
        self.instance(pool).await?.class_id(pool).await
    }
}

impl ClassAdapter<(HubuumClass, HubuumClass), (HubuumClassID, HubuumClassID)>
    for NewHubuumClassRelation
{
    async fn class_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(HubuumClass, HubuumClass), ApiError> {
        let prepared = prepare_class_relation(pool, self).await?;
        Ok((prepared.from_class().clone(), prepared.to_class().clone()))
    }

    async fn class_id_adapter(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<(HubuumClassID, HubuumClassID), ApiError> {
        Ok((
            HubuumClassID::new(self.from_hubuum_class_id)?,
            HubuumClassID::new(self.to_hubuum_class_id)?,
        ))
    }
}

impl ObjectAdapter<(HubuumObject, HubuumObject), (HubuumObjectID, HubuumObjectID)>
    for NewHubuumObjectRelation
{
    async fn object_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(HubuumObject, HubuumObject), ApiError> {
        let prepared = prepare_object_relation(pool, self).await?;
        Ok((prepared.from_object().clone(), prepared.to_object().clone()))
    }

    async fn object_id_adapter(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<(HubuumObjectID, HubuumObjectID), ApiError> {
        Ok((
            HubuumObjectID::new(self.from_hubuum_object_id)?,
            HubuumObjectID::new(self.to_hubuum_object_id)?,
        ))
    }
}

impl ObjectAdapter<(HubuumObject, HubuumObject), (HubuumObjectID, HubuumObjectID)>
    for HubuumObjectRelationID
{
    async fn object_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(HubuumObject, HubuumObject), ApiError> {
        let target = resolve_object_relation(pool, *self).await?;
        Ok((target.from_object().clone(), target.to_object().clone()))
    }

    async fn object_id_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(HubuumObjectID, HubuumObjectID), ApiError> {
        self.instance(pool).await?.object_id(pool).await
    }
}

impl ObjectAdapter<(HubuumObject, HubuumObject), (HubuumObjectID, HubuumObjectID)>
    for HubuumObjectRelation
{
    async fn object_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(HubuumObject, HubuumObject), ApiError> {
        let target = resolve_object_relation(pool, HubuumObjectRelationID::new(self.id)?).await?;
        Ok((target.from_object().clone(), target.to_object().clone()))
    }

    async fn object_id_adapter(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<(HubuumObjectID, HubuumObjectID), ApiError> {
        Ok((
            HubuumObjectID::new(self.from_hubuum_object_id)?,
            HubuumObjectID::new(self.to_hubuum_object_id)?,
        ))
    }
}

impl ClassGraphRow {
    pub fn to_ascendant_class(&self) -> HubuumClass {
        HubuumClass {
            id: self.ancestor_class_id,
            name: self.ancestor_name.clone(),
            collection_id: self.ancestor_collection_id,
            description: self.ancestor_description.clone(),
            json_schema: self.ancestor_json_schema.clone(),
            validate_schema: self.ancestor_validate_schema,
            created_at: self.ancestor_created_at,
            updated_at: self.ancestor_updated_at,
            revision: self.ancestor_revision,
        }
    }

    pub fn to_descendant_class(&self) -> HubuumClass {
        HubuumClass {
            id: self.descendant_class_id,
            name: self.descendant_name.clone(),
            collection_id: self.descendant_collection_id,
            description: self.descendant_description.clone(),
            json_schema: self.descendant_json_schema.clone(),
            validate_schema: self.descendant_validate_schema,
            created_at: self.descendant_created_at,
            updated_at: self.descendant_updated_at,
            revision: self.descendant_revision,
        }
    }

    pub fn to_descendant_class_with_path(&self) -> HubuumClassWithPath {
        HubuumClassWithPath {
            id: self.descendant_class_id,
            name: self.descendant_name.clone(),
            collection_id: self.descendant_collection_id,
            description: self.descendant_description.clone(),
            json_schema: self.descendant_json_schema.clone(),
            validate_schema: self.descendant_validate_schema,
            created_at: self.descendant_created_at,
            updated_at: self.descendant_updated_at,
            revision: self.descendant_revision,
            path: self.path.clone(),
        }
    }
}

pub trait ToHubuumClasses {
    fn to_descendant_classes(self) -> Vec<HubuumClass>;
    fn to_descendant_classes_with_path(self) -> Vec<HubuumClassWithPath>;
    fn to_ascendant_classes(self) -> Vec<HubuumClass>;
}

impl ToHubuumClasses for Vec<ClassGraphRow> {
    fn to_descendant_classes(self) -> Vec<HubuumClass> {
        self.into_iter()
            .map(|ocv| ocv.to_descendant_class())
            .collect()
    }

    fn to_descendant_classes_with_path(self) -> Vec<HubuumClassWithPath> {
        self.into_iter()
            .map(|ocv| ocv.to_descendant_class_with_path())
            .collect()
    }

    fn to_ascendant_classes(self) -> Vec<HubuumClass> {
        self.into_iter()
            .map(|ocv| ocv.to_ascendant_class())
            .collect()
    }
}

impl CursorPaginated for HubuumClassRelation {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::ClassFrom
                | FilterField::ClassTo
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
                | FilterField::Revision
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::ClassFrom => CursorValue::Integer(self.from_hubuum_class_id as i64),
            FilterField::ClassTo => CursorValue::Integer(self.to_hubuum_class_id as i64),
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            FilterField::Revision => CursorValue::Integer(self.revision.get()),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for class relations",
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

impl CursorPaginated for HubuumObjectRelation {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::ClassRelation
                | FilterField::ObjectFrom
                | FilterField::ObjectTo
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
                | FilterField::Revision
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::ClassRelation => CursorValue::Integer(self.class_relation_id as i64),
            FilterField::ObjectFrom => CursorValue::Integer(self.from_hubuum_object_id as i64),
            FilterField::ObjectTo => CursorValue::Integer(self.to_hubuum_object_id as i64),
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            FilterField::Revision => CursorValue::Integer(self.revision.get()),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for object relations",
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

impl CursorPaginated for HubuumClassRelationTransitive {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::ClassFrom | FilterField::ClassTo | FilterField::Depth | FilterField::Path
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::ClassFrom => CursorValue::Integer(self.ancestor_class_id as i64),
            FilterField::ClassTo => CursorValue::Integer(self.descendant_class_id as i64),
            FilterField::Depth => CursorValue::Integer(self.depth as i64),
            FilterField::Path => {
                CursorValue::IntegerArray(self.path.iter().filter_map(|item| *item).collect())
            }
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for transitive class relations",
                    field
                )));
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![
            SortParam {
                field: FilterField::Depth,
                descending: false,
            },
            SortParam {
                field: FilterField::Path,
                descending: false,
            },
        ]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        vec![
            SortParam {
                field: FilterField::ClassFrom,
                descending: false,
            },
            SortParam {
                field: FilterField::ClassTo,
                descending: false,
            },
            SortParam {
                field: FilterField::Depth,
                descending: false,
            },
            SortParam {
                field: FilterField::Path,
                descending: false,
            },
        ]
    }
}

impl CursorPaginated for ClassGraphRow {
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
                | FilterField::ClassFrom
                | FilterField::ClassTo
                | FilterField::CollectionsFrom
                | FilterField::CollectionsTo
                | FilterField::NameFrom
                | FilterField::NameTo
                | FilterField::DescriptionFrom
                | FilterField::DescriptionTo
                | FilterField::CreatedAtFrom
                | FilterField::CreatedAtTo
                | FilterField::UpdatedAtFrom
                | FilterField::UpdatedAtTo
                | FilterField::Depth
                | FilterField::Path
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id
            | FilterField::ClassTo
            | FilterField::ClassId
            | FilterField::Classes => CursorValue::Integer(self.descendant_class_id as i64),
            FilterField::ClassFrom => CursorValue::Integer(self.ancestor_class_id as i64),
            FilterField::Name | FilterField::NameTo => {
                CursorValue::String(self.descendant_name.clone())
            }
            FilterField::NameFrom => CursorValue::String(self.ancestor_name.clone()),
            FilterField::Description | FilterField::DescriptionTo => {
                CursorValue::String(self.descendant_description.clone())
            }
            FilterField::DescriptionFrom => CursorValue::String(self.ancestor_description.clone()),
            FilterField::Collections | FilterField::CollectionId | FilterField::CollectionsTo => {
                CursorValue::Integer(self.descendant_collection_id as i64)
            }
            FilterField::CollectionsFrom => {
                CursorValue::Integer(self.ancestor_collection_id as i64)
            }
            FilterField::CreatedAt | FilterField::CreatedAtTo => {
                CursorValue::DateTime(self.descendant_created_at)
            }
            FilterField::CreatedAtFrom => CursorValue::DateTime(self.ancestor_created_at),
            FilterField::UpdatedAt | FilterField::UpdatedAtTo => {
                CursorValue::DateTime(self.descendant_updated_at)
            }
            FilterField::UpdatedAtFrom => CursorValue::DateTime(self.ancestor_updated_at),
            FilterField::Depth => CursorValue::Integer(self.depth as i64),
            FilterField::Path => CursorValue::IntegerArray(self.path.clone()),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for related classes",
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
                field: FilterField::ClassId,
                descending: false,
            },
        ]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}

impl CursorPaginated for ObjectGraphRow {
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
                | FilterField::ObjectFrom
                | FilterField::ObjectTo
                | FilterField::ClassFrom
                | FilterField::ClassTo
                | FilterField::CollectionsFrom
                | FilterField::CollectionsTo
                | FilterField::NameFrom
                | FilterField::NameTo
                | FilterField::DescriptionFrom
                | FilterField::DescriptionTo
                | FilterField::CreatedAtFrom
                | FilterField::CreatedAtTo
                | FilterField::UpdatedAtFrom
                | FilterField::UpdatedAtTo
                | FilterField::Depth
                | FilterField::Path
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id | FilterField::ObjectTo => {
                CursorValue::Integer(self.descendant_object_id as i64)
            }
            FilterField::ObjectFrom => CursorValue::Integer(self.ancestor_object_id as i64),
            FilterField::Name | FilterField::NameTo => {
                CursorValue::String(self.descendant_name.clone())
            }
            FilterField::NameFrom => CursorValue::String(self.ancestor_name.clone()),
            FilterField::Description | FilterField::DescriptionTo => {
                CursorValue::String(self.descendant_description.clone())
            }
            FilterField::DescriptionFrom => CursorValue::String(self.ancestor_description.clone()),
            FilterField::Collections | FilterField::CollectionId | FilterField::CollectionsTo => {
                CursorValue::Integer(self.descendant_collection_id as i64)
            }
            FilterField::CollectionsFrom => {
                CursorValue::Integer(self.ancestor_collection_id as i64)
            }
            FilterField::ClassId | FilterField::Classes | FilterField::ClassTo => {
                CursorValue::Integer(self.descendant_class_id as i64)
            }
            FilterField::ClassFrom => CursorValue::Integer(self.ancestor_class_id as i64),
            FilterField::CreatedAt | FilterField::CreatedAtTo => {
                CursorValue::DateTime(self.descendant_created_at)
            }
            FilterField::CreatedAtFrom => CursorValue::DateTime(self.ancestor_created_at),
            FilterField::UpdatedAt | FilterField::UpdatedAtTo => {
                CursorValue::DateTime(self.descendant_updated_at)
            }
            FilterField::UpdatedAtFrom => CursorValue::DateTime(self.ancestor_updated_at),
            FilterField::Depth => CursorValue::Integer(self.depth as i64),
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

impl CursorPaginated for RelatedObjectGraphRow {
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
                | FilterField::ObjectFrom
                | FilterField::ObjectTo
                | FilterField::ClassFrom
                | FilterField::ClassTo
                | FilterField::CollectionsFrom
                | FilterField::CollectionsTo
                | FilterField::NameFrom
                | FilterField::NameTo
                | FilterField::DescriptionFrom
                | FilterField::DescriptionTo
                | FilterField::CreatedAtFrom
                | FilterField::CreatedAtTo
                | FilterField::UpdatedAtFrom
                | FilterField::UpdatedAtTo
                | FilterField::Depth
                | FilterField::Path
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id | FilterField::ObjectTo => {
                CursorValue::Integer(self.descendant_object_id as i64)
            }
            FilterField::ObjectFrom => CursorValue::Integer(self.ancestor_object_id as i64),
            FilterField::Name | FilterField::NameTo => {
                CursorValue::String(self.descendant_name.clone())
            }
            FilterField::NameFrom => CursorValue::String(self.ancestor_name.clone()),
            FilterField::Description | FilterField::DescriptionTo => {
                CursorValue::String(self.descendant_description.clone())
            }
            FilterField::DescriptionFrom => CursorValue::String(self.ancestor_description.clone()),
            FilterField::Collections | FilterField::CollectionId | FilterField::CollectionsTo => {
                CursorValue::Integer(self.descendant_collection_id as i64)
            }
            FilterField::CollectionsFrom => {
                CursorValue::Integer(self.ancestor_collection_id as i64)
            }
            FilterField::ClassId | FilterField::Classes | FilterField::ClassTo => {
                CursorValue::Integer(self.descendant_class_id as i64)
            }
            FilterField::ClassFrom => CursorValue::Integer(self.ancestor_class_id as i64),
            FilterField::CreatedAt | FilterField::CreatedAtTo => {
                CursorValue::DateTime(self.descendant_created_at)
            }
            FilterField::CreatedAtFrom => CursorValue::DateTime(self.ancestor_created_at),
            FilterField::UpdatedAt | FilterField::UpdatedAtTo => {
                CursorValue::DateTime(self.descendant_updated_at)
            }
            FilterField::UpdatedAtFrom => CursorValue::DateTime(self.ancestor_updated_at),
            FilterField::Depth => CursorValue::Integer(self.depth as i64),
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
