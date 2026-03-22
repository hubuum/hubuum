use crate::db::DbPool;
use crate::db::traits::object::{
    DeleteObjectRecord, LoadObjectRecord, ObjectClassLookup, ObjectNamespaceLookup,
    SaveObjectRecord, UpdateObjectRecord, ValidateObjectRecord, ValidateObjectSchema,
};
use crate::errors::ApiError;

use crate::models::class::HubuumClass;
use crate::models::namespace::Namespace;
use crate::models::object::{
    HubuumObject, HubuumObjectID, HubuumObjectWithPath, NewHubuumObject, UpdateHubuumObject,
};
use crate::models::search::{FilterField, SortParam};
use crate::traits::accessors::{ClassAdapter, IdAccessor, InstanceAdapter, NamespaceAdapter};
use crate::traits::crud::{DeleteAdapter, SaveAdapter, UpdateAdapter};
use crate::traits::{
    BackendContext, ClassAccessors, CursorPaginated, CursorSqlField, CursorSqlMapping,
    CursorSqlType, CursorValue, NamespaceAccessors, PermissionController, Validate,
    ValidateAgainstSchema,
};

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
            namespace_id: update.namespace_id.unwrap_or(self.namespace_id),
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

// Validators
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

    async fn save_adapter(&self, pool: &DbPool) -> Result<Self::Output, ApiError> {
        self.save_object_record(pool).await
    }
}

impl SaveAdapter for NewHubuumObject {
    type Output = HubuumObject;

    async fn save_adapter(&self, pool: &DbPool) -> Result<Self::Output, ApiError> {
        self.save_object_record(pool).await
    }
}

impl UpdateAdapter for UpdateHubuumObject {
    type Output = HubuumObject;

    async fn update_adapter(
        &self,
        pool: &DbPool,
        object_id: i32,
    ) -> Result<Self::Output, ApiError> {
        self.update_object_record(pool, object_id).await
    }
}

impl DeleteAdapter for HubuumObject {
    async fn delete_adapter(&self, pool: &DbPool) -> Result<(), ApiError> {
        self.delete_object_record(pool).await
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

impl NamespaceAdapter for HubuumObject {
    async fn namespace_adapter(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        self.lookup_object_namespace(pool).await
    }

    async fn namespace_id_adapter(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.namespace_id)
    }
}

impl ClassAdapter for HubuumObject {
    async fn class_adapter(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        self.lookup_object_class(pool).await
    }

    async fn class_id_adapter(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.hubuum_class_id)
    }
}

impl IdAccessor for HubuumObjectID {
    fn accessor_id(&self) -> i32 {
        self.0
    }
}

impl InstanceAdapter<HubuumObject> for HubuumObjectID {
    async fn instance_adapter(&self, pool: &DbPool) -> Result<HubuumObject, ApiError> {
        self.load_object_record(pool).await
    }
}

impl NamespaceAdapter for HubuumObjectID {
    async fn namespace_adapter(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        self.lookup_object_namespace(pool).await
    }

    async fn namespace_id_adapter(&self, pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.namespace(pool).await?.id)
    }
}

impl ClassAdapter for HubuumObjectID {
    async fn class_adapter(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        self.lookup_object_class(pool).await
    }

    async fn class_id_adapter(&self, pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.class(pool).await?.id)
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
                | FilterField::Namespaces
                | FilterField::NamespaceId
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
            FilterField::Namespaces | FilterField::NamespaceId => {
                CursorValue::Integer(self.namespace_id as i64)
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
            FilterField::Namespaces | FilterField::NamespaceId => CursorSqlField {
                column: "hubuumobject.namespace_id",
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
                | FilterField::Namespaces
                | FilterField::NamespaceId
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
            FilterField::Namespaces | FilterField::NamespaceId => {
                CursorValue::Integer(self.namespace_id as i64)
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
            FilterField::Namespaces | FilterField::NamespaceId => CursorSqlField {
                column: "descendant_namespace_id",
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
