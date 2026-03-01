use diesel::prelude::*;
use jsonschema;
use serde_json;

use crate::db::{with_connection, DbPool};
use crate::errors::ApiError;
use crate::models::traits::GroupAccessors;

use crate::models::class::{HubuumClass, HubuumClassID};
use crate::models::namespace::Namespace;
use crate::models::object::{
    HubuumObject, HubuumObjectID, HubuumObjectWithPath, NewHubuumObject, UpdateHubuumObject,
};
use crate::models::permissions::{NewPermission, Permission, Permissions, PermissionsList};
use crate::models::search::{FilterField, SortParam};
use crate::models::user::User;
use crate::traits::{
    CanDelete, CanSave, CanUpdate, ClassAccessors, CursorPaginated, CursorSqlField,
    CursorSqlMapping, CursorSqlType, CursorValue, NamespaceAccessors, PermissionController,
    SelfAccessors, Validate, ValidateAgainstSchema,
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
    async fn validate(&self, pool: &DbPool) -> Result<(), ApiError> {
        let class = HubuumClassID(self.hubuum_class_id).class(pool).await?;

        if class.validate_schema {
            if let Some(ref schema) = class.json_schema {
                self.validate_against_schema(schema).await?;
            }
        }
        Ok(())
    }
}

impl ValidateAgainstSchema for HubuumObject {
    async fn validate_against_schema(&self, schema: &serde_json::Value) -> Result<(), ApiError> {
        jsonschema::validate(schema, &self.data)
            .map_err(|err| ApiError::ValidationError(err.to_string()))?;
        Ok(())
    }
}

impl Validate for NewHubuumObject {
    async fn validate(&self, pool: &DbPool) -> Result<(), ApiError> {
        let class = HubuumClassID(self.hubuum_class_id).class(pool).await?;

        if class.validate_schema {
            if let Some(ref schema) = class.json_schema {
                self.validate_against_schema(schema).await?;
            }
        }
        Ok(())
    }
}

impl ValidateAgainstSchema for NewHubuumObject {
    async fn validate_against_schema(&self, schema: &serde_json::Value) -> Result<(), ApiError> {
        jsonschema::validate(schema, &self.data)
            .map_err(|err| ApiError::ValidationError(err.to_string()))?;
        Ok(())
    }
}

impl Validate for (&UpdateHubuumObject, i32) {
    async fn validate(&self, pool: &DbPool) -> Result<(), ApiError> {
        let (update_obj, object_id) = self;
        let original = HubuumObjectID(*object_id).instance(pool).await?;
        let merged = original.merge_update(update_obj);
        let class = HubuumClassID(merged.hubuum_class_id).class(pool).await?;
        if class.validate_schema {
            if let Some(ref schema) = class.json_schema {
                merged.validate_against_schema(schema).await?;
            }
        }
        Ok(())
    }
}

//
// Save/Update/Delete
//
impl CanSave for HubuumObject {
    type Output = HubuumObject;

    async fn save(&self, pool: &DbPool) -> Result<Self::Output, ApiError> {
        let updated_object = UpdateHubuumObject {
            name: Some(self.name.clone()),
            namespace_id: Some(self.namespace_id),
            hubuum_class_id: Some(self.hubuum_class_id),
            data: Some(self.data.clone()),
            description: Some(self.description.clone()),
        };
        (&updated_object, self.id).validate(pool).await?;
        updated_object.update(pool, self.id).await
    }
}

impl CanSave for NewHubuumObject {
    type Output = HubuumObject;

    async fn save(&self, pool: &DbPool) -> Result<Self::Output, ApiError> {
        use crate::schema::hubuumobject::dsl::*;

        self.validate(pool).await?;

        let mut conn = pool.get()?;
        let result = diesel::insert_into(hubuumobject)
            .values(self)
            .get_result::<Self::Output>(&mut conn)?;

        Ok(result)
    }
}

impl CanUpdate for UpdateHubuumObject {
    type Output = HubuumObject;

    async fn update(&self, pool: &DbPool, object_id: i32) -> Result<Self::Output, ApiError> {
        use crate::schema::hubuumobject::dsl::*;

        let mut conn = pool.get()?;
        let result = diesel::update(hubuumobject)
            .filter(id.eq(object_id))
            .set(self)
            .get_result::<Self::Output>(&mut conn)?;

        Ok(result)
    }
}

impl CanDelete for HubuumObject {
    async fn delete(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};

        let mut conn = pool.get()?;
        diesel::delete(hubuumobject.filter(id.eq(self.id))).execute(&mut conn)?;

        Ok(())
    }
}

//
// Accessors
//
impl SelfAccessors<HubuumObject> for HubuumObject {
    fn id(&self) -> i32 {
        self.id
    }

    async fn instance(&self, _pool: &DbPool) -> Result<HubuumObject, ApiError> {
        Ok(self.clone())
    }
}

impl NamespaceAccessors for HubuumObject {
    async fn namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        use crate::schema::namespaces::dsl::{id, namespaces};

        let mut conn = pool.get()?;
        let namespace = namespaces
            .filter(id.eq(self.namespace_id))
            .first::<Namespace>(&mut conn)?;

        Ok(namespace)
    }

    async fn namespace_id(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.namespace_id)
    }
}

impl ClassAccessors for HubuumObject {
    async fn class(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};

        let mut conn = pool.get()?;
        let class = hubuumclass
            .filter(id.eq(self.hubuum_class_id))
            .first::<HubuumClass>(&mut conn)?;

        Ok(class)
    }

    async fn class_id(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.hubuum_class_id)
    }
}

impl SelfAccessors<HubuumObject> for HubuumObjectID {
    fn id(&self) -> i32 {
        self.0
    }

    async fn instance(&self, pool: &DbPool) -> Result<HubuumObject, ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};
        use diesel::prelude::*;

        let mut conn = pool.get()?;
        let object = hubuumobject
            .filter(id.eq(self.0))
            .first::<HubuumObject>(&mut conn)?;

        Ok(object)
    }
}

impl NamespaceAccessors for HubuumObjectID {
    async fn namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};

        let mut conn = pool.get()?;
        let object = hubuumobject
            .filter(id.eq(self.0))
            .first::<HubuumObject>(&mut conn)?;

        object.namespace(pool).await
    }

    async fn namespace_id(&self, pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.namespace(pool).await?.id)
    }
}

impl ClassAccessors for HubuumObjectID {
    async fn class(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};

        let mut conn = pool.get()?;
        let object = hubuumobject
            .filter(id.eq(self.0))
            .first::<HubuumObject>(&mut conn)?;

        object.class(pool).await
    }

    async fn class_id(&self, pool: &DbPool) -> Result<i32, ApiError> {
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
                )))
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
                )))
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
                )))
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
                column: "object_closure_view.descendant_object_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "object_closure_view.descendant_name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Namespaces | FilterField::NamespaceId => CursorSqlField {
                column: "object_closure_view.descendant_namespace_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ClassId | FilterField::Classes => CursorSqlField {
                column: "object_closure_view.descendant_class_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "object_closure_view.descendant_created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "object_closure_view.descendant_updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::Path => CursorSqlField {
                column: "object_closure_view.path",
                sql_type: CursorSqlType::IntegerArray,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for related objects",
                    field
                )))
            }
        })
    }
}
