use diesel::{
    AsChangeset, ExpressionMethods, Insertable, QueryDsl, Queryable, QueryableByName, Selectable,
};
use diesel_async::RunQueryDsl;

use crate::errors::ApiError;
use crate::models::{
    HubuumObject, HubuumObjectID, HubuumObjectRelation, HubuumObjectRelationID, NewHubuumObject,
    NewHubuumObjectRelation, UpdateHubuumObject,
};
use crate::pagination::{CursorSqlField, CursorSqlMapping, CursorSqlType};
use crate::storage::postgres::operations::GetObject;
use crate::storage::postgres::{PostgresRevision, with_connection};
use crate::traits::{CursorPaginated, CursorValue, SelfAccessors};

/// PostgreSQL representation of an object row.
///
/// The domain object is intentionally free of Diesel and schema bindings. All
/// query construction and row decoding stay inside the PostgreSQL adapter.
#[derive(Clone, Queryable, QueryableByName, Selectable)]
#[diesel(table_name = crate::schema::hubuumobject)]
pub(crate) struct HubuumObjectRow {
    pub(crate) id: i32,
    pub(crate) name: String,
    pub(crate) collection_id: i32,
    pub(crate) hubuum_class_id: i32,
    pub(crate) data: serde_json::Value,
    pub(crate) description: String,
    pub(crate) created_at: chrono::NaiveDateTime,
    pub(crate) updated_at: chrono::NaiveDateTime,
    pub(crate) revision: PostgresRevision,
}

impl From<HubuumObjectRow> for HubuumObject {
    fn from(row: HubuumObjectRow) -> Self {
        Self {
            id: row.id,
            name: row.name,
            collection_id: row.collection_id,
            hubuum_class_id: row.hubuum_class_id,
            data: row.data,
            description: row.description,
            created_at: row.created_at,
            updated_at: row.updated_at,
            revision: row.revision.into_domain(),
        }
    }
}

/// Legacy import-workflow insert shape.
///
/// Ordinary object lifecycle writes are owned by `hubuum-storage-postgres`.
/// This remains local only until the import workflow moves into that adapter.
#[derive(Insertable)]
#[diesel(table_name = crate::schema::hubuumobject)]
pub(crate) struct NewHubuumObjectRow<'value> {
    name: &'value str,
    collection_id: i32,
    hubuum_class_id: i32,
    data: &'value serde_json::Value,
    description: &'value str,
}

impl<'value> From<&'value NewHubuumObject> for NewHubuumObjectRow<'value> {
    fn from(object: &'value NewHubuumObject) -> Self {
        Self {
            name: &object.name,
            collection_id: object.collection_id,
            hubuum_class_id: object.hubuum_class_id,
            data: &object.data,
            description: &object.description,
        }
    }
}

/// Legacy import-workflow update shape.
#[derive(AsChangeset)]
#[diesel(table_name = crate::schema::hubuumobject)]
pub(crate) struct UpdateHubuumObjectRow<'value> {
    name: Option<&'value str>,
    collection_id: Option<i32>,
    hubuum_class_id: Option<i32>,
    data: Option<&'value serde_json::Value>,
    description: Option<&'value str>,
}

impl<'value> From<&'value UpdateHubuumObject> for UpdateHubuumObjectRow<'value> {
    fn from(update: &'value UpdateHubuumObject) -> Self {
        Self {
            name: update.name.as_deref(),
            collection_id: update.collection_id,
            hubuum_class_id: update.hubuum_class_id,
            data: update.data.as_ref(),
            description: update.description.as_deref(),
        }
    }
}

impl CursorPaginated for HubuumObjectRow {
    fn supports_sort(field: &crate::models::search::FilterField) -> bool {
        HubuumObject::supports_sort(field)
    }

    fn cursor_value(
        &self,
        field: &crate::models::search::FilterField,
    ) -> Result<CursorValue, ApiError> {
        Ok(match field {
            crate::models::search::FilterField::Id => CursorValue::Integer(self.id.into()),
            crate::models::search::FilterField::Name => CursorValue::String(self.name.clone()),
            crate::models::search::FilterField::Description => {
                CursorValue::String(self.description.clone())
            }
            crate::models::search::FilterField::Collections
            | crate::models::search::FilterField::CollectionId => {
                CursorValue::Integer(self.collection_id.into())
            }
            crate::models::search::FilterField::ClassId
            | crate::models::search::FilterField::Classes => {
                CursorValue::Integer(self.hubuum_class_id.into())
            }
            crate::models::search::FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            crate::models::search::FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            crate::models::search::FilterField::Revision => {
                CursorValue::Integer(self.revision.get())
            }
            other => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{other}' is not orderable for objects"
                )));
            }
        })
    }

    fn default_sort() -> Vec<crate::models::search::SortParam> {
        HubuumObject::default_sort()
    }

    fn tie_breaker_sort() -> Vec<crate::models::search::SortParam> {
        HubuumObject::tie_breaker_sort()
    }
}

impl CursorSqlMapping for HubuumObjectRow {
    fn sql_field(field: &crate::models::search::FilterField) -> Result<CursorSqlField, ApiError> {
        use crate::models::search::FilterField;

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
            FilterField::Revision => CursorSqlField {
                column: "hubuumobject.revision",
                sql_type: CursorSqlType::BigInt,
                nullable: false,
            },
            other => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{other}' is not orderable for objects"
                )));
            }
        })
    }
}

impl GetObject<(HubuumObject, HubuumObject)> for HubuumObjectRelationID {
    async fn object_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(HubuumObject, HubuumObject), ApiError> {
        use crate::schema::hubuumobject::dsl as obj;
        use crate::schema::hubuumobject_relation::dsl as obj_rel;
        use crate::storage::postgres::prelude::*;

        let objects = with_connection(pool, async |conn| {
            diesel_async::RunQueryDsl::load::<HubuumObjectRow>(
                obj_rel::hubuumobject_relation
                    .filter(obj_rel::id.eq(self.id()))
                    .inner_join(
                        obj::hubuumobject.on(obj::id
                            .eq(obj_rel::from_hubuum_object_id)
                            .or(obj::id.eq(obj_rel::to_hubuum_object_id))),
                    )
                    .select(obj::hubuumobject::all_columns()),
                conn,
            )
            .await
        })
        .await?;

        if objects.len() != 2 {
            return Err(ApiError::NotFound(
                "Could not find two objects for object relation".to_string(),
            ));
        }

        Ok((objects[0].clone().into(), objects[1].clone().into()))
    }
}

impl GetObject<(HubuumObject, HubuumObject)> for NewHubuumObjectRelation {
    async fn object_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(HubuumObject, HubuumObject), ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};
        let objects = with_connection(pool, async |conn| {
            hubuumobject
                .filter(id.eq_any(vec![self.from_hubuum_object_id, self.to_hubuum_object_id]))
                .load::<HubuumObjectRow>(conn)
                .await
        })
        .await?;

        if objects.len() != 2 {
            return Err(ApiError::NotFound(
                format!(
                    "Could not find objects ({}, {}) for object relation",
                    self.from_hubuum_object_id, self.to_hubuum_object_id,
                )
                .to_string(),
            ));
        }
        Ok((objects[0].clone().into(), objects[1].clone().into()))
    }
}

impl GetObject<(HubuumObject, HubuumObject)> for HubuumObjectRelation {
    async fn object_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(HubuumObject, HubuumObject), ApiError> {
        use crate::schema::hubuumobject::dsl as obj;
        use crate::schema::hubuumobject_relation::dsl as obj_rel;
        use crate::storage::postgres::prelude::*;

        let objects = with_connection(pool, async |conn| {
            diesel_async::RunQueryDsl::load::<HubuumObjectRow>(
                obj_rel::hubuumobject_relation
                    .filter(obj_rel::id.eq(self.id))
                    .inner_join(
                        obj::hubuumobject.on(obj::id
                            .eq(obj_rel::from_hubuum_object_id)
                            .or(obj::id.eq(obj_rel::to_hubuum_object_id))),
                    )
                    .select(obj::hubuumobject::all_columns()),
                conn,
            )
            .await
        })
        .await?;

        if objects.len() != 2 {
            return Err(ApiError::NotFound(
                "Could not find two objects for object relation".to_string(),
            ));
        }

        Ok((objects[0].clone().into(), objects[1].clone().into()))
    }
}

pub trait LoadObjectRecord {
    async fn load_object_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumObject, ApiError>;
}

impl LoadObjectRecord for HubuumObject {
    async fn load_object_record(
        &self,
        _pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumObject, ApiError> {
        Ok(self.clone())
    }
}

impl LoadObjectRecord for HubuumObjectID {
    async fn load_object_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumObject, ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};

        with_connection(pool, async |conn| {
            hubuumobject
                .filter(id.eq(self.id()))
                .first::<HubuumObjectRow>(conn)
                .await
        })
        .await
        .map(Into::into)
    }
}
