use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};
use crate::models::{
    ClassGraphRow, HubuumClassRelation, HubuumClassRelationTransitive, HubuumObjectRelation,
    HubuumObjectTransitiveLink, NewHubuumClassRelation, NewHubuumObjectRelation,
    ObjectRelationLimit, RelatedObjectForRootRow, RelatedObjectGraphRow, RelatedObjectIncludeRow,
};
use crate::storage::postgres::prelude::*;
use crate::storage::{
    StorageClassGraphRow, StorageClassRelation, StorageGraphClass, StorageGraphObject,
    StorageGraphResource, StorageObjectGraphRow, StorageObjectRelation, StorageRecordMetadata,
    StorageRelatedObjectForRootRow, StorageRelatedObjectIncludeRow,
};
use crate::traits::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};

#[derive(Clone, Queryable, QueryableByName, Selectable)]
#[diesel(table_name = crate::schema::hubuumclass_relation)]
pub(crate) struct HubuumClassRelationRow {
    pub(crate) id: i32,
    pub(crate) from_hubuum_class_id: i32,
    pub(crate) to_hubuum_class_id: i32,
    pub(crate) forward_template_alias: Option<String>,
    pub(crate) reverse_template_alias: Option<String>,
    pub(crate) created_at: chrono::NaiveDateTime,
    pub(crate) updated_at: chrono::NaiveDateTime,
    pub(crate) from_max_relations: Option<i32>,
    pub(crate) to_max_relations: Option<i32>,
    pub(crate) revision: crate::models::ResourceRevision,
}

fn persisted_relation_limit(value: Option<i32>) -> Result<Option<ObjectRelationLimit>, ApiError> {
    value
        .map(|value| {
            ObjectRelationLimit::new(value).map_err(|_| {
                ApiError::InternalServerError(format!(
                    "persisted object relation limit '{value}' is not positive"
                ))
            })
        })
        .transpose()
}

impl TryFrom<HubuumClassRelationRow> for HubuumClassRelation {
    type Error = ApiError;

    fn try_from(row: HubuumClassRelationRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            from_hubuum_class_id: row.from_hubuum_class_id,
            to_hubuum_class_id: row.to_hubuum_class_id,
            forward_template_alias: row.forward_template_alias,
            reverse_template_alias: row.reverse_template_alias,
            created_at: row.created_at,
            updated_at: row.updated_at,
            from_max_relations: persisted_relation_limit(row.from_max_relations)?,
            to_max_relations: persisted_relation_limit(row.to_max_relations)?,
            revision: row.revision,
        })
    }
}

impl CursorPaginated for HubuumClassRelationRow {
    fn supports_sort(field: &FilterField) -> bool {
        <HubuumClassRelation as CursorPaginated>::supports_sort(field)
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
        <HubuumClassRelation as CursorPaginated>::default_sort()
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        <HubuumClassRelation as CursorPaginated>::tie_breaker_sort()
    }
}

impl CursorSqlMapping for HubuumClassRelationRow {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "hubuumclass_relation.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ClassFrom => CursorSqlField {
                column: "hubuumclass_relation.from_hubuum_class_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ClassTo => CursorSqlField {
                column: "hubuumclass_relation.to_hubuum_class_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "hubuumclass_relation.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "hubuumclass_relation.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::Revision => CursorSqlField {
                column: "hubuumclass_relation.revision",
                sql_type: CursorSqlType::BigInt,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for class relations",
                    field
                )));
            }
        })
    }
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::hubuumclass_relation)]
pub(crate) struct NewHubuumClassRelationRow<'a> {
    from_hubuum_class_id: i32,
    to_hubuum_class_id: i32,
    forward_template_alias: Option<&'a str>,
    reverse_template_alias: Option<&'a str>,
    from_max_relations: Option<i32>,
    to_max_relations: Option<i32>,
}

impl<'a> From<&'a NewHubuumClassRelation> for NewHubuumClassRelationRow<'a> {
    fn from(relation: &'a NewHubuumClassRelation) -> Self {
        Self {
            from_hubuum_class_id: relation.from_hubuum_class_id,
            to_hubuum_class_id: relation.to_hubuum_class_id,
            forward_template_alias: relation.forward_template_alias.as_deref(),
            reverse_template_alias: relation.reverse_template_alias.as_deref(),
            from_max_relations: relation.from_max_relations.map(ObjectRelationLimit::value),
            to_max_relations: relation.to_max_relations.map(ObjectRelationLimit::value),
        }
    }
}

#[derive(Clone, Copy, Queryable, QueryableByName, Selectable)]
#[diesel(table_name = crate::schema::hubuumobject_relation)]
pub(crate) struct HubuumObjectRelationRow {
    pub(crate) id: i32,
    pub(crate) from_hubuum_object_id: i32,
    pub(crate) to_hubuum_object_id: i32,
    pub(crate) class_relation_id: i32,
    pub(crate) created_at: chrono::NaiveDateTime,
    pub(crate) updated_at: chrono::NaiveDateTime,
    pub(crate) revision: crate::models::ResourceRevision,
}

impl From<HubuumObjectRelationRow> for HubuumObjectRelation {
    fn from(row: HubuumObjectRelationRow) -> Self {
        Self {
            id: row.id,
            from_hubuum_object_id: row.from_hubuum_object_id,
            to_hubuum_object_id: row.to_hubuum_object_id,
            class_relation_id: row.class_relation_id,
            created_at: row.created_at,
            updated_at: row.updated_at,
            revision: row.revision,
        }
    }
}

impl CursorPaginated for HubuumObjectRelationRow {
    fn supports_sort(field: &FilterField) -> bool {
        <HubuumObjectRelation as CursorPaginated>::supports_sort(field)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        HubuumObjectRelation::from(*self).cursor_value(field)
    }

    fn default_sort() -> Vec<SortParam> {
        <HubuumObjectRelation as CursorPaginated>::default_sort()
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        <HubuumObjectRelation as CursorPaginated>::tie_breaker_sort()
    }
}

impl CursorSqlMapping for HubuumObjectRelationRow {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "hubuumobject_relation.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ClassRelation => CursorSqlField {
                column: "hubuumobject_relation.class_relation_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ObjectFrom => CursorSqlField {
                column: "hubuumobject_relation.from_hubuum_object_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ObjectTo => CursorSqlField {
                column: "hubuumobject_relation.to_hubuum_object_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "hubuumobject_relation.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "hubuumobject_relation.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::Revision => CursorSqlField {
                column: "hubuumobject_relation.revision",
                sql_type: CursorSqlType::BigInt,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for object relations",
                    field
                )));
            }
        })
    }
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::hubuumobject_relation)]
pub(crate) struct NewHubuumObjectRelationRow {
    from_hubuum_object_id: i32,
    to_hubuum_object_id: i32,
    class_relation_id: i32,
}

impl From<&NewHubuumObjectRelation> for NewHubuumObjectRelationRow {
    fn from(relation: &NewHubuumObjectRelation) -> Self {
        Self {
            from_hubuum_object_id: relation.from_hubuum_object_id,
            to_hubuum_object_id: relation.to_hubuum_object_id,
            class_relation_id: relation.class_relation_id,
        }
    }
}

#[derive(Clone, QueryableByName)]
pub(crate) struct HubuumClassRelationTransitiveRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub(crate) ancestor_class_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub(crate) descendant_class_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub(crate) depth: i32,
    #[diesel(sql_type = diesel::sql_types::Array<diesel::sql_types::Nullable<diesel::sql_types::Integer>>)]
    pub(crate) path: Vec<Option<i32>>,
}

impl From<HubuumClassRelationTransitiveRow> for HubuumClassRelationTransitive {
    fn from(row: HubuumClassRelationTransitiveRow) -> Self {
        Self {
            ancestor_class_id: row.ancestor_class_id,
            descendant_class_id: row.descendant_class_id,
            depth: row.depth,
            path: row.path,
        }
    }
}

impl CursorPaginated for HubuumClassRelationTransitiveRow {
    fn supports_sort(field: &FilterField) -> bool {
        <HubuumClassRelationTransitive as CursorPaginated>::supports_sort(field)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        HubuumClassRelationTransitive::from(self.clone()).cursor_value(field)
    }

    fn default_sort() -> Vec<SortParam> {
        <HubuumClassRelationTransitive as CursorPaginated>::default_sort()
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        <HubuumClassRelationTransitive as CursorPaginated>::tie_breaker_sort()
    }
}

impl CursorSqlMapping for HubuumClassRelationTransitiveRow {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::ClassFrom => CursorSqlField {
                column: "ancestor_class_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ClassTo => CursorSqlField {
                column: "descendant_class_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Depth => CursorSqlField {
                column: "depth",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Path => CursorSqlField {
                column: "path",
                sql_type: CursorSqlType::IntegerArray,
                nullable: true,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for transitive class relations",
                    field
                )));
            }
        })
    }
}

#[derive(QueryableByName)]
pub(crate) struct HubuumObjectTransitiveLinkRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    target_object_id: i32,
    #[diesel(sql_type = diesel::sql_types::Array<diesel::sql_types::Integer>)]
    path: Vec<i32>,
}

impl From<HubuumObjectTransitiveLinkRow> for HubuumObjectTransitiveLink {
    fn from(row: HubuumObjectTransitiveLinkRow) -> Self {
        Self::new(row.target_object_id, row.path)
    }
}

#[derive(Clone, Queryable, QueryableByName)]
pub(crate) struct ClassGraphQueryRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub(crate) ancestor_class_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub(crate) descendant_class_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub(crate) depth: i32,
    #[diesel(sql_type = diesel::sql_types::Array<diesel::sql_types::Integer>)]
    pub(crate) path: Vec<i32>,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub(crate) ancestor_name: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub(crate) descendant_name: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub(crate) ancestor_collection_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub(crate) descendant_collection_id: i32,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Jsonb>)]
    pub(crate) ancestor_json_schema: Option<serde_json::Value>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Jsonb>)]
    pub(crate) descendant_json_schema: Option<serde_json::Value>,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    pub(crate) ancestor_validate_schema: bool,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    pub(crate) descendant_validate_schema: bool,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub(crate) ancestor_description: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub(crate) descendant_description: String,
    #[diesel(sql_type = diesel::sql_types::Timestamp)]
    pub(crate) ancestor_created_at: chrono::NaiveDateTime,
    #[diesel(sql_type = diesel::sql_types::Timestamp)]
    pub(crate) descendant_created_at: chrono::NaiveDateTime,
    #[diesel(sql_type = diesel::sql_types::Timestamp)]
    pub(crate) ancestor_updated_at: chrono::NaiveDateTime,
    #[diesel(sql_type = diesel::sql_types::Timestamp)]
    pub(crate) descendant_updated_at: chrono::NaiveDateTime,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub(crate) ancestor_revision: crate::models::ResourceRevision,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub(crate) descendant_revision: crate::models::ResourceRevision,
}

impl From<ClassGraphQueryRow> for ClassGraphRow {
    fn from(row: ClassGraphQueryRow) -> Self {
        Self {
            ancestor_class_id: row.ancestor_class_id,
            descendant_class_id: row.descendant_class_id,
            depth: row.depth,
            path: row.path,
            ancestor_name: row.ancestor_name,
            descendant_name: row.descendant_name,
            ancestor_collection_id: row.ancestor_collection_id,
            descendant_collection_id: row.descendant_collection_id,
            ancestor_json_schema: row.ancestor_json_schema,
            descendant_json_schema: row.descendant_json_schema,
            ancestor_validate_schema: row.ancestor_validate_schema,
            descendant_validate_schema: row.descendant_validate_schema,
            ancestor_description: row.ancestor_description,
            descendant_description: row.descendant_description,
            ancestor_created_at: row.ancestor_created_at,
            descendant_created_at: row.descendant_created_at,
            ancestor_updated_at: row.ancestor_updated_at,
            descendant_updated_at: row.descendant_updated_at,
            ancestor_revision: row.ancestor_revision,
            descendant_revision: row.descendant_revision,
        }
    }
}

impl CursorPaginated for ClassGraphQueryRow {
    fn supports_sort(field: &FilterField) -> bool {
        <ClassGraphRow as CursorPaginated>::supports_sort(field)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        ClassGraphRow::from(self.clone()).cursor_value(field)
    }

    fn default_sort() -> Vec<SortParam> {
        <ClassGraphRow as CursorPaginated>::default_sort()
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        <ClassGraphRow as CursorPaginated>::tie_breaker_sort()
    }
}

impl CursorSqlMapping for ClassGraphQueryRow {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id
            | FilterField::ClassTo
            | FilterField::ClassId
            | FilterField::Classes => CursorSqlField {
                column: "related_classes.descendant_class_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ClassFrom => CursorSqlField {
                column: "related_classes.ancestor_class_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name | FilterField::NameTo => CursorSqlField {
                column: "related_classes.descendant_name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::NameFrom => CursorSqlField {
                column: "related_classes.ancestor_name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Description | FilterField::DescriptionTo => CursorSqlField {
                column: "related_classes.descendant_description",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::DescriptionFrom => CursorSqlField {
                column: "related_classes.ancestor_description",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Collections | FilterField::CollectionId | FilterField::CollectionsTo => {
                CursorSqlField {
                    column: "related_classes.descendant_collection_id",
                    sql_type: CursorSqlType::Integer,
                    nullable: false,
                }
            }
            FilterField::CollectionsFrom => CursorSqlField {
                column: "related_classes.ancestor_collection_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::CreatedAt | FilterField::CreatedAtTo => CursorSqlField {
                column: "related_classes.descendant_created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::CreatedAtFrom => CursorSqlField {
                column: "related_classes.ancestor_created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt | FilterField::UpdatedAtTo => CursorSqlField {
                column: "related_classes.descendant_updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAtFrom => CursorSqlField {
                column: "related_classes.ancestor_updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::Depth => CursorSqlField {
                column: "related_classes.depth",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Path => CursorSqlField {
                column: "related_classes.path",
                sql_type: CursorSqlType::IntegerArray,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for related classes",
                    field
                )));
            }
        })
    }
}

#[derive(Clone, QueryableByName)]
pub(crate) struct RelatedObjectGraphQueryRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub(crate) ancestor_object_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub(crate) descendant_object_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub(crate) depth: i32,
    #[diesel(sql_type = diesel::sql_types::Array<diesel::sql_types::Integer>)]
    pub(crate) path: Vec<i32>,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub(crate) ancestor_name: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub(crate) descendant_name: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub(crate) ancestor_collection_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub(crate) descendant_collection_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub(crate) ancestor_class_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub(crate) descendant_class_id: i32,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub(crate) ancestor_description: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub(crate) descendant_description: String,
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    pub(crate) ancestor_data: serde_json::Value,
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    pub(crate) descendant_data: serde_json::Value,
    #[diesel(sql_type = diesel::sql_types::Timestamp)]
    pub(crate) ancestor_created_at: chrono::NaiveDateTime,
    #[diesel(sql_type = diesel::sql_types::Timestamp)]
    pub(crate) descendant_created_at: chrono::NaiveDateTime,
    #[diesel(sql_type = diesel::sql_types::Timestamp)]
    pub(crate) ancestor_updated_at: chrono::NaiveDateTime,
    #[diesel(sql_type = diesel::sql_types::Timestamp)]
    pub(crate) descendant_updated_at: chrono::NaiveDateTime,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub(crate) ancestor_revision: crate::models::ResourceRevision,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub(crate) descendant_revision: crate::models::ResourceRevision,
}

impl From<RelatedObjectGraphQueryRow> for RelatedObjectGraphRow {
    fn from(row: RelatedObjectGraphQueryRow) -> Self {
        Self {
            ancestor_object_id: row.ancestor_object_id,
            descendant_object_id: row.descendant_object_id,
            depth: row.depth,
            path: row.path,
            ancestor_name: row.ancestor_name,
            descendant_name: row.descendant_name,
            ancestor_collection_id: row.ancestor_collection_id,
            descendant_collection_id: row.descendant_collection_id,
            ancestor_class_id: row.ancestor_class_id,
            descendant_class_id: row.descendant_class_id,
            ancestor_description: row.ancestor_description,
            descendant_description: row.descendant_description,
            ancestor_data: row.ancestor_data,
            descendant_data: row.descendant_data,
            ancestor_created_at: row.ancestor_created_at,
            descendant_created_at: row.descendant_created_at,
            ancestor_updated_at: row.ancestor_updated_at,
            descendant_updated_at: row.descendant_updated_at,
            ancestor_revision: row.ancestor_revision,
            descendant_revision: row.descendant_revision,
        }
    }
}

impl CursorPaginated for RelatedObjectGraphQueryRow {
    fn supports_sort(field: &FilterField) -> bool {
        <RelatedObjectGraphRow as CursorPaginated>::supports_sort(field)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        RelatedObjectGraphRow::from(self.clone()).cursor_value(field)
    }

    fn default_sort() -> Vec<SortParam> {
        <RelatedObjectGraphRow as CursorPaginated>::default_sort()
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        <RelatedObjectGraphRow as CursorPaginated>::tie_breaker_sort()
    }
}

impl CursorSqlMapping for RelatedObjectGraphQueryRow {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id | FilterField::ObjectTo => CursorSqlField {
                column: "related_objects.descendant_object_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ObjectFrom => CursorSqlField {
                column: "related_objects.ancestor_object_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name | FilterField::NameTo => CursorSqlField {
                column: "related_objects.descendant_name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::NameFrom => CursorSqlField {
                column: "related_objects.ancestor_name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Description | FilterField::DescriptionTo => CursorSqlField {
                column: "related_objects.descendant_description",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::DescriptionFrom => CursorSqlField {
                column: "related_objects.ancestor_description",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Collections | FilterField::CollectionId | FilterField::CollectionsTo => {
                CursorSqlField {
                    column: "related_objects.descendant_collection_id",
                    sql_type: CursorSqlType::Integer,
                    nullable: false,
                }
            }
            FilterField::CollectionsFrom => CursorSqlField {
                column: "related_objects.ancestor_collection_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ClassId | FilterField::Classes | FilterField::ClassTo => CursorSqlField {
                column: "related_objects.descendant_class_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ClassFrom => CursorSqlField {
                column: "related_objects.ancestor_class_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::CreatedAt | FilterField::CreatedAtTo => CursorSqlField {
                column: "related_objects.descendant_created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::CreatedAtFrom => CursorSqlField {
                column: "related_objects.ancestor_created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt | FilterField::UpdatedAtTo => CursorSqlField {
                column: "related_objects.descendant_updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAtFrom => CursorSqlField {
                column: "related_objects.ancestor_updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::Depth => CursorSqlField {
                column: "related_objects.depth",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Path => CursorSqlField {
                column: "related_objects.path",
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

#[derive(QueryableByName)]
pub(crate) struct RelatedObjectIncludeQueryRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    root_object_id: i32,
    #[diesel(embed)]
    graph: RelatedObjectGraphQueryRow,
}

impl From<RelatedObjectIncludeQueryRow> for RelatedObjectIncludeRow {
    fn from(row: RelatedObjectIncludeQueryRow) -> Self {
        let root_object_id = row.root_object_id;
        let graph = RelatedObjectGraphRow::from(row.graph);
        Self {
            root_object_id,
            ancestor_object_id: graph.ancestor_object_id,
            descendant_object_id: graph.descendant_object_id,
            depth: graph.depth,
            path: graph.path,
            ancestor_name: graph.ancestor_name,
            descendant_name: graph.descendant_name,
            ancestor_collection_id: graph.ancestor_collection_id,
            descendant_collection_id: graph.descendant_collection_id,
            ancestor_class_id: graph.ancestor_class_id,
            descendant_class_id: graph.descendant_class_id,
            ancestor_description: graph.ancestor_description,
            descendant_description: graph.descendant_description,
            ancestor_data: graph.ancestor_data,
            descendant_data: graph.descendant_data,
            ancestor_created_at: graph.ancestor_created_at,
            descendant_created_at: graph.descendant_created_at,
            ancestor_updated_at: graph.ancestor_updated_at,
            descendant_updated_at: graph.descendant_updated_at,
            ancestor_revision: graph.ancestor_revision,
            descendant_revision: graph.descendant_revision,
        }
    }
}

#[derive(QueryableByName)]
pub(crate) struct RelatedObjectForRootQueryRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub(crate) root_object_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub(crate) descendant_object_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub(crate) depth: i32,
    #[diesel(sql_type = diesel::sql_types::Array<diesel::sql_types::Integer>)]
    pub(crate) path: Vec<i32>,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub(crate) descendant_name: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub(crate) descendant_collection_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub(crate) descendant_class_id: i32,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub(crate) descendant_description: String,
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    pub(crate) descendant_data: serde_json::Value,
    #[diesel(sql_type = diesel::sql_types::Timestamp)]
    pub(crate) descendant_created_at: chrono::NaiveDateTime,
    #[diesel(sql_type = diesel::sql_types::Timestamp)]
    pub(crate) descendant_updated_at: chrono::NaiveDateTime,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub(crate) descendant_revision: crate::models::ResourceRevision,
}

impl From<RelatedObjectForRootQueryRow> for RelatedObjectForRootRow {
    fn from(row: RelatedObjectForRootQueryRow) -> Self {
        Self {
            root_object_id: row.root_object_id,
            descendant_object_id: row.descendant_object_id,
            depth: row.depth,
            path: row.path,
            descendant_name: row.descendant_name,
            descendant_collection_id: row.descendant_collection_id,
            descendant_class_id: row.descendant_class_id,
            descendant_description: row.descendant_description,
            descendant_data: row.descendant_data,
            descendant_created_at: row.descendant_created_at,
            descendant_updated_at: row.descendant_updated_at,
            descendant_revision: row.descendant_revision,
        }
    }
}

fn metadata(
    id: i32,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
    revision: i64,
) -> StorageRecordMetadata {
    StorageRecordMetadata::new(id, created_at, updated_at, revision)
}

pub(super) fn class_relation_to_storage(row: HubuumClassRelation) -> StorageClassRelation {
    StorageClassRelation::new(
        metadata(row.id, row.created_at, row.updated_at, row.revision.get()),
        row.from_hubuum_class_id,
        row.to_hubuum_class_id,
    )
    .with_template_aliases(row.forward_template_alias, row.reverse_template_alias)
    .with_relation_limits(
        row.from_max_relations.map(|limit| limit.value()),
        row.to_max_relations.map(|limit| limit.value()),
    )
}

pub(super) fn object_relation_to_storage(row: HubuumObjectRelation) -> StorageObjectRelation {
    StorageObjectRelation::new(
        metadata(row.id, row.created_at, row.updated_at, row.revision.get()),
        row.from_hubuum_object_id,
        row.to_hubuum_object_id,
        row.class_relation_id,
    )
}

fn graph_resource(
    id: i32,
    name: String,
    collection_id: i32,
    description: String,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
    revision: i64,
) -> StorageGraphResource {
    StorageGraphResource::new(
        metadata(id, created_at, updated_at, revision),
        name,
        collection_id,
        description,
    )
}

pub(super) fn class_graph_to_storage(row: ClassGraphRow) -> StorageClassGraphRow {
    let ancestor = StorageGraphClass::new(
        graph_resource(
            row.ancestor_class_id,
            row.ancestor_name,
            row.ancestor_collection_id,
            row.ancestor_description,
            row.ancestor_created_at,
            row.ancestor_updated_at,
            row.ancestor_revision.get(),
        ),
        row.ancestor_json_schema,
        row.ancestor_validate_schema,
    );
    let descendant = StorageGraphClass::new(
        graph_resource(
            row.descendant_class_id,
            row.descendant_name,
            row.descendant_collection_id,
            row.descendant_description,
            row.descendant_created_at,
            row.descendant_updated_at,
            row.descendant_revision.get(),
        ),
        row.descendant_json_schema,
        row.descendant_validate_schema,
    );
    StorageClassGraphRow::new(ancestor, descendant, row.depth, row.path)
}

fn graph_object(
    resource: StorageGraphResource,
    class_id: i32,
    data: serde_json::Value,
) -> StorageGraphObject {
    StorageGraphObject::new(resource, class_id, data)
}

pub(super) fn object_graph_to_storage(row: RelatedObjectGraphRow) -> StorageObjectGraphRow {
    let ancestor = graph_object(
        graph_resource(
            row.ancestor_object_id,
            row.ancestor_name,
            row.ancestor_collection_id,
            row.ancestor_description,
            row.ancestor_created_at,
            row.ancestor_updated_at,
            row.ancestor_revision.get(),
        ),
        row.ancestor_class_id,
        row.ancestor_data,
    );
    let descendant = graph_object(
        graph_resource(
            row.descendant_object_id,
            row.descendant_name,
            row.descendant_collection_id,
            row.descendant_description,
            row.descendant_created_at,
            row.descendant_updated_at,
            row.descendant_revision.get(),
        ),
        row.descendant_class_id,
        row.descendant_data,
    );
    StorageObjectGraphRow::new(ancestor, descendant, row.depth, row.path)
}

pub(super) fn related_include_to_storage(
    row: RelatedObjectIncludeRow,
) -> StorageRelatedObjectIncludeRow {
    let root_object_id = row.root_object_id;
    StorageRelatedObjectIncludeRow::new(
        root_object_id,
        object_graph_to_storage(RelatedObjectGraphRow {
            ancestor_object_id: row.ancestor_object_id,
            descendant_object_id: row.descendant_object_id,
            depth: row.depth,
            path: row.path,
            ancestor_name: row.ancestor_name,
            descendant_name: row.descendant_name,
            ancestor_collection_id: row.ancestor_collection_id,
            descendant_collection_id: row.descendant_collection_id,
            ancestor_class_id: row.ancestor_class_id,
            descendant_class_id: row.descendant_class_id,
            ancestor_description: row.ancestor_description,
            descendant_description: row.descendant_description,
            ancestor_data: row.ancestor_data,
            descendant_data: row.descendant_data,
            ancestor_created_at: row.ancestor_created_at,
            descendant_created_at: row.descendant_created_at,
            ancestor_updated_at: row.ancestor_updated_at,
            descendant_updated_at: row.descendant_updated_at,
            ancestor_revision: row.ancestor_revision,
            descendant_revision: row.descendant_revision,
        }),
    )
}

pub(super) fn related_for_root_to_storage(
    row: RelatedObjectForRootRow,
) -> StorageRelatedObjectForRootRow {
    let descendant = graph_object(
        graph_resource(
            row.descendant_object_id,
            row.descendant_name,
            row.descendant_collection_id,
            row.descendant_description,
            row.descendant_created_at,
            row.descendant_updated_at,
            row.descendant_revision.get(),
        ),
        row.descendant_class_id,
        row.descendant_data,
    );
    StorageRelatedObjectForRootRow::new(row.root_object_id, descendant, row.depth, row.path)
}
