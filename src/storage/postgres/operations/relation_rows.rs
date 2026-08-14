use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};
use crate::models::{
    HubuumClassRelation, HubuumClassRelationTransitive, HubuumObjectRelation,
    HubuumObjectTransitiveLink, NewHubuumClassRelation, NewHubuumObjectRelation,
    ObjectRelationLimit,
};
use crate::pagination::{CursorSqlField, CursorSqlMapping, CursorSqlType};
use crate::storage::postgres::prelude::*;
use crate::traits::{CursorPaginated, CursorValue};

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
    pub(crate) revision: PostgresRevision,
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
            revision: row.revision.into_domain(),
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
    pub(crate) revision: PostgresRevision,
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
            revision: row.revision.into_domain(),
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
