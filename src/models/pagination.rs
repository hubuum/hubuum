use std::collections::HashMap;

use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::errors::ApiError;
use crate::models::output::GroupPermission;
use crate::models::search::{FilterField, QueryOptions, SortParam};
use crate::models::{
    Group, HubuumClassExpanded, HubuumClassRelation, HubuumClassRelationTransitive, HubuumObject,
    HubuumObjectRelation, HubuumObjectWithPath, Namespace, ObjectClosureView, User, UserToken,
};

pub const DEFAULT_PAGE_LIMIT: usize = 100;
pub const MAX_PAGE_LIMIT: usize = 250;
pub const NEXT_CURSOR_HEADER: &str = "X-Next-Cursor";

#[derive(Debug, Clone, PartialEq)]
pub struct CursorPageRequest {
    pub limit: usize,
    pub sorts: Vec<SortParam>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CursorToken {
    sorts: Vec<CursorSort>,
    values: Vec<CursorValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CursorSort {
    field: String,
    descending: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum CursorValue {
    Null,
    Boolean(bool),
    Integer(i64),
    String(String),
    DateTime(chrono::NaiveDateTime),
    IntegerArray(Vec<i32>),
}

pub trait CursorPaginated: Clone {
    fn supports_sort(field: &FilterField) -> bool;
    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError>;
    fn default_sort() -> Vec<SortParam>;
    fn tie_breaker_sort() -> Vec<SortParam>;
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorSqlType {
    Boolean,
    Integer,
    String,
    DateTime,
    IntegerArray,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CursorSqlField {
    pub column: &'static str,
    pub sql_type: CursorSqlType,
    pub nullable: bool,
}

pub trait CursorSqlMapping: CursorPaginated {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError>;
}

pub fn validate_page_limit(limit: usize) -> Result<usize, ApiError> {
    if limit == 0 {
        return Err(ApiError::BadRequest(
            "limit must be greater than 0".to_string(),
        ));
    }

    if limit > MAX_PAGE_LIMIT {
        return Err(ApiError::BadRequest(format!(
            "limit must be at most {MAX_PAGE_LIMIT}"
        )));
    }

    Ok(limit)
}

pub fn prepare_db_pagination<T>(query_options: &QueryOptions) -> Result<QueryOptions, ApiError>
where
    T: CursorPaginated,
{
    let limit = validate_page_limit(query_options.limit.unwrap_or(DEFAULT_PAGE_LIMIT))?;
    let sorts = normalized_sorts::<T>(&query_options.sort)?;

    if let Some(cursor) = &query_options.cursor {
        let _ = decode_cursor_values(cursor, &sorts)?;
    }

    let mut prepared = query_options.clone();
    prepared.sort = sorts;
    prepared.limit = Some(limit + 1);
    Ok(prepared)
}

pub fn finalize_page<T>(
    mut items: Vec<T>,
    query_options: &QueryOptions,
) -> Result<Page<T>, ApiError>
where
    T: CursorPaginated,
{
    let request = page_request::<T>(query_options)?;
    let has_more = items.len() > request.limit;
    if has_more {
        items.truncate(request.limit);
    }

    let next_cursor = if has_more {
        items
            .last()
            .map(|item| encode_cursor(item, &request.sorts))
            .transpose()?
    } else {
        None
    };

    Ok(Page { items, next_cursor })
}

pub fn next_cursor_header(next_cursor: &Option<String>) -> Option<HashMap<String, String>> {
    next_cursor.as_ref().map(|cursor| {
        let mut headers = HashMap::new();
        headers.insert(NEXT_CURSOR_HEADER.to_string(), cursor.clone());
        headers
    })
}

pub fn page_request<T>(query_options: &QueryOptions) -> Result<CursorPageRequest, ApiError>
where
    T: CursorPaginated,
{
    Ok(CursorPageRequest {
        limit: validate_page_limit(query_options.limit.unwrap_or(DEFAULT_PAGE_LIMIT))?,
        sorts: normalized_sorts::<T>(&query_options.sort)?,
    })
}

pub fn cursor_sql_field<T>(field: &FilterField) -> Result<CursorSqlField, ApiError>
where
    T: CursorSqlMapping,
{
    T::sql_field(field)
}

pub fn order_sql_clause<T>(sort: &SortParam) -> Result<String, ApiError>
where
    T: CursorSqlMapping,
{
    let field = cursor_sql_field::<T>(&sort.field)?;
    let direction = if sort.descending { "DESC" } else { "ASC" };
    let nulls = if field.nullable {
        if sort.descending {
            " NULLS LAST"
        } else {
            " NULLS FIRST"
        }
    } else {
        ""
    };

    Ok(format!("{} {}{}", field.column, direction, nulls))
}

pub fn cursor_filter_sql<T>(
    sorts: &[SortParam],
    cursor: Option<&str>,
) -> Result<Option<String>, ApiError>
where
    T: CursorSqlMapping,
{
    let Some(cursor) = cursor else {
        return Ok(None);
    };

    let cursor_values = decode_cursor_values(cursor, sorts)?;
    let fields = sorts
        .iter()
        .map(|sort| cursor_sql_field::<T>(&sort.field))
        .collect::<Result<Vec<_>, _>>()?;

    let mut clauses = Vec::with_capacity(sorts.len());
    for current_index in 0..sorts.len() {
        let mut clause_parts = Vec::with_capacity(current_index + 1);
        for prefix_index in 0..current_index {
            clause_parts.push(cursor_equality_sql(
                &fields[prefix_index],
                &cursor_values[prefix_index],
            )?);
        }

        clause_parts.push(cursor_after_sql(
            &fields[current_index],
            &sorts[current_index],
            &cursor_values[current_index],
        )?);

        clauses.push(format!("({})", clause_parts.join(" AND ")));
    }

    Ok(Some(format!("({})", clauses.join(" OR "))))
}

pub fn normalized_sorts<T>(requested: &[SortParam]) -> Result<Vec<SortParam>, ApiError>
where
    T: CursorPaginated,
{
    let mut sorts = if requested.is_empty() {
        T::default_sort()
    } else {
        requested.to_vec()
    };

    for sort in &sorts {
        if !T::supports_sort(&sort.field) {
            return Err(ApiError::BadRequest(format!(
                "Field '{}' is not orderable for this resource",
                sort.field
            )));
        }
    }

    for sort in T::tie_breaker_sort() {
        if !sorts.iter().any(|existing| existing.field == sort.field) {
            sorts.push(sort);
        }
    }

    Ok(sorts)
}

fn encode_cursor<T>(item: &T, sorts: &[SortParam]) -> Result<String, ApiError>
where
    T: CursorPaginated,
{
    let sorts_for_cursor: Vec<CursorSort> = sorts
        .iter()
        .map(|sort| CursorSort {
            field: sort.field.to_string(),
            descending: sort.descending,
        })
        .collect();

    let values: Vec<CursorValue> = sorts
        .iter()
        .map(|sort| item.cursor_value(&sort.field))
        .collect::<Result<_, _>>()?;

    let token = CursorToken {
        sorts: sorts_for_cursor,
        values,
    };

    let bytes = serde_json::to_vec(&token).map_err(|error| {
        ApiError::InternalServerError(format!("failed to serialize cursor: {error}"))
    })?;

    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

fn decode_cursor(cursor: &str, sorts: &[SortParam]) -> Result<CursorToken, ApiError> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|error| ApiError::BadRequest(format!("invalid cursor: {error}")))?;

    let token: CursorToken = serde_json::from_slice(&bytes)
        .map_err(|error| ApiError::BadRequest(format!("invalid cursor: {error}")))?;

    let expected_sorts: Vec<CursorSort> = sorts
        .iter()
        .map(|sort| CursorSort {
            field: sort.field.to_string(),
            descending: sort.descending,
        })
        .collect();

    if token.sorts != expected_sorts {
        return Err(ApiError::BadRequest(
            "cursor does not match current sort order".to_string(),
        ));
    }

    Ok(token)
}

pub fn decode_cursor_values(
    cursor: &str,
    sorts: &[SortParam],
) -> Result<Vec<CursorValue>, ApiError> {
    Ok(decode_cursor(cursor, sorts)?.values)
}

fn cursor_equality_sql(field: &CursorSqlField, value: &CursorValue) -> Result<String, ApiError> {
    match value {
        CursorValue::Null => {
            if !field.nullable {
                return Err(ApiError::BadRequest(format!(
                    "cursor contains null for non-nullable field '{}'",
                    field.column
                )));
            }
            Ok(format!("{} IS NULL", field.column))
        }
        _ => Ok(format!(
            "{} = {}",
            field.column,
            cursor_literal_sql(field, value)?
        )),
    }
}

fn cursor_after_sql(
    field: &CursorSqlField,
    sort: &SortParam,
    value: &CursorValue,
) -> Result<String, ApiError> {
    match value {
        CursorValue::Null => {
            if !field.nullable {
                return Err(ApiError::BadRequest(format!(
                    "cursor contains null for non-nullable field '{}'",
                    field.column
                )));
            }

            if sort.descending {
                Ok("FALSE".to_string())
            } else {
                Ok(format!("{} IS NOT NULL", field.column))
            }
        }
        _ => {
            let literal = cursor_literal_sql(field, value)?;
            if field.nullable && sort.descending {
                Ok(format!(
                    "({} < {} OR {} IS NULL)",
                    field.column, literal, field.column
                ))
            } else {
                let operator = if sort.descending { "<" } else { ">" };
                Ok(format!("{} {} {}", field.column, operator, literal))
            }
        }
    }
}

fn cursor_literal_sql(field: &CursorSqlField, value: &CursorValue) -> Result<String, ApiError> {
    match (field.sql_type, value) {
        (_, CursorValue::Null) => Err(ApiError::BadRequest(format!(
            "cursor contains null for field '{}'",
            field.column
        ))),
        (CursorSqlType::Boolean, CursorValue::Boolean(value)) => Ok(if *value {
            "TRUE".to_string()
        } else {
            "FALSE".to_string()
        }),
        (CursorSqlType::Integer, CursorValue::Integer(value)) => Ok(value.to_string()),
        (CursorSqlType::String, CursorValue::String(value)) => {
            Ok(format!("'{}'", value.replace('\'', "''")))
        }
        (CursorSqlType::DateTime, CursorValue::DateTime(value)) => Ok(format!(
            "'{}'::timestamp",
            value.format("%Y-%m-%d %H:%M:%S%.f")
        )),
        (CursorSqlType::IntegerArray, CursorValue::IntegerArray(values)) => {
            if values.is_empty() {
                Ok("ARRAY[]::integer[]".to_string())
            } else {
                Ok(format!(
                    "ARRAY[{}]::integer[]",
                    values
                        .iter()
                        .map(std::string::ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                ))
            }
        }
        _ => Err(ApiError::BadRequest(format!(
            "cursor value does not match expected type for '{}'",
            field.column
        ))),
    }
}

macro_rules! sort_param {
    ($field:expr) => {
        SortParam {
            field: $field,
            descending: false,
        }
    };
}

fn string_or_null(value: Option<&str>) -> CursorValue {
    match value {
        Some(value) => CursorValue::String(value.to_string()),
        None => CursorValue::Null,
    }
}

impl CursorPaginated for Namespace {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id | FilterField::Name | FilterField::CreatedAt | FilterField::UpdatedAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::Name => CursorValue::String(self.name.clone()),
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for namespaces",
                    field
                )))
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![sort_param!(FilterField::Id)]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        vec![sort_param!(FilterField::Id)]
    }
}

impl CursorSqlMapping for Namespace {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "namespaces.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "namespaces.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "namespaces.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "namespaces.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for namespaces",
                    field
                )))
            }
        })
    }
}

impl CursorPaginated for HubuumClassExpanded {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::Namespaces
                | FilterField::NamespaceId
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::Name => CursorValue::String(self.name.clone()),
            FilterField::Namespaces | FilterField::NamespaceId => {
                CursorValue::Integer(self.namespace.id as i64)
            }
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for classes",
                    field
                )))
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![sort_param!(FilterField::Id)]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        vec![sort_param!(FilterField::Id)]
    }
}

impl CursorSqlMapping for HubuumClassExpanded {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "hubuumclass.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "hubuumclass.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Namespaces | FilterField::NamespaceId => CursorSqlField {
                column: "hubuumclass.namespace_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "hubuumclass.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "hubuumclass.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for classes",
                    field
                )))
            }
        })
    }
}

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
        vec![sort_param!(FilterField::Id)]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        vec![sort_param!(FilterField::Id)]
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

impl CursorPaginated for User {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::Username
                | FilterField::Email
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::Name | FilterField::Username => CursorValue::String(self.username.clone()),
            FilterField::Email => string_or_null(self.email.as_deref()),
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for users",
                    field
                )))
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![sort_param!(FilterField::Id)]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        vec![sort_param!(FilterField::Id)]
    }
}

impl CursorSqlMapping for User {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "users.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name | FilterField::Username => CursorSqlField {
                column: "users.username",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Email => CursorSqlField {
                column: "users.email",
                sql_type: CursorSqlType::String,
                nullable: true,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "users.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "users.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for users",
                    field
                )))
            }
        })
    }
}

impl CursorPaginated for Group {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::Groupname
                | FilterField::Description
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::Name | FilterField::Groupname => {
                CursorValue::String(self.groupname.clone())
            }
            FilterField::Description => CursorValue::String(self.description.clone()),
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for groups",
                    field
                )))
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![sort_param!(FilterField::Id)]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        vec![sort_param!(FilterField::Id)]
    }
}

impl CursorSqlMapping for Group {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "groups.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name | FilterField::Groupname => CursorSqlField {
                column: "groups.groupname",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Description => CursorSqlField {
                column: "groups.description",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "groups.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "groups.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for groups",
                    field
                )))
            }
        })
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
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::ClassFrom => CursorValue::Integer(self.from_hubuum_class_id as i64),
            FilterField::ClassTo => CursorValue::Integer(self.to_hubuum_class_id as i64),
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for class relations",
                    field
                )))
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![sort_param!(FilterField::Id)]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        vec![sort_param!(FilterField::Id)]
    }
}

impl CursorSqlMapping for HubuumClassRelation {
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
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for class relations",
                    field
                )))
            }
        })
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
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for object relations",
                    field
                )))
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![sort_param!(FilterField::Id)]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        vec![sort_param!(FilterField::Id)]
    }
}

impl CursorSqlMapping for HubuumObjectRelation {
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
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for object relations",
                    field
                )))
            }
        })
    }
}

impl CursorPaginated for GroupPermission {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::Groupname
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.permission.id as i64),
            FilterField::Name | FilterField::Groupname => {
                CursorValue::String(self.group.groupname.clone())
            }
            FilterField::CreatedAt => CursorValue::DateTime(self.permission.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.permission.updated_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for group permissions",
                    field
                )))
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![sort_param!(FilterField::Id)]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        vec![sort_param!(FilterField::Id)]
    }
}

impl CursorSqlMapping for GroupPermission {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "permissions.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name | FilterField::Groupname => CursorSqlField {
                column: "groups.groupname",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "permissions.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "permissions.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for group permissions",
                    field
                )))
            }
        })
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
                )))
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![
            sort_param!(FilterField::Depth),
            sort_param!(FilterField::Path),
        ]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        vec![
            sort_param!(FilterField::ClassFrom),
            sort_param!(FilterField::ClassTo),
            sort_param!(FilterField::Depth),
            sort_param!(FilterField::Path),
        ]
    }
}

impl CursorSqlMapping for HubuumClassRelationTransitive {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::ClassFrom => CursorSqlField {
                column: "hubuumclass_closure.ancestor_class_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ClassTo => CursorSqlField {
                column: "hubuumclass_closure.descendant_class_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Depth => CursorSqlField {
                column: "hubuumclass_closure.depth",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Path => CursorSqlField {
                column: "hubuumclass_closure.path",
                sql_type: CursorSqlType::IntegerArray,
                nullable: true,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for transitive class relations",
                    field
                )))
            }
        })
    }
}

impl CursorPaginated for ObjectClosureView {
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
                | FilterField::ObjectFrom
                | FilterField::ObjectTo
                | FilterField::ClassFrom
                | FilterField::ClassTo
                | FilterField::NamespacesFrom
                | FilterField::NamespacesTo
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
            FilterField::Namespaces | FilterField::NamespaceId | FilterField::NamespacesTo => {
                CursorValue::Integer(self.descendant_namespace_id as i64)
            }
            FilterField::NamespacesFrom => CursorValue::Integer(self.ancestor_namespace_id as i64),
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
                )))
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![sort_param!(FilterField::Path), sort_param!(FilterField::Id)]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        vec![sort_param!(FilterField::Path), sort_param!(FilterField::Id)]
    }
}

impl CursorSqlMapping for ObjectClosureView {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id | FilterField::ObjectTo => CursorSqlField {
                column: "object_closure_view.descendant_object_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ObjectFrom => CursorSqlField {
                column: "object_closure_view.ancestor_object_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name | FilterField::NameTo => CursorSqlField {
                column: "object_closure_view.descendant_name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::NameFrom => CursorSqlField {
                column: "object_closure_view.ancestor_name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Description | FilterField::DescriptionTo => CursorSqlField {
                column: "object_closure_view.descendant_description",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::DescriptionFrom => CursorSqlField {
                column: "object_closure_view.ancestor_description",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Namespaces | FilterField::NamespaceId | FilterField::NamespacesTo => {
                CursorSqlField {
                    column: "object_closure_view.descendant_namespace_id",
                    sql_type: CursorSqlType::Integer,
                    nullable: false,
                }
            }
            FilterField::NamespacesFrom => CursorSqlField {
                column: "object_closure_view.ancestor_namespace_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ClassId | FilterField::Classes | FilterField::ClassTo => CursorSqlField {
                column: "object_closure_view.descendant_class_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ClassFrom => CursorSqlField {
                column: "object_closure_view.ancestor_class_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::CreatedAt | FilterField::CreatedAtTo => CursorSqlField {
                column: "object_closure_view.descendant_created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::CreatedAtFrom => CursorSqlField {
                column: "object_closure_view.ancestor_created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt | FilterField::UpdatedAtTo => CursorSqlField {
                column: "object_closure_view.descendant_updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAtFrom => CursorSqlField {
                column: "object_closure_view.ancestor_updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::Depth => CursorSqlField {
                column: "object_closure_view.depth",
                sql_type: CursorSqlType::Integer,
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
        vec![sort_param!(FilterField::Path), sort_param!(FilterField::Id)]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        vec![sort_param!(FilterField::Path), sort_param!(FilterField::Id)]
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

impl CursorPaginated for UserToken {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(field, FilterField::IssuedAt | FilterField::Name)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::IssuedAt => CursorValue::DateTime(self.issued),
            FilterField::Name => CursorValue::String(self.token.clone()),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for user tokens",
                    field
                )))
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![
            SortParam {
                field: FilterField::IssuedAt,
                descending: true,
            },
            sort_param!(FilterField::Name),
        ]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        vec![sort_param!(FilterField::Name)]
    }
}

impl CursorSqlMapping for UserToken {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::IssuedAt => CursorSqlField {
                column: "tokens.issued",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "tokens.token",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for user tokens",
                    field
                )))
            }
        })
    }
}

#[macro_export]
macro_rules! apply_cursor_ordering {
    ($query:ident, $sorts:expr, $ty:ty) => {{
        use diesel::dsl::sql;
        use diesel::sql_types::{Array, Bool, Integer, Nullable, Text, Timestamp};

        let mut is_first_order = true;
        for sort in $sorts.iter() {
            let sql_field = $crate::models::pagination::cursor_sql_field::<$ty>(&sort.field)?;
            let order_sql = $crate::models::pagination::order_sql_clause::<$ty>(sort)?;

            $query = match (is_first_order, sql_field.sql_type, sql_field.nullable) {
                (true, $crate::models::pagination::CursorSqlType::Boolean, false) => {
                    $query.order_by(sql::<Bool>(&order_sql))
                }
                (false, $crate::models::pagination::CursorSqlType::Boolean, false) => {
                    $query.then_order_by(sql::<Bool>(&order_sql))
                }
                (true, $crate::models::pagination::CursorSqlType::Boolean, true) => {
                    $query.order_by(sql::<Nullable<Bool>>(&order_sql))
                }
                (false, $crate::models::pagination::CursorSqlType::Boolean, true) => {
                    $query.then_order_by(sql::<Nullable<Bool>>(&order_sql))
                }
                (true, $crate::models::pagination::CursorSqlType::Integer, false) => {
                    $query.order_by(sql::<Integer>(&order_sql))
                }
                (false, $crate::models::pagination::CursorSqlType::Integer, false) => {
                    $query.then_order_by(sql::<Integer>(&order_sql))
                }
                (true, $crate::models::pagination::CursorSqlType::Integer, true) => {
                    $query.order_by(sql::<Nullable<Integer>>(&order_sql))
                }
                (false, $crate::models::pagination::CursorSqlType::Integer, true) => {
                    $query.then_order_by(sql::<Nullable<Integer>>(&order_sql))
                }
                (true, $crate::models::pagination::CursorSqlType::String, false) => {
                    $query.order_by(sql::<Text>(&order_sql))
                }
                (false, $crate::models::pagination::CursorSqlType::String, false) => {
                    $query.then_order_by(sql::<Text>(&order_sql))
                }
                (true, $crate::models::pagination::CursorSqlType::String, true) => {
                    $query.order_by(sql::<Nullable<Text>>(&order_sql))
                }
                (false, $crate::models::pagination::CursorSqlType::String, true) => {
                    $query.then_order_by(sql::<Nullable<Text>>(&order_sql))
                }
                (true, $crate::models::pagination::CursorSqlType::DateTime, false) => {
                    $query.order_by(sql::<Timestamp>(&order_sql))
                }
                (false, $crate::models::pagination::CursorSqlType::DateTime, false) => {
                    $query.then_order_by(sql::<Timestamp>(&order_sql))
                }
                (true, $crate::models::pagination::CursorSqlType::DateTime, true) => {
                    $query.order_by(sql::<Nullable<Timestamp>>(&order_sql))
                }
                (false, $crate::models::pagination::CursorSqlType::DateTime, true) => {
                    $query.then_order_by(sql::<Nullable<Timestamp>>(&order_sql))
                }
                (true, $crate::models::pagination::CursorSqlType::IntegerArray, false) => {
                    $query.order_by(sql::<Array<Integer>>(&order_sql))
                }
                (false, $crate::models::pagination::CursorSqlType::IntegerArray, false) => {
                    $query.then_order_by(sql::<Array<Integer>>(&order_sql))
                }
                (true, $crate::models::pagination::CursorSqlType::IntegerArray, true) => {
                    $query.order_by(sql::<Array<Nullable<Integer>>>(&order_sql))
                }
                (false, $crate::models::pagination::CursorSqlType::IntegerArray, true) => {
                    $query.then_order_by(sql::<Array<Nullable<Integer>>>(&order_sql))
                }
            };

            is_first_order = false;
        }
    }};
}

#[macro_export]
macro_rules! apply_query_options {
    ($query:ident, $query_options:expr, $ty:ty) => {{
        let query_options = &$query_options;

        if let Some(cursor_sql) = $crate::models::pagination::cursor_filter_sql::<$ty>(
            &query_options.sort,
            query_options.cursor.as_deref(),
        )? {
            $query = $query.filter(diesel::dsl::sql::<diesel::sql_types::Bool>(&cursor_sql));
        }

        $crate::apply_cursor_ordering!($query, query_options.sort, $ty);

        if let Some(limit) = query_options.limit {
            $query = $query.limit(limit as i64);
        }
    }};
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::*;

    fn namespace(id: i32, name: &str) -> Namespace {
        Namespace {
            id,
            name: name.to_string(),
            description: format!("namespace {id}"),
            created_at: NaiveDate::from_ymd_opt(2024, 1, id as u32)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            updated_at: NaiveDate::from_ymd_opt(2024, 1, id as u32)
                .unwrap()
                .and_hms_opt(1, 0, 0)
                .unwrap(),
        }
    }

    #[test]
    fn test_paginate_namespaces_with_cursor() {
        let namespaces = vec![
            namespace(1, "alpha"),
            namespace(2, "beta"),
            namespace(3, "gamma"),
        ];

        let first_page = finalize_page(
            namespaces.clone(),
            &QueryOptions {
                filters: vec![],
                sort: vec![],
                limit: Some(2),
                cursor: None,
            },
        )
        .unwrap();

        assert_eq!(
            first_page
                .items
                .iter()
                .map(|item| item.id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(first_page.next_cursor.is_some());

        let prepared_query = prepare_db_pagination::<Namespace>(&QueryOptions {
            filters: vec![],
            sort: vec![],
            limit: Some(2),
            cursor: first_page.next_cursor.clone(),
        })
        .unwrap();

        let cursor_sql =
            cursor_filter_sql::<Namespace>(&prepared_query.sort, prepared_query.cursor.as_deref())
                .unwrap();

        assert_eq!(cursor_sql, Some("((namespaces.id > 2))".to_string()));

        let second_page = finalize_page(
            vec![namespace(3, "gamma")],
            &QueryOptions {
                filters: vec![],
                sort: vec![],
                limit: Some(2),
                cursor: first_page.next_cursor,
            },
        )
        .unwrap();

        assert_eq!(
            second_page
                .items
                .iter()
                .map(|item| item.id)
                .collect::<Vec<_>>(),
            vec![3]
        );
        assert!(second_page.next_cursor.is_none());
    }

    #[test]
    fn test_paginate_namespaces_descending() {
        let namespaces = vec![
            namespace(3, "gamma"),
            namespace(2, "beta"),
            namespace(1, "alpha"),
        ];

        let page = finalize_page(
            namespaces,
            &QueryOptions {
                filters: vec![],
                sort: vec![SortParam {
                    field: FilterField::Name,
                    descending: true,
                }],
                limit: Some(2),
                cursor: None,
            },
        )
        .unwrap();

        assert_eq!(
            page.items
                .iter()
                .map(|item| item.name.clone())
                .collect::<Vec<_>>(),
            vec!["gamma".to_string(), "beta".to_string()]
        );
        assert!(page.next_cursor.is_some());
    }

    #[test]
    fn test_prepare_db_pagination_adds_limit_and_tie_breaker() {
        let prepared = prepare_db_pagination::<User>(&QueryOptions {
            filters: vec![],
            sort: vec![SortParam {
                field: FilterField::Username,
                descending: false,
            }],
            limit: None,
            cursor: None,
        })
        .unwrap();

        assert_eq!(prepared.limit, Some(DEFAULT_PAGE_LIMIT + 1));
        assert_eq!(prepared.sort.len(), 2);
        assert_eq!(prepared.sort[0].field, FilterField::Username);
        assert_eq!(prepared.sort[1].field, FilterField::Id);
    }

    #[test]
    fn test_cursor_filter_sql_handles_nullable_descending_strings() {
        let sql = cursor_filter_sql::<User>(
            &[SortParam {
                field: FilterField::Email,
                descending: true,
            }],
            Some(
                &base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
                    serde_json::to_vec(&CursorToken {
                        sorts: vec![CursorSort {
                            field: "email".to_string(),
                            descending: true,
                        }],
                        values: vec![CursorValue::String("b@example.com".to_string())],
                    })
                    .unwrap(),
                ),
            ),
        )
        .unwrap();

        assert_eq!(
            sql,
            Some("(((users.email < 'b@example.com' OR users.email IS NULL)))".to_string())
        );
    }
}
