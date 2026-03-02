use std::collections::HashMap;

use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::config::{DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT, get_config};
use crate::errors::ApiError;
use crate::models::search::{FilterField, QueryOptions, SortParam};
pub use crate::traits::pagination::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};

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

pub fn page_limits() -> Result<(usize, usize), ApiError> {
    let config = get_config()?;
    Ok((config.default_page_limit, config.max_page_limit))
}

pub fn page_limits_or_defaults() -> (usize, usize) {
    page_limits().unwrap_or((DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT))
}

pub fn validate_page_limit(limit: usize) -> Result<usize, ApiError> {
    let (_, max_page_limit) = page_limits()?;

    if limit == 0 {
        return Err(ApiError::BadRequest(
            "limit must be greater than 0".to_string(),
        ));
    }

    if limit > max_page_limit {
        return Err(ApiError::BadRequest(format!(
            "limit must be at most {max_page_limit}"
        )));
    }

    Ok(limit)
}

pub fn prepare_db_pagination<T>(query_options: &QueryOptions) -> Result<QueryOptions, ApiError>
where
    T: CursorPaginated,
{
    let (default_page_limit, _) = page_limits()?;
    let limit = validate_page_limit(query_options.limit.unwrap_or(default_page_limit))?;
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
        limit: validate_page_limit(query_options.limit.unwrap_or(page_limits()?.0))?,
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

#[macro_export]
macro_rules! apply_cursor_ordering {
    ($query:ident, $sorts:expr, $ty:ty) => {{
        use diesel::dsl::sql;
        use diesel::sql_types::{Array, Bool, Integer, Nullable, Text, Timestamp};

        let mut is_first_order = true;
        for sort in $sorts.iter() {
            let sql_field = $crate::pagination::cursor_sql_field::<$ty>(&sort.field)?;
            let order_sql = $crate::pagination::order_sql_clause::<$ty>(sort)?;

            $query = match (is_first_order, sql_field.sql_type, sql_field.nullable) {
                (true, $crate::pagination::CursorSqlType::Boolean, false) => {
                    $query.order_by(sql::<Bool>(&order_sql))
                }
                (false, $crate::pagination::CursorSqlType::Boolean, false) => {
                    $query.then_order_by(sql::<Bool>(&order_sql))
                }
                (true, $crate::pagination::CursorSqlType::Boolean, true) => {
                    $query.order_by(sql::<Nullable<Bool>>(&order_sql))
                }
                (false, $crate::pagination::CursorSqlType::Boolean, true) => {
                    $query.then_order_by(sql::<Nullable<Bool>>(&order_sql))
                }
                (true, $crate::pagination::CursorSqlType::Integer, false) => {
                    $query.order_by(sql::<Integer>(&order_sql))
                }
                (false, $crate::pagination::CursorSqlType::Integer, false) => {
                    $query.then_order_by(sql::<Integer>(&order_sql))
                }
                (true, $crate::pagination::CursorSqlType::Integer, true) => {
                    $query.order_by(sql::<Nullable<Integer>>(&order_sql))
                }
                (false, $crate::pagination::CursorSqlType::Integer, true) => {
                    $query.then_order_by(sql::<Nullable<Integer>>(&order_sql))
                }
                (true, $crate::pagination::CursorSqlType::String, false) => {
                    $query.order_by(sql::<Text>(&order_sql))
                }
                (false, $crate::pagination::CursorSqlType::String, false) => {
                    $query.then_order_by(sql::<Text>(&order_sql))
                }
                (true, $crate::pagination::CursorSqlType::String, true) => {
                    $query.order_by(sql::<Nullable<Text>>(&order_sql))
                }
                (false, $crate::pagination::CursorSqlType::String, true) => {
                    $query.then_order_by(sql::<Nullable<Text>>(&order_sql))
                }
                (true, $crate::pagination::CursorSqlType::DateTime, false) => {
                    $query.order_by(sql::<Timestamp>(&order_sql))
                }
                (false, $crate::pagination::CursorSqlType::DateTime, false) => {
                    $query.then_order_by(sql::<Timestamp>(&order_sql))
                }
                (true, $crate::pagination::CursorSqlType::DateTime, true) => {
                    $query.order_by(sql::<Nullable<Timestamp>>(&order_sql))
                }
                (false, $crate::pagination::CursorSqlType::DateTime, true) => {
                    $query.then_order_by(sql::<Nullable<Timestamp>>(&order_sql))
                }
                (true, $crate::pagination::CursorSqlType::IntegerArray, false) => {
                    $query.order_by(sql::<Array<Integer>>(&order_sql))
                }
                (false, $crate::pagination::CursorSqlType::IntegerArray, false) => {
                    $query.then_order_by(sql::<Array<Integer>>(&order_sql))
                }
                (true, $crate::pagination::CursorSqlType::IntegerArray, true) => {
                    $query.order_by(sql::<Array<Nullable<Integer>>>(&order_sql))
                }
                (false, $crate::pagination::CursorSqlType::IntegerArray, true) => {
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

        if let Some(cursor_sql) = $crate::pagination::cursor_filter_sql::<$ty>(
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
    use crate::models::{Namespace, User};

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
