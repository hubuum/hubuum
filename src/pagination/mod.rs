use std::cmp::Ordering;
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
pub const PAGE_LIMIT_HEADER: &str = "X-Page-Limit";
pub const TOTAL_COUNT_HEADER: &str = "X-Total-Count";
pub const SKIPPED_TOTAL_COUNT: i64 = -1;

pub async fn exact_count_or_skipped(
    query_options: &QueryOptions,
    count: impl AsyncFnOnce() -> Result<i64, ApiError>,
) -> Result<i64, ApiError> {
    if query_options.include_total {
        count().await
    } else {
        Ok(SKIPPED_TOTAL_COUNT)
    }
}

pub fn known_count_or_skipped(query_options: &QueryOptions, count: i64) -> i64 {
    if query_options.include_total {
        count
    } else {
        SKIPPED_TOTAL_COUNT
    }
}

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
    validate_page_limit_with_max(limit, max_page_limit)
}

/// Config-free variant of [`validate_page_limit`] that takes the maximum limit
/// explicitly instead of reading it from the global configuration. Useful where
/// the caller already holds the limits, or wants to avoid touching global config
/// (for example, benchmarks).
pub fn validate_page_limit_with_max(
    limit: usize,
    max_page_limit: usize,
) -> Result<usize, ApiError> {
    if limit == 0 {
        return Err(ApiError::BadRequest(
            "limit must be greater than 0".to_string(),
        ));
    }
    if max_page_limit == 0 {
        return Err(ApiError::BadRequest(
            "max_page_limit must be greater than 0".to_string(),
        ));
    }

    Ok(limit.min(max_page_limit))
}

pub fn effective_page_limit(query_options: &QueryOptions) -> Result<usize, ApiError> {
    validate_page_limit(query_options.limit.unwrap_or(page_limits()?.0))
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
    prepared.limit = Some(limit.saturating_add(1));
    Ok(prepared)
}

pub fn count_query_options(query_options: &QueryOptions) -> QueryOptions {
    let mut prepared = query_options.clone();
    prepared.sort.clear();
    prepared.limit = None;
    prepared.cursor = None;
    prepared
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

/// Apply the same stable ordering and cursor semantics as the SQL pagination
/// macros to rows synthesized or authorized outside PostgreSQL. `limit` is
/// applied last; callers should pass the prepared `limit + 1` value so
/// [`finalize_page`] can produce the next cursor normally.
pub fn paginate_in_memory<T>(
    mut items: Vec<T>,
    query_options: &QueryOptions,
) -> Result<Vec<T>, ApiError>
where
    T: CursorPaginated,
{
    let sorts = normalized_sorts::<T>(&query_options.sort)?;
    items.sort_by(|left, right| {
        compare_cursor_values(left, right, &sorts).unwrap_or(Ordering::Equal)
    });

    if let Some(cursor) = query_options.cursor.as_deref() {
        let cursor_values = decode_cursor_values(cursor, &sorts)?;
        let mut filtered = Vec::with_capacity(items.len());
        for item in items {
            if compare_item_to_values(&item, &cursor_values, &sorts)? == Ordering::Greater {
                filtered.push(item);
            }
        }
        items = filtered;
    }

    if let Some(limit) = query_options.limit {
        items.truncate(limit);
    }
    Ok(items)
}

fn compare_cursor_values<T>(left: &T, right: &T, sorts: &[SortParam]) -> Result<Ordering, ApiError>
where
    T: CursorPaginated,
{
    for sort in sorts {
        let ordering = left
            .cursor_value(&sort.field)?
            .cmp(&right.cursor_value(&sort.field)?);
        let ordering = if sort.descending {
            ordering.reverse()
        } else {
            ordering
        };
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

fn compare_item_to_values<T>(
    item: &T,
    values: &[CursorValue],
    sorts: &[SortParam],
) -> Result<Ordering, ApiError>
where
    T: CursorPaginated,
{
    for (sort, cursor_value) in sorts.iter().zip(values) {
        let ordering = item.cursor_value(&sort.field)?.cmp(cursor_value);
        let ordering = if sort.descending {
            ordering.reverse()
        } else {
            ordering
        };
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

pub fn pagination_headers(
    next_cursor: &Option<String>,
    total_count: i64,
    effective_limit: usize,
) -> HashMap<String, String> {
    let mut headers = HashMap::from([(PAGE_LIMIT_HEADER.to_string(), effective_limit.to_string())]);
    if total_count != SKIPPED_TOTAL_COUNT {
        headers.insert(TOTAL_COUNT_HEADER.to_string(), total_count.to_string());
    }
    if let Some(cursor) = next_cursor.as_ref() {
        headers.insert(NEXT_CURSOR_HEADER.to_string(), cursor.clone());
    }
    headers
}

pub fn page_request<T>(query_options: &QueryOptions) -> Result<CursorPageRequest, ApiError>
where
    T: CursorPaginated,
{
    Ok(CursorPageRequest {
        limit: effective_page_limit(query_options)?,
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
        use diesel::sql_types::{Array, Integer, Nullable, Text, Timestamp};

        let mut is_first_order = true;
        for sort in $sorts.iter() {
            let sql_field = $crate::pagination::cursor_sql_field::<$ty>(&sort.field)?;
            let order_sql = $crate::pagination::order_sql_clause::<$ty>(sort)?;

            $query = match (is_first_order, sql_field.sql_type, sql_field.nullable) {
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
    use crate::models::{Collection, UserWithName};

    fn collection(id: i32, name: &str) -> Collection {
        Collection {
            id,
            name: name.to_string(),
            description: format!("collection {id}"),
            created_at: NaiveDate::from_ymd_opt(2024, 1, id as u32)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            updated_at: NaiveDate::from_ymd_opt(2024, 1, id as u32)
                .unwrap()
                .and_hms_opt(1, 0, 0)
                .unwrap(),
            parent_collection_id: None,
        }
    }

    #[test]
    fn test_paginate_collections_with_cursor() {
        let collections = vec![
            collection(1, "alpha"),
            collection(2, "beta"),
            collection(3, "gamma"),
        ];

        let first_page = finalize_page(
            collections.clone(),
            &QueryOptions {
                filters: vec![],
                sort: vec![],
                limit: Some(2),
                cursor: None,
                include_total: true,
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

        let prepared_query = prepare_db_pagination::<Collection>(&QueryOptions {
            filters: vec![],
            sort: vec![],
            limit: Some(2),
            cursor: first_page.next_cursor.clone(),
            include_total: true,
        })
        .unwrap();

        let cursor_sql =
            cursor_filter_sql::<Collection>(&prepared_query.sort, prepared_query.cursor.as_deref())
                .unwrap();

        assert_eq!(cursor_sql, Some("((collections.id > 2))".to_string()));

        let second_page = finalize_page(
            vec![collection(3, "gamma")],
            &QueryOptions {
                filters: vec![],
                sort: vec![],
                limit: Some(2),
                cursor: first_page.next_cursor,
                include_total: true,
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
    fn test_paginate_collections_descending() {
        let collections = vec![
            collection(3, "gamma"),
            collection(2, "beta"),
            collection(1, "alpha"),
        ];

        let page = finalize_page(
            collections,
            &QueryOptions {
                filters: vec![],
                sort: vec![SortParam {
                    field: FilterField::Name,
                    descending: true,
                }],
                limit: Some(2),
                cursor: None,
                include_total: true,
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
        let prepared = prepare_db_pagination::<UserWithName>(&QueryOptions {
            filters: vec![],
            sort: vec![SortParam {
                field: FilterField::Username,
                descending: false,
            }],
            limit: None,
            cursor: None,
            include_total: true,
        })
        .unwrap();

        assert_eq!(prepared.limit, Some(DEFAULT_PAGE_LIMIT + 1));
        assert_eq!(prepared.sort.len(), 2);
        assert_eq!(prepared.sort[0].field, FilterField::Username);
        assert_eq!(prepared.sort[1].field, FilterField::Id);
    }

    #[tokio::test]
    async fn exact_total_count_can_be_skipped() {
        let options = QueryOptions {
            filters: vec![],
            sort: vec![],
            limit: None,
            cursor: None,
            include_total: false,
        };
        let count = exact_count_or_skipped(&options, async || {
            panic!("count query must not execute when include_total is false")
        })
        .await
        .unwrap();
        assert_eq!(count, SKIPPED_TOTAL_COUNT);

        let headers = pagination_headers(&None, count, 25);
        assert!(!headers.contains_key(TOTAL_COUNT_HEADER));
        assert_eq!(headers.get(PAGE_LIMIT_HEADER), Some(&"25".to_string()));
    }

    #[test]
    fn test_cursor_filter_sql_handles_nullable_descending_strings() {
        let sql = cursor_filter_sql::<UserWithName>(
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

    #[test]
    fn validate_page_limit_with_max_accepts_within_range() {
        assert_eq!(validate_page_limit_with_max(10, 100).unwrap(), 10);
        assert_eq!(validate_page_limit_with_max(100, 100).unwrap(), 100);
    }

    #[test]
    fn validate_page_limit_with_max_rejects_zero() {
        let error = validate_page_limit_with_max(0, 100).unwrap_err();
        assert_eq!(error.to_string(), "limit must be greater than 0");
    }

    #[test]
    fn validate_page_limit_with_max_rejects_zero_maximum() {
        let error = validate_page_limit_with_max(1, 0).unwrap_err();
        assert_eq!(error.to_string(), "max_page_limit must be greater than 0");
    }

    #[test]
    fn validate_page_limit_with_max_clamps_above_maximum() {
        assert_eq!(validate_page_limit_with_max(101, 100).unwrap(), 100);
    }
}
