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
pub const MAX_ENCODED_CURSOR_BYTES: usize = 64 * 1024;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageLimits {
    default: usize,
    maximum: usize,
}

impl PageLimits {
    pub fn new(default: usize, maximum: usize) -> Result<Self, ApiError> {
        if default == 0 {
            return Err(ApiError::BadRequest(
                "default_page_limit must be greater than 0".to_string(),
            ));
        }
        if maximum == 0 {
            return Err(ApiError::BadRequest(
                "max_page_limit must be greater than 0".to_string(),
            ));
        }
        if default > maximum {
            return Err(ApiError::BadRequest(format!(
                "default_page_limit ({default}) must be less than or equal to max_page_limit ({maximum})"
            )));
        }

        Ok(Self { default, maximum })
    }

    pub fn default_limit(self) -> usize {
        self.default
    }

    pub fn maximum_limit(self) -> usize {
        self.maximum
    }

    pub fn clamp(self, limit: usize) -> usize {
        limit.min(self.maximum)
    }

    pub fn resolve(self, requested: Option<usize>) -> Result<usize, ApiError> {
        let limit = requested.unwrap_or(self.default);
        if limit == 0 {
            return Err(ApiError::BadRequest(
                "limit must be greater than 0".to_string(),
            ));
        }
        Ok(self.clamp(limit))
    }
}

impl Default for PageLimits {
    fn default() -> Self {
        Self {
            default: DEFAULT_PAGE_LIMIT,
            maximum: MAX_PAGE_LIMIT,
        }
    }
}

pub fn page_limits() -> Result<PageLimits, ApiError> {
    let config = get_config()?;
    PageLimits::new(config.default_page_limit, config.max_page_limit)
}

pub fn page_limits_or_defaults() -> PageLimits {
    page_limits().unwrap_or_default()
}

pub fn validate_page_limit(limit: usize) -> Result<usize, ApiError> {
    page_limits()?.resolve(Some(limit))
}

pub fn effective_page_limit(query_options: &QueryOptions) -> Result<usize, ApiError> {
    page_limits()?.resolve(query_options.limit)
}

pub fn prepare_db_pagination<T>(query_options: &QueryOptions) -> Result<QueryOptions, ApiError>
where
    T: CursorPaginated,
{
    let limit = page_limits()?.resolve(query_options.limit)?;
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

    finalize_page_items(items, &request.sorts, has_more)
}

pub(crate) fn finalize_partial_page<T>(
    items: Vec<T>,
    query_options: &QueryOptions,
    has_more: bool,
) -> Result<Page<T>, ApiError>
where
    T: CursorPaginated,
{
    let request = page_request::<T>(query_options)?;
    if items.len() > request.limit || (has_more && items.is_empty()) {
        return Err(ApiError::InternalServerError(
            "Partial cursor page has invalid item bounds".to_string(),
        ));
    }
    finalize_page_items(items, &request.sorts, has_more)
}

fn finalize_page_items<T>(
    items: Vec<T>,
    sorts: &[SortParam],
    has_more: bool,
) -> Result<Page<T>, ApiError>
where
    T: CursorPaginated,
{
    let next_cursor = if has_more {
        items
            .last()
            .map(|item| encode_cursor(item, sorts))
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
    items: Vec<T>,
    query_options: &QueryOptions,
) -> Result<Vec<T>, ApiError>
where
    T: CursorPaginated,
{
    let sorts = normalized_sorts::<T>(&query_options.sort)?;
    let cursor_values = query_options
        .cursor
        .as_deref()
        .map(|cursor| decode_cursor_values(cursor, &sorts))
        .transpose()?;
    paginate_in_memory_with_values(items, query_options, &sorts, cursor_values.as_deref())
}

fn paginate_in_memory_with_values<T>(
    mut items: Vec<T>,
    query_options: &QueryOptions,
    sorts: &[SortParam],
    cursor_values: Option<&[CursorValue]>,
) -> Result<Vec<T>, ApiError>
where
    T: CursorPaginated,
{
    items.sort_by(|left, right| {
        compare_cursor_values(left, right, sorts).unwrap_or(Ordering::Equal)
    });

    if let Some(cursor_values) = cursor_values {
        let mut filtered = Vec::with_capacity(items.len());
        for item in items {
            if compare_item_to_values(&item, cursor_values, sorts)? == Ordering::Greater {
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

impl From<CursorSqlField> for CursorSqlField<String> {
    fn from(field: CursorSqlField) -> Self {
        Self {
            column: field.column.to_string(),
            sql_type: field.sql_type,
            nullable: field.nullable,
        }
    }
}

pub fn order_sql_clause_for_field<T>(sort: &SortParam, field: &CursorSqlField<T>) -> String
where
    T: AsRef<str>,
{
    order_sql_clause_for_expression(sort, field.expression(), field.nullable)
}

fn order_sql_clause_for_expression(sort: &SortParam, expression: &str, nullable: bool) -> String {
    let direction = if sort.descending { "DESC" } else { "ASC" };
    let nulls = if nullable {
        if sort.descending {
            " NULLS LAST"
        } else {
            " NULLS FIRST"
        }
    } else {
        ""
    };

    format!("{expression} {direction}{nulls}")
}

pub fn order_sql_clause<T>(sort: &SortParam) -> Result<String, ApiError>
where
    T: CursorSqlMapping,
{
    let field = cursor_sql_field::<T>(&sort.field)?;
    Ok(order_sql_clause_for_expression(
        sort,
        field.expression(),
        field.nullable,
    ))
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

    let fields = sorts
        .iter()
        .map(|sort| cursor_sql_field::<T>(&sort.field))
        .collect::<Result<Vec<_>, _>>()?;

    cursor_filter_sql_from_fields(sorts, &fields, Some(cursor))
}

pub fn cursor_filter_sql_for_fields<T>(
    sorts: &[SortParam],
    fields: &[CursorSqlField<T>],
    cursor: Option<&str>,
) -> Result<Option<String>, ApiError>
where
    T: AsRef<str>,
{
    cursor_filter_sql_from_fields(sorts, fields, cursor)
}

fn cursor_filter_sql_from_fields<T>(
    sorts: &[SortParam],
    fields: &[CursorSqlField<T>],
    cursor: Option<&str>,
) -> Result<Option<String>, ApiError>
where
    T: AsRef<str>,
{
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    if fields.len() != sorts.len() {
        return Err(ApiError::InternalServerError(
            "cursor SQL field count does not match sort count".to_string(),
        ));
    }
    let cursor_values = decode_and_validate_cursor_values(cursor, sorts, fields)?;

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

fn decode_and_validate_cursor_values<T>(
    cursor: &str,
    sorts: &[SortParam],
    fields: &[CursorSqlField<T>],
) -> Result<Vec<CursorValue>, ApiError>
where
    T: AsRef<str>,
{
    let cursor_values = decode_cursor_values(cursor, sorts)?;
    for (field, value) in fields.iter().zip(&cursor_values) {
        validate_cursor_value(field, value)?;
    }
    Ok(cursor_values)
}

fn validate_cursor_value<T>(field: &CursorSqlField<T>, value: &CursorValue) -> Result<(), ApiError>
where
    T: AsRef<str>,
{
    match value {
        CursorValue::Null if field.nullable => Ok(()),
        CursorValue::Null => Err(ApiError::BadRequest(format!(
            "cursor contains null for non-nullable field '{}'",
            field.expression()
        ))),
        _ => cursor_literal_sql(field, value).map(|_| ()),
    }
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

pub(crate) fn encode_cursor<T>(item: &T, sorts: &[SortParam]) -> Result<String, ApiError>
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
    for value in &values {
        match value {
            CursorValue::String(value) => validate_cursor_string(value)?,
            CursorValue::Json(value) => validate_postgres_jsonb_cursor_value(value)?,
            _ => {}
        }
    }

    let token = CursorToken {
        sorts: sorts_for_cursor,
        values,
    };

    let bytes = serde_json::to_vec(&token).map_err(|error| {
        ApiError::InternalServerError(format!("failed to serialize cursor: {error}"))
    })?;
    let encoded_length = bytes.len().saturating_mul(4).saturating_add(2) / 3;
    if encoded_length > MAX_ENCODED_CURSOR_BYTES {
        return Err(cursor_too_large());
    }

    let cursor = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    ensure_cursor_within_limit(&cursor)?;
    Ok(cursor)
}

fn decode_cursor(cursor: &str, sorts: &[SortParam]) -> Result<CursorToken, ApiError> {
    ensure_cursor_within_limit(cursor)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|error| ApiError::BadRequest(format!("invalid cursor: {error}")))?;

    let mut token: CursorToken = serde_json::from_slice(&bytes)
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

    if token.values.len() != sorts.len() {
        return Err(ApiError::BadRequest(
            "cursor value count does not match current sort order".to_string(),
        ));
    }

    for value in &mut token.values {
        if let CursorValue::Decimal(source) = value {
            *source =
                hubuum_computed_fields::canonical_decimal_string(source).ok_or_else(|| {
                    ApiError::BadRequest("cursor contains an invalid decimal value".to_string())
                })?;
        }
        if let CursorValue::String(value) = value {
            validate_cursor_string(value)?;
        }
    }

    Ok(token)
}

fn ensure_cursor_within_limit(cursor: &str) -> Result<(), ApiError> {
    if cursor.len() > MAX_ENCODED_CURSOR_BYTES {
        return Err(cursor_too_large());
    }
    Ok(())
}

fn validate_cursor_string(value: &str) -> Result<(), ApiError> {
    if value.contains('\0') {
        return Err(ApiError::BadRequest(
            "cursor string values cannot contain an embedded NUL byte".to_string(),
        ));
    }
    Ok(())
}

fn cursor_too_large() -> ApiError {
    ApiError::BadRequest(format!(
        "pagination cursor exceeds the maximum encoded size of {MAX_ENCODED_CURSOR_BYTES} bytes; use smaller sort values"
    ))
}

pub fn decode_cursor_values(
    cursor: &str,
    sorts: &[SortParam],
) -> Result<Vec<CursorValue>, ApiError> {
    Ok(decode_cursor(cursor, sorts)?.values)
}

fn cursor_equality_sql<T>(
    field: &CursorSqlField<T>,
    value: &CursorValue,
) -> Result<String, ApiError>
where
    T: AsRef<str>,
{
    match value {
        CursorValue::Null => {
            if !field.nullable {
                return Err(ApiError::BadRequest(format!(
                    "cursor contains null for non-nullable field '{}'",
                    field.expression()
                )));
            }
            Ok(format!("{} IS NULL", field.expression()))
        }
        _ => Ok(format!(
            "{} = {}",
            field.expression(),
            cursor_literal_sql(field, value)?
        )),
    }
}

fn cursor_after_sql<T>(
    field: &CursorSqlField<T>,
    sort: &SortParam,
    value: &CursorValue,
) -> Result<String, ApiError>
where
    T: AsRef<str>,
{
    match value {
        CursorValue::Null => {
            if !field.nullable {
                return Err(ApiError::BadRequest(format!(
                    "cursor contains null for non-nullable field '{}'",
                    field.expression()
                )));
            }

            if sort.descending {
                Ok("FALSE".to_string())
            } else {
                Ok(format!("{} IS NOT NULL", field.expression()))
            }
        }
        _ => {
            let literal = cursor_literal_sql(field, value)?;
            if field.nullable && sort.descending {
                Ok(format!(
                    "({} < {} OR {} IS NULL)",
                    field.expression(),
                    literal,
                    field.expression()
                ))
            } else {
                let operator = if sort.descending { "<" } else { ">" };
                Ok(format!("{} {} {}", field.expression(), operator, literal))
            }
        }
    }
}

fn cursor_literal_sql<T>(field: &CursorSqlField<T>, value: &CursorValue) -> Result<String, ApiError>
where
    T: AsRef<str>,
{
    match (field.sql_type, value) {
        (_, CursorValue::Null) => Err(ApiError::BadRequest(format!(
            "cursor contains null for field '{}'",
            field.expression()
        ))),
        (CursorSqlType::Integer, CursorValue::Integer(value)) => Ok(value.to_string()),
        (CursorSqlType::Numeric, CursorValue::Decimal(value)) => {
            let value =
                hubuum_computed_fields::canonical_decimal_string(value).ok_or_else(|| {
                    ApiError::BadRequest("cursor contains an invalid decimal value".to_string())
                })?;
            Ok(format!("{value}::numeric"))
        }
        (CursorSqlType::Boolean, CursorValue::Boolean(value)) => Ok(value.to_string()),
        (CursorSqlType::String, CursorValue::String(value)) => {
            validate_cursor_string(value)?;
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
        (CursorSqlType::Json, CursorValue::Json(value)) => {
            validate_postgres_jsonb_cursor_value(value)?;
            Ok(format!(
                "'{}'::jsonb",
                serde_json::to_string(value)
                    .map_err(ApiError::from)?
                    .replace('\'', "''")
            ))
        }
        _ => Err(ApiError::BadRequest(format!(
            "cursor value does not match expected type for '{}'",
            field.expression()
        ))),
    }
}

fn validate_postgres_jsonb_cursor_value(value: &serde_json::Value) -> Result<(), ApiError> {
    match crate::db::json::validate_postgres_jsonb_value(value) {
        Ok(()) => Ok(()),
        Err(crate::db::json::PostgresJsonbValidationError::UnsupportedValue) => {
            Err(invalid_postgres_jsonb_cursor())
        }
        Err(crate::db::json::PostgresJsonbValidationError::NestingTooDeep) => {
            Err(ApiError::BadRequest(format!(
                "cursor JSON exceeds the maximum nesting depth of {}",
                crate::db::json::MAX_POSTGRES_JSONB_NESTING_DEPTH
            )))
        }
    }
}

fn invalid_postgres_jsonb_cursor() -> ApiError {
    ApiError::BadRequest("cursor contains JSON that PostgreSQL JSONB cannot represent".to_string())
}

#[cfg(test)]
const MAX_JSON_CURSOR_NESTING_DEPTH: usize = crate::db::json::MAX_POSTGRES_JSONB_NESTING_DEPTH;

#[macro_export]
macro_rules! apply_cursor_ordering_fields {
    ($query:ident, $sorts:expr, $sql_fields:expr) => {{
        use diesel::dsl::sql;
        use diesel::sql_types::{Array, Bool, Integer, Jsonb, Nullable, Numeric, Text, Timestamp};

        let mut is_first_order = true;
        for (sort, sql_field) in $sorts.iter().zip($sql_fields.iter()) {
            let order_sql = $crate::pagination::order_sql_clause_for_field(sort, sql_field);

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
                (true, $crate::pagination::CursorSqlType::Numeric, false) => {
                    $query.order_by(sql::<Numeric>(&order_sql))
                }
                (false, $crate::pagination::CursorSqlType::Numeric, false) => {
                    $query.then_order_by(sql::<Numeric>(&order_sql))
                }
                (true, $crate::pagination::CursorSqlType::Numeric, true) => {
                    $query.order_by(sql::<Nullable<Numeric>>(&order_sql))
                }
                (false, $crate::pagination::CursorSqlType::Numeric, true) => {
                    $query.then_order_by(sql::<Nullable<Numeric>>(&order_sql))
                }
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
                (true, $crate::pagination::CursorSqlType::Json, false) => {
                    $query.order_by(sql::<Jsonb>(&order_sql))
                }
                (false, $crate::pagination::CursorSqlType::Json, false) => {
                    $query.then_order_by(sql::<Jsonb>(&order_sql))
                }
                (true, $crate::pagination::CursorSqlType::Json, true) => {
                    $query.order_by(sql::<Nullable<Jsonb>>(&order_sql))
                }
                (false, $crate::pagination::CursorSqlType::Json, true) => {
                    $query.then_order_by(sql::<Nullable<Jsonb>>(&order_sql))
                }
            };

            is_first_order = false;
        }
    }};
}

#[macro_export]
macro_rules! apply_query_options_with_fields {
    ($query:ident, $query_options:expr, $sql_fields:expr) => {{
        let query_options = &$query_options;

        if let Some(cursor_sql) = $crate::pagination::cursor_filter_sql_for_fields(
            &query_options.sort,
            &$sql_fields,
            query_options.cursor.as_deref(),
        )? {
            $query = $query.filter(diesel::dsl::sql::<diesel::sql_types::Bool>(&cursor_sql));
        }

        $crate::apply_cursor_ordering_fields!($query, query_options.sort, $sql_fields);

        if let Some(limit) = query_options.limit {
            $query = $query.limit(limit as i64);
        }
    }};
}

#[macro_export]
macro_rules! apply_query_options {
    ($query:ident, $query_options:expr, $ty:ty) => {{
        let query_options = &$query_options;
        let sql_fields = query_options
            .sort
            .iter()
            .map(|sort| $crate::pagination::cursor_sql_field::<$ty>(&sort.field))
            .collect::<Result<Vec<_>, $crate::errors::ApiError>>()?;
        $crate::apply_query_options_with_fields!($query, query_options, sql_fields);
    }};
}

#[cfg(test)]
mod tests;
