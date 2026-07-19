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

/// Apply in-memory pagination after validating cursor values against the
/// resolved SQL field types used by the equivalent database-backed query.
pub fn paginate_in_memory_with_fields<T>(
    items: Vec<T>,
    query_options: &QueryOptions,
    fields: &[OwnedCursorSqlField],
) -> Result<Vec<T>, ApiError>
where
    T: CursorPaginated,
{
    let sorts = normalized_sorts::<T>(&query_options.sort)?;
    if fields.len() != sorts.len() {
        return Err(ApiError::InternalServerError(
            "cursor SQL field count does not match sort count".to_string(),
        ));
    }
    let cursor_values = query_options
        .cursor
        .as_deref()
        .map(|cursor| decode_and_validate_cursor_values(cursor, &sorts, fields))
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedCursorSqlField {
    pub expression: String,
    pub sql_type: CursorSqlType,
    pub nullable: bool,
}

impl From<CursorSqlField> for OwnedCursorSqlField {
    fn from(field: CursorSqlField) -> Self {
        Self {
            expression: field.column.to_string(),
            sql_type: field.sql_type,
            nullable: field.nullable,
        }
    }
}

trait CursorSqlFieldView {
    fn expression(&self) -> &str;
    fn sql_type(&self) -> CursorSqlType;
    fn nullable(&self) -> bool;
}

impl CursorSqlFieldView for CursorSqlField {
    fn expression(&self) -> &str {
        self.column
    }

    fn sql_type(&self) -> CursorSqlType {
        self.sql_type
    }

    fn nullable(&self) -> bool {
        self.nullable
    }
}

impl CursorSqlFieldView for OwnedCursorSqlField {
    fn expression(&self) -> &str {
        &self.expression
    }

    fn sql_type(&self) -> CursorSqlType {
        self.sql_type
    }

    fn nullable(&self) -> bool {
        self.nullable
    }
}

pub fn order_sql_clause_for_field(sort: &SortParam, field: &OwnedCursorSqlField) -> String {
    order_sql_clause_for_expression(sort, field.expression(), field.nullable())
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
        field.nullable(),
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

pub fn cursor_filter_sql_for_fields(
    sorts: &[SortParam],
    fields: &[OwnedCursorSqlField],
    cursor: Option<&str>,
) -> Result<Option<String>, ApiError> {
    cursor_filter_sql_from_fields(sorts, fields, cursor)
}

fn cursor_filter_sql_from_fields<F>(
    sorts: &[SortParam],
    fields: &[F],
    cursor: Option<&str>,
) -> Result<Option<String>, ApiError>
where
    F: CursorSqlFieldView,
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

fn decode_and_validate_cursor_values<F>(
    cursor: &str,
    sorts: &[SortParam],
    fields: &[F],
) -> Result<Vec<CursorValue>, ApiError>
where
    F: CursorSqlFieldView,
{
    let cursor_values = decode_cursor_values(cursor, sorts)?;
    for (field, value) in fields.iter().zip(&cursor_values) {
        validate_cursor_value(field, value)?;
    }
    Ok(cursor_values)
}

fn validate_cursor_value<F>(field: &F, value: &CursorValue) -> Result<(), ApiError>
where
    F: CursorSqlFieldView,
{
    match value {
        CursorValue::Null if field.nullable() => Ok(()),
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
    }

    Ok(token)
}

fn ensure_cursor_within_limit(cursor: &str) -> Result<(), ApiError> {
    if cursor.len() > MAX_ENCODED_CURSOR_BYTES {
        return Err(cursor_too_large());
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

fn cursor_equality_sql<F>(field: &F, value: &CursorValue) -> Result<String, ApiError>
where
    F: CursorSqlFieldView,
{
    match value {
        CursorValue::Null => {
            if !field.nullable() {
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

fn cursor_after_sql<F>(field: &F, sort: &SortParam, value: &CursorValue) -> Result<String, ApiError>
where
    F: CursorSqlFieldView,
{
    match value {
        CursorValue::Null => {
            if !field.nullable() {
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
            if field.nullable() && sort.descending {
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

fn cursor_literal_sql<F>(field: &F, value: &CursorValue) -> Result<String, ApiError>
where
    F: CursorSqlFieldView,
{
    match (field.sql_type(), value) {
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

const POSTGRES_NUMERIC_MAX_INTEGRAL_DIGITS: i64 = 131_072;
const POSTGRES_NUMERIC_MAX_FRACTIONAL_DIGITS: i64 = 16_383;
const POSTGRES_NUMERIC_MAX_EXPONENT_ABS: i64 = i32::MAX as i64 / 2;
const MAX_JSON_CURSOR_NESTING_DEPTH: usize = 64;

fn validate_postgres_jsonb_cursor_value(value: &serde_json::Value) -> Result<(), ApiError> {
    let mut pending = vec![(value, 0_usize)];
    while let Some((value, depth)) = pending.pop() {
        match value {
            serde_json::Value::String(value) if value.contains('\0') => {
                return Err(invalid_postgres_jsonb_cursor());
            }
            serde_json::Value::Number(value) if !postgres_numeric_can_represent(value) => {
                return Err(invalid_postgres_jsonb_cursor());
            }
            serde_json::Value::Array(values) => {
                ensure_json_cursor_container_depth(depth)?;
                pending.extend(values.iter().map(|value| (value, depth + 1)));
            }
            serde_json::Value::Object(values) => {
                ensure_json_cursor_container_depth(depth)?;
                for (key, value) in values {
                    if key.contains('\0') {
                        return Err(invalid_postgres_jsonb_cursor());
                    }
                    pending.push((value, depth + 1));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn ensure_json_cursor_container_depth(depth: usize) -> Result<(), ApiError> {
    if depth >= MAX_JSON_CURSOR_NESTING_DEPTH {
        return Err(ApiError::BadRequest(format!(
            "cursor JSON exceeds the maximum nesting depth of {MAX_JSON_CURSOR_NESTING_DEPTH}"
        )));
    }
    Ok(())
}

fn invalid_postgres_jsonb_cursor() -> ApiError {
    ApiError::BadRequest("cursor contains JSON that PostgreSQL JSONB cannot represent".to_string())
}

fn postgres_numeric_can_represent(value: &serde_json::Number) -> bool {
    // PostgreSQL strips leading zero groups when determining numeric weight,
    // but retains the input scale after applying any exponent.
    let source = value.to_string();
    let unsigned = source.strip_prefix('-').unwrap_or(&source);
    let exponent_start = unsigned.find(['e', 'E']);
    let (mantissa, exponent) = match exponent_start {
        Some(index) => {
            let Ok(exponent) = unsigned[index + 1..].parse::<i64>() else {
                return false;
            };
            (&unsigned[..index], exponent)
        }
        None => (unsigned, 0),
    };
    if !(-POSTGRES_NUMERIC_MAX_EXPONENT_ABS..=POSTGRES_NUMERIC_MAX_EXPONENT_ABS).contains(&exponent)
    {
        return false;
    }
    let integral_digits = mantissa.find('.').unwrap_or(mantissa.len());
    let total_digits = mantissa.len() - usize::from(mantissa.contains('.'));
    let first_nonzero = mantissa
        .bytes()
        .filter(|digit| *digit != b'.')
        .position(|digit| digit != b'0');
    let Ok(integral_digits) = i64::try_from(integral_digits) else {
        return false;
    };
    let Ok(total_digits) = i64::try_from(total_digits) else {
        return false;
    };
    let Some(decimal_position) = integral_digits.checked_add(exponent) else {
        return false;
    };
    let digits_before_decimal = match first_nonzero {
        Some(first_nonzero) => {
            let Ok(first_nonzero) = i64::try_from(first_nonzero) else {
                return false;
            };
            decimal_position.saturating_sub(first_nonzero).max(0)
        }
        None => 0,
    };
    let fractional_digits = total_digits - integral_digits;
    let digits_after_decimal = fractional_digits.saturating_sub(exponent).max(0);

    digits_before_decimal <= POSTGRES_NUMERIC_MAX_INTEGRAL_DIGITS
        && digits_after_decimal <= POSTGRES_NUMERIC_MAX_FRACTIONAL_DIGITS
}

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
macro_rules! apply_cursor_ordering {
    ($query:ident, $sorts:expr, $ty:ty) => {{
        use diesel::dsl::sql;
        use diesel::sql_types::{Array, Bool, Integer, Jsonb, Nullable, Numeric, Text, Timestamp};

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
    use rstest::rstest;

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
    fn cursor_encoding_rejects_an_oversized_token() {
        let error = finalize_page(
            vec![
                collection(1, &"a".repeat(MAX_ENCODED_CURSOR_BYTES)),
                collection(2, "z"),
            ],
            &QueryOptions {
                filters: vec![],
                sort: vec![SortParam {
                    field: FilterField::Name,
                    descending: false,
                }],
                limit: Some(1),
                cursor: None,
                include_total: true,
            },
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            format!(
                "pagination cursor exceeds the maximum encoded size of {MAX_ENCODED_CURSOR_BYTES} bytes; use smaller sort values"
            )
        );
    }

    #[test]
    fn cursor_decoding_rejects_an_oversized_token_before_parsing() {
        let sort = SortParam {
            field: FilterField::Id,
            descending: false,
        };

        let error =
            decode_cursor_values(&"a".repeat(MAX_ENCODED_CURSOR_BYTES + 1), &[sort]).unwrap_err();

        assert_eq!(
            error.to_string(),
            format!(
                "pagination cursor exceeds the maximum encoded size of {MAX_ENCODED_CURSOR_BYTES} bytes; use smaller sort values"
            )
        );
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

    fn encoded_cursor(sort: &SortParam, value: CursorValue) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&CursorToken {
                sorts: vec![CursorSort {
                    field: sort.field.to_string(),
                    descending: sort.descending,
                }],
                values: vec![value],
            })
            .unwrap(),
        )
    }

    #[test]
    fn in_memory_cursor_rejects_a_value_with_the_wrong_resolved_type() {
        let sort = SortParam {
            field: FilterField::Id,
            descending: false,
        };
        let fields = [OwnedCursorSqlField {
            expression: "computed_value".to_string(),
            sql_type: CursorSqlType::Boolean,
            nullable: false,
        }];
        let cursor = encoded_cursor(&sort, CursorValue::String("true".to_string()));
        let query_options = QueryOptions {
            filters: vec![],
            sort: vec![sort],
            limit: Some(2),
            cursor: Some(cursor),
            include_total: true,
        };

        let error =
            paginate_in_memory_with_fields(Vec::<Collection>::new(), &query_options, &fields)
                .unwrap_err();

        assert_eq!(
            error,
            ApiError::BadRequest(
                "cursor value does not match expected type for 'computed_value'".to_string()
            )
        );
    }

    #[rstest]
    #[case::nul_string(r#"{"value":"\u0000"}"#)]
    #[case::nul_key(r#"{"\u0000":true}"#)]
    #[case::integral_overflow(r#"{"value":1e131072}"#)]
    #[case::fractional_overflow(r#"{"value":1e-16384}"#)]
    fn in_memory_json_cursor_rejects_values_postgres_jsonb_cannot_represent(#[case] json: &str) {
        let sort = SortParam {
            field: FilterField::Id,
            descending: false,
        };
        let fields = [OwnedCursorSqlField {
            expression: "computed_value".to_string(),
            sql_type: CursorSqlType::Json,
            nullable: false,
        }];
        let value = serde_json::from_str(json).unwrap();
        let cursor = encoded_cursor(&sort, CursorValue::Json(value));
        let query_options = QueryOptions {
            filters: vec![],
            sort: vec![sort],
            limit: Some(2),
            cursor: Some(cursor),
            include_total: true,
        };

        let error =
            paginate_in_memory_with_fields(Vec::<Collection>::new(), &query_options, &fields)
                .unwrap_err();

        assert_eq!(
            error,
            ApiError::BadRequest(
                "cursor contains JSON that PostgreSQL JSONB cannot represent".to_string()
            )
        );
    }

    #[test]
    fn cursor_decoding_rejects_a_mismatched_value_count() {
        let sort = SortParam {
            field: FilterField::Id,
            descending: false,
        };
        let cursor = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&CursorToken {
                sorts: vec![CursorSort {
                    field: sort.field.to_string(),
                    descending: sort.descending,
                }],
                values: vec![],
            })
            .unwrap(),
        );

        let error = decode_cursor_values(&cursor, &[sort]).unwrap_err();

        assert_eq!(
            error,
            ApiError::BadRequest(
                "cursor value count does not match current sort order".to_string()
            )
        );
    }

    #[test]
    fn numeric_cursor_sql_uses_a_canonical_decimal_literal() {
        let sort = SortParam {
            field: FilterField::Id,
            descending: false,
        };
        let fields = [OwnedCursorSqlField {
            expression: "computed_value".to_string(),
            sql_type: CursorSqlType::Numeric,
            nullable: false,
        }];
        let cursor = encoded_cursor(&sort, CursorValue::Decimal("1_".to_string()));

        let sql = cursor_filter_sql_for_fields(&[sort], &fields, Some(&cursor)).unwrap();

        assert_eq!(sql.as_deref(), Some("((computed_value > 1::numeric))"));
    }

    #[test]
    fn numeric_cursor_sql_rejects_a_decimal_outside_evaluator_bounds() {
        let sort = SortParam {
            field: FilterField::Id,
            descending: false,
        };
        let fields = [OwnedCursorSqlField {
            expression: "computed_value".to_string(),
            sql_type: CursorSqlType::Numeric,
            nullable: false,
        }];
        let cursor = encoded_cursor(&sort, CursorValue::Decimal("1e200000".to_string()));

        let error = cursor_filter_sql_for_fields(&[sort], &fields, Some(&cursor)).unwrap_err();

        assert_eq!(
            error.to_string(),
            "cursor contains an invalid decimal value"
        );
    }

    #[rstest]
    #[case::nul_string(r#"{"value":"\u0000"}"#)]
    #[case::nul_key(r#"{"\u0000":true}"#)]
    #[case::integral_overflow(r#"{"value":1e131072}"#)]
    #[case::fractional_overflow(r#"{"value":1e-16384}"#)]
    fn json_cursor_sql_rejects_values_postgres_jsonb_cannot_represent(#[case] json: &str) {
        let sort = SortParam {
            field: FilterField::Id,
            descending: false,
        };
        let fields = [OwnedCursorSqlField {
            expression: "computed_value".to_string(),
            sql_type: CursorSqlType::Json,
            nullable: false,
        }];
        let value = serde_json::from_str(json).unwrap();
        let cursor = encoded_cursor(&sort, CursorValue::Json(value));

        let error = cursor_filter_sql_for_fields(&[sort], &fields, Some(&cursor)).unwrap_err();

        assert_eq!(
            error,
            ApiError::BadRequest(
                "cursor contains JSON that PostgreSQL JSONB cannot represent".to_string()
            )
        );
    }

    #[rstest]
    #[case::maximum_integral_digits(r#"{"value":1e131071}"#)]
    #[case::normalized_maximum_integral_digits(r#"{"value":0.1e131072}"#)]
    #[case::maximum_fractional_digits(r#"{"value":1e-16383}"#)]
    fn json_cursor_sql_accepts_postgres_numeric_boundaries(#[case] json: &str) {
        let sort = SortParam {
            field: FilterField::Id,
            descending: false,
        };
        let fields = [OwnedCursorSqlField {
            expression: "computed_value".to_string(),
            sql_type: CursorSqlType::Json,
            nullable: false,
        }];
        let value = serde_json::from_str(json).unwrap();
        let cursor = encoded_cursor(&sort, CursorValue::Json(value));

        let sql = cursor_filter_sql_for_fields(&[sort], &fields, Some(&cursor)).unwrap();

        assert!(sql.is_some());
    }

    fn nested_json_arrays(depth: usize) -> serde_json::Value {
        (0..depth).fold(serde_json::Value::Null, |value, _| {
            serde_json::Value::Array(vec![value])
        })
    }

    #[test]
    fn json_cursor_accepts_the_maximum_nesting_depth() {
        let value = nested_json_arrays(MAX_JSON_CURSOR_NESTING_DEPTH);

        validate_postgres_jsonb_cursor_value(&value).unwrap();
    }

    #[test]
    fn json_cursor_rejects_nesting_above_the_maximum() {
        let value = nested_json_arrays(MAX_JSON_CURSOR_NESTING_DEPTH + 1);

        let error = validate_postgres_jsonb_cursor_value(&value).unwrap_err();

        assert_eq!(
            error,
            ApiError::BadRequest(format!(
                "cursor JSON exceeds the maximum nesting depth of {MAX_JSON_CURSOR_NESTING_DEPTH}"
            ))
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
