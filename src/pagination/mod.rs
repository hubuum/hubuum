use std::cmp::Ordering;
use std::collections::HashMap;

#[cfg(test)]
use base64::Engine as _;
#[cfg(test)]
use serde::{Deserialize, Serialize};

use crate::config::{DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT, get_config};
use crate::errors::ApiError;
use crate::models::search::{FilterField, QueryOptions, SortParam};
pub use crate::traits::pagination::{CursorPaginated, CursorValue};
pub use hubuum_query::MAX_ENCODED_CURSOR_BYTES;
pub use hubuum_storage_postgres::cursor::{CursorSqlField, CursorSqlType};

// These mirrors exist only so application compatibility tests can construct
// deliberately malformed tokens without exposing the codec representation.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CursorToken {
    sorts: Vec<CursorSort>,
    values: Vec<CursorValue>,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CursorSort {
    field: String,
    descending: bool,
}

pub trait CursorSqlMapping: CursorPaginated {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError>;
}

pub const NEXT_CURSOR_HEADER: &str = "X-Next-Cursor";
pub const PAGE_LIMIT_HEADER: &str = "X-Page-Limit";
pub const TOTAL_COUNT_HEADER: &str = "X-Total-Count";
pub const SKIPPED_TOTAL_COUNT: i64 = -1;

pub async fn exact_count_or_skipped(
    query_options: &QueryOptions,
    count: impl AsyncFnOnce() -> Result<i64, ApiError>,
) -> Result<i64, ApiError> {
    if query_options.include_total() {
        count().await
    } else {
        Ok(SKIPPED_TOTAL_COUNT)
    }
}

pub fn known_count_or_skipped(query_options: &QueryOptions, count: i64) -> i64 {
    if query_options.include_total() {
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
    page_limits()?.resolve(query_options.limit())
}

pub fn prepare_db_pagination<T>(query_options: &QueryOptions) -> Result<QueryOptions, ApiError>
where
    T: CursorPaginated,
{
    let limit = page_limits()?.resolve(query_options.limit())?;
    let sorts = normalized_sorts::<T>(query_options.sort())?;

    if let Some(cursor) = query_options.cursor() {
        let _ = decode_cursor_values(cursor, &sorts)?;
    }

    let mut prepared = query_options.clone();
    let mut requested_sorts = sorts;
    let tie_breaker = (requested_sorts.len() > hubuum_query::MAX_QUERY_SORT_FIELDS)
        .then(|| requested_sorts.pop())
        .flatten();
    prepared.set_sort(requested_sorts.try_into()?);
    if let Some(tie_breaker) = tie_breaker {
        prepared.sort_mut().append_tie_breaker(tie_breaker)?;
    }
    prepared.set_limit(Some(limit.saturating_add(1)));
    Ok(prepared)
}

pub fn count_query_options(query_options: &QueryOptions) -> QueryOptions {
    let mut prepared = query_options.clone();
    prepared.set_sort(Default::default());
    prepared.set_limit(None);
    prepared.clear_cursor();
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
    let sorts = normalized_sorts::<T>(query_options.sort())?;
    let cursor_values = query_options
        .cursor()
        .map(|cursor| cursor.as_str())
        .map(|cursor| decode_cursor_values(cursor, &sorts))
        .transpose()?;
    paginate_in_memory_with_values(items, query_options, &sorts, cursor_values.as_deref())
}

fn paginate_in_memory_with_values<T>(
    items: Vec<T>,
    query_options: &QueryOptions,
    sorts: &[SortParam],
    cursor_values: Option<&[CursorValue]>,
) -> Result<Vec<T>, ApiError>
where
    T: CursorPaginated,
{
    let mut keyed_items = items
        .into_iter()
        .map(|item| {
            let values = sorts
                .iter()
                .map(|sort| item.cursor_value(&sort.field))
                .collect::<Result<Vec<_>, _>>()?;
            Ok((item, values))
        })
        .collect::<Result<Vec<_>, ApiError>>()?;

    keyed_items.sort_by(|(_, left), (_, right)| compare_cursor_values(left, right, sorts));

    if let Some(cursor_values) = cursor_values {
        keyed_items.retain(|(_, values)| {
            compare_cursor_values(values, cursor_values, sorts) == Ordering::Greater
        });
    }

    if let Some(limit) = query_options.limit() {
        keyed_items.truncate(limit);
    }
    Ok(keyed_items.into_iter().map(|(item, _)| item).collect())
}

fn compare_cursor_values(
    left: &[CursorValue],
    right: &[CursorValue],
    sorts: &[SortParam],
) -> Ordering {
    for ((left, right), sort) in left.iter().zip(right).zip(sorts) {
        let ordering = left.cmp(right);
        let ordering = if sort.descending {
            ordering.reverse()
        } else {
            ordering
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
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
        sorts: normalized_sorts::<T>(query_options.sort())?,
    })
}

pub fn cursor_sql_field<T>(field: &FilterField) -> Result<CursorSqlField, ApiError>
where
    T: CursorSqlMapping,
{
    T::sql_field(field)
}

pub fn order_sql_clause_for_field<T>(sort: &SortParam, field: &CursorSqlField<T>) -> String
where
    T: AsRef<str>,
{
    hubuum_storage_postgres::cursor::order_sql_clause_for_field(sort, field)
}

pub fn order_sql_clause<T>(sort: &SortParam) -> Result<String, ApiError>
where
    T: CursorSqlMapping,
{
    let field = cursor_sql_field::<T>(&sort.field)?;
    Ok(hubuum_storage_postgres::cursor::order_sql_clause_for_field(
        sort, &field,
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

    cursor_filter_sql_for_fields(sorts, &fields, Some(cursor))
}

pub fn cursor_filter_sql_for_fields<T>(
    sorts: &[SortParam],
    fields: &[CursorSqlField<T>],
    cursor: Option<&str>,
) -> Result<Option<String>, ApiError>
where
    T: AsRef<str>,
{
    hubuum_storage_postgres::cursor::cursor_filter_sql_for_fields(sorts, fields, cursor)
        .map_err(|error| ApiError::from(crate::storage::StorageError::from(error)))
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
    let values = sorts
        .iter()
        .map(|sort| item.cursor_value(&sort.field))
        .collect::<Result<Vec<_>, _>>()?;
    for value in &values {
        if let CursorValue::Json(value) = value {
            validate_postgres_jsonb_cursor_value(value)?;
        }
    }
    hubuum_query::encode_cursor_values(sorts, values).map_err(cursor_codec_error)
}

pub fn decode_cursor_values(
    cursor: &str,
    sorts: &[SortParam],
) -> Result<Vec<CursorValue>, ApiError> {
    hubuum_query::decode_cursor_values(cursor, sorts).map_err(cursor_codec_error)
}

fn cursor_codec_error(error: hubuum_query::CursorCodecError) -> ApiError {
    match error {
        hubuum_query::CursorCodecError::Invalid(message) => ApiError::BadRequest(message),
        hubuum_query::CursorCodecError::Encoding(message) => ApiError::InternalServerError(message),
    }
}

fn validate_postgres_jsonb_cursor_value(value: &serde_json::Value) -> Result<(), ApiError> {
    match hubuum_domain::validate_storage_json_value(value) {
        Ok(()) => Ok(()),
        Err(hubuum_domain::StorageJsonValidationError::UnsupportedValue) => {
            Err(invalid_postgres_jsonb_cursor())
        }
        Err(hubuum_domain::StorageJsonValidationError::NestingTooDeep) => {
            Err(ApiError::BadRequest(format!(
                "cursor JSON exceeds the maximum nesting depth of {}",
                hubuum_domain::MAX_STORAGE_JSON_NESTING_DEPTH
            )))
        }
    }
}

fn invalid_postgres_jsonb_cursor() -> ApiError {
    ApiError::BadRequest("cursor contains JSON that PostgreSQL JSONB cannot represent".to_string())
}

#[cfg(test)]
const MAX_JSON_CURSOR_NESTING_DEPTH: usize = hubuum_domain::MAX_STORAGE_JSON_NESTING_DEPTH;

#[macro_export]
macro_rules! apply_cursor_ordering_fields {
    ($query:ident, $sorts:expr, $sql_fields:expr) => {{
        use diesel::dsl::sql;
        use diesel::sql_types::{
            Array, BigInt, Bool, Integer, Jsonb, Nullable, Numeric, Text, Timestamp,
        };

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
                (true, $crate::pagination::CursorSqlType::BigInt, false) => {
                    $query.order_by(sql::<BigInt>(&order_sql))
                }
                (false, $crate::pagination::CursorSqlType::BigInt, false) => {
                    $query.then_order_by(sql::<BigInt>(&order_sql))
                }
                (true, $crate::pagination::CursorSqlType::BigInt, true) => {
                    $query.order_by(sql::<Nullable<BigInt>>(&order_sql))
                }
                (false, $crate::pagination::CursorSqlType::BigInt, true) => {
                    $query.then_order_by(sql::<Nullable<BigInt>>(&order_sql))
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
            query_options.sort(),
            &$sql_fields,
            query_options.cursor().map(|cursor| cursor.as_str()),
        )? {
            $query = $query.filter(diesel::dsl::sql::<diesel::sql_types::Bool>(&cursor_sql));
        }

        $crate::apply_cursor_ordering_fields!($query, query_options.sort(), $sql_fields);

        if let Some(limit) = query_options.limit() {
            $query = $query.limit(limit as i64);
        }
    }};
}

#[macro_export]
macro_rules! apply_query_options {
    ($query:ident, $query_options:expr, $ty:ty) => {{
        let query_options = &$query_options;
        let sql_fields = query_options
            .sort()
            .iter()
            .map(|sort| $crate::pagination::cursor_sql_field::<$ty>(&sort.field))
            .collect::<Result<Vec<_>, $crate::errors::ApiError>>()?;
        $crate::apply_query_options_with_fields!($query, query_options, sql_fields);
    }};
}

#[cfg(test)]
mod tests;
