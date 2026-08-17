//! PostgreSQL cursor predicate and ordering construction.
//!
//! The opaque token and its values are backend-neutral and live in
//! `hubuum-query`. This module owns the PostgreSQL-specific mapping from those
//! values to typed SQL expressions.

use hubuum_domain::{
    MAX_STORAGE_JSON_NESTING_DEPTH, StorageJsonValidationError, validate_storage_json_value,
};
use hubuum_query::{CursorCodecError, CursorValue, SortParam};

use crate::PostgresStorageError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorSqlType {
    Integer,
    BigInt,
    Numeric,
    Boolean,
    String,
    DateTime,
    IntegerArray,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CursorSqlField<T = &'static str> {
    pub column: T,
    pub sql_type: CursorSqlType,
    pub nullable: bool,
}

impl<T> CursorSqlField<T>
where
    T: AsRef<str>,
{
    pub fn expression(&self) -> &str {
        self.column.as_ref()
    }
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
    format!("{} {direction}{nulls}", field.expression())
}

pub fn cursor_filter_sql_for_fields<T>(
    sorts: &[SortParam],
    fields: &[CursorSqlField<T>],
    cursor: Option<&str>,
) -> Result<Option<String>, PostgresStorageError>
where
    T: AsRef<str>,
{
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    if fields.len() != sorts.len() {
        return Err(PostgresStorageError::database(
            "Cursor SQL field count does not match sort count",
        ));
    }
    let values = hubuum_query::decode_cursor_values(cursor, sorts).map_err(cursor_codec_error)?;
    for (field, value) in fields.iter().zip(&values) {
        validate_cursor_value(field, value)?;
    }

    let mut clauses = Vec::with_capacity(sorts.len());
    for current_index in 0..sorts.len() {
        let mut clause_parts = Vec::with_capacity(current_index + 1);
        for prefix_index in 0..current_index {
            clause_parts.push(cursor_equality_sql(
                &fields[prefix_index],
                &values[prefix_index],
            )?);
        }
        clause_parts.push(cursor_after_sql(
            &fields[current_index],
            &sorts[current_index],
            &values[current_index],
        )?);
        clauses.push(format!("({})", clause_parts.join(" AND ")));
    }
    Ok(Some(format!("({})", clauses.join(" OR "))))
}

fn cursor_codec_error(error: CursorCodecError) -> PostgresStorageError {
    match error {
        CursorCodecError::Invalid(message) => PostgresStorageError::bad_request(message),
        CursorCodecError::Encoding(message) => PostgresStorageError::database(message),
    }
}

fn validate_cursor_value<T>(
    field: &CursorSqlField<T>,
    value: &CursorValue,
) -> Result<(), PostgresStorageError>
where
    T: AsRef<str>,
{
    match value {
        CursorValue::Null if field.nullable => Ok(()),
        CursorValue::Null => Err(PostgresStorageError::bad_request(format!(
            "cursor contains null for non-nullable field '{}'",
            field.expression()
        ))),
        _ => cursor_literal_sql(field, value).map(|_| ()),
    }
}

fn cursor_equality_sql<T>(
    field: &CursorSqlField<T>,
    value: &CursorValue,
) -> Result<String, PostgresStorageError>
where
    T: AsRef<str>,
{
    match value {
        CursorValue::Null if field.nullable => Ok(format!("{} IS NULL", field.expression())),
        CursorValue::Null => Err(PostgresStorageError::bad_request(format!(
            "cursor contains null for non-nullable field '{}'",
            field.expression()
        ))),
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
) -> Result<String, PostgresStorageError>
where
    T: AsRef<str>,
{
    match value {
        CursorValue::Null if !field.nullable => Err(PostgresStorageError::bad_request(format!(
            "cursor contains null for non-nullable field '{}'",
            field.expression()
        ))),
        CursorValue::Null if sort.descending => Ok("FALSE".to_string()),
        CursorValue::Null => Ok(format!("{} IS NOT NULL", field.expression())),
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
                Ok(format!("{} {operator} {literal}", field.expression()))
            }
        }
    }
}

fn cursor_literal_sql<T>(
    field: &CursorSqlField<T>,
    value: &CursorValue,
) -> Result<String, PostgresStorageError>
where
    T: AsRef<str>,
{
    match (field.sql_type, value) {
        (_, CursorValue::Null) => Err(PostgresStorageError::bad_request(format!(
            "cursor contains null for field '{}'",
            field.expression()
        ))),
        (CursorSqlType::Integer | CursorSqlType::BigInt, CursorValue::Integer(value)) => {
            Ok(value.to_string())
        }
        (CursorSqlType::Numeric, CursorValue::Decimal(value)) => Ok(format!("{value}::numeric")),
        (CursorSqlType::Boolean, CursorValue::Boolean(value)) => Ok(value.to_string()),
        (CursorSqlType::String, CursorValue::String(value)) => {
            Ok(format!("'{}'", value.replace('\'', "''")))
        }
        (CursorSqlType::DateTime, CursorValue::DateTime(value)) => Ok(format!(
            "'{}'::timestamp",
            value.format("%Y-%m-%d %H:%M:%S%.f")
        )),
        (CursorSqlType::IntegerArray, CursorValue::IntegerArray(values)) if values.is_empty() => {
            Ok("ARRAY[]::integer[]".to_string())
        }
        (CursorSqlType::IntegerArray, CursorValue::IntegerArray(values)) => Ok(format!(
            "ARRAY[{}]::integer[]",
            values
                .iter()
                .map(std::string::ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        )),
        (CursorSqlType::Json, CursorValue::Json(value)) => {
            validate_postgres_jsonb_cursor_value(value)?;
            let value = serde_json::to_string(value).map_err(|error| {
                PostgresStorageError::database(format!("Unable to serialize JSON cursor: {error}"))
            })?;
            Ok(format!("'{}'::jsonb", value.replace('\'', "''")))
        }
        _ => Err(PostgresStorageError::bad_request(format!(
            "cursor value does not match expected type for '{}'",
            field.expression()
        ))),
    }
}

fn validate_postgres_jsonb_cursor_value(
    value: &serde_json::Value,
) -> Result<(), PostgresStorageError> {
    match validate_storage_json_value(value) {
        Ok(()) => Ok(()),
        Err(StorageJsonValidationError::UnsupportedValue) => {
            Err(PostgresStorageError::bad_request(
                "cursor contains JSON that PostgreSQL JSONB cannot represent",
            ))
        }
        Err(StorageJsonValidationError::NestingTooDeep) => {
            Err(PostgresStorageError::bad_request(format!(
                "cursor JSON exceeds the maximum nesting depth of {MAX_STORAGE_JSON_NESTING_DEPTH}"
            )))
        }
    }
}

#[macro_export]
macro_rules! apply_cursor_ordering_fields {
    ($query:ident, $sorts:expr, $sql_fields:expr) => {{
        use diesel::dsl::sql;
        use diesel::sql_types::{
            Array, BigInt, Bool, Integer, Jsonb, Nullable, Numeric, Text, Timestamp,
        };

        let mut is_first_order = true;
        for (sort, sql_field) in $sorts.iter().zip($sql_fields.iter()) {
            let order_sql = $crate::cursor::order_sql_clause_for_field(sort, sql_field);
            $query = match (is_first_order, sql_field.sql_type, sql_field.nullable) {
                (true, $crate::cursor::CursorSqlType::Integer, false) => {
                    $query.order_by(sql::<Integer>(&order_sql))
                }
                (false, $crate::cursor::CursorSqlType::Integer, false) => {
                    $query.then_order_by(sql::<Integer>(&order_sql))
                }
                (true, $crate::cursor::CursorSqlType::Integer, true) => {
                    $query.order_by(sql::<Nullable<Integer>>(&order_sql))
                }
                (false, $crate::cursor::CursorSqlType::Integer, true) => {
                    $query.then_order_by(sql::<Nullable<Integer>>(&order_sql))
                }
                (true, $crate::cursor::CursorSqlType::BigInt, false) => {
                    $query.order_by(sql::<BigInt>(&order_sql))
                }
                (false, $crate::cursor::CursorSqlType::BigInt, false) => {
                    $query.then_order_by(sql::<BigInt>(&order_sql))
                }
                (true, $crate::cursor::CursorSqlType::BigInt, true) => {
                    $query.order_by(sql::<Nullable<BigInt>>(&order_sql))
                }
                (false, $crate::cursor::CursorSqlType::BigInt, true) => {
                    $query.then_order_by(sql::<Nullable<BigInt>>(&order_sql))
                }
                (true, $crate::cursor::CursorSqlType::Numeric, false) => {
                    $query.order_by(sql::<Numeric>(&order_sql))
                }
                (false, $crate::cursor::CursorSqlType::Numeric, false) => {
                    $query.then_order_by(sql::<Numeric>(&order_sql))
                }
                (true, $crate::cursor::CursorSqlType::Numeric, true) => {
                    $query.order_by(sql::<Nullable<Numeric>>(&order_sql))
                }
                (false, $crate::cursor::CursorSqlType::Numeric, true) => {
                    $query.then_order_by(sql::<Nullable<Numeric>>(&order_sql))
                }
                (true, $crate::cursor::CursorSqlType::Boolean, false) => {
                    $query.order_by(sql::<Bool>(&order_sql))
                }
                (false, $crate::cursor::CursorSqlType::Boolean, false) => {
                    $query.then_order_by(sql::<Bool>(&order_sql))
                }
                (true, $crate::cursor::CursorSqlType::Boolean, true) => {
                    $query.order_by(sql::<Nullable<Bool>>(&order_sql))
                }
                (false, $crate::cursor::CursorSqlType::Boolean, true) => {
                    $query.then_order_by(sql::<Nullable<Bool>>(&order_sql))
                }
                (true, $crate::cursor::CursorSqlType::String, false) => {
                    $query.order_by(sql::<Text>(&order_sql))
                }
                (false, $crate::cursor::CursorSqlType::String, false) => {
                    $query.then_order_by(sql::<Text>(&order_sql))
                }
                (true, $crate::cursor::CursorSqlType::String, true) => {
                    $query.order_by(sql::<Nullable<Text>>(&order_sql))
                }
                (false, $crate::cursor::CursorSqlType::String, true) => {
                    $query.then_order_by(sql::<Nullable<Text>>(&order_sql))
                }
                (true, $crate::cursor::CursorSqlType::DateTime, false) => {
                    $query.order_by(sql::<Timestamp>(&order_sql))
                }
                (false, $crate::cursor::CursorSqlType::DateTime, false) => {
                    $query.then_order_by(sql::<Timestamp>(&order_sql))
                }
                (true, $crate::cursor::CursorSqlType::DateTime, true) => {
                    $query.order_by(sql::<Nullable<Timestamp>>(&order_sql))
                }
                (false, $crate::cursor::CursorSqlType::DateTime, true) => {
                    $query.then_order_by(sql::<Nullable<Timestamp>>(&order_sql))
                }
                (true, $crate::cursor::CursorSqlType::IntegerArray, false) => {
                    $query.order_by(sql::<Array<Integer>>(&order_sql))
                }
                (false, $crate::cursor::CursorSqlType::IntegerArray, false) => {
                    $query.then_order_by(sql::<Array<Integer>>(&order_sql))
                }
                (true, $crate::cursor::CursorSqlType::IntegerArray, true) => {
                    $query.order_by(sql::<Array<Nullable<Integer>>>(&order_sql))
                }
                (false, $crate::cursor::CursorSqlType::IntegerArray, true) => {
                    $query.then_order_by(sql::<Array<Nullable<Integer>>>(&order_sql))
                }
                (true, $crate::cursor::CursorSqlType::Json, false) => {
                    $query.order_by(sql::<Jsonb>(&order_sql))
                }
                (false, $crate::cursor::CursorSqlType::Json, false) => {
                    $query.then_order_by(sql::<Jsonb>(&order_sql))
                }
                (true, $crate::cursor::CursorSqlType::Json, true) => {
                    $query.order_by(sql::<Nullable<Jsonb>>(&order_sql))
                }
                (false, $crate::cursor::CursorSqlType::Json, true) => {
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
        if let Some(cursor_sql) = $crate::cursor::cursor_filter_sql_for_fields(
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

#[cfg(test)]
mod tests {
    use hubuum_query::{FilterField, SortParam, encode_cursor_values};

    use super::{CursorSqlField, CursorSqlType, cursor_filter_sql_for_fields};

    #[test]
    fn cursor_predicate_preserves_nullable_descending_semantics() {
        let sorts = [SortParam {
            field: FilterField::Name,
            descending: true,
        }];
        let cursor = encode_cursor_values(
            &sorts,
            vec![hubuum_query::CursorValue::String("beta".to_string())],
        )
        .unwrap();
        let fields = [CursorSqlField {
            column: "resources.name",
            sql_type: CursorSqlType::String,
            nullable: true,
        }];

        let sql = cursor_filter_sql_for_fields(&sorts, &fields, Some(&cursor)).unwrap();

        assert_eq!(
            sql.as_deref(),
            Some("(((resources.name < 'beta' OR resources.name IS NULL)))")
        );
    }
}
