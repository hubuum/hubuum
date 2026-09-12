//! PostgreSQL cursor predicate and ordering construction.
//!
//! The opaque token and its values are backend-neutral and live in
//! `hubuum-query`. This module owns the PostgreSQL-specific mapping from those
//! values to typed SQL expressions.

use hubuum_domain::{
    MAX_STORAGE_JSON_NESTING_DEPTH, StorageJsonValidationError, validate_storage_json_value,
};
use hubuum_query::{CursorCodecError, CursorValue, FilterField, QueryOptions, SortParam};

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

/// One adapter-owned unique sort projection used to make a page deterministic.
pub(crate) struct CursorTieBreaker<T = &'static str> {
    field: FilterField,
    descending: bool,
    sql_field: CursorSqlField<T>,
}

impl<T> CursorTieBreaker<T> {
    pub(crate) const fn new(
        field: FilterField,
        descending: bool,
        sql_field: CursorSqlField<T>,
    ) -> Self {
        Self {
            field,
            descending,
            sql_field,
        }
    }
}

pub(crate) fn normalize_query_options(
    mut options: QueryOptions,
    field: FilterField,
    descending: bool,
) -> Result<QueryOptions, PostgresStorageError> {
    if !options.sort().iter().any(|sort| sort.field == field) {
        options
            .sort_mut()
            .append_tie_breaker(SortParam { field, descending })
            .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?;
    }
    validated_query_limit(options.limit())?;
    Ok(options)
}

pub(crate) fn normalize_query_fields<T>(
    options: &QueryOptions,
    mut sql_fields: Vec<CursorSqlField<T>>,
    tie_breaker: CursorTieBreaker<T>,
) -> Result<(QueryOptions, Vec<CursorSqlField<T>>), PostgresStorageError> {
    let append_sql_field = !options
        .sort()
        .iter()
        .any(|sort| sort.field == tie_breaker.field);
    let options =
        normalize_query_options(options.clone(), tie_breaker.field, tie_breaker.descending)?;
    if append_sql_field {
        sql_fields.push(tie_breaker.sql_field);
    }
    Ok((options, sql_fields))
}

pub(crate) fn validated_query_limit(
    limit: Option<usize>,
) -> Result<Option<i64>, PostgresStorageError> {
    limit
        .map(|limit| {
            if limit == 0 {
                return Err(PostgresStorageError::invalid_input(
                    "query limit must be greater than zero",
                ));
            }
            i64::try_from(limit).map_err(|_| {
                PostgresStorageError::invalid_input("query limit exceeds the supported range")
            })
        })
        .transpose()
}

impl<T> CursorSqlField<T>
where
    T: AsRef<str>,
{
    pub fn expression(&self) -> &str {
        self.column.as_ref()
    }

    // Cursor comparisons also run in Rust when policy authorization assembles
    // a response page. Use byte ordering for both SQL ordering and predicates,
    // independently of the database/column locale. Ordinary filters retain
    // their configured collation.
    fn ordering_expression(&self) -> String {
        if self.sql_type == CursorSqlType::String {
            format!("({}) COLLATE \"C\"", self.expression())
        } else {
            self.expression().to_string()
        }
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
    format!("{} {direction}{nulls}", field.ordering_expression())
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
        CursorCodecError::Invalid(message) => PostgresStorageError::invalid_input(message),
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
        CursorValue::Null => Err(PostgresStorageError::invalid_input(format!(
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
        CursorValue::Null => Err(PostgresStorageError::invalid_input(format!(
            "cursor contains null for non-nullable field '{}'",
            field.expression()
        ))),
        _ => Ok(format!(
            "{} = {}",
            field.ordering_expression(),
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
        CursorValue::Null if !field.nullable => Err(PostgresStorageError::invalid_input(format!(
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
                    field.ordering_expression(),
                    literal,
                    field.expression()
                ))
            } else {
                let operator = if sort.descending { "<" } else { ">" };
                Ok(format!(
                    "{} {operator} {literal}",
                    field.ordering_expression()
                ))
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
        (_, CursorValue::Null) => Err(PostgresStorageError::invalid_input(format!(
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
        _ => Err(PostgresStorageError::invalid_input(format!(
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
            Err(PostgresStorageError::invalid_input(
                "cursor contains JSON that PostgreSQL JSONB cannot represent",
            ))
        }
        Err(StorageJsonValidationError::NestingTooDeep) => {
            Err(PostgresStorageError::invalid_input(format!(
                "cursor JSON exceeds the maximum nesting depth of {MAX_STORAGE_JSON_NESTING_DEPTH}"
            )))
        }
    }
}

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

macro_rules! apply_query_options_with_fields {
    ($query:ident, $query_options:expr, $sql_fields:expr, $tie_breaker:expr) => {{
        let (query_options, sql_fields) =
            $crate::cursor::normalize_query_fields(&$query_options, $sql_fields, $tie_breaker)?;
        if let Some(cursor_sql) = $crate::cursor::cursor_filter_sql_for_fields(
            query_options.sort(),
            &sql_fields,
            query_options.cursor().map(|cursor| cursor.as_str()),
        )? {
            $query = $query.filter(diesel::dsl::sql::<diesel::sql_types::Bool>(&cursor_sql));
        }
        $crate::apply_cursor_ordering_fields!($query, query_options.sort(), sql_fields);
        if let Some(limit) = $crate::cursor::validated_query_limit(query_options.limit())? {
            $query = $query.limit(limit);
        }
    }};
}

pub(crate) use {apply_cursor_ordering_fields, apply_query_options_with_fields};

#[cfg(test)]
mod tests {
    use hubuum_query::{FilterField, QueryOptions, SortParam, encode_cursor_values};

    use super::{
        CursorSqlField, CursorSqlType, CursorTieBreaker, cursor_filter_sql_for_fields,
        normalize_query_fields, validated_query_limit,
    };

    #[cfg(feature = "integration-test-support")]
    #[rstest::rstest]
    #[case::ascending(false)]
    #[case::descending(true)]
    #[tokio::test]
    async fn string_cursor_order_matches_rust_under_a_locale_collation(#[case] descending: bool) {
        use diesel::QueryableByName;
        use diesel::sql_types::{Integer, Nullable, Text};
        use diesel_async::RunQueryDsl;
        use hubuum_query::CursorValue;

        use super::order_sql_clause_for_field;
        use crate::test_support::integration_test_pool;
        use crate::with_connection;

        #[derive(QueryableByName)]
        struct Collation {
            #[diesel(sql_type = Text)]
            name: String,
        }
        #[derive(Debug, PartialEq, QueryableByName)]
        struct Row {
            #[diesel(sql_type = Integer)]
            id: i32,
            #[diesel(sql_type = Nullable<Text>)]
            name: Option<String>,
        }
        impl Row {
            fn value(&self) -> CursorValue {
                self.name
                    .clone()
                    .map_or(CursorValue::Null, CursorValue::String)
            }
        }

        let pool = integration_test_pool(1);
        with_connection(&pool, async |connection| {
            let collation = diesel::sql_query(
                "SELECT collname AS name FROM pg_collation WHERE collname IN \
                 ('en-x-icu', 'en-US-x-icu', 'en_US.utf8', 'en_US.UTF-8', 'en_US') \
                 ORDER BY collname LIMIT 1",
            )
            .get_result::<Collation>(connection)
            .await?;
            let source = format!(
                "WITH resources AS (SELECT id, name COLLATE \"{}\" AS name \
                 FROM (VALUES (1, NULL), (2, 'a'), (3, 'Z'), (4, 'a'), \
                 (5, 'é'), (6, 'z'), (7, 'A')) AS input(id, name))",
                collation.name.replace('"', "\"\"")
            );
            let mut expected = diesel::sql_query(format!(
                "{source} SELECT id, name FROM resources ORDER BY name ASC NULLS FIRST, id"
            ))
            .load::<Row>(connection)
            .await?;
            let locale_order = expected.iter().map(|row| row.id).collect::<Vec<_>>();
            expected.sort_by(|left, right| {
                let order = left.value().cmp(&right.value());
                (if descending { order.reverse() } else { order })
                    .then_with(|| left.id.cmp(&right.id))
            });
            if !descending {
                assert_ne!(locale_order, expected.iter().map(|row| row.id).collect::<Vec<_>>(),
                    "fixture must exercise a collation that differs from Rust");
            }
            let sorts = [SortParam::new(FilterField::Name, descending), SortParam::new(FilterField::Id, false)];
            let fields = [
                CursorSqlField { column: "resources.name", sql_type: CursorSqlType::String, nullable: true },
                CursorSqlField { column: "resources.id", sql_type: CursorSqlType::Integer, nullable: false },
            ];
            let order = sorts.iter().zip(&fields)
                .map(|(sort, field)| order_sql_clause_for_field(sort, field))
                .collect::<Vec<_>>().join(", ");
            let mut cursor = None;
            let mut actual = Vec::new();
            loop {
                let predicate = cursor_filter_sql_for_fields(&sorts, &fields, cursor.as_deref()).unwrap()
                    .unwrap_or_else(|| "TRUE".to_string());
                let mut page = diesel::sql_query(format!(
                    "{source} SELECT id, name FROM resources WHERE {predicate} ORDER BY {order} LIMIT 1"
                )).load::<Row>(connection).await?;
                let Some(row) = page.pop() else { break; };
                cursor = Some(encode_cursor_values(&sorts, vec![row.value(), CursorValue::Integer(i64::from(row.id))]).unwrap());
                actual.push(row);
                assert!(actual.len() <= expected.len(), "cursor repeated a row");
            }
            assert_eq!(actual, expected);
            Ok::<_, diesel::result::Error>(())
        }).await.unwrap();
    }

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
            Some("((((resources.name) COLLATE \"C\" < 'beta' OR resources.name IS NULL)))")
        );
    }

    #[test]
    fn query_normalization_appends_the_adapter_tie_breaker() {
        let options = QueryOptions::new(
            Vec::new(),
            vec![SortParam {
                field: FilterField::Name,
                descending: true,
            }],
            Some(10),
            None,
            true,
        )
        .unwrap();
        let fields = vec![CursorSqlField {
            column: "resources.name",
            sql_type: CursorSqlType::String,
            nullable: false,
        }];

        let (options, fields) = normalize_query_fields(
            &options,
            fields,
            CursorTieBreaker::new(
                FilterField::Id,
                false,
                CursorSqlField {
                    column: "resources.id",
                    sql_type: CursorSqlType::Integer,
                    nullable: false,
                },
            ),
        )
        .unwrap();

        assert_eq!(
            options.sort().as_slice(),
            [
                SortParam {
                    field: FilterField::Name,
                    descending: true,
                },
                SortParam {
                    field: FilterField::Id,
                    descending: false,
                },
            ]
        );
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[1].column, "resources.id");
    }

    #[test]
    fn query_limits_are_checked_before_native_execution() {
        assert!(validated_query_limit(Some(0)).is_err());
        if usize::BITS >= i64::BITS {
            assert!(validated_query_limit(Some(usize::MAX)).is_err());
        }
        assert_eq!(validated_query_limit(Some(10)).unwrap(), Some(10));
    }
}
