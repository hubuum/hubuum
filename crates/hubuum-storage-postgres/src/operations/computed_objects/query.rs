use std::collections::{BTreeSet, HashMap};

use diesel::SelectableHelper;
use diesel::prelude::{BoolExpressionMethods, ExpressionMethods, OptionalExtension, QueryDsl};
use diesel_async::RunQueryDsl;
use hubuum_query::{
    ComputedFieldScope, ComputedQueryValueType, FilterField, Operator, ParsedQueryParam,
    QueryFilters, QuerySort, SortParam,
};

use crate::cursor::{CursorSqlField, CursorSqlType};
use crate::operations::catalog::object_cursor_field;
use crate::operations::computed_definition::{
    ComputedDefinitionRow, PERSONAL_VISIBILITY, SHARED_VISIBILITY,
};
use crate::operations::computed_materialization::acquire_computed_class_shared_lock;
use crate::operations::dynamic_sql::{
    BoundSqlPredicate, SqlComponent, SqlValue, bound_sql_predicate,
};
use crate::{PostgresConnection, PostgresStorageError};

const MAX_FILTERS_WITH_COMPUTED: usize = 2;
const MAX_SORT_FIELDS_WITH_COMPUTED: usize = 2;

#[derive(Clone, Debug)]
pub(crate) struct ComputedQuerySnapshot {
    class_id: i32,
    definitions: Vec<ComputedDefinitionRow>,
    evaluation_revision: i64,
    query_fields: HashMap<(ComputedFieldScope, String), ResolvedComputedQueryField>,
}

#[derive(Clone, Debug)]
struct ResolvedComputedQueryField {
    sql_expression: String,
    value_type: ComputedQueryValueType,
}

impl ComputedQuerySnapshot {
    pub(crate) const fn class_id(&self) -> i32 {
        self.class_id
    }

    pub(crate) fn definitions(&self) -> &[ComputedDefinitionRow] {
        &self.definitions
    }

    pub(crate) const fn evaluation_revision(&self) -> i64 {
        self.evaluation_revision
    }

    fn query_field(
        &self,
        field: &FilterField,
    ) -> Result<&ResolvedComputedQueryField, PostgresStorageError> {
        let computed = field.computed_query().ok_or_else(|| {
            PostgresStorageError::internal(format!("Field '{field}' is not a computed field"))
        })?;
        self.query_fields
            .get(&(computed.scope(), computed.key().to_string()))
            .ok_or_else(|| {
                PostgresStorageError::internal(format!(
                    "Computed field '{}' was not resolved",
                    computed.key()
                ))
            })
    }
}

#[derive(diesel::Queryable, diesel::Selectable)]
#[diesel(table_name = crate::schema::class_computation_state)]
struct ComputationStateRow {
    evaluation_revision: i64,
}

pub(crate) async fn resolve_computed_query_fields(
    connection: &mut PostgresConnection,
    target_class_id: i32,
    personal_owner_id: Option<i32>,
    filters: &mut QueryFilters,
    sorts: &mut QuerySort,
) -> Result<ComputedQuerySnapshot, PostgresStorageError> {
    validate_positive_id(target_class_id, "computed field class id")?;
    if let Some(owner_id) = personal_owner_id {
        validate_positive_id(owner_id, "computed field owner id")?;
    }
    validate_computed_filter_count(
        filters
            .iter()
            .filter(|filter| filter.field.computed_query().is_some())
            .count(),
    )?;
    let requested = filters
        .iter()
        .filter_map(|filter| filter.field.computed_query())
        .chain(sorts.iter().filter_map(|sort| sort.field.computed_query()))
        .map(|field| (field.scope(), field.key().to_string()))
        .collect::<BTreeSet<_>>();
    if requested.is_empty() {
        return Err(PostgresStorageError::internal(
            "Computed query resolution requires at least one computed field",
        ));
    }
    if personal_owner_id.is_none()
        && requested
            .iter()
            .any(|(scope, _)| *scope == ComputedFieldScope::Personal)
    {
        return Err(PostgresStorageError::invalid_input(
            "Personal computed fields can only be filtered or sorted by their owning human user",
        ));
    }

    acquire_computed_class_shared_lock(connection, target_class_id).await?;
    use crate::schema::class_computation_state::dsl as state;
    use crate::schema::computed_field_definitions::dsl as definition;
    let mut query = definition::computed_field_definitions
        .filter(definition::class_id.eq(target_class_id))
        .into_boxed();
    query = match personal_owner_id {
        Some(owner_id) => query.filter(
            definition::visibility
                .eq(SHARED_VISIBILITY)
                .or(definition::visibility
                    .eq(PERSONAL_VISIBILITY)
                    .and(definition::owner_user_id.eq(Some(owner_id)))),
        ),
        None => query.filter(definition::visibility.eq(SHARED_VISIBILITY)),
    };
    let definitions = query
        .order(definition::id.asc())
        .select(ComputedDefinitionRow::as_select())
        .load::<ComputedDefinitionRow>(connection)
        .await?;
    let evaluation_revision = state::class_computation_state
        .filter(state::class_id.eq(target_class_id))
        .select(ComputationStateRow::as_select())
        .first(connection)
        .await
        .optional()?
        .map_or(0, |state| state.evaluation_revision);

    for definition in definitions.iter().filter(|definition| definition.enabled()) {
        let _ = definition.evaluator_definition()?;
    }
    let definitions_by_key = definitions
        .iter()
        .filter(|definition| definition.enabled())
        .map(|definition| {
            let scope = if definition.is_shared() {
                ComputedFieldScope::Shared
            } else {
                ComputedFieldScope::Personal
            };
            ((scope, definition.key().to_string()), definition)
        })
        .collect::<HashMap<_, _>>();
    let shared_scope_sql = computed_scope_sql(
        definitions
            .iter()
            .filter(|definition| definition.is_shared() && definition.enabled()),
    )?;
    let personal_scope_sql = computed_scope_sql(definitions.iter().filter(|definition| {
        definition.enabled()
            && personal_owner_id.is_some_and(|owner_id| definition.is_personal_for(owner_id))
    }))?;

    let mut query_fields = HashMap::new();
    for (scope, key) in &requested {
        let definition = definitions_by_key
            .get(&(*scope, key.clone()))
            .ok_or_else(|| {
                PostgresStorageError::invalid_input(format!(
                    "Enabled {} computed field '{}' was not found for this class",
                    scope.as_str(),
                    key
                ))
            })?;
        let value_type = definition.query_value_type()?;
        let scope_sql = match scope {
            ComputedFieldScope::Shared => &shared_scope_sql,
            ComputedFieldScope::Personal => &personal_scope_sql,
        };
        query_fields.insert(
            (*scope, key.clone()),
            ResolvedComputedQueryField {
                sql_expression: computed_query_value_sql(
                    definition,
                    scope_sql,
                    evaluation_revision,
                )?,
                value_type,
            },
        );
    }

    let resolve_type = |field: &hubuum_query::ComputedQueryField| {
        query_fields
            .get(&(field.scope(), field.key().to_string()))
            .map(|resolved| resolved.value_type)
            .ok_or_else(|| {
                PostgresStorageError::internal(format!(
                    "Computed field '{}' was not resolved",
                    field.key()
                ))
            })
    };
    filters.try_resolve_computed_fields(resolve_type)?;
    sorts.try_resolve_computed_fields(resolve_type)?;

    Ok(ComputedQuerySnapshot {
        class_id: target_class_id,
        definitions,
        evaluation_revision,
        query_fields,
    })
}

pub(super) fn resolve_query_option_types(
    options: &mut hubuum_query::QueryOptions,
    snapshot: &ComputedQuerySnapshot,
) -> Result<(), PostgresStorageError> {
    options.try_resolve_computed_fields(|field| {
        snapshot
            .query_fields
            .get(&(field.scope(), field.key().to_string()))
            .map(|resolved| resolved.value_type)
            .ok_or_else(|| {
                PostgresStorageError::internal(format!(
                    "Computed field '{}' was not resolved for response pagination",
                    field.key()
                ))
            })
    })
}

fn validate_computed_filter_count(filter_count: usize) -> Result<(), PostgresStorageError> {
    if filter_count > MAX_FILTERS_WITH_COMPUTED {
        return Err(PostgresStorageError::invalid_input(format!(
            "Computed filtering supports at most {MAX_FILTERS_WITH_COMPUTED} computed filter parameters per request"
        )));
    }
    Ok(())
}

pub(super) fn validate_explicit_sort_count(sort_count: usize) -> Result<(), PostgresStorageError> {
    if sort_count > MAX_SORT_FIELDS_WITH_COMPUTED {
        return Err(PostgresStorageError::invalid_input(format!(
            "Computed sorting supports at most {MAX_SORT_FIELDS_WITH_COMPUTED} explicit sort fields per request"
        )));
    }
    Ok(())
}

pub(super) fn object_cursor_sql_fields(
    sorts: &[SortParam],
    snapshot: &ComputedQuerySnapshot,
) -> Result<Vec<CursorSqlField<String>>, PostgresStorageError> {
    sorts
        .iter()
        .map(|sort| {
            if sort.field.computed_query().is_none() {
                return object_cursor_field(&sort.field).map(Into::into);
            }
            object_computed_sql_field(&sort.field, snapshot)
        })
        .collect()
}

fn object_computed_sql_field(
    field: &FilterField,
    snapshot: &ComputedQuerySnapshot,
) -> Result<CursorSqlField<String>, PostgresStorageError> {
    let resolved = snapshot.query_field(field)?;
    let expression = &resolved.sql_expression;
    let (expression, sql_type) = match resolved.value_type {
        ComputedQueryValueType::String => {
            (format!("({expression} #>> '{{}}')"), CursorSqlType::String)
        }
        ComputedQueryValueType::Number | ComputedQueryValueType::Integer => (
            format!("try_numeric({expression} #>> '{{}}')"),
            CursorSqlType::Numeric,
        ),
        ComputedQueryValueType::Boolean => (
            format!("try_boolean({expression} #>> '{{}}')"),
            CursorSqlType::Boolean,
        ),
        ComputedQueryValueType::Object | ComputedQueryValueType::Array => {
            (expression.clone(), CursorSqlType::Json)
        }
    };
    Ok(CursorSqlField {
        column: expression,
        sql_type,
        nullable: true,
    })
}

fn computed_scope_sql<'definition>(
    definitions: impl Iterator<Item = &'definition ComputedDefinitionRow>,
) -> Result<String, PostgresStorageError> {
    let definitions = definitions
        .map(|definition| {
            serde_json::json!({
                "key": definition.key(),
                "operation": definition.operation(),
                "result_type": definition.result_type_name(),
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&definitions)
        .map(|value| value.replace('\'', "''"))
        .map_err(|error| PostgresStorageError::internal(error.to_string()))
}

fn computed_query_value_sql(
    definition: &ComputedDefinitionRow,
    scope_sql: &str,
    evaluation_revision: i64,
) -> Result<String, PostgresStorageError> {
    let key = definition.key().replace('\'', "''");
    let live_value = format!(
        "NULLIF(\
            hubuum_computed_evaluate_scope(\
                hubuumobject.data, \
                '{scope_sql}'::jsonb\
            ) -> 'values' -> '{key}', \
            'null'::jsonb\
        )"
    );
    if !definition.is_shared() {
        return Ok(live_value);
    }
    let cached_value = format!("sort_values.values -> '{key}'");
    let cached_value_matches_type = match definition.query_value_type()? {
        ComputedQueryValueType::String => {
            format!("jsonb_typeof({cached_value}) = 'string'")
        }
        ComputedQueryValueType::Number => format!(
            "jsonb_typeof({cached_value}) = 'number' \
             AND hubuum_computed_numeric({cached_value}) IS NOT NULL"
        ),
        ComputedQueryValueType::Integer => format!(
            "jsonb_typeof({cached_value}) = 'number' \
             AND hubuum_computed_numeric({cached_value}) IS NOT NULL \
             AND trunc(hubuum_computed_numeric({cached_value})) = \
                 hubuum_computed_numeric({cached_value})"
        ),
        ComputedQueryValueType::Boolean => {
            format!("jsonb_typeof({cached_value}) = 'boolean'")
        }
        ComputedQueryValueType::Object => {
            format!("jsonb_typeof({cached_value}) = 'object'")
        }
        ComputedQueryValueType::Array => {
            format!("jsonb_typeof({cached_value}) = 'array'")
        }
    };
    Ok(format!(
        "(SELECT CASE \
            WHEN sort_cache.present THEN sort_cache.value \
            ELSE {live_value} \
          END \
          FROM (VALUES (TRUE)) AS sort_fallback(seed) \
          LEFT JOIN LATERAL ( \
            SELECT TRUE AS present, \
                   NULLIF(sort_values.values -> '{key}', 'null'::jsonb) AS value \
            FROM object_computed_data AS sort_values \
            WHERE sort_values.object_id = hubuumobject.id \
              AND sort_values.class_id = {} \
              AND sort_values.evaluation_revision = {evaluation_revision} \
              AND sort_values.source_data_sha256 = \
                  hubuum_computed_source_sha256(hubuumobject.data) \
              AND hubuum_computed_materialization_valid( \
                  sort_values.values, \
                  sort_values.errors, \
                  '{scope_sql}'::jsonb \
              ) \
              AND jsonb_exists(sort_values.values, '{key}') \
              AND ({cached_value} = 'null'::jsonb OR ({cached_value_matches_type})) \
          ) AS sort_cache ON TRUE)",
        definition.class_id()
    ))
}

pub(crate) fn computed_filter_predicate(
    parameter: &ParsedQueryParam,
    snapshot: &ComputedQuerySnapshot,
) -> Result<BoundSqlPredicate, PostgresStorageError> {
    bound_sql_predicate(computed_filter_sql_component(parameter, snapshot)?)
}

pub(crate) fn computed_filter_sql_component(
    parameter: &ParsedQueryParam,
    snapshot: &ComputedQuerySnapshot,
) -> Result<SqlComponent, PostgresStorageError> {
    if parameter.value.contains('\0') {
        return Err(PostgresStorageError::invalid_input(format!(
            "Filter value for computed field '{}' contains a null character",
            parameter.field
        )));
    }
    let computed = parameter.field.computed_query().ok_or_else(|| {
        PostgresStorageError::internal(format!("Field '{}' is not computed", parameter.field))
    })?;
    let value_type = computed.value_type().ok_or_else(|| {
        PostgresStorageError::internal(format!(
            "Computed field '{}' has no resolved result type",
            computed.key()
        ))
    })?;
    let field = object_computed_sql_field(&parameter.field, snapshot)?;
    let (operator, negated) = parameter.operator.op_and_neg();

    if operator == Operator::IsNull {
        let should_be_null = value_as_boolean(parameter)? != negated;
        return Ok(SqlComponent {
            sql: format!(
                "{} IS {}NULL",
                field.expression(),
                if should_be_null { "" } else { "NOT " }
            ),
            bind_variables: Vec::new(),
        });
    }
    match value_type {
        ComputedQueryValueType::String => {
            computed_string_filter(field.expression(), parameter, operator, negated)
        }
        ComputedQueryValueType::Number | ComputedQueryValueType::Integer => {
            computed_numeric_filter(field.expression(), parameter, operator, negated)
        }
        ComputedQueryValueType::Boolean => {
            computed_boolean_filter(field.expression(), parameter, operator, negated)
        }
        ComputedQueryValueType::Object | ComputedQueryValueType::Array => {
            computed_json_filter(field.expression(), parameter, value_type, operator, negated)
        }
    }
}

fn validate_positive_id(id: i32, name: &str) -> Result<(), PostgresStorageError> {
    if id <= 0 {
        return Err(PostgresStorageError::invalid_input(format!(
            "{name} must be greater than zero"
        )));
    }
    Ok(())
}

fn computed_string_filter(
    expression: &str,
    parameter: &ParsedQueryParam,
    operator: Operator,
    negated: bool,
) -> Result<SqlComponent, PostgresStorageError> {
    let (sql, values) = match operator {
        Operator::Equals => (format!("{expression} = ?"), vec![parameter.value.clone()]),
        Operator::IEquals => (
            format!("{expression} ILIKE ?"),
            vec![parameter.value.clone()],
        ),
        Operator::Contains => (
            format!("{expression} LIKE ?"),
            vec![format!("%{}%", parameter.value)],
        ),
        Operator::IContains => (
            format!("{expression} ILIKE ?"),
            vec![format!("%{}%", parameter.value)],
        ),
        Operator::StartsWith => (
            format!("{expression} LIKE ?"),
            vec![format!("{}%", parameter.value)],
        ),
        Operator::IStartsWith => (
            format!("{expression} ILIKE ?"),
            vec![format!("{}%", parameter.value)],
        ),
        Operator::EndsWith => (
            format!("{expression} LIKE ?"),
            vec![format!("%{}", parameter.value)],
        ),
        Operator::IEndsWith => (
            format!("{expression} ILIKE ?"),
            vec![format!("%{}", parameter.value)],
        ),
        Operator::Like => (
            format!("{expression} LIKE ?"),
            vec![parameter.value.clone()],
        ),
        Operator::Regex => (format!("{expression} ~ ?"), vec![parameter.value.clone()]),
        Operator::In => {
            let values = exact_comma_separated_values(parameter, 50)?;
            let placeholders = values.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
            (format!("{expression} IN ({placeholders})"), values)
        }
        _ => return Err(computed_operator_mismatch(parameter, "string")),
    };
    Ok(SqlComponent {
        sql: maybe_negate(sql, negated),
        bind_variables: values.into_iter().map(SqlValue::String).collect(),
    })
}

fn computed_numeric_filter(
    expression: &str,
    parameter: &ParsedQueryParam,
    operator: Operator,
    negated: bool,
) -> Result<SqlComponent, PostgresStorageError> {
    let raw_values = trimmed_comma_separated_values(parameter, 50)?;
    let values = raw_values
        .iter()
        .map(|value| {
            hubuum_computed_fields::canonical_decimal_string(value).ok_or_else(|| {
                PostgresStorageError::invalid_input(format!(
                    "Invalid numeric value '{}' for computed field '{}'",
                    value, parameter.field
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let sql = match operator {
        Operator::Equals | Operator::In => {
            let placeholders = values
                .iter()
                .map(|_| "?::numeric")
                .collect::<Vec<_>>()
                .join(", ");
            format!("{expression} IN ({placeholders})")
        }
        Operator::Gt | Operator::Gte | Operator::Lt | Operator::Lte => {
            require_computed_value_count(parameter, values.len(), 1)?;
            let sql_operator = match operator {
                Operator::Gt => ">",
                Operator::Gte => ">=",
                Operator::Lt => "<",
                Operator::Lte => "<=",
                _ => unreachable!(),
            };
            format!("{expression} {sql_operator} ?::numeric")
        }
        Operator::Between => {
            require_computed_value_count(parameter, values.len(), 2)?;
            format!("{expression} BETWEEN ?::numeric AND ?::numeric")
        }
        _ => return Err(computed_operator_mismatch(parameter, "numeric")),
    };
    Ok(SqlComponent {
        sql: maybe_negate(sql, negated),
        bind_variables: values.into_iter().map(SqlValue::String).collect(),
    })
}

fn computed_boolean_filter(
    expression: &str,
    parameter: &ParsedQueryParam,
    operator: Operator,
    negated: bool,
) -> Result<SqlComponent, PostgresStorageError> {
    if operator != Operator::Equals {
        return Err(computed_operator_mismatch(parameter, "boolean"));
    }
    Ok(SqlComponent {
        sql: maybe_negate(format!("{expression} = ?"), negated),
        bind_variables: vec![SqlValue::Boolean(value_as_boolean(parameter)?)],
    })
}

fn computed_json_filter(
    expression: &str,
    parameter: &ParsedQueryParam,
    value_type: ComputedQueryValueType,
    operator: Operator,
    negated: bool,
) -> Result<SqlComponent, PostgresStorageError> {
    if operator == Operator::HasKey {
        return Ok(SqlComponent {
            sql: maybe_negate(format!("jsonb_exists({expression}, ?)"), negated),
            bind_variables: vec![SqlValue::String(parameter.value.clone())],
        });
    }
    if operator == Operator::ArrayLength && value_type == ComputedQueryValueType::Array {
        let length = parameter.value.parse::<i32>().map_err(|_| {
            PostgresStorageError::invalid_input(format!(
                "array_length requires an integer, got '{}'",
                parameter.value
            ))
        })?;
        if length < 0 {
            return Err(PostgresStorageError::invalid_input(
                "array_length requires a non-negative integer",
            ));
        }
        return Ok(SqlComponent {
            sql: maybe_negate(format!("jsonb_array_length({expression}) = ?"), negated),
            bind_variables: vec![SqlValue::Integer(length)],
        });
    }
    if !matches!(operator, Operator::Equals | Operator::Contains) {
        return Err(computed_operator_mismatch(parameter, value_type.as_str()));
    }

    let value: serde_json::Value = serde_json::from_str(&parameter.value).map_err(|error| {
        PostgresStorageError::invalid_input(format!(
            "Invalid JSON value for computed field '{}': {error}",
            parameter.field
        ))
    })?;
    let type_matches = matches!(
        (value_type, &value),
        (ComputedQueryValueType::Object, serde_json::Value::Object(_))
            | (ComputedQueryValueType::Array, serde_json::Value::Array(_))
    );
    if !type_matches {
        return Err(PostgresStorageError::invalid_input(format!(
            "Filter value for computed field '{}' must be a JSON {}",
            parameter.field,
            value_type.as_str()
        )));
    }
    validate_computed_filter_json(&value)?;
    let json = serde_json::to_string(&value)
        .map_err(|error| PostgresStorageError::internal(error.to_string()))?;
    let sql_operator = if operator == Operator::Equals {
        "="
    } else {
        "@>"
    };
    Ok(SqlComponent {
        sql: maybe_negate(format!("{expression} {sql_operator} ?::jsonb"), negated),
        bind_variables: vec![SqlValue::String(json)],
    })
}

fn validate_computed_filter_json(value: &serde_json::Value) -> Result<(), PostgresStorageError> {
    match hubuum_domain::validate_storage_json_value(value) {
        Ok(()) => Ok(()),
        Err(hubuum_domain::StorageJsonValidationError::UnsupportedValue) => {
            Err(PostgresStorageError::invalid_input(
                "Computed filter contains JSON that PostgreSQL JSONB cannot represent",
            ))
        }
        Err(hubuum_domain::StorageJsonValidationError::NestingTooDeep) => {
            Err(PostgresStorageError::invalid_input(format!(
                "Computed filter JSON exceeds the maximum nesting depth of {}",
                hubuum_domain::MAX_STORAGE_JSON_NESTING_DEPTH
            )))
        }
    }
}

fn exact_comma_separated_values(
    parameter: &ParsedQueryParam,
    maximum: usize,
) -> Result<Vec<String>, PostgresStorageError> {
    let values = parameter
        .value
        .split(',')
        .map(str::to_string)
        .collect::<Vec<_>>();
    validate_comma_separated_values(parameter, maximum, &values)?;
    Ok(values)
}

fn trimmed_comma_separated_values(
    parameter: &ParsedQueryParam,
    maximum: usize,
) -> Result<Vec<String>, PostgresStorageError> {
    let values = parameter
        .value
        .split(',')
        .map(str::trim)
        .map(str::to_string)
        .collect::<Vec<_>>();
    validate_comma_separated_values(parameter, maximum, &values)?;
    Ok(values)
}

fn validate_comma_separated_values(
    parameter: &ParsedQueryParam,
    maximum: usize,
    values: &[String],
) -> Result<(), PostgresStorageError> {
    if values.is_empty() || values.iter().any(String::is_empty) {
        return Err(PostgresStorageError::invalid_input(format!(
            "Filtering computed field '{}' requires a value",
            parameter.field
        )));
    }
    if values.len() > maximum {
        return Err(PostgresStorageError::invalid_input(format!(
            "Filtering computed field '{}' accepts at most {maximum} values",
            parameter.field
        )));
    }
    Ok(())
}

fn require_computed_value_count(
    parameter: &ParsedQueryParam,
    actual: usize,
    expected: usize,
) -> Result<(), PostgresStorageError> {
    if actual != expected {
        return Err(PostgresStorageError::invalid_input(format!(
            "Operator '{}' requires {expected} value(s) for field '{}'",
            parameter.operator, parameter.field
        )));
    }
    Ok(())
}

fn value_as_boolean(parameter: &ParsedQueryParam) -> Result<bool, PostgresStorageError> {
    match parameter.value.as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(PostgresStorageError::invalid_input(format!(
            "Invalid boolean value '{}' for field '{}'",
            parameter.value, parameter.field
        ))),
    }
}

fn maybe_negate(sql: String, negated: bool) -> String {
    if negated { format!("NOT ({sql})") } else { sql }
}

fn computed_operator_mismatch(
    parameter: &ParsedQueryParam,
    value_type: &str,
) -> PostgresStorageError {
    PostgresStorageError::invalid_input(format!(
        "Operator '{}' is not applicable to computed field '{}' (type: {value_type})",
        parameter.operator, parameter.field
    ))
}

#[cfg(test)]
mod tests {
    use super::{validate_computed_filter_count, validate_explicit_sort_count};

    #[test]
    fn computed_filter_count_is_enforced_inside_the_adapter() {
        let error = validate_computed_filter_count(3).unwrap_err();

        assert!(error.to_string().contains("at most 2"));
    }

    #[test]
    fn computed_sort_count_is_enforced_inside_the_adapter() {
        validate_explicit_sort_count(2).unwrap();
        let error = validate_explicit_sort_count(3).unwrap_err();

        assert!(error.to_string().contains("at most 2"));
    }
}
