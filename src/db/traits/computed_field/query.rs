use super::*;

pub async fn resolve_computed_query_fields(
    pool: &DbPool,
    target_class_id: i32,
    personal_owner_id: Option<i32>,
    filters: &mut [ParsedQueryParam],
    sorts: &mut [SortParam],
) -> Result<ComputedQuerySnapshot, ApiError> {
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
        return Err(ApiError::InternalServerError(
            "Computed query resolution requires at least one computed field".to_string(),
        ));
    }
    if sorts
        .iter()
        .any(|sort| sort.field.computed_query().is_some())
    {
        validate_computed_query_count(sorts.len())?;
    }
    if personal_owner_id.is_none()
        && requested
            .iter()
            .any(|(scope, _)| *scope == ComputedFieldScope::Personal)
    {
        return Err(ApiError::BadRequest(
            "Personal computed fields can only be filtered or sorted by their owning human user"
                .to_string(),
        ));
    }

    let (definitions, state) = with_transaction(pool, async |conn| -> Result<_, ApiError> {
        acquire_computed_class_shared_lock(conn, target_class_id).await?;
        use crate::schema::class_computation_state::dsl as state;
        use crate::schema::computed_field_definitions::dsl as definition;
        let mut query = definition::computed_field_definitions
            .filter(definition::class_id.eq(target_class_id))
            .into_boxed();
        query = match personal_owner_id {
            Some(owner_id) => query.filter(
                definition::visibility
                    .eq(COMPUTED_FIELD_VISIBILITY_SHARED)
                    .or(definition::visibility
                        .eq(COMPUTED_FIELD_VISIBILITY_PERSONAL)
                        .and(definition::owner_user_id.eq(Some(owner_id)))),
            ),
            None => query.filter(definition::visibility.eq(COMPUTED_FIELD_VISIBILITY_SHARED)),
        };
        let definitions = query
            .order(definition::id.asc())
            .select(ComputedFieldDefinition::as_select())
            .load::<ComputedFieldDefinition>(conn)
            .await?;
        let state = state::class_computation_state
            .filter(state::class_id.eq(target_class_id))
            .select(ClassComputationState::as_select())
            .first(conn)
            .await
            .optional()?
            .unwrap_or_else(|| ClassComputationState::ready_without_definitions(target_class_id));
        Ok((definitions, state))
    })
    .await?;
    for definition in definitions.iter().filter(|definition| definition.enabled) {
        let _ = definition.evaluator_definition()?;
    }
    let definitions_by_key = definitions
        .iter()
        .filter(|definition| definition.enabled)
        .map(|definition| {
            let scope = if definition.is_shared() {
                ComputedFieldScope::Shared
            } else {
                ComputedFieldScope::Personal
            };
            ((scope, definition.key.clone()), definition)
        })
        .collect::<HashMap<_, _>>();
    let shared_scope_sql = computed_scope_sql(
        definitions
            .iter()
            .filter(|definition| definition.is_shared() && definition.enabled),
    )?;
    let personal_scope_sql = computed_scope_sql(definitions.iter().filter(|definition| {
        definition.enabled
            && personal_owner_id.is_some_and(|owner_id| definition.is_personal_for(owner_id))
    }))?;

    let mut query_fields = HashMap::new();
    for field in filters
        .iter_mut()
        .filter_map(|filter| filter.field.computed_query_mut())
        .chain(
            sorts
                .iter_mut()
                .filter_map(|sort| sort.field.computed_query_mut()),
        )
    {
        let definition = definitions_by_key
            .get(&(field.scope(), field.key().to_string()))
            .ok_or_else(|| {
                ApiError::BadRequest(format!(
                    "Enabled {} computed field '{}' was not found for this class",
                    field.scope().as_str(),
                    field.key()
                ))
            })?;
        let value_type =
            crate::models::ComputedResultType::from_db(&definition.result_type)?.into();
        let scope_sql = match field.scope() {
            ComputedFieldScope::Shared => &shared_scope_sql,
            ComputedFieldScope::Personal => &personal_scope_sql,
        };
        query_fields.insert(
            (field.scope(), field.key().to_string()),
            ResolvedComputedQueryField {
                sql_expression: computed_query_value_sql(
                    definition,
                    scope_sql,
                    state.evaluation_revision,
                )?,
                value_type,
            },
        );
        field.resolve(value_type);
    }
    Ok(ComputedQuerySnapshot {
        class_id: target_class_id,
        definitions,
        state,
        query_fields,
    })
}

pub(super) fn validate_computed_filter_count(filter_count: usize) -> Result<(), ApiError> {
    if filter_count > MAX_FILTERS_WITH_COMPUTED {
        return Err(ApiError::BadRequest(format!(
            "Computed filtering supports at most {MAX_FILTERS_WITH_COMPUTED} computed filter parameters per request"
        )));
    }
    Ok(())
}

pub(super) fn validate_computed_query_count(sort_count: usize) -> Result<(), ApiError> {
    if sort_count > MAX_SORT_FIELDS_WITH_COMPUTED {
        return Err(ApiError::BadRequest(format!(
            "Computed sorting supports at most {MAX_SORT_FIELDS_WITH_COMPUTED} explicit sort fields per request"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct ComputedQuerySnapshot {
    pub(super) class_id: i32,
    pub(super) definitions: Vec<ComputedFieldDefinition>,
    pub(super) state: ClassComputationState,
    query_fields: HashMap<(ComputedFieldScope, String), ResolvedComputedQueryField>,
}

#[derive(Debug, Clone)]
struct ResolvedComputedQueryField {
    sql_expression: String,
    value_type: ComputedQueryValueType,
}

impl ComputedQuerySnapshot {
    pub(crate) const fn class_id(&self) -> i32 {
        self.class_id
    }

    pub(crate) fn definitions(&self) -> &[ComputedFieldDefinition] {
        &self.definitions
    }

    fn query_field(&self, field: &FilterField) -> Result<&ResolvedComputedQueryField, ApiError> {
        let computed = field.computed_query().ok_or_else(|| {
            ApiError::InternalServerError(format!("Field '{field}' is not a computed field"))
        })?;
        self.query_fields
            .get(&(computed.scope(), computed.key().to_string()))
            .ok_or_else(|| {
                ApiError::InternalServerError(format!(
                    "Computed field '{}' was not resolved",
                    computed.key()
                ))
            })
    }
}

pub(crate) fn object_cursor_sql_fields(
    sorts: &[SortParam],
    snapshot: &ComputedQuerySnapshot,
) -> Result<Vec<CursorSqlField<String>>, ApiError> {
    sorts
        .iter()
        .map(|sort| {
            if sort.field.computed_query().is_none() {
                return <HubuumObject as CursorSqlMapping>::sql_field(&sort.field).map(Into::into);
            }
            object_computed_sql_field(&sort.field, snapshot)
        })
        .collect()
}

fn object_computed_sql_field(
    field: &FilterField,
    snapshot: &ComputedQuerySnapshot,
) -> Result<CursorSqlField<String>, ApiError> {
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

fn computed_scope_sql<'a>(
    definitions: impl Iterator<Item = &'a ComputedFieldDefinition>,
) -> Result<String, ApiError> {
    let definitions = definitions
        .map(|definition| {
            serde_json::json!({
                "key": definition.key,
                "operation": definition.operation,
                "result_type": definition.result_type,
            })
        })
        .collect::<Vec<_>>();
    Ok(serde_json::to_string(&definitions)?.replace('\'', "''"))
}

fn computed_query_value_sql(
    definition: &ComputedFieldDefinition,
    scope_sql: &str,
    evaluation_revision: i64,
) -> Result<String, ApiError> {
    let key = definition.key.replace('\'', "''");
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
    let cached_value_matches_type = match ComputedResultType::from_db(&definition.result_type)? {
        ComputedResultType::String => format!("jsonb_typeof({cached_value}) = 'string'"),
        ComputedResultType::Number => format!(
            "jsonb_typeof({cached_value}) = 'number' \
             AND hubuum_computed_numeric({cached_value}) IS NOT NULL"
        ),
        ComputedResultType::Integer => format!(
            "jsonb_typeof({cached_value}) = 'number' \
             AND hubuum_computed_numeric({cached_value}) IS NOT NULL \
             AND trunc(hubuum_computed_numeric({cached_value})) = \
                 hubuum_computed_numeric({cached_value})"
        ),
        ComputedResultType::Boolean => format!("jsonb_typeof({cached_value}) = 'boolean'"),
        ComputedResultType::Object => format!("jsonb_typeof({cached_value}) = 'object'"),
        ComputedResultType::Array => format!("jsonb_typeof({cached_value}) = 'array'"),
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
        definition.class_id
    ))
}

pub(crate) fn computed_filter_predicate(
    param: &ParsedQueryParam,
    snapshot: &ComputedQuerySnapshot,
) -> Result<JsonSqlPredicate, ApiError> {
    dynamic_sql_predicate(computed_filter_sql_component(param, snapshot)?)
}

pub(crate) fn computed_filter_sql_component(
    param: &ParsedQueryParam,
    snapshot: &ComputedQuerySnapshot,
) -> Result<SQLComponent, ApiError> {
    if param.value.contains('\0') {
        return Err(ApiError::BadRequest(format!(
            "Filter value for computed field '{}' contains a null character",
            param.field
        )));
    }
    let computed = param.field.computed_query().ok_or_else(|| {
        ApiError::InternalServerError(format!("Field '{}' is not computed", param.field))
    })?;
    let value_type = computed.value_type().ok_or_else(|| {
        ApiError::InternalServerError(format!(
            "Computed field '{}' has no resolved result type",
            computed.key()
        ))
    })?;
    let field = object_computed_sql_field(&param.field, snapshot)?;
    let (operator, negated) = param.operator.op_and_neg();

    let component = if operator == Operator::IsNull {
        let should_be_null = param.value_as_boolean()? != negated;
        SQLComponent {
            sql: format!(
                "{} IS {}NULL",
                field.expression(),
                if should_be_null { "" } else { "NOT " }
            ),
            bind_variables: Vec::new(),
        }
    } else {
        match value_type {
            ComputedQueryValueType::String => {
                computed_string_filter(field.expression(), param, operator, negated)?
            }
            ComputedQueryValueType::Number | ComputedQueryValueType::Integer => {
                computed_numeric_filter(field.expression(), param, operator, negated)?
            }
            ComputedQueryValueType::Boolean => {
                computed_boolean_filter(field.expression(), param, operator, negated)?
            }
            ComputedQueryValueType::Object | ComputedQueryValueType::Array => {
                computed_json_filter(field.expression(), param, value_type, operator, negated)?
            }
        }
    };
    Ok(component)
}

fn computed_string_filter(
    expression: &str,
    param: &ParsedQueryParam,
    operator: Operator,
    negated: bool,
) -> Result<SQLComponent, ApiError> {
    let (sql, values) = match operator {
        Operator::Equals => (format!("{expression} = ?"), vec![param.value.clone()]),
        Operator::IEquals => (format!("{expression} ILIKE ?"), vec![param.value.clone()]),
        Operator::Contains => (
            format!("{expression} LIKE ?"),
            vec![format!("%{}%", param.value)],
        ),
        Operator::IContains => (
            format!("{expression} ILIKE ?"),
            vec![format!("%{}%", param.value)],
        ),
        Operator::StartsWith => (
            format!("{expression} LIKE ?"),
            vec![format!("{}%", param.value)],
        ),
        Operator::IStartsWith => (
            format!("{expression} ILIKE ?"),
            vec![format!("{}%", param.value)],
        ),
        Operator::EndsWith => (
            format!("{expression} LIKE ?"),
            vec![format!("%{}", param.value)],
        ),
        Operator::IEndsWith => (
            format!("{expression} ILIKE ?"),
            vec![format!("%{}", param.value)],
        ),
        Operator::Like => (format!("{expression} LIKE ?"), vec![param.value.clone()]),
        Operator::Regex => (format!("{expression} ~ ?"), vec![param.value.clone()]),
        Operator::In => {
            let values = exact_comma_separated_values(param, 50)?;
            let placeholders = values.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
            (format!("{expression} IN ({placeholders})"), values)
        }
        _ => return Err(computed_operator_mismatch(param, "string")),
    };
    Ok(SQLComponent {
        sql: maybe_negate(sql, negated),
        bind_variables: values.into_iter().map(SQLValue::String).collect(),
    })
}

fn computed_numeric_filter(
    expression: &str,
    param: &ParsedQueryParam,
    operator: Operator,
    negated: bool,
) -> Result<SQLComponent, ApiError> {
    let raw_values = trimmed_comma_separated_values(param, 50)?;
    let values = raw_values
        .iter()
        .map(|value| {
            hubuum_computed_fields::canonical_decimal_string(value).ok_or_else(|| {
                ApiError::BadRequest(format!(
                    "Invalid numeric value '{}' for computed field '{}'",
                    value, param.field
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
            require_computed_value_count(param, values.len(), 1)?;
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
            require_computed_value_count(param, values.len(), 2)?;
            format!("{expression} BETWEEN ?::numeric AND ?::numeric")
        }
        _ => return Err(computed_operator_mismatch(param, "numeric")),
    };
    Ok(SQLComponent {
        sql: maybe_negate(sql, negated),
        bind_variables: values.into_iter().map(SQLValue::String).collect(),
    })
}

fn computed_boolean_filter(
    expression: &str,
    param: &ParsedQueryParam,
    operator: Operator,
    negated: bool,
) -> Result<SQLComponent, ApiError> {
    if operator != Operator::Equals {
        return Err(computed_operator_mismatch(param, "boolean"));
    }
    Ok(SQLComponent {
        sql: maybe_negate(format!("{expression} = ?"), negated),
        bind_variables: vec![SQLValue::Boolean(param.value_as_boolean()?)],
    })
}

fn computed_json_filter(
    expression: &str,
    param: &ParsedQueryParam,
    value_type: ComputedQueryValueType,
    operator: Operator,
    negated: bool,
) -> Result<SQLComponent, ApiError> {
    if operator == Operator::HasKey {
        return Ok(SQLComponent {
            sql: maybe_negate(format!("jsonb_exists({expression}, ?)"), negated),
            bind_variables: vec![SQLValue::String(param.value.clone())],
        });
    }
    if operator == Operator::ArrayLength && value_type == ComputedQueryValueType::Array {
        let length = param.value.parse::<i32>().map_err(|_| {
            ApiError::BadRequest(format!(
                "array_length requires an integer, got '{}'",
                param.value
            ))
        })?;
        if length < 0 {
            return Err(ApiError::BadRequest(
                "array_length requires a non-negative integer".to_string(),
            ));
        }
        return Ok(SQLComponent {
            sql: maybe_negate(format!("jsonb_array_length({expression}) = ?"), negated),
            bind_variables: vec![SQLValue::Integer(length)],
        });
    }
    if !matches!(operator, Operator::Equals | Operator::Contains) {
        return Err(computed_operator_mismatch(param, value_type.as_str()));
    }

    let value: serde_json::Value = serde_json::from_str(&param.value).map_err(|error| {
        ApiError::BadRequest(format!(
            "Invalid JSON value for computed field '{}': {error}",
            param.field
        ))
    })?;
    let type_matches = matches!(
        (value_type, &value),
        (ComputedQueryValueType::Object, serde_json::Value::Object(_))
            | (ComputedQueryValueType::Array, serde_json::Value::Array(_))
    );
    if !type_matches {
        return Err(ApiError::BadRequest(format!(
            "Filter value for computed field '{}' must be a JSON {}",
            param.field,
            value_type.as_str()
        )));
    }
    validate_computed_filter_json(&value)?;
    let json = serde_json::to_string(&value)?;
    let sql_operator = if operator == Operator::Equals {
        "="
    } else {
        "@>"
    };
    Ok(SQLComponent {
        sql: maybe_negate(format!("{expression} {sql_operator} ?::jsonb"), negated),
        bind_variables: vec![SQLValue::String(json)],
    })
}

fn validate_computed_filter_json(value: &serde_json::Value) -> Result<(), ApiError> {
    match crate::db::json::validate_postgres_jsonb_value(value) {
        Ok(()) => Ok(()),
        Err(crate::db::json::PostgresJsonbValidationError::UnsupportedValue) => {
            Err(ApiError::BadRequest(
                "Computed filter contains JSON that PostgreSQL JSONB cannot represent".to_string(),
            ))
        }
        Err(crate::db::json::PostgresJsonbValidationError::NestingTooDeep) => {
            Err(ApiError::BadRequest(format!(
                "Computed filter JSON exceeds the maximum nesting depth of {}",
                crate::db::json::MAX_POSTGRES_JSONB_NESTING_DEPTH
            )))
        }
    }
}

fn exact_comma_separated_values(
    param: &ParsedQueryParam,
    maximum: usize,
) -> Result<Vec<String>, ApiError> {
    let values = param
        .value
        .split(',')
        .map(str::to_string)
        .collect::<Vec<_>>();
    validate_comma_separated_values(param, maximum, &values)?;
    Ok(values)
}

fn trimmed_comma_separated_values(
    param: &ParsedQueryParam,
    maximum: usize,
) -> Result<Vec<String>, ApiError> {
    let values = param
        .value
        .split(',')
        .map(str::trim)
        .map(str::to_string)
        .collect::<Vec<_>>();
    validate_comma_separated_values(param, maximum, &values)?;
    Ok(values)
}

fn validate_comma_separated_values(
    param: &ParsedQueryParam,
    maximum: usize,
    values: &[String],
) -> Result<(), ApiError> {
    if values.is_empty() || values.iter().any(String::is_empty) {
        return Err(ApiError::BadRequest(format!(
            "Filtering computed field '{}' requires a value",
            param.field
        )));
    }
    if values.len() > maximum {
        return Err(ApiError::BadRequest(format!(
            "Filtering computed field '{}' accepts at most {maximum} values",
            param.field
        )));
    }
    Ok(())
}

fn require_computed_value_count(
    param: &ParsedQueryParam,
    actual: usize,
    expected: usize,
) -> Result<(), ApiError> {
    if actual != expected {
        return Err(ApiError::OperatorMismatch(format!(
            "Operator '{}' requires {expected} value(s) for field '{}'",
            param.operator, param.field
        )));
    }
    Ok(())
}

fn maybe_negate(sql: String, negated: bool) -> String {
    if negated { format!("NOT ({sql})") } else { sql }
}

fn computed_operator_mismatch(param: &ParsedQueryParam, value_type: &str) -> ApiError {
    ApiError::OperatorMismatch(format!(
        "Operator '{}' is not applicable to computed field '{}' (type: {value_type})",
        param.operator, param.field
    ))
}
