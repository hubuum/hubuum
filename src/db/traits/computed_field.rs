use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Instant;

use diesel::sql_types::{BigInt, Integer, Text};
use hubuum_computed_fields::{
    EvaluationLimits, EvaluationResult, MAX_PERSONAL_DEFINITIONS, MAX_SHARED_DEFINITIONS, evaluate,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{info, warn};

use crate::db::prelude::*;
use crate::db::traits::search::{JsonSqlPredicate, dynamic_sql_predicate};
use crate::db::traits::task::{
    TaskBackend, TaskStateUpdate, emit_internal_task_event, insert_internal_queued_task,
};
use crate::db::{DbConnection, DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent, emit_event};
use crate::models::search::{
    ComputedFieldScope, ComputedSortValueType, Operator, ParsedQueryParam, ParsedQueryParamExt,
    QueryOptions, SQLComponent, SQLValue, SortParam,
};
use crate::models::traits::object::object_computed_sql_field;
use crate::models::{
    COMPUTED_FIELD_VISIBILITY_PERSONAL, COMPUTED_FIELD_VISIBILITY_SHARED, ClassComputationState,
    ComputedFieldDefinition, ComputedFieldDefinitionPatch, ComputedFieldDefinitionRequest,
    ComputedFieldErrorResponse, ComputedFieldMutationResponse, ComputedObjectScopesResponse,
    ComputedScopeResponse, HubuumClass, HubuumObject, HubuumObjectComputedResponse,
    NewObjectComputedData, NewTaskEventRecord, ObjectComputedData, SharedComputedScopeResponse,
    TaskKind, TaskRecord, TaskStatus, ValidatedComputedFieldPatch,
};

const COMPUTED_CLASS_LOCK_NAMESPACE: i32 = 1_133_113;
const REINDEX_PAYLOAD_TYPE: &str = "computed_fields";
pub const MAX_SORT_FIELDS_WITH_COMPUTED: usize = 2;

fn reindex_batch_size() -> i64 {
    crate::config::get_config()
        .map(|config| config.computed_reindex_batch_size)
        .unwrap_or(crate::config::DEFAULT_COMPUTED_REINDEX_BATCH_SIZE)
        .min(crate::config::MAX_COMPUTED_REINDEX_BATCH_SIZE) as i64
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComputedReindexPayload {
    #[serde(rename = "type")]
    pub payload_type: String,
    pub class_id: i32,
    pub target_revision: i64,
    pub object_upper_bound: i32,
}

impl ComputedReindexPayload {
    fn new(class_id: i32, target_revision: i64, object_upper_bound: i32) -> Self {
        Self {
            payload_type: REINDEX_PAYLOAD_TYPE.to_string(),
            class_id,
            target_revision,
            object_upper_bound,
        }
    }

    fn validate(&self) -> Result<(), ApiError> {
        if self.payload_type != REINDEX_PAYLOAD_TYPE
            || self.class_id <= 0
            || self.target_revision < 0
            || self.object_upper_bound < 0
        {
            return Err(ApiError::BadRequest(
                "Computed-field reindex task payload is invalid".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(QueryableByName)]
struct ReturnedTaskId {
    #[diesel(sql_type = Integer)]
    id: i32,
}

#[derive(QueryableByName)]
struct ObjectBoundary {
    #[diesel(sql_type = Integer)]
    upper_bound: i32,
    #[diesel(sql_type = BigInt)]
    total_items: i64,
}

pub(crate) async fn acquire_computed_class_shared_lock(
    conn: &mut DbConnection,
    class_id: i32,
) -> Result<(), ApiError> {
    diesel::sql_query("SELECT pg_advisory_xact_lock_shared($1, $2)")
        .bind::<Integer, _>(COMPUTED_CLASS_LOCK_NAMESPACE)
        .bind::<Integer, _>(class_id)
        .execute(conn)
        .await?;
    Ok(())
}

async fn acquire_computed_class_exclusive_lock(
    conn: &mut DbConnection,
    class_id: i32,
) -> Result<(), ApiError> {
    diesel::sql_query("SELECT pg_advisory_xact_lock($1, $2)")
        .bind::<Integer, _>(COMPUTED_CLASS_LOCK_NAMESPACE)
        .bind::<Integer, _>(class_id)
        .execute(conn)
        .await?;
    Ok(())
}

async fn acquire_personal_definition_scope_lock(
    conn: &mut DbConnection,
    class_id: i32,
    owner_id: i32,
) -> Result<(), ApiError> {
    if class_id <= 0 || owner_id <= 0 {
        return Err(ApiError::BadRequest(
            "Computed-field class and owner ids must be greater than zero".to_string(),
        ));
    }
    // The negative owner key keeps this two-id lock domain separate from the
    // positive namespace/class pair used by shared materialization locks.
    diesel::sql_query("SELECT pg_advisory_xact_lock($1, $2)")
        .bind::<Integer, _>(class_id)
        .bind::<Integer, _>(-owner_id)
        .execute(conn)
        .await?;
    Ok(())
}

async fn ensure_computation_state(
    conn: &mut DbConnection,
    target_class_id: i32,
) -> Result<ClassComputationState, ApiError> {
    use crate::schema::class_computation_state::dsl::{class_computation_state, class_id};

    diesel::insert_into(class_computation_state)
        .values(class_id.eq(target_class_id))
        .on_conflict(class_id)
        .do_nothing()
        .execute(conn)
        .await?;
    Ok(class_computation_state
        .filter(class_id.eq(target_class_id))
        .select(ClassComputationState::as_select())
        .first(conn)
        .await?)
}

pub async fn class_computation_state_for(
    pool: &DbPool,
    target_class_id: i32,
) -> Result<ClassComputationState, ApiError> {
    use crate::schema::class_computation_state::dsl::{class_computation_state, class_id};

    Ok(with_connection(pool, async |conn| {
        class_computation_state
            .filter(class_id.eq(target_class_id))
            .select(ClassComputationState::as_select())
            .first(conn)
            .await
            .optional()
    })
    .await?
    .unwrap_or_else(|| ClassComputationState::ready_without_definitions(target_class_id)))
}

pub async fn resolve_computed_sort_fields(
    pool: &DbPool,
    target_class_id: i32,
    personal_owner_id: Option<i32>,
    sorts: &mut [SortParam],
) -> Result<ComputedSortSnapshot, ApiError> {
    resolve_computed_query_fields(pool, target_class_id, personal_owner_id, &mut [], sorts).await
}

pub async fn resolve_computed_query_fields(
    pool: &DbPool,
    target_class_id: i32,
    personal_owner_id: Option<i32>,
    filters: &mut [ParsedQueryParam],
    sorts: &mut [SortParam],
) -> Result<ComputedSortSnapshot, ApiError> {
    let requested = filters
        .iter()
        .filter_map(|filter| filter.field.computed_sort())
        .chain(sorts.iter().filter_map(|sort| sort.field.computed_sort()))
        .map(|field| (field.scope(), field.key().to_string()))
        .collect::<BTreeSet<_>>();
    if requested.is_empty() {
        return Err(ApiError::InternalServerError(
            "Computed query resolution requires at least one computed field".to_string(),
        ));
    }
    if sorts
        .iter()
        .any(|sort| sort.field.computed_sort().is_some())
    {
        validate_computed_sort_count(sorts.len())?;
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

    for field in filters
        .iter_mut()
        .filter_map(|filter| filter.field.computed_sort_mut())
        .chain(
            sorts
                .iter_mut()
                .filter_map(|sort| sort.field.computed_sort_mut()),
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
        field.resolve(
            computed_sort_value_sql(definition, scope_sql, state.evaluation_revision),
            value_type,
        );
    }
    Ok(ComputedSortSnapshot {
        class_id: target_class_id,
        definitions,
        state,
    })
}

fn validate_computed_sort_count(sort_count: usize) -> Result<(), ApiError> {
    if sort_count > MAX_SORT_FIELDS_WITH_COMPUTED {
        return Err(ApiError::BadRequest(format!(
            "Computed sorting supports at most {MAX_SORT_FIELDS_WITH_COMPUTED} explicit sort fields per request"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct ComputedSortSnapshot {
    class_id: i32,
    definitions: Vec<ComputedFieldDefinition>,
    state: ClassComputationState,
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

fn computed_sort_value_sql(
    definition: &ComputedFieldDefinition,
    scope_sql: &str,
    evaluation_revision: i64,
) -> String {
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
        return live_value;
    }
    format!(
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
              AND jsonb_exists(sort_values.values, '{key}') \
          ) AS sort_cache ON TRUE)",
        definition.class_id
    )
}

pub(crate) fn computed_filter_predicate(
    param: &ParsedQueryParam,
) -> Result<JsonSqlPredicate, ApiError> {
    if param.value.contains('\0') {
        return Err(ApiError::BadRequest(format!(
            "Filter value for computed field '{}' contains a null character",
            param.field
        )));
    }
    let computed = param.field.computed_sort().ok_or_else(|| {
        ApiError::InternalServerError(format!("Field '{}' is not computed", param.field))
    })?;
    let value_type = computed.value_type().ok_or_else(|| {
        ApiError::InternalServerError(format!(
            "Computed field '{}' has no resolved result type",
            computed.key()
        ))
    })?;
    let field = object_computed_sql_field(&param.field)?;
    let (operator, negated) = param.operator.op_and_neg();

    let component = if operator == Operator::IsNull {
        let should_be_null = param.value_as_boolean()? != negated;
        SQLComponent {
            sql: format!(
                "{} IS {}NULL",
                field.expression,
                if should_be_null { "" } else { "NOT " }
            ),
            bind_variables: Vec::new(),
        }
    } else {
        match value_type {
            ComputedSortValueType::String => {
                computed_string_filter(&field.expression, param, operator, negated)?
            }
            ComputedSortValueType::Number | ComputedSortValueType::Integer => {
                computed_numeric_filter(&field.expression, param, operator, negated)?
            }
            ComputedSortValueType::Boolean => {
                computed_boolean_filter(&field.expression, param, operator, negated)?
            }
            ComputedSortValueType::Object | ComputedSortValueType::Array => {
                computed_json_filter(&field.expression, param, value_type, operator, negated)?
            }
        }
    };
    dynamic_sql_predicate(component)
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
            let values = comma_separated_values(param, 50)?;
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
    let raw_values = comma_separated_values(param, 50)?;
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
    value_type: ComputedSortValueType,
    operator: Operator,
    negated: bool,
) -> Result<SQLComponent, ApiError> {
    if operator == Operator::HasKey {
        return Ok(SQLComponent {
            sql: maybe_negate(format!("jsonb_exists({expression}, ?)"), negated),
            bind_variables: vec![SQLValue::String(param.value.clone())],
        });
    }
    if operator == Operator::ArrayLength && value_type == ComputedSortValueType::Array {
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
        (ComputedSortValueType::Object, serde_json::Value::Object(_))
            | (ComputedSortValueType::Array, serde_json::Value::Array(_))
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

fn comma_separated_values(
    param: &ParsedQueryParam,
    maximum: usize,
) -> Result<Vec<String>, ApiError> {
    let values = param
        .value
        .split(',')
        .map(str::trim)
        .map(str::to_string)
        .collect::<Vec<_>>();
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
    Ok(values)
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

pub async fn list_shared_definitions(
    pool: &DbPool,
    target_class_id: i32,
) -> Result<Vec<ComputedFieldDefinition>, ApiError> {
    use crate::schema::computed_field_definitions::dsl::{
        class_id, computed_field_definitions, id, visibility,
    };

    with_connection(pool, async |conn| {
        computed_field_definitions
            .filter(class_id.eq(target_class_id))
            .filter(visibility.eq(COMPUTED_FIELD_VISIBILITY_SHARED))
            .order(id.asc())
            .select(ComputedFieldDefinition::as_select())
            .load(conn)
            .await
    })
    .await
}

pub async fn list_personal_definitions_page(
    pool: &DbPool,
    owner_id: i32,
    class_filter: Option<i32>,
    query_options: &QueryOptions,
) -> Result<(Vec<ComputedFieldDefinition>, i64), ApiError> {
    use crate::schema::computed_field_definitions::dsl::{
        class_id, computed_field_definitions, owner_user_id, visibility,
    };

    let count = crate::pagination::exact_count_or_skipped(query_options, async || {
        with_connection(pool, async |conn| {
            let mut query = computed_field_definitions
                .filter(owner_user_id.eq(Some(owner_id)))
                .filter(visibility.eq(COMPUTED_FIELD_VISIBILITY_PERSONAL))
                .into_boxed();
            if let Some(target_class_id) = class_filter {
                query = query.filter(class_id.eq(target_class_id));
            }
            query.count().get_result::<i64>(conn).await
        })
        .await
    })
    .await?;

    let rows = with_connection(pool, async |conn| -> Result<_, ApiError> {
        let mut query = computed_field_definitions
            .filter(owner_user_id.eq(Some(owner_id)))
            .filter(visibility.eq(COMPUTED_FIELD_VISIBILITY_PERSONAL))
            .into_boxed();
        if let Some(target_class_id) = class_filter {
            query = query.filter(class_id.eq(target_class_id));
        }
        crate::apply_query_options!(query, query_options, ComputedFieldDefinition);
        Ok(query
            .select(ComputedFieldDefinition::as_select())
            .load(conn)
            .await?)
    })
    .await?;
    Ok((rows, count))
}

pub async fn get_computed_definition(
    pool: &DbPool,
    definition_id: i32,
) -> Result<ComputedFieldDefinition, ApiError> {
    use crate::schema::computed_field_definitions::dsl::{computed_field_definitions, id};
    with_connection(pool, async |conn| {
        computed_field_definitions
            .filter(id.eq(definition_id))
            .select(ComputedFieldDefinition::as_select())
            .first(conn)
            .await
    })
    .await
}

fn computed_field_event(
    definition: &ComputedFieldDefinition,
    class: &HubuumClass,
    action: Action,
    context: &EventContext,
    summary: String,
) -> Result<NewEvent, ApiError> {
    Ok(NewEvent::new(
        EntityType::ComputedFieldDefinition,
        action,
        context.actor_kind(),
        summary,
    )?
    .with_context(context)
    .with_entity_id(definition.id)
    .with_entity_name(definition.key.clone())
    .with_collection_id(class.collection_id)
    .with_metadata(serde_json::json!({ "class_id": class.id })))
}

async fn class_record(
    conn: &mut DbConnection,
    target_class_id: i32,
) -> Result<HubuumClass, ApiError> {
    use crate::schema::hubuumclass::dsl::{hubuumclass, id};
    Ok(hubuumclass
        .filter(id.eq(target_class_id))
        .first::<HubuumClass>(conn)
        .await?)
}

async fn locked_class_record_in_collection(
    conn: &mut DbConnection,
    target_class_id: i32,
    authorized_collection_id: i32,
) -> Result<HubuumClass, ApiError> {
    use crate::schema::hubuumclass::dsl::{hubuumclass, id};
    let class = hubuumclass
        .filter(id.eq(target_class_id))
        .for_share()
        .first::<HubuumClass>(conn)
        .await?;
    if class.collection_id != authorized_collection_id {
        return Err(ApiError::Conflict(format!(
            "Class {target_class_id} moved from collection {authorized_collection_id}; authorize the request again"
        )));
    }
    Ok(class)
}

async fn cancel_queued_reindex_tasks(
    conn: &mut DbConnection,
    target_class_id: i32,
    actor_id: Option<i32>,
) -> Result<(), ApiError> {
    let cancelled = diesel::sql_query(
        "UPDATE tasks SET status='cancelled', summary='Superseded by a newer computed-field rebuild', \
         finished_at=(clock_timestamp() AT TIME ZONE 'UTC'), request_payload=NULL, \
         request_redacted_at=(clock_timestamp() AT TIME ZONE 'UTC'), \
         updated_at=(clock_timestamp() AT TIME ZONE 'UTC') \
         WHERE kind='reindex' AND status='queued' \
           AND request_payload->>'type'='computed_fields' \
           AND request_payload->>'class_id'=$1 \
         RETURNING id",
    )
    .bind::<Text, _>(target_class_id.to_string())
    .load::<ReturnedTaskId>(conn)
    .await?;

    for task in cancelled {
        emit_internal_task_event(
            conn,
            &NewTaskEventRecord {
                task_id: task.id,
                event_type: TaskStatus::Cancelled.as_str().to_string(),
                message: "Computed-field rebuild superseded".to_string(),
                data: None,
            },
            actor_id,
            TaskKind::Reindex,
        )
        .await?;
    }
    Ok(())
}

async fn object_boundary(
    conn: &mut DbConnection,
    target_class_id: i32,
) -> Result<ObjectBoundary, ApiError> {
    Ok(diesel::sql_query(
        "SELECT COALESCE(MAX(id), 0)::int AS upper_bound, COUNT(*)::bigint AS total_items \
         FROM hubuumobject WHERE hubuum_class_id=$1",
    )
    .bind::<Integer, _>(target_class_id)
    .get_result(conn)
    .await?)
}

async fn enqueue_rebuild(
    conn: &mut DbConnection,
    target_class_id: i32,
    target_revision: i64,
    actor_id: Option<i32>,
) -> Result<ClassComputationState, ApiError> {
    use crate::schema::class_computation_state::dsl::{
        active_task_id, class_computation_state, class_id, last_error, rebuild_status, updated_at,
    };

    cancel_queued_reindex_tasks(conn, target_class_id, actor_id).await?;
    let boundary = object_boundary(conn, target_class_id).await?;
    let total_items = i32::try_from(boundary.total_items).unwrap_or(i32::MAX);
    let task = insert_internal_queued_task(
        conn,
        TaskKind::Reindex,
        serde_json::to_value(ComputedReindexPayload::new(
            target_class_id,
            target_revision,
            boundary.upper_bound,
        ))?,
        total_items,
        actor_id,
    )
    .await?;

    Ok(
        diesel::update(class_computation_state.filter(class_id.eq(target_class_id)))
            .set((
                rebuild_status.eq("rebuilding"),
                active_task_id.eq(Some(task.id)),
                last_error.eq::<Option<String>>(None),
                updated_at.eq(diesel::dsl::now),
            ))
            .returning(ClassComputationState::as_returning())
            .get_result(conn)
            .await?,
    )
}

async fn advance_revision_and_enqueue(
    conn: &mut DbConnection,
    target_class_id: i32,
    actor_id: Option<i32>,
) -> Result<ClassComputationState, ApiError> {
    use crate::schema::class_computation_state::dsl::{
        class_computation_state, class_id, evaluation_revision, updated_at,
    };

    ensure_computation_state(conn, target_class_id).await?;
    let revision = diesel::update(class_computation_state.filter(class_id.eq(target_class_id)))
        .set((
            evaluation_revision.eq(evaluation_revision + 1),
            updated_at.eq(diesel::dsl::now),
        ))
        .returning(evaluation_revision)
        .get_result::<i64>(conn)
        .await?;
    enqueue_rebuild(conn, target_class_id, revision, actor_id).await
}

pub async fn create_shared_definition(
    pool: &DbPool,
    target_class_id: i32,
    authorized_collection_id: i32,
    actor_id: i32,
    request: ComputedFieldDefinitionRequest,
    context: &EventContext,
) -> Result<ComputedFieldMutationResponse, ApiError> {
    let input = request.into_new_shared(target_class_id, actor_id)?;
    with_transaction(pool, async |conn| -> Result<_, ApiError> {
        acquire_computed_class_exclusive_lock(conn, target_class_id).await?;
        let class =
            locked_class_record_in_collection(conn, target_class_id, authorized_collection_id)
                .await?;
        use crate::schema::computed_field_definitions::dsl::{
            class_id, computed_field_definitions, visibility,
        };
        let count = computed_field_definitions
            .filter(class_id.eq(target_class_id))
            .filter(visibility.eq(COMPUTED_FIELD_VISIBILITY_SHARED))
            .count()
            .get_result::<i64>(conn)
            .await?;
        if count >= MAX_SHARED_DEFINITIONS as i64 {
            return Err(ApiError::BadRequest(format!(
                "A class may have at most {MAX_SHARED_DEFINITIONS} shared computed fields"
            )));
        }
        let definition = diesel::insert_into(computed_field_definitions)
            .values(input)
            .returning(ComputedFieldDefinition::as_returning())
            .get_result(conn)
            .await?;
        let state = advance_revision_and_enqueue(conn, target_class_id, Some(actor_id)).await?;
        let event = computed_field_event(
            &definition,
            &class,
            Action::Created,
            context,
            format!("Shared computed field '{}' created", definition.key),
        )?
        .with_after(serde_json::to_value(&definition)?);
        emit_event(conn, &event).await?;
        Ok(ComputedFieldMutationResponse { definition, state })
    })
    .await
}

async fn locked_definition(
    conn: &mut DbConnection,
    definition_id: i32,
) -> Result<ComputedFieldDefinition, ApiError> {
    use crate::schema::computed_field_definitions::dsl::{computed_field_definitions, id};
    computed_field_definitions
        .filter(id.eq(definition_id))
        .for_update()
        .select(ComputedFieldDefinition::as_select())
        .first(conn)
        .await
        .map_err(ApiError::from)
}

async fn apply_definition_patch(
    conn: &mut DbConnection,
    current: &ComputedFieldDefinition,
    patch: &ValidatedComputedFieldPatch,
    actor_id: i32,
) -> Result<ComputedFieldDefinition, ApiError> {
    use crate::schema::computed_field_definitions::dsl::{
        computed_field_definitions, description, enabled, id, key, label, operation, result_type,
        revision, updated_at, updated_by,
    };
    Ok(
        diesel::update(computed_field_definitions.filter(id.eq(current.id)))
            .set((
                key.eq(&patch.key),
                label.eq(&patch.label),
                description.eq(&patch.description),
                operation.eq(&patch.operation),
                result_type.eq(&patch.result_type),
                enabled.eq(patch.enabled),
                revision.eq(revision + 1),
                updated_by.eq(Some(actor_id)),
                updated_at.eq(diesel::dsl::now),
            ))
            .returning(ComputedFieldDefinition::as_returning())
            .get_result(conn)
            .await?,
    )
}

pub async fn update_shared_definition(
    pool: &DbPool,
    target_class_id: i32,
    authorized_collection_id: i32,
    definition_id: i32,
    actor_id: i32,
    patch: ComputedFieldDefinitionPatch,
    context: &EventContext,
) -> Result<ComputedFieldMutationResponse, ApiError> {
    with_transaction(pool, async |conn| -> Result<_, ApiError> {
        acquire_computed_class_exclusive_lock(conn, target_class_id).await?;
        let class =
            locked_class_record_in_collection(conn, target_class_id, authorized_collection_id)
                .await?;
        let current = locked_definition(conn, definition_id).await?;
        if current.class_id != target_class_id || !current.is_shared() {
            return Err(ApiError::NotFound(format!(
                "Shared computed field {definition_id} was not found in class {target_class_id}"
            )));
        }
        if current.revision != patch.expected_revision {
            return Err(ApiError::Conflict(format!(
                "Computed field revision is {}; expected {}",
                current.revision, patch.expected_revision
            )));
        }
        let validated = patch.validate_against(&current)?;
        let changed = validated.key != current.key
            || validated.label != current.label
            || validated.description != current.description
            || validated.operation != current.operation
            || validated.result_type != current.result_type
            || validated.enabled != current.enabled;
        if !changed {
            return Ok(ComputedFieldMutationResponse {
                definition: current,
                state: ensure_computation_state(conn, target_class_id).await?,
            });
        }
        let definition = apply_definition_patch(conn, &current, &validated, actor_id).await?;
        let state = if validated.value_affecting {
            advance_revision_and_enqueue(conn, target_class_id, Some(actor_id)).await?
        } else {
            ensure_computation_state(conn, target_class_id).await?
        };
        let event = computed_field_event(
            &definition,
            &class,
            Action::Updated,
            context,
            format!("Shared computed field '{}' updated", definition.key),
        )?
        .with_before(serde_json::to_value(&current)?)
        .with_after(serde_json::to_value(&definition)?);
        emit_event(conn, &event).await?;
        Ok(ComputedFieldMutationResponse { definition, state })
    })
    .await
}

pub async fn delete_shared_definition(
    pool: &DbPool,
    target_class_id: i32,
    authorized_collection_id: i32,
    definition_id: i32,
    actor_id: i32,
    expected_revision: i64,
    context: &EventContext,
) -> Result<ClassComputationState, ApiError> {
    with_transaction(pool, async |conn| -> Result<_, ApiError> {
        acquire_computed_class_exclusive_lock(conn, target_class_id).await?;
        let class =
            locked_class_record_in_collection(conn, target_class_id, authorized_collection_id)
                .await?;
        let current = locked_definition(conn, definition_id).await?;
        if current.class_id != target_class_id || !current.is_shared() {
            return Err(ApiError::NotFound(format!(
                "Shared computed field {definition_id} was not found in class {target_class_id}"
            )));
        }
        if current.revision != expected_revision {
            return Err(ApiError::Conflict(format!(
                "Computed field revision is {}; expected {expected_revision}",
                current.revision
            )));
        }
        use crate::schema::computed_field_definitions::dsl::{computed_field_definitions, id};
        diesel::delete(computed_field_definitions.filter(id.eq(definition_id)))
            .execute(conn)
            .await?;
        let state = advance_revision_and_enqueue(conn, target_class_id, Some(actor_id)).await?;
        let event = computed_field_event(
            &current,
            &class,
            Action::Deleted,
            context,
            format!("Shared computed field '{}' deleted", current.key),
        )?
        .with_before(serde_json::to_value(&current)?);
        emit_event(conn, &event).await?;
        Ok(state)
    })
    .await
}

pub async fn create_personal_definition(
    pool: &DbPool,
    target_class_id: i32,
    owner_id: i32,
    request: ComputedFieldDefinitionRequest,
) -> Result<ComputedFieldDefinition, ApiError> {
    let input = request.into_new_personal(target_class_id, owner_id)?;
    with_transaction(pool, async |conn| -> Result<_, ApiError> {
        acquire_personal_definition_scope_lock(conn, target_class_id, owner_id).await?;
        let _ = class_record(conn, target_class_id).await?;
        use crate::schema::computed_field_definitions::dsl::{
            class_id, computed_field_definitions, owner_user_id, visibility,
        };
        let count = computed_field_definitions
            .filter(class_id.eq(target_class_id))
            .filter(owner_user_id.eq(Some(owner_id)))
            .filter(visibility.eq(COMPUTED_FIELD_VISIBILITY_PERSONAL))
            .count()
            .get_result::<i64>(conn)
            .await?;
        if count >= MAX_PERSONAL_DEFINITIONS as i64 {
            return Err(ApiError::BadRequest(format!(
                "A user may have at most {MAX_PERSONAL_DEFINITIONS} personal computed fields per class"
            )));
        }
        Ok(diesel::insert_into(computed_field_definitions)
            .values(input)
            .returning(ComputedFieldDefinition::as_returning())
            .get_result(conn)
            .await?)
    })
    .await
}

pub async fn update_personal_definition(
    pool: &DbPool,
    owner_id: i32,
    definition_id: i32,
    patch: ComputedFieldDefinitionPatch,
) -> Result<ComputedFieldDefinition, ApiError> {
    with_transaction(pool, async |conn| -> Result<_, ApiError> {
        let current = locked_definition(conn, definition_id).await?;
        if !current.is_personal_for(owner_id) {
            return Err(ApiError::NotFound(format!(
                "Personal computed field {definition_id} was not found"
            )));
        }
        if current.revision != patch.expected_revision {
            return Err(ApiError::Conflict(format!(
                "Computed field revision is {}; expected {}",
                current.revision, patch.expected_revision
            )));
        }
        let validated = patch.validate_against(&current)?;
        let changed = validated.key != current.key
            || validated.label != current.label
            || validated.description != current.description
            || validated.operation != current.operation
            || validated.result_type != current.result_type
            || validated.enabled != current.enabled;
        if !changed {
            return Ok(current);
        }
        apply_definition_patch(conn, &current, &validated, owner_id).await
    })
    .await
}

pub async fn delete_personal_definition(
    pool: &DbPool,
    owner_id: i32,
    definition_id: i32,
    expected_revision: i64,
) -> Result<(), ApiError> {
    with_transaction(pool, async |conn| -> Result<_, ApiError> {
        let current = locked_definition(conn, definition_id).await?;
        if !current.is_personal_for(owner_id) {
            return Err(ApiError::NotFound(format!(
                "Personal computed field {definition_id} was not found"
            )));
        }
        if current.revision != expected_revision {
            return Err(ApiError::Conflict(format!(
                "Computed field revision is {}; expected {expected_revision}",
                current.revision
            )));
        }
        use crate::schema::computed_field_definitions::dsl::{computed_field_definitions, id};
        diesel::delete(computed_field_definitions.filter(id.eq(definition_id)))
            .execute(conn)
            .await?;
        Ok(())
    })
    .await
}

pub async fn request_class_rebuild(
    pool: &DbPool,
    target_class_id: i32,
    authorized_collection_id: i32,
    actor_id: Option<i32>,
) -> Result<ClassComputationState, ApiError> {
    with_transaction(pool, async |conn| -> Result<_, ApiError> {
        acquire_computed_class_exclusive_lock(conn, target_class_id).await?;
        let _ = locked_class_record_in_collection(conn, target_class_id, authorized_collection_id)
            .await?;
        let state = ensure_computation_state(conn, target_class_id).await?;
        if let Some(task_id_value) = state.active_task_id {
            use crate::schema::tasks::dsl::{id, request_payload, status, tasks};
            let active = tasks
                .filter(id.eq(task_id_value))
                .filter(status.eq_any([
                    TaskStatus::Queued.as_str(),
                    TaskStatus::Validating.as_str(),
                    TaskStatus::Running.as_str(),
                ]))
                .select(request_payload)
                .first::<Option<serde_json::Value>>(conn)
                .await
                .optional()?
                .flatten()
                .and_then(|value| serde_json::from_value::<ComputedReindexPayload>(value).ok())
                .is_some_and(|payload| {
                    payload.payload_type == REINDEX_PAYLOAD_TYPE
                        && payload.class_id == target_class_id
                        && payload.target_revision == state.evaluation_revision
                });
            if active {
                return Ok(state);
            }
        }
        enqueue_rebuild(conn, target_class_id, state.evaluation_revision, actor_id).await
    })
    .await
}

pub(crate) async fn enqueue_restored_computed_rebuilds(
    conn: &mut DbConnection,
) -> Result<(), ApiError> {
    use crate::schema::computed_field_definitions::dsl::{
        class_id, computed_field_definitions, visibility,
    };
    let class_ids = computed_field_definitions
        .filter(visibility.eq(COMPUTED_FIELD_VISIBILITY_SHARED))
        .select(class_id)
        .distinct()
        .order(class_id.asc())
        .load::<i32>(conn)
        .await?;
    for target_class_id in class_ids {
        acquire_computed_class_exclusive_lock(conn, target_class_id).await?;
        advance_revision_and_enqueue(conn, target_class_id, None).await?;
    }
    Ok(())
}

fn shared_definitions_from_rows(
    definitions: &[ComputedFieldDefinition],
) -> Result<Vec<hubuum_computed_fields::Definition>, ApiError> {
    definitions
        .iter()
        .filter(|definition| definition.enabled)
        .map(ComputedFieldDefinition::evaluator_definition)
        .collect()
}

fn evaluate_definitions(
    data: &serde_json::Value,
    definitions: &[ComputedFieldDefinition],
    maximum: usize,
    scope: &'static str,
) -> Result<EvaluationResult, ApiError> {
    let definitions = shared_definitions_from_rows(definitions)?;
    let result =
        evaluate(data, &definitions, maximum, EvaluationLimits::standard()).map_err(|error| {
            ApiError::InternalServerError(format!("Computed-field evaluation failed: {error}"))
        })?;
    crate::observability::metrics::computed_evaluation(scope, &result);
    Ok(result)
}

pub fn preview_computed_definition(
    data: &serde_json::Value,
    request: &ComputedFieldDefinitionRequest,
) -> Result<crate::models::ComputedFieldPreviewResponse, ApiError> {
    let definition = request.validate()?;
    let result =
        evaluate(data, &[definition], 1, EvaluationLimits::standard()).map_err(|error| {
            ApiError::InternalServerError(format!("Computed-field preview failed: {error}"))
        })?;
    crate::observability::metrics::computed_evaluation("preview", &result);
    Ok(crate::models::ComputedFieldPreviewResponse {
        value: result
            .values
            .get(&request.key)
            .cloned()
            .unwrap_or(serde_json::Value::Null),
        error: result
            .errors
            .get(&request.key)
            .cloned()
            .map(ComputedFieldErrorResponse::from),
    })
}

fn canonical_json(value: &serde_json::Value, output: &mut String) -> Result<(), ApiError> {
    match value {
        serde_json::Value::Object(values) => {
            output.push('{');
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                output.push_str(&serde_json::to_string(key)?);
                output.push(':');
                canonical_json(&values[key], output)?;
            }
            output.push('}');
        }
        serde_json::Value::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                canonical_json(value, output)?;
            }
            output.push(']');
        }
        _ => output.push_str(&serde_json::to_string(value)?),
    }
    Ok(())
}

pub fn source_data_sha256(data: &serde_json::Value) -> Result<String, ApiError> {
    let mut canonical = String::new();
    canonical_json(data, &mut canonical)?;
    Ok(Sha256::digest(canonical.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

async fn upsert_materialized(
    conn: &mut DbConnection,
    object: &HubuumObject,
    revision: i64,
    result: EvaluationResult,
) -> Result<(), ApiError> {
    use crate::schema::object_computed_data::dsl::{
        class_id, computed_at, errors, evaluation_revision, object_computed_data, object_id,
        source_data_sha256 as stored_hash, values,
    };
    let input = NewObjectComputedData {
        object_id: object.id,
        class_id: object.hubuum_class_id,
        evaluation_revision: revision,
        source_data_sha256: source_data_sha256(&object.data)?,
        values: serde_json::to_value(result.values)?,
        errors: serde_json::to_value(result.errors)?,
    };
    diesel::insert_into(object_computed_data)
        .values(&input)
        .on_conflict(object_id)
        .do_update()
        .set((
            class_id.eq(input.class_id),
            evaluation_revision.eq(input.evaluation_revision),
            stored_hash.eq(&input.source_data_sha256),
            values.eq(&input.values),
            errors.eq(&input.errors),
            computed_at.eq(diesel::dsl::now),
        ))
        .execute(conn)
        .await?;
    Ok(())
}

async fn shared_definitions_conn(
    conn: &mut DbConnection,
    target_class_id: i32,
) -> Result<Vec<ComputedFieldDefinition>, ApiError> {
    use crate::schema::computed_field_definitions::dsl::{
        class_id, computed_field_definitions, id, visibility,
    };
    Ok(computed_field_definitions
        .filter(class_id.eq(target_class_id))
        .filter(visibility.eq(COMPUTED_FIELD_VISIBILITY_SHARED))
        .order(id.asc())
        .select(ComputedFieldDefinition::as_select())
        .load(conn)
        .await?)
}

/// Materialize one canonical object inside the caller's write transaction.
pub(crate) async fn materialize_object_in_transaction(
    conn: &mut DbConnection,
    object: &HubuumObject,
) -> Result<(), ApiError> {
    acquire_computed_class_shared_lock(conn, object.hubuum_class_id).await?;
    let definitions = shared_definitions_conn(conn, object.hubuum_class_id).await?;
    if definitions.is_empty() {
        use crate::schema::object_computed_data::dsl::{object_computed_data, object_id};
        diesel::delete(object_computed_data.filter(object_id.eq(object.id)))
            .execute(conn)
            .await?;
        return Ok(());
    }
    let state = ensure_computation_state(conn, object.hubuum_class_id).await?;
    let result =
        evaluate_definitions(&object.data, &definitions, MAX_SHARED_DEFINITIONS, "shared")?;
    upsert_materialized(conn, object, state.evaluation_revision, result).await
}

type ResponseEvaluationMaps = (
    BTreeMap<String, serde_json::Value>,
    BTreeMap<String, ComputedFieldErrorResponse>,
);

#[derive(Clone, Copy)]
enum MaterializationRepair {
    Apply,
    Defer,
}

fn evaluation_maps(result: EvaluationResult) -> ResponseEvaluationMaps {
    (
        result.values,
        result
            .errors
            .into_iter()
            .map(|(key, error)| (key, error.into()))
            .collect(),
    )
}

fn stored_evaluation_maps(row: &ObjectComputedData) -> Result<ResponseEvaluationMaps, ApiError> {
    Ok((
        serde_json::from_value(row.values.clone()).map_err(|error| {
            ApiError::InternalServerError(format!("Stored computed values are invalid: {error}"))
        })?,
        serde_json::from_value::<BTreeMap<String, hubuum_computed_fields::FieldError>>(
            row.errors.clone(),
        )
        .map_err(|error| {
            ApiError::InternalServerError(format!("Stored computed errors are invalid: {error}"))
        })?
        .into_iter()
        .map(|(key, error)| (key, error.into()))
        .collect(),
    ))
}

pub async fn enrich_objects_with_computed(
    pool: &DbPool,
    objects: Vec<HubuumObject>,
    personal_owner_id: Option<i32>,
) -> Result<Vec<HubuumObjectComputedResponse>, ApiError> {
    if objects.is_empty() {
        return Ok(Vec::new());
    }
    let class_ids = objects
        .iter()
        .map(|object| object.hubuum_class_id)
        .collect::<BTreeSet<_>>();
    let object_ids = objects.iter().map(|object| object.id).collect::<Vec<_>>();
    let class_id_values = class_ids.iter().copied().collect::<Vec<_>>();

    let (definitions, states, materialized) =
        with_transaction(pool, async |conn| -> Result<_, ApiError> {
            for target_class_id in &class_ids {
                acquire_computed_class_shared_lock(conn, *target_class_id).await?;
            }
            use crate::schema::class_computation_state::dsl as state;
            use crate::schema::computed_field_definitions::dsl as definition;
            use crate::schema::object_computed_data::dsl as computed;
            let mut definition_query = definition::computed_field_definitions
                .filter(definition::class_id.eq_any(&class_id_values))
                .into_boxed();
            definition_query = match personal_owner_id {
                Some(owner_id) => definition_query.filter(
                    definition::visibility
                        .eq(COMPUTED_FIELD_VISIBILITY_SHARED)
                        .or(definition::visibility
                            .eq(COMPUTED_FIELD_VISIBILITY_PERSONAL)
                            .and(definition::owner_user_id.eq(Some(owner_id)))),
                ),
                None => definition_query
                    .filter(definition::visibility.eq(COMPUTED_FIELD_VISIBILITY_SHARED)),
            };
            let definitions = definition_query
                .order((definition::class_id.asc(), definition::id.asc()))
                .select(ComputedFieldDefinition::as_select())
                .load::<ComputedFieldDefinition>(conn)
                .await?;
            let states = state::class_computation_state
                .filter(state::class_id.eq_any(&class_id_values))
                .select(ClassComputationState::as_select())
                .load::<ClassComputationState>(conn)
                .await?;
            let materialized = computed::object_computed_data
                .filter(computed::object_id.eq_any(&object_ids))
                .select(ObjectComputedData::as_select())
                .load::<ObjectComputedData>(conn)
                .await?;
            Ok((definitions, states, materialized))
        })
        .await?;

    enrich_objects_from_snapshot(
        pool,
        objects,
        personal_owner_id,
        definitions,
        states,
        materialized,
        MaterializationRepair::Apply,
    )
    .await
}

pub async fn enrich_objects_with_computed_sort_snapshot(
    pool: &DbPool,
    objects: Vec<HubuumObject>,
    personal_owner_id: Option<i32>,
    snapshot: &ComputedSortSnapshot,
) -> Result<Vec<HubuumObjectComputedResponse>, ApiError> {
    if objects.is_empty() {
        return Ok(Vec::new());
    }
    if objects
        .iter()
        .any(|object| object.hubuum_class_id != snapshot.class_id)
    {
        return Err(ApiError::InternalServerError(
            "Computed sort snapshot cannot enrich objects from another class".to_string(),
        ));
    }
    let object_ids = objects.iter().map(|object| object.id).collect::<Vec<_>>();
    let materialized = with_connection(pool, async |conn| {
        use crate::schema::object_computed_data::dsl as computed;
        computed::object_computed_data
            .filter(computed::object_id.eq_any(&object_ids))
            .select(ObjectComputedData::as_select())
            .load::<ObjectComputedData>(conn)
            .await
    })
    .await?;

    enrich_objects_from_snapshot(
        pool,
        objects,
        personal_owner_id,
        snapshot.definitions.clone(),
        vec![snapshot.state.clone()],
        materialized,
        MaterializationRepair::Defer,
    )
    .await
}

async fn enrich_objects_from_snapshot(
    pool: &DbPool,
    objects: Vec<HubuumObject>,
    personal_owner_id: Option<i32>,
    definitions: Vec<ComputedFieldDefinition>,
    states: Vec<ClassComputationState>,
    materialized: Vec<ObjectComputedData>,
    repair: MaterializationRepair,
) -> Result<Vec<HubuumObjectComputedResponse>, ApiError> {
    let mut shared_by_class: HashMap<i32, Vec<ComputedFieldDefinition>> = HashMap::new();
    let mut personal_by_class: HashMap<i32, Vec<ComputedFieldDefinition>> = HashMap::new();
    for definition in definitions {
        if definition.is_shared() {
            shared_by_class
                .entry(definition.class_id)
                .or_default()
                .push(definition);
        } else if personal_owner_id.is_some_and(|owner| definition.is_personal_for(owner)) {
            personal_by_class
                .entry(definition.class_id)
                .or_default()
                .push(definition);
        }
    }
    let states = states
        .into_iter()
        .map(|state| (state.class_id, state))
        .collect::<HashMap<_, _>>();
    let materialized = materialized
        .into_iter()
        .map(|row| (row.object_id, row))
        .collect::<HashMap<_, _>>();

    let mut stale_objects = Vec::new();
    let mut enriched = Vec::with_capacity(objects.len());
    for object in objects {
        let state = states
            .get(&object.hubuum_class_id)
            .cloned()
            .unwrap_or_else(|| {
                ClassComputationState::ready_without_definitions(object.hubuum_class_id)
            });
        let definitions = shared_by_class
            .get(&object.hubuum_class_id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let hash = source_data_sha256(&object.data)?;
        let stored = materialized.get(&object.id);
        let fresh = definitions.is_empty()
            || stored.is_some_and(|row| {
                row.class_id == object.hubuum_class_id
                    && row.evaluation_revision == state.evaluation_revision
                    && row.source_data_sha256 == hash
            });
        let (shared_values, shared_errors) = if definitions.is_empty() {
            (BTreeMap::new(), BTreeMap::new())
        } else if fresh {
            stored_evaluation_maps(stored.expect("fresh materialization exists"))?
        } else {
            if matches!(repair, MaterializationRepair::Apply) {
                stale_objects.push(object.clone());
            }
            crate::observability::metrics::computed_live_fallback();
            evaluation_maps(evaluate_definitions(
                &object.data,
                definitions,
                MAX_SHARED_DEFINITIONS,
                "shared",
            )?)
        };

        let personal = if personal_owner_id.is_some() {
            let definitions = personal_by_class
                .get(&object.hubuum_class_id)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let result = evaluate_definitions(
                &object.data,
                definitions,
                MAX_PERSONAL_DEFINITIONS,
                "personal",
            )?;
            let (values, errors) = evaluation_maps(result);
            Some(ComputedScopeResponse { values, errors })
        } else {
            None
        };
        enriched.push(HubuumObjectComputedResponse {
            object,
            computed: ComputedObjectScopesResponse {
                shared: SharedComputedScopeResponse {
                    revision: state.evaluation_revision,
                    materialization_stale: !fresh,
                    values: shared_values,
                    errors: shared_errors,
                },
                personal,
            },
        });
    }

    if !stale_objects.is_empty() {
        match repair_stale_materializations(pool, stale_objects).await {
            Ok(()) => crate::observability::metrics::computed_read_repair("success"),
            Err(error) => {
                crate::observability::metrics::computed_read_repair("failure");
                warn!(message = "Computed-field read repair failed", error = %error);
            }
        }
    }
    Ok(enriched)
}

async fn repair_stale_materializations(
    pool: &DbPool,
    mut stale_objects: Vec<HubuumObject>,
) -> Result<(), ApiError> {
    stale_objects.sort_by_key(|object| object.id);
    let object_ids = stale_objects
        .iter()
        .map(|object| object.id)
        .collect::<Vec<_>>();
    with_transaction(pool, async |conn| -> Result<_, ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};
        let current_objects = hubuumobject
            .filter(id.eq_any(&object_ids))
            .order(id.asc())
            .for_update()
            .load::<HubuumObject>(conn)
            .await?;

        // Object writes lock their row before taking the class advisory lock.
        // Repairs and rebuild batches use the same order so an exclusive
        // definition update cannot form a row/advisory-lock cycle.
        let class_ids = current_objects
            .iter()
            .map(|object| object.hubuum_class_id)
            .collect::<BTreeSet<_>>();
        for class_id in &class_ids {
            acquire_computed_class_shared_lock(conn, *class_id).await?;
        }

        let class_id_values = class_ids.iter().copied().collect::<Vec<_>>();
        use crate::schema::computed_field_definitions::dsl as definition;
        let definitions = definition::computed_field_definitions
            .filter(definition::class_id.eq_any(&class_id_values))
            .filter(definition::visibility.eq(COMPUTED_FIELD_VISIBILITY_SHARED))
            .order((definition::class_id.asc(), definition::id.asc()))
            .select(ComputedFieldDefinition::as_select())
            .load::<ComputedFieldDefinition>(conn)
            .await?;
        let mut definitions_by_class: HashMap<i32, Vec<ComputedFieldDefinition>> = HashMap::new();
        for definition in definitions {
            definitions_by_class
                .entry(definition.class_id)
                .or_default()
                .push(definition);
        }
        let mut revisions = HashMap::new();
        for class_id in class_ids {
            let state = ensure_computation_state(conn, class_id).await?;
            revisions.insert(class_id, state.evaluation_revision);
        }

        let mut without_definitions = Vec::new();
        for object in current_objects {
            let definitions = definitions_by_class
                .get(&object.hubuum_class_id)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            if definitions.is_empty() {
                without_definitions.push(object.id);
                continue;
            }
            let result =
                evaluate_definitions(&object.data, definitions, MAX_SHARED_DEFINITIONS, "shared")?;
            upsert_materialized(conn, &object, revisions[&object.hubuum_class_id], result).await?;
        }
        if !without_definitions.is_empty() {
            use crate::schema::object_computed_data::dsl::{object_computed_data, object_id};
            diesel::delete(object_computed_data.filter(object_id.eq_any(without_definitions)))
                .execute(conn)
                .await?;
        }
        Ok(())
    })
    .await
}

enum ReindexBatch {
    Superseded,
    Rows { last_id: i32, count: i32 },
    Complete,
}

async fn process_reindex_batch(
    pool: &DbPool,
    task: &TaskRecord,
    payload: &ComputedReindexPayload,
    cursor: i32,
) -> Result<ReindexBatch, ApiError> {
    with_transaction(pool, async |conn| -> Result<_, ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuum_class_id, hubuumobject, id};
        let objects = hubuumobject
            .filter(hubuum_class_id.eq(payload.class_id))
            .filter(id.gt(cursor))
            .filter(id.le(payload.object_upper_bound))
            .order(id.asc())
            .limit(reindex_batch_size())
            .for_update()
            .load::<HubuumObject>(conn)
            .await?;
        acquire_computed_class_shared_lock(conn, payload.class_id).await?;
        let state = ensure_computation_state(conn, payload.class_id).await?;
        if state.evaluation_revision != payload.target_revision
            || state.active_task_id != Some(task.id)
        {
            return Ok(ReindexBatch::Superseded);
        }
        if objects.is_empty() {
            return Ok(ReindexBatch::Complete);
        }
        let definitions = shared_definitions_conn(conn, payload.class_id).await?;
        for object in &objects {
            if definitions.is_empty() {
                use crate::schema::object_computed_data::dsl::{object_computed_data, object_id};
                diesel::delete(object_computed_data.filter(object_id.eq(object.id)))
                    .execute(conn)
                    .await?;
            } else {
                let result = evaluate_definitions(
                    &object.data,
                    &definitions,
                    MAX_SHARED_DEFINITIONS,
                    "shared",
                )?;
                upsert_materialized(conn, object, payload.target_revision, result).await?;
            }
        }
        Ok(ReindexBatch::Rows {
            last_id: objects.last().expect("non-empty batch").id,
            count: objects.len() as i32,
        })
    })
    .await
}

pub async fn execute_computed_reindex_task(
    pool: &DbPool,
    task: &TaskRecord,
) -> Result<(), ApiError> {
    let started = Instant::now();
    let payload: ComputedReindexPayload = serde_json::from_value(
        task.request_payload
            .clone()
            .ok_or_else(|| ApiError::BadRequest("Reindex task payload is missing".to_string()))?,
    )?;
    payload.validate()?;
    let mut cursor = 0;
    let mut processed = 0;
    loop {
        match process_reindex_batch(pool, task, &payload, cursor).await? {
            ReindexBatch::Superseded => {
                task.finalize_terminal(
                    pool,
                    TaskStateUpdate {
                        status: TaskStatus::Cancelled,
                        summary: Some("Computed-field rebuild superseded".to_string()),
                        processed_items: processed,
                        success_items: processed,
                        failed_items: 0,
                        started_at: task.started_at,
                        finished_at: None,
                    },
                    NewTaskEventRecord {
                        task_id: task.id,
                        event_type: TaskStatus::Cancelled.as_str().to_string(),
                        message: "Computed-field rebuild superseded".to_string(),
                        data: None,
                    },
                )
                .await?;
                crate::observability::metrics::computed_rebuild_finished(
                    "cancelled",
                    started.elapsed(),
                );
                return Ok(());
            }
            ReindexBatch::Rows { last_id, count } => {
                crate::observability::metrics::computed_rebuild_batch(count as usize);
                cursor = last_id;
                processed = processed.saturating_add(count);
                task.update_state(
                    pool,
                    TaskStateUpdate {
                        status: TaskStatus::Running,
                        summary: Some(format!(
                            "Rebuilt {processed} of {} objects",
                            task.total_items
                        )),
                        processed_items: processed,
                        success_items: processed,
                        failed_items: 0,
                        started_at: task.started_at,
                        finished_at: None,
                    },
                )
                .await?;
            }
            ReindexBatch::Complete => {
                crate::observability::metrics::computed_rebuild_batch(0);
                break;
            }
        }
    }

    let ready = with_transaction(pool, async |conn| -> Result<bool, ApiError> {
        acquire_computed_class_shared_lock(conn, payload.class_id).await?;
        use crate::schema::class_computation_state::dsl::{
            active_task_id, class_computation_state, class_id, evaluation_revision, last_error,
            rebuild_status, updated_at,
        };
        let changed = diesel::update(
            class_computation_state
                .filter(class_id.eq(payload.class_id))
                .filter(evaluation_revision.eq(payload.target_revision))
                .filter(active_task_id.eq(Some(task.id))),
        )
        .set((
            rebuild_status.eq("ready"),
            active_task_id.eq::<Option<i32>>(None),
            last_error.eq::<Option<String>>(None),
            updated_at.eq(diesel::dsl::now),
        ))
        .execute(conn)
        .await?;
        Ok(changed == 1)
    })
    .await?;
    let (status, summary) = if ready {
        (
            TaskStatus::Succeeded,
            format!("Computed-field rebuild completed for {processed} objects"),
        )
    } else {
        (
            TaskStatus::Cancelled,
            "Computed-field rebuild superseded before completion".to_string(),
        )
    };
    task.finalize_terminal(
        pool,
        TaskStateUpdate {
            status,
            summary: Some(summary.clone()),
            processed_items: processed,
            success_items: processed,
            failed_items: 0,
            started_at: task.started_at,
            finished_at: None,
        },
        NewTaskEventRecord {
            task_id: task.id,
            event_type: status.as_str().to_string(),
            message: summary,
            data: None,
        },
    )
    .await?;
    crate::observability::metrics::computed_rebuild_finished(status.as_str(), started.elapsed());
    info!(
        message = "Computed-field rebuild finished",
        task_id = task.id,
        class_id = payload.class_id,
        revision = payload.target_revision,
        processed
    );
    Ok(())
}

pub async fn mark_computed_reindex_failed(
    pool: &DbPool,
    task: &TaskRecord,
    stored_error: &str,
) -> Result<(), ApiError> {
    let Some(payload) = task
        .request_payload
        .clone()
        .and_then(|value| serde_json::from_value::<ComputedReindexPayload>(value).ok())
    else {
        return Ok(());
    };
    with_connection(pool, async |conn| {
        use crate::schema::class_computation_state::dsl::{
            active_task_id, class_computation_state, class_id, evaluation_revision, last_error,
            rebuild_status, updated_at,
        };
        diesel::update(
            class_computation_state
                .filter(class_id.eq(payload.class_id))
                .filter(evaluation_revision.eq(payload.target_revision))
                .filter(active_task_id.eq(Some(task.id))),
        )
        .set((
            rebuild_status.eq("failed"),
            active_task_id.eq::<Option<i32>>(None),
            last_error.eq(Some(stored_error.chars().take(512).collect::<String>())),
            updated_at.eq(diesel::dsl::now),
        ))
        .execute(conn)
        .await
    })
    .await?;
    crate::observability::metrics::computed_rebuild_finished("failed", std::time::Duration::ZERO);
    Ok(())
}

pub(crate) async fn mark_recovered_computed_reindex_failed(
    conn: &mut DbConnection,
    task_id: i32,
    stored_error: &str,
) -> Result<(), ApiError> {
    use crate::schema::class_computation_state::dsl::{
        active_task_id, class_computation_state, last_error, rebuild_status, updated_at,
    };
    diesel::update(class_computation_state.filter(active_task_id.eq(Some(task_id))))
        .set((
            rebuild_status.eq("failed"),
            active_task_id.eq::<Option<i32>>(None),
            last_error.eq(Some(stored_error.chars().take(512).collect::<String>())),
            updated_at.eq(diesel::dsl::now),
        ))
        .execute(conn)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::search::parse_query_parameter;

    #[test]
    fn canonical_hash_ignores_object_key_order() {
        let left = serde_json::json!({"a": 1, "b": {"c": 2, "d": 3}});
        let right = serde_json::json!({"b": {"d": 3, "c": 2}, "a": 1});

        assert_eq!(
            source_data_sha256(&left).unwrap(),
            source_data_sha256(&right).unwrap()
        );
    }

    #[test]
    fn canonical_hash_preserves_array_order() {
        assert_ne!(
            source_data_sha256(&serde_json::json!([1, 2])).unwrap(),
            source_data_sha256(&serde_json::json!([2, 1])).unwrap()
        );
    }

    #[test]
    fn computed_sort_request_size_is_bounded_before_sql_resolution() {
        let sorts = parse_query_parameter("sort=computed.shared.first,computed.shared.second,name")
            .unwrap()
            .sort;

        let error = validate_computed_sort_count(sorts.len()).unwrap_err();

        assert_eq!(
            error.to_string(),
            "Computed sorting supports at most 2 explicit sort fields per request"
        );
    }
}
