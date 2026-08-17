//! PostgreSQL planning for object-list filters over the relation graph.

use std::collections::BTreeMap;

use hubuum_query::{
    DEFAULT_RELATED_FILTER_DEPTH, FilterField, MAX_INTEGER_FILTER_VALUES, MAX_RELATED_FILTER_DEPTH,
    MAX_RELATED_FILTER_GROUPS, Operator, ParsedQueryParam, RelatedClassField, RelatedFilterTarget,
    RelatedObjectField, SearchOperator,
};
use hubuum_storage_core::{AuthorizationPermission, StorageVisibility};

use crate::operations::dynamic_sql::{
    BoundSqlPredicate, SqlComponent, SqlValue, bound_sql_predicate,
};
use crate::operations::json_filter::json_filter_sql;
use crate::operations::visibility::authorized_collection_ids;
use crate::{PostgresConnection, PostgresStorageError};

struct RelatedFilterGroup<'filter> {
    class_filter: &'filter ParsedQueryParam,
    object_filters: Vec<(&'filter ParsedQueryParam, RelatedObjectField)>,
    max_depth: i32,
}

/// Build one bound graph predicate for every related-filter group in an
/// ordinary object catalog query.
pub(crate) async fn related_object_filter_predicate(
    connection: &mut PostgresConnection,
    filters: &[ParsedQueryParam],
    visibility: &StorageVisibility,
) -> Result<Option<BoundSqlPredicate>, PostgresStorageError> {
    let groups = related_filter_groups(filters)?;
    if groups.is_empty() {
        return Ok(None);
    }
    tracing::debug!(
        operation = "plan_related_object_filters",
        authorization = "sql_pushdown",
        group_count = groups.len(),
        max_depths = ?groups.iter().map(|group| group.max_depth).collect::<Vec<_>>(),
        "planning PostgreSQL related-object catalog filters"
    );

    let graph_permissions = [
        AuthorizationPermission::ReadObject,
        AuthorizationPermission::ReadObjectRelation,
    ];
    let class_permissions = [
        AuthorizationPermission::ReadClass,
        AuthorizationPermission::ReadCollection,
    ];
    if !visibility.allows_permissions(&graph_permissions)
        || !visibility.allows_permissions(&class_permissions)
    {
        return bound_sql_predicate(SqlComponent {
            sql: "FALSE".to_string(),
            bind_variables: Vec::new(),
        })
        .map(Some);
    }

    let graph_collection_ids =
        authorized_collection_ids(connection, visibility, &graph_permissions).await?;
    let class_collection_ids =
        authorized_collection_ids(connection, visibility, &class_permissions).await?;
    if graph_collection_ids.is_empty() || class_collection_ids.is_empty() {
        return bound_sql_predicate(SqlComponent {
            sql: "FALSE".to_string(),
            bind_variables: Vec::new(),
        })
        .map(Some);
    }

    bound_sql_predicate(build_related_object_filter_sql(
        &groups,
        &graph_collection_ids,
        &class_collection_ids,
        visibility,
    )?)
    .map(Some)
}

fn related_filter_groups(
    filters: &[ParsedQueryParam],
) -> Result<Vec<RelatedFilterGroup<'_>>, PostgresStorageError> {
    #[derive(Default)]
    struct PendingGroup<'filter> {
        class_filters: Vec<&'filter ParsedQueryParam>,
        object_filters: Vec<(&'filter ParsedQueryParam, RelatedObjectField)>,
        depth_filters: Vec<&'filter ParsedQueryParam>,
    }

    let mut groups = BTreeMap::<&str, PendingGroup<'_>>::new();
    for filter in filters {
        let Some(field) = filter.field.related_query() else {
            continue;
        };
        let group = groups.entry(field.alias()).or_default();
        match field.target() {
            RelatedFilterTarget::Class(_) => group.class_filters.push(filter),
            RelatedFilterTarget::Object(object_field) => {
                if let Some(data_type) = object_field.data_type()
                    && !filter.operator.is_applicable_to(data_type)
                {
                    return Err(PostgresStorageError::bad_request(format!(
                        "Operator '{}' is not applicable to related object field '{}'",
                        filter.operator,
                        object_field.as_str()
                    )));
                }
                group.object_filters.push((filter, object_field));
            }
            RelatedFilterTarget::Depth => group.depth_filters.push(filter),
        }
    }
    if groups.len() > MAX_RELATED_FILTER_GROUPS {
        return Err(PostgresStorageError::bad_request(format!(
            "query accepts at most {MAX_RELATED_FILTER_GROUPS} related filter groups"
        )));
    }

    groups
        .into_iter()
        .map(|(alias, group)| {
            if group.class_filters.len() != 1 {
                return Err(PostgresStorageError::bad_request(format!(
                    "Related filter group '{alias}' requires exactly one class.id or class.name selector"
                )));
            }
            let class_filter = group.class_filters[0];
            if class_filter.operator != (SearchOperator::Equals { is_negated: false }) {
                return Err(PostgresStorageError::bad_request(format!(
                    "Related filter group '{alias}' requires an unnegated equality class selector"
                )));
            }
            if group.depth_filters.len() > 1 {
                return Err(PostgresStorageError::bad_request(format!(
                    "Related filter group '{alias}' accepts at most one depth__lte filter"
                )));
            }
            let max_depth = match group.depth_filters.first() {
                Some(filter) => {
                    if filter.operator != (SearchOperator::Lte { is_negated: false }) {
                        return Err(PostgresStorageError::bad_request(format!(
                            "Related filter group '{alias}' only supports depth__lte"
                        )));
                    }
                    let depth = filter.value.parse::<u8>().map_err(|_| {
                        PostgresStorageError::bad_request(format!(
                            "Related filter depth must be an integer from 1 to {MAX_RELATED_FILTER_DEPTH}"
                        ))
                    })?;
                    if !(1..=MAX_RELATED_FILTER_DEPTH).contains(&depth) {
                        return Err(PostgresStorageError::bad_request(format!(
                            "Related filter depth must be an integer from 1 to {MAX_RELATED_FILTER_DEPTH}"
                        )));
                    }
                    i32::from(depth)
                }
                None => i32::from(DEFAULT_RELATED_FILTER_DEPTH),
            };
            Ok(RelatedFilterGroup {
                class_filter,
                object_filters: group.object_filters,
                max_depth,
            })
        })
        .collect()
}

fn build_related_object_filter_sql(
    groups: &[RelatedFilterGroup<'_>],
    graph_collection_ids: &[i32],
    class_collection_ids: &[i32],
    visibility: &StorageVisibility,
) -> Result<SqlComponent, PostgresStorageError> {
    let mut bind_variables = Vec::new();
    let graph_collections_sql = sql_integer_array(graph_collection_ids, &mut bind_variables);
    let class_collections_sql = sql_integer_array(class_collection_ids, &mut bind_variables);
    let valid_scope_objects_sql = if let Some(scope) = visibility.resources() {
        let collection_scope_sql = sql_integer_array(scope.collection_ids(), &mut bind_variables);
        let class_scope_sql = sql_integer_array(scope.class_ids(), &mut bind_variables);
        let object_scope_sql = sql_integer_array(scope.object_ids(), &mut bind_variables);
        format!(
            "SELECT id AS object_id FROM hubuumobject WHERE collection_id = ANY({collection_scope_sql}) OR hubuum_class_id = ANY({class_scope_sql}) OR id = ANY({object_scope_sql})"
        )
    } else {
        "SELECT id AS object_id FROM hubuumobject".to_string()
    };
    let valid_scope_classes_sql = if let Some(scope) = visibility.resources() {
        let collection_scope_sql = sql_integer_array(scope.collection_ids(), &mut bind_variables);
        let class_scope_sql = sql_integer_array(scope.class_ids(), &mut bind_variables);
        format!(
            "SELECT id AS class_id FROM hubuumclass WHERE collection_id = ANY({collection_scope_sql}) OR id = ANY({class_scope_sql})"
        )
    } else {
        "SELECT id AS class_id FROM hubuumclass".to_string()
    };

    let mut seed_queries = Vec::with_capacity(groups.len());
    for (group_id, group) in groups.iter().enumerate() {
        bind_variables.push(SqlValue::Integer(i32::try_from(group_id).map_err(
            |_| PostgresStorageError::internal("Related filter group index overflow"),
        )?));
        bind_variables.push(SqlValue::Integer(group.max_depth));

        let class_field = group
            .class_filter
            .field
            .related_query()
            .and_then(|field| match field.target() {
                RelatedFilterTarget::Class(class_field) => Some(class_field),
                _ => None,
            })
            .ok_or_else(|| {
                PostgresStorageError::internal("Related filter group lost its class selector")
            })?;
        let class_clause = match class_field {
            RelatedClassField::Id => {
                let values = parse_integer_values(group.class_filter)?;
                if values.len() != 1 {
                    return Err(PostgresStorageError::bad_request(
                        "related.<alias>.class.id requires exactly one integer",
                    ));
                }
                bind_variables.push(SqlValue::Integer(values[0]));
                "target_class.id = ?".to_string()
            }
            RelatedClassField::Name => {
                bind_variables.push(SqlValue::String(group.class_filter.value.clone()));
                "target_class.name = ?".to_string()
            }
        };

        let mut target_clauses = vec![class_clause];
        for (filter, field) in &group.object_filters {
            target_clauses.push(related_target_object_clause(
                filter,
                *field,
                &mut bind_variables,
            )?);
        }
        seed_queries.push(format!(
            r#"    SELECT ?::integer AS group_id,
           target_object.id AS seed_id,
           ?::integer AS max_depth
    FROM hubuumobject target_object
    JOIN hubuumclass target_class
      ON target_class.id = target_object.hubuum_class_id
    WHERE target_object.collection_id IN (SELECT collection_id FROM valid_graph_collections)
      AND target_class.collection_id IN (SELECT collection_id FROM valid_class_collections)
      AND target_object.id IN (SELECT object_id FROM valid_scope_objects)
      AND target_class.id IN (SELECT class_id FROM valid_scope_classes)
      AND {}"#,
            target_clauses.join("\n      AND ")
        ));
    }

    bind_variables.push(SqlValue::Integer(i32::try_from(groups.len()).map_err(
        |_| PostgresStorageError::internal("Related filter group count overflow"),
    )?));

    let sql = format!(
        r#"hubuumobject.id IN (
WITH RECURSIVE
valid_graph_collections AS (
    SELECT unnest({graph_collections_sql}) AS collection_id
),
valid_class_collections AS (
    SELECT unnest({class_collections_sql}) AS collection_id
),
valid_scope_objects AS (
    {valid_scope_objects_sql}
),
valid_scope_classes AS (
    {valid_scope_classes_sql}
),
target_seeds AS (
{}
),
object_edges AS (
    SELECT relation.from_hubuum_object_id AS source_object_id,
           relation.to_hubuum_object_id AS target_object_id
    FROM hubuumobject_relation relation
    JOIN hubuumobject source_object
      ON source_object.id = relation.from_hubuum_object_id
    JOIN hubuumobject target_object
      ON target_object.id = relation.to_hubuum_object_id
    WHERE source_object.collection_id IN (SELECT collection_id FROM valid_graph_collections)
      AND target_object.collection_id IN (SELECT collection_id FROM valid_graph_collections)
      AND source_object.id IN (SELECT object_id FROM valid_scope_objects)
      AND target_object.id IN (SELECT object_id FROM valid_scope_objects)

    UNION ALL

    SELECT relation.to_hubuum_object_id AS source_object_id,
           relation.from_hubuum_object_id AS target_object_id
    FROM hubuumobject_relation relation
    JOIN hubuumobject source_object
      ON source_object.id = relation.to_hubuum_object_id
    JOIN hubuumobject target_object
      ON target_object.id = relation.from_hubuum_object_id
    WHERE source_object.collection_id IN (SELECT collection_id FROM valid_graph_collections)
      AND target_object.collection_id IN (SELECT collection_id FROM valid_graph_collections)
      AND source_object.id IN (SELECT object_id FROM valid_scope_objects)
      AND target_object.id IN (SELECT object_id FROM valid_scope_objects)
),
reachable AS (
    SELECT target_seeds.group_id,
           target_seeds.seed_id,
           object_edges.target_object_id AS object_id,
           1 AS depth,
           target_seeds.max_depth
    FROM target_seeds
    JOIN object_edges
      ON object_edges.source_object_id = target_seeds.seed_id
    WHERE target_seeds.max_depth >= 1

    UNION

    SELECT reachable.group_id,
           reachable.seed_id,
           object_edges.target_object_id AS object_id,
           reachable.depth + 1,
           reachable.max_depth
    FROM reachable
    JOIN object_edges
      ON object_edges.source_object_id = reachable.object_id
    WHERE reachable.depth < reachable.max_depth
)
SELECT reachable.object_id
FROM reachable
WHERE reachable.object_id <> reachable.seed_id
GROUP BY reachable.object_id
HAVING COUNT(DISTINCT reachable.group_id) = ?
)"#,
        seed_queries.join("\n\n    UNION ALL\n\n")
    );

    Ok(SqlComponent {
        sql,
        bind_variables,
    })
}

fn related_target_object_clause(
    parameter: &ParsedQueryParam,
    field: RelatedObjectField,
    bind_variables: &mut Vec<SqlValue>,
) -> Result<String, PostgresStorageError> {
    let column = match field {
        RelatedObjectField::Id => "target_object.id",
        RelatedObjectField::Name => "target_object.name",
        RelatedObjectField::Description => "target_object.description",
        RelatedObjectField::CollectionId => "target_object.collection_id",
        RelatedObjectField::CreatedAt => "target_object.created_at",
        RelatedObjectField::UpdatedAt => "target_object.updated_at",
        RelatedObjectField::Revision => "target_object.revision",
        RelatedObjectField::JsonData => {
            let mut json_parameter = parameter.clone();
            json_parameter.field = FilterField::JsonData;
            let predicate = json_filter_sql(&json_parameter, "target_object.data")?;
            bind_variables.extend(predicate.bind_variables);
            return Ok(format!("({})", predicate.sql));
        }
    };

    let (operator, negated) = parameter.operator.op_and_neg();
    if operator == Operator::IsNull {
        let should_be_null = hubuum_query::parse_boolean_value(&parameter.value)
            .map_err(|error| PostgresStorageError::bad_request(error.to_string()))?
            != negated;
        return Ok(format!(
            "{column} IS {}NULL",
            if should_be_null { "" } else { "NOT " }
        ));
    }

    match field {
        RelatedObjectField::Id | RelatedObjectField::CollectionId => {
            related_integer_clause(parameter, column, bind_variables)
        }
        RelatedObjectField::Revision => related_revision_clause(parameter, column, bind_variables),
        RelatedObjectField::CreatedAt | RelatedObjectField::UpdatedAt => {
            related_date_clause(parameter, column, bind_variables)
        }
        RelatedObjectField::Name | RelatedObjectField::Description => {
            related_string_clause(parameter, column, bind_variables)
        }
        RelatedObjectField::JsonData => unreachable!("JSON returned above"),
    }
}

fn related_integer_clause(
    parameter: &ParsedQueryParam,
    column: &str,
    bind_variables: &mut Vec<SqlValue>,
) -> Result<String, PostgresStorageError> {
    let values = parse_integer_values(parameter)?;
    let minimum = *values.iter().min().ok_or_else(|| {
        PostgresStorageError::bad_request(format!(
            "Searching on field '{}' requires a value",
            parameter.field
        ))
    })?;
    let maximum = *values.iter().max().unwrap_or(&minimum);
    let (operator, negated) = parameter.operator.op_and_neg();
    let sql = match operator {
        Operator::Equals | Operator::In => {
            if values.len() > 50 {
                return Err(PostgresStorageError::bad_request(format!(
                    "Operator '{operator}' is limited to 50 values, got {}",
                    values.len()
                )));
            }
            let array = sql_integer_array(&values, bind_variables);
            format!("{column} = ANY({array})")
        }
        Operator::Gt => bound_comparison(column, ">", SqlValue::Integer(maximum), bind_variables),
        Operator::Gte => bound_comparison(column, ">=", SqlValue::Integer(maximum), bind_variables),
        Operator::Lt => bound_comparison(column, "<", SqlValue::Integer(minimum), bind_variables),
        Operator::Lte => bound_comparison(column, "<=", SqlValue::Integer(minimum), bind_variables),
        Operator::Between if values.len() == 2 => {
            bind_variables.push(SqlValue::Integer(values[0]));
            bind_variables.push(SqlValue::Integer(values[1]));
            format!("{column} BETWEEN ? AND ?")
        }
        Operator::Between => {
            return Err(PostgresStorageError::bad_request(format!(
                "Operator 'between' requires 2 values for field '{}'",
                parameter.field
            )));
        }
        _ => return Err(unsupported_related_operator(parameter, "numeric")),
    };
    Ok(wrap_negated(sql, negated))
}

fn related_revision_clause(
    parameter: &ParsedQueryParam,
    column: &str,
    bind_variables: &mut Vec<SqlValue>,
) -> Result<String, PostgresStorageError> {
    let values = hubuum_query::parse_positive_bigint_list_with_limit(
        &parameter.value,
        MAX_INTEGER_FILTER_VALUES,
    )
    .map_err(|error| PostgresStorageError::bad_request(error.to_string()))?;
    let (operator, negated) = parameter.operator.op_and_neg();
    let sql = match operator {
        Operator::Equals if values.len() == 1 => {
            bound_comparison(column, "=", SqlValue::BigInteger(values[0]), bind_variables)
        }
        Operator::Equals => {
            return Err(PostgresStorageError::bad_request(format!(
                "Operator '{operator}' requires exactly 1 value for field '{}'",
                parameter.field
            )));
        }
        Operator::In => {
            let array = sql_bigint_array(&values, bind_variables);
            format!("{column} = ANY({array})")
        }
        Operator::Gt | Operator::Gte | Operator::Lt | Operator::Lte if values.len() == 1 => {
            let sql_operator = match operator {
                Operator::Gt => ">",
                Operator::Gte => ">=",
                Operator::Lt => "<",
                Operator::Lte => "<=",
                _ => unreachable!(),
            };
            bound_comparison(
                column,
                sql_operator,
                SqlValue::BigInteger(values[0]),
                bind_variables,
            )
        }
        Operator::Between if values.len() == 2 => {
            bind_variables.push(SqlValue::BigInteger(values[0]));
            bind_variables.push(SqlValue::BigInteger(values[1]));
            format!("{column} BETWEEN ? AND ?")
        }
        Operator::Gt | Operator::Gte | Operator::Lt | Operator::Lte | Operator::Between => {
            return Err(PostgresStorageError::bad_request(format!(
                "Operator '{operator}' has the wrong number of values for field '{}'",
                parameter.field
            )));
        }
        _ => return Err(unsupported_related_operator(parameter, "revision")),
    };
    Ok(wrap_negated(sql, negated))
}

fn related_date_clause(
    parameter: &ParsedQueryParam,
    column: &str,
    bind_variables: &mut Vec<SqlValue>,
) -> Result<String, PostgresStorageError> {
    let values = hubuum_query::parse_datetime_list(&parameter.value)
        .map_err(|error| PostgresStorageError::bad_request(error.to_string()))?;
    let minimum = *values.iter().min().ok_or_else(|| {
        PostgresStorageError::bad_request(format!(
            "Searching on field '{}' requires a value",
            parameter.field
        ))
    })?;
    let maximum = *values.iter().max().unwrap_or(&minimum);
    let (operator, negated) = parameter.operator.op_and_neg();
    let sql = match operator {
        Operator::Equals | Operator::In => {
            let array = sql_datetime_array(&values, bind_variables);
            format!("{column} = ANY({array})")
        }
        Operator::Gt => bound_comparison(column, ">", SqlValue::DateTime(maximum), bind_variables),
        Operator::Gte => {
            bound_comparison(column, ">=", SqlValue::DateTime(maximum), bind_variables)
        }
        Operator::Lt => bound_comparison(column, "<", SqlValue::DateTime(minimum), bind_variables),
        Operator::Lte => {
            bound_comparison(column, "<=", SqlValue::DateTime(minimum), bind_variables)
        }
        Operator::Between if values.len() == 2 => {
            bind_variables.push(SqlValue::DateTime(values[0]));
            bind_variables.push(SqlValue::DateTime(values[1]));
            format!("{column} BETWEEN ? AND ?")
        }
        Operator::Between => {
            return Err(PostgresStorageError::bad_request(format!(
                "Operator 'between' requires 2 values for field '{}'",
                parameter.field
            )));
        }
        _ => return Err(unsupported_related_operator(parameter, "date")),
    };
    Ok(wrap_negated(sql, negated))
}

fn related_string_clause(
    parameter: &ParsedQueryParam,
    column: &str,
    bind_variables: &mut Vec<SqlValue>,
) -> Result<String, PostgresStorageError> {
    let (operator, negated) = parameter.operator.op_and_neg();
    let sql = match operator {
        Operator::In => {
            let values = parameter
                .value
                .split(',')
                .map(str::to_string)
                .collect::<Vec<_>>();
            let array = sql_text_array(&values, bind_variables);
            format!("{column} = ANY({array})")
        }
        Operator::Equals
        | Operator::IEquals
        | Operator::Contains
        | Operator::IContains
        | Operator::StartsWith
        | Operator::IStartsWith
        | Operator::EndsWith
        | Operator::IEndsWith
        | Operator::Like
        | Operator::Regex => {
            let (sql_operator, value) = match operator {
                Operator::Equals => ("=", parameter.value.clone()),
                Operator::IEquals => ("ILIKE", parameter.value.clone()),
                Operator::Contains => ("LIKE", format!("%{}%", parameter.value)),
                Operator::IContains => ("ILIKE", format!("%{}%", parameter.value)),
                Operator::StartsWith => ("LIKE", format!("{}%", parameter.value)),
                Operator::IStartsWith => ("ILIKE", format!("{}%", parameter.value)),
                Operator::EndsWith => ("LIKE", format!("%{}", parameter.value)),
                Operator::IEndsWith => ("ILIKE", format!("%{}", parameter.value)),
                Operator::Like => ("LIKE", parameter.value.clone()),
                Operator::Regex => ("~", parameter.value.clone()),
                _ => unreachable!(),
            };
            bound_comparison(
                column,
                sql_operator,
                SqlValue::String(value),
                bind_variables,
            )
        }
        _ => return Err(unsupported_related_operator(parameter, "string")),
    };
    Ok(wrap_negated(sql, negated))
}

fn parse_integer_values(parameter: &ParsedQueryParam) -> Result<Vec<i32>, PostgresStorageError> {
    hubuum_query::parse_integer_list(&parameter.value)
        .map_err(|error| PostgresStorageError::bad_request(error.to_string()))
}

fn bound_comparison(
    column: &str,
    operator: &str,
    value: SqlValue,
    bind_variables: &mut Vec<SqlValue>,
) -> String {
    bind_variables.push(value);
    format!("{column} {operator} ?")
}

fn wrap_negated(sql: String, negated: bool) -> String {
    if negated { format!("NOT ({sql})") } else { sql }
}

fn unsupported_related_operator(
    parameter: &ParsedQueryParam,
    data_type: &str,
) -> PostgresStorageError {
    PostgresStorageError::bad_request(format!(
        "Operator '{}' is not implemented for related {data_type} field '{}'",
        parameter.operator, parameter.field
    ))
}

fn sql_integer_array(values: &[i32], bind_variables: &mut Vec<SqlValue>) -> String {
    let placeholders = values
        .iter()
        .map(|value| {
            bind_variables.push(SqlValue::Integer(*value));
            "?"
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("ARRAY[{placeholders}]::integer[]")
}

fn sql_bigint_array(values: &[i64], bind_variables: &mut Vec<SqlValue>) -> String {
    let placeholders = values
        .iter()
        .map(|value| {
            bind_variables.push(SqlValue::BigInteger(*value));
            "?"
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("ARRAY[{placeholders}]::bigint[]")
}

fn sql_text_array(values: &[String], bind_variables: &mut Vec<SqlValue>) -> String {
    let placeholders = values
        .iter()
        .map(|value| {
            bind_variables.push(SqlValue::String(value.clone()));
            "?"
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("ARRAY[{placeholders}]::text[]")
}

fn sql_datetime_array(
    values: &[chrono::NaiveDateTime],
    bind_variables: &mut Vec<SqlValue>,
) -> String {
    let placeholders = values
        .iter()
        .map(|value| {
            bind_variables.push(SqlValue::DateTime(*value));
            "?"
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("ARRAY[{placeholders}]::timestamp[]")
}

#[cfg(test)]
mod tests {
    use hubuum_query::parse_query_parameter_with_computed_and_related_filters_and_passthrough;
    use hubuum_storage_core::UnifiedSearchResourceScope;

    use super::*;

    fn query_options(query: &str) -> hubuum_query::QueryOptions {
        parse_query_parameter_with_computed_and_related_filters_and_passthrough(query, &[])
            .unwrap()
            .0
    }

    #[test]
    fn target_seed_applies_class_resource_scope() {
        let options = query_options("related.room.class.id=7&related.room.object.name=foo");
        let groups = related_filter_groups(options.filters()).unwrap();
        let visibility = StorageVisibility::new(
            11,
            false,
            None::<Vec<AuthorizationPermission>>,
            Some(UnifiedSearchResourceScope::new([], [], [11])),
        );

        let component = build_related_object_filter_sql(&groups, &[2], &[2], &visibility).unwrap();

        assert!(
            component
                .sql
                .contains("target_class.id IN (SELECT class_id FROM valid_scope_classes)")
        );
        assert!(
            component
                .sql
                .contains("target_object.id IN (SELECT object_id FROM valid_scope_objects)")
        );
    }

    #[test]
    fn multiple_groups_require_all_aliases() {
        let options = query_options(
            "related.room.class.id=7&related.room.object.name=foo&related.site.class.name=site",
        );
        let groups = related_filter_groups(options.filters()).unwrap();
        let visibility =
            StorageVisibility::new(11, true, None::<Vec<AuthorizationPermission>>, None);

        let component = build_related_object_filter_sql(&groups, &[2], &[2], &visibility).unwrap();

        assert!(
            component
                .sql
                .contains("HAVING COUNT(DISTINCT reachable.group_id) = ?")
        );
        assert!(matches!(
            component.bind_variables.last(),
            Some(SqlValue::Integer(2))
        ));
    }
}
