//! Typed Diesel predicate for PostgreSQL JSON filters.

use std::iter::from_fn;
use std::net::IpAddr;
use std::str::FromStr;

use diesel::expression::{AppearsOnTable, Expression, SelectableExpression, ValidGrouping};
use diesel::pg::Pg;
use diesel::query_builder::{AstPass, QueryFragment, QueryId};
use diesel::result::QueryResult;
use diesel::sql_types::{Bool, Integer, Text, Timestamp};
use hubuum_query::{
    JsonFieldPathRef, Operator, ParsedQueryParam, SQLMappedType,
    get_jsonb_field_type_from_value_and_operator,
};
use ipnet::{IpNet, Ipv4Net, Ipv6Net};

use crate::PostgresStorageError;

#[derive(Clone, Debug)]
pub(crate) enum SqlValue {
    Integer(i32),
    String(String),
    Boolean(bool),
    DateTime(chrono::NaiveDateTime),
}

pub(crate) struct JsonSqlComponent {
    pub(crate) sql: String,
    pub(crate) bind_variables: Vec<SqlValue>,
}

#[derive(Debug, Clone)]
pub(crate) struct JsonSqlPredicate {
    sql: String,
    bind_variables: Vec<SqlValue>,
}

impl Expression for JsonSqlPredicate {
    type SqlType = Bool;
}

impl QueryId for JsonSqlPredicate {
    type QueryId = ();

    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for JsonSqlPredicate {
    fn walk_ast<'bind>(&'bind self, mut output: AstPass<'_, 'bind, Pg>) -> QueryResult<()> {
        output.unsafe_to_cache_prepared();
        let mut start = 0;
        for (bind_variable, offset) in self
            .bind_variables
            .iter()
            .zip(bind_placeholder_offsets(&self.sql))
        {
            output.push_sql(&self.sql[start..offset]);
            match bind_variable {
                SqlValue::Integer(value) => output.push_bind_param::<Integer, _>(value)?,
                SqlValue::String(value) => output.push_bind_param::<Text, _>(value)?,
                SqlValue::Boolean(value) => output.push_bind_param::<Bool, _>(value)?,
                SqlValue::DateTime(value) => output.push_bind_param::<Timestamp, _>(value)?,
            }
            start = offset + 1;
        }
        output.push_sql(&self.sql[start..]);
        Ok(())
    }
}

impl<QuerySource> SelectableExpression<QuerySource> for JsonSqlPredicate {}
impl<QuerySource> AppearsOnTable<QuerySource> for JsonSqlPredicate {}

impl<GroupBy> ValidGrouping<GroupBy> for JsonSqlPredicate {
    type IsAggregate = diesel::expression::is_aggregate::Never;
}

pub(crate) fn json_predicate(
    parameter: &ParsedQueryParam,
    json_expression: &str,
) -> Result<JsonSqlPredicate, PostgresStorageError> {
    let JsonSqlComponent {
        sql,
        bind_variables,
    } = json_filter_sql(parameter, json_expression)?;
    let placeholder_count = bind_placeholder_offsets(&sql).count();
    if placeholder_count != bind_variables.len() {
        return Err(PostgresStorageError::internal(format!(
            "Dynamic SQL predicate has {placeholder_count} placeholders but {} bind values",
            bind_variables.len()
        )));
    }
    Ok(JsonSqlPredicate {
        sql,
        bind_variables,
    })
}

pub(crate) fn json_filter_sql(
    parameter: &ParsedQueryParam,
    json_expression: &str,
) -> Result<JsonSqlComponent, PostgresStorageError> {
    if !parameter.is_json() {
        return Err(PostgresStorageError::internal(format!(
            "Attempt to filter '{}' as JSON",
            parameter.field
        )));
    }
    let (operator, negated) = parameter.operator.op_and_neg();
    if operator == Operator::IsNull {
        let path = json_path(&parameter.value)?;
        let expression = json_text_path_expression(json_expression, path);
        return Ok(JsonSqlComponent {
            sql: format!("{expression} IS {}NULL", if negated { "NOT " } else { "" }),
            bind_variables: Vec::new(),
        });
    }
    let (raw_path, value) = parameter.value.split_once('=').ok_or_else(|| {
        PostgresStorageError::bad_request("Expected exactly two parts of key=value")
    })?;
    let path = json_path(raw_path)?;
    let text_expression = json_text_path_expression(json_expression, path);

    if operator.is_ip_operator() {
        return json_ip_filter(&text_expression, value, operator, negated);
    }
    match operator {
        Operator::HasKey => {
            let expression = json_value_path_expression(json_expression, path);
            return Ok(negated_json_component(
                format!("jsonb_has_key({expression}, ?)"),
                vec![SqlValue::String(value.to_string())],
                negated,
            ));
        }
        Operator::All => {
            let values = comma_values(value, "all")?;
            let placeholders = placeholders(values.len());
            let expression = json_value_path_expression(json_expression, path);
            return Ok(negated_json_component(
                format!("jsonb_contains_all({expression}, ARRAY[{placeholders}])"),
                values.into_iter().map(SqlValue::String).collect(),
                negated,
            ));
        }
        Operator::ArrayLength => {
            let length = value.parse::<i32>().map_err(|_| {
                PostgresStorageError::bad_request(format!(
                    "array_length requires an integer, got '{value}'"
                ))
            })?;
            let expression = json_value_path_expression(json_expression, path);
            let comparison = if negated { "!=" } else { "=" };
            return Ok(JsonSqlComponent {
                sql: format!(
                    "jsonb_typeof({expression}) = 'array' AND jsonb_array_length({expression}) {comparison} ?"
                ),
                bind_variables: vec![SqlValue::Integer(length)],
            });
        }
        Operator::In => {
            let values = comma_values(value, "in")?;
            let scalar_placeholders = placeholders(values.len());
            let array_placeholders = placeholders(values.len());
            let json_value_expression = json_value_path_expression(json_expression, path);
            let sql = format!(
                "({text_expression} IN ({scalar_placeholders}) OR jsonb_contains_any({json_value_expression}, ARRAY[{array_placeholders}]))"
            );
            let mut bind_variables = values
                .iter()
                .cloned()
                .map(SqlValue::String)
                .collect::<Vec<_>>();
            bind_variables.extend(values.into_iter().map(SqlValue::String));
            return Ok(negated_json_component(sql, bind_variables, negated));
        }
        _ => {}
    }

    let mapped_type = get_jsonb_field_type_from_value_and_operator(value, operator.clone());
    match mapped_type {
        Some(SQLMappedType::Numeric) => {
            let values = hubuum_query::parse_integer_list(value)
                .map_err(|error| PostgresStorageError::bad_request(error.to_string()))?
                .into_iter()
                .map(SqlValue::Integer)
                .collect();
            typed_json_filter(
                &format!("try_numeric({text_expression})"),
                values,
                operator,
                negated,
                parameter,
            )
        }
        Some(SQLMappedType::Date) => {
            let values = hubuum_query::parse_datetime_list(value)
                .map_err(|error| PostgresStorageError::bad_request(error.to_string()))?
                .into_iter()
                .map(SqlValue::DateTime)
                .collect();
            typed_json_filter(
                &format!("try_timestamp({text_expression})"),
                values,
                operator,
                negated,
                parameter,
            )
        }
        Some(SQLMappedType::Boolean) => {
            let value = hubuum_query::parse_boolean_value(value)
                .map_err(|error| PostgresStorageError::bad_request(error.to_string()))?;
            typed_json_filter(
                &format!("try_boolean({text_expression})"),
                vec![SqlValue::Boolean(value)],
                operator,
                negated,
                parameter,
            )
        }
        Some(SQLMappedType::String | SQLMappedType::None) => {
            let (sql_operator, value) = match operator {
                Operator::Equals => ("=", value.to_string()),
                Operator::IEquals => ("ILIKE", value.to_string()),
                Operator::Contains | Operator::Like => ("LIKE", format!("%{value}%")),
                Operator::IContains => ("ILIKE", format!("%{value}%")),
                Operator::StartsWith => ("LIKE", format!("{value}%")),
                Operator::IStartsWith => ("ILIKE", format!("{value}%")),
                Operator::EndsWith => ("LIKE", format!("%{value}")),
                Operator::IEndsWith => ("ILIKE", format!("%{value}")),
                Operator::Regex => ("~", value.to_string()),
                Operator::Gt => (">", value.to_string()),
                Operator::Gte => (">=", value.to_string()),
                Operator::Lt => ("<", value.to_string()),
                Operator::Lte => ("<=", value.to_string()),
                _ => {
                    return Err(PostgresStorageError::bad_request(format!(
                        "Invalid operator for JSON: '{operator:?}'"
                    )));
                }
            };
            Ok(negated_json_component(
                format!("{text_expression} {sql_operator} ?"),
                vec![SqlValue::String(value)],
                negated,
            ))
        }
        None => Err(PostgresStorageError::bad_request(format!(
            "Invalid JSON type mapping between key '{path}' and operator '{}'",
            parameter.operator
        ))),
    }
}

fn typed_json_filter(
    expression: &str,
    bind_variables: Vec<SqlValue>,
    operator: Operator,
    negated: bool,
    parameter: &ParsedQueryParam,
) -> Result<JsonSqlComponent, PostgresStorageError> {
    let required_values = if operator == Operator::Between { 2 } else { 1 };
    if bind_variables.len() != required_values {
        return Err(PostgresStorageError::bad_request(format!(
            "Operator '{operator}' requires exactly {required_values} value(s) for JSON field '{}'",
            parameter.field
        )));
    }
    let predicate = match operator {
        Operator::Equals => format!("{expression} = ?"),
        Operator::Gt => format!("{expression} > ?"),
        Operator::Gte => format!("{expression} >= ?"),
        Operator::Lt => format!("{expression} < ?"),
        Operator::Lte => format!("{expression} <= ?"),
        Operator::Between => format!("{expression} BETWEEN ? AND ?"),
        _ => {
            return Err(PostgresStorageError::bad_request(format!(
                "Invalid operator for typed JSON search: '{operator:?}'"
            )));
        }
    };
    let predicate = if negated {
        format!("NOT ({predicate})")
    } else {
        predicate
    };
    Ok(JsonSqlComponent {
        sql: format!("{expression} IS NOT NULL AND {predicate}"),
        bind_variables,
    })
}

fn json_ip_filter(
    expression: &str,
    value: &str,
    operator: Operator,
    negated: bool,
) -> Result<JsonSqlComponent, PostgresStorageError> {
    let value = match operator {
        Operator::ContainsIp => value
            .parse::<IpAddr>()
            .map(|address| address.to_string())
            .map_err(|_| {
                PostgresStorageError::bad_request(format!("Invalid IP address: '{value}'"))
            })?,
        Operator::WithinNetwork
        | Operator::ContainsNetwork
        | Operator::OverlapsNetwork
        | Operator::InetEquals => parse_ip_or_host_network(value)?,
        _ => {
            return Err(PostgresStorageError::internal(
                "non-IP operator reached the JSON IP filter",
            ));
        }
    };
    let sql_operator = match operator {
        Operator::InetEquals => "=",
        Operator::WithinNetwork => "<<=",
        Operator::ContainsNetwork => ">>=",
        Operator::ContainsIp => ">>",
        Operator::OverlapsNetwork => "&&",
        _ => unreachable!("operator validated above"),
    };
    let inet_expression = format!("try_inet({expression})");
    let predicate = format!("{inet_expression} {sql_operator} ?::inet");
    let predicate = if negated {
        format!("NOT ({predicate})")
    } else {
        predicate
    };
    Ok(JsonSqlComponent {
        sql: format!("{inet_expression} IS NOT NULL AND {predicate}"),
        bind_variables: vec![SqlValue::String(value)],
    })
}

fn parse_ip_or_host_network(value: &str) -> Result<String, PostgresStorageError> {
    if let Ok(network) = IpNet::from_str(value) {
        return Ok(network.to_string());
    }
    let network = match value.parse::<IpAddr>() {
        Ok(IpAddr::V4(address)) => Ipv4Net::new(address, 32).map(IpNet::from),
        Ok(IpAddr::V6(address)) => Ipv6Net::new(address, 128).map(IpNet::from),
        Err(_) => {
            return Err(PostgresStorageError::bad_request(format!(
                "Invalid IP/CIDR: '{value}'"
            )));
        }
    };
    network
        .map(|network| network.to_string())
        .map_err(|_| PostgresStorageError::bad_request(format!("Invalid IP/CIDR: '{value}'")))
}

fn json_path(value: &str) -> Result<JsonFieldPathRef<'_>, PostgresStorageError> {
    JsonFieldPathRef::new(value)
        .map_err(|error| PostgresStorageError::bad_request(error.to_string()))
}

fn json_text_path_expression(expression: &str, path: JsonFieldPathRef<'_>) -> String {
    format!("{expression} #>> '{{{path}}}'")
}

fn json_value_path_expression(expression: &str, path: JsonFieldPathRef<'_>) -> String {
    format!("{expression} #> '{{{path}}}'")
}

fn comma_values(value: &str, operator: &str) -> Result<Vec<String>, PostgresStorageError> {
    let values = value.split(',').map(str::to_string).collect::<Vec<_>>();
    if values.is_empty() {
        Err(PostgresStorageError::bad_request(format!(
            "'{operator}' requires at least one value"
        )))
    } else {
        Ok(values)
    }
}

fn placeholders(count: usize) -> String {
    std::iter::repeat_n("?", count)
        .collect::<Vec<_>>()
        .join(", ")
}

fn negated_json_component(
    predicate: String,
    bind_variables: Vec<SqlValue>,
    negated: bool,
) -> JsonSqlComponent {
    JsonSqlComponent {
        sql: if negated {
            format!("NOT ({predicate})")
        } else {
            predicate
        },
        bind_variables,
    }
}

fn bind_placeholder_offsets(sql: &str) -> impl Iterator<Item = usize> + '_ {
    let mut characters = sql.char_indices().peekable();
    let mut in_single_quoted_string = false;
    from_fn(move || {
        while let Some((offset, character)) = characters.next() {
            if character == '\'' {
                if in_single_quoted_string
                    && characters
                        .peek()
                        .is_some_and(|(_, next_character)| *next_character == '\'')
                {
                    let _ = characters.next();
                } else {
                    in_single_quoted_string = !in_single_quoted_string;
                }
            } else if character == '?' && !in_single_quoted_string {
                return Some(offset);
            }
        }
        None
    })
}

#[cfg(test)]
mod tests {
    use super::bind_placeholder_offsets;

    #[test]
    fn placeholders_ignore_question_marks_in_sql_strings() {
        let sql = "scope = '[{\"path\":\"/answer?\"}]' AND escaped = 'it''s?' AND value = ?";

        assert_eq!(
            bind_placeholder_offsets(sql).collect::<Vec<_>>(),
            vec![sql.len() - 1]
        );
    }
}
