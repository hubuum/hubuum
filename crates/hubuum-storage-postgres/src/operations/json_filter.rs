//! PostgreSQL JSON filter compilation.

use std::net::IpAddr;
use std::str::FromStr;

use hubuum_query::{
    JsonFieldPathRef, Operator, ParsedQueryParam, QueryScalarType, infer_query_scalar_type,
};
use ipnet::{IpNet, Ipv4Net, Ipv6Net};

use crate::PostgresStorageError;
use crate::operations::dynamic_sql::{
    BoundSqlPredicate, SqlComponent, SqlValue, bound_sql_predicate,
};

pub(crate) fn json_predicate(
    parameter: &ParsedQueryParam,
    json_expression: &str,
) -> Result<BoundSqlPredicate, PostgresStorageError> {
    bound_sql_predicate(json_filter_sql(parameter, json_expression)?)
}

pub(crate) fn json_filter_sql(
    parameter: &ParsedQueryParam,
    json_expression: &str,
) -> Result<SqlComponent, PostgresStorageError> {
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
        return Ok(SqlComponent {
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
            return Ok(SqlComponent {
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

    let mapped_type = infer_query_scalar_type(value, operator.clone());
    match mapped_type {
        Some(QueryScalarType::Numeric) => {
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
        Some(QueryScalarType::Date) => {
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
        Some(QueryScalarType::Boolean) => {
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
        Some(QueryScalarType::String | QueryScalarType::None) => {
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

/// Compile one JSON predicate without exposing adapter-private SQL value types.
///
/// This is an internal benchmark seam, not a storage-contract operation.
#[doc(hidden)]
pub fn compile_json_filter_for_benchmark(
    parameter: &ParsedQueryParam,
) -> Result<(String, usize), PostgresStorageError> {
    let component = json_filter_sql(parameter, "json_data")?;
    Ok((component.sql, component.bind_variables.len()))
}

fn typed_json_filter(
    expression: &str,
    bind_variables: Vec<SqlValue>,
    operator: Operator,
    negated: bool,
    parameter: &ParsedQueryParam,
) -> Result<SqlComponent, PostgresStorageError> {
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
    Ok(SqlComponent {
        sql: format!("{expression} IS NOT NULL AND {predicate}"),
        bind_variables,
    })
}

fn json_ip_filter(
    expression: &str,
    value: &str,
    operator: Operator,
    negated: bool,
) -> Result<SqlComponent, PostgresStorageError> {
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
    Ok(SqlComponent {
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
) -> SqlComponent {
    SqlComponent {
        sql: if negated {
            format!("NOT ({predicate})")
        } else {
            predicate
        },
        bind_variables,
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use hubuum_query::{ParsedQueryParam, SearchOperator};

    use super::*;

    fn parameter(operator: SearchOperator, value: &str) -> ParsedQueryParam {
        ParsedQueryParam::new("json_data", Some(operator), value)
            .expect("test JSON filter must parse")
    }

    #[test]
    fn malformed_json_paths_are_rejected_before_sql_construction() {
        let error = json_filter_sql(
            &parameter(
                SearchOperator::Equals { is_negated: false },
                "profile,bad}=value",
            ),
            "json_data",
        )
        .expect_err("an unsafe JSON path must be rejected");

        assert_eq!(
            error.kind(),
            hubuum_storage_core::StorageErrorKind::InvalidInput
        );
    }

    #[test]
    fn string_numeric_date_and_boolean_filters_use_typed_bind_values() {
        let cases = [
            (
                parameter(
                    SearchOperator::IContains { is_negated: false },
                    "profile,name=Alice",
                ),
                "json_data #>> '{profile,name}' ILIKE ?",
                "string",
            ),
            (
                parameter(
                    SearchOperator::Gte { is_negated: false },
                    "metrics,count=42",
                ),
                "try_numeric(json_data #>> '{metrics,count}') IS NOT NULL AND try_numeric(json_data #>> '{metrics,count}') >= ?",
                "numeric",
            ),
            (
                parameter(
                    SearchOperator::Gte { is_negated: false },
                    "metadata,created=2021-01-01",
                ),
                "try_timestamp(json_data #>> '{metadata,created}') IS NOT NULL AND try_timestamp(json_data #>> '{metadata,created}') >= ?",
                "date",
            ),
            (
                parameter(
                    SearchOperator::Equals { is_negated: false },
                    "flags,enabled=true",
                ),
                "try_boolean(json_data #>> '{flags,enabled}') IS NOT NULL AND try_boolean(json_data #>> '{flags,enabled}') = ?",
                "boolean",
            ),
        ];

        for (parameter, expected_sql, value_type) in cases {
            let component = json_filter_sql(&parameter, "json_data").expect("filter must compile");
            assert_eq!(component.sql, expected_sql);
            assert_eq!(component.bind_variables.len(), 1);
            match (value_type, &component.bind_variables[0]) {
                ("string", SqlValue::String(value)) => assert_eq!(value, "%Alice%"),
                ("numeric", SqlValue::Integer(value)) => assert_eq!(*value, 42),
                ("date", SqlValue::DateTime(value)) => assert_eq!(
                    *value,
                    NaiveDate::from_ymd_opt(2021, 1, 1)
                        .expect("valid date")
                        .and_hms_opt(0, 0, 0)
                        .expect("valid timestamp")
                ),
                ("boolean", SqlValue::Boolean(value)) => assert!(*value),
                _ => panic!("unexpected bind type for {value_type}"),
            }
        }
    }

    #[test]
    fn host_addresses_are_normalized_and_invalid_networks_are_rejected() {
        let component = json_filter_sql(
            &parameter(
                SearchOperator::InetEquals { is_negated: false },
                "network,address=10.0.0.1",
            ),
            "json_data",
        )
        .expect("host address must compile");
        assert!(component.sql.contains("try_inet"));
        assert!(matches!(
            component.bind_variables.as_slice(),
            [SqlValue::String(value)] if value == "10.0.0.1/32"
        ));

        let error = json_filter_sql(
            &parameter(
                SearchOperator::WithinNetwork { is_negated: false },
                "network,address=not-a-network",
            ),
            "json_data",
        )
        .expect_err("invalid CIDR must be rejected");
        assert_eq!(
            error.kind(),
            hubuum_storage_core::StorageErrorKind::InvalidInput
        );
    }

    #[test]
    fn negation_is_explicit_and_non_json_fields_are_rejected() {
        let component = json_filter_sql(
            &parameter(
                SearchOperator::Equals { is_negated: true },
                "profile,name=Alice",
            ),
            "json_data",
        )
        .expect("negated filter must compile");
        assert_eq!(component.sql, "NOT (json_data #>> '{profile,name}' = ?)");

        let non_json =
            ParsedQueryParam::new("name", None, "Alice").expect("ordinary test field must parse");
        let error = json_filter_sql(&non_json, "json_data")
            .expect_err("non-JSON filter must not enter JSON compilation");
        assert_eq!(
            error.kind(),
            hubuum_storage_core::StorageErrorKind::Internal
        );
    }
}
