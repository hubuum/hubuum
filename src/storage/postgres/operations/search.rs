use chrono::NaiveDateTime;
use diesel::expression::{AppearsOnTable, Expression, SelectableExpression, ValidGrouping};
use diesel::pg::Pg;
use diesel::query_builder::{AstPass, QueryFragment, QueryId};
use diesel::result::QueryResult;
use diesel::sql_types::{BigInt, Bool, Integer, Text, Timestamp};
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use std::iter::from_fn;
use std::net::IpAddr;
use std::str::FromStr;
use tracing::debug;

use crate::errors::ApiError;
use crate::models::search::{Operator, ParsedQueryParam};
use crate::utilities::extensions::CustomStringExtensions;
use hubuum_query::{JsonFieldPathRef, SQLMappedType, get_jsonb_field_type_from_value_and_operator};

pub(crate) trait ParsedQueryParamSqlExt {
    fn as_json_sql(&self) -> Result<SQLComponent, ApiError>;

    fn as_json_sql_for_field_expr(&self, jsonb_field_expr: &str) -> Result<SQLComponent, ApiError>;

    fn as_json_ip_sql(
        &self,
        field_expr: &str,
        value: &str,
        op: Operator,
        negated: bool,
    ) -> Result<SQLComponent, ApiError>;

    fn as_json_is_null_sql(
        &self,
        jsonb_field_expr: &str,
        negated: bool,
    ) -> Result<SQLComponent, ApiError>;

    fn as_json_has_key_sql(
        &self,
        jsonb_field_expr: &str,
        path: JsonFieldPathRef<'_>,
        key_name: &str,
        negated: bool,
    ) -> Result<SQLComponent, ApiError>;

    fn as_json_in_sql(
        &self,
        jsonb_field_expr: &str,
        field_expr: &str,
        path: JsonFieldPathRef<'_>,
        value: &str,
        negated: bool,
    ) -> Result<SQLComponent, ApiError>;

    fn as_json_all_sql(
        &self,
        jsonb_field_expr: &str,
        path: JsonFieldPathRef<'_>,
        value: &str,
        negated: bool,
    ) -> Result<SQLComponent, ApiError>;

    fn as_json_array_length_sql(
        &self,
        jsonb_field_expr: &str,
        path: JsonFieldPathRef<'_>,
        value: &str,
        negated: bool,
    ) -> Result<SQLComponent, ApiError>;

    fn as_json_numeric_sql(
        &self,
        field_expr: &str,
        value: &str,
        op: Operator,
        negated: bool,
    ) -> Result<SQLComponent, ApiError>;

    fn as_json_date_sql(
        &self,
        field_expr: &str,
        value: &str,
        op: Operator,
        negated: bool,
    ) -> Result<SQLComponent, ApiError>;

    fn as_json_boolean_sql(
        &self,
        field_expr: &str,
        value: &str,
        op: Operator,
        negated: bool,
    ) -> Result<SQLComponent, ApiError>;

    fn as_json_cast_sql(
        &self,
        lhs_expr: &str,
        bind_variables: Vec<SQLValue>,
        op: Operator,
        negated: bool,
    ) -> Result<SQLComponent, ApiError>;
}

/// An internal SQL fragment paired with its typed bind values.
///
/// This struct holds a SQL query and a list of bind variables. The SQL query is a string that
/// represents a part of a SQL query, and the bind variables are the values that should be bound to
/// the query when it is executed. Note that the place holders used are ?, which is not what you want
/// for sql_query in diesel (you need $1, $2, etc.). But, as we don't know what part we are in the final
/// query, we don't know our indexes, so this needs replacing later.
///
/// replace_question_mark_with_indexed_n does this on &str and string via
/// crate::utilities::extensions::CustomStringExtensions.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SQLComponent {
    pub(crate) sql: String,
    pub(crate) bind_variables: Vec<SQLValue>,
}

/// An internal typed value for a dynamic SQL bind parameter.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SQLValue {
    String(String),
    Integer(i32),
    BigInteger(i64),
    Date(NaiveDateTime),
    Boolean(bool),
}

impl ParsedQueryParamSqlExt for ParsedQueryParam {
    fn as_json_sql(&self) -> Result<SQLComponent, ApiError> {
        let json_column = self.field.json_column().ok_or_else(|| {
            ApiError::InternalServerError(format!("Attempt to filter '{}' as JSON!", self.field))
        })?;
        self.as_json_sql_for_field_expr(json_column)
    }

    fn as_json_sql_for_field_expr(&self, jsonb_field_expr: &str) -> Result<SQLComponent, ApiError> {
        if !self.is_json() {
            return Err(ApiError::InternalServerError(format!(
                "Attempt to filter '{}' as JSON!",
                self.field
            )));
        }

        // TODO: Since we may have a schema, we may have typing info, so we can also
        // validatethe value and the operator against the defined type in the schema.

        let (op, neg) = self.operator.op_and_neg();

        // is_null has no value part — the entire RHS is the JSON path
        if op == Operator::IsNull {
            return self.as_json_is_null_sql(jsonb_field_expr, neg);
        }

        let (key, value) = self.value.split_once('=').ok_or_else(|| {
            ApiError::BadRequest("Expected exactly two parts of key=value".to_string())
        })?;
        let path = JsonFieldPathRef::new(key)?;

        // Validate the value, no longer needed as we're using bind variables
        /*
        if !value.is_valid_jsonb_search_value() {
            return Err(ApiError::BadRequest(format!(
                "Invalid JSON search value: '{}'",
                value
            )));
        }
        */

        let field_expr = json_text_path_expression(jsonb_field_expr, path);

        // The bind variables for the SQL query. We can't bind the key as using
        // bind variables for the key itself is not supported in Postgres.
        let mut bind_variables = vec![];

        // TODO: Optionally validate that the keys exist:
        // https://github.com/terjekv/hubuum_rust/issues/4

        let neg_str = if neg { "NOT " } else { "" };

        if op.is_ip_operator() {
            return self.as_json_ip_sql(&field_expr, value, op, neg);
        }

        if op == Operator::HasKey {
            return self.as_json_has_key_sql(jsonb_field_expr, path, value, neg);
        }

        if op == Operator::All {
            return self.as_json_all_sql(jsonb_field_expr, path, value, neg);
        }

        if op == Operator::ArrayLength {
            return self.as_json_array_length_sql(jsonb_field_expr, path, value, neg);
        }

        if op == Operator::In {
            return self.as_json_in_sql(jsonb_field_expr, &field_expr, path, value, neg);
        }

        let sql_type = get_jsonb_field_type_from_value_and_operator(value, op.clone());

        // TODO: Add JSON Schema usage type support via
        // get_jsonb_field_type_from_json_schema(schema, key)

        match sql_type {
            Some(SQLMappedType::Numeric) => {
                return self.as_json_numeric_sql(&field_expr, value, op, neg);
            }
            Some(SQLMappedType::Date) => {
                return self.as_json_date_sql(&field_expr, value, op, neg);
            }
            Some(SQLMappedType::Boolean) => {
                return self.as_json_boolean_sql(&field_expr, value, op, neg);
            }
            _ => {}
        }

        let (sql_op, value) = match op {
            Operator::Equals => ("=", (*value).to_string()),
            Operator::IEquals => ("ILIKE", (*value).to_string()),
            Operator::Contains | Operator::Like => ("LIKE", format!("%{value}%")),
            Operator::IContains => ("ILIKE", format!("%{value}%")),
            Operator::StartsWith => ("LIKE", format!("{value}%")),
            Operator::IStartsWith => ("ILIKE", format!("{value}%")),
            Operator::EndsWith => ("LIKE", format!("%{value}")),
            Operator::IEndsWith => ("ILIKE", format!("%{value}")),
            Operator::Regex => ("~", (*value).to_string()),
            Operator::Gt => (">", (*value).to_string()),
            Operator::Gte => (">=", (*value).to_string()),
            Operator::Lt => ("<", (*value).to_string()),
            Operator::Lte => ("<=", (*value).to_string()),

            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Invalid operator for JSON: '{op:?}'"
                )));
            }
        };

        let sql = match sql_type {
            None => {
                return Err(ApiError::BadRequest(format!(
                    "Invalid JSON type mapping between key '{}' and operator '{:?}'",
                    path, self.operator
                )));
            }
            Some(SQLMappedType::String) | Some(SQLMappedType::None) => {
                bind_variables.push(SQLValue::String(value));
                format!("{}{} {} ?", neg_str, field_expr, sql_op)
            }
            Some(SQLMappedType::Numeric)
            | Some(SQLMappedType::Date)
            | Some(SQLMappedType::Boolean) => unreachable!(),
        };

        debug!(message = "SQL JSONB generation", sql = %sql, bind_varaibles = ?bind_variables);

        Ok(SQLComponent {
            sql,
            bind_variables,
        })
    }
    fn as_json_ip_sql(
        &self,
        field_expr: &str,
        value: &str,
        op: Operator,
        negated: bool,
    ) -> Result<SQLComponent, ApiError> {
        let value = parse_json_ip_filter_value(value, &op)?;
        let lhs_expr = format!("try_inet({field_expr})");
        let sql_op = match op {
            Operator::InetEquals => "=",
            Operator::WithinNetwork => "<<=",
            Operator::ContainsNetwork => ">>=",
            Operator::ContainsIp => ">>",
            Operator::OverlapsNetwork => "&&",
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Invalid operator for JSON IP search: '{op:?}'"
                )));
            }
        };
        let predicate = if negated {
            format!("NOT ({lhs_expr} {sql_op} ?::inet)")
        } else {
            format!("{lhs_expr} {sql_op} ?::inet")
        };

        Ok(SQLComponent {
            sql: format!("{lhs_expr} IS NOT NULL AND {predicate}"),
            bind_variables: vec![SQLValue::String(value)],
        })
    }

    fn as_json_is_null_sql(
        &self,
        jsonb_field_expr: &str,
        negated: bool,
    ) -> Result<SQLComponent, ApiError> {
        let path = &self.value;
        let path = JsonFieldPathRef::new(path)?;
        let field_expr = json_text_path_expression(jsonb_field_expr, path);
        let sql = if negated {
            format!("{field_expr} IS NOT NULL")
        } else {
            format!("{field_expr} IS NULL")
        };
        Ok(SQLComponent {
            sql,
            bind_variables: vec![],
        })
    }

    fn as_json_has_key_sql(
        &self,
        jsonb_field_expr: &str,
        path: JsonFieldPathRef<'_>,
        key_name: &str,
        negated: bool,
    ) -> Result<SQLComponent, ApiError> {
        let jsonb_expr = json_value_path_expression(jsonb_field_expr, path);
        let predicate = format!("jsonb_has_key({jsonb_expr}, ?)");
        let sql = if negated {
            format!("NOT ({predicate})")
        } else {
            predicate
        };
        Ok(SQLComponent {
            sql,
            bind_variables: vec![SQLValue::String(key_name.to_string())],
        })
    }

    fn as_json_in_sql(
        &self,
        jsonb_field_expr: &str,
        field_expr: &str,
        path: JsonFieldPathRef<'_>,
        value: &str,
        negated: bool,
    ) -> Result<SQLComponent, ApiError> {
        let values: Vec<&str> = value.split(',').collect();
        if values.is_empty() {
            return Err(ApiError::BadRequest(
                "'in' requires at least one value".to_string(),
            ));
        }

        // Scalar check: text extraction IN
        let scalar_placeholders = values.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let scalar_check = format!("{field_expr} IN ({scalar_placeholders})");

        // Array check: jsonb containment
        let array_placeholders = values.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let jsonb_expr = json_value_path_expression(jsonb_field_expr, path);
        let array_check = format!("jsonb_contains_any({jsonb_expr}, ARRAY[{array_placeholders}])");

        let combined = format!("({scalar_check} OR {array_check})");
        let sql = if negated {
            format!("NOT {combined}")
        } else {
            combined
        };

        // Bind values twice: once for IN, once for ARRAY
        let mut bind_variables: Vec<SQLValue> = values
            .iter()
            .map(|v| SQLValue::String(v.to_string()))
            .collect();
        let array_binds: Vec<SQLValue> = values
            .iter()
            .map(|v| SQLValue::String(v.to_string()))
            .collect();
        bind_variables.extend(array_binds);

        Ok(SQLComponent {
            sql,
            bind_variables,
        })
    }

    fn as_json_all_sql(
        &self,
        jsonb_field_expr: &str,
        path: JsonFieldPathRef<'_>,
        value: &str,
        negated: bool,
    ) -> Result<SQLComponent, ApiError> {
        let values: Vec<&str> = value.split(',').collect();
        if values.is_empty() {
            return Err(ApiError::BadRequest(
                "'all' requires at least one value".to_string(),
            ));
        }
        let placeholders = values.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let jsonb_expr = json_value_path_expression(jsonb_field_expr, path);
        let predicate = format!("jsonb_contains_all({jsonb_expr}, ARRAY[{placeholders}])");
        let sql = if negated {
            format!("NOT ({predicate})")
        } else {
            predicate
        };
        let bind_variables = values
            .iter()
            .map(|v| SQLValue::String(v.to_string()))
            .collect();
        Ok(SQLComponent {
            sql,
            bind_variables,
        })
    }

    fn as_json_array_length_sql(
        &self,
        jsonb_field_expr: &str,
        path: JsonFieldPathRef<'_>,
        value: &str,
        negated: bool,
    ) -> Result<SQLComponent, ApiError> {
        let length: i32 = value.parse().map_err(|_| {
            ApiError::BadRequest(format!("array_length requires an integer, got '{value}'"))
        })?;
        let jsonb_expr = json_value_path_expression(jsonb_field_expr, path);
        let len_expr = format!("jsonb_array_length({jsonb_expr})");
        let cmp = if negated { "!=" } else { "=" };
        let sql = format!("jsonb_typeof({jsonb_expr}) = 'array' AND {len_expr} {cmp} ?");
        Ok(SQLComponent {
            sql,
            bind_variables: vec![SQLValue::Integer(length)],
        })
    }

    fn as_json_numeric_sql(
        &self,
        field_expr: &str,
        value: &str,
        op: Operator,
        negated: bool,
    ) -> Result<SQLComponent, ApiError> {
        let bind_variables = value
            .as_integer()?
            .into_iter()
            .map(SQLValue::Integer)
            .collect::<Vec<_>>();
        self.as_json_cast_sql(
            &format!("try_numeric({field_expr})"),
            bind_variables,
            op,
            negated,
        )
    }

    fn as_json_date_sql(
        &self,
        field_expr: &str,
        value: &str,
        op: Operator,
        negated: bool,
    ) -> Result<SQLComponent, ApiError> {
        let bind_variables = value
            .as_date()?
            .into_iter()
            .map(SQLValue::Date)
            .collect::<Vec<_>>();
        self.as_json_cast_sql(
            &format!("try_timestamp({field_expr})"),
            bind_variables,
            op,
            negated,
        )
    }

    fn as_json_boolean_sql(
        &self,
        field_expr: &str,
        value: &str,
        op: Operator,
        negated: bool,
    ) -> Result<SQLComponent, ApiError> {
        let bind_variables = vec![SQLValue::Boolean(value.as_boolean()?)];
        self.as_json_cast_sql(
            &format!("try_boolean({field_expr})"),
            bind_variables,
            op,
            negated,
        )
    }

    fn as_json_cast_sql(
        &self,
        lhs_expr: &str,
        bind_variables: Vec<SQLValue>,
        op: Operator,
        negated: bool,
    ) -> Result<SQLComponent, ApiError> {
        let predicate = match op {
            Operator::Equals => {
                if bind_variables.len() != 1 {
                    return Err(ApiError::BadRequest(format!(
                        "Operator 'equals' requires exactly 1 value for JSON field '{}'",
                        self.field
                    )));
                }
                format!("{lhs_expr} = ?")
            }
            Operator::Gt => {
                if bind_variables.len() != 1 {
                    return Err(ApiError::BadRequest(format!(
                        "Operator 'gt' requires exactly 1 value for JSON field '{}'",
                        self.field
                    )));
                }
                format!("{lhs_expr} > ?")
            }
            Operator::Gte => {
                if bind_variables.len() != 1 {
                    return Err(ApiError::BadRequest(format!(
                        "Operator 'gte' requires exactly 1 value for JSON field '{}'",
                        self.field
                    )));
                }
                format!("{lhs_expr} >= ?")
            }
            Operator::Lt => {
                if bind_variables.len() != 1 {
                    return Err(ApiError::BadRequest(format!(
                        "Operator 'lt' requires exactly 1 value for JSON field '{}'",
                        self.field
                    )));
                }
                format!("{lhs_expr} < ?")
            }
            Operator::Lte => {
                if bind_variables.len() != 1 {
                    return Err(ApiError::BadRequest(format!(
                        "Operator 'lte' requires exactly 1 value for JSON field '{}'",
                        self.field
                    )));
                }
                format!("{lhs_expr} <= ?")
            }
            Operator::Between => {
                if bind_variables.len() != 2 {
                    return Err(ApiError::BadRequest(format!(
                        "Operator 'between' requires exactly 2 values for JSON field '{}'",
                        self.field
                    )));
                }
                format!("{lhs_expr} BETWEEN ? AND ?")
            }
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Invalid operator for typed JSON search: '{op:?}'"
                )));
            }
        };

        let predicate = if negated {
            format!("NOT ({predicate})")
        } else {
            predicate
        };

        Ok(SQLComponent {
            sql: format!("{lhs_expr} IS NOT NULL AND {predicate}"),
            bind_variables,
        })
    }
}

fn json_path_sql_literal(path: JsonFieldPathRef<'_>) -> String {
    format!("'{{{path}}}'")
}

fn json_text_path_expression(jsonb_field_expr: &str, path: JsonFieldPathRef<'_>) -> String {
    format!("{jsonb_field_expr} #>> {}", json_path_sql_literal(path))
}

fn json_value_path_expression(jsonb_field_expr: &str, path: JsonFieldPathRef<'_>) -> String {
    format!("{jsonb_field_expr} #> {}", json_path_sql_literal(path))
}

fn parse_json_ip_filter_value(value: &str, operator: &Operator) -> Result<String, ApiError> {
    match operator {
        Operator::ContainsIp => value
            .parse::<IpAddr>()
            .map(|ip| ip.to_string())
            .map_err(|_| ApiError::BadRequest(format!("Invalid IP address: '{value}'"))),
        Operator::WithinNetwork
        | Operator::ContainsNetwork
        | Operator::OverlapsNetwork
        | Operator::InetEquals => parse_ip_or_host_network(value),
        _ => Err(ApiError::InternalServerError(format!(
            "Unexpected non-IP operator passed to IP parser: '{operator:?}'"
        ))),
    }
}

fn parse_ip_or_host_network(value: &str) -> Result<String, ApiError> {
    IpNet::from_str(value)
        .or_else(|_| ip_to_host_net(value))
        .map(|network| network.to_string())
        .map_err(|_| ApiError::BadRequest(format!("Invalid IP/CIDR: '{value}'")))
}

fn ip_to_host_net(value: &str) -> Result<IpNet, ()> {
    match value.parse::<IpAddr>() {
        Ok(IpAddr::V4(address)) => Ipv4Net::new(address, 32).map(IpNet::from).map_err(|_| ()),
        Ok(IpAddr::V6(address)) => Ipv6Net::new(address, 128).map(IpNet::from).map_err(|_| ()),
        Err(_) => Err(()),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct JsonSqlPredicate {
    sql: String,
    bind_variables: Vec<SQLValue>,
}

impl Expression for JsonSqlPredicate {
    type SqlType = Bool;
}

impl QueryId for JsonSqlPredicate {
    type QueryId = ();

    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for JsonSqlPredicate {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();
        let mut start = 0;
        for (bind_variable, offset) in self
            .bind_variables
            .iter()
            .zip(bind_placeholder_offsets(&self.sql))
        {
            out.push_sql(&self.sql[start..offset]);
            bind_sql_value(&mut out, bind_variable)?;
            start = offset + 1;
        }
        out.push_sql(&self.sql[start..]);
        Ok(())
    }
}

impl<QS> SelectableExpression<QS> for JsonSqlPredicate {}
impl<QS> AppearsOnTable<QS> for JsonSqlPredicate {}

impl<GB> ValidGrouping<GB> for JsonSqlPredicate {
    type IsAggregate = diesel::expression::is_aggregate::Never;
}

fn bind_sql_value<'b>(out: &mut AstPass<'_, 'b, Pg>, value: &'b SQLValue) -> QueryResult<()> {
    match value {
        SQLValue::String(value) => out.push_bind_param::<Text, _>(value),
        SQLValue::Integer(value) => out.push_bind_param::<Integer, _>(value),
        SQLValue::BigInteger(value) => out.push_bind_param::<BigInt, _>(value),
        SQLValue::Date(value) => out.push_bind_param::<Timestamp, _>(value),
        SQLValue::Boolean(value) => out.push_bind_param::<Bool, _>(value),
    }
}

pub(crate) fn dynamic_sql_predicate(component: SQLComponent) -> Result<JsonSqlPredicate, ApiError> {
    let placeholder_count = bind_placeholder_offsets(&component.sql).count();
    if placeholder_count != component.bind_variables.len() {
        return Err(ApiError::InternalServerError(format!(
            "Dynamic SQL predicate has {placeholder_count} placeholders but {} bind values",
            component.bind_variables.len()
        )));
    }
    Ok(JsonSqlPredicate {
        sql: component.sql,
        bind_variables: component.bind_variables,
    })
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

pub trait JsonPredicateExt {
    fn as_json_predicate(&self) -> Result<JsonSqlPredicate, ApiError>;
}

impl JsonPredicateExt for ParsedQueryParam {
    fn as_json_predicate(&self) -> Result<JsonSqlPredicate, ApiError> {
        dynamic_sql_predicate(self.as_json_sql()?)
    }
}

#[cfg(test)]
mod tests {
    use super::bind_placeholder_offsets;

    #[test]
    fn bind_placeholders_ignore_question_marks_in_sql_strings() {
        let sql = "scope = '[{\"path\":\"/answer?\"}]' AND escaped = 'it''s?' AND value = ?";

        assert_eq!(
            bind_placeholder_offsets(sql).collect::<Vec<_>>(),
            vec![sql.len() - 1]
        );
    }

    #[test]
    fn bind_placeholders_returns_each_unquoted_offset() {
        let sql = "first = ? AND second = ?";

        assert_eq!(
            bind_placeholder_offsets(sql).collect::<Vec<_>>(),
            vec![8, 23]
        );
    }
}
