use chrono::NaiveDateTime;
use diesel::expression::{AppearsOnTable, Expression, SelectableExpression, ValidGrouping};
use diesel::pg::Pg;
use diesel::query_builder::{AstPass, QueryFragment, QueryId};
use diesel::result::QueryResult;
use diesel::sql_types::{Bool, Float8, Integer, Text, Timestamp};
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::IpAddr;
use std::str::FromStr;
use tracing::debug;

pub use hubuum_query::{
    DataType, FilterField, Operator, ParsedQueryParam, QueryOptions, SQLMappedType, SearchOperator,
    SortParam, StatementTimeoutMs, get_jsonb_field_type_from_value_and_operator,
};
#[cfg(test)]
use hubuum_query::{get_jsonb_field_type_from_json_schema, get_sql_mapped_type_from_value};

use crate::errors::ApiError;
use crate::models::permissions::{Permissions, PermissionsList};
use crate::pagination::validate_page_limit;
use crate::traits::SelfAccessors;
use crate::utilities::extensions::CustomStringExtensions;

use super::HubuumClassID;

/// ## Parse a query string into search parameters
///
/// ## Arguments
///
/// * `query_string` - A string that contains the query parameters
///
/// ## Returns
///
pub fn parse_query_parameter(qs: &str) -> Result<QueryOptions, ApiError> {
    let (query_options, _) = parse_query_parameter_with_passthrough(qs, &[])?;
    Ok(query_options)
}

pub fn parse_query_parameter_with_passthrough(
    qs: &str,
    passthrough_keys: &[&str],
) -> Result<(QueryOptions, HashMap<String, Vec<String>>), ApiError> {
    let (mut query_options, passthrough) =
        hubuum_query::parse_query_parameter_with_passthrough(qs, passthrough_keys)?;
    query_options.limit = query_options.limit.map(validate_page_limit).transpose()?;
    Ok((query_options, passthrough))
}

impl From<hubuum_query::QueryError> for ApiError {
    fn from(error: hubuum_query::QueryError) -> Self {
        match error {
            hubuum_query::QueryError::BadRequest(message) => ApiError::BadRequest(message),
            hubuum_query::QueryError::InvalidIntegerRange(message) => {
                ApiError::InvalidIntegerRange(message)
            }
        }
    }
}

/// ## A struct that represents a SQL query component.
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
pub struct SQLComponent {
    pub sql: String,
    pub bind_variables: Vec<SQLValue>,
}

impl SQLComponent {
    fn placeholder_count(&self) -> usize {
        self.sql.chars().filter(|c| *c == '?').count()
    }

    pub fn into_predicate(self) -> Result<JsonSqlPredicate, ApiError> {
        let placeholder_count = self.placeholder_count();
        if placeholder_count != self.bind_variables.len() {
            return Err(ApiError::InternalServerError(format!(
                "JSON SQL predicate has {placeholder_count} placeholders but {} bind values",
                self.bind_variables.len()
            )));
        }

        Ok(JsonSqlPredicate {
            sql: self.sql,
            bind_variables: self.bind_variables,
        })
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

        // `?` is reserved for bind placeholders in these generated fragments.
        // `SQLComponent::into_predicate` checks that placeholder and bind counts match.
        let mut sql_parts = self.sql.split('?');
        if let Some(first_part) = sql_parts.next() {
            out.push_sql(first_part);
        }

        for (bind_variable, sql_part) in self.bind_variables.iter().zip(sql_parts) {
            bind_sql_value(&mut out, bind_variable)?;
            out.push_sql(sql_part);
        }

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
        SQLValue::Float(value) => out.push_bind_param::<Float8, _>(value),
        SQLValue::Integer(value) => out.push_bind_param::<Integer, _>(value),
        SQLValue::Date(value) => out.push_bind_param::<Timestamp, _>(value),
        SQLValue::Boolean(value) => out.push_bind_param::<Bool, _>(value),
    }
}

/// ## An sql value for bind variables
///
/// This enum represents the different types of values that can be bound to a SQL query. The types
/// are defined as we need to bind the correct type in Diesel.
#[derive(Debug, Clone, PartialEq)]
pub enum SQLValue {
    String(String),
    Float(f64),
    Integer(i32),
    Date(NaiveDateTime),
    Boolean(bool),
}

pub trait QueryOptionsExt {
    /// ## Ensure that a filter is present in the query options
    ///
    /// This function checks if a filter with the given field and identifier is
    /// already present in the filters list. If not, it adds a new filter with
    /// the given field and identifier.
    ///
    /// ### Arguments
    ///
    /// * `field` - The field to check for
    /// * `operator` - The operator to check for
    /// * `identifier` - The identifier to add if the filter is not present
    ///
    /// ### Returns
    ///
    /// * None
    fn ensure_filter<I, T>(
        &mut self,
        field: FilterField,
        operator: SearchOperator,
        identifier: &I,
    ) -> bool
    where
        I: SelfAccessors<T>;

    fn ensure_filter_exact(&mut self, field: FilterField, identifier: &HubuumClassID) -> bool;
}

impl QueryOptionsExt for QueryOptions {
    fn ensure_filter<I, T>(
        &mut self,
        field: FilterField,
        operator: SearchOperator,
        identifier: &I,
    ) -> bool
    where
        I: SelfAccessors<T>,
    {
        let id_string = identifier.id().to_string();
        self.filters.ensure_filter(field, operator, &id_string)
    }

    /// ## Ensure that an equality filter is present in the query options
    ///
    /// This function checks if an equality filter with the given field and identifier is already
    /// present in the filters list. If not, it adds a new equality filter with the given field and identifier.
    ///
    /// ### Arguments
    ///
    /// * `field` - The field to check for
    /// * `identifier` - The identifier to add if the filter is not present
    ///
    /// ### Returns
    ///
    /// * bool - true if the filter was added, false if it already existed
    fn ensure_filter_exact(&mut self, field: FilterField, identifier: &HubuumClassID) -> bool {
        self.filters.ensure_filter(
            field,
            SearchOperator::Equals { is_negated: false },
            &identifier.id().to_string(),
        )
    }
}

pub trait ParsedQueryParamExt {
    /// ## Coerce the value into a Permissions enum
    ///
    /// ### Returns
    ///
    /// * A Permissions enum or ApiError::BadRequest if the value is invalid
    fn value_as_permission(&self) -> Result<Permissions, ApiError>;

    /// ## Coerce the value into a list of integers
    ///
    /// Accepts the format given to the [`as_integer`] trait.
    ///
    /// ### Returns
    ///
    /// * A vector of integers or ApiError::BadRequest if the value is invalid
    fn value_as_integer(&self) -> Result<Vec<i32>, ApiError>;

    /// ## Coerce the value into a list of dates
    ///
    /// Accepts a comma separated list of RFC3339 dates.
    /// https://www.rfc-editor.org/rfc/rfc3339
    ///     
    /// ### Returns
    ///
    /// * A vector of NaiveDateTime or ApiError::BadRequest if the value is invalid
    fn value_as_date(&self) -> Result<Vec<NaiveDateTime>, ApiError>;

    /// ## Coerce the value into a boolean
    ///
    /// Accepted values are "true" and "false" (case insensitive)
    ///
    /// ### Returns
    ///
    /// * A boolean or ApiError::BadRequest if the value is invalid
    fn value_as_boolean(&self) -> Result<bool, ApiError>;

    /// ## Coerce the entire ParsedQueryParam into a JSONB SQLComponent
    ///
    /// This is creates a JSONB SQLComponent from a ParsedQueryParam.
    ///
    /// ### Constraints on the ParseQueryParam
    ///
    /// * The field must be a JSONB field (see `is_json`).
    /// * The value must be a key=value pair. The key is the JSONB
    ///   key to search in and the value is the value to search for.
    ///
    /// The operator is used to determine the type of search to perform. In the future we
    /// may also use the schema to determine the type of the value.
    ///
    /// Note that the key is not validated against the schema, this is something that may be
    /// added in the future. Right now, having a typo in the key will just result in no matches.
    ///
    /// ### Returns
    ///
    /// * A SQLComponent or:
    ///   * ApiError::InternalServerError if the field is not JSONB
    ///   * ApiError::BadRequest if the value is not a key=value pair
    fn as_json_sql(&self) -> Result<SQLComponent, ApiError>;

    fn as_json_predicate(&self) -> Result<JsonSqlPredicate, ApiError>;

    fn as_json_sql_for_field_expr(&self, jsonb_field_expr: &str) -> Result<SQLComponent, ApiError>;
}

trait ParsedQueryParamSqlExt {
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
        path: &str,
        key_name: &str,
        negated: bool,
    ) -> Result<SQLComponent, ApiError>;

    fn as_json_in_sql(
        &self,
        jsonb_field_expr: &str,
        field_expr: &str,
        path: &str,
        value: &str,
        negated: bool,
    ) -> Result<SQLComponent, ApiError>;

    fn as_json_all_sql(
        &self,
        jsonb_field_expr: &str,
        path: &str,
        value: &str,
        negated: bool,
    ) -> Result<SQLComponent, ApiError>;

    fn as_json_array_length_sql(
        &self,
        jsonb_field_expr: &str,
        path: &str,
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

impl ParsedQueryParamExt for ParsedQueryParam {
    fn value_as_permission(&self) -> Result<Permissions, ApiError> {
        self.value.as_permission()
    }

    fn value_as_integer(&self) -> Result<Vec<i32>, ApiError> {
        self.value.as_integer()
    }

    fn value_as_date(&self) -> Result<Vec<NaiveDateTime>, ApiError> {
        self.value.as_date()
    }

    fn value_as_boolean(&self) -> Result<bool, ApiError> {
        self.value.as_boolean()
    }

    fn as_json_sql(&self) -> Result<SQLComponent, ApiError> {
        let field = self.field.clone();
        self.as_json_sql_for_field_expr(field.table_field())
    }

    fn as_json_predicate(&self) -> Result<JsonSqlPredicate, ApiError> {
        self.as_json_sql()?.into_predicate()
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

        // split the value on key=value
        let parts: Vec<&str> = self.value.splitn(2, '=').collect();

        let (key, value) = match parts.as_slice() {
            [key, value] => (*key, *value),
            _ => {
                return Err(ApiError::BadRequest(
                    "Expected exactly two parts of key=value".to_string(),
                ));
            }
        };

        // Validate the key
        if !key.is_valid_jsonb_search_key() {
            return Err(ApiError::BadRequest(format!(
                "Invalid JSON search key: '{key}'"
            )));
        }

        // Validate the value, no longer needed as we're using bind variables
        /*
        if !value.is_valid_jsonb_search_value() {
            return Err(ApiError::BadRequest(format!(
                "Invalid JSON search value: '{}'",
                value
            )));
        }
        */

        let raw_key = key;
        let key = format!("'{{{raw_key}}}'");
        let field_expr = format!("{jsonb_field_expr} #>> {key}");

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
            return self.as_json_has_key_sql(jsonb_field_expr, raw_key, value, neg);
        }

        if op == Operator::All {
            return self.as_json_all_sql(jsonb_field_expr, raw_key, value, neg);
        }

        if op == Operator::ArrayLength {
            return self.as_json_array_length_sql(jsonb_field_expr, raw_key, value, neg);
        }

        if op == Operator::In {
            return self.as_json_in_sql(jsonb_field_expr, &field_expr, raw_key, value, neg);
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
                    key, self.operator
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
}

impl ParsedQueryParamSqlExt for ParsedQueryParam {
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
        if !path.is_valid_jsonb_search_key() {
            return Err(ApiError::BadRequest(format!(
                "Invalid JSON search key: '{path}'"
            )));
        }
        let key = format!("'{{{path}}}'");
        let field_expr = format!("{jsonb_field_expr} #>> {key}");
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
        path: &str,
        key_name: &str,
        negated: bool,
    ) -> Result<SQLComponent, ApiError> {
        let key = format!("'{{{path}}}'");
        let jsonb_expr = format!("{jsonb_field_expr} #> {key}");
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
        path: &str,
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
        let key = format!("'{{{path}}}'");
        let jsonb_expr = format!("{jsonb_field_expr} #> {key}");
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
        path: &str,
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
        let key = format!("'{{{path}}}'");
        let jsonb_expr = format!("{jsonb_field_expr} #> {key}");
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
        path: &str,
        value: &str,
        negated: bool,
    ) -> Result<SQLComponent, ApiError> {
        let length: i32 = value.parse().map_err(|_| {
            ApiError::BadRequest(format!("array_length requires an integer, got '{value}'"))
        })?;
        let key = format!("'{{{path}}}'");
        let jsonb_expr = format!("{jsonb_field_expr} #> {key}");
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

pub trait QueryParamsExt {
    /// ## Get a list of permissions from a list of parsed query parameters
    ///
    /// Iterate over the parsed query parameters and filter out the ones that are permissions,
    /// defined as having the `field` set as "permissions". For each value of each parsed query
    /// parameter, attempt to parse it into a Permissions enum. If the value is not a valid
    /// permission, return an ApiError::BadRequest.
    ///
    /// ### Returns    
    ///
    /// * A PermissionsList of Permissions or ApiError::BadRequest if the permissions are invalid
    fn permissions(&self) -> Result<PermissionsList<Permissions>, ApiError>;

    /// ## Get a list of all JSON Schema elements in a list of parsed query parameters
    ///
    /// Iterate over the parsed query parameters and filter out the ones that are JSON Schemas,
    /// defined as having the `field` set as "json_schema". Also validates both keys and values
    /// and their matching to the operator.
    fn json_schemas(&self) -> Result<Vec<&ParsedQueryParam>, ApiError>;

    /// ## Get a list of all JSON Data elements in a list of parsed query parameters
    ///
    /// Iterate over the parsed query parameters and filter out the ones that are JSON Data,
    /// defined as having the `field` set as "json_data". Also validates both keys and values
    /// and their matching to the operator.
    fn json_datas(&self, filter: FilterField) -> Result<Vec<&ParsedQueryParam>, ApiError>;

    /// ## Add a filter to the query options
    ///
    /// Blindly add a filter to the query params. This may lead to duplicate filters.
    ///
    /// ### Arguments
    ///
    /// * `field` - The field to add
    /// * `operator` - The operator to add
    ///
    /// ### Returns
    ///
    /// * None
    fn add_filter(&mut self, field: FilterField, operator: SearchOperator, value: &str);

    /// ## Ensure a filter is present in the query options
    ///
    /// This function checks if a filter with the given field and operator exists in the list of
    /// parsed query parameters. If not, it adds a new filter with the given field and operator.
    ///
    /// ### Arguments
    ///
    /// * `field` - The field to check for
    /// * `operator` - The operator to check for
    /// * `value` - The value to check for
    ///
    /// ### Returns
    ///
    /// * true if the filter was added, false if it already exists
    fn ensure_filter(&mut self, field: FilterField, operator: SearchOperator, value: &str) -> bool;

    /// ## Check if a filter exists
    ///
    /// This function checks if a filter with the given field and operator exists in the list of
    /// parsed query parameters.
    ///
    /// ### Arguments
    ///
    /// * `field` - The field to check for
    /// * `operator` - The operator to check for
    ///
    /// ### Returns
    ///
    /// * true if the filter exists, false if it does not
    fn filter_exists(&self, field: FilterField, operator: SearchOperator) -> bool;
}

impl QueryParamsExt for Vec<ParsedQueryParam> {
    /// ## Get a list of all Permissions in a list of parsed query parameters
    ///
    /// Iterate over the parsed query parameters and filter out the ones that are permissions,
    /// defined as having the `field` set as "permissions". For each value of a matching parsed query
    /// parameter, attempt to parse it into a Permissions enum.
    ///
    /// Note that the list is not sorted and duplicates are removed.
    ///
    /// If any value is not a valid permission, return an ApiError::BadRequest.
    fn permissions(&self) -> Result<PermissionsList<Permissions>, ApiError> {
        let mut unique_permissions = HashSet::new();
        for param in self.iter().filter(|p| p.is_permission()) {
            let permission = param.value_as_permission()?;
            unique_permissions.insert(permission);
        }
        Ok(PermissionsList::new(unique_permissions))
    }
    /// ## Get a list of all JSON schema entries in a list of parsed query parameters
    ///
    /// Iterate over the parsed query parameters and filter out the ones that are JSON Schemas,
    /// defined as having the `field` set as "json_schema".
    fn json_schemas(&self) -> Result<Vec<&ParsedQueryParam>, ApiError> {
        let json_schema: Vec<&ParsedQueryParam> =
            self.iter().filter(|p| p.is_json_schema()).collect();

        Ok(json_schema)
    }

    /// ## Get a list of all JSON data entries in a list of parsed query parameters
    ///
    /// Iterate over the parsed query parameters and filter out the ones that are JSON Schemas,
    /// defined as having the `field` set as "json_data".
    fn json_datas(&self, field: FilterField) -> Result<Vec<&ParsedQueryParam>, ApiError> {
        let json_schema: Vec<&ParsedQueryParam> = self
            .iter()
            .filter(|p| p.is_json_data() && p.field == field)
            .collect();

        Ok(json_schema)
    }

    fn add_filter(&mut self, field: FilterField, operator: SearchOperator, value: &str) {
        self.push(ParsedQueryParam {
            field,
            operator,
            value: value.to_string(),
        });
    }

    fn filter_exists(&self, field: FilterField, operator: SearchOperator) -> bool {
        self.iter()
            .any(|p| p.field == field && p.operator == operator)
    }

    fn ensure_filter(&mut self, field: FilterField, operator: SearchOperator, value: &str) -> bool {
        if !self.filter_exists(field.clone(), operator.clone()) {
            self.add_filter(field, operator, value);
            true
        } else {
            false
        }
    }
}

/// Operators
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
        .map(|net| net.to_string())
        .map_err(|_| ApiError::BadRequest(format!("Invalid IP/CIDR: '{value}'")))
}

fn ip_to_host_net(value: &str) -> Result<IpNet, ()> {
    match value.parse::<IpAddr>() {
        Ok(IpAddr::V4(addr)) => Ipv4Net::new(addr, 32).map(IpNet::from).map_err(|_| ()),
        Ok(IpAddr::V6(addr)) => Ipv6Net::new(addr, 128).map(IpNet::from).map_err(|_| ()),
        Err(_) => Err(()),
    }
}

// TODO: Rewrite to use rstest cases...
#[cfg(test)]
mod test {
    use std::vec;

    use super::*;

    struct TestCase {
        query_string: &'static str,
        expected: Vec<ParsedQueryParam>,
    }

    fn pq(field: &str, operator: SearchOperator, value: &str) -> ParsedQueryParam {
        ParsedQueryParam {
            field: FilterField::from_str(field).unwrap(),
            operator,
            value: value.to_string(),
        }
    }

    #[test]
    fn test_empty_query_string_returns_empty_vec() {
        let result = parse_query_parameter("").unwrap();
        assert_eq!(result.filters, vec![]);
    }

    #[test]
    fn test_query_string_without_equal_sign_returns_error() {
        let result = parse_query_parameter("name");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_integer_list_single() {
        let test_cases = vec![
            ("1", vec![1]),
            ("2,4", vec![2, 4]),
            ("3,3,3,6", vec![3, 6]),
            ("4,1,4,1,5", vec![1, 4, 5]),
        ];

        for (input, expected) in test_cases {
            let result = input.as_integer();
            assert_eq!(result, Ok(expected), "Failed test case for input: {input}");
        }
    }

    #[test]
    fn test_parse_integer_list_range() {
        let test_cases = vec![
            ("1-4", vec![1, 2, 3, 4]),
            ("2-4", vec![2, 3, 4]),
            ("3-4", vec![3, 4]),
            ("4-4", vec![4]),
        ];

        for (input, expected) in test_cases {
            let result = input.as_integer();
            assert_eq!(result, Ok(expected), "Failed test case for input: {input}");
        }
    }

    #[test]
    fn test_parse_integer_list_mixed() {
        let test_cases = vec![
            ("1,2,3,4", vec![1, 2, 3, 4]),
            ("1-4,6-8", vec![1, 2, 3, 4, 6, 7, 8]),
            ("1,2,3-5,7", vec![1, 2, 3, 4, 5, 7]),
            ("1-4,3,3,8", vec![1, 2, 3, 4, 8]),
            ("-4--2", vec![-4, -3, -2]),
            ("-90", vec![-90]),
        ];

        for (input, expected) in test_cases {
            let result = input.as_integer();
            assert_eq!(result, Ok(expected), "Failed test case for input: {input}",);
        }
    }

    #[test]
    fn test_parse_integer_list_failures() {
        let test_cases = vec!["1-", "-4--6", "1-2-3"];

        for input in test_cases {
            let result = input.as_integer();
            assert!(
                result.is_err(),
                "Failed test case for input: {input} (no error) {result:?}",
            );
        }
    }

    #[test]
    fn test_query_string_bad_request() {
        let test_cases = vec![
            "name__icontains=foo&description=bar&invalid",
            "name__icontains=foo&description=bar&invalid=",
            "name__icontains=foo&description=bar&invalid=foo&name__invalid=bar",
        ];

        let test_case_errors = [
            "Invalid query parameter: 'invalid'",
            "Invalid query parameter: 'invalid', no value",
            "Invalid search field: 'invalid'",
        ];

        for (i, case) in test_cases.into_iter().enumerate() {
            let result = parse_query_parameter(case);
            assert!(
                result.is_err(),
                "Failed test case for query: {case} (no error) {result:?}",
            );
            let result_err = result.unwrap_err();
            assert_eq!(
                result_err,
                ApiError::BadRequest(test_case_errors[i].to_string()),
                "Failed test case for query: {case} ({} vs {})",
                result_err,
                test_case_errors[i]
            );
        }
    }

    #[test]
    fn test_query_string_parsing() {
        let test_cases = vec![
            TestCase {
                query_string: "name__icontains=foo&description=bar",
                expected: vec![
                    pq(
                        "name",
                        SearchOperator::IContains { is_negated: false },
                        "foo",
                    ),
                    pq(
                        "description",
                        SearchOperator::Equals { is_negated: false },
                        "bar",
                    ),
                ],
            },
            TestCase {
                query_string: "name__contains=foo&description__icontains=bar&created_at__gte=2021-01-01&updated_at__lte=2021-12-31",
                expected: vec![
                    pq(
                        "name",
                        SearchOperator::Contains { is_negated: false },
                        "foo",
                    ),
                    pq(
                        "description",
                        SearchOperator::IContains { is_negated: false },
                        "bar",
                    ),
                    pq(
                        "created_at",
                        SearchOperator::Gte { is_negated: false },
                        "2021-01-01",
                    ),
                    pq(
                        "updated_at",
                        SearchOperator::Lte { is_negated: false },
                        "2021-12-31",
                    ),
                ],
            },
            TestCase {
                query_string: "name__not_icontains=foo&description=bar&permissions=CanRead&validate_schema=true",
                expected: vec![
                    pq(
                        "name",
                        SearchOperator::IContains { is_negated: true },
                        "foo",
                    ),
                    pq(
                        "description",
                        SearchOperator::Equals { is_negated: false },
                        "bar",
                    ),
                    pq(
                        "permissions",
                        SearchOperator::Equals { is_negated: false },
                        "CanRead",
                    ),
                    pq(
                        "validate_schema",
                        SearchOperator::Equals { is_negated: false },
                        "true",
                    ),
                ],
            },
            TestCase {
                query_string: "json_data__within_network=network,address=10.0.0.0/24&json_data__contains_ip=network,address=10.0.0.10",
                expected: vec![
                    pq(
                        "json_data",
                        SearchOperator::WithinNetwork { is_negated: false },
                        "network,address=10.0.0.0/24",
                    ),
                    pq(
                        "json_data",
                        SearchOperator::ContainsIp { is_negated: false },
                        "network,address=10.0.0.10",
                    ),
                ],
            },
        ];

        for case in test_cases {
            let parsed_query_params = parse_query_parameter(case.query_string).unwrap();
            assert_eq!(
                parsed_query_params.filters, case.expected,
                "Failed test case for query: {}",
                case.query_string
            );
        }
    }

    #[test]
    fn test_json_schema_sql_query_text_generation() {
        let field = "json_schema";
        let test_cases = vec![
            (
                pq(
                    "json_schema",
                    SearchOperator::Equals { is_negated: false },
                    "key=foo",
                ),
                format!("{field} #>> '{{key}}' = ?"),
                SQLValue::String("foo".to_string()),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::IEquals { is_negated: true },
                    "key=foo",
                ),
                format!("NOT {field} #>> '{{key}}' ILIKE ?"),
                SQLValue::String("foo".to_string()),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Gt { is_negated: false },
                    "key,subkey=3",
                ),
                format!(
                    "try_numeric({field} #>> '{{key,subkey}}') IS NOT NULL AND try_numeric({field} #>> '{{key,subkey}}') > ?"
                ),
                SQLValue::Integer(3),
            ),
        ];

        for (param, expected, sqlvalue) in test_cases {
            let result = param.as_json_sql();
            assert_eq!(
                result.unwrap(),
                SQLComponent {
                    sql: expected.to_string(),
                    bind_variables: vec![sqlvalue]
                },
                "Failed test case for param: {param:?}",
            );
        }
    }

    #[test]
    fn test_json_schema_sql_query_ip_generation() {
        let field = "json_schema";
        let test_cases = vec![
            (
                pq(
                    "json_schema",
                    SearchOperator::WithinNetwork { is_negated: false },
                    "key,subkey=10.0.0.0/24",
                ),
                format!(
                    "try_inet({field} #>> '{{key,subkey}}') IS NOT NULL AND try_inet({field} #>> '{{key,subkey}}') <<= ?::inet"
                ),
                SQLValue::String("10.0.0.0/24".to_string()),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::ContainsIp { is_negated: false },
                    "key=10.0.0.10",
                ),
                format!(
                    "try_inet({field} #>> '{{key}}') IS NOT NULL AND try_inet({field} #>> '{{key}}') >> ?::inet"
                ),
                SQLValue::String("10.0.0.10".to_string()),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::InetEquals { is_negated: true },
                    "key=10.0.0.10",
                ),
                format!(
                    "try_inet({field} #>> '{{key}}') IS NOT NULL AND NOT (try_inet({field} #>> '{{key}}') = ?::inet)"
                ),
                SQLValue::String("10.0.0.10/32".to_string()),
            ),
        ];

        for (param, expected, sqlvalue) in test_cases {
            let result = param.as_json_sql();
            assert_eq!(
                result.unwrap(),
                SQLComponent {
                    sql: expected.to_string(),
                    bind_variables: vec![sqlvalue]
                },
                "Failed test case for param: {param:?}",
            );
        }
    }

    #[test]
    fn test_json_schema_sql_query_ip_validation() {
        let test_cases = vec![
            pq(
                "json_schema",
                SearchOperator::WithinNetwork { is_negated: false },
                "key=not-an-ip",
            ),
            pq(
                "json_schema",
                SearchOperator::ContainsIp { is_negated: false },
                "key=10.0.0.0/24",
            ),
        ];

        for param in test_cases {
            let result = param.as_json_sql();
            assert!(
                result.is_err(),
                "Expected bad request for param: {param:?}, got {result:?}",
            );
        }
    }

    #[test]
    fn test_json_schema_sql_query_date_generation() {
        let field = "json_schema";
        let test_cases = vec![
            (
                pq(
                    "json_schema",
                    SearchOperator::Equals { is_negated: false },
                    "key=2021-01-01",
                ),
                format!(
                    "try_timestamp({field} #>> '{{key}}') IS NOT NULL AND try_timestamp({field} #>> '{{key}}') = ?"
                ),
                SQLValue::Date("2021-01-01".as_date().unwrap()[0]),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Gt { is_negated: false },
                    "key,subkey=2021-01-01",
                ),
                format!(
                    "try_timestamp({field} #>> '{{key,subkey}}') IS NOT NULL AND try_timestamp({field} #>> '{{key,subkey}}') > ?"
                ),
                SQLValue::Date("2021-01-01".as_date().unwrap()[0]),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Gt { is_negated: true },
                    "key,subkey=2021-01-01",
                ),
                format!(
                    "try_timestamp({field} #>> '{{key,subkey}}') IS NOT NULL AND NOT (try_timestamp({field} #>> '{{key,subkey}}') > ?)"
                ),
                SQLValue::Date("2021-01-01".as_date().unwrap()[0]),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Between { is_negated: false },
                    "key=2021-01-01,2021-01-31",
                ),
                format!(
                    "try_timestamp({field} #>> '{{key}}') IS NOT NULL AND try_timestamp({field} #>> '{{key}}') BETWEEN ? AND ?"
                ),
                SQLValue::Date("2021-01-01,2021-01-31".as_date().unwrap()[0]),
            ),
        ];

        for (index, (param, expected, sqlvalue)) in test_cases.into_iter().enumerate() {
            let result = param.as_json_sql();
            let expected_bindings = if index == 3 {
                "2021-01-01,2021-01-31"
                    .as_date()
                    .unwrap()
                    .into_iter()
                    .map(SQLValue::Date)
                    .collect::<Vec<_>>()
            } else {
                vec![sqlvalue]
            };
            assert_eq!(
                result.unwrap(),
                SQLComponent {
                    sql: expected.to_string(),
                    bind_variables: expected_bindings
                },
                "Failed test case for param: {param:?}",
            );
        }
    }

    #[test]
    fn test_json_schema_sql_query_numerical_generation() {
        let field = "json_schema";
        let test_cases = vec![
            (
                pq(
                    "json_schema",
                    SearchOperator::Equals { is_negated: false },
                    "key=3",
                ),
                format!(
                    "try_numeric({field} #>> '{{key}}') IS NOT NULL AND try_numeric({field} #>> '{{key}}') = ?"
                ),
                SQLValue::Integer(3),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Gt { is_negated: false },
                    "key,subkey=3",
                ),
                format!(
                    "try_numeric({field} #>> '{{key,subkey}}') IS NOT NULL AND try_numeric({field} #>> '{{key,subkey}}') > ?"
                ),
                SQLValue::Integer(3),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Gt { is_negated: true },
                    "key,subkey=3",
                ),
                format!(
                    "try_numeric({field} #>> '{{key,subkey}}') IS NOT NULL AND NOT (try_numeric({field} #>> '{{key,subkey}}') > ?)"
                ),
                SQLValue::Integer(3),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Between { is_negated: false },
                    "key=3,5",
                ),
                format!(
                    "try_numeric({field} #>> '{{key}}') IS NOT NULL AND try_numeric({field} #>> '{{key}}') BETWEEN ? AND ?"
                ),
                SQLValue::Integer(3),
            ),
        ];

        for (index, (param, expected, sqlvalue)) in test_cases.into_iter().enumerate() {
            let result = param.as_json_sql();
            let expected_bindings = if index == 3 {
                vec![SQLValue::Integer(3), SQLValue::Integer(5)]
            } else {
                vec![sqlvalue]
            };
            assert_eq!(
                result.unwrap(),
                SQLComponent {
                    sql: expected.to_string(),
                    bind_variables: expected_bindings
                },
                "Failed test case for param: {param:?}",
            );
        }
    }

    #[test]
    fn test_json_schema_sql_query_boolean_generation() {
        let field = "json_schema";
        let test_cases = vec![
            (
                pq(
                    "json_schema",
                    SearchOperator::Equals { is_negated: false },
                    "key=true",
                ),
                format!(
                    "try_boolean({field} #>> '{{key}}') IS NOT NULL AND try_boolean({field} #>> '{{key}}') = ?"
                ),
                SQLValue::Boolean(true),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Equals { is_negated: true },
                    "key=false",
                ),
                format!(
                    "try_boolean({field} #>> '{{key}}') IS NOT NULL AND NOT (try_boolean({field} #>> '{{key}}') = ?)"
                ),
                SQLValue::Boolean(false),
            ),
        ];

        for (param, expected, sqlvalue) in test_cases {
            let result = param.as_json_sql();
            assert_eq!(
                result.unwrap(),
                SQLComponent {
                    sql: expected.to_string(),
                    bind_variables: vec![sqlvalue]
                },
                "Failed test case for param: {param:?}",
            );
        }
    }

    #[test]
    fn test_json_schema_sql_generation_wrapping() {
        let field = "json_schema";
        let test_cases = vec![
            (
                pq(
                    "json_schema",
                    SearchOperator::Equals { is_negated: false },
                    "key,subkey=3",
                ),
                format!(
                    "try_numeric({field} #>> '{{key,subkey}}') IS NOT NULL AND try_numeric({field} #>> '{{key,subkey}}') = ?"
                ),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Equals { is_negated: false },
                    "key,subkey,subsubkey=3",
                ),
                format!(
                    "try_numeric({field} #>> '{{key,subkey,subsubkey}}') IS NOT NULL AND try_numeric({field} #>> '{{key,subkey,subsubkey}}') = ?"
                ),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Equals { is_negated: true },
                    "key,subkey,subsubkey,subsubsubkey=3",
                ),
                format!(
                    "try_numeric({field} #>> '{{key,subkey,subsubkey,subsubsubkey}}') IS NOT NULL AND NOT (try_numeric({field} #>> '{{key,subkey,subsubkey,subsubsubkey}}') = ?)"
                ),
            ),
        ];

        for (param, expected) in test_cases {
            let result = param.as_json_sql();
            assert_eq!(
                result.unwrap().sql,
                expected.to_string(),
                "Failed test case for param: {param:?}",
            );
        }
    }

    #[test]
    fn test_json_field_type_from_schema() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "name": {
                    "type": "string"
                },
                "age": {
                    "type": "number"
                },
                "is_active": {
                    "type": "boolean"
                },
                "date_of_birth": {
                    "type": "string",
                    "format": "date"
                },
                "last_updated": {
                    "type": "string",
                    "format": "date-time"
                },
                "address": {
                    "type": "object",
                    "properties": {
                        "street": {
                            "type": "string"
                        },
                        "city": {
                            "type": "string"
                        },
                        "zip": {
                            "type": "number"
                        }
                    }
                }
            }
        });

        let test_cases = vec![
            ("name", SQLMappedType::String),
            ("age", SQLMappedType::Numeric),
            ("is_active", SQLMappedType::Boolean),
            ("date_of_birth", SQLMappedType::Date),
            ("last_updated", SQLMappedType::Date),
            ("address,street", SQLMappedType::String),
            ("address,city", SQLMappedType::String),
            ("address,zip", SQLMappedType::Numeric),
        ];

        for (key, expected) in test_cases {
            let result = get_jsonb_field_type_from_json_schema(&schema, key);
            assert_eq!(result, Some(expected), "Failed test case for key: {key}");
        }
    }

    #[test]
    fn test_json_field_type_from_schema_failures() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "name": {
                    "type": "string"
                },
                "age": {
                    "type": "number"
                },
                "is_active": {
                    "type": "boolean"
                },
                "address": {
                    "type": "object",
                    "properties": {
                        "street": {
                            "type": "string"
                        },
                        "city": {
                            "type": "string"
                        },
                        "zip": {
                            "type": "number"
                        }
                    }
                }
            }
        });

        let test_cases = vec!["invalid", "address,invalid", "address,zip,invalid"];

        for key in test_cases {
            let result = get_jsonb_field_type_from_json_schema(&schema, key);
            assert_eq!(result, None, "Failed test case for key: {key}");
        }
    }

    #[test]
    fn test_get_sql_mapped_type_from_value() {
        let test_cases = vec![
            ("foo", SQLMappedType::String),
            ("3", SQLMappedType::Numeric),
            ("3.14", SQLMappedType::Numeric),
            ("2021-01-01", SQLMappedType::Date),
            ("2021-01-01T00:00:00Z", SQLMappedType::Date),
            ("true", SQLMappedType::Boolean),
            ("false", SQLMappedType::Boolean),
            ("null", SQLMappedType::None),
        ];

        for (value, expected) in test_cases {
            let result = get_sql_mapped_type_from_value(
                value,
                &[
                    SQLMappedType::Date,
                    SQLMappedType::Numeric,
                    SQLMappedType::Boolean,
                    SQLMappedType::None,
                    SQLMappedType::String,
                ],
            );
            assert_eq!(
                result,
                Some(expected),
                "Failed test case for value: '{value}'"
            );
        }
    }

    #[test]
    fn test_get_sql_mapped_type_from_value_and_operator() {
        let test_cases = vec![
            ("foo", Operator::Equals, Some(SQLMappedType::String)),
            ("3", Operator::Equals, Some(SQLMappedType::Numeric)),
            ("2021-01-01", Operator::Equals, Some(SQLMappedType::Date)),
            ("true", Operator::Equals, Some(SQLMappedType::Boolean)),
            ("FALSe", Operator::Equals, Some(SQLMappedType::Boolean)),
            ("null", Operator::Equals, Some(SQLMappedType::None)),
            ("true", Operator::Equals, Some(SQLMappedType::Boolean)),
            ("2021-01-01", Operator::Gt, Some(SQLMappedType::Date)),
            ("3", Operator::Gt, Some(SQLMappedType::Numeric)),
            (
                "2021-01-01,2021-01-31",
                Operator::Between,
                Some(SQLMappedType::Date),
            ),
            ("3,5", Operator::Between, Some(SQLMappedType::Numeric)),
            ("3", Operator::Contains, Some(SQLMappedType::String)),
            ("null", Operator::Gt, None),
            ("foo", Operator::Gt, None),
        ];

        for (value, operator, expected) in test_cases {
            let result = get_jsonb_field_type_from_value_and_operator(value, operator.clone());
            assert_eq!(
                result, expected,
                "Failed test case for value: '{value}', operator: '{operator:?}'"
            );
        }
    }

    #[test]
    fn test_new_from_string() {
        type SO = SearchOperator;

        let test_cases = vec![
            ("equals", SO::Equals { is_negated: false }),
            ("iequals", SO::IEquals { is_negated: false }),
            ("contains", SO::Contains { is_negated: false }),
            ("icontains", SO::IContains { is_negated: false }),
            ("startswith", SO::StartsWith { is_negated: false }),
            ("istartswith", SO::IStartsWith { is_negated: false }),
            ("endswith", SO::EndsWith { is_negated: false }),
            ("iendswith", SO::IEndsWith { is_negated: false }),
            ("like", SO::Like { is_negated: false }),
            ("regex", SO::Regex { is_negated: false }),
            ("gt", SO::Gt { is_negated: false }),
            ("gte", SO::Gte { is_negated: false }),
            ("lt", SO::Lt { is_negated: false }),
            ("lte", SO::Lte { is_negated: false }),
            ("between", SO::Between { is_negated: false }),
            ("within_network", SO::WithinNetwork { is_negated: false }),
            (
                "contains_network",
                SO::ContainsNetwork { is_negated: false },
            ),
            ("contains_ip", SO::ContainsIp { is_negated: false }),
            (
                "overlaps_network",
                SO::OverlapsNetwork { is_negated: false },
            ),
            ("inet_equals", SO::InetEquals { is_negated: false }),
            ("not_equals", SO::Equals { is_negated: true }),
            ("not_iequals", SO::IEquals { is_negated: true }),
            ("not_contains", SO::Contains { is_negated: true }),
            ("not_icontains", SO::IContains { is_negated: true }),
            ("not_startswith", SO::StartsWith { is_negated: true }),
            ("not_istartswith", SO::IStartsWith { is_negated: true }),
            ("not_endswith", SO::EndsWith { is_negated: true }),
            ("not_iendswith", SO::IEndsWith { is_negated: true }),
            ("not_like", SO::Like { is_negated: true }),
            ("not_regex", SO::Regex { is_negated: true }),
            ("not_gt", SO::Gt { is_negated: true }),
            ("not_gte", SO::Gte { is_negated: true }),
            ("not_lt", SO::Lt { is_negated: true }),
            ("not_lte", SO::Lte { is_negated: true }),
            ("not_within_network", SO::WithinNetwork { is_negated: true }),
            (
                "not_contains_network",
                SO::ContainsNetwork { is_negated: true },
            ),
            ("not_contains_ip", SO::ContainsIp { is_negated: true }),
            (
                "not_overlaps_network",
                SO::OverlapsNetwork { is_negated: true },
            ),
            ("not_inet_equals", SO::InetEquals { is_negated: true }),
        ];

        for (input, expected) in test_cases {
            let result = SO::new_from_string(input);
            assert_eq!(
                result,
                Ok(expected),
                "Failed test case for input: '{input}'",
            );
        }
    }

    #[test]
    fn test_is_applicable_to() {
        type SO = SearchOperator;
        type DT = DataType;

        let test_cases = vec![
            (SO::Equals { is_negated: false }, DT::String, true),
            (SO::Equals { is_negated: false }, DT::NumericOrDate, true),
            (SO::Equals { is_negated: false }, DT::Boolean, true),
            (SO::IEquals { is_negated: false }, DT::String, true),
            (SO::IEquals { is_negated: false }, DT::NumericOrDate, false),
            (SO::IEquals { is_negated: false }, DT::Boolean, false),
            (SO::Contains { is_negated: false }, DT::String, true),
            (SO::Contains { is_negated: false }, DT::NumericOrDate, false),
            (SO::Contains { is_negated: false }, DT::Boolean, false),
            (SO::IContains { is_negated: false }, DT::String, true),
            (
                SO::IContains { is_negated: false },
                DT::NumericOrDate,
                false,
            ),
            (SO::IContains { is_negated: false }, DT::Boolean, false),
            (SO::StartsWith { is_negated: false }, DT::String, true),
            (
                SO::StartsWith { is_negated: false },
                DT::NumericOrDate,
                false,
            ),
            (SO::StartsWith { is_negated: false }, DT::Boolean, false),
            (SO::IStartsWith { is_negated: false }, DT::String, true),
            (
                SO::IStartsWith { is_negated: false },
                DT::NumericOrDate,
                false,
            ),
            (SO::IStartsWith { is_negated: false }, DT::Boolean, false),
            (SO::EndsWith { is_negated: false }, DT::String, true),
            (SO::EndsWith { is_negated: false }, DT::NumericOrDate, false),
            (SO::EndsWith { is_negated: false }, DT::Boolean, false),
            (SO::IEndsWith { is_negated: false }, DT::String, true),
            (
                SO::IEndsWith { is_negated: false },
                DT::NumericOrDate,
                false,
            ),
            (SO::IEndsWith { is_negated: false }, DT::Boolean, false),
            (SO::Like { is_negated: false }, DT::String, true),
            (SO::Like { is_negated: false }, DT::NumericOrDate, false),
            (SO::Like { is_negated: false }, DT::Boolean, false),
            (SO::Regex { is_negated: false }, DT::String, true),
            (SO::Regex { is_negated: false }, DT::NumericOrDate, false),
            (SO::Regex { is_negated: false }, DT::Boolean, false),
            (SO::Gt { is_negated: false }, DT::String, false),
            (SO::Gt { is_negated: false }, DT::NumericOrDate, true),
            (SO::Gt { is_negated: false }, DT::Boolean, false),
            (SO::Gte { is_negated: false }, DT::String, false),
            (SO::Gte { is_negated: false }, DT::NumericOrDate, true),
            (SO::Gte { is_negated: false }, DT::Boolean, false),
            (SO::Lt { is_negated: false }, DT::String, false),
            (SO::Lt { is_negated: false }, DT::NumericOrDate, true),
            (SO::Lt { is_negated: false }, DT::Boolean, false),
            (SO::Lte { is_negated: false }, DT::String, false),
            (SO::Lte { is_negated: false }, DT::NumericOrDate, true),
            (SO::Lte { is_negated: false }, DT::Boolean, false),
            (SO::Between { is_negated: false }, DT::String, false),
            (SO::Between { is_negated: false }, DT::NumericOrDate, true),
            (SO::Between { is_negated: false }, DT::Boolean, false),
        ];

        for (operator, data_type, expected) in test_cases {
            let result = operator.is_applicable_to(data_type);
            assert_eq!(
                result, expected,
                "Failed test case for operator: '{operator:?}', data_type: '{data_type:?}'",
            );
        }
    }

    #[test]
    fn test_parse_query_parameter_with_cursor() {
        let query_options =
            parse_query_parameter("limit=2&sort=id.desc&cursor=test-cursor").unwrap();

        assert_eq!(query_options.limit, Some(2));
        assert_eq!(query_options.sort.len(), 1);
        assert_eq!(query_options.sort[0].field, FilterField::Id);
        assert!(query_options.sort[0].descending);
        assert_eq!(query_options.cursor, Some("test-cursor".to_string()));
    }

    #[test]
    fn parse_query_parameter_supports_total_count_opt_out() {
        let options = parse_query_parameter("include_total=false").unwrap();
        assert!(!options.include_total);

        let defaults = parse_query_parameter("").unwrap();
        assert!(defaults.include_total);

        let duplicate =
            parse_query_parameter("include_total=true&include_total=false").unwrap_err();
        assert_eq!(duplicate.to_string(), "duplicate include_total");
    }

    #[test]
    fn test_parse_query_parameter_with_passthrough_extracts_endpoint_local_values() {
        let (query_options, passthrough) = parse_query_parameter_with_passthrough(
            "name__contains=alpha&ignore_classes=1,2&ignore_self_class=false&sort=id.asc",
            &["ignore_classes", "ignore_self_class"],
        )
        .unwrap();

        assert_eq!(query_options.filters.len(), 1);
        assert_eq!(query_options.filters[0].field, FilterField::Name);
        assert_eq!(
            query_options.filters[0].operator,
            SearchOperator::Contains { is_negated: false }
        );
        assert_eq!(query_options.filters[0].value, "alpha");
        assert_eq!(query_options.sort.len(), 1);
        assert_eq!(
            passthrough.get("ignore_classes"),
            Some(&vec!["1,2".to_string()])
        );
        assert_eq!(
            passthrough.get("ignore_self_class"),
            Some(&vec!["false".to_string()])
        );
    }

    #[test]
    fn test_parse_query_parameter_with_passthrough_preserves_repeated_local_keys() {
        let (_, passthrough) = parse_query_parameter_with_passthrough(
            "ignore_self_class=true&ignore_self_class=false",
            &["ignore_self_class"],
        )
        .unwrap();

        assert_eq!(
            passthrough.get("ignore_self_class"),
            Some(&vec!["true".to_string(), "false".to_string()])
        );
    }

    #[test]
    fn test_parse_query_parameter_rejects_duplicate_cursor() {
        let error = parse_query_parameter("cursor=one&cursor=two").unwrap_err();
        assert!(matches!(error, ApiError::BadRequest(_)));
        assert_eq!(error.to_string(), "duplicate cursor");
    }

    // Covers docs/querying.md "Sorting" (`order_by` is accepted as an alias).
    #[test]
    fn docs_parse_query_parameter_accepts_order_by_alias() {
        let query_options = parse_query_parameter("order_by=name.desc,id.asc").unwrap();

        assert_eq!(query_options.sort.len(), 2);
        assert_eq!(query_options.sort[0].field, FilterField::Name);
        assert!(query_options.sort[0].descending);
        assert_eq!(query_options.sort[1].field, FilterField::Id);
        assert!(!query_options.sort[1].descending);
    }

    // Covers docs/querying.md "Query syntax" (`field=value` means `field__equals=value`).
    #[test]
    fn docs_parse_query_parameter_plain_filter_defaults_to_equals() {
        let query_options = parse_query_parameter("name=alpha").unwrap();

        assert_eq!(query_options.filters.len(), 1);
        assert_eq!(query_options.filters[0].field, FilterField::Name);
        assert_eq!(
            query_options.filters[0].operator,
            SearchOperator::Equals { is_negated: false }
        );
        assert_eq!(query_options.filters[0].value, "alpha");
    }

    #[test]
    fn test_parse_query_parameter_decodes_keys_values_and_plus() {
        let query_options = parse_query_parameter("name%5F%5Fcontains=alpha+beta").unwrap();

        assert_eq!(query_options.filters.len(), 1);
        assert_eq!(query_options.filters[0].field, FilterField::Name);
        assert_eq!(
            query_options.filters[0].operator,
            SearchOperator::Contains { is_negated: false }
        );
        assert_eq!(query_options.filters[0].value, "alpha beta");
    }

    // Covers docs/querying.md "Negation" (`not_` works with `between`).
    #[test]
    fn docs_parse_query_parameter_accepts_negated_between_filter() {
        let query_options = parse_query_parameter(
            "created_at__not_between=2026-01-01T00:00:00Z,2026-02-01T00:00:00Z",
        )
        .unwrap();

        assert_eq!(query_options.filters.len(), 1);
        assert_eq!(query_options.filters[0].field, FilterField::CreatedAt);
        assert_eq!(
            query_options.filters[0].operator,
            SearchOperator::Between { is_negated: true }
        );
        assert_eq!(
            query_options.filters[0].value,
            "2026-01-01T00:00:00Z,2026-02-01T00:00:00Z"
        );
    }

    // Covers docs/querying.md "JSON filtering" (`json_data` aliases target object JSON payload data).
    #[test]
    fn docs_json_data_aliases_map_to_object_data_column() {
        assert_eq!(FilterField::JsonData.table_field(), "data");
        assert_eq!(FilterField::JsonDataFrom.table_field(), "data");
        assert_eq!(FilterField::JsonDataTo.table_field(), "data");
    }

    #[test]
    fn test_parse_query_parameter_rejects_zero_limit() {
        let error = parse_query_parameter("limit=0").unwrap_err();
        assert!(matches!(error, ApiError::BadRequest(_)));
        assert_eq!(error.to_string(), "limit must be greater than 0");
    }

    // Covers docs/querying.md "Cursor pagination" (maximum page size is `250`).
    #[test]
    fn docs_parse_query_parameter_rejects_limit_above_maximum() {
        let error = parse_query_parameter("limit=251").unwrap_err();
        assert!(matches!(error, ApiError::BadRequest(_)));
        assert_eq!(error.to_string(), "limit must be at most 250");
    }
}
