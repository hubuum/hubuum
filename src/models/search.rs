#![allow(dead_code)]
use actix_web::web::Json;
use diesel::sql_types::Bool;
use std::{collections::HashSet, f32::consts::E};
use tracing::field;

use crate::models::permissions::Permissions;
use crate::{errors::ApiError, schema::hubuumobject::data};
use chrono::{format, DateTime, NaiveDateTime, Utc};

/// ## Parse a query string into search parameters
///
/// ## Arguments
///
/// * `query_string` - A string that contains the query parameters
///
/// ## Returns
///
/// * A vector of parsed query parameters or ApiError::BadRequest if the query string is invalid
pub fn parse_query_parameter(query_string: &str) -> Result<Vec<ParsedQueryParam>, ApiError> {
    let mut parsed_query_params = Vec::new();

    for query_param in query_string.split('&') {
        let query_param_parts: Vec<&str> = query_param.splitn(2, '=').collect();

        if query_param_parts.len() != 2 {
            return Err(ApiError::BadRequest(format!(
                "Invalid query parameter: '{}'",
                query_param
            )));
        }

        let field_and_op: Vec<&str> = query_param_parts[0].splitn(2, "__").collect();
        let value = query_param_parts[1].to_string();
        let field = field_and_op[0].to_string();

        if value.is_empty() {
            return Err(ApiError::BadRequest(format!(
                "Invalid query parameter: '{}', no value",
                query_param
            )));
        }

        let operator = if field_and_op.len() == 1 {
            "equals"
        } else {
            field_and_op[1]
        };

        let operator = SearchOperator::new_from_string(operator)?;

        let parsed_query_param = ParsedQueryParam {
            field,
            operator,
            value,
        };

        parsed_query_params.push(parsed_query_param);
    }

    Ok(parsed_query_params)
}

#[derive(Debug, PartialEq, Clone)]
pub struct SQLComponent {
    pub sql: String,
    pub value: String,
    pub mapped_type: SQLMappedType,
}

/// ## A struct that represents a parsed query parameter
///  
/// This struct holds a field, operator, and values for a search.
///
/// The field is the name of the field to search on, the operator is the type of search to perform,
/// and the value is the value to search for.
///
/// The reason the data in this struct is stored as strings is because it is parsed from a query
/// string, which is always a string. Parsing the data into the correct types is done in the
/// functions that use this struct as they have some context about the data based on the type of
/// the field involved. This may or may not involve parsing the data into a different type, such as
/// parsing the value into an integer, a date, or a permission.
#[derive(Debug, PartialEq, Clone)]
pub struct ParsedQueryParam {
    pub field: String,
    pub operator: SearchOperator,
    pub value: String,
}

impl ParsedQueryParam {
    /// ## Create a new ParsedQueryParam
    ///
    /// Note:
    ///   * If no operator is provided, the default is "equals".
    ///   * For permissions the operator is always "equals" and the value is "true".
    ///
    /// ### Arguments
    ///
    /// * `field` - The name of the field to search on
    /// * `operator` - The type of search to perform
    /// * `value` - The value to search for
    ///
    /// ### Returns
    ///
    /// * A new ParsedQueryParam instance
    pub fn new(field: &str, operator: Option<SearchOperator>, value: &str) -> Self {
        let operator = operator.unwrap_or(SearchOperator::Equals { is_negated: false });

        ParsedQueryParam {
            field: field.to_string(),
            operator,
            value: value.to_string(),
        }
    }

    pub fn is_permission(&self) -> bool {
        self.field == "permission"
    }

    pub fn is_namespace(&self) -> bool {
        self.field == "namespace"
    }

    pub fn is_json_schema(&self) -> bool {
        self.field == "json_schema"
    }

    pub fn is_json_data(&self) -> bool {
        self.field == "json_data"
    }

    pub fn is_json(&self) -> bool {
        self.is_json_schema() || self.is_json_data()
    }

    /// ## Coerce the value into a Permissions enum
    ///
    /// ### Returns
    ///
    /// * A Permissions enum or ApiError::BadRequest if the value is invalid
    pub fn value_as_permission(&self) -> Result<Permissions, ApiError> {
        Permissions::from_string(&self.value)
    }

    /// ## Coerce the value into a list of integers
    ///
    /// Accepts the format given to the [`parse_integer_list`] function.
    ///
    /// ### Returns
    ///
    /// * A vector of integers or ApiError::BadRequest if the value is invalid
    pub fn value_as_integer(&self) -> Result<Vec<i32>, ApiError> {
        parse_integer_list(&self.value)
    }

    /// ## Coerce the value into a list of dates
    ///
    /// Accepts a comma separated list of RFC3339 dates.
    /// https://www.rfc-editor.org/rfc/rfc3339
    ///     
    /// ### Returns
    ///
    /// * A vector of NaiveDateTime or ApiError::BadRequest if the value is invalid
    pub fn value_as_date(&self) -> Result<Vec<NaiveDateTime>, ApiError> {
        self.value
            .split(',')
            .map(|part| part.trim())
            .map(|part| {
                DateTime::parse_from_rfc3339(part)
                    .map(|dt| dt.with_timezone(&Utc)) // Convert to Utc
                    .map(|utc_dt| utc_dt.naive_utc()) // Convert to NaiveDateTime
                    .map_err(|e| e.into()) // Convert chrono::ParseError (or any error) into ApiError
            })
            .collect() // Collect into a Result<Vec<NaiveDateTime>, ApiError>
    }

    /// ## Coerce the value into a boolean
    ///
    /// Accepted values are "true" and "false" (case insensitive)
    ///
    /// ### Returns
    ///
    /// * A boolean or ApiError::BadRequest if the value is invalid
    pub fn value_as_boolean(&self) -> Result<bool, ApiError> {
        match self.value.to_lowercase().as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(ApiError::BadRequest(format!(
                "Invalid boolean value: '{}'",
                self.value
            ))),
        }
    }

    pub fn as_json_sql(&self) -> Result<String, ApiError> {
        use diesel::dsl::sql;
        use diesel::expression::AsExpression;
        use diesel::prelude::*;
        use serde_json::from_str;

        if !self.is_json() {
            return Err(ApiError::InternalServerError(format!(
                "Attempt to filter '{}' as JSON!",
                self.field
            )));
        }

        // TODO: Avoid SQL injections by validating the key and value
        // TODO: Validate the key
        // TODO: Validate the value
        // TODO: Since we have a schema, we have typing info, so we can also validate
        //       the value and the operator against the defined type in the schema

        let field = self.field.clone();

        // split the value on key=value
        let parts: Vec<&str> = self.value.splitn(2, '=').collect();

        let (key, value) = match parts.as_slice() {
            [key, value] => (key, value),
            _ => {
                return Err(ApiError::BadRequest(
                    "Expected exactly two parts of key=value".to_string(),
                ))
            }
        };

        let key = format!("'{{{}}}'", key);

        let (op, neg) = self.operator.op_and_neg();
        let neg_str = if neg { "NOT " } else { "" };

        let sql_type = get_jsonb_field_type_from_value_and_operator(value, op.clone());

        // TODO: Add JSON Schema usage type support via
        // get_jsonb_field_type_from_json_schema(schema, key)
        let search_key = match sql_type {
            None => {
                return Err(ApiError::BadRequest(format!(
                    "Invalid JSON type mapping between key '{}' and operator '{:?}'",
                    key, self.operator
                )))
            }
            Some(SQLMappedType::String) | Some(SQLMappedType::None) => {
                format!("{}#>>{}", field, key)
            }
            Some(SQLMappedType::Numeric) => format!("({}#>>{}::text)::numeric", field, key),
            Some(SQLMappedType::Date) => format!("({}#>>{}::text)::date", field, key),
            Some(SQLMappedType::Boolean) => format!("({}#>>{}::text)::boolean", field, key),
        };

        let (sql_op, value) = match op {
            Operator::Equals => ("=", (*value).to_string()),
            Operator::IEquals => ("ILIKE", (*value).to_string()),
            Operator::Contains | Operator::Like => ("LIKE", format!("%{}%", value)),
            Operator::IContains => ("ILIKE", format!("%{}%", value)),
            Operator::StartsWith => ("LIKE", format!("{}%", value)),
            Operator::IStartsWith => ("ILIKE", format!("{}%", value)),
            Operator::EndsWith => ("LIKE", format!("%{}", value)),
            Operator::IEndsWith => ("ILIKE", format!("%{}", value)),
            Operator::Regex => ("~", (*value).to_string()),
            Operator::Gt => (">", (*value).to_string()),
            Operator::Gte => (">=", (*value).to_string()),
            Operator::Lt => ("<", (*value).to_string()),
            Operator::Lte => ("<=", (*value).to_string()),

            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Invalid operator for JSON: '{:?}'",
                    op
                )))
            }
        };

        if sql_type.is_some() {
            let sql_type = sql_type.unwrap();
            if sql_type == SQLMappedType::String || sql_type == SQLMappedType::Date {
                return Ok(format!("{}{} {} '{}'", neg_str, search_key, sql_op, value));
            }
        }

        Ok(format!("{}{} {} {}", neg_str, search_key, sql_op, value))

        //        Ok(SQLComponent {
        //            sql: format!("{}{} {} ?", neg_str, search_key, sql_op),
        //            value,
        //            mapped_type: sql_type.unwrap(), // Safe to unwrap, we checked for None above
        //        })
    }
}

pub trait QueryParamsExt {
    /// ## Get a list of permissions from a list of parsed query parameters
    ///
    /// Iterate over the parsed query parameters and filter out the ones that are permissions,
    /// defined as having the `field` set as "permission". For each value of each parsed query
    /// parameter, attempt to parse it into a Permissions enum. If the value is not a valid
    /// permission, return an ApiError::BadRequest.
    ///
    /// Note that duplicates may occur, it is up to the caller to handle this if necessary.
    ///
    /// ### Returns    
    ///
    /// * A vector of Permissions or ApiError::BadRequest if the permissions are invalid
    fn permissions(&self) -> Result<Vec<Permissions>, ApiError>;

    /// ## Get a sorted list of namespace ids from a list of parsed query parameters
    ///
    /// Iterate over the parsed query parameters and filter out the ones that are namespaces,
    /// defined as having the `field` set as "namespace". For each value of each parsed query
    /// parameter, attempt to parse it into a list integers via [`parse_integer_list`].
    ///
    /// If the value is not a valid list of integers, return an ApiError::BadRequest.
    ///
    /// Note that the result is sorted and that duplicates are removed.
    ///
    /// ### Returns
    ///
    /// * A vector of integers or ApiError::BadRequest if any of the namespace values are invalid
    fn namespaces(&self) -> Result<Vec<i32>, ApiError>;

    /// ## Get a list of all JSON Schema elements in a list of parsed query parameters
    ///
    /// Iterate over the parsed query parameters and filter out the ones that are JSON Schemas,
    /// defined as having the `field` set as "json_schema". Also validates both keys and values
    /// and their matching to the operator.
    fn json_schemas(&self) -> Result<Vec<&ParsedQueryParam>, ApiError>;
}

impl QueryParamsExt for Vec<ParsedQueryParam> {
    /// ## Get a list of all Permissions in a list of parsed query parameters
    ///
    /// Iterate over the parsed query parameters and filter out the ones that are permissions,
    /// defined as having the `field` set as "permission". For each value of a matching parsed query
    /// parameter, attempt to parse it into a Permissions enum.
    ///
    /// Note that the list is not sorted and duplicates are removed.
    ///
    /// If any value is not a valid permission, return an ApiError::BadRequest.
    fn permissions(&self) -> Result<Vec<Permissions>, ApiError> {
        let mut unique_permissions = HashSet::new();

        for param in self.iter().filter(|p| p.is_permission()) {
            match param.value_as_permission() {
                Ok(permission) => {
                    unique_permissions.insert(permission);
                }
                Err(e) => return Err(e),
            }
        }

        Ok(unique_permissions.into_iter().collect())
    }
    /// ## Get a sorted list of namespace ids from a list of parsed query parameters
    ///
    /// Iterate over the parsed query parameters and filter out the ones that are namespaces,
    /// defined as having the `field` set as "namespace". For each value of a matching parsed query
    /// parameter, attempt to parse it into a list of integers via [`parse_integer_list`].
    ///
    /// If any value is not a valid list of integers, return an ApiError::BadRequest.
    fn namespaces(&self) -> Result<Vec<i32>, ApiError> {
        let mut nids = vec![];

        for p in self.iter() {
            if p.field == "namespace" {
                nids.extend(parse_integer_list(&p.value)?);
            }
        }

        nids.sort_unstable();
        nids.dedup();
        Ok(nids)
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
}

/// ## Parse a list of integers from a string
///
/// ### Arguments
///
/// * `input` - A string that contains a list of integers. The integers can be comma separated or
///   ranges separated by a hyphen. For example:
///     * "1,2,3,4"
///     * "1-4,6-8"
///     * "1,2,3-5,7"
///
/// ### Returns
///
/// * A sorted vector of unique integers or ApiError::InvalidIntegerRange if the input is invalid
pub fn parse_integer_list(input: &str) -> Result<Vec<i32>, ApiError> {
    let mut result = Vec::new();
    for part in input.split(',') {
        let range: Vec<&str> = part.split('-').collect();
        match range.len() {
            1 => {
                let num = range[0].parse::<i32>().map_err(|_| {
                    ApiError::InvalidIntegerRange(format!("Invalid number: '{}'", part))
                })?;
                result.push(num);
            }
            2 => {
                let start = range[0].parse::<i32>().map_err(|_| {
                    ApiError::InvalidIntegerRange(format!("Invalid start of range: '{}'", part))
                })?;
                let end = range[1].parse::<i32>().map_err(|_| {
                    ApiError::InvalidIntegerRange(format!("Invalid end of range: '{}'", part))
                })?;
                if end < start {
                    return Err(ApiError::InvalidIntegerRange(format!(
                        "Invalid integer range, start greater than end: '{}'",
                        part
                    )));
                }
                result.extend(start..=end);
            }
            _ => {
                return Err(ApiError::InvalidIntegerRange(format!(
                    "Invalid integer range, parse error: '{}'",
                    part
                )))
            }
        }
    }
    result.sort_unstable();
    result.dedup();

    Ok(result)
}
/// Operators
///
/// These are operators without metadata, just their names.
#[derive(Debug, PartialEq, Clone)]
pub enum Operator {
    Equals,
    IEquals,
    Contains,
    IContains,
    StartsWith,
    IStartsWith,
    EndsWith,
    IEndsWith,
    Like,
    Regex,
    Gt,
    Gte,
    Lt,
    Lte,
    Between,
}

/// ## An enum that represents a search operator
///
/// This enum represents the different types of search operators that can be used in a search query,
/// such as equals, greater than, less than, etc. The types they apply to are defined in is_applicable_to.
#[derive(Debug, PartialEq, Clone)]
pub enum SearchOperator {
    Equals { is_negated: bool },
    IEquals { is_negated: bool },
    Contains { is_negated: bool },
    IContains { is_negated: bool },
    StartsWith { is_negated: bool },
    IStartsWith { is_negated: bool },
    EndsWith { is_negated: bool },
    IEndsWith { is_negated: bool },
    Like { is_negated: bool },
    Regex { is_negated: bool },
    Gt { is_negated: bool },
    Gte { is_negated: bool },
    Lt { is_negated: bool },
    Lte { is_negated: bool },
    Between { is_negated: bool },
}
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum DataType {
    String,
    NumericOrDate,
    Boolean,
}

impl SearchOperator {
    /// Checks if the operator is applicable to a given data type.
    pub fn is_applicable_to(&self, data_type: DataType) -> bool {
        type SO = SearchOperator;
        match self {
            SO::Equals { .. } => true,
            SO::Gt { .. }
            | SO::Gte { .. }
            | SO::Lt { .. }
            | SO::Lte { .. }
            | SO::Between { .. } => matches!(data_type, DataType::NumericOrDate),
            _ => {
                matches!(data_type, DataType::String)
            }
        }
    }

    pub fn op_and_neg(&self) -> (Operator, bool) {
        match self {
            SearchOperator::Equals { is_negated } => (Operator::Equals, *is_negated),
            SearchOperator::IEquals { is_negated, .. } => (Operator::IEquals, *is_negated),
            SearchOperator::Contains { is_negated, .. } => (Operator::Contains, *is_negated),
            SearchOperator::IContains { is_negated, .. } => (Operator::IContains, *is_negated),
            SearchOperator::StartsWith { is_negated, .. } => (Operator::StartsWith, *is_negated),
            SearchOperator::IStartsWith { is_negated, .. } => (Operator::IStartsWith, *is_negated),
            SearchOperator::EndsWith { is_negated, .. } => (Operator::EndsWith, *is_negated),
            SearchOperator::IEndsWith { is_negated, .. } => (Operator::IEndsWith, *is_negated),
            SearchOperator::Like { is_negated, .. } => (Operator::Like, *is_negated),
            SearchOperator::Regex { is_negated, .. } => (Operator::Regex, *is_negated),
            SearchOperator::Gt { is_negated, .. } => (Operator::Gt, *is_negated),
            SearchOperator::Gte { is_negated, .. } => (Operator::Gte, *is_negated),
            SearchOperator::Lt { is_negated, .. } => (Operator::Lt, *is_negated),
            SearchOperator::Lte { is_negated, .. } => (Operator::Lte, *is_negated),
            SearchOperator::Between { is_negated, .. } => (Operator::Between, *is_negated),
        }
    }

    pub fn new_from_string(operator: &str) -> Result<SearchOperator, ApiError> {
        type SO = SearchOperator;

        let mut negated = false;

        let operator = match operator {
            operator if operator.starts_with("not_") => {
                negated = true;
                operator.trim_start_matches("not_")
            }
            operator => operator,
        };

        match operator {
            "equals" => Ok(SO::Equals {
                is_negated: negated,
            }),
            "iequals" => Ok(SO::IEquals {
                is_negated: negated,
            }),
            "contains" => Ok(SO::Contains {
                is_negated: negated,
            }),
            "icontains" => Ok(SO::IContains {
                is_negated: negated,
            }),
            "startswith" => Ok(SO::StartsWith {
                is_negated: negated,
            }),
            "istartswith" => Ok(SO::IStartsWith {
                is_negated: negated,
            }),
            "endswith" => Ok(SO::EndsWith {
                is_negated: negated,
            }),
            "iendswith" => Ok(SO::IEndsWith {
                is_negated: negated,
            }),
            "like" => Ok(SO::Like {
                is_negated: negated,
            }),
            "regex" => Ok(SO::Regex {
                is_negated: negated,
            }),
            "gt" => Ok(SO::Gt {
                is_negated: negated,
            }),
            "gte" => Ok(SO::Gte {
                is_negated: negated,
            }),
            "lt" => Ok(SO::Lt {
                is_negated: negated,
            }),
            "lte" => Ok(SO::Lte {
                is_negated: negated,
            }),
            "between" => Ok(SO::Between {
                is_negated: negated,
            }),

            _ => Err(ApiError::BadRequest(format!(
                "Invalid search operator: '{}'",
                operator
            ))),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum SQLMappedType {
    String,
    Numeric,
    Date,
    Boolean,
    None,
}

#[derive(Debug, PartialEq, Clone)]
pub struct JsonbFieldType {
    pub value: String,
    pub mapping: SQLMappedType,
    pub operator: Operator,
}

/// ## Get the type of a field within a JSON schema
///
/// This function takes a JSON schema and a key, and returns the type of the field at that key.
/// The key can be a nested key, separated by commas.
///
/// ### Arguments
///
/// * `schema` - A JSON schema (assumed to be valid)
/// * `key` - The key to get the type for, may be a nested key separated by commas
///
/// ### Returns
///
/// * Some(SQLMappedType) if the key exists and has a type, None otherwise
///
/// ### Example
///
/// Given the following JSON schema:
///
/// ```json
/// {
///   "type": "object",
///   "properties": {
///     "name": {
///       "type": "string"
///     },
///     "age": {
///       "type": "number"
///     },
///     "date_of_birth": {
///       "type": "string",
///       "format": "date-time"
///     },
///     "is_active": {
///       "type": "boolean"
///     },
///     "address": {
///       "type": "object",
///       "properties": {
///         "street": {
///            "type": "string"
///       },
///       "city": {
///         "type": "string"
///       },
///       "zip": {
///         "type": "number"
///       }
///     }
///   }
/// }
/// ```
///
/// The following keys would return the following types:
///
/// * "name" -> Some(SQLMappedType::String)
/// * "age" -> Some(SQLMappedType::Numeric)
/// * "is_active" -> Some(SQLMappedType::Boolean)
/// * "date_of_birth" -> Some(SQLMappedType::Date)
/// * "address,street" -> Some(SQLMappedType::String)
/// * "address,city" -> Some(SQLMappedType::String)
/// * "address,zip" -> Some(SQLMappedType::Numeric)
///
fn get_jsonb_field_type_from_json_schema(
    schema: &serde_json::Value,
    key: &str,
) -> Option<SQLMappedType> {
    use serde_json::Value;

    let mut current_schema = schema;

    for key in key.split(',') {
        match current_schema {
            Value::Object(ref map) => {
                if let Some(sub_schema) = map.get("properties").and_then(|p| p.get(key)) {
                    current_schema = sub_schema;
                } else if let Some(items_schema) = map.get("items") {
                    current_schema = items_schema;
                } else {
                    return None;
                }
            }
            _ => return None,
        }
    }

    // If we have a specific format, we can use that to determine the type from a set of
    // predefined formats.
    match current_schema.get("format") {
        Some(Value::String(format_str)) => match format_str.as_ref() {
            "date-time" | "date" => return Some(SQLMappedType::Date),
            _ => {}
        },
        _ => {}
    };

    // We do not have a specific format, we rely on the more generic type.
    match current_schema.get("type") {
        Some(Value::String(type_str)) => match type_str.as_ref() {
            "string" => Some(SQLMappedType::String),
            "number" | "integer" => Some(SQLMappedType::Numeric),
            "boolean" => Some(SQLMappedType::Boolean),
            _ => None,
        },
        _ => None,
    }
}

/// ## Get the type of a JSON field
///
/// This function takes a JSON field and an operator and returns a best guess of type of the field.
/// The operator is used to eliminate some types, for example, if the operator is "gt" or "lt",
/// the field is assumed to be a number or a date. If no valid type can be determined, the function
/// returns None.
///
/// ### Arguments
///
/// * `value` - The value of the field
/// * `operator` - The operator that is being applied
///
/// ### Returns
///
/// * Some(SQLMappedType) if the type can be determined, None otherwise
pub fn get_jsonb_field_type_from_value_and_operator(
    value: &str,
    operator: Operator,
) -> Option<SQLMappedType> {
    match operator {
        Operator::Equals => {
            return get_sql_mapped_type_from_value(
                value,
                &[
                    SQLMappedType::Date,
                    SQLMappedType::Boolean,
                    SQLMappedType::Numeric,
                    SQLMappedType::None,
                    SQLMappedType::String,
                ],
            );
        }
        Operator::Contains => {
            return get_sql_mapped_type_from_value(
                value,
                &[
                    SQLMappedType::Date,
                    SQLMappedType::Numeric,
                    SQLMappedType::String,
                ],
            );
        }
        Operator::Gt | Operator::Gte | Operator::Lt | Operator::Lte => {
            get_sql_mapped_type_from_value(value, &[SQLMappedType::Date, SQLMappedType::Numeric])
        }
        Operator::Between => {
            let parts = value.split("--").collect::<Vec<&str>>();
            if parts.len() != 2 {
                return None;
            }
            let lval = get_sql_mapped_type_from_value(
                parts[0],
                &[SQLMappedType::Date, SQLMappedType::Numeric],
            );
            let rval = get_sql_mapped_type_from_value(
                parts[1],
                &[SQLMappedType::Date, SQLMappedType::Numeric],
            );
            if lval.is_none() || rval.is_none() || lval != rval {
                return None;
            }
            return lval; // Already a Some() from get_sql_mapped_type_from_value
        }
        Operator::IEquals
        | Operator::IContains
        | Operator::StartsWith
        | Operator::IStartsWith
        | Operator::EndsWith
        | Operator::IEndsWith
        | Operator::Like
        | Operator::Regex => Some(SQLMappedType::String),
    }
}

/// ## Get an SQL type from a value based on a list of accepted types
///
/// This function takes a value and a list of accepted types and returns the first type that the
/// value can be parsed into. If the value cannot be parsed into any of the accepted types, the
/// function returns None.
///
/// ### Arguments
///
/// * `value` - The value to parse
/// * `accepted_types` - A list of accepted types
///
/// ### Returns
///
/// * Some(SQLMappedType) if the value can be parsed into one of the accepted types, None otherwise
fn get_sql_mapped_type_from_value(
    value: &str,
    accepted_types: &[SQLMappedType],
) -> Option<SQLMappedType> {
    use chrono::{DateTime, NaiveDate, Utc};

    for t in accepted_types {
        match t {
            SQLMappedType::String => {
                return Some(SQLMappedType::String);
            }
            SQLMappedType::Numeric => {
                if value.parse::<f64>().is_ok() {
                    return Some(SQLMappedType::Numeric);
                }
            }
            SQLMappedType::Date => {
                if DateTime::parse_from_rfc3339(value).is_ok() {
                    return Some(SQLMappedType::Date);
                }

                let format = "%Y-%m-%d";
                if NaiveDate::parse_from_str(value, format).is_ok() {
                    return Some(SQLMappedType::Date);
                }
            }
            SQLMappedType::Boolean => {
                if value.to_lowercase() == "true" || value.to_lowercase() == "false" {
                    return Some(SQLMappedType::Boolean);
                }
            }
            SQLMappedType::None => {
                if value.is_empty() || value.to_lowercase() == "null" {
                    return Some(SQLMappedType::None);
                }
            }
        }
    }

    None
}

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
            field: field.to_string(),
            operator,
            value: value.to_string(),
        }
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
            let result = parse_integer_list(input);
            assert_eq!(
                result,
                Ok(expected),
                "Failed test case for input: {}",
                input
            );
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
            let result = parse_integer_list(input);
            assert_eq!(
                result,
                Ok(expected),
                "Failed test case for input: {}",
                input
            );
        }
    }

    #[test]
    fn test_parse_integer_list_mixed() {
        let test_cases = vec![
            ("1,2,3,4", vec![1, 2, 3, 4]),
            ("1-4,6-8", vec![1, 2, 3, 4, 6, 7, 8]),
            ("1,2,3-5,7", vec![1, 2, 3, 4, 5, 7]),
            ("1-4,3,3,8", vec![1, 2, 3, 4, 8]),
        ];

        for (input, expected) in test_cases {
            let result = parse_integer_list(input);
            assert_eq!(
                result,
                Ok(expected),
                "Failed test case for input: {}",
                input
            );
        }
    }

    #[test]
    fn test_parse_integer_list_failures() {
        let test_cases = vec!["1-", "-1", "1-2-3"];

        for input in test_cases {
            let result = parse_integer_list(input);
            assert!(
                result.is_err(),
                "Failed test case for input: {} (no error) {:?}",
                input,
                result
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

        let test_case_errors = vec![
            "Invalid query parameter: 'invalid'",
            "Invalid query parameter: 'invalid=', no value",
            "Invalid search operator: 'invalid'",
        ];

        let mut i = 0;
        for case in test_cases {
            let result = parse_query_parameter(case);
            assert!(
                result.is_err(),
                "Failed test case for query: {} (no error) {:?}",
                case,
                result
            );
            let result_err = result.unwrap_err();
            assert_eq!(
                result_err,
                ApiError::BadRequest(test_case_errors[i].to_string()),
                "Failed test case for query: {} ({} vs {})",
                case,
                result_err,
                test_case_errors[i]
            );
            i += 1;
        }
    }

    #[test]
    fn test_query_string_parsing() {
        let test_cases = vec![
            TestCase {
                query_string: "name__icontains=foo&description=bar",
                expected: vec![
                    pq("name", SearchOperator::IContains{ is_negated: false }, "foo"),
                    pq("description", SearchOperator::Equals{ is_negated: false}, "bar"),
                ],
            },
            TestCase {
                query_string: "name__contains=foo&description__icontains=bar&created_at__gte=2021-01-01&updated_at__lte=2021-12-31",
                expected: vec![
                    pq("name", SearchOperator::Contains{ is_negated: false}, "foo"),
                    pq("description", SearchOperator::IContains{ is_negated: false}, "bar"),
                    pq("created_at", SearchOperator::Gte{ is_negated: false}, "2021-01-01"),
                    pq("updated_at", SearchOperator::Lte{ is_negated: false}, "2021-12-31"),
                ],
            },
            TestCase {
                query_string: "name__not_icontains=foo&description=bar&permission=CanRead&validate_schema=true",
                expected: vec![
                    pq("name", SearchOperator::IContains{ is_negated: true}, "foo"),
                    pq("description", SearchOperator::Equals{ is_negated: false}, "bar"),
                    pq("permission", SearchOperator::Equals{ is_negated: false}, "CanRead"),
                    pq("validate_schema", SearchOperator::Equals{ is_negated: false}, "true"),
                ],
            },
        ];

        for case in test_cases {
            let parsed_query_params = parse_query_parameter(case.query_string).unwrap();
            assert_eq!(
                parsed_query_params, case.expected,
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
                format!("{}#>>'{{key}}' = 'foo'", field),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::IEquals { is_negated: true },
                    "key=foo",
                ),
                format!("NOT {}#>>'{{key}}' ILIKE 'foo'", field),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Gt { is_negated: false },
                    "key,subkey=3",
                ),
                format!("({}#>>'{{key,subkey}}'::text)::numeric > 3", field),
            ),
        ];

        for (param, expected) in test_cases {
            let result = param.as_json_sql();
            assert_eq!(
                result,
                Ok(expected.to_string()),
                "Failed test case for param: {:?}",
                param,
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
                format!("({}#>>'{{key}}'::text)::date = '2021-01-01'", field),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Gt { is_negated: false },
                    "key,subkey=2021-01-01",
                ),
                format!("({}#>>'{{key,subkey}}'::text)::date > '2021-01-01'", field),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Gt { is_negated: true },
                    "key,subkey=2021-01-01",
                ),
                format!(
                    "NOT ({}#>>'{{key,subkey}}'::text)::date > '2021-01-01'",
                    field
                ),
            ),
        ];

        for (param, expected) in test_cases {
            let result = param.as_json_sql();
            assert_eq!(
                result,
                Ok(expected.to_string()),
                "Failed test case for param: {:?}",
                param,
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
                format!("({}#>>'{{key}}'::text)::numeric = 3", field),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Gt { is_negated: false },
                    "key,subkey=3",
                ),
                format!("({}#>>'{{key,subkey}}'::text)::numeric > 3", field),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Gt { is_negated: true },
                    "key,subkey=3",
                ),
                format!("NOT ({}#>>'{{key,subkey}}'::text)::numeric > 3", field),
            ),
        ];

        for (param, expected) in test_cases {
            let result = param.as_json_sql();
            assert_eq!(
                result,
                Ok(expected.to_string()),
                "Failed test case for param: {:?}",
                param,
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
                format!("({}#>>'{{key,subkey}}'::text)::numeric = 3", field),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Equals { is_negated: false },
                    "key,subkey,subsubkey=3",
                ),
                format!(
                    "({}#>>'{{key,subkey,subsubkey}}'::text)::numeric = 3",
                    field
                ),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Equals { is_negated: true },
                    "key,subkey,subsubkey,subsubsubkey=3",
                ),
                format!(
                    "NOT ({}#>>'{{key,subkey,subsubkey,subsubsubkey}}'::text)::numeric = 3",
                    field
                ),
            ),
        ];

        for (param, expected) in test_cases {
            let result = param.as_json_sql();
            assert_eq!(
                result,
                Ok(expected.to_string()),
                "Failed test case for param: {:?}",
                param,
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
            assert_eq!(result, Some(expected), "Failed test case for key: {}", key);
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
            assert_eq!(result, None, "Failed test case for key: {}", key);
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
                "Failed test case for value: '{}'",
                value
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
            ("null", Operator::Gt, None),
            ("foo", Operator::Gt, None),
        ];

        for (value, operator, expected) in test_cases {
            let result = get_jsonb_field_type_from_value_and_operator(value, operator.clone());
            assert_eq!(
                result, expected,
                "Failed test case for value: '{}', operator: '{:?}'",
                value, operator
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
        ];

        for (input, expected) in test_cases {
            let result = SO::new_from_string(input);
            assert_eq!(
                result,
                Ok(expected),
                "Failed test case for input: '{}'",
                input
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
                "Failed test case for operator: '{:?}', data_type: '{:?}'",
                operator, data_type
            );
        }
    }
}
