#![allow(dead_code)]
use actix_web::web::Json;
use chrono::{format, DateTime, NaiveDateTime, Utc};
use diesel::dsl::Filter;
use diesel::sql_types::Bool;
use std::collections::HashSet;
use std::str::FromStr;
use tracing::debug;

use crate::models::permissions::{Permissions, PermissionsList};
use crate::traits::SelfAccessors;
use crate::utilities::extensions::CustomStringExtensions;
use crate::{errors::ApiError, schema::hubuumobject::data};

use super::{HubuumClassID, Permission};

/// ## Parse a query string into search parameters
///
/// ## Arguments
///
/// * `query_string` - A string that contains the query parameters
///
/// ## Returns
///
pub fn parse_query_parameter(qs: &str) -> Result<QueryOptions, ApiError> {
    let mut filters = Vec::new();
    let mut sort = Vec::new();
    let mut limit = None;

    if qs.is_empty() {
        return Ok(QueryOptions {
            filters,
            sort,
            limit,
        });
    }

    for chunk in qs.split('&') {
        let parts: Vec<_> = chunk.splitn(2, '=').collect();
        if parts.len() != 2 {
            return Err(ApiError::BadRequest(format!(
                "Invalid query parameter: '{chunk}'"
            )));
        }

        let key = parts[0];
        let value = match percent_encoding::percent_decode(parts[1].as_bytes()).decode_utf8() {
            Ok(value) => value.to_string(),
            Err(e) => {
                return Err(ApiError::BadRequest(format!(
                    "Invalid query parameter: '{chunk}', invalid value: {e}",
                )));
            }
        };

        match key {
            // LIMIT: e.g. limit=10, for limiting the number of results
            "limit" => {
                if limit.is_some() {
                    return Err(ApiError::BadRequest("duplicate limit".into()));
                }
                limit = Some(
                    value
                        .parse::<usize>()
                        .map_err(|e| ApiError::BadRequest(format!("bad limit: {e}")))?,
                );
            }

            // SORT / ORDER BY: e.g. sort=created_at,-name,email.desc
            "sort" | "order_by" => {
                for piece in value.split(',') {
                    let descending = piece.starts_with('-') || piece.ends_with(".desc");
                    let field_name = piece
                        .trim_start_matches('-')
                        .trim_end_matches(".asc")
                        .trim_end_matches(".desc");
                    let field = FilterField::from_str(field_name)?;
                    sort.push(SortParam { field, descending });
                }
            }

            // FILTER: e.g. field__op=value
            _ => {
                let param = parse_single_filter(key, &value)?;
                filters.push(param);
            }
        }
    }

    Ok(QueryOptions {
        filters,
        sort,
        limit,
    })
}

fn parse_single_filter(key: &str, value: &str) -> Result<ParsedQueryParam, ApiError> {
    let field_and_op: Vec<&str> = key.splitn(2, "__").collect();
    let value = value.to_string();
    let field = field_and_op[0].to_string();

    if value.is_empty() {
        return Err(ApiError::BadRequest(format!(
            "Invalid query parameter: '{key}', no value",
        )));
    }

    let operator = if field_and_op.len() == 1 {
        SearchOperator::new_from_string("equals")?
    } else {
        SearchOperator::new_from_string(field_and_op[1])?
    };

    let parsed_query_param = ParsedQueryParam {
        field: FilterField::from_str(&field)?,
        operator,
        value,
    };

    Ok(parsed_query_param)
}

/// ## A struct that represents a set of query options
///
/// This struct holds a list of filters, a list of sort parameters, and a limit on the number of results.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryOptions {
    pub filters: Vec<ParsedQueryParam>,
    pub sort: Vec<SortParam>,
    pub limit: Option<usize>,
}

/// ## A struct that represents a filter field
#[derive(Debug, Clone, PartialEq)]
pub struct SortParam {
    pub field: FilterField,
    pub descending: bool,
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
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedQueryParam {
    pub field: FilterField,
    pub operator: SearchOperator,
    pub value: String,
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

impl QueryOptions {
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
    pub fn ensure_filter<I, T>(
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
    pub fn ensure_filter_exact(&mut self, field: FilterField, identifier: &HubuumClassID) -> bool {
        self.filters.ensure_filter(
            field,
            SearchOperator::Equals { is_negated: false },
            &identifier.id().to_string(),
        )
    }
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
    pub fn new(
        field: &str,
        operator: Option<SearchOperator>,
        value: &str,
    ) -> Result<Self, ApiError> {
        let operator = operator.unwrap_or(SearchOperator::Equals { is_negated: false });

        Ok(ParsedQueryParam {
            field: FilterField::from_str(field)?,
            operator,
            value: value.to_string(),
        })
    }

    pub fn is_permission(&self) -> bool {
        self.field == FilterField::Permissions
    }

    pub fn is_namespace(&self) -> bool {
        self.field == FilterField::Namespaces
    }

    pub fn is_json_schema(&self) -> bool {
        self.field == FilterField::JsonSchema
    }

    pub fn is_json_data(&self) -> bool {
        self.field == FilterField::JsonData
            || self.field == FilterField::JsonDataFrom
            || self.field == FilterField::JsonDataTo
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
        self.value.as_permission()
    }

    /// ## Coerce the value into a list of integers
    ///
    /// Accepts the format given to the [`as_integer`] trait.
    ///
    /// ### Returns
    ///
    /// * A vector of integers or ApiError::BadRequest if the value is invalid
    pub fn value_as_integer(&self) -> Result<Vec<i32>, ApiError> {
        self.value.as_integer()
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
        self.value.as_date()
    }

    /// ## Coerce the value into a boolean
    ///
    /// Accepted values are "true" and "false" (case insensitive)
    ///
    /// ### Returns
    ///
    /// * A boolean or ApiError::BadRequest if the value is invalid
    pub fn value_as_boolean(&self) -> Result<bool, ApiError> {
        self.value.as_boolean()
    }

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
    pub fn as_json_sql(&self) -> Result<SQLComponent, ApiError> {
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

        // TODO: Since we may have a schema, we may have typing info, so we can also
        // validatethe value and the operator against the defined type in the schema.

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

        let key = format!("'{{{key}}}'");

        // The bind variables for the SQL query. We can't bind the key as using
        // bind variables for the key itself is not supported in Postgres.
        let mut bind_variables = vec![];

        // TODO: Optionally validate that the keys exist:
        // https://github.com/terjekv/hubuum_rust/issues/4

        let (op, neg) = self.operator.op_and_neg();
        let neg_str = if neg { "NOT " } else { "" };

        let sql_type = get_jsonb_field_type_from_value_and_operator(value, op.clone());

        // TODO: Add JSON Schema usage type support via
        // get_jsonb_field_type_from_json_schema(schema, key)

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
                )))
            }
        };

        let sql = match sql_type {
            None => {
                return Err(ApiError::BadRequest(format!(
                    "Invalid JSON type mapping between key '{}' and operator '{:?}'",
                    key, self.operator
                )))
            }
            Some(SQLMappedType::String) | Some(SQLMappedType::None) => {
                bind_variables.push(SQLValue::String(value));
                format!(
                    "{}{} #>> {} {} ?",
                    neg_str,
                    field.table_field(),
                    key,
                    sql_op
                )
            }
            Some(SQLMappedType::Numeric) => {
                let ints = value.as_integer()?;
                bind_variables.push(SQLValue::Integer(ints[0]));
                format!(
                    "{}({} #>> {})::numeric {} ?",
                    neg_str,
                    field.table_field(),
                    key,
                    sql_op
                )
            }
            Some(SQLMappedType::Date) => {
                let dates = value.as_date()?;
                bind_variables.push(SQLValue::Date(dates[0]));
                format!(
                    "{}({} #>> {})::date {} ?",
                    neg_str,
                    field.table_field(),
                    key,
                    sql_op
                )
            }
            Some(SQLMappedType::Boolean) => {
                let boolean = value.as_boolean()?;
                bind_variables.push(SQLValue::Boolean(boolean));
                format!(
                    "{}({} #>> {})::boolean {} ?",
                    neg_str,
                    field.table_field(),
                    key,
                    sql_op
                )
            }
        };

        debug!(message = "SQL JSONB generation", sql = %sql, bind_varaibles = ?bind_variables);

        Ok(SQLComponent {
            sql,
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

    /// ## Get a sorted list of namespace ids from a list of parsed query parameters
    ///
    /// Iterate over the parsed query parameters and filter out the ones that are namespaces,
    /// defined as having the `field` set as "namespaces". For each value of each parsed query
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
            match param.value_as_permission() {
                Ok(permission) => {
                    unique_permissions.insert(permission);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(PermissionsList::new(unique_permissions))
    }
    /// ## Get a sorted list of namespace ids from a list of parsed query parameters
    ///
    /// Iterate over the parsed query parameters and filter out the ones that are namespaces,
    /// defined as having the `field` set as "namespaces". For each value of a matching parsed query
    /// parameter, attempt to parse it into a list of integers via [`parse_integer_list`].
    ///
    /// If any value is not a valid list of integers, return an ApiError::BadRequest.
    fn namespaces(&self) -> Result<Vec<i32>, ApiError> {
        let mut nids = vec![];

        for p in self.iter() {
            if p.field == FilterField::Namespaces {
                nids.extend(p.value.as_integer()?);
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

impl std::fmt::Display for Operator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let op = match self {
            Operator::Equals => "equals",
            Operator::IEquals => "iequals",
            Operator::Contains => "contains",
            Operator::IContains => "icontains",
            Operator::StartsWith => "startswith",
            Operator::IStartsWith => "istartswith",
            Operator::EndsWith => "endswith",
            Operator::IEndsWith => "iendswith",
            Operator::Like => "like",
            Operator::Regex => "regex",
            Operator::Gt => "gt",
            Operator::Gte => "gte",
            Operator::Lt => "lt",
            Operator::Lte => "lte",
            Operator::Between => "between",
        };
        write!(f, "{op}")
    }
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
    Array,
}

impl std::fmt::Display for SearchOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let (op, neg) = self.op_and_neg();
        let neg_str = if neg { "not_" } else { "" };
        write!(f, "{neg_str}{op}")
    }
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
            SO::Contains { .. } => {
                matches!(data_type, DataType::String) || matches!(data_type, DataType::Array)
            }
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
                "Invalid search operator: '{operator}'"
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
    if let Some(Value::String(format_str)) = current_schema.get("format") {
        match format_str.as_ref() {
            "date-time" | "date" => return Some(SQLMappedType::Date),
            _ => {}
        }
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
        Operator::Equals => get_sql_mapped_type_from_value(
            value,
            &[
                SQLMappedType::Date,
                SQLMappedType::Boolean,
                SQLMappedType::Numeric,
                SQLMappedType::None,
                SQLMappedType::String,
            ],
        ),
        Operator::Contains => get_sql_mapped_type_from_value(
            value,
            &[
                SQLMappedType::Date,
                SQLMappedType::Numeric,
                SQLMappedType::String,
            ],
        ),
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
            lval // Already a Some() from get_sql_mapped_type_from_value
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

// Generate the FilterField enum and its associated functions. We use a macro
// to generate the enum and its functions to ensure that the FromStr and query_string
// functions are always in sync.
macro_rules! filter_fields {
    ($(($variant:ident, $str_rep:expr)),* $(,)?) => {
        /// Valid search fields in URLS.
        ///
        /// Each enum variant corresponds to a field that can be searched on. As a general rule, fields that may
        /// be issued repeatedly in the query string are puralized while fields that are unique are singular.
        ///
        /// JSON fields (JsonSchema and JsonData) also have a table field, which is used for JSON SQL query generation
        /// to map into the correct JSON field in the database as these fields do not use the macro–defined searches
        /// and interpolate the field directly.
        #[derive(Debug, PartialEq, Clone)]
        pub enum FilterField {
            $($variant),*
        }

        impl std::str::FromStr for FilterField {
            type Err = ApiError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    $(
                        $str_rep => Ok(FilterField::$variant),
                    )*
                    _ => Err(ApiError::BadRequest(format!(
                        "Invalid search field: '{}'",
                        s
                    ))),
                }
            }
        }

        impl std::fmt::Display for FilterField {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $(
                        FilterField::$variant => write!(f, "{}", $str_rep),
                    )*
                }
            }
        }

        impl FilterField {
            pub fn table_field(&self) -> &'static str {
                match self {
                    FilterField::JsonSchema => "json_schema",
                    FilterField::JsonData => "json_data",
                    _ => panic!("{:?} should not be used as a table field", self),
                }
            }
        }
    }
}

filter_fields!(
    (Id, "id"),
    (Namespaces, "namespaces"),
    (NamespaceId, "namespace_id"),
    (Name, "name"),
    (Groupname, "groupname"),
    (Username, "username"),
    (Description, "description"),
    (Email, "email"),
    (ValidateSchema, "validate_schema"),
    (JsonSchema, "json_schema"),
    (JsonData, "json_data"),
    (Permissions, "permissions"),
    (Classes, "classes"),
    (ClassId, "class_id"),
    (CreatedAt, "created_at"),
    (UpdatedAt, "updated_at"),
    (NameFrom, "from_name"),
    (NameTo, "to_name"),
    (DescriptionFrom, "from_description"),
    (DescriptionTo, "to_description"),
    (ObjectFrom, "from_objects"),
    (ObjectTo, "to_objects"),
    (ClassTo, "to_classes"),
    (ClassFrom, "from_classes"),
    (ClassToName, "to_class_name"),
    (ClassFromName, "from_class_name"),
    (NamespacesFrom, "from_namespaces"),
    (NamespacesTo, "to_namespaces"),
    (JsonDataFrom, "from_json_data"),
    (JsonDataTo, "to_json_data"),
    (CreatedAtFrom, "from_created_at"),
    (CreatedAtTo, "to_created_at"),
    (UpdatedAtFrom, "from_updated_at"),
    (UpdatedAtTo, "to_updated_at"),
    (ClassRelation, "class_relation"),
    (Depth, "depth"),
    (Path, "path"),
);

// TODO: Rewrite to use yare::parametrized...
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
                query_string: "name__not_icontains=foo&description=bar&permissions=CanRead&validate_schema=true",
                expected: vec![
                    pq("name", SearchOperator::IContains{ is_negated: true}, "foo"),
                    pq("description", SearchOperator::Equals{ is_negated: false}, "bar"),
                    pq("permissions", SearchOperator::Equals{ is_negated: false}, "CanRead"),
                    pq("validate_schema", SearchOperator::Equals{ is_negated: false}, "true"),
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
                format!("({field} #>> '{{key,subkey}}')::numeric > ?"),
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
    fn test_json_schema_sql_query_date_generation() {
        let field = "json_schema";
        let test_cases = vec![
            (
                pq(
                    "json_schema",
                    SearchOperator::Equals { is_negated: false },
                    "key=2021-01-01",
                ),
                format!("({field} #>> '{{key}}')::date = ?"),
                SQLValue::Date("2021-01-01".as_date().unwrap()[0]),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Gt { is_negated: false },
                    "key,subkey=2021-01-01",
                ),
                format!("({field} #>> '{{key,subkey}}')::date > ?"),
                SQLValue::Date("2021-01-01".as_date().unwrap()[0]),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Gt { is_negated: true },
                    "key,subkey=2021-01-01",
                ),
                format!("NOT ({field} #>> '{{key,subkey}}')::date > ?"),
                SQLValue::Date("2021-01-01".as_date().unwrap()[0]),
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
    fn test_json_schema_sql_query_numerical_generation() {
        let field = "json_schema";
        let test_cases = vec![
            (
                pq(
                    "json_schema",
                    SearchOperator::Equals { is_negated: false },
                    "key=3",
                ),
                format!("({field} #>> '{{key}}')::numeric = ?"),
                SQLValue::Integer(3),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Gt { is_negated: false },
                    "key,subkey=3",
                ),
                format!("({field} #>> '{{key,subkey}}')::numeric > ?"),
                SQLValue::Integer(3),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Gt { is_negated: true },
                    "key,subkey=3",
                ),
                format!("NOT ({field} #>> '{{key,subkey}}')::numeric > ?"),
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
    fn test_json_schema_sql_generation_wrapping() {
        let field = "json_schema";
        let test_cases = vec![
            (
                pq(
                    "json_schema",
                    SearchOperator::Equals { is_negated: false },
                    "key,subkey=3",
                ),
                format!("({field} #>> '{{key,subkey}}')::numeric = ?"),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Equals { is_negated: false },
                    "key,subkey,subsubkey=3",
                ),
                format!("({field} #>> '{{key,subkey,subsubkey}}')::numeric = ?"),
            ),
            (
                pq(
                    "json_schema",
                    SearchOperator::Equals { is_negated: true },
                    "key,subkey,subsubkey,subsubsubkey=3",
                ),
                format!("NOT ({field} #>> '{{key,subkey,subsubkey,subsubsubkey}}')::numeric = ?",),
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
}
