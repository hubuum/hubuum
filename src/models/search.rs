#![allow(dead_code)]
use tracing::field;

use crate::errors::ApiError;
use crate::models::permissions::Permissions;

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
        let query_param_parts: Vec<&str> = query_param.split('=').collect();

        if query_param_parts.len() != 2 {
            return Err(ApiError::BadRequest(format!(
                "Invalid query parameter: '{}'",
                query_param
            )));
        }

        let field_and_op: Vec<&str> = query_param_parts[0].split("__").collect();
        let value = query_param_parts[1].to_string();
        let field = field_and_op[0].to_string();
        let operator;

        if value.is_empty() {
            return Err(ApiError::BadRequest(format!(
                "Invalid query parameter: '{}', no value",
                query_param
            )));
        }

        if field_and_op.len() == 1 {
            operator = "equals";
        } else if field_and_op.len() == 2 {
            operator = field_and_op[1];
        } else {
            return Err(ApiError::BadRequest(format!(
                "Invalid query parameter: '{}', multiple elementens, unable to parse value and operator",
                query_param
            )));
        }

        let parsed_query_param = ParsedQueryParam {
            field,
            operator: SearchOperator::new_from_string(operator)?,
            value,
        };

        parsed_query_params.push(parsed_query_param);
    }

    Ok(parsed_query_params)
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
        let operator =
            operator.unwrap_or(SearchOperator::Universal(UniversialSearchOperator::Equals));

        ParsedQueryParam {
            field: field.to_string(),
            operator,
            value: value.to_string(),
        }
    }

    pub fn is_permission(&self) -> bool {
        self.field == "permission"
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
}

impl QueryParamsExt for Vec<ParsedQueryParam> {
    fn permissions(&self) -> Result<Vec<Permissions>, ApiError> {
        self.iter()
            .filter(|p| p.is_permission())
            .map(|p| Permissions::from_string(&p.value))
            .collect()
    }
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
/// * A vector of integers or ApiError::BadRequest if the input is invalid
pub fn parse_integer_list(input: &str) -> Result<Vec<i32>, ApiError> {
    let mut result = Vec::new();
    for part in input.split(',') {
        let range: Vec<&str> = part.split('-').collect();
        match range.len() {
            1 => {
                if let Ok(num) = range[0].parse::<i32>() {
                    result.push(num);
                }
            }
            2 => {
                if let (Ok(start), Ok(end)) = (range[0].parse::<i32>(), range[1].parse::<i32>()) {
                    result.extend((start..=end).collect::<Vec<i32>>());
                }
            }
            _ => Err(ApiError::BadRequest(format!(
                "Invalid integer range: '{}'",
                part
            )))?,
        }
    }
    Ok(result)
}

#[derive(Debug, PartialEq, Clone)]
pub enum UniversialSearchOperator {
    Equals,
}

#[derive(Debug, PartialEq, Clone)]
pub enum StringSearchOperator {
    IEquals,
    Contains,
    IContains,
    StartsWith,
    IStartsWith,
    EndsWith,
    IEndsWith,
    Regex,
}

#[derive(Debug, PartialEq, Clone)]
pub enum NumericOrDateSearchOperator {
    Gt,
    Gte,
    Lt,
    Lte,
    Between,
}

#[derive(Debug, PartialEq, Clone)]
pub enum JsonSearchOperator {
    String(StringSearchOperator),
    NumericOrDate(NumericOrDateSearchOperator),
    UniversialOperator(UniversialSearchOperator),
}

/// ## An enum that represents a search operator
///
/// This enum represents the different types of search operators that can be used in a search query,
/// such as equals, greater than, less than, etc, and the different types of data they can be used on.
#[derive(Debug, PartialEq, Clone)]
pub enum SearchOperator {
    String(StringSearchOperator),
    NumericOrDate(NumericOrDateSearchOperator),
    Universal(UniversialSearchOperator),
    Json(JsonSearchOperator),
}

impl SearchOperator {
    pub fn new_from_string(operator: &str) -> Result<SearchOperator, ApiError> {
        match operator {
            "equals" => Ok(SearchOperator::Universal(UniversialSearchOperator::Equals)),
            "iequals" => Ok(SearchOperator::String(StringSearchOperator::IEquals)),
            "contains" => Ok(SearchOperator::String(StringSearchOperator::Contains)),
            "icontains" => Ok(SearchOperator::String(StringSearchOperator::IContains)),
            "startswith" => Ok(SearchOperator::String(StringSearchOperator::StartsWith)),
            "istartswith" => Ok(SearchOperator::String(StringSearchOperator::IStartsWith)),
            "endswith" => Ok(SearchOperator::String(StringSearchOperator::EndsWith)),
            "iendswith" => Ok(SearchOperator::String(StringSearchOperator::IEndsWith)),
            "regex" => Ok(SearchOperator::String(StringSearchOperator::Regex)),
            "gt" => Ok(SearchOperator::NumericOrDate(
                NumericOrDateSearchOperator::Gt,
            )),
            "gte" => Ok(SearchOperator::NumericOrDate(
                NumericOrDateSearchOperator::Gte,
            )),
            "lt" => Ok(SearchOperator::NumericOrDate(
                NumericOrDateSearchOperator::Lt,
            )),
            "lte" => Ok(SearchOperator::NumericOrDate(
                NumericOrDateSearchOperator::Lte,
            )),
            "within" => Ok(SearchOperator::NumericOrDate(
                NumericOrDateSearchOperator::Between,
            )),
            _ => Err(ApiError::BadRequest(format!(
                "Invalid search operator: '{}'",
                operator
            ))),
        }
    }
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
            assert!(result.is_err(), "Failed test case for query: {}", case);
            assert_eq!(
                result.unwrap_err(),
                ApiError::BadRequest(test_case_errors[i].to_string()),
                "Failed test case for query: {}",
                case
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
                    pq("name", SearchOperator::String(StringSearchOperator::IContains), "foo"),
                    pq("description", SearchOperator::Universal(UniversialSearchOperator::Equals), "bar"),
                ],
            },
            TestCase {
                query_string: "name__contains=foo&description__icontains=bar&created_at__gte=2021-01-01&updated_at__lte=2021-12-31",
                expected: vec![
                    pq("name", SearchOperator::String(StringSearchOperator::Contains), "foo"),
                    pq("description", SearchOperator::String(StringSearchOperator::IContains), "bar"),
                    pq("created_at", SearchOperator::NumericOrDate(NumericOrDateSearchOperator::Gte), "2021-01-01"),
                    pq("updated_at", SearchOperator::NumericOrDate(NumericOrDateSearchOperator::Lte), "2021-12-31"),
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
}
