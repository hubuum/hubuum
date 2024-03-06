#![allow(dead_code)]
use tracing::field;

use crate::errors::ApiError;
use crate::models::permissions::Permissions;
use chrono::{DateTime, NaiveDateTime, Utc};

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
        let parsed_dates: Result<Vec<NaiveDateTime>, _> = self
            .value
            .split(',')
            .map(|part| part.trim())
            .map(|part| {
                DateTime::parse_from_rfc3339(part)
                    .map(|dt| dt.with_timezone(&Utc)) // Convert to Utc
                    .map(|utc_dt| utc_dt.naive_utc()) // Convert to NaiveDateTime
                    .map_err(|e| e.into()) // Convert chrono::ParseError (or any error) into ApiError
            })
            .collect(); // Collect into a Result<Vec<NaiveDateTime>, ApiError>

        parsed_dates
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
    /// ## Get a list of all Permissions in a list of parsed query parameters
    ///
    /// Iterate over the parsed query parameters and filter out the ones that are permissions,
    /// defined as having the `field` set as "permission". For each value of a matching parsed query
    /// parameter, attempt to parse it into a Permissions enum.
    ///
    /// If any value is not a valid permission, return an ApiError::BadRequest.
    fn permissions(&self) -> Result<Vec<Permissions>, ApiError> {
        self.iter()
            .filter(|p| p.is_permission())
            .map(|p| p.value_as_permission())
            .collect()
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
/// such as equals, greater than, less than, etc, and the different types of data they can be used on.
#[derive(Debug, PartialEq, Clone)]
pub enum SearchOperator {
    Equals {
        is_negated: bool,
    },
    IEquals {
        data_type: DataType,
        is_negated: bool,
    },
    Contains {
        data_type: DataType,
        is_negated: bool,
    },
    IContains {
        data_type: DataType,
        is_negated: bool,
    },
    StartsWith {
        data_type: DataType,
        is_negated: bool,
    },
    IStartsWith {
        data_type: DataType,
        is_negated: bool,
    },
    EndsWith {
        data_type: DataType,
        is_negated: bool,
    },
    IEndsWith {
        data_type: DataType,
        is_negated: bool,
    },
    Like {
        data_type: DataType,
        is_negated: bool,
    },
    Regex {
        data_type: DataType,
        is_negated: bool,
    },
    Gt {
        data_type: DataType,
        is_negated: bool,
    },
    Gte {
        data_type: DataType,
        is_negated: bool,
    },
    Lt {
        data_type: DataType,
        is_negated: bool,
    },
    Lte {
        data_type: DataType,
        is_negated: bool,
    },
    Between {
        data_type: DataType,
        is_negated: bool,
    },
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
        match self {
            SearchOperator::Equals { is_negated: _ } => true,
            SearchOperator::IEquals {
                data_type: dt,
                is_negated: _,
            }
            | SearchOperator::Contains {
                data_type: dt,
                is_negated: _,
            }
            | SearchOperator::IContains {
                data_type: dt,
                is_negated: _,
            }
            | SearchOperator::StartsWith {
                data_type: dt,
                is_negated: _,
            }
            | SearchOperator::IStartsWith {
                data_type: dt,
                is_negated: _,
            }
            | SearchOperator::EndsWith {
                data_type: dt,
                is_negated: _,
            }
            | SearchOperator::IEndsWith {
                data_type: dt,
                is_negated: _,
            }
            | SearchOperator::Like {
                data_type: dt,
                is_negated: _,
            }
            | SearchOperator::Regex {
                data_type: dt,
                is_negated: _,
            }
            | SearchOperator::Gt {
                data_type: dt,
                is_negated: _,
            }
            | SearchOperator::Gte {
                data_type: dt,
                is_negated: _,
            }
            | SearchOperator::Lt {
                data_type: dt,
                is_negated: _,
            }
            | SearchOperator::Lte {
                data_type: dt,
                is_negated: _,
            }
            | SearchOperator::Between {
                data_type: dt,
                is_negated: _,
            } => *dt == data_type,
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
                data_type: DataType::String,
                is_negated: negated,
            }),
            "contains" => Ok(SO::Contains {
                data_type: DataType::String,
                is_negated: negated,
            }),
            "icontains" => Ok(SO::IContains {
                data_type: DataType::String,
                is_negated: negated,
            }),
            "startswith" => Ok(SO::StartsWith {
                data_type: DataType::String,
                is_negated: negated,
            }),
            "istartswith" => Ok(SO::IStartsWith {
                data_type: DataType::String,
                is_negated: negated,
            }),
            "endswith" => Ok(SO::EndsWith {
                data_type: DataType::String,
                is_negated: negated,
            }),
            "iendswith" => Ok(SO::IEndsWith {
                data_type: DataType::String,
                is_negated: negated,
            }),
            "like" => Ok(SO::Like {
                data_type: DataType::String,
                is_negated: negated,
            }),
            "regex" => Ok(SO::Regex {
                data_type: DataType::String,
                is_negated: negated,
            }),
            "gt" => Ok(SO::Gt {
                data_type: DataType::NumericOrDate,
                is_negated: negated,
            }),
            "gte" => Ok(SO::Gte {
                data_type: DataType::NumericOrDate,
                is_negated: negated,
            }),
            "lt" => Ok(SO::Lt {
                data_type: DataType::NumericOrDate,
                is_negated: negated,
            }),
            "lte" => Ok(SO::Lte {
                data_type: DataType::NumericOrDate,
                is_negated: negated,
            }),
            "between" => Ok(SO::Between {
                data_type: DataType::NumericOrDate,
                is_negated: negated,
            }),

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
                    pq("name", SearchOperator::IContains{ data_type: DataType::String, is_negated: false }, "foo"),
                    pq("description", SearchOperator::Equals{ is_negated: false}, "bar"),
                ],
            },
            TestCase {
                query_string: "name__contains=foo&description__icontains=bar&created_at__gte=2021-01-01&updated_at__lte=2021-12-31",
                expected: vec![
                    pq("name", SearchOperator::Contains{ data_type: DataType::String, is_negated: false}, "foo"),
                    pq("description", SearchOperator::IContains{ data_type: DataType::String, is_negated: false}, "bar"),
                    pq("created_at", SearchOperator::Gte{ data_type: DataType::NumericOrDate, is_negated: false}, "2021-01-01"),
                    pq("updated_at", SearchOperator::Lte{ data_type: DataType::NumericOrDate, is_negated: false}, "2021-12-31"),
                ],
            },
            TestCase {
                query_string: "name__not_icontains=foo&description=bar&permission=CanRead&validate_schema=true",
                expected: vec![
                    pq("name", SearchOperator::IContains{ data_type: DataType::String, is_negated: true}, "foo"),
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
}
