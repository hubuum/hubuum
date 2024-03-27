use crate::errors::ApiError;
use crate::models::permissions::Permissions;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};

pub trait CustomStringExtensions {
    /// ## Check if the value is a valid json key for hubuum
    ///
    /// We support only lowercase alphanumeric characters and underscores, as
    /// well as the comma character for nested keys. No spaces are allowed.
    ///
    /// ### Returns
    ///
    /// * A boolean
    fn is_valid_jsonb_search_key(&self) -> bool;

    /// ## Check if the value is a valid json value for searching in jsonb fields.
    ///
    /// It seems that the diesel ORM does not allow a run-time defined number of bind
    /// variables of unknown (at compile time) types, so it seems we can't use
    /// parameterized queries for jsonb fields.
    ///
    /// This leaves is with the issue of generating these queries manually, which apart
    /// from a performance hit, also opens the door to SQL injection attacks. This function
    /// is a first line of defense against such attacks.
    ///
    /// ### Returns
    ///
    /// * A boolean
    fn is_valid_jsonb_search_value(&self) -> bool;

    /// ## Coerce the value into a boolean
    ///
    /// Accepted values are "true" and "false" (case insensitive)
    ///
    /// ### Returns
    ///
    /// * A boolean or ApiError::BadRequest if the value is invalid
    fn as_boolean(&self) -> Result<bool, ApiError>;

    /// ## Coerce the value into a list of dates
    ///
    /// Accepts a comma separated list of RFC3339 dates.
    /// https://www.rfc-editor.org/rfc/rfc3339
    ///     
    /// ### Returns
    ///
    /// * A vector of NaiveDateTime or ApiError::BadRequest if the value is invalid
    fn as_date(&self) -> Result<Vec<NaiveDateTime>, ApiError>;

    /// ## Coerce the value into a Permissions enum
    ///
    /// ### Returns
    ///
    /// * A Permissions enum or ApiError::BadRequest if the value is invalid
    fn as_permission(&self) -> Result<Permissions, ApiError>;

    /// ## Coerce the value into a list of integers
    ///
    /// Accepts the format given to the [`parse_integer_list`] function.
    ///
    /// ### Returns
    ///
    /// * A vector of integers or ApiError::BadRequest if the value is invalid
    fn as_integer(&self) -> Result<Vec<i32>, ApiError>;

    /// ## Replace ? with $n in a string
    ///
    /// This is used to replace the ? placeholders in a query with the $n placeholders
    ///
    /// ### Returns
    ///
    /// * A string with the ? placeholders replaced with $n
    fn replace_question_mark_with_indexed_n(&self) -> String;
}

// Implement the trait for the `str` type
impl CustomStringExtensions for str {
    fn replace_question_mark_with_indexed_n(&self) -> String {
        let mut n = 1;
        let mut result = self.to_string();
        while let Some(pos) = result.find('?') {
            // Replace '?' with '$n'
            result.replace_range(pos..pos + 1, &format!("${}", n));
            n += 1;
        }
        result
    }

    fn is_valid_jsonb_search_key(&self) -> bool {
        self.chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == ',' || c == '$')
    }

    fn is_valid_jsonb_search_value(&self) -> bool {
        self.chars().all(|c| {
            c.is_alphanumeric()
                || c.is_whitespace()
                || c == '_'
                || c == '-'
                || c == ','
                || c == '.'
                || c == ':'
                || c == '/'
                || c == '\\'
                || c == '['
                || c == ']'
                || c == '{'
                || c == '}'
                || c == '*'
                || c == '@'
        })
    }

    fn as_integer(&self) -> Result<Vec<i32>, ApiError> {
        parse_integer_list(self)
    }

    fn as_boolean(&self) -> Result<bool, ApiError> {
        match self.to_lowercase().as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(ApiError::BadRequest(format!(
                "Invalid boolean value: '{}'",
                self
            ))),
        }
    }

    /*
    fn as_date(&self) -> Result<Vec<NaiveDateTime>, ApiError> {
        self.split(',')
            .map(|part| part.trim())
            .map(|part| {
                DateTime::parse_from_rfc3339(part)
                    .map(|dt| dt.with_timezone(&Utc))
                    .map(|utc_dt| utc_dt.naive_utc())
                    .map_err(|e| e.into())
            })
            .collect()
    }
    */

    fn as_date(&self) -> Result<Vec<NaiveDateTime>, ApiError> {
        self.split(',')
            .map(|part| part.trim())
            .map(|part| {
                DateTime::parse_from_rfc3339(part)
                    .map(|dt| dt.with_timezone(&Utc).naive_utc())
                    .or_else(|_| {
                        NaiveDate::parse_from_str(part, "%Y-%m-%d")
                            .map(|date| date.and_hms_opt(0, 0, 0).unwrap())
                            .map_err(Into::<ApiError>::into)
                    })
                    .map_err(Into::<ApiError>::into)
            })
            .collect()
    }

    fn as_permission(&self) -> Result<Permissions, ApiError> {
        Permissions::from_string(self)
    }
}

// Also implement the trait for the `String` type
impl CustomStringExtensions for String {
    fn replace_question_mark_with_indexed_n(&self) -> String {
        self.as_str().replace_question_mark_with_indexed_n()
    }

    fn is_valid_jsonb_search_key(&self) -> bool {
        self.as_str().is_valid_jsonb_search_key()
    }

    fn is_valid_jsonb_search_value(&self) -> bool {
        self.as_str().is_valid_jsonb_search_value()
    }

    fn as_integer(&self) -> Result<Vec<i32>, ApiError> {
        self.as_str().as_integer()
    }

    fn as_boolean(&self) -> Result<bool, ApiError> {
        self.as_str().as_boolean()
    }

    fn as_date(&self) -> Result<Vec<NaiveDateTime>, ApiError> {
        self.as_str().as_date()
    }

    fn as_permission(&self) -> Result<Permissions, ApiError> {
        self.as_str().as_permission()
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
