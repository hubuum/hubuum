use crate::errors::ApiError;
use crate::models::permissions::Permissions;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use tracing::error;

pub trait CustomStringExtensions {
    /// ## Check if the value is a valid json key for hubuum
    ///
    /// We support only ASCII alphanumeric characters, underscores, `$`, and
    /// comma separators for nested keys. No spaces are allowed.
    ///
    /// ### Returns
    ///
    /// * A boolean
    fn is_valid_jsonb_search_key(&self) -> bool;

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

impl<T: AsRef<str>> CustomStringExtensions for T {
    fn as_permission(&self) -> Result<Permissions, ApiError> {
        Permissions::from_string(self.as_ref())
    }

    fn replace_question_mark_with_indexed_n(&self) -> String {
        let mut n = 1;
        let mut result = self.as_ref().to_string();
        while let Some(pos) = result.find('?') {
            // Replace '?' with '$n'
            result.replace_range(pos..pos + 1, &format!("${n}"));
            n += 1;
        }
        result
    }

    fn is_valid_jsonb_search_key(&self) -> bool {
        self.as_ref()
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ',' || c == '$')
    }

    fn as_integer(&self) -> Result<Vec<i32>, ApiError> {
        parse_integer_list(self.as_ref())
    }

    fn as_boolean(&self) -> Result<bool, ApiError> {
        match self.as_ref().to_lowercase().as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(ApiError::BadRequest(format!(
                "Invalid boolean value: '{}'",
                self.as_ref()
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
        self.as_ref()
            .split(',')
            .map(|part| part.trim())
            .map(|part| {
                DateTime::parse_from_rfc3339(part)
                    .map(|dt| dt.with_timezone(&Utc).naive_utc())
                    .or_else(|_| {
                        NaiveDate::parse_from_str(part, "%Y-%m-%d")
                            .map_err(|e| {
                                error!("Failed to parse date: {}", e);
                                ApiError::BadRequest(format!("Invalid date format: {}", part))
                            })?
                            .and_hms_opt(0, 0, 0)
                            .ok_or_else(|| {
                                ApiError::BadRequest(format!(
                                    "Failed to create time for date: {}",
                                    part
                                ))
                            })
                    })
            })
            .collect()
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
///     * "-90"
///     * "-6--2"
///
/// ### Returns
///
/// * A sorted vector of unique integers or ApiError::InvalidIntegerRange if the input is invalid
pub fn parse_integer_list(input: &str) -> Result<Vec<i32>, ApiError> {
    hubuum_query::parse_integer_list(input).map_err(|error| match error {
        hubuum_query::QueryError::BadRequest(message) => ApiError::BadRequest(message),
        hubuum_query::QueryError::InvalidIntegerRange(message) => {
            ApiError::InvalidIntegerRange(message)
        }
    })
}
