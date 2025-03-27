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
            result.replace_range(pos..pos + 1, &format!("${}", n));
            n += 1;
        }
        result
    }

    fn is_valid_jsonb_search_key(&self) -> bool {
        self.as_ref()
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == ',' || c == '$')
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
                            .map(|date| date.and_hms_opt(0, 0, 0).unwrap())
                            .map_err(Into::<ApiError>::into)
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
    let mut numbers = Vec::new();

    // Split the input string on commas to handle individual numbers or ranges separately.
    for segment in input.split(',') {
        // Identify and handle ranges.
        // For negative ranges, like "-4--2", ensure they are parsed correctly.
        if segment.contains("--") {
            let parts: Vec<&str> = segment.split("--").collect();
            if parts.len() != 2 {
                return Err(ApiError::InvalidIntegerRange(format!(
                    "Invalid format: '{}'",
                    segment
                )));
            }
            let start = parts[0].parse::<i32>().map_err(|_| {
                ApiError::InvalidIntegerRange(format!("Invalid start of range: '{}'", parts[0]))
            })?;
            let end = format!("-{}", parts[1]).parse::<i32>().map_err(|_| {
                ApiError::InvalidIntegerRange(format!("Invalid end of range: '{}'", parts[1]))
            })?;
            if start > end {
                return Err(ApiError::InvalidIntegerRange(format!(
                    "Range start is greater than end: '{}'",
                    segment
                )));
            }
            numbers.extend(start..=end);
        } else if let Some(idx) = segment.find('-') {
            if idx == 0 {
                // It's a negative number, not a range.
                numbers.push(segment.parse::<i32>().map_err(|_| {
                    ApiError::InvalidIntegerRange(format!("Invalid number: '{}'", segment))
                })?);
            } else {
                // It's a positive range.
                let (start, end) = segment.split_at(idx);
                let end = &end[1..]; // Skip the hyphen
                let start = start.parse::<i32>().map_err(|_| {
                    ApiError::InvalidIntegerRange(format!("Invalid start of range: '{}'", start))
                })?;
                let end = end.parse::<i32>().map_err(|_| {
                    ApiError::InvalidIntegerRange(format!("Invalid end of range: '{}'", end))
                })?;
                if start > end {
                    return Err(ApiError::InvalidIntegerRange(format!(
                        "Range start is greater than end: '{}'",
                        segment
                    )));
                }
                numbers.extend(start..=end);
            }
        } else {
            // Handle a single number.
            numbers.push(segment.parse::<i32>().map_err(|_| {
                ApiError::InvalidIntegerRange(format!("Invalid number: '{}'", segment))
            })?);
        }
    }

    numbers.sort_unstable();
    numbers.dedup();
    Ok(numbers)
}
