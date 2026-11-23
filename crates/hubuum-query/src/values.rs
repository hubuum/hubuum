use super::{MAX_INTEGER_FILTER_VALUES, QueryError};
use std::collections::HashSet;

/// Parse a bounded comma-separated list of positive signed 64-bit values.
/// Revision filters deliberately do not accept range expansion syntax: range
/// comparisons use the `gt`/`gte`/`lt`/`lte`/`between` operators instead.
pub fn parse_positive_bigint_list_with_limit(
    input: &str,
    max_values: usize,
) -> Result<Vec<i64>, QueryError> {
    let mut values = Vec::new();
    for segment in input.split(',') {
        if segment.is_empty() {
            return Err(QueryError::BadRequest(
                "Revision filters cannot contain an empty value".to_string(),
            ));
        }
        if values.len() == max_values {
            return Err(QueryError::BadRequest(format!(
                "Revision filter contains more than {max_values} values"
            )));
        }
        let value = segment.parse::<i64>().map_err(|_| {
            QueryError::BadRequest(format!(
                "Invalid revision '{segment}': expected a positive int64"
            ))
        })?;
        if value <= 0 {
            return Err(QueryError::BadRequest(format!(
                "Invalid revision '{segment}': expected a positive int64"
            )));
        }
        values.push(value);
    }
    Ok(values)
}

pub fn parse_integer_list(input: &str) -> Result<Vec<i32>, QueryError> {
    parse_integer_list_with_limit(input, MAX_INTEGER_FILTER_VALUES)
}

/// Parse one boolean query value using the common case-insensitive grammar.
pub fn parse_boolean_value(input: &str) -> Result<bool, QueryError> {
    if input.eq_ignore_ascii_case("true") {
        Ok(true)
    } else if input.eq_ignore_ascii_case("false") {
        Ok(false)
    } else {
        Err(QueryError::BadRequest(format!(
            "Invalid boolean value: '{input}'"
        )))
    }
}

/// Parse a comma-separated list of RFC 3339 timestamps or UTC calendar dates.
pub fn parse_datetime_list(input: &str) -> Result<Vec<chrono::NaiveDateTime>, QueryError> {
    input
        .split(',')
        .map(str::trim)
        .map(|value| {
            if let Ok(timestamp) = chrono::DateTime::parse_from_rfc3339(value) {
                return Ok(timestamp.to_utc().naive_utc());
            }
            chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .map_err(|_| QueryError::BadRequest(format!("Invalid date format: {value}")))?
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| {
                    QueryError::BadRequest(format!("Failed to create time for date: {value}"))
                })
        })
        .collect()
}

/// Parse an integer list while enforcing a caller-provided unique-value cap.
///
/// The limit is checked during insertion rather than after collecting and
/// sorting, which keeps very large ranges and heavily duplicated inputs
/// bounded in both memory and work.
pub fn parse_integer_list_with_limit(
    input: &str,
    max_values: usize,
) -> Result<Vec<i32>, QueryError> {
    let mut numbers = IntegerAccumulator::new(max_values);

    for segment in input.split(',') {
        if let Some((start, negative_end)) = segment.split_once("--") {
            if negative_end.contains("--") {
                return Err(QueryError::InvalidIntegerRange(format!(
                    "Invalid format: '{segment}'"
                )));
            }
            let start = start.parse::<i32>().map_err(|_| {
                QueryError::InvalidIntegerRange(format!("Invalid start of range: '{start}'"))
            })?;
            let end = format!("-{negative_end}").parse::<i32>().map_err(|_| {
                QueryError::InvalidIntegerRange(format!("Invalid end of range: '{negative_end}'"))
            })?;
            insert_integer_range(&mut numbers, start, end, input, segment)?;
        } else if let Some(idx) = segment.find('-') {
            if idx == 0 {
                let value = segment.parse::<i32>().map_err(|_| {
                    QueryError::InvalidIntegerRange(format!("Invalid number: '{segment}'"))
                })?;
                insert_integer(&mut numbers, value, input)?;
            } else {
                let (start, end) = segment.split_at(idx);
                let end = &end[1..];
                let start = start.parse::<i32>().map_err(|_| {
                    QueryError::InvalidIntegerRange(format!("Invalid start of range: '{start}'"))
                })?;
                let end = end.parse::<i32>().map_err(|_| {
                    QueryError::InvalidIntegerRange(format!("Invalid end of range: '{end}'"))
                })?;
                insert_integer_range(&mut numbers, start, end, input, segment)?;
            }
        } else {
            let value = segment.parse::<i32>().map_err(|_| {
                QueryError::InvalidIntegerRange(format!("Invalid number: '{segment}'"))
            })?;
            insert_integer(&mut numbers, value, input)?;
        }
    }

    Ok(numbers.finish())
}

enum IntegerAccumulator {
    Values {
        values: Vec<i32>,
        max_values: usize,
    },
    Unique {
        values: HashSet<i32>,
        max_values: usize,
    },
}

impl IntegerAccumulator {
    fn new(max_values: usize) -> Self {
        Self::Values {
            values: Vec::new(),
            max_values,
        }
    }

    fn insert(&mut self, value: i32, input: &str) -> Result<(), QueryError> {
        if let Self::Values { values, max_values } = self {
            if values.len() < *max_values {
                values.push(value);
                return Ok(());
            }
            self.promote();
        }

        let Self::Unique { values, max_values } = self else {
            unreachable!("integer accumulator must be promoted")
        };
        values.insert(value);
        if values.len() > *max_values {
            return Err(integer_filter_limit_error(input, *max_values));
        }
        Ok(())
    }

    fn extend_range(&mut self, start: i32, end: i32, input: &str) -> Result<(), QueryError> {
        if let Self::Values { values, max_values } = self {
            let range_len = (i64::from(end) - i64::from(start) + 1) as u64;
            let available = max_values.saturating_sub(values.len()) as u64;
            if range_len <= available {
                values.extend(start..=end);
                return Ok(());
            }
            self.promote();
        }

        for value in start..=end {
            self.insert(value, input)?;
        }
        Ok(())
    }

    fn promote(&mut self) {
        let Self::Values { values, max_values } = self else {
            return;
        };
        let max_values = *max_values;
        let unique = std::mem::take(values).into_iter().collect();
        *self = Self::Unique {
            values: unique,
            max_values,
        };
    }

    fn finish(self) -> Vec<i32> {
        let mut values = match self {
            Self::Values { values, .. } => values,
            Self::Unique { values, .. } => values.into_iter().collect(),
        };
        values.sort_unstable();
        values.dedup();
        values
    }
}

fn insert_integer_range(
    numbers: &mut IntegerAccumulator,
    start: i32,
    end: i32,
    input: &str,
    segment: &str,
) -> Result<(), QueryError> {
    if start > end {
        return Err(QueryError::InvalidIntegerRange(format!(
            "Range start is greater than end: '{segment}'"
        )));
    }
    numbers.extend_range(start, end, input)
}

fn insert_integer(
    numbers: &mut IntegerAccumulator,
    value: i32,
    input: &str,
) -> Result<(), QueryError> {
    numbers.insert(value, input)
}

fn integer_filter_limit_error(input: &str, max_values: usize) -> QueryError {
    QueryError::InvalidIntegerRange(format!(
        "Integer filter '{input}' expands to more than {max_values} unique values"
    ))
}
