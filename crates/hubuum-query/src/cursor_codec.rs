use super::{MAX_ENCODED_CURSOR_BYTES, MAX_QUERY_SORT_FIELDS, SortParam};
use base64::Engine as _;
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

/// Backend-neutral value stored in an opaque cursor token.
///
/// Storage adapters decide how each variant maps to their native ordering
/// expressions. The token codec validates framing, sort identity, bounded
/// size, strings, and decimal syntax without depending on a database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum CursorValue {
    Null,
    Integer(i64),
    Decimal(String),
    Boolean(bool),
    String(String),
    DateTime(chrono::NaiveDateTime),
    IntegerArray(Vec<i32>),
    Json(serde_json::Value),
}

impl CursorValue {
    const fn rank(&self) -> u8 {
        match self {
            Self::Null => 0,
            Self::Integer(_) => 1,
            Self::Decimal(_) => 2,
            Self::Boolean(_) => 3,
            Self::String(_) => 4,
            Self::DateTime(_) => 5,
            Self::IntegerArray(_) => 6,
            Self::Json(_) => 7,
        }
    }
}

impl PartialOrd for CursorValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CursorValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (self, other) {
            (Self::Null, Self::Null) => Ordering::Equal,
            (Self::Integer(left), Self::Integer(right)) => left.cmp(right),
            (Self::Decimal(left), Self::Decimal(right)) => compare_decimal_strings(left, right),
            (Self::Boolean(left), Self::Boolean(right)) => left.cmp(right),
            (Self::String(left), Self::String(right)) => left.cmp(right),
            (Self::DateTime(left), Self::DateTime(right)) => left.cmp(right),
            (Self::IntegerArray(left), Self::IntegerArray(right)) => left.cmp(right),
            (Self::Json(left), Self::Json(right)) => compare_jsonb(left, right),
            _ => self.rank().cmp(&other.rank()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CursorCodecError {
    Invalid(String),
    Encoding(String),
}

impl fmt::Display for CursorCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) | Self::Encoding(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for CursorCodecError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CursorToken {
    sorts: Vec<CursorSort>,
    values: Vec<CursorValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CursorSort {
    field: String,
    descending: bool,
}

/// Encode values using the stable cursor token shared by applications and
/// storage adapters.
pub fn encode_cursor_values(
    sorts: &[SortParam],
    values: Vec<CursorValue>,
) -> Result<String, CursorCodecError> {
    if values.len() != sorts.len() {
        return Err(CursorCodecError::Encoding(
            "cursor value count does not match current sort order".to_string(),
        ));
    }
    validate_cursor_values(&values)?;
    let token = CursorToken {
        sorts: cursor_sorts(sorts),
        values,
    };
    let bytes = serde_json::to_vec(&token).map_err(|error| {
        CursorCodecError::Encoding(format!("failed to serialize cursor: {error}"))
    })?;
    let encoded_length = bytes.len().saturating_mul(4).saturating_add(2) / 3;
    if encoded_length > MAX_ENCODED_CURSOR_BYTES {
        return Err(cursor_too_large());
    }
    let cursor = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    ensure_cursor_within_limit(&cursor)?;
    Ok(cursor)
}

/// Decode and validate a cursor against the exact current sort order.
pub fn decode_cursor_values(
    cursor: &str,
    sorts: &[SortParam],
) -> Result<Vec<CursorValue>, CursorCodecError> {
    ensure_cursor_within_limit(cursor)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|error| CursorCodecError::Invalid(format!("invalid cursor: {error}")))?;
    let mut token: CursorToken = serde_json::from_slice(&bytes)
        .map_err(|error| CursorCodecError::Invalid(format!("invalid cursor: {error}")))?;

    if token.sorts != cursor_sorts(sorts) {
        return Err(CursorCodecError::Invalid(
            "cursor does not match current sort order".to_string(),
        ));
    }
    if token.values.len() != sorts.len() {
        return Err(CursorCodecError::Invalid(
            "cursor value count does not match current sort order".to_string(),
        ));
    }
    normalize_cursor_values(&mut token.values)?;
    Ok(token.values)
}

fn normalize_cursor_values(values: &mut [CursorValue]) -> Result<(), CursorCodecError> {
    for value in &mut *values {
        if let CursorValue::Decimal(source) = value {
            *source = canonical_decimal_string(source).ok_or_else(|| {
                CursorCodecError::Invalid("cursor contains an invalid decimal value".to_string())
            })?;
        }
    }
    validate_cursor_values(values)
}

fn cursor_sorts(sorts: &[SortParam]) -> Vec<CursorSort> {
    sorts
        .iter()
        .map(|sort| CursorSort {
            field: sort.field.to_string(),
            descending: sort.descending,
        })
        .collect()
}

fn validate_cursor_values(values: &[CursorValue]) -> Result<(), CursorCodecError> {
    for value in values {
        if let CursorValue::String(value) = value
            && value.contains('\0')
        {
            return Err(CursorCodecError::Invalid(
                "cursor string values cannot contain an embedded NUL byte".to_string(),
            ));
        }
    }
    Ok(())
}

fn ensure_cursor_within_limit(cursor: &str) -> Result<(), CursorCodecError> {
    if cursor.len() > MAX_ENCODED_CURSOR_BYTES {
        return Err(cursor_too_large());
    }
    Ok(())
}

fn cursor_too_large() -> CursorCodecError {
    CursorCodecError::Invalid(format!(
        "pagination cursor exceeds the maximum encoded size of {MAX_ENCODED_CURSOR_BYTES} bytes; use smaller sort values"
    ))
}

fn canonical_decimal_string(source: &str) -> Option<String> {
    const MAX_DECIMAL_SOURCE_BYTES: usize = 512;
    const MAX_DECIMAL_SIGNIFICANT_DIGITS: usize = 34;
    const MIN_DECIMAL_EXPONENT: i64 = -308;
    const MAX_DECIMAL_EXPONENT: i64 = 308;

    if source.len() > MAX_DECIMAL_SOURCE_BYTES {
        return None;
    }
    let value = BigDecimal::from_str(source).ok()?.normalized();
    let (integer, scale) = value.as_bigint_and_exponent();
    let digits = integer.to_string().trim_start_matches('-').len();
    let exponent = if integer == 0.into() {
        0
    } else {
        digits as i64 - scale - 1
    };
    if digits > MAX_DECIMAL_SIGNIFICANT_DIGITS
        || !(MIN_DECIMAL_EXPONENT..=MAX_DECIMAL_EXPONENT).contains(&exponent)
    {
        return None;
    }
    Some(value.to_string())
}

fn compare_decimal_strings(left: &str, right: &str) -> std::cmp::Ordering {
    match (BigDecimal::from_str(left), BigDecimal::from_str(right)) {
        (Ok(left), Ok(right)) => left.cmp(&right),
        _ => left.cmp(right),
    }
}

fn compare_jsonb(left: &serde_json::Value, right: &serde_json::Value) -> std::cmp::Ordering {
    use serde_json::Value;
    use std::cmp::Ordering;

    let rank = |value: &Value| match value {
        Value::Null => 0,
        Value::String(_) => 1,
        Value::Number(_) => 2,
        Value::Bool(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    };
    let rank_order = rank(left).cmp(&rank(right));
    if rank_order != Ordering::Equal {
        return rank_order;
    }
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::String(left), Value::String(right)) => left.cmp(right),
        (Value::Number(left), Value::Number(right)) => {
            compare_decimal_strings(&left.to_string(), &right.to_string())
        }
        (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
        (Value::Array(left), Value::Array(right)) => left
            .len()
            .cmp(&right.len())
            .then_with(|| compare_jsonb_sequences(left, right)),
        (Value::Object(left), Value::Object(right)) => {
            let mut left = left.iter().collect::<Vec<_>>();
            let mut right = right.iter().collect::<Vec<_>>();
            let key_order = |(left, _): &(&String, &Value), (right, _): &(&String, &Value)| {
                left.len().cmp(&right.len()).then_with(|| left.cmp(right))
            };
            left.sort_by(key_order);
            right.sort_by(key_order);
            left.len().cmp(&right.len()).then_with(|| {
                left.iter()
                    .zip(right.iter())
                    .find_map(|((left_key, left_value), (right_key, right_value))| {
                        let ordering = left_key
                            .cmp(right_key)
                            .then_with(|| compare_jsonb(left_value, right_value));
                        (ordering != Ordering::Equal).then_some(ordering)
                    })
                    .unwrap_or(Ordering::Equal)
            })
        }
        _ => Ordering::Equal,
    }
}

fn compare_jsonb_sequences(
    left: &[serde_json::Value],
    right: &[serde_json::Value],
) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    left.iter()
        .zip(right.iter())
        .find_map(|(left, right)| {
            let ordering = compare_jsonb(left, right);
            (ordering != Ordering::Equal).then_some(ordering)
        })
        .unwrap_or(Ordering::Equal)
}

/// A validated position captured from storage, without HTTP token framing or
/// size limits. Keep it typed while walking a bounded candidate set; only a
/// client-visible cursor needs to pass through the token codec.
#[derive(Clone, PartialEq, Eq)]
pub struct QueryContinuation {
    sorts: Vec<CursorSort>,
    values: Vec<CursorValue>,
}

impl fmt::Debug for QueryContinuation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryContinuation")
            .field("sorts", &self.sorts)
            .field("value_count", &self.values.len())
            .finish()
    }
}

impl QueryContinuation {
    pub fn new(
        sorts: &[SortParam],
        mut values: Vec<CursorValue>,
    ) -> Result<Self, CursorCodecError> {
        if sorts.is_empty()
            || sorts.len() > MAX_QUERY_SORT_FIELDS + 1
            || sorts.len() != values.len()
        {
            return Err(CursorCodecError::Invalid(
                "continuation values must match a bounded, nonempty sort order".to_string(),
            ));
        }
        normalize_cursor_values(&mut values)?;
        Ok(Self {
            sorts: cursor_sorts(sorts),
            values,
        })
    }

    pub(super) fn values_for(
        &self,
        sorts: &[SortParam],
    ) -> Result<&[CursorValue], CursorCodecError> {
        if self.sorts != cursor_sorts(sorts) {
            return Err(CursorCodecError::Invalid(
                "continuation does not match current sort order".to_string(),
            ));
        }
        Ok(&self.values)
    }
}
