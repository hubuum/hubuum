use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};

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
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CursorValue {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Null, Self::Null) => Ordering::Equal,
            (Self::Integer(left), Self::Integer(right)) => left.cmp(right),
            (Self::Decimal(left), Self::Decimal(right)) => {
                hubuum_computed_fields::compare_decimal_strings(left, right)
                    .unwrap_or_else(|| left.cmp(right))
            }
            (Self::Boolean(left), Self::Boolean(right)) => left.cmp(right),
            (Self::String(left), Self::String(right)) => left.cmp(right),
            (Self::DateTime(left), Self::DateTime(right)) => left.cmp(right),
            (Self::IntegerArray(left), Self::IntegerArray(right)) => left.cmp(right),
            (Self::Json(left), Self::Json(right)) => compare_jsonb(left, right),
            _ => self.rank().cmp(&other.rank()),
        }
    }
}

fn compare_jsonb(left: &serde_json::Value, right: &serde_json::Value) -> Ordering {
    use serde_json::Value;

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
            hubuum_computed_fields::compare_decimal_strings(&left.to_string(), &right.to_string())
                .unwrap_or_else(|| left.to_string().cmp(&right.to_string()))
        }
        (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
        (Value::Array(left), Value::Array(right)) => left
            .len()
            .cmp(&right.len())
            .then_with(|| compare_jsonb_sequences(left, right)),
        (Value::Object(left), Value::Object(right)) => {
            let mut left = left.iter().collect::<Vec<_>>();
            let mut right = right.iter().collect::<Vec<_>>();
            // PostgreSQL compares object cardinality first, then key/value
            // pairs with keys in C-collation lexical order.
            let key_order =
                |(left, _): &(&String, &Value), (right, _): &(&String, &Value)| left.cmp(right);
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

fn compare_jsonb_sequences(left: &[serde_json::Value], right: &[serde_json::Value]) -> Ordering {
    left.iter()
        .zip(right.iter())
        .find_map(|(left, right)| {
            let ordering = compare_jsonb(left, right);
            (ordering != Ordering::Equal).then_some(ordering)
        })
        .unwrap_or(Ordering::Equal)
}

pub trait CursorPaginated: Clone {
    fn supports_sort(field: &FilterField) -> bool;
    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError>;
    fn default_sort() -> Vec<SortParam>;
    fn tie_breaker_sort() -> Vec<SortParam>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorSqlType {
    Integer,
    Numeric,
    Boolean,
    String,
    DateTime,
    IntegerArray,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CursorSqlField {
    pub column: &'static str,
    pub sql_type: CursorSqlType,
    pub nullable: bool,
}

pub trait CursorSqlMapping: CursorPaginated {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jsonb_object_comparison_uses_lexical_key_order() {
        let left = CursorValue::Json(serde_json::json!({"aa": 0}));
        let right = CursorValue::Json(serde_json::json!({"b": 0}));

        assert_eq!(left.cmp(&right), Ordering::Less);
    }
}
