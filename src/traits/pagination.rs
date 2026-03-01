use serde::{Deserialize, Serialize};

use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum CursorValue {
    Null,
    Boolean(bool),
    Integer(i64),
    String(String),
    DateTime(chrono::NaiveDateTime),
    IntegerArray(Vec<i32>),
}

pub trait CursorPaginated: Clone {
    fn supports_sort(field: &FilterField) -> bool;
    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError>;
    fn default_sort() -> Vec<SortParam>;
    fn tie_breaker_sort() -> Vec<SortParam>;
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorSqlType {
    Boolean,
    Integer,
    String,
    DateTime,
    IntegerArray,
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
