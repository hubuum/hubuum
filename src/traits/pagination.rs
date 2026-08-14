pub use hubuum_query::CursorValue;

use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};

pub trait CursorPaginated {
    fn supports_sort(field: &FilterField) -> bool;
    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError>;
    fn default_sort() -> Vec<SortParam>;
    fn tie_breaker_sort() -> Vec<SortParam>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    use rstest::rstest;
    use serde_json::json;

    #[rstest]
    #[case(json!({"aa": 0}), json!({"b": 0}), Ordering::Less)]
    #[case(
        json!({"aa": 0, "z": 0}),
        json!({"b": 0, "zz": 0}),
        Ordering::Greater
    )]
    fn jsonb_object_comparison_matches_stored_key_iteration(
        #[case] left: serde_json::Value,
        #[case] right: serde_json::Value,
        #[case] expected: Ordering,
    ) {
        let left = CursorValue::Json(left);
        let right = CursorValue::Json(right);

        assert_eq!(left.cmp(&right), expected);
    }
}
