use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use chrono::NaiveDate;
use rstest::rstest;

use super::*;
use crate::models::Collection;
use crate::models::search::FilterField;

struct UserCursorContract;

impl CursorPaginated for UserCursorContract {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(field, FilterField::Id | FilterField::Username)
    }

    fn cursor_value(&self, _field: &FilterField) -> Result<CursorValue, ApiError> {
        unreachable!("the pagination preparation contract does not inspect a row")
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Username,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }
}

#[derive(Clone, Debug)]
struct JsonCursorItem {
    id: i64,
    value: serde_json::Value,
}

impl CursorPaginated for JsonCursorItem {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(field, FilterField::Id | FilterField::JsonData)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        match field {
            FilterField::Id => Ok(CursorValue::Integer(self.id)),
            FilterField::JsonData => Ok(CursorValue::Json(self.value.clone())),
            _ => Err(ApiError::InternalServerError(
                "unsupported test cursor field".to_string(),
            )),
        }
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct NonCloneCursorItem(i64);

impl CursorPaginated for NonCloneCursorItem {
    fn supports_sort(field: &FilterField) -> bool {
        field == &FilterField::Id
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        match field {
            FilterField::Id => Ok(CursorValue::Integer(self.0)),
            _ => Err(ApiError::InternalServerError(
                "unsupported non-clone cursor field".to_string(),
            )),
        }
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}

#[test]
fn in_memory_pagination_accepts_non_clone_rows() {
    let rows = paginate_in_memory(
        vec![
            NonCloneCursorItem(3),
            NonCloneCursorItem(1),
            NonCloneCursorItem(2),
        ],
        &QueryOptions::new(Vec::new(), Vec::new(), Some(2), None, false).unwrap(),
    )
    .unwrap();

    assert_eq!(rows, vec![NonCloneCursorItem(1), NonCloneCursorItem(2)]);
}

fn collection(id: i32, name: &str) -> Collection {
    Collection {
        id,
        name: name.to_string(),
        description: format!("collection {id}"),
        created_at: NaiveDate::from_ymd_opt(2024, 1, id as u32)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        updated_at: NaiveDate::from_ymd_opt(2024, 1, id as u32)
            .unwrap()
            .and_hms_opt(1, 0, 0)
            .unwrap(),
        parent_collection_id: None,
        revision: crate::models::ResourceRevision::INITIAL,
    }
}

#[test]
fn test_paginate_collections_with_cursor() {
    let collections = vec![
        collection(1, "alpha"),
        collection(2, "beta"),
        collection(3, "gamma"),
    ];

    let first_page = finalize_page(
        collections.clone(),
        &QueryOptions::new(vec![], vec![], Some(2), None, true).unwrap(),
    )
    .unwrap();

    assert_eq!(
        first_page
            .items
            .iter()
            .map(|item| item.id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert!(first_page.next_cursor.is_some());

    let second_page = finalize_page(
        vec![collection(3, "gamma")],
        &QueryOptions::new(vec![], vec![], Some(2), first_page.next_cursor, true).unwrap(),
    )
    .unwrap();

    assert_eq!(
        second_page
            .items
            .iter()
            .map(|item| item.id)
            .collect::<Vec<_>>(),
        vec![3]
    );
    assert!(second_page.next_cursor.is_none());
}

#[test]
fn test_paginate_collections_descending() {
    let collections = vec![
        collection(3, "gamma"),
        collection(2, "beta"),
        collection(1, "alpha"),
    ];

    let page = finalize_page(
        collections,
        &QueryOptions::new(
            vec![],
            vec![SortParam {
                field: FilterField::Name,
                descending: true,
            }],
            Some(2),
            None,
            true,
        )
        .unwrap(),
    )
    .unwrap();

    assert_eq!(
        page.items
            .iter()
            .map(|item| item.name.clone())
            .collect::<Vec<_>>(),
        vec!["gamma".to_string(), "beta".to_string()]
    );
    assert!(page.next_cursor.is_some());
}

#[test]
fn cursor_encoding_rejects_an_oversized_token() {
    let error = finalize_page(
        vec![
            collection(1, &"a".repeat(MAX_ENCODED_CURSOR_BYTES)),
            collection(2, "z"),
        ],
        &QueryOptions::new(
            vec![],
            vec![SortParam {
                field: FilterField::Name,
                descending: false,
            }],
            Some(1),
            None,
            true,
        )
        .unwrap(),
    )
    .unwrap_err();

    assert_eq!(
        error.to_string(),
        format!(
            "pagination cursor exceeds the maximum encoded size of {MAX_ENCODED_CURSOR_BYTES} bytes; use smaller sort values"
        )
    );
}

#[test]
fn cursor_encoding_rejects_a_string_with_an_embedded_nul() {
    let error = finalize_page(
        vec![collection(1, "a\0b"), collection(2, "z")],
        &QueryOptions::new(
            vec![],
            vec![SortParam {
                field: FilterField::Name,
                descending: false,
            }],
            Some(1),
            None,
            true,
        )
        .unwrap(),
    )
    .unwrap_err();

    assert_eq!(
        error,
        ApiError::BadRequest(
            "cursor string values cannot contain an embedded NUL byte".to_string()
        )
    );
}

#[test]
fn cursor_encoding_rejects_json_that_decoding_would_reject() {
    let error = finalize_page(
        vec![
            JsonCursorItem {
                id: 1,
                value: nested_json_arrays(MAX_JSON_CURSOR_NESTING_DEPTH + 1),
            },
            JsonCursorItem {
                id: 2,
                value: serde_json::json!([]),
            },
        ],
        &QueryOptions::new(
            vec![],
            vec![SortParam {
                field: FilterField::JsonData,
                descending: false,
            }],
            Some(1),
            None,
            true,
        )
        .unwrap(),
    )
    .unwrap_err();

    assert_eq!(
        error,
        ApiError::BadRequest(format!(
            "cursor JSON exceeds the maximum nesting depth of {MAX_JSON_CURSOR_NESTING_DEPTH}"
        ))
    );
}

#[test]
fn cursor_decoding_rejects_an_oversized_token_before_parsing() {
    let sort = SortParam {
        field: FilterField::Id,
        descending: false,
    };

    let error =
        decode_cursor_values(&"a".repeat(MAX_ENCODED_CURSOR_BYTES + 1), &[sort]).unwrap_err();

    assert_eq!(
        error.to_string(),
        format!(
            "pagination cursor exceeds the maximum encoded size of {MAX_ENCODED_CURSOR_BYTES} bytes; use smaller sort values"
        )
    );
}

#[test]
fn test_prepare_db_pagination_adds_limit_and_tie_breaker() {
    let prepared = prepare_db_pagination::<UserCursorContract>(
        &QueryOptions::new(
            vec![],
            vec![SortParam {
                field: FilterField::Username,
                descending: false,
            }],
            None,
            None,
            true,
        )
        .unwrap(),
    )
    .unwrap();

    assert_eq!(prepared.limit(), Some(DEFAULT_PAGE_LIMIT + 1));
    assert_eq!(prepared.sort().len(), 2);
    assert_eq!(prepared.sort()[0].field, FilterField::Username);
    assert_eq!(prepared.sort()[1].field, FilterField::Id);
}

#[tokio::test]
async fn exact_total_count_can_be_skipped() {
    let options = QueryOptions::new(vec![], vec![], None, None, false).unwrap();
    let count = exact_count_or_skipped(&options, async || {
        panic!("count query must not execute when include_total is false")
    })
    .await
    .unwrap();
    assert_eq!(count, SKIPPED_TOTAL_COUNT);

    let headers = pagination_headers(&None, count, 25);
    assert!(!headers.contains_key(TOTAL_COUNT_HEADER));
    assert_eq!(headers.get(PAGE_LIMIT_HEADER), Some(&"25".to_string()));
}

#[test]
fn cursor_decoding_rejects_a_mismatched_value_count() {
    let sort = SortParam {
        field: FilterField::Id,
        descending: false,
    };
    let cursor = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&CursorToken {
            sorts: vec![CursorSort {
                field: sort.field.to_string(),
                descending: sort.descending,
            }],
            values: vec![],
        })
        .unwrap(),
    );

    let error = decode_cursor_values(&cursor, &[sort]).unwrap_err();

    assert_eq!(
        error,
        ApiError::BadRequest("cursor value count does not match current sort order".to_string())
    );
}

fn nested_json_arrays(depth: usize) -> serde_json::Value {
    (0..depth).fold(serde_json::Value::Null, |value, _| {
        serde_json::Value::Array(vec![value])
    })
}

#[test]
fn json_cursor_accepts_the_maximum_nesting_depth() {
    let value = nested_json_arrays(MAX_JSON_CURSOR_NESTING_DEPTH);

    validate_postgres_jsonb_cursor_value(&value).unwrap();
}

#[test]
fn json_cursor_rejects_nesting_above_the_maximum() {
    let value = nested_json_arrays(MAX_JSON_CURSOR_NESTING_DEPTH + 1);

    let error = validate_postgres_jsonb_cursor_value(&value).unwrap_err();

    assert_eq!(
        error,
        ApiError::BadRequest(format!(
            "cursor JSON exceeds the maximum nesting depth of {MAX_JSON_CURSOR_NESTING_DEPTH}"
        ))
    );
}

#[rstest]
#[case(None, 10)]
#[case(Some(1), 1)]
#[case(Some(100), 100)]
#[case(Some(101), 100)]
fn page_limits_resolve_defaults_and_clamp(
    #[case] requested: Option<usize>,
    #[case] expected: usize,
) {
    let limits = PageLimits::new(10, 100).unwrap();

    assert_eq!(limits.resolve(requested).unwrap(), expected);
}

#[rstest]
#[case(0, 100, "default_page_limit must be greater than 0")]
#[case(10, 0, "max_page_limit must be greater than 0")]
#[case(
    101,
    100,
    "default_page_limit (101) must be less than or equal to max_page_limit (100)"
)]
fn page_limits_reject_invalid_invariants(
    #[case] default: usize,
    #[case] maximum: usize,
    #[case] expected: &str,
) {
    let error = PageLimits::new(default, maximum).unwrap_err();

    assert_eq!(error.to_string(), expected);
}

#[test]
fn page_limits_reject_a_zero_requested_limit() {
    let error = PageLimits::new(10, 100)
        .unwrap()
        .resolve(Some(0))
        .unwrap_err();

    assert_eq!(error.to_string(), "limit must be greater than 0");
}

#[derive(Clone, Debug)]
struct InstrumentedCursorItem {
    id: i64,
    calls: Option<Arc<AtomicUsize>>,
    fail: bool,
}

impl CursorPaginated for InstrumentedCursorItem {
    fn supports_sort(field: &FilterField) -> bool {
        field == &FilterField::Id
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        match field {
            FilterField::Id if self.fail => Err(ApiError::InternalServerError(
                "failed to extract test cursor value".to_string(),
            )),
            FilterField::Id => {
                if let Some(calls) = &self.calls {
                    calls.fetch_add(1, AtomicOrdering::Relaxed);
                }
                Ok(CursorValue::Integer(self.id))
            }
            _ => Err(ApiError::InternalServerError(
                "unsupported instrumented cursor field".to_string(),
            )),
        }
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}

#[test]
fn in_memory_pagination_extracts_each_sort_key_once() {
    let calls = Arc::new(AtomicUsize::new(0));
    let items = (0..128)
        .rev()
        .map(|id| InstrumentedCursorItem {
            id,
            calls: Some(Arc::clone(&calls)),
            fail: false,
        })
        .collect();

    paginate_in_memory(
        items,
        &QueryOptions::new(Vec::new(), Vec::new(), None, None, false).unwrap(),
    )
    .unwrap();

    assert_eq!(calls.load(AtomicOrdering::Relaxed), 128);
}

#[test]
fn in_memory_pagination_propagates_sort_key_errors() {
    let error = paginate_in_memory(
        vec![
            InstrumentedCursorItem {
                id: 2,
                calls: None,
                fail: false,
            },
            InstrumentedCursorItem {
                id: 1,
                calls: None,
                fail: true,
            },
        ],
        &QueryOptions::new(Vec::new(), Vec::new(), None, None, false).unwrap(),
    )
    .unwrap_err();

    assert_eq!(
        error,
        ApiError::InternalServerError("failed to extract test cursor value".to_string())
    );
}
