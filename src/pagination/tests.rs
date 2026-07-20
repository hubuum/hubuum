use chrono::NaiveDate;
use rstest::rstest;

use super::*;
use crate::models::{Collection, UserWithName};

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
        &QueryOptions {
            filters: vec![],
            sort: vec![],
            limit: Some(2),
            cursor: None,
            include_total: true,
        },
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

    let prepared_query = prepare_db_pagination::<Collection>(&QueryOptions {
        filters: vec![],
        sort: vec![],
        limit: Some(2),
        cursor: first_page.next_cursor.clone(),
        include_total: true,
    })
    .unwrap();

    let cursor_sql =
        cursor_filter_sql::<Collection>(&prepared_query.sort, prepared_query.cursor.as_deref())
            .unwrap();

    assert_eq!(cursor_sql, Some("((collections.id > 2))".to_string()));

    let second_page = finalize_page(
        vec![collection(3, "gamma")],
        &QueryOptions {
            filters: vec![],
            sort: vec![],
            limit: Some(2),
            cursor: first_page.next_cursor,
            include_total: true,
        },
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
        &QueryOptions {
            filters: vec![],
            sort: vec![SortParam {
                field: FilterField::Name,
                descending: true,
            }],
            limit: Some(2),
            cursor: None,
            include_total: true,
        },
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
        &QueryOptions {
            filters: vec![],
            sort: vec![SortParam {
                field: FilterField::Name,
                descending: false,
            }],
            limit: Some(1),
            cursor: None,
            include_total: true,
        },
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
        &QueryOptions {
            filters: vec![],
            sort: vec![SortParam {
                field: FilterField::JsonData,
                descending: false,
            }],
            limit: Some(1),
            cursor: None,
            include_total: true,
        },
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
    let prepared = prepare_db_pagination::<UserWithName>(&QueryOptions {
        filters: vec![],
        sort: vec![SortParam {
            field: FilterField::Username,
            descending: false,
        }],
        limit: None,
        cursor: None,
        include_total: true,
    })
    .unwrap();

    assert_eq!(prepared.limit, Some(DEFAULT_PAGE_LIMIT + 1));
    assert_eq!(prepared.sort.len(), 2);
    assert_eq!(prepared.sort[0].field, FilterField::Username);
    assert_eq!(prepared.sort[1].field, FilterField::Id);
}

#[tokio::test]
async fn exact_total_count_can_be_skipped() {
    let options = QueryOptions {
        filters: vec![],
        sort: vec![],
        limit: None,
        cursor: None,
        include_total: false,
    };
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
fn test_cursor_filter_sql_handles_nullable_descending_strings() {
    let sql = cursor_filter_sql::<UserWithName>(
        &[SortParam {
            field: FilterField::Email,
            descending: true,
        }],
        Some(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
                serde_json::to_vec(&CursorToken {
                    sorts: vec![CursorSort {
                        field: "email".to_string(),
                        descending: true,
                    }],
                    values: vec![CursorValue::String("b@example.com".to_string())],
                })
                .unwrap(),
            ),
        ),
    )
    .unwrap();

    assert_eq!(
        sql,
        Some("(((users.email < 'b@example.com' OR users.email IS NULL)))".to_string())
    );
}

fn encoded_cursor(sort: &SortParam, value: CursorValue) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&CursorToken {
            sorts: vec![CursorSort {
                field: sort.field.to_string(),
                descending: sort.descending,
            }],
            values: vec![value],
        })
        .unwrap(),
    )
}

#[test]
fn sql_cursor_filter_rejects_a_value_with_the_wrong_resolved_type() {
    let sort = SortParam {
        field: FilterField::Id,
        descending: false,
    };
    let fields = [CursorSqlField {
        column: "computed_value".to_string(),
        sql_type: CursorSqlType::Boolean,
        nullable: false,
    }];
    let cursor = encoded_cursor(&sort, CursorValue::String("true".to_string()));
    let query_options = QueryOptions {
        filters: vec![],
        sort: vec![sort],
        limit: Some(2),
        cursor: Some(cursor),
        include_total: true,
    };

    let error = cursor_filter_sql_for_fields(
        &query_options.sort,
        &fields,
        query_options.cursor.as_deref(),
    )
    .unwrap_err();

    assert_eq!(
        error,
        ApiError::BadRequest(
            "cursor value does not match expected type for 'computed_value'".to_string()
        )
    );
}

#[rstest]
#[case::nul_string(r#"{"value":"\u0000"}"#)]
#[case::nul_key(r#"{"\u0000":true}"#)]
#[case::integral_overflow(r#"{"value":1e131072}"#)]
#[case::fractional_overflow(r#"{"value":1e-16384}"#)]
fn sql_json_cursor_rejects_values_postgres_jsonb_cannot_represent(#[case] json: &str) {
    let sort = SortParam {
        field: FilterField::Id,
        descending: false,
    };
    let fields = [CursorSqlField {
        column: "computed_value".to_string(),
        sql_type: CursorSqlType::Json,
        nullable: false,
    }];
    let value = serde_json::from_str(json).unwrap();
    let cursor = encoded_cursor(&sort, CursorValue::Json(value));
    let query_options = QueryOptions {
        filters: vec![],
        sort: vec![sort],
        limit: Some(2),
        cursor: Some(cursor),
        include_total: true,
    };

    let error = cursor_filter_sql_for_fields(
        &query_options.sort,
        &fields,
        query_options.cursor.as_deref(),
    )
    .unwrap_err();

    assert_eq!(
        error,
        ApiError::BadRequest(
            "cursor contains JSON that PostgreSQL JSONB cannot represent".to_string()
        )
    );
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

#[test]
fn numeric_cursor_sql_uses_a_canonical_decimal_literal() {
    let sort = SortParam {
        field: FilterField::Id,
        descending: false,
    };
    let fields = [CursorSqlField {
        column: "computed_value".to_string(),
        sql_type: CursorSqlType::Numeric,
        nullable: false,
    }];
    let cursor = encoded_cursor(&sort, CursorValue::Decimal("1_".to_string()));

    let sql = cursor_filter_sql_for_fields(&[sort], &fields, Some(&cursor)).unwrap();

    assert_eq!(sql.as_deref(), Some("((computed_value > 1::numeric))"));
}

#[test]
fn numeric_cursor_sql_rejects_a_decimal_outside_evaluator_bounds() {
    let sort = SortParam {
        field: FilterField::Id,
        descending: false,
    };
    let fields = [CursorSqlField {
        column: "computed_value".to_string(),
        sql_type: CursorSqlType::Numeric,
        nullable: false,
    }];
    let cursor = encoded_cursor(&sort, CursorValue::Decimal("1e200000".to_string()));

    let error = cursor_filter_sql_for_fields(&[sort], &fields, Some(&cursor)).unwrap_err();

    assert_eq!(
        error.to_string(),
        "cursor contains an invalid decimal value"
    );
}

#[rstest]
#[case::nul_string(r#"{"value":"\u0000"}"#)]
#[case::nul_key(r#"{"\u0000":true}"#)]
#[case::integral_overflow(r#"{"value":1e131072}"#)]
#[case::fractional_overflow(r#"{"value":1e-16384}"#)]
fn json_cursor_sql_rejects_values_postgres_jsonb_cannot_represent(#[case] json: &str) {
    let sort = SortParam {
        field: FilterField::Id,
        descending: false,
    };
    let fields = [CursorSqlField {
        column: "computed_value".to_string(),
        sql_type: CursorSqlType::Json,
        nullable: false,
    }];
    let value = serde_json::from_str(json).unwrap();
    let cursor = encoded_cursor(&sort, CursorValue::Json(value));

    let error = cursor_filter_sql_for_fields(&[sort], &fields, Some(&cursor)).unwrap_err();

    assert_eq!(
        error,
        ApiError::BadRequest(
            "cursor contains JSON that PostgreSQL JSONB cannot represent".to_string()
        )
    );
}

#[rstest]
#[case::maximum_integral_digits(r#"{"value":1e131071}"#)]
#[case::normalized_maximum_integral_digits(r#"{"value":0.1e131072}"#)]
#[case::maximum_fractional_digits(r#"{"value":1e-16383}"#)]
fn json_cursor_sql_accepts_postgres_numeric_boundaries(#[case] json: &str) {
    let sort = SortParam {
        field: FilterField::Id,
        descending: false,
    };
    let fields = [CursorSqlField {
        column: "computed_value".to_string(),
        sql_type: CursorSqlType::Json,
        nullable: false,
    }];
    let value = serde_json::from_str(json).unwrap();
    let cursor = encoded_cursor(&sort, CursorValue::Json(value));

    let sql = cursor_filter_sql_for_fields(&[sort], &fields, Some(&cursor)).unwrap();

    assert!(sql.is_some());
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

#[test]
fn validate_page_limit_with_max_accepts_within_range() {
    assert_eq!(validate_page_limit_with_max(10, 100).unwrap(), 10);
    assert_eq!(validate_page_limit_with_max(100, 100).unwrap(), 100);
}

#[test]
fn validate_page_limit_with_max_rejects_zero() {
    let error = validate_page_limit_with_max(0, 100).unwrap_err();
    assert_eq!(error.to_string(), "limit must be greater than 0");
}

#[test]
fn validate_page_limit_with_max_rejects_zero_maximum() {
    let error = validate_page_limit_with_max(1, 0).unwrap_err();
    assert_eq!(error.to_string(), "max_page_limit must be greater than 0");
}

#[test]
fn validate_page_limit_with_max_clamps_above_maximum() {
    assert_eq!(validate_page_limit_with_max(101, 100).unwrap(), 100);
}
