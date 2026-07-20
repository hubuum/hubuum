use super::*;

fn cursor_budget() -> ObjectAggregateCursorBudget {
    ObjectAggregateCursorBudget::for_request_target(
        "/api/v1/classes/1/object-aggregates",
        "group_by=name",
    )
    .unwrap()
}

fn encoded_cursor(dimension: &str, sort_key: serde_json::Value, object_count: i64) -> String {
    let token = ObjectAggregateCursorToken {
        version: 1,
        dimensions: vec![dimension.to_string()],
        sort: ObjectAggregateSort::DimensionsAscending,
        sort_key,
        object_count,
    };
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(&token).unwrap())
}

#[test]
fn parses_ordered_multidimensional_group_query() {
    let query = parse_object_aggregate_query(
        "json_data__equals=status=active&group_by=json_data.location,country&group_by=computed.shared.lifecycle&sort=object_count.desc&limit=50",
    )
    .unwrap();
    let (options, spec) = query.into_parts();
    assert_eq!(options.filters.len(), 1);
    assert_eq!(options.limit, Some(50));
    assert_eq!(spec.sort(), ObjectAggregateSort::ObjectCountDescending);
    assert_eq!(
        spec.dimension_names(),
        vec![
            "json_data.location,country".to_string(),
            "computed.shared.lifecycle".to_string()
        ]
    );
}

#[test]
fn rejects_missing_group_dimensions() {
    let error = parse_object_aggregate_query("").unwrap_err();
    assert!(error.to_string().contains("between 1 and 3"));
}

#[test]
fn rejects_more_than_three_group_dimensions() {
    let error = parse_object_aggregate_query(
        "group_by=name&group_by=description&group_by=collection_id&group_by=created_at",
    )
    .unwrap_err();
    assert!(error.to_string().contains("between 1 and 3"));
}

#[test]
fn rejects_empty_or_malformed_json_paths() {
    for query in [
        "group_by=json_data.",
        "group_by=json_data.location,,country",
        "group_by=json_data.location%20country",
    ] {
        let error = parse_object_aggregate_query(query).unwrap_err();
        assert!(error.to_string().contains("JSON path"));
    }
}

#[test]
fn rejects_object_list_sort_fields() {
    let error = parse_object_aggregate_query("group_by=name&order_by=created_at.desc").unwrap_err();
    assert!(error.to_string().contains("Object-list sort fields"));
}

#[test]
fn cursor_is_bound_to_dimension_and_sort_spec() {
    let first = ObjectAggregateSpec::new(
        vec![ObjectAggregateDimension::from_str("name").unwrap()],
        ObjectAggregateSort::DimensionsAscending,
    )
    .unwrap();
    let second = ObjectAggregateSpec::new(
        vec![ObjectAggregateDimension::from_str("description").unwrap()],
        ObjectAggregateSort::DimensionsAscending,
    )
    .unwrap();
    let row = ObjectAggregateRow::from_database(
        serde_json::json!([{"field": "name", "state": "value", "value": "a"}]),
        1,
        serde_json::json!([[0, "a"]]),
    )
    .unwrap();
    let budget = cursor_budget();
    let cursor = first.encode_cursor(&row, budget).unwrap();
    let error = second.decode_cursor(&cursor, budget).unwrap_err();
    assert!(error.to_string().contains("does not match"));
}

#[rstest::rstest]
#[case(0)]
#[case(-1)]
fn rejects_non_positive_object_counts(#[case] object_count: i64) {
    let error = ObjectAggregateRow::from_database(
        serde_json::json!([{"field": "name", "state": "value", "value": "a"}]),
        object_count,
        serde_json::json!([[0, "a"]]),
    )
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("invalid object aggregate ordering data")
    );
}

#[test]
fn refuses_to_emit_an_unreplayable_group_cursor() {
    let spec = ObjectAggregateSpec::new(
        vec![ObjectAggregateDimension::from_str("json_data.large").unwrap()],
        ObjectAggregateSort::DimensionsAscending,
    )
    .unwrap();
    let large_value = "x".repeat(MAX_OBJECT_AGGREGATE_CURSOR_LENGTH);
    let row = ObjectAggregateRow::from_database(
        serde_json::json!([{
            "field": "json_data.large",
            "state": "value",
            "value": large_value.clone(),
        }]),
        1,
        serde_json::json!([[0, large_value]]),
    )
    .unwrap();

    let error = spec.encode_cursor(&row, cursor_budget()).unwrap_err();

    assert!(matches!(error, ApiError::PayloadTooLarge(_)));
    assert!(error.to_string().contains("replay-safe limit"));
}

#[test]
fn cursor_budget_accounts_for_the_complete_replay_target() {
    let short = ObjectAggregateCursorBudget::for_request_target(
        "/api/v1/classes/1/object-aggregates",
        "group_by=name",
    )
    .unwrap();
    let long = ObjectAggregateCursorBudget::for_request_target(
        "/api/v1/classes/1/object-aggregates",
        &format!("name__contains={}&group_by=name", "x".repeat(5_000)),
    )
    .unwrap();

    assert!(long.max_encoded_bytes() < short.max_encoded_bytes());
    assert_eq!(
        MAX_OBJECT_AGGREGATE_CURSOR_LENGTH
            + NEXT_CURSOR_HEADER_PREFIX.len()
            + HTTP_LINE_TERMINATOR.len(),
        COMMON_HTTP_LINE_LIMIT_BYTES
    );
}

#[test]
fn existing_cursor_does_not_reduce_its_replacement_budget() {
    let without_cursor = ObjectAggregateCursorBudget::for_request_target(
        "/api/v1/classes/1/object-aggregates",
        "group_by=name&limit=1",
    )
    .unwrap();
    let with_cursor = ObjectAggregateCursorBudget::for_request_target(
        "/api/v1/classes/1/object-aggregates",
        "group_by=name&cursor=opaque&limit=1",
    )
    .unwrap();

    assert_eq!(with_cursor, without_cursor);
}

#[test]
fn cursor_emission_uses_the_request_specific_budget() {
    let spec = ObjectAggregateSpec::new(
        vec![ObjectAggregateDimension::from_str("json_data.large").unwrap()],
        ObjectAggregateSort::DimensionsAscending,
    )
    .unwrap();
    let boundary_value = "x".repeat(1_000);
    let row = ObjectAggregateRow::from_database(
        serde_json::json!([{
            "field": "json_data.large",
            "state": "value",
            "value": boundary_value.clone(),
        }]),
        1,
        serde_json::json!([[0, boundary_value]]),
    )
    .unwrap();
    let budget = ObjectAggregateCursorBudget::for_request_target(
        "/api/v1/classes/1/object-aggregates",
        &format!(
            "name__contains={}&group_by=json_data.large",
            "x".repeat(7_000)
        ),
    )
    .unwrap();

    let error = spec.encode_cursor(&row, budget).unwrap_err();

    assert!(matches!(error, ApiError::PayloadTooLarge(_)));
    assert!(error.to_string().contains("for this request"));
}

#[test]
fn rejects_an_oversized_group_cursor_before_decoding() {
    let spec = ObjectAggregateSpec::new(
        vec![ObjectAggregateDimension::from_str("name").unwrap()],
        ObjectAggregateSort::DimensionsAscending,
    )
    .unwrap();

    let error = spec
        .decode_cursor(
            &"a".repeat(MAX_OBJECT_AGGREGATE_CURSOR_LENGTH + 1),
            cursor_budget(),
        )
        .unwrap_err();

    assert!(matches!(error, ApiError::PayloadTooLarge(_)));
    assert!(error.to_string().contains("replay-safe limit"));
}

#[rstest::rstest]
#[case(serde_json::json!([null]), 1)]
#[case(serde_json::json!([[0]]), 1)]
#[case(serde_json::json!([[0, null]]), 1)]
#[case(serde_json::json!([[1, null]]), 1)]
#[case(serde_json::json!([[4, "value"]]), 1)]
#[case(serde_json::json!([[0, "value"]]), 0)]
fn rejects_cursor_with_invalid_ordering_values(
    #[case] sort_key: serde_json::Value,
    #[case] object_count: i64,
) {
    let spec = ObjectAggregateSpec::new(
        vec![ObjectAggregateDimension::from_str("name").unwrap()],
        ObjectAggregateSort::DimensionsAscending,
    )
    .unwrap();

    let error = spec
        .decode_cursor(
            &encoded_cursor("name", sort_key, object_count),
            cursor_budget(),
        )
        .unwrap_err();

    assert!(error.to_string().contains("invalid ordering values"));
}

#[rstest::rstest]
#[case("name", serde_json::json!(42))]
#[case("description", serde_json::json!(false))]
#[case("collection_id", serde_json::json!("42"))]
#[case("collection_id", serde_json::json!(0))]
#[case("created_at", serde_json::json!(true))]
#[case("updated_at", serde_json::json!("not-a-timestamp"))]
fn rejects_cursor_with_wrong_scalar_value_type(
    #[case] dimension: &str,
    #[case] value: serde_json::Value,
) {
    let spec = ObjectAggregateSpec::new(
        vec![ObjectAggregateDimension::from_str(dimension).unwrap()],
        ObjectAggregateSort::DimensionsAscending,
    )
    .unwrap();

    let error = spec
        .decode_cursor(
            &encoded_cursor(dimension, serde_json::json!([[0, value]]), 1),
            cursor_budget(),
        )
        .unwrap_err();

    assert!(error.to_string().contains("invalid ordering values"));
}

#[rstest::rstest]
#[case("name", serde_json::json!("router"))]
#[case("description", serde_json::json!("edge device"))]
#[case("collection_id", serde_json::json!(42))]
#[case("created_at", serde_json::json!("2026-07-20T12:34:56.123456"))]
#[case("updated_at", serde_json::json!("2026-07-20T12:34:56"))]
fn accepts_cursor_with_correct_scalar_value_type(
    #[case] dimension: &str,
    #[case] value: serde_json::Value,
) {
    let spec = ObjectAggregateSpec::new(
        vec![ObjectAggregateDimension::from_str(dimension).unwrap()],
        ObjectAggregateSort::DimensionsAscending,
    )
    .unwrap();

    spec.decode_cursor(
        &encoded_cursor(dimension, serde_json::json!([[0, value]]), 1),
        cursor_budget(),
    )
    .unwrap();
}

#[test]
fn parses_computed_source_filters() {
    let query = parse_object_aggregate_query(
        "computed.shared.lifecycle__equals=active&group_by=description",
    )
    .unwrap();

    assert!(query.has_computed_filter());
    assert!(!query.has_personal_computed_filter());
    assert!(query.uses_computed_values());
}

#[rstest::rstest]
#[case(vec![Permissions::ReadObject])]
#[case(vec![Permissions::ReadCollection])]
fn aggregate_authorization_requires_object_and_collection_access(
    #[case] permissions: Vec<Permissions>,
) {
    let error = ObjectAggregateAuthorization::new(permissions, None)
        .err()
        .expect("incomplete authorization must fail");

    assert!(error.to_string().contains("ReadObject and ReadCollection"));
}
