use super::*;

#[test]
fn parses_ordered_multidimensional_group_query() {
    let query = parse_object_aggregate_query(
        "json_data__equals=status=active&group_by=json_data.location,country&group_by=computed.shared.lifecycle&sort=object_count.desc&limit=50",
    )
    .unwrap();
    let (options, spec) = query.into_parts();
    assert_eq!(options.filters().len(), 1);
    assert_eq!(options.limit(), Some(50));
    assert_eq!(spec.sort(), ObjectAggregateSort::ObjectCountDescending);
    assert_eq!(
        spec.dimensions()
            .iter()
            .map(ObjectAggregateDimension::canonical)
            .collect::<Vec<_>>(),
        vec![
            "json_data.location,country".to_string(),
            "computed.shared.lifecycle".to_string()
        ]
    );
}

#[test]
fn rejects_missing_group_dimensions() {
    let error = parse_object_aggregate_query("").unwrap_err();
    assert!(
        error
            .to_string()
            .contains("group_by dimension or aggregate")
    );
}

#[test]
fn rejects_more_than_three_group_dimensions() {
    let error = parse_object_aggregate_query(
        "group_by=name&group_by=description&group_by=collection_id&group_by=created_at",
    )
    .unwrap_err();
    assert!(error.to_string().contains("at most 3"));
}

#[test]
fn parses_global_numeric_measures_without_grouping() {
    let query = parse_object_aggregate_query(
        "aggregate=sum:json_data.cost&aggregate=average:computed.shared.score",
    )
    .unwrap();

    assert!(query.spec().dimensions().is_empty());
    assert_eq!(query.spec().measures().len(), 2);
    assert_eq!(query.spec().measures()[0].canonical(), "sum:json_data.cost");
    assert!(query.uses_computed_values());
}

#[test]
fn rejects_duplicate_or_excessive_measures() {
    let duplicate =
        parse_object_aggregate_query("aggregate=sum:json_data.cost&aggregate=sum:json_data.cost")
            .unwrap_err();
    assert!(duplicate.to_string().contains("Duplicate"));

    let excessive = parse_object_aggregate_query(
        "aggregate=sum:json_data.a&aggregate=sum:json_data.b&aggregate=sum:json_data.c&aggregate=sum:json_data.d&aggregate=sum:json_data.e",
    )
    .unwrap_err();
    assert!(excessive.to_string().contains("at most 4"));
}

#[rstest::rstest]
#[case("aggregate=count:json_data.cost")]
#[case("aggregate=sum:name")]
#[case("aggregate=sum:json_data.cost%27%29%3BDROP%20TABLE%20hubuumobject%3B--")]
#[case("aggregate=sum:computed.shared.score%27%3BSELECT%20pg_sleep%2810%29%3B--")]
#[case("group_by=json_data.cost%5C%27%3BDROP%20TABLE%20hubuumobject%3B--")]
fn rejects_invalid_or_injection_shaped_aggregate_syntax(#[case] query: &str) {
    assert!(parse_object_aggregate_query(query).is_err());
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
fn database_measure_values_gain_typed_request_metadata() {
    let spec = ObjectAggregateSpec::with_measures(
        Vec::new(),
        vec![ObjectAggregateMeasure::from_str("average:json_data.cost").unwrap()],
        ObjectAggregateSort::DimensionsAscending,
    )
    .unwrap();
    let row = ObjectAggregateRow::from_database(
        &spec,
        serde_json::json!([{
            "state": "value",
            "value_count": 2,
            "skipped_count": 1,
            "value": 12.5
        }]),
        3,
        serde_json::json!([]),
    )
    .unwrap();

    assert!(row.dimensions().is_empty());
    assert_eq!(row.measures()[0].field(), "json_data.cost");
    assert_eq!(
        row.measures()[0].operation(),
        ObjectAggregateMeasureOperation::Average
    );
    assert_eq!(row.measures()[0].value(), Some(&serde_json::json!(12.5)));
}

#[rstest::rstest]
#[case(0)]
#[case(-1)]
fn rejects_non_positive_object_counts(#[case] object_count: i64) {
    let spec = ObjectAggregateSpec::new(
        vec![ObjectAggregateDimension::from_str("name").unwrap()],
        ObjectAggregateSort::DimensionsAscending,
    )
    .unwrap();
    let error = ObjectAggregateRow::from_database(
        &spec,
        serde_json::json!([]),
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
