use super::*;

#[rstest::rstest]
#[case("name", 5)]
#[case("description", 3)]
#[case("collection_id", 1)]
#[case("created_at", 0)]
#[case("updated_at", 0)]
#[tokio::test]
async fn groups_each_allow_listed_scalar_field(
    #[future(awt)] test_context: TestContext,
    #[case] field: &str,
    #[case] expected_groups: usize,
) {
    let fixture = fixture(&test_context, &format!("scalar {field}")).await;
    let page = aggregate_rows(
        &test_context,
        &fixture,
        &test_context.admin_token,
        &format!("group_by={field}"),
    )
    .await;

    assert_eq!(summed_count(&page.rows), fixture.objects.len() as i64);
    if expected_groups > 0 {
        assert_eq!(page.rows.len(), expected_groups);
    }
    assert!(page.rows.iter().all(|row| {
        row["dimensions"][0]["field"] == field && row["dimensions"][0]["state"] == "value"
    }));
    assert_eq!(
        page.total_count.unwrap().parse::<usize>().unwrap(),
        page.rows.len()
    );

    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[case("collections")]
#[case("collection_id")]
#[tokio::test]
async fn collection_filter_aliases_apply_before_grouping(
    #[future(awt)] test_context: TestContext,
    #[case] filter: &str,
) {
    let fixture = fixture(&test_context, &format!("{filter} filter alias")).await;
    let page = aggregate_rows(
        &test_context,
        &fixture,
        &test_context.admin_token,
        &format!("{filter}=2147483647&group_by=name"),
    )
    .await;

    assert!(page.rows.is_empty());
    assert_eq!(page.total_count.as_deref(), Some("0"));

    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn nested_json_groups_preserve_json_types(#[future(awt)] test_context: TestContext) {
    let fixture = fixture(&test_context, "json types").await;
    let page = aggregate_rows(
        &test_context,
        &fixture,
        &test_context.admin_token,
        "group_by=json_data.typed",
    )
    .await;
    let values = page
        .rows
        .iter()
        .map(|row| &row["dimensions"][0]["value"])
        .collect::<Vec<_>>();

    assert!(values.iter().any(|value| value.is_string()));
    assert!(values.iter().any(|value| value.is_number()));
    assert!(values.iter().any(|value| value.is_boolean()));
    assert!(values.iter().any(|value| value.is_array()));
    assert!(values.iter().any(|value| value.is_object()));

    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn json_null_and_missing_path_are_distinct_buckets(#[future(awt)] test_context: TestContext) {
    let fixture = fixture(&test_context, "json states").await;
    let page = aggregate_rows(
        &test_context,
        &fixture,
        &test_context.admin_token,
        "group_by=json_data.nullable",
    )
    .await;
    let count_for = |state: &str| {
        page.rows
            .iter()
            .find(|row| row["dimensions"][0]["state"] == state)
            .map(|row| row["object_count"].as_i64().unwrap())
    };

    assert_eq!(count_for("value"), Some(1));
    assert_eq!(count_for("null"), Some(1));
    assert_eq!(count_for("missing"), Some(3));
    assert!(page.rows.iter().all(|row| {
        row["dimensions"][0]["state"] == "value" || row["dimensions"][0].get("value").is_none()
    }));

    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn filters_apply_before_multidimensional_grouping(#[future(awt)] test_context: TestContext) {
    let fixture = fixture(&test_context, "multidimensional filters").await;
    let page = aggregate_rows(
        &test_context,
        &fixture,
        &test_context.admin_token,
        "json_data__equals=status=active&group_by=description&group_by=json_data.location,country",
    )
    .await;

    assert_eq!(page.rows.len(), 3);
    assert_eq!(summed_count(&page.rows), 4);
    assert!(
        page.rows
            .iter()
            .all(|row| row["dimensions"].as_array().unwrap().len() == 2)
    );
    assert!(page.rows.iter().any(|row| row["object_count"] == 2));

    fixture.cleanup().await.unwrap();
}
