#[rstest::rstest]
#[tokio::test]
async fn computed_query_rejects_a_cursor_too_large_for_the_next_request(
    #[future(awt)] test_context: TestContext,
) {
    let mut fixture = fixture(&test_context, "computed sort cursor size").await;
    for (key, path) in [("first_large", "/first"), ("second_large", "/second")] {
        let request = serde_json::from_value(serde_json::json!({
            "key": key,
            "label": key,
            "operation": {"type": "first_non_null", "paths": [path]},
            "result_type": "string",
            "enabled": true
        }))
        .unwrap();
        create_shared_definition(
            &test_context.pool,
            fixture.class.id,
            fixture.class.collection_id,
            test_context.admin_user.id,
            request,
            &EventContext::system(),
        )
        .await
        .unwrap();
    }
    finish_active_rebuild(&test_context, fixture.class.id).await;

    for index in 0..2 {
        fixture.objects.push(
            NewHubuumObject {
                collection_id: fixture.class.collection_id,
                hubuum_class_id: fixture.class.id,
                name: test_context.scoped_name(&format!("large cursor {index}")),
                description: "Large computed cursor object".to_string(),
                data: serde_json::json!({
                    "first": format!("{index}{}", "a".repeat(50 * 1024)),
                    "second": "b".repeat(50 * 1024)
                }),
            }
            .save_without_events(&test_context.pool)
            .await
            .unwrap(),
        );
    }

    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/?sort=computed.shared.first_large.desc,computed.shared.second_large.desc&limit=1",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::BAD_REQUEST).await;
    let body: serde_json::Value = test::read_body_json(response).await;

    assert_eq!(
        body["message"],
        "pagination cursor exceeds the maximum encoded size of 65536 bytes; use smaller sort values"
    );

    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn shared_computed_query_rejects_a_cache_row_for_old_source_data(
    #[future(awt)] test_context: TestContext,
) {
    let mut fixture = fixture(&test_context, "computed sorting stale source hash").await;
    fixture.objects.push(
        NewHubuumObject {
            collection_id: fixture.class.collection_id,
            hubuum_class_id: fixture.class.id,
            name: test_context.scoped_name("beta computed sort"),
            description: "Computed sorting object".to_string(),
            data: serde_json::json!({"inventory": {"hostname": "beta.example"}}),
        }
        .save_without_events(&test_context.pool)
        .await
        .unwrap(),
    );
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
        definition("display_name"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;
    finish_active_rebuild(&test_context, fixture.class.id).await;

    with_connection(&test_context.pool, async |conn| {
        use crate::schema::hubuumobject::dsl::{data, hubuumobject, id};
        diesel::update(hubuumobject.filter(id.eq(fixture.objects[0].id)))
            .set(data.eq(serde_json::json!({
                "inventory": {"hostname": "aardvark.example"}
            })))
            .execute(conn)
            .await
    })
    .await
    .unwrap();

    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/?include=computed&sort=computed.shared.display_name&limit=1",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let page: Vec<serde_json::Value> = test::read_body_json(response).await;

    assert_eq!(page[0]["id"], fixture.objects[0].id);
    assert_eq!(
        page[0]["computed"]["shared"]["values"]["display_name"],
        "aardvark.example"
    );

    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[case::missing_key(serde_json::json!({}), serde_json::json!({}))]
#[case::wrong_type(
    serde_json::json!({"display_name": 42}),
    serde_json::json!({})
)]
#[case::extra_key(
    serde_json::json!({"display_name": "zulu-stale.example", "retired": "secret"}),
    serde_json::json!({})
)]
#[case::malformed_error(
    serde_json::json!({"display_name": "zulu-stale.example"}),
    serde_json::json!({"display_name": "invalid"})
)]
#[tokio::test]
async fn shared_computed_query_falls_back_for_an_invalid_fresh_materialization(
    #[future(awt)] test_context: TestContext,
    #[case] cached_values: serde_json::Value,
    #[case] cached_errors: serde_json::Value,
) {
    let mut fixture = fixture(&test_context, "computed sorting invalid materialization").await;
    let boundary = NewHubuumObject {
        collection_id: fixture.class.collection_id,
        hubuum_class_id: fixture.class.id,
        name: test_context.scoped_name("alpha computed sort"),
        description: "Computed sorting cursor boundary".to_string(),
        data: serde_json::json!({"inventory": {"hostname": "alpha.example"}}),
    }
    .save_without_events(&test_context.pool)
    .await
    .unwrap();
    fixture.objects.push(boundary.clone());
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
        definition("display_name"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;
    finish_active_rebuild(&test_context, fixture.class.id).await;

    with_connection(&test_context.pool, async |conn| {
        use crate::schema::object_computed_data::dsl::{
            errors, object_computed_data, object_id, values,
        };
        diesel::update(object_computed_data.filter(object_id.eq(boundary.id)))
            .set((values.eq(cached_values), errors.eq(cached_errors)))
            .execute(conn)
            .await
    })
    .await
    .unwrap();

    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/?include=computed&sort=computed.shared.display_name&limit=1",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let cursor = header_value(&response, NEXT_CURSOR_HEADER).expect("next cursor");
    let page: Vec<serde_json::Value> = test::read_body_json(response).await;

    assert_eq!(page[0]["id"], boundary.id);
    assert_eq!(
        page[0]["computed"]["shared"]["values"]["display_name"],
        "alpha.example"
    );
    assert_eq!(
        page[0]["computed"]["shared"]["values"]
            .as_object()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(page[0]["computed"]["shared"]["materialization_stale"], true);

    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/?sort=computed.shared.display_name&limit=1&cursor={cursor}",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let page: Vec<serde_json::Value> = test::read_body_json(response).await;

    assert_eq!(page.len(), 1);
    assert_eq!(page[0]["id"], fixture.objects[0].id);

    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/?computed.shared.display_name=alpha.example",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let filtered: Vec<serde_json::Value> = test::read_body_json(response).await;

    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0]["id"], boundary.id);

    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn computed_query_cursor_uses_the_resolved_definition_snapshot(
    #[future(awt)] test_context: TestContext,
) {
    let mut fixture = fixture(&test_context, "computed sorting definition snapshot").await;
    fixture.objects.push(
        NewHubuumObject {
            collection_id: fixture.class.collection_id,
            hubuum_class_id: fixture.class.id,
            name: test_context.scoped_name("snapshot second object"),
            description: "Computed sorting snapshot object".to_string(),
            data: serde_json::json!({"inventory": {"hostname": "second.example"}}),
        }
        .save_without_events(&test_context.pool)
        .await
        .unwrap(),
    );
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
        definition("display_name"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;
    finish_active_rebuild(&test_context, fixture.class.id).await;

    let mut params =
        parse_query_parameter("sort=computed.shared.display_name&limit=1&include_total=false")
            .unwrap();
    let snapshot = resolve_computed_query_fields(
        &test_context.pool,
        fixture.class.id,
        Some(test_context.admin_user.id),
        &mut params.filters,
        &mut params.sort,
    )
    .await
    .unwrap();
    with_connection(&test_context.pool, async |conn| {
        use crate::schema::computed_field_definitions::dsl::{
            class_id, computed_field_definitions,
        };
        diesel::delete(computed_field_definitions.filter(class_id.eq(fixture.class.id)))
            .execute(conn)
            .await
    })
    .await
    .unwrap();

    let enriched = enrich_objects_with_computed_query_snapshot(
        &test_context.pool,
        fixture.objects.clone(),
        Some(test_context.admin_user.id),
        &snapshot,
    )
    .await
    .unwrap();
    let page = finalize_page(enriched, &params).unwrap();

    assert_eq!(page.items.len(), 1);
    assert!(page.next_cursor.is_some());
    assert!(page.items[0].computed.shared.values["display_name"].is_string());

    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn numeric_computed_query_cursor_matches_domain_precision(
    #[future(awt)] test_context: TestContext,
) {
    let mut fixture = fixture(&test_context, "numeric computed sorting").await;
    for (index, numerator) in [1, 1, 2].into_iter().enumerate() {
        fixture.objects.push(
            NewHubuumObject {
                collection_id: fixture.class.collection_id,
                hubuum_class_id: fixture.class.id,
                name: test_context.scoped_name(&format!("numeric sort {index} {numerator}")),
                description: "Numeric computed sorting object".to_string(),
                data: serde_json::json!({"left": numerator, "middle": 0, "right": 0}),
            }
            .save_without_events(&test_context.pool)
            .await
            .unwrap(),
        );
    }
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
        serde_json::json!({
            "key": "average_value",
            "label": "Average value",
            "operation": {
                "type": "average",
                "paths": ["/left", "/middle", "/right"]
            },
            "result_type": "number"
        }),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;

    let expected = [
        fixture.objects[3].id,
        fixture.objects[1].id,
        fixture.objects[2].id,
        fixture.objects[0].id,
    ];
    let mut cursor = None;
    for expected_id in expected {
        let cursor_query = cursor
            .as_ref()
            .map_or_else(String::new, |cursor| format!("&cursor={cursor}"));
        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!(
                "/api/v1/classes/{}/?include=computed&sort=computed.shared.average_value.desc&limit=1{cursor_query}",
                fixture.class.id
            ),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        cursor = header_value(&response, NEXT_CURSOR_HEADER);
        let page: Vec<serde_json::Value> = test::read_body_json(response).await;
        assert_eq!(page[0]["id"], expected_id);
    }
    assert!(cursor.is_none());

    finish_active_rebuild(&test_context, fixture.class.id).await;
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn numeric_computed_query_cursor_preserves_a_power_boundary_aggregate(
    #[future(awt)] test_context: TestContext,
) {
    let mut fixture = fixture(&test_context, "numeric computed power boundary").await;
    for (index, source) in ["9", "9.999999999999999999999999999999999", "11"]
        .into_iter()
        .enumerate()
    {
        fixture.objects.push(
            NewHubuumObject {
                collection_id: fixture.class.collection_id,
                hubuum_class_id: fixture.class.id,
                name: test_context.scoped_name(&format!("boundary sort {index}")),
                description: "Numeric boundary sorting object".to_string(),
                data: serde_json::from_str(&format!(r#"{{"value":{source}}}"#)).unwrap(),
            }
            .save_without_events(&test_context.pool)
            .await
            .unwrap(),
        );
    }
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
        serde_json::json!({
            "key": "boundary_value",
            "label": "Boundary value",
            "operation": {"type": "sum", "paths": ["/value"]},
            "result_type": "number"
        }),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;

    let expected = [
        fixture.objects[0].id,
        fixture.objects[1].id,
        fixture.objects[2].id,
        fixture.objects[3].id,
    ];
    let mut cursor = None;
    for (index, expected_id) in expected.into_iter().enumerate() {
        let cursor_query = cursor
            .as_ref()
            .map_or_else(String::new, |cursor| format!("&cursor={cursor}"));
        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!(
                "/api/v1/classes/{}/?include=computed&sort=computed.shared.boundary_value&limit=1{cursor_query}",
                fixture.class.id
            ),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        cursor = header_value(&response, NEXT_CURSOR_HEADER);
        let page: Vec<serde_json::Value> = test::read_body_json(response).await;
        assert_eq!(page[0]["id"], expected_id);
        assert_eq!(cursor.is_some(), index < expected.len() - 1);
    }

    finish_active_rebuild(&test_context, fixture.class.id).await;
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn personal_computed_query_is_owner_scoped_and_numeric(
    #[future(awt)] test_context: TestContext,
) {
    let mut fixture = fixture(&test_context, "personal computed sorting").await;
    fixture.objects.push(
        NewHubuumObject {
            collection_id: fixture.class.collection_id,
            hubuum_class_id: fixture.class.id,
            name: test_context.scoped_name("one present"),
            description: "One present value".to_string(),
            data: serde_json::json!({"manual": {"hostname": "one.example"}}),
        }
        .save_without_events(&test_context.pool)
        .await
        .unwrap(),
    );
    fixture.objects.push(
        NewHubuumObject {
            collection_id: fixture.class.collection_id,
            hubuum_class_id: fixture.class.id,
            name: test_context.scoped_name("none present"),
            description: "No present values".to_string(),
            data: serde_json::json!({}),
        }
        .save_without_events(&test_context.pool)
        .await
        .unwrap(),
    );
    let group = grant_normal_user(
        &test_context,
        &fixture,
        &[
            Permissions::ReadClass,
            Permissions::ReadCollection,
            Permissions::ReadObject,
        ],
    )
    .await;
    let response = post_request(
        &test_context.pool,
        &test_context.normal_token,
        "/api/v1/iam/me/computed-fields",
        serde_json::json!({
            "class_id": fixture.class.id,
            "key": "my_present_count",
            "label": "My present count",
            "operation": {
                "type": "count_present",
                "paths": ["/inventory/hostname", "/manual/hostname"]
            },
            "result_type": "integer"
        }),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;

    let response = get_request(
        &test_context.pool,
        &test_context.normal_token,
        &format!(
            "/api/v1/classes/{}/?include=computed&sort=computed.personal.my_present_count&limit=3",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let objects: Vec<serde_json::Value> = test::read_body_json(response).await;
    assert_eq!(
        objects
            .iter()
            .map(|object| object["id"].as_i64().unwrap() as i32)
            .collect::<Vec<_>>(),
        vec![
            fixture.objects[2].id,
            fixture.objects[1].id,
            fixture.objects[0].id
        ]
    );
    assert_eq!(
        objects
            .iter()
            .map(|object| {
                object["computed"]["personal"]["values"]["my_present_count"]
                    .as_i64()
                    .unwrap()
            })
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );

    let response = get_request(
        &test_context.pool,
        &test_context.normal_token,
        &format!(
            "/api/v1/classes/{}/?computed.personal.my_present_count__gte=1&sort=id",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let filtered: Vec<serde_json::Value> = test::read_body_json(response).await;
    assert_eq!(
        filtered
            .iter()
            .map(|object| object["id"].as_i64().unwrap() as i32)
            .collect::<Vec<_>>(),
        vec![fixture.objects[0].id, fixture.objects[1].id]
    );

    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/?sort=computed.personal.my_present_count",
            fixture.class.id
        ),
    )
    .await;
    assert_response_status(response, StatusCode::BAD_REQUEST).await;

    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn raw_computed_sort_only_enriches_a_nonterminal_cursor_boundary(
    #[future(awt)] test_context: TestContext,
) {
    let mut fixture = fixture(&test_context, "computed sort query count").await;
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
        definition("display_name"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;
    finish_active_rebuild(&test_context, fixture.class.id).await;

    for index in 1..8 {
        fixture.objects.push(
            NewHubuumObject {
                collection_id: fixture.class.collection_id,
                hubuum_class_id: fixture.class.id,
                name: test_context.scoped_name(&format!("computed query count {index}")),
                description: "Computed sorting query-count object".to_string(),
                data: serde_json::json!({
                    "manual": {"hostname": format!("host-{index:02}.example")}
                }),
            }
            .save_without_events(&test_context.pool)
            .await
            .unwrap(),
        );
    }

    with_connection(&test_context.pool, async |conn| {
        use crate::schema::object_computed_data::dsl::{class_id, object_computed_data};
        diesel::delete(object_computed_data.filter(class_id.eq(fixture.class.id)))
            .execute(conn)
            .await
    })
    .await
    .unwrap();

    let small_endpoint = format!(
        "/api/v1/classes/{}/?include_total=false&sort=computed.shared.display_name&limit=1",
        fixture.class.id
    );
    let (small_response, small_queries) = capture_queries(get_request(
        &test_context.pool,
        &test_context.admin_token,
        &small_endpoint,
    ))
    .await;
    let small_response = assert_response_status(small_response, StatusCode::OK).await;
    assert!(header_value(&small_response, NEXT_CURSOR_HEADER).is_some());
    let small_page: Vec<serde_json::Value> = test::read_body_json(small_response).await;
    assert_eq!(small_page.len(), 1);

    let large_endpoint = format!(
        "/api/v1/classes/{}/?include_total=false&sort=computed.shared.display_name&limit=8",
        fixture.class.id
    );
    let (large_response, large_queries) = capture_queries(get_request(
        &test_context.pool,
        &test_context.admin_token,
        &large_endpoint,
    ))
    .await;
    let large_response = assert_response_status(large_response, StatusCode::OK).await;
    assert!(header_value(&large_response, NEXT_CURSOR_HEADER).is_none());
    let large_page: Vec<serde_json::Value> = test::read_body_json(large_response).await;
    assert_eq!(large_page.len(), 8);

    assert_eq!(
        small_queries.domain_queries(),
        large_queries.domain_queries() + 1
    );
    assert_eq!(
        small_queries.connection_checkouts(),
        large_queries.connection_checkouts() + 1
    );
    assert_eq!(
        large_queries.queries_matching("hubuum_computed_evaluate_scope"),
        1
    );
    assert_eq!(
        small_queries.queries_matching("hubuum_computed_evaluate_scope"),
        1
    );
    let materialized_count = with_connection(&test_context.pool, async |conn| {
        use crate::schema::object_computed_data::dsl::{class_id, object_computed_data};
        object_computed_data
            .filter(class_id.eq(fixture.class.id))
            .count()
            .get_result::<i64>(conn)
            .await
    })
    .await
    .unwrap();
    assert_eq!(materialized_count, 0);

    fixture.cleanup().await.unwrap();
}
use super::*;
