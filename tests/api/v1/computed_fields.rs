#[cfg(test)]
mod tests {
    use crate::db::prelude::*;
    use actix_web::{http::StatusCode, test};

    use crate::db::traits::computed_field::{
        class_computation_state_for, create_personal_definition, create_shared_definition,
        enrich_objects_with_computed_sort_snapshot, execute_computed_reindex_task,
        request_class_rebuild, resolve_computed_sort_fields, source_data_sha256,
    };
    use crate::db::traits::task::recover_expired_task_leases;
    use crate::db::{capture_queries, with_connection};
    use crate::events::EventContext;
    use crate::models::search::parse_query_parameter;
    use crate::models::{
        ComputedFieldDefinitionRequest, HubuumClassID, NewHubuumClass, NewHubuumObject,
        Permissions, TaskID, TaskStatus,
    };
    use crate::pagination::{NEXT_CURSOR_HEADER, TOTAL_COUNT_HEADER, finalize_page};
    use crate::tests::api_operations::{get_request, patch_request, post_request};
    use crate::tests::asserts::{assert_response_status, header_value};
    use crate::tests::{
        TestContext, create_test_group, create_test_service_account, service_account_token,
        test_context,
    };
    use crate::traits::{CanDelete, CanSave, PermissionController, SelfAccessors};

    #[derive(QueryableByName)]
    struct ComputedSortSqlValue {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Jsonb>)]
        value: Option<serde_json::Value>,
    }

    #[derive(QueryableByName)]
    struct ComputedSourceHashSqlValue {
        #[diesel(sql_type = diesel::sql_types::Text)]
        value: String,
    }

    async fn evaluate_sql_scope(
        context: &TestContext,
        data: &serde_json::Value,
        definitions: &[hubuum_computed_fields::Definition],
    ) -> serde_json::Value {
        let definitions = serde_json::to_value(definitions).unwrap();
        with_connection(&context.pool, async |conn| {
            diesel::sql_query("SELECT hubuum_computed_evaluate_scope($1, $2) AS value")
                .bind::<diesel::sql_types::Jsonb, _>(data)
                .bind::<diesel::sql_types::Jsonb, _>(&definitions)
                .get_result::<ComputedSortSqlValue>(conn)
                .await
        })
        .await
        .unwrap()
        .value
        .expect("scope evaluator always returns JSON")
    }

    fn evaluator_definition(value: serde_json::Value) -> hubuum_computed_fields::Definition {
        serde_json::from_value(value).unwrap()
    }

    fn definition(key: &str) -> serde_json::Value {
        serde_json::json!({
            "key": key,
            "label": "Display name",
            "description": "",
            "operation": {
                "type": "first_non_null",
                "paths": ["/inventory/hostname", "/manual/hostname"]
            },
            "result_type": "string",
            "enabled": true
        })
    }

    fn definition_request(key: &str) -> ComputedFieldDefinitionRequest {
        serde_json::from_value(definition(key)).unwrap()
    }

    async fn fixture(context: &TestContext, label: &str) -> crate::tests::ObjectFixture {
        context
            .object_fixture(
                label,
                NewHubuumClass {
                    collection_id: 0,
                    name: context.scoped_name("computed class"),
                    description: "Computed field test class".to_string(),
                    json_schema: None,
                    validate_schema: Some(false),
                },
                vec![NewHubuumObject {
                    collection_id: 0,
                    hubuum_class_id: 0,
                    name: context.scoped_name("computed object"),
                    description: "Computed field test object".to_string(),
                    data: serde_json::json!({
                        "inventory": {"hostname": "inventory.example"},
                        "manual": {"hostname": "manual.example"}
                    }),
                }],
            )
            .await
            .unwrap()
    }

    async fn active_rebuild_task(
        context: &TestContext,
        class_id: i32,
    ) -> crate::models::TaskRecord {
        for _ in 0..50 {
            let state = class_computation_state_for(&context.pool, class_id)
                .await
                .unwrap();
            let task_id = match state.active_task_id {
                Some(task_id) => task_id,
                None => {
                    let class = HubuumClassID::new(class_id)
                        .unwrap()
                        .instance(&context.pool)
                        .await
                        .unwrap();
                    request_class_rebuild(
                        &context.pool,
                        class_id,
                        class.collection_id,
                        Some(context.admin_user.id),
                    )
                    .await
                    .unwrap()
                    .active_task_id
                    .expect("manual rebuild task")
                }
            };
            if let Ok(task) = TaskID::new(task_id).unwrap().instance(&context.pool).await {
                return task;
            }
            tokio::task::yield_now().await;
        }
        panic!("could not load an active computed-field rebuild task");
    }

    async fn finish_active_rebuild(context: &TestContext, class_id: i32) {
        for _ in 0..50 {
            let state = class_computation_state_for(&context.pool, class_id)
                .await
                .unwrap();
            if state.active_task_id.is_none() && state.rebuild_status == "ready" {
                return;
            }
            let task = active_rebuild_task(context, class_id).await;
            let _ = execute_computed_reindex_task(&context.pool, &task).await;
            tokio::task::yield_now().await;
        }
        panic!("computed-field rebuild did not reach ready state");
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn database_sort_evaluator_matches_the_domain_operation_catalog(
        #[future(awt)] test_context: TestContext,
    ) {
        let cases = [
            (
                serde_json::json!({"type": "first_non_null", "paths": ["/missing", "/value"]}),
                "string",
                serde_json::json!({"value": "chosen"}),
            ),
            (
                serde_json::json!({"type": "sum", "paths": ["/left", "/right"]}),
                "number",
                serde_json::json!({"left": 2.25, "right": 3.75}),
            ),
            (
                serde_json::json!({"type": "sum", "paths": ["/left", "/right"]}),
                "number",
                serde_json::from_str(r#"{"left": 9999999999999999999999999999999999, "right": 6}"#)
                    .unwrap(),
            ),
            (
                serde_json::json!({"type": "average", "paths": ["/left", "/right"]}),
                "number",
                serde_json::json!({"left": 2, "right": 5}),
            ),
            (
                serde_json::json!({"type": "average", "paths": ["/left", "/right", "/third"]}),
                "number",
                serde_json::json!({"left": 1, "right": 0, "third": 0}),
            ),
            (
                serde_json::json!({"type": "min", "paths": ["/left", "/right"]}),
                "integer",
                serde_json::json!({"left": -4, "right": 9}),
            ),
            (
                serde_json::json!({"type": "max", "paths": ["/left", "/right"]}),
                "integer",
                serde_json::json!({"left": -4, "right": 9}),
            ),
            (
                serde_json::json!({"type": "all_present", "paths": ["/false", "/zero"]}),
                "boolean",
                serde_json::json!({"false": false, "zero": 0}),
            ),
            (
                serde_json::json!({"type": "any_present", "paths": ["/missing", "/empty"]}),
                "boolean",
                serde_json::json!({"empty": ""}),
            ),
            (
                serde_json::json!({"type": "count_present", "paths": ["/missing", "/null", "/array"]}),
                "integer",
                serde_json::json!({"null": null, "array": []}),
            ),
            (
                serde_json::json!({"type": "all_present_and_equal", "paths": ["/left", "/right"]}),
                "boolean",
                serde_json::json!({"left": {"a": 1, "b": [2]}, "right": {"b": [2], "a": 1.0}}),
            ),
            (
                serde_json::json!({"type": "first_non_null", "paths": ["/value"]}),
                "object",
                serde_json::json!({"value": {"nested": true}}),
            ),
            (
                serde_json::json!({"type": "first_non_null", "paths": ["/value"]}),
                "array",
                serde_json::json!({"value": [3, 2, 1]}),
            ),
            (
                serde_json::json!({"type": "sum", "paths": ["/left", "/right"]}),
                "number",
                serde_json::json!({"left": 2, "right": "invalid"}),
            ),
        ];

        for (operation, result_type, data) in cases {
            let definition: hubuum_computed_fields::Definition =
                serde_json::from_value(serde_json::json!({
                    "key": "sort_value",
                    "label": "Sort value",
                    "operation": operation.clone(),
                    "result_type": result_type,
                    "enabled": true
                }))
                .unwrap();
            let expected = hubuum_computed_fields::evaluate(
                &data,
                &[definition],
                1,
                hubuum_computed_fields::EvaluationLimits::standard(),
            )
            .unwrap()
            .values
            .remove("sort_value")
            .unwrap();
            let actual = with_connection(&test_context.pool, async |conn| {
                diesel::sql_query("SELECT hubuum_computed_sort_value($1, $2, $3) AS value")
                    .bind::<diesel::sql_types::Jsonb, _>(&data)
                    .bind::<diesel::sql_types::Jsonb, _>(&operation)
                    .bind::<diesel::sql_types::Text, _>(result_type)
                    .get_result::<ComputedSortSqlValue>(conn)
                    .await
            })
            .await
            .unwrap()
            .value
            .unwrap_or(serde_json::Value::Null);
            match (actual.as_number(), expected.as_number()) {
                (Some(actual), Some(expected)) => assert_eq!(
                    hubuum_computed_fields::compare_decimal_strings(
                        &actual.to_string(),
                        &expected.to_string()
                    ),
                    Some(std::cmp::Ordering::Equal),
                    "operation: {operation}, data: {data}, actual: {actual}, expected: {expected}"
                ),
                _ => assert_eq!(actual, expected, "operation: {operation}, data: {data}"),
            }
        }
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn database_scope_evaluator_preserves_the_empty_pointer_token(
        #[future(awt)] test_context: TestContext,
    ) {
        let definition = evaluator_definition(serde_json::json!({
            "key": "empty_key",
            "label": "Empty key",
            "operation": {"type": "first_non_null", "paths": ["/"]},
            "result_type": "string"
        }));
        let data = serde_json::json!({"": "selected"});
        let actual = evaluate_sql_scope(&test_context, &data, &[definition]).await;

        assert_eq!(actual["values"]["empty_key"], "selected");
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn database_source_hash_matches_the_domain_canonical_json(
        #[future(awt)] test_context: TestContext,
    ) {
        let data = serde_json::json!({
            "z": [1, {"quote": "'\"", "unicode": "blåbær 🫐"}],
            "a": {"nested": true, "control": "line\nbreak"}
        });
        let expected = source_data_sha256(&data).unwrap();

        let actual = with_connection(&test_context.pool, async |conn| {
            diesel::sql_query("SELECT hubuum_computed_source_sha256($1) AS value")
                .bind::<diesel::sql_types::Jsonb, _>(&data)
                .get_result::<ComputedSourceHashSqlValue>(conn)
                .await
        })
        .await
        .unwrap();

        assert_eq!(actual.value, expected);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn database_scope_evaluator_measures_compact_json_input(
        #[future(awt)] test_context: TestContext,
    ) {
        let definition = evaluator_definition(serde_json::json!({
            "key": "present",
            "label": "Present",
            "operation": {"type": "count_present", "paths": ["/value"]},
            "result_type": "integer"
        }));
        let data = serde_json::json!({
            "value": "x".repeat(hubuum_computed_fields::MAX_INPUT_BYTES - 12)
        });
        assert_eq!(
            serde_json::to_vec(&data).unwrap().len(),
            hubuum_computed_fields::MAX_INPUT_BYTES
        );

        let actual = evaluate_sql_scope(&test_context, &data, &[definition]).await;

        assert_eq!(actual["values"]["present"], 1);
        assert_eq!(actual["errors"], serde_json::json!({}));
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn database_scope_evaluator_preserves_scope_output_limits(
        #[future(awt)] test_context: TestContext,
    ) {
        let definitions = (0..5)
            .map(|index| {
                evaluator_definition(serde_json::json!({
                    "key": format!("value_{index}"),
                    "label": format!("Value {index}"),
                    "operation": {"type": "first_non_null", "paths": ["/value"]},
                    "result_type": "string"
                }))
            })
            .collect::<Vec<_>>();
        let data = serde_json::json!({"value": "x".repeat(60_000)});
        let expected = hubuum_computed_fields::evaluate(
            &data,
            &definitions,
            definitions.len(),
            hubuum_computed_fields::EvaluationLimits::standard(),
        )
        .unwrap();

        let actual = evaluate_sql_scope(&test_context, &data, &definitions).await;

        assert_eq!(actual, serde_json::to_value(expected).unwrap());
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn database_scope_evaluator_preserves_scope_work_limits(
        #[future(awt)] test_context: TestContext,
    ) {
        let definitions = (0..9)
            .map(|index| {
                evaluator_definition(serde_json::json!({
                    "key": format!("equal_{index}"),
                    "label": format!("Equal {index}"),
                    "operation": {
                        "type": "all_present_and_equal",
                        "paths": ["/left", "/right"]
                    },
                    "result_type": "boolean"
                }))
            })
            .collect::<Vec<_>>();
        let values = (0..6_000).collect::<Vec<_>>();
        let data = serde_json::json!({"left": values.clone(), "right": values});
        let expected = hubuum_computed_fields::evaluate(
            &data,
            &definitions,
            definitions.len(),
            hubuum_computed_fields::EvaluationLimits::standard(),
        )
        .unwrap();

        let actual = evaluate_sql_scope(&test_context, &data, &definitions).await;

        assert_eq!(actual, serde_json::to_value(expected).unwrap());
    }

    async fn grant_normal_user(
        context: &TestContext,
        fixture: &crate::tests::ObjectFixture,
        permissions: &[Permissions],
    ) -> crate::models::Group {
        let group = create_test_group(&context.pool).await;
        group
            .add_member_without_events(&context.pool, &context.normal_user)
            .await
            .unwrap();
        for permission in permissions {
            fixture
                .collection
                .collection
                .grant_one(&context.pool, group.id, *permission)
                .await
                .unwrap();
        }
        group
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn shared_definition_live_fallback_repairs_stale_materialization(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "shared fallback").await;
        let endpoint = format!("/api/v1/classes/{}/computed-fields", fixture.class.id);
        let response = post_request(
            &test_context.pool,
            &test_context.admin_token,
            &endpoint,
            definition("display_name"),
        )
        .await;
        let response = assert_response_status(response, StatusCode::CREATED).await;
        let mutation: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(mutation["state"]["rebuild_status"], "rebuilding");
        assert!(mutation["state"]["active_task_id"].is_number());

        finish_active_rebuild(&test_context, fixture.class.id).await;
        with_connection(&test_context.pool, async |conn| {
            use crate::schema::object_computed_data::dsl::{object_computed_data, object_id};

            diesel::delete(object_computed_data.filter(object_id.eq(fixture.objects[0].id)))
                .execute(conn)
                .await
        })
        .await
        .unwrap();

        let object_endpoint = format!(
            "/api/v1/classes/{}/{}?include=computed",
            fixture.class.id, fixture.objects[0].id
        );
        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &object_endpoint,
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let first: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(
            first["computed"]["shared"]["values"]["display_name"],
            "inventory.example"
        );
        assert_eq!(first["computed"]["shared"]["materialization_stale"], true);

        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &object_endpoint,
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let repaired: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(
            repaired["computed"]["shared"]["materialization_stale"],
            false
        );

        fixture.cleanup().await.unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn object_writes_materialize_the_current_shared_revision(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "write materialization").await;
        let endpoint = format!("/api/v1/classes/{}/computed-fields", fixture.class.id);
        let response = post_request(
            &test_context.pool,
            &test_context.admin_token,
            &endpoint,
            definition("display_name"),
        )
        .await;
        assert_response_status(response, StatusCode::CREATED).await;

        let object_endpoint = format!(
            "/api/v1/classes/{}/{}",
            fixture.class.id, fixture.objects[0].id
        );
        let response = patch_request(
            &test_context.pool,
            &test_context.admin_token,
            &object_endpoint,
            serde_json::json!({
                "data": {"manual": {"hostname": "updated.example"}}
            }),
        )
        .await;
        assert_response_status(response, StatusCode::OK).await;

        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!("{object_endpoint}?include=computed"),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let object: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(
            object["computed"]["shared"]["values"]["display_name"],
            "updated.example"
        );
        assert_eq!(object["computed"]["shared"]["materialization_stale"], false);

        finish_active_rebuild(&test_context, fixture.class.id).await;
        fixture.cleanup().await.unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn personal_definitions_are_separate_from_shared_values(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "personal values").await;
        let response = post_request(
            &test_context.pool,
            &test_context.admin_token,
            "/api/v1/iam/me/computed-fields",
            serde_json::json!({
                "class_id": fixture.class.id,
                "key": "my_name",
                "label": "My name",
                "operation": {
                    "type": "first_non_null",
                    "paths": ["/manual/hostname"]
                },
                "result_type": "string",
                "enabled": true
            }),
        )
        .await;
        let response = assert_response_status(response, StatusCode::CREATED).await;
        let created: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(created["visibility"], "personal");

        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!(
                "/api/v1/classes/{}/{}?include=computed",
                fixture.class.id, fixture.objects[0].id
            ),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let object: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(
            object["computed"]["personal"]["values"]["my_name"],
            "manual.example"
        );
        assert_eq!(
            object["computed"]["shared"]["values"],
            serde_json::json!({})
        );

        fixture.cleanup().await.unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn computed_input_is_rejected_even_when_null(#[future(awt)] test_context: TestContext) {
        let fixture = fixture(&test_context, "computed input rejection").await;
        let response = post_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!("/api/v1/classes/{}/", fixture.class.id),
            serde_json::json!({
                "name": "rejected",
                "collection_id": fixture.class.collection_id,
                "hubuum_class_id": fixture.class.id,
                "data": {},
                "description": "rejected",
                "computed": null
            }),
        )
        .await;
        assert_response_status(response, StatusCode::BAD_REQUEST).await;

        fixture.cleanup().await.unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn shared_patch_requires_the_current_definition_revision(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "revision conflict").await;
        let endpoint = format!("/api/v1/classes/{}/computed-fields", fixture.class.id);
        let response = post_request(
            &test_context.pool,
            &test_context.admin_token,
            &endpoint,
            definition("display_name"),
        )
        .await;
        let response = assert_response_status(response, StatusCode::CREATED).await;
        let created: serde_json::Value = test::read_body_json(response).await;
        let field_id = created["definition"]["id"].as_i64().unwrap();

        let response = patch_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!("{endpoint}/{field_id}"),
            serde_json::json!({
                "expected_revision": 999,
                "label": "Wrong revision"
            }),
        )
        .await;
        assert_response_status(response, StatusCode::CONFLICT).await;

        finish_active_rebuild(&test_context, fixture.class.id).await;
        fixture.cleanup().await.unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn update_class_permission_cannot_manage_shared_definitions(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "shared mutation permission").await;
        let group = grant_normal_user(&test_context, &fixture, &[Permissions::UpdateClass]).await;
        let endpoint = format!("/api/v1/classes/{}/computed-fields", fixture.class.id);

        let response = post_request(
            &test_context.pool,
            &test_context.normal_token,
            &endpoint,
            definition("forbidden_field"),
        )
        .await;
        assert_response_status(response, StatusCode::FORBIDDEN).await;

        fixture.cleanup().await.unwrap();
        group
            .delete_without_events(&test_context.pool)
            .await
            .unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn update_collection_permission_can_manage_shared_definitions(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "shared update collection permission").await;
        let group =
            grant_normal_user(&test_context, &fixture, &[Permissions::UpdateCollection]).await;
        let response = post_request(
            &test_context.pool,
            &test_context.normal_token,
            &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
            definition("allowed_field"),
        )
        .await;
        assert_response_status(response, StatusCode::CREATED).await;

        finish_active_rebuild(&test_context, fixture.class.id).await;
        fixture.cleanup().await.unwrap();
        group
            .delete_without_events(&test_context.pool)
            .await
            .unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn shared_mutation_rejects_a_stale_authorized_collection(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "shared stale collection authorization").await;

        let error = create_shared_definition(
            &test_context.pool,
            fixture.class.id,
            fixture.class.collection_id + 1,
            test_context.admin_user.id,
            definition_request("stale_authorization"),
            &EventContext::system(),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, crate::errors::ApiError::Conflict(_)));
        fixture.cleanup().await.unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn personal_values_are_not_visible_to_another_human(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "personal isolation").await;
        let group = grant_normal_user(
            &test_context,
            &fixture,
            &[Permissions::ReadClass, Permissions::ReadObject],
        )
        .await;
        let response = post_request(
            &test_context.pool,
            &test_context.admin_token,
            "/api/v1/iam/me/computed-fields",
            serde_json::json!({
                "class_id": fixture.class.id,
                "key": "admin_only",
                "label": "Admin only",
                "operation": {"type": "first_non_null", "paths": ["/manual/hostname"]},
                "result_type": "string"
            }),
        )
        .await;
        assert_response_status(response, StatusCode::CREATED).await;

        let response = get_request(
            &test_context.pool,
            &test_context.normal_token,
            &format!(
                "/api/v1/classes/{}/{}?include=computed",
                fixture.class.id, fixture.objects[0].id
            ),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let object: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(
            object["computed"]["personal"]["values"],
            serde_json::json!({})
        );

        fixture.cleanup().await.unwrap();
        group
            .delete_without_events(&test_context.pool)
            .await
            .unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn service_accounts_cannot_manage_or_receive_personal_fields(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "service account personal exclusion").await;
        let group = create_test_group(&test_context.pool).await;
        for permission in [Permissions::ReadClass, Permissions::ReadObject] {
            fixture
                .collection
                .collection
                .grant_one(&test_context.pool, group.id, permission)
                .await
                .unwrap();
        }
        let account = create_test_service_account(&test_context.pool, &group, None).await;
        group
            .add_member_without_events(&test_context.pool, &account)
            .await
            .unwrap();
        let token = service_account_token(&test_context.pool, &account, None, None).await;

        let response = post_request(
            &test_context.pool,
            &token,
            "/api/v1/iam/me/computed-fields",
            serde_json::json!({
                "class_id": fixture.class.id,
                "key": "not_allowed",
                "label": "Not allowed",
                "operation": {"type": "first_non_null", "paths": ["/manual/hostname"]},
                "result_type": "string"
            }),
        )
        .await;
        assert_response_status(response, StatusCode::FORBIDDEN).await;

        let response = get_request(
            &test_context.pool,
            &token,
            &format!(
                "/api/v1/classes/{}/{}?include=computed",
                fixture.class.id, fixture.objects[0].id
            ),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let object: serde_json::Value = test::read_body_json(response).await;
        assert!(object["computed"].get("personal").is_none());

        let response = get_request(
            &test_context.pool,
            &token,
            &format!(
                "/api/v1/classes/{}/?sort=computed.personal.not_allowed",
                fixture.class.id
            ),
        )
        .await;
        assert_response_status(response, StatusCode::BAD_REQUEST).await;

        fixture.cleanup().await.unwrap();
        crate::models::ServiceAccountID::new(account.id)
            .unwrap()
            .delete_without_events(&test_context.pool)
            .await
            .unwrap();
        group
            .delete_without_events(&test_context.pool)
            .await
            .unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn enriched_pagination_preserves_totals_and_cursors(
        #[future(awt)] test_context: TestContext,
    ) {
        let mut fixture = fixture(&test_context, "computed pagination").await;
        let second = NewHubuumObject {
            collection_id: fixture.class.collection_id,
            hubuum_class_id: fixture.class.id,
            name: test_context.scoped_name("computed page object"),
            description: "Second computed pagination object".to_string(),
            data: serde_json::json!({"manual": {"hostname": "second.example"}}),
        }
        .save_without_events(&test_context.pool)
        .await
        .unwrap();
        fixture.objects.push(second);
        let response = post_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
            definition("display_name"),
        )
        .await;
        assert_response_status(response, StatusCode::CREATED).await;

        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!(
                "/api/v1/classes/{}/?include=computed&sort=id&limit=1",
                fixture.class.id
            ),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        assert_eq!(
            header_value(&response, TOTAL_COUNT_HEADER).as_deref(),
            Some("2")
        );
        let cursor = header_value(&response, NEXT_CURSOR_HEADER).expect("next cursor");
        let first_page: Vec<serde_json::Value> = test::read_body_json(response).await;
        assert_eq!(first_page.len(), 1);
        assert!(first_page[0]["computed"]["shared"]["values"]["display_name"].is_string());

        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!(
                "/api/v1/classes/{}/?include=computed&sort=id&limit=1&cursor={cursor}",
                fixture.class.id
            ),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        assert_eq!(
            header_value(&response, TOTAL_COUNT_HEADER).as_deref(),
            Some("2")
        );
        let second_page: Vec<serde_json::Value> = test::read_body_json(response).await;
        assert_eq!(second_page.len(), 1);
        assert_ne!(first_page[0]["id"], second_page[0]["id"]);

        finish_active_rebuild(&test_context, fixture.class.id).await;
        fixture.cleanup().await.unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn shared_computed_sort_is_stable_across_cursor_pages(
        #[future(awt)] test_context: TestContext,
    ) {
        let mut fixture = fixture(&test_context, "shared computed sorting").await;
        for hostname in ["aardvark.example", "zulu.example"] {
            fixture.objects.push(
                NewHubuumObject {
                    collection_id: fixture.class.collection_id,
                    hubuum_class_id: fixture.class.id,
                    name: test_context.scoped_name(hostname),
                    description: "Computed sorting object".to_string(),
                    data: serde_json::json!({"manual": {"hostname": hostname}}),
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
            definition("display_name"),
        )
        .await;
        assert_response_status(response, StatusCode::CREATED).await;

        let expected = [
            fixture.objects[1].id,
            fixture.objects[0].id,
            fixture.objects[2].id,
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
                    "/api/v1/classes/{}/?include=computed&sort=computed.shared.display_name&limit=1{cursor_query}",
                    fixture.class.id
                ),
            )
            .await;
            let response = assert_response_status(response, StatusCode::OK).await;
            assert_eq!(
                header_value(&response, TOTAL_COUNT_HEADER).as_deref(),
                Some("3")
            );
            cursor = header_value(&response, NEXT_CURSOR_HEADER);
            let page: Vec<serde_json::Value> = test::read_body_json(response).await;
            assert_eq!(page.len(), 1);
            assert_eq!(page[0]["id"], expected_id);
            assert!(page[0]["computed"]["shared"]["values"]["display_name"].is_string());
            assert_eq!(cursor.is_some(), index < 2);
        }

        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!(
                "/api/v1/classes/{}/?sort=computed.shared.display_name.desc&limit=3",
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
                fixture.objects[0].id,
                fixture.objects[1].id
            ]
        );
        assert!(
            objects
                .iter()
                .all(|object| object.get("computed").is_none())
        );

        finish_active_rebuild(&test_context, fixture.class.id).await;
        fixture.cleanup().await.unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn shared_computed_sort_rejects_a_cache_row_for_old_source_data(
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
    #[tokio::test]
    async fn computed_sort_cursor_uses_the_resolved_definition_snapshot(
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
        let snapshot = resolve_computed_sort_fields(
            &test_context.pool,
            fixture.class.id,
            Some(test_context.admin_user.id),
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

        let enriched = enrich_objects_with_computed_sort_snapshot(
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
    async fn numeric_computed_sort_cursor_matches_domain_precision(
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
    async fn personal_computed_sort_is_owner_scoped_and_numeric(
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
    async fn computed_sort_query_count_is_constant_with_page_size(
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

        let small_endpoint = format!(
            "/api/v1/classes/{}/?include=computed&include_total=false&sort=computed.shared.display_name&limit=1",
            fixture.class.id
        );
        let (small_response, small_queries) = capture_queries(get_request(
            &test_context.pool,
            &test_context.admin_token,
            &small_endpoint,
        ))
        .await;
        let small_response = assert_response_status(small_response, StatusCode::OK).await;
        let small_page: Vec<serde_json::Value> = test::read_body_json(small_response).await;
        assert_eq!(small_page.len(), 1);

        let large_endpoint = format!(
            "/api/v1/classes/{}/?include=computed&include_total=false&sort=computed.shared.display_name&limit=8",
            fixture.class.id
        );
        let (large_response, large_queries) = capture_queries(get_request(
            &test_context.pool,
            &test_context.admin_token,
            &large_endpoint,
        ))
        .await;
        let large_response = assert_response_status(large_response, StatusCode::OK).await;
        let large_page: Vec<serde_json::Value> = test::read_body_json(large_response).await;
        assert_eq!(large_page.len(), 8);

        assert_eq!(large_queries.total_queries(), small_queries.total_queries());
        assert_eq!(
            large_queries.domain_queries(),
            small_queries.domain_queries()
        );
        assert_eq!(
            large_queries.connection_checkouts(),
            small_queries.connection_checkouts()
        );
        assert_eq!(large_queries.query_counts(), small_queries.query_counts());
        assert_eq!(
            large_queries.queries_matching("hubuum_computed_evaluate_scope"),
            1
        );

        fixture.cleanup().await.unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn object_and_materialization_rollback_together_on_unexpected_failure(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "computed atomicity").await;
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
            use crate::schema::computed_field_definitions::dsl::{
                class_id, computed_field_definitions, operation,
            };
            diesel::update(computed_field_definitions.filter(class_id.eq(fixture.class.id)))
                .set(operation.eq(serde_json::json!({"type": "unknown", "paths": ["/a"]})))
                .execute(conn)
                .await
        })
        .await
        .unwrap();

        let response = patch_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!(
                "/api/v1/classes/{}/{}",
                fixture.class.id, fixture.objects[0].id
            ),
            serde_json::json!({"data": {"manual": {"hostname": "must-rollback"}}}),
        )
        .await;
        assert_response_status(response, StatusCode::INTERNAL_SERVER_ERROR).await;
        let current = crate::models::HubuumObjectID::new(fixture.objects[0].id)
            .unwrap()
            .instance(&test_context.pool)
            .await
            .unwrap();
        assert_eq!(current.data, fixture.objects[0].data);

        fixture.cleanup().await.unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn concurrent_definition_and_object_updates_return_current_values(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "computed concurrent definition").await;
        let endpoint = format!("/api/v1/classes/{}/computed-fields", fixture.class.id);
        let response = post_request(
            &test_context.pool,
            &test_context.admin_token,
            &endpoint,
            definition("display_name"),
        )
        .await;
        let response = assert_response_status(response, StatusCode::CREATED).await;
        let created: serde_json::Value = test::read_body_json(response).await;
        let field_id = created["definition"]["id"].as_i64().unwrap();
        finish_active_rebuild(&test_context, fixture.class.id).await;

        let definition_endpoint = format!("{endpoint}/{field_id}");
        let object_endpoint = format!(
            "/api/v1/classes/{}/{}",
            fixture.class.id, fixture.objects[0].id
        );
        let definition_update = patch_request(
            &test_context.pool,
            &test_context.admin_token,
            &definition_endpoint,
            serde_json::json!({
                "expected_revision": 1,
                "operation": {"type": "first_non_null", "paths": ["/manual/hostname"]}
            }),
        );
        let object_update = patch_request(
            &test_context.pool,
            &test_context.admin_token,
            &object_endpoint,
            serde_json::json!({
                "data": {"manual": {"hostname": "concurrent.example"}}
            }),
        );
        let (definition_response, object_response) = tokio::join!(definition_update, object_update);
        assert_response_status(definition_response, StatusCode::OK).await;
        assert_response_status(object_response, StatusCode::OK).await;

        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!("{object_endpoint}?include=computed"),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let object: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(
            object["computed"]["shared"]["values"]["display_name"],
            "concurrent.example"
        );

        finish_active_rebuild(&test_context, fixture.class.id).await;
        fixture.cleanup().await.unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn concurrent_backfill_and_object_update_cannot_restore_old_source_data(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "computed concurrent backfill").await;
        let response = post_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
            definition("display_name"),
        )
        .await;
        assert_response_status(response, StatusCode::CREATED).await;
        let task = active_rebuild_task(&test_context, fixture.class.id).await;
        let object_endpoint = format!(
            "/api/v1/classes/{}/{}",
            fixture.class.id, fixture.objects[0].id
        );
        let rebuild = execute_computed_reindex_task(&test_context.pool, &task);
        let update = patch_request(
            &test_context.pool,
            &test_context.admin_token,
            &object_endpoint,
            serde_json::json!({
                "data": {"manual": {"hostname": "after-backfill.example"}}
            }),
        );
        let (rebuild_result, update_response) = tokio::join!(rebuild, update);
        let _ = rebuild_result;
        assert_response_status(update_response, StatusCode::OK).await;
        finish_active_rebuild(&test_context, fixture.class.id).await;

        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!("{object_endpoint}?include=computed"),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let object: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(
            object["computed"]["shared"]["values"]["display_name"],
            "after-backfill.example"
        );
        assert_eq!(object["computed"]["shared"]["materialization_stale"], false);

        fixture.cleanup().await.unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn lease_recovery_marks_the_class_rebuild_failed(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "computed lease recovery").await;
        let response = post_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
            definition("display_name"),
        )
        .await;
        assert_response_status(response, StatusCode::CREATED).await;
        let state = class_computation_state_for(&test_context.pool, fixture.class.id)
            .await
            .unwrap();
        let task_id = state.active_task_id.unwrap();
        with_connection(&test_context.pool, async |conn| {
            use crate::schema::tasks::dsl::{id, status, tasks};
            diesel::update(tasks.filter(id.eq(task_id)))
                .set(status.eq(TaskStatus::Running.as_str()))
                .execute(conn)
                .await
        })
        .await
        .unwrap();

        let recovered = recover_expired_task_leases(&test_context.pool, 100)
            .await
            .unwrap();
        assert!(recovered.iter().any(|task| task.id == task_id));
        let state = class_computation_state_for(&test_context.pool, fixture.class.id)
            .await
            .unwrap();
        assert_eq!(state.rebuild_status, "failed");
        assert_eq!(state.active_task_id, None);

        fixture.cleanup().await.unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn user_anonymization_removes_personal_definitions(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "computed anonymization").await;
        let group = grant_normal_user(&test_context, &fixture, &[Permissions::ReadClass]).await;
        let response = post_request(
            &test_context.pool,
            &test_context.normal_token,
            "/api/v1/iam/me/computed-fields",
            serde_json::json!({
                "class_id": fixture.class.id,
                "key": "remove_me",
                "label": "Remove me",
                "operation": {"type": "first_non_null", "paths": ["/manual/hostname"]},
                "result_type": "string"
            }),
        )
        .await;
        assert_response_status(response, StatusCode::CREATED).await;

        let response = post_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!(
                "/api/v1/iam/users/{}/anonymize",
                test_context.normal_user.id
            ),
            serde_json::json!({}),
        )
        .await;
        assert_response_status(response, StatusCode::NO_CONTENT).await;
        let remaining = with_connection(&test_context.pool, async |conn| {
            use crate::schema::computed_field_definitions::dsl::{
                computed_field_definitions, owner_user_id,
            };
            computed_field_definitions
                .filter(owner_user_id.eq(Some(test_context.normal_user.id)))
                .count()
                .get_result::<i64>(conn)
                .await
        })
        .await
        .unwrap();
        assert_eq!(remaining, 0);

        fixture.cleanup().await.unwrap();
        group
            .delete_without_events(&test_context.pool)
            .await
            .unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn concurrent_personal_creates_preserve_scope_capacity(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "computed personal capacity").await;
        for index in 0..15 {
            create_personal_definition(
                &test_context.pool,
                fixture.class.id,
                test_context.normal_user.id,
                definition_request(&format!("existing_{index}")),
            )
            .await
            .unwrap();
        }

        let first = create_personal_definition(
            &test_context.pool,
            fixture.class.id,
            test_context.normal_user.id,
            definition_request("concurrent_first"),
        );
        let second = create_personal_definition(
            &test_context.pool,
            fixture.class.id,
            test_context.normal_user.id,
            definition_request("concurrent_second"),
        );
        let (first, second) = tokio::join!(first, second);
        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);

        let count = with_connection(&test_context.pool, async |conn| {
            use crate::schema::computed_field_definitions::dsl::{
                class_id, computed_field_definitions, owner_user_id,
            };
            computed_field_definitions
                .filter(class_id.eq(fixture.class.id))
                .filter(owner_user_id.eq(Some(test_context.normal_user.id)))
                .count()
                .get_result::<i64>(conn)
                .await
        })
        .await
        .unwrap();
        assert_eq!(count, 16);

        fixture.cleanup().await.unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn deleting_a_class_cascades_definitions_and_materialization(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "computed class cascade").await;
        let class_id_value = fixture.class.id;
        let object_id_value = fixture.objects[0].id;
        let response = post_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!("/api/v1/classes/{class_id_value}/computed-fields"),
            definition("shared_value"),
        )
        .await;
        assert_response_status(response, StatusCode::CREATED).await;
        finish_active_rebuild(&test_context, class_id_value).await;
        let response = post_request(
            &test_context.pool,
            &test_context.admin_token,
            "/api/v1/iam/me/computed-fields",
            serde_json::json!({
                "class_id": class_id_value,
                "key": "personal_value",
                "label": "Personal value",
                "operation": {"type": "first_non_null", "paths": ["/manual/hostname"]},
                "result_type": "string"
            }),
        )
        .await;
        assert_response_status(response, StatusCode::CREATED).await;

        fixture.cleanup().await.unwrap();
        let (definitions, materializations) =
            with_connection(&test_context.pool, async |conn| -> QueryResult<_> {
                use crate::schema::computed_field_definitions::dsl as definition;
                use crate::schema::object_computed_data::dsl as computed;
                let definitions = definition::computed_field_definitions
                    .filter(definition::class_id.eq(class_id_value))
                    .count()
                    .get_result::<i64>(conn)
                    .await?;
                let materializations = computed::object_computed_data
                    .filter(computed::object_id.eq(object_id_value))
                    .count()
                    .get_result::<i64>(conn)
                    .await?;
                Ok((definitions, materializations))
            })
            .await
            .unwrap();
        assert_eq!((definitions, materializations), (0, 0));
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn manual_rebuild_queues_the_current_revision(#[future(awt)] test_context: TestContext) {
        let fixture = fixture(&test_context, "computed manual rebuild").await;
        let definition_endpoint = format!("/api/v1/classes/{}/computed-fields", fixture.class.id);
        let response = post_request(
            &test_context.pool,
            &test_context.admin_token,
            &definition_endpoint,
            definition("display_name"),
        )
        .await;
        assert_response_status(response, StatusCode::CREATED).await;
        finish_active_rebuild(&test_context, fixture.class.id).await;
        let before = class_computation_state_for(&test_context.pool, fixture.class.id)
            .await
            .unwrap();
        let rebuild_endpoint = format!("{definition_endpoint}/rebuild");

        let response = post_request(
            &test_context.pool,
            &test_context.admin_token,
            &rebuild_endpoint,
            serde_json::json!({}),
        )
        .await;
        let response = assert_response_status(response, StatusCode::ACCEPTED).await;
        let response: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(response["evaluation_revision"], before.evaluation_revision);
        assert_eq!(response["rebuild_status"], "rebuilding");
        assert!(response["active_task_id"].is_number());

        finish_active_rebuild(&test_context, fixture.class.id).await;
        fixture.cleanup().await.unwrap();
    }
}
