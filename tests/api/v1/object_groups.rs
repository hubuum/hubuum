#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use actix_web::{App, http::StatusCode, test, web::Data};
    use base64::Engine;

    use crate::db::traits::computed_field::{
        class_computation_state_for, create_personal_definition, create_shared_definition,
        execute_computed_reindex_task,
    };
    use crate::events::EventContext;
    use crate::models::{
        ComputedFieldDefinitionRequest, HubuumObject, MAX_OBJECT_GROUP_CURSOR_LENGTH,
        NewHubuumClass, NewHubuumObject, Permissions, ServiceAccountID, TaskID, UpdateHubuumObject,
    };
    use crate::pagination::{NEXT_CURSOR_HEADER, TOTAL_COUNT_HEADER};
    use crate::permissions::test_support::mock_treetop::{MockAllowRule, MockTreetopBackend};
    use crate::permissions::{AppContext, PermissionBackend, ResourceAttrs, ResourceKind};
    use crate::tests::api_operations::get_request;
    use crate::tests::asserts::{assert_response_status, header_value};
    use crate::tests::{
        ObjectFixture, TestContext, create_test_group, create_test_service_account, scoped_token,
        service_account_token, test_context,
    };
    use crate::traits::{CanDelete, CanUpdate, PermissionController, SelfAccessors};

    async fn fixture(context: &TestContext, label: &str) -> ObjectFixture {
        let object = |name: &str, description: &str, data: serde_json::Value| NewHubuumObject {
            collection_id: 0,
            hubuum_class_id: 0,
            name: context.scoped_name(name),
            description: description.to_string(),
            data,
        };
        context
            .object_fixture(
                label,
                NewHubuumClass {
                    collection_id: 0,
                    name: context.scoped_name(&format!("object group class {label}")),
                    description: "Object group test class".to_string(),
                    json_schema: None,
                    validate_schema: Some(false),
                },
                vec![
                    object(
                        "group object one",
                        "alpha",
                        serde_json::json!({
                            "status": "active",
                            "location": {"country": "NO"},
                            "typed": "text",
                            "nullable": "present",
                            "bucket": "a"
                        }),
                    ),
                    object(
                        "group object two",
                        "alpha",
                        serde_json::json!({
                            "status": "active",
                            "location": {"country": "NO"},
                            "typed": 7,
                            "nullable": null,
                            "bucket": null
                        }),
                    ),
                    object(
                        "group object three",
                        "beta",
                        serde_json::json!({
                            "status": "inactive",
                            "location": {"country": "SE"},
                            "typed": true,
                            "bucket": 12
                        }),
                    ),
                    object(
                        "group object four",
                        "beta",
                        serde_json::json!({
                            "status": "active",
                            "location": {"country": ["NO"]},
                            "typed": ["x"]
                        }),
                    ),
                    object(
                        "group object five",
                        "gamma",
                        serde_json::json!({
                            "status": "active",
                            "typed": {"nested_null": null},
                            "bucket": "a"
                        }),
                    ),
                ],
            )
            .await
            .unwrap()
    }

    struct GroupPage {
        rows: Vec<serde_json::Value>,
        total_count: Option<String>,
        cache_control: Option<String>,
    }

    async fn group_rows(
        context: &TestContext,
        fixture: &ObjectFixture,
        token: &str,
        query: &str,
    ) -> GroupPage {
        let response = get_request(
            &context.pool,
            token,
            &format!("/api/v1/classes/{}/object-groups?{query}", fixture.class.id),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let total_count = header_value(&response, TOTAL_COUNT_HEADER);
        let cache_control = header_value(&response, "Cache-Control");
        let rows = test::read_body_json(response).await;
        GroupPage {
            rows,
            total_count,
            cache_control,
        }
    }

    fn summed_count(rows: &[serde_json::Value]) -> i64 {
        rows.iter()
            .map(|row| row["object_count"].as_i64().unwrap())
            .sum()
    }

    fn encoded_group_cursor(sort_key: serde_json::Value, object_count: i64) -> String {
        let token = serde_json::json!({
            "version": 1,
            "dimensions": ["name"],
            "sort": "dimensions_ascending",
            "sort_key": sort_key,
            "object_count": object_count,
        });
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(&token).unwrap())
    }

    fn computed_definition(key: &str, path: &str, enabled: bool) -> ComputedFieldDefinitionRequest {
        serde_json::from_value(serde_json::json!({
            "key": key,
            "label": key,
            "description": "",
            "operation": {"type": "first_non_null", "paths": [path]},
            "result_type": "string",
            "enabled": enabled
        }))
        .unwrap()
    }

    async fn finish_active_rebuild(context: &TestContext, class_id: i32) {
        for _ in 0..20 {
            let state = class_computation_state_for(&context.pool, class_id)
                .await
                .unwrap();
            if state.active_task_id.is_none() {
                return;
            }
            let task = TaskID::new(state.active_task_id.unwrap())
                .unwrap()
                .instance(&context.pool)
                .await
                .unwrap();
            let _ = execute_computed_reindex_task(&context.pool, &task).await;
            tokio::task::yield_now().await;
        }
        panic!("computed-field rebuild did not finish");
    }

    async fn grant_normal_user_read_access(
        context: &TestContext,
        fixture: &ObjectFixture,
    ) -> crate::models::Group {
        let group = create_test_group(&context.pool).await;
        group
            .add_member_without_events(&context.pool, &context.normal_user)
            .await
            .unwrap();
        for permission in [
            Permissions::ReadCollection,
            Permissions::ReadClass,
            Permissions::ReadObject,
        ] {
            fixture
                .collection
                .collection
                .grant_one(&context.pool, group.id, permission)
                .await
                .unwrap();
        }
        group
    }

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
        let page = group_rows(
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
        let page = group_rows(
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
        let page = group_rows(
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
    async fn json_null_and_missing_path_are_distinct_buckets(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "json states").await;
        let page = group_rows(
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
    async fn filters_apply_before_multidimensional_grouping(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "multidimensional filters").await;
        let page = group_rows(
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

    #[rstest::rstest]
    #[tokio::test]
    async fn count_sort_cursor_is_deterministic_across_duplicate_counts(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "count cursor").await;
        let endpoint = format!(
            "/api/v1/classes/{}/object-groups?group_by=description&sort=object_count.desc&limit=1",
            fixture.class.id
        );
        let mut cursor = None;
        let mut values = Vec::new();
        for _ in 0..3 {
            let response = get_request(
                &test_context.pool,
                &test_context.admin_token,
                &cursor
                    .as_ref()
                    .map(|cursor| format!("{endpoint}&cursor={cursor}"))
                    .unwrap_or_else(|| endpoint.clone()),
            )
            .await;
            let response = assert_response_status(response, StatusCode::OK).await;
            assert_eq!(
                header_value(&response, TOTAL_COUNT_HEADER).as_deref(),
                Some("3")
            );
            cursor = header_value(&response, NEXT_CURSOR_HEADER);
            let rows: Vec<serde_json::Value> = test::read_body_json(response).await;
            assert_eq!(rows.len(), 1);
            values.push(rows[0]["dimensions"][0]["value"].clone());
        }

        assert_eq!(
            values,
            vec![
                serde_json::json!("alpha"),
                serde_json::json!("beta"),
                serde_json::json!("gamma")
            ]
        );
        assert!(cursor.is_none());

        fixture.cleanup().await.unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn oversized_group_key_is_rejected_before_a_cursor_is_emitted(
        #[future(awt)] test_context: TestContext,
    ) {
        let large_value = format!("a{}", "x".repeat(MAX_OBJECT_GROUP_CURSOR_LENGTH));
        let fixture = test_context
            .object_fixture(
                "oversized group cursor",
                NewHubuumClass {
                    collection_id: 0,
                    name: test_context.scoped_name("oversized group cursor class"),
                    description: "Oversized group cursor".to_string(),
                    json_schema: None,
                    validate_schema: Some(false),
                },
                vec![
                    NewHubuumObject {
                        collection_id: 0,
                        hubuum_class_id: 0,
                        name: test_context.scoped_name("oversized group cursor object"),
                        description: "Oversized".to_string(),
                        data: serde_json::json!({"group_key": large_value}),
                    },
                    NewHubuumObject {
                        collection_id: 0,
                        hubuum_class_id: 0,
                        name: test_context.scoped_name("small group cursor object"),
                        description: "Small".to_string(),
                        data: serde_json::json!({"group_key": "z"}),
                    },
                ],
            )
            .await
            .unwrap();
        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!(
                "/api/v1/classes/{}/object-groups?group_by=json_data.group_key&limit=1",
                fixture.class.id
            ),
        )
        .await;

        assert_response_status(response, StatusCode::PAYLOAD_TOO_LARGE).await;

        fixture.cleanup().await.unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn include_total_false_omits_group_cardinality_header(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "skip total").await;
        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!(
                "/api/v1/classes/{}/object-groups?group_by=description&include_total=false",
                fixture.class.id
            ),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;

        assert!(header_value(&response, TOTAL_COUNT_HEADER).is_none());

        fixture.cleanup().await.unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn empty_class_returns_empty_group_page(#[future(awt)] test_context: TestContext) {
        let fixture = test_context
            .object_fixture(
                "empty groups",
                NewHubuumClass {
                    collection_id: 0,
                    name: test_context.scoped_name("empty group class"),
                    description: "Empty".to_string(),
                    json_schema: None,
                    validate_schema: Some(false),
                },
                Vec::new(),
            )
            .await
            .unwrap();
        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!(
                "/api/v1/classes/{}/object-groups?group_by=name",
                fixture.class.id
            ),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        assert_eq!(
            header_value(&response, TOTAL_COUNT_HEADER).as_deref(),
            Some("0")
        );
        let rows: Vec<serde_json::Value> = test::read_body_json(response).await;
        assert!(rows.is_empty());

        fixture.cleanup().await.unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn empty_group_page_rejects_a_malformed_cursor(#[future(awt)] test_context: TestContext) {
        let fixture = test_context
            .object_fixture(
                "empty malformed group cursor",
                NewHubuumClass {
                    collection_id: 0,
                    name: test_context.scoped_name("empty malformed group cursor class"),
                    description: "Empty".to_string(),
                    json_schema: None,
                    validate_schema: Some(false),
                },
                Vec::new(),
            )
            .await
            .unwrap();
        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!(
                "/api/v1/classes/{}/object-groups?group_by=name&cursor=not-a-cursor",
                fixture.class.id
            ),
        )
        .await;

        assert_response_status(response, StatusCode::BAD_REQUEST).await;

        fixture.cleanup().await.unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn empty_group_page_rejects_structurally_invalid_cursor(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "invalid cursor ordering values").await;
        let cursor = encoded_group_cursor(serde_json::json!([null]), 1);
        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!(
                "/api/v1/classes/{}/object-groups?name__equals=no-such-object&group_by=name&cursor={cursor}",
                fixture.class.id
            ),
        )
        .await;

        assert_response_status(response, StatusCode::BAD_REQUEST).await;

        fixture.cleanup().await.unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn empty_group_page_rejects_a_cursor_for_different_dimensions(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "mismatched empty group cursor").await;
        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!(
                "/api/v1/classes/{}/object-groups?group_by=description&limit=1",
                fixture.class.id
            ),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let cursor = header_value(&response, NEXT_CURSOR_HEADER).unwrap();
        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!(
                "/api/v1/classes/{}/object-groups?name__equals=no-such-object&group_by=name&cursor={cursor}",
                fixture.class.id
            ),
        )
        .await;

        assert_response_status(response, StatusCode::BAD_REQUEST).await;

        fixture.cleanup().await.unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn shared_computed_groups_evaluate_snapshots_and_use_unavailable_bucket(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "shared computed groups").await;
        create_shared_definition(
            &test_context.pool,
            fixture.class.id,
            fixture.class.collection_id,
            test_context.admin_user.id,
            computed_definition("shared_bucket", "/bucket", true),
            &EventContext::system(),
        )
        .await
        .unwrap();

        let page = group_rows(
            &test_context,
            &fixture,
            &test_context.admin_token,
            "group_by=computed.shared.shared_bucket",
        )
        .await;
        let count_for = |state: &str| {
            page.rows
                .iter()
                .find(|row| row["dimensions"][0]["state"] == state)
                .map(|row| row["object_count"].as_i64().unwrap())
        };
        assert_eq!(count_for("value"), Some(2));
        assert_eq!(count_for("null"), Some(2));
        assert_eq!(count_for("unavailable"), Some(1));
        assert_eq!(page.cache_control.as_deref(), Some("private, no-store"));

        finish_active_rebuild(&test_context, fixture.class.id).await;
        fixture.cleanup().await.unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn personal_computed_grouping_enforces_owner_and_human_principal(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "personal computed groups").await;
        let group = grant_normal_user_read_access(&test_context, &fixture).await;
        create_personal_definition(
            &test_context.pool,
            fixture.class.id,
            test_context.normal_user.id,
            computed_definition("priority", "/bucket", true),
        )
        .await
        .unwrap();
        create_personal_definition(
            &test_context.pool,
            fixture.class.id,
            test_context.admin_user.id,
            computed_definition("admin_only", "/status", true),
        )
        .await
        .unwrap();

        let page = group_rows(
            &test_context,
            &fixture,
            &test_context.normal_token,
            "group_by=computed.personal.priority",
        )
        .await;
        assert_eq!(summed_count(&page.rows), 5);

        let response = get_request(
            &test_context.pool,
            &test_context.normal_token,
            &format!(
                "/api/v1/classes/{}/object-groups?group_by=computed.personal.admin_only",
                fixture.class.id
            ),
        )
        .await;
        let response = assert_response_status(response, StatusCode::BAD_REQUEST).await;
        let error: serde_json::Value = test::read_body_json(response).await;
        assert!(
            error["message"]
                .as_str()
                .unwrap()
                .contains("accessible field")
        );

        let account = create_test_service_account(&test_context.pool, &group, None).await;
        group
            .add_member_without_events(&test_context.pool, &account)
            .await
            .unwrap();
        let token = service_account_token(&test_context.pool, &account, None, None).await;
        let response = get_request(
            &test_context.pool,
            &token,
            &format!(
                "/api/v1/classes/{}/object-groups?group_by=computed.personal.priority",
                fixture.class.id
            ),
        )
        .await;
        let response = assert_response_status(response, StatusCode::BAD_REQUEST).await;
        let error: serde_json::Value = test::read_body_json(response).await;
        assert!(
            error["message"]
                .as_str()
                .unwrap()
                .contains("Service accounts")
        );

        ServiceAccountID::new(account.id)
            .unwrap()
            .delete_without_events(&test_context.pool)
            .await
            .unwrap();
        fixture.cleanup().await.unwrap();
        group
            .delete_without_events(&test_context.pool)
            .await
            .unwrap();
    }

    #[rstest::rstest]
    #[case("unknown", false)]
    #[case("disabled", true)]
    #[tokio::test]
    async fn invalid_computed_selectors_are_bad_requests(
        #[future(awt)] test_context: TestContext,
        #[case] key: &str,
        #[case] create_disabled: bool,
    ) {
        let fixture = fixture(&test_context, &format!("invalid computed {key}")).await;
        let group = grant_normal_user_read_access(&test_context, &fixture).await;
        if create_disabled {
            create_personal_definition(
                &test_context.pool,
                fixture.class.id,
                test_context.normal_user.id,
                computed_definition(key, "/bucket", false),
            )
            .await
            .unwrap();
        }
        let response = get_request(
            &test_context.pool,
            &test_context.normal_token,
            &format!(
                "/api/v1/classes/{}/object-groups?group_by=computed.personal.{key}",
                fixture.class.id
            ),
        )
        .await;
        let response = assert_response_status(response, StatusCode::BAD_REQUEST).await;
        let error: serde_json::Value = test::read_body_json(response).await;
        let message = error["message"].as_str().unwrap();
        if create_disabled {
            assert!(message.contains("disabled"));
        } else {
            assert!(message.contains("accessible field"));
        }

        fixture.cleanup().await.unwrap();
        group
            .delete_without_events(&test_context.pool)
            .await
            .unwrap();
    }

    async fn get_with_permission_backend(
        context: &TestContext,
        token: &str,
        backend: Arc<dyn PermissionBackend>,
        endpoint: &str,
    ) -> actix_web::dev::ServiceResponse {
        let app = test::init_service(
            App::new()
                .app_data(Data::new(context.pool.get_ref().clone()))
                .app_data(Data::new(AppContext::new(
                    context.pool.get_ref().clone(),
                    backend,
                )))
                .configure(crate::api::config),
        )
        .await;
        test::TestRequest::get()
            .insert_header((
                actix_web::http::header::AUTHORIZATION,
                format!("Bearer {token}"),
            ))
            .uri(endpoint)
            .send_request(&app)
            .await
            .map_into_boxed_body()
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn non_pushdown_authorization_filters_objects_before_aggregation(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "external permission groups").await;
        let group = create_test_group(&test_context.pool).await;
        group
            .add_member_without_events(&test_context.pool, &test_context.normal_user)
            .await
            .unwrap();
        let backend = Arc::new(MockTreetopBackend::new());
        for object in fixture.objects.iter().take(2) {
            backend.add_rule(MockAllowRule {
                group_id: group.id,
                action: Permissions::ReadObject,
                resource_kind: ResourceKind::Object,
                resource_id: Some(object.id),
                attrs: ResourceAttrs::default(),
            });
        }

        let response = get_with_permission_backend(
            &test_context,
            &test_context.normal_token,
            backend.clone(),
            &format!(
                "/api/v1/classes/{}/object-groups?group_by=description",
                fixture.class.id
            ),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        assert_eq!(
            header_value(&response, TOTAL_COUNT_HEADER).as_deref(),
            Some("1")
        );
        let rows: Vec<serde_json::Value> = test::read_body_json(response).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["object_count"], 2);
        assert_eq!(rows[0]["dimensions"][0]["value"], "alpha");
        assert_eq!(backend.authorization_batch_sizes(), vec![2, 2, 1]);

        fixture.cleanup().await.unwrap();
        group
            .delete_without_events(&test_context.pool)
            .await
            .unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn non_pushdown_high_cardinality_groups_paginate_from_bounded_accumulator(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "external accumulator pagination").await;
        let group = create_test_group(&test_context.pool).await;
        group
            .add_member_without_events(&test_context.pool, &test_context.normal_user)
            .await
            .unwrap();
        let backend = Arc::new(MockTreetopBackend::new());
        for object in &fixture.objects {
            backend.add_rule(MockAllowRule {
                group_id: group.id,
                action: Permissions::ReadObject,
                resource_kind: ResourceKind::Object,
                resource_id: Some(object.id),
                attrs: ResourceAttrs::default(),
            });
        }

        let endpoint = format!(
            "/api/v1/classes/{}/object-groups?collection_id={}&group_by=name&limit=2",
            fixture.class.id, fixture.collection.collection.id
        );
        let mut cursor = None;
        let mut values = Vec::new();
        for _ in 0..3 {
            let response = get_with_permission_backend(
                &test_context,
                &test_context.normal_token,
                backend.clone(),
                &cursor
                    .as_ref()
                    .map(|cursor| format!("{endpoint}&cursor={cursor}"))
                    .unwrap_or_else(|| endpoint.clone()),
            )
            .await;
            let response = assert_response_status(response, StatusCode::OK).await;
            assert_eq!(
                header_value(&response, TOTAL_COUNT_HEADER).as_deref(),
                Some("5")
            );
            cursor = header_value(&response, NEXT_CURSOR_HEADER);
            let rows: Vec<serde_json::Value> = test::read_body_json(response).await;
            values.extend(
                rows.into_iter()
                    .map(|row| row["dimensions"][0]["value"].as_str().unwrap().to_string()),
            );
        }

        let mut expected = fixture
            .objects
            .iter()
            .map(|object| object.name.clone())
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(values, expected);
        assert!(cursor.is_none());

        fixture.cleanup().await.unwrap();
        group
            .delete_without_events(&test_context.pool)
            .await
            .unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn non_pushdown_grouping_uses_the_authorized_object_snapshot(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "external authorization snapshot").await;
        let group = create_test_group(&test_context.pool).await;
        group
            .add_member_without_events(&test_context.pool, &test_context.normal_user)
            .await
            .unwrap();
        let authorized_object = fixture.objects[0].clone();
        let renamed = test_context.scoped_name("renamed after authorization input");
        let backend = Arc::new(MockTreetopBackend::new());
        backend.add_rule(MockAllowRule {
            group_id: group.id,
            action: Permissions::ReadObject,
            resource_kind: ResourceKind::Object,
            resource_id: Some(authorized_object.id),
            attrs: ResourceAttrs {
                name: Some(authorized_object.name.clone()),
                ..Default::default()
            },
        });
        let pool = test_context.pool.clone();
        let renamed_for_hook = renamed.clone();
        backend.set_authorization_hook(move || async move {
            UpdateHubuumObject {
                name: Some(renamed_for_hook),
                collection_id: None,
                hubuum_class_id: None,
                data: None,
                description: None,
            }
            .update_without_events(&pool, authorized_object.id)
            .await
            .unwrap();
        });

        let response = get_with_permission_backend(
            &test_context,
            &test_context.normal_token,
            backend,
            &format!(
                "/api/v1/classes/{}/object-groups?group_by=name",
                fixture.class.id
            ),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let rows: Vec<serde_json::Value> = test::read_body_json(response).await;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["dimensions"][0]["value"], fixture.objects[0].name);
        assert_ne!(rows[0]["dimensions"][0]["value"], renamed);

        fixture.cleanup().await.unwrap();
        group
            .delete_without_events(&test_context.pool)
            .await
            .unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn non_pushdown_authorization_honors_permission_filters(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "external filtered permissions").await;
        let group = create_test_group(&test_context.pool).await;
        group
            .add_member_without_events(&test_context.pool, &test_context.normal_user)
            .await
            .unwrap();
        let backend = Arc::new(MockTreetopBackend::new());
        for object in fixture.objects.iter().take(2) {
            backend.add_rule(MockAllowRule {
                group_id: group.id,
                action: Permissions::ReadObject,
                resource_kind: ResourceKind::Object,
                resource_id: Some(object.id),
                attrs: ResourceAttrs::default(),
            });
        }
        backend.add_rule(MockAllowRule {
            group_id: group.id,
            action: Permissions::UpdateObject,
            resource_kind: ResourceKind::Object,
            resource_id: Some(fixture.objects[0].id),
            attrs: ResourceAttrs::default(),
        });

        let response = get_with_permission_backend(
            &test_context,
            &test_context.normal_token,
            backend,
            &format!(
                "/api/v1/classes/{}/object-groups?permissions=UpdateObject&group_by=description",
                fixture.class.id
            ),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        assert_eq!(
            header_value(&response, TOTAL_COUNT_HEADER).as_deref(),
            Some("1")
        );
        let rows: Vec<serde_json::Value> = test::read_body_json(response).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["object_count"], 1);

        fixture.cleanup().await.unwrap();
        group
            .delete_without_events(&test_context.pool)
            .await
            .unwrap();
    }

    #[rstest::rstest]
    #[case(Permissions::ReadClass, ResourceKind::Class)]
    #[case(Permissions::ReadCollection, ResourceKind::Collection)]
    #[tokio::test]
    async fn non_pushdown_permission_filters_use_compatible_resource_kinds(
        #[future(awt)] test_context: TestContext,
        #[case] filtered_permission: Permissions,
        #[case] filtered_resource_kind: ResourceKind,
    ) {
        let fixture = fixture(&test_context, "external compatible permission resources").await;
        let group = create_test_group(&test_context.pool).await;
        group
            .add_member_without_events(&test_context.pool, &test_context.normal_user)
            .await
            .unwrap();
        let backend = Arc::new(MockTreetopBackend::new());
        for object in fixture.objects.iter().take(2) {
            backend.add_rule(MockAllowRule {
                group_id: group.id,
                action: Permissions::ReadObject,
                resource_kind: ResourceKind::Object,
                resource_id: Some(object.id),
                attrs: ResourceAttrs::default(),
            });
        }
        let filtered_resource_id = match filtered_permission {
            Permissions::ReadClass => fixture.class.id,
            Permissions::ReadCollection => fixture.collection.collection.id,
            _ => unreachable!("test covers class and collection permissions"),
        };
        backend.add_rule(MockAllowRule {
            group_id: group.id,
            action: filtered_permission,
            resource_kind: filtered_resource_kind,
            resource_id: Some(filtered_resource_id),
            attrs: ResourceAttrs::default(),
        });

        let response = get_with_permission_backend(
            &test_context,
            &test_context.normal_token,
            backend.clone(),
            &format!(
                "/api/v1/classes/{}/object-groups?permissions={filtered_permission}&group_by=description",
                fixture.class.id
            ),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let rows: Vec<serde_json::Value> = test::read_body_json(response).await;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["object_count"], 2);
        assert_eq!(backend.authorization_batch_sizes(), vec![4, 4, 2]);

        fixture.cleanup().await.unwrap();
        group
            .delete_without_events(&test_context.pool)
            .await
            .unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn non_pushdown_permission_filters_respect_token_scopes(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "external permission scopes").await;
        let group = create_test_group(&test_context.pool).await;
        group
            .add_member_without_events(&test_context.pool, &test_context.normal_user)
            .await
            .unwrap();
        let backend = Arc::new(MockTreetopBackend::new());
        for action in [Permissions::ReadObject, Permissions::UpdateObject] {
            backend.add_rule(MockAllowRule {
                group_id: group.id,
                action,
                resource_kind: ResourceKind::Object,
                resource_id: Some(fixture.objects[0].id),
                attrs: ResourceAttrs::default(),
            });
        }
        let token = scoped_token(
            &test_context.pool,
            test_context.normal_user.id,
            &[Permissions::ReadObject],
        )
        .await;

        let response = get_with_permission_backend(
            &test_context,
            &token,
            backend,
            &format!(
                "/api/v1/classes/{}/object-groups?permissions=UpdateObject&group_by=description",
                fixture.class.id
            ),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        assert_eq!(
            header_value(&response, TOTAL_COUNT_HEADER).as_deref(),
            Some("0")
        );
        let rows: Vec<serde_json::Value> = test::read_body_json(response).await;
        assert!(rows.is_empty());

        fixture.cleanup().await.unwrap();
        group
            .delete_without_events(&test_context.pool)
            .await
            .unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn route_class_constraint_overrides_a_conflicting_filter(
        #[future(awt)] test_context: TestContext,
    ) {
        let route_fixture = fixture(&test_context, "route class constraint").await;
        let other_fixture = fixture(&test_context, "conflicting class filter").await;
        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!(
                "/api/v1/classes/{}/object-groups?class_id={}&group_by=name",
                route_fixture.class.id, other_fixture.class.id
            ),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        assert_eq!(
            header_value(&response, TOTAL_COUNT_HEADER).as_deref(),
            Some("0")
        );
        let rows: Vec<serde_json::Value> = test::read_body_json(response).await;
        assert!(rows.is_empty());

        route_fixture.cleanup().await.unwrap();
        other_fixture.cleanup().await.unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn standard_object_list_contract_is_unchanged(#[future(awt)] test_context: TestContext) {
        let fixture = fixture(&test_context, "unchanged object list").await;
        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!("/api/v1/classes/{}/", fixture.class.id),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let objects: Vec<HubuumObject> = test::read_body_json(response).await;

        assert_eq!(objects.len(), fixture.objects.len());

        fixture.cleanup().await.unwrap();
    }
}
