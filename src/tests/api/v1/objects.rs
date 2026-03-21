#[cfg(test)]
mod tests {
    use diesel::prelude::*;
    use diesel::sql_types::Text;
    use rstest::rstest;

    use crate::db::with_connection;
    use crate::models::{HubuumObject, NewHubuumClass, NewHubuumObject, UpdateHubuumObject};
    use crate::traits::{CanDelete, CanSave};
    use actix_web::{http::StatusCode, test};

    use crate::pagination::NEXT_CURSOR_HEADER;
    use crate::tests::api_operations::{delete_request, get_request, patch_request, post_request};
    use crate::tests::asserts::{assert_response_status, header_value};
    use crate::tests::constants::{SchemaType, get_schema};
    use crate::tests::{ObjectFixture, TestContext, create_object_fixture, test_context};
    // use crate::{assert_contains_all, assert_contains_same_ids};

    use crate::tests::api::v1::classes::tests::{cleanup, create_test_classes};

    const OBJECT_ENDPOINT: &str = "/api/v1/classes";
    fn object_in_class_endpoint(class_id: i32, object_id: i32) -> String {
        format!("{OBJECT_ENDPOINT}/{class_id}/{object_id}")
    }

    fn objects_in_class_endpoint(class_id: i32) -> String {
        format!("{OBJECT_ENDPOINT}/{class_id}/")
    }

    #[derive(QueryableByName)]
    struct ExplainRow {
        #[diesel(sql_type = Text)]
        #[diesel(column_name = "QUERY PLAN")]
        query_plan: String,
    }

    async fn create_test_objects(
        context: &TestContext,
        prefix: &str,
        count: usize,
    ) -> ObjectFixture {
        let class = NewHubuumClass {
            namespace_id: 0,
            name: format!("test class {prefix}"),
            description: "Test class description".to_string(),
            json_schema: None,
            validate_schema: None,
        };

        let mut objects = Vec::new();

        for i in 0..count {
            objects.push(NewHubuumObject {
                namespace_id: 0,
                hubuum_class_id: 0,
                data: serde_json::json!({"test": format!("data_{i}")}),
                name: format!("{prefix} test object {i}"),
                description: format!("{prefix} test object description {i}"),
            });
        }

        create_object_fixture(
            &context.pool,
            context.namespace_fixture(prefix).await,
            class,
            objects,
        )
        .await
        .unwrap()
    }

    async fn create_json_ip_filter_fixture(
        context: &TestContext,
        prefix: &str,
    ) -> (crate::tests::NamespaceFixture, crate::models::HubuumClass) {
        let namespace = context.namespace_fixture(prefix).await;
        let class = NewHubuumClass {
            namespace_id: namespace.namespace.id,
            name: format!("json ip filter class {prefix}"),
            description: format!("json ip filter class {prefix}"),
            json_schema: None,
            validate_schema: None,
        }
        .save(&context.pool)
        .await
        .unwrap();

        let test_objects = [
            (
                "network_filter_object_0",
                serde_json::json!({
                    "network": { "address": "10.0.0.10" }
                }),
            ),
            (
                "network_filter_object_1",
                serde_json::json!({
                    "network": { "address": "10.0.0.0/24" }
                }),
            ),
            (
                "network_filter_object_2",
                serde_json::json!({
                    "network": { "address": "10.0.0.0/25" }
                }),
            ),
            (
                "network_filter_object_3",
                serde_json::json!({
                    "network": { "address": "not-an-ip" }
                }),
            ),
            (
                "network_filter_object_4",
                serde_json::json!({
                    "network": { "address": "2001:db8::10" }
                }),
            ),
            (
                "network_filter_object_5",
                serde_json::json!({
                    "network": { "address": "10.0.1.10" }
                }),
            ),
            (
                "network_filter_object_6",
                serde_json::json!({
                    "hostname": "missing-network"
                }),
            ),
        ];

        for (name, data) in test_objects {
            NewHubuumObject {
                namespace_id: namespace.namespace.id,
                hubuum_class_id: class.id,
                data,
                name: name.to_string(),
                description: name.to_string(),
            }
            .save(&context.pool)
            .await
            .unwrap();
        }

        (namespace, class)
    }

    async fn assert_json_ip_filter_query(
        context: &TestContext,
        prefix: &str,
        query_string: &str,
        expected_names: Vec<&str>,
    ) {
        let (namespace, class) = create_json_ip_filter_fixture(context, prefix).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{OBJECT_ENDPOINT}/{}/?namespaces={}&{}&sort=id",
                class.id, namespace.namespace.id, query_string
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let objects: Vec<HubuumObject> = test::read_body_json(resp).await;

        let object_names: Vec<&str> = objects.iter().map(|object| object.name.as_str()).collect();
        assert_eq!(object_names, expected_names);

        namespace.cleanup().await.unwrap();
    }

    async fn create_json_typed_filter_fixture(
        context: &TestContext,
        prefix: &str,
    ) -> (crate::tests::NamespaceFixture, crate::models::HubuumClass) {
        let namespace = context.namespace_fixture(prefix).await;
        let class = NewHubuumClass {
            namespace_id: namespace.namespace.id,
            name: format!("json typed filter class {prefix}"),
            description: format!("json typed filter class {prefix}"),
            json_schema: None,
            validate_schema: Some(false),
        }
        .save(&context.pool)
        .await
        .unwrap();

        let test_objects = [
            (
                "typed_filter_object_0",
                serde_json::json!({
                    "metrics": { "cpu_count": 2 },
                    "flags": { "enabled": true },
                    "maintenance": { "window_start": "2026-03-01" }
                }),
            ),
            (
                "typed_filter_object_1",
                serde_json::json!({
                    "metrics": { "cpu_count": 8 },
                    "flags": { "enabled": false },
                    "maintenance": { "window_start": "2026-03-15T08:00:00Z" }
                }),
            ),
            (
                "typed_filter_object_2",
                serde_json::json!({
                    "metrics": { "cpu_count": 16 },
                    "flags": { "enabled": true },
                    "maintenance": { "window_start": "2026-04-01T01:00:00+01:00" }
                }),
            ),
            (
                "typed_filter_object_invalid",
                serde_json::json!({
                    "metrics": { "cpu_count": "many" },
                    "flags": { "enabled": "sometimes" },
                    "maintenance": { "window_start": "not-a-date" }
                }),
            ),
            (
                "typed_filter_object_missing",
                serde_json::json!({
                    "hostname": "no-typed-fields"
                }),
            ),
        ];

        for (name, data) in test_objects {
            NewHubuumObject {
                namespace_id: namespace.namespace.id,
                hubuum_class_id: class.id,
                data,
                name: name.to_string(),
                description: name.to_string(),
            }
            .save(&context.pool)
            .await
            .unwrap();
        }

        (namespace, class)
    }

    async fn assert_json_typed_filter_query(
        context: &TestContext,
        prefix: &str,
        query_string: &str,
        expected_names: Vec<&str>,
    ) {
        let (namespace, class) = create_json_typed_filter_fixture(context, prefix).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{OBJECT_ENDPOINT}/{}/?namespaces={}&{}&sort=id",
                class.id, namespace.namespace.id, query_string
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let objects: Vec<HubuumObject> = test::read_body_json(resp).await;

        let object_names: Vec<&str> = objects.iter().map(|object| object.name.as_str()).collect();
        assert_eq!(object_names, expected_names);

        namespace.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_rt::test]
    async fn get_patch_and_delete_objects_in_class(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let classes = create_test_classes(&context, "get_patch_and_delete_objects_in_class").await;

        let class = &classes[0];

        let object = NewHubuumObject {
            namespace_id: class.namespace_id,
            hubuum_class_id: class.id,
            data: serde_json::json!({"test": "data"}),
            name: "test object".to_string(),
            description: "test object description".to_string(),
        };

        let object = object.save(&context.pool).await.unwrap();

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &object_in_class_endpoint(class.id, object.id),
        )
        .await;

        let resp = assert_response_status(resp, StatusCode::OK).await;

        let object_from_api: HubuumObject = test::read_body_json(resp).await;
        assert_eq!(object_from_api, object);

        let updated_object = UpdateHubuumObject {
            namespace_id: None,
            hubuum_class_id: None,
            data: None,
            name: Some("updated object".to_string()),
            description: None,
        };

        let resp = patch_request(
            &context.pool,
            &context.admin_token,
            &object_in_class_endpoint(class.id, object.id),
            updated_object,
        )
        .await;

        let resp = assert_response_status(resp, StatusCode::OK).await;
        let updated_object_from_req: HubuumObject = test::read_body_json(resp).await;
        assert_eq!(updated_object_from_req.name, "updated object");

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &object_in_class_endpoint(class.id, object.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let updated_object_from_api: HubuumObject = test::read_body_json(resp).await;

        assert_eq!(updated_object_from_api, updated_object_from_req);

        let resp = delete_request(
            &context.pool,
            &context.admin_token,
            &object_in_class_endpoint(class.id, object.id),
        )
        .await;

        assert_response_status(resp, StatusCode::NO_CONTENT).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &object_in_class_endpoint(class.id, object.id),
        )
        .await;
        assert_response_status(resp, StatusCode::NOT_FOUND).await;

        cleanup(&classes).await;
    }

    // This will create objects with the same name but potentially in differnet classes.
    // This is to test that the name is unique within the class.
    // [class_idx1, class_idx2] [expected_status1, expected_status2]
    #[rstest]
    #[case::class_0_0_conflict([0, 0], [StatusCode::CREATED, StatusCode::CONFLICT])]
    #[case::class_0_1_ok([0, 1], [StatusCode::CREATED, StatusCode::CREATED])]
    #[case::class_0_2_ok([0, 2], [StatusCode::CREATED, StatusCode::CREATED])]
    #[case::class_1_1_conflict([1, 1], [StatusCode::CREATED, StatusCode::CONFLICT])]
    #[case::class_2_2_conflict([2, 2], [StatusCode::CREATED, StatusCode::CONFLICT])]
    #[actix_web::test]
    async fn create_object_in_class(
        #[case] class_ids: [i32; 2],
        #[case] expected_statuses: [StatusCode; 2],
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let literal = format!(
            "create_object_in_class_{}",
            class_ids
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<String>>()
                .join("_")
        );
        let classes = create_test_classes(&context, &literal).await;

        for (class_id, expected_status) in class_ids.iter().zip(expected_statuses.iter()) {
            let class = &classes[*class_id as usize];

            let object = NewHubuumObject {
                namespace_id: class.namespace_id,
                hubuum_class_id: class.id,
                data: serde_json::json!({"test": "data"}),
                name: "test create object".to_string(),
                description: "test create object description".to_string(),
            };

            let resp = post_request(
                &context.pool,
                &context.admin_token,
                &format!("{}/{}/", OBJECT_ENDPOINT, class.id),
                &object,
            )
            .await;

            let resp = assert_response_status(resp, *expected_status).await;

            if expected_status == &StatusCode::CREATED {
                let headers = resp.headers().clone();

                let object_from_api: HubuumObject = test::read_body_json(resp).await;
                assert_eq!(object_from_api.name, object.name);
                assert_eq!(object_from_api.description, object.description);
                assert_eq!(object_from_api.data, object.data);
                assert_eq!(object_from_api.namespace_id, object.namespace_id);
                assert_eq!(object_from_api.hubuum_class_id, object.hubuum_class_id);

                let object_url = format!("{}/{}/{}", OBJECT_ENDPOINT, class.id, object_from_api.id);

                let created_object_url = headers.get("Location").unwrap().to_str().unwrap();
                assert_eq!(created_object_url, object_url);
            }
        }
        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_rt::test]
    async fn get_objects_in_class(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let classes = create_test_classes(&context, "get_objects_in_class").await;

        let class = &classes[0];

        let mut objects = vec![];

        for i in 0..5 {
            let object = NewHubuumObject {
                namespace_id: class.namespace_id,
                hubuum_class_id: classes[0].id,
                data: serde_json::json!({"test": format!("data_{i}")}),
                name: format!("test get objects {i}"),
                description: format!("test object description {i}"),
            };
            objects.push(object.save(&context.pool).await.unwrap());
        }

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &objects_in_class_endpoint(class.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let objects_from_api: Vec<HubuumObject> = test::read_body_json(resp).await;

        assert_eq!(objects_from_api.len(), objects.len());
        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_rt::test]
    async fn admin_can_list_objects_without_direct_owner_group_membership(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let hidden_fixture = create_object_fixture(
            &context.pool,
            context
                .scope
                .namespace_fixture("admin_lists_hidden_objects")
                .await,
            NewHubuumClass {
                namespace_id: 0,
                name: "hidden object class".to_string(),
                description: "hidden object class".to_string(),
                json_schema: None,
                validate_schema: Some(false),
            },
            vec![
                NewHubuumObject {
                    namespace_id: 0,
                    hubuum_class_id: 0,
                    data: serde_json::json!({"name": "hidden-object-1"}),
                    name: "hidden object 1".to_string(),
                    description: "hidden object 1".to_string(),
                },
                NewHubuumObject {
                    namespace_id: 0,
                    hubuum_class_id: 0,
                    data: serde_json::json!({"name": "hidden-object-2"}),
                    name: "hidden object 2".to_string(),
                    description: "hidden object 2".to_string(),
                },
            ],
        )
        .await
        .unwrap();

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{OBJECT_ENDPOINT}/{}/?namespaces={}&sort=id",
                hidden_fixture.class.id, hidden_fixture.namespace.namespace.id
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let objects_from_api: Vec<HubuumObject> = test::read_body_json(resp).await;

        assert_eq!(
            objects_from_api
                .iter()
                .map(|object| object.id)
                .collect::<Vec<_>>(),
            hidden_fixture
                .iter()
                .map(|object| object.id)
                .collect::<Vec<_>>()
        );

        hidden_fixture.cleanup().await.unwrap();
    }

    // Covers docs/querying.md "JSON filtering" object `json_data` examples.
    #[rstest]
    #[case::filter_status_equals(
        "json_data__equals=status=active",
        vec!["json_filter_object_0", "json_filter_object_2"]
    )]
    #[case::filter_hostname_contains(
        "json_data__contains=hostname=srv",
        vec!["json_filter_object_0", "json_filter_object_1"]
    )]
    #[case::filter_missing_path("json_data__equals=missing=value", vec![])]
    #[actix_web::test]
    async fn docs_api_objects_filter_json_data_examples(
        #[case] query_string: &str,
        #[case] expected_names: Vec<&str>,
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;

        let namespace_name = format!(
            "test_api_objects_filter_json_data_examples_{}",
            query_string
                .chars()
                .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
                .collect::<String>()
        );
        let namespace = context.namespace_fixture(&namespace_name).await;
        let class = NewHubuumClass {
            namespace_id: namespace.namespace.id,
            name: format!("json filter class {namespace_name}"),
            description: format!("json filter class {namespace_name}"),
            json_schema: None,
            validate_schema: None,
        }
        .save(&context.pool)
        .await
        .unwrap();

        let test_objects = [
            (
                "json_filter_object_0",
                serde_json::json!({
                    "hostname": "srv-01",
                    "status": "active",
                    "ip": "10.0.0.10"
                }),
            ),
            (
                "json_filter_object_1",
                serde_json::json!({
                    "hostname": "srv-02",
                    "status": "inactive",
                    "ip": "10.0.0.11"
                }),
            ),
            (
                "json_filter_object_2",
                serde_json::json!({
                    "hostname": "db-01",
                    "status": "active",
                    "ip": "10.0.0.12"
                }),
            ),
        ];

        for (name, data) in test_objects {
            NewHubuumObject {
                namespace_id: namespace.namespace.id,
                hubuum_class_id: class.id,
                data,
                name: name.to_string(),
                description: name.to_string(),
            }
            .save(&context.pool)
            .await
            .unwrap();
        }

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{OBJECT_ENDPOINT}/{}/?namespaces={}&{}&sort=id",
                class.id, namespace.namespace.id, query_string
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let objects: Vec<HubuumObject> = test::read_body_json(resp).await;

        let object_names: Vec<&str> = objects.iter().map(|object| object.name.as_str()).collect();
        assert_eq!(object_names, expected_names);

        namespace.cleanup().await.unwrap();
    }

    // Covers docs/querying.md typed JSON examples and ensures malformed stored values do not fail the request.
    #[rstest]
    #[case::json_numeric_equals(
        "json_data__equals=metrics,cpu_count=8",
        vec!["typed_filter_object_1"]
    )]
    #[case::json_numeric_between(
        "json_data__between=metrics,cpu_count=4,16",
        vec!["typed_filter_object_1", "typed_filter_object_2"]
    )]
    #[case::json_boolean_equals(
        "json_data__equals=flags,enabled=true",
        vec!["typed_filter_object_0", "typed_filter_object_2"]
    )]
    #[case::json_date_gt(
        "json_data__gt=maintenance,window_start=2026-03-01",
        vec!["typed_filter_object_1", "typed_filter_object_2"]
    )]
    #[case::json_date_between(
        "json_data__between=maintenance,window_start=2026-03-01,2026-03-31",
        vec!["typed_filter_object_0", "typed_filter_object_1"]
    )]
    #[case::json_date_not_between(
        "json_data__not_between=maintenance,window_start=2026-03-01,2026-03-31",
        vec!["typed_filter_object_2"]
    )]
    #[actix_web::test]
    async fn docs_api_objects_filter_json_data_typed_examples(
        #[case] query_string: &str,
        #[case] expected_names: Vec<&str>,
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let unique = format!(
            "docs_api_objects_filter_json_data_typed_examples_{}",
            query_string
                .chars()
                .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
                .collect::<String>()
        );
        assert_json_typed_filter_query(&context, &unique, query_string, expected_names).await;
    }

    #[rstest]
    #[case::is_in_network(
        "json_data__is_in_network=network,address=10.0.0.0/24",
        vec![
            "network_filter_object_0",
            "network_filter_object_1",
            "network_filter_object_2"
        ]
    )]
    #[case::contains_network(
        "json_data__contains_network=network,address=10.0.0.0/25",
        vec!["network_filter_object_1", "network_filter_object_2"]
    )]
    #[case::contains_ip(
        "json_data__contains_ip=network,address=10.0.0.10",
        vec!["network_filter_object_1", "network_filter_object_2"]
    )]
    #[case::overlaps_network(
        "json_data__overlaps_network=network,address=10.0.0.64/26",
        vec!["network_filter_object_1", "network_filter_object_2"]
    )]
    #[case::ip_equals(
        "json_data__ip_equals=network,address=10.0.0.10/32",
        vec!["network_filter_object_0"]
    )]
    #[case::not_is_in_network(
        "json_data__not_is_in_network=network,address=10.0.0.0/24",
        vec!["network_filter_object_4", "network_filter_object_5"]
    )]
    #[actix_web::test]
    async fn test_api_objects_filter_json_data_ip_operators(
        #[case] query_string: &str,
        #[case] expected_names: Vec<&str>,
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let unique = format!(
            "test_api_objects_filter_json_data_ip_operators_{}",
            query_string
                .chars()
                .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
                .collect::<String>()
        );
        assert_json_ip_filter_query(&context, &unique, query_string, expected_names).await;
    }

    // Covers docs/querying.md "JSON filtering" network-aware JSON/IP operator examples.
    #[rstest]
    #[case::docs_is_in_network(
        "json_data__is_in_network=network,address=10.0.0.0/24",
        vec![
            "network_filter_object_0",
            "network_filter_object_1",
            "network_filter_object_2"
        ]
    )]
    #[case::docs_contains_network(
        "json_data__contains_network=network,address=10.0.0.0/25",
        vec!["network_filter_object_1", "network_filter_object_2"]
    )]
    #[case::docs_contains_ip(
        "json_data__contains_ip=network,address=10.0.0.10",
        vec!["network_filter_object_1", "network_filter_object_2"]
    )]
    #[case::docs_overlaps_network(
        "json_data__overlaps_network=network,address=10.0.0.64/26",
        vec!["network_filter_object_1", "network_filter_object_2"]
    )]
    #[case::docs_ip_equals(
        "json_data__ip_equals=network,address=10.0.0.10",
        vec!["network_filter_object_0"]
    )]
    #[actix_web::test]
    async fn docs_api_objects_filter_json_data_ip_examples(
        #[case] query_string: &str,
        #[case] expected_names: Vec<&str>,
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let unique = format!(
            "docs_api_objects_filter_json_data_ip_examples_{}",
            query_string
                .chars()
                .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
                .collect::<String>()
        );
        assert_json_ip_filter_query(&context, &unique, query_string, expected_names).await;
    }

    #[rstest]
    #[case("json_data__is_in_network=network,address=not-an-ip")]
    #[case("json_data__contains_ip=network,address=10.0.0.0/24")]
    #[actix_web::test]
    async fn test_api_objects_filter_json_data_ip_operators_reject_invalid_rhs(
        #[case] query_string: &str,
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let unique = format!(
            "test_api_objects_filter_json_data_ip_operators_reject_invalid_rhs_{}",
            query_string
                .chars()
                .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
                .collect::<String>()
        );
        let namespace = context.namespace_fixture(&unique).await;
        let class = NewHubuumClass {
            namespace_id: namespace.namespace.id,
            name: format!("json ip invalid filter class {unique}"),
            description: format!("json ip invalid filter class {unique}"),
            json_schema: None,
            validate_schema: None,
        }
        .save(&context.pool)
        .await
        .unwrap();

        NewHubuumObject {
            namespace_id: namespace.namespace.id,
            hubuum_class_id: class.id,
            data: serde_json::json!({
                "network": { "address": "10.0.0.10" }
            }),
            name: "network_filter_invalid_rhs".to_string(),
            description: "network_filter_invalid_rhs".to_string(),
        }
        .save(&context.pool)
        .await
        .unwrap();

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{OBJECT_ENDPOINT}/{}/?namespaces={}&{}",
                class.id, namespace.namespace.id, query_string
            ),
        )
        .await;
        let _ = assert_response_status(resp, StatusCode::BAD_REQUEST).await;

        namespace.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_objects_filter_json_data_ip_operators_large_dataset_and_expression_index(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let namespace = context
            .namespace_fixture(
                "test_api_objects_filter_json_data_ip_operators_large_dataset_and_expression_index",
            )
            .await;
        let class = NewHubuumClass {
            namespace_id: namespace.namespace.id,
            name: "json ip large dataset class".to_string(),
            description: "json ip large dataset class".to_string(),
            json_schema: None,
            validate_schema: None,
        }
        .save(&context.pool)
        .await
        .unwrap();

        let mut expected_names = Vec::new();
        for subnet in 0..4 {
            for host in 1..=150 {
                let name = format!("large_network_object_{subnet}_{host}");
                let address = format!("10.42.{subnet}.{host}");
                if subnet == 1 {
                    expected_names.push(name.clone());
                }

                NewHubuumObject {
                    namespace_id: namespace.namespace.id,
                    hubuum_class_id: class.id,
                    data: serde_json::json!({
                        "network": { "address": address }
                    }),
                    name: name.clone(),
                    description: name,
                }
                .save(&context.pool)
                .await
                .unwrap();
            }
        }

        for suffix in ["invalid", "missing", "ipv6"] {
            let (name, data) = match suffix {
                "invalid" => (
                    "large_network_invalid".to_string(),
                    serde_json::json!({ "network": { "address": "not-an-ip" } }),
                ),
                "missing" => (
                    "large_network_missing".to_string(),
                    serde_json::json!({ "hostname": "no-network" }),
                ),
                _ => (
                    "large_network_ipv6".to_string(),
                    serde_json::json!({ "network": { "address": "2001:db8::10" } }),
                ),
            };

            NewHubuumObject {
                namespace_id: namespace.namespace.id,
                hubuum_class_id: class.id,
                data,
                name: name.clone(),
                description: name,
            }
            .save(&context.pool)
            .await
            .unwrap();
        }

        with_connection(&context.pool, |conn| {
            diesel::sql_query(
                "CREATE INDEX idx_test_json_ip_expression \
                 ON hubuumobject (try_inet(data #>> '{network,address}')) \
                 WHERE try_inet(data #>> '{network,address}') IS NOT NULL",
            )
            .execute(conn)?;
            diesel::sql_query("ANALYZE hubuumobject").execute(conn)?;
            diesel::sql_query("SET enable_seqscan = off").execute(conn)?;
            Ok::<_, diesel::result::Error>(())
        })
        .unwrap();

        let plan_rows = with_connection(&context.pool, |conn| {
            diesel::sql_query(
                "EXPLAIN SELECT id FROM hubuumobject \
                 WHERE try_inet(data #>> '{network,address}') IS NOT NULL \
                 AND try_inet(data #>> '{network,address}') <<= '10.42.1.0/24'::inet",
            )
            .load::<ExplainRow>(conn)
        })
        .unwrap();
        let plan_text = plan_rows
            .iter()
            .map(|row| row.query_plan.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            plan_text.contains("idx_test_json_ip_expression"),
            "Expected EXPLAIN plan to reference idx_test_json_ip_expression, got:\n{plan_text}",
        );

        with_connection(&context.pool, |conn| {
            diesel::sql_query("SET enable_seqscan = on").execute(conn)
        })
        .unwrap();

        let mut cursor = None;
        let mut fetched_names = Vec::new();
        loop {
            let cursor_param = cursor
                .as_ref()
                .map(|value: &String| format!("&cursor={value}"))
                .unwrap_or_default();
            let resp = get_request(
                &context.pool,
                &context.admin_token,
                &format!(
                    "{OBJECT_ENDPOINT}/{}/?namespaces={}&json_data__is_in_network=network,address=10.42.1.0/24&sort=id&limit=100{}",
                    class.id, namespace.namespace.id, cursor_param
                ),
            )
            .await;
            let resp = assert_response_status(resp, StatusCode::OK).await;
            let next_cursor = header_value(&resp, NEXT_CURSOR_HEADER);
            let objects: Vec<HubuumObject> = test::read_body_json(resp).await;
            fetched_names.extend(objects.into_iter().map(|object| object.name));

            if let Some(next_cursor) = next_cursor {
                cursor = Some(next_cursor);
            } else {
                break;
            }
        }

        assert_eq!(fetched_names.len(), expected_names.len());
        assert_eq!(fetched_names, expected_names);

        namespace.cleanup().await.unwrap();
    }

    #[rstest]
    #[case::ok_40_74(r#"{"latitude": 40.7128, "longitude": -74.0060}"#, true)]
    #[case::failed_91_74(r#"{"latitude": 91, "longitude": 200}"#, false)]
    #[case::failed_neg91_74(r#"{"latitude": -91, "longitude": 200}"#, false)]
    #[case::failed_40_181(r#"{"latitude": 40.7128, "longitude": 181}"#, false)]
    #[case::failed_40_neg181(r#"{"latitude": 40.7128, "longitude": -181}"#, false)]
    #[case::failed_100_200(r#"{"latitude": 100, "longitude": 200}"#, false)]
    #[case::failed_lat_missing(r#"{"longitude": 0}"#, false)]
    #[case::failed_long_missing(r#"{"latitude": 0}"#, false)]
    #[case::ok_extra_fields(
        r#"{"latitude": 40.7128, "longitude": -74.0060, "extra_field": "value"}"#,
        true
    )]
    #[actix_web::test]
    async fn create_objects_in_class_failing_validation(
        #[case] json_data: &str,
        #[case] expected: bool,
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;

        let unique_name = format!("{json_data}_create_objects_in_class_failing_validation");

        let namespace = context.namespace_fixture(&unique_name).await;

        let schema = get_schema(SchemaType::Geo);
        let class = NewHubuumClass {
            name: unique_name.clone(),
            namespace_id: namespace.namespace.id,
            description: "Test class".to_string(),
            json_schema: Some(schema.clone()),
            validate_schema: Some(true),
        }
        .save(&context.pool)
        .await
        .unwrap();

        let object = NewHubuumObject {
            name: unique_name.clone(),
            namespace_id: namespace.namespace.id,
            hubuum_class_id: class.id,
            data: serde_json::from_str(json_data).unwrap(),
            description: "Test object".to_string(),
        };

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &format!("{}/{}/", OBJECT_ENDPOINT, class.id),
            &object,
        )
        .await;

        let resp = assert_response_status(
            resp,
            if expected {
                StatusCode::CREATED
            } else {
                StatusCode::NOT_ACCEPTABLE
            },
        )
        .await;

        if expected {
            let object_from_api: HubuumObject = test::read_body_json(resp).await;
            assert_eq!(object_from_api.name, object.name);
            assert_eq!(object_from_api.description, object.description);
            assert_eq!(object_from_api.data, object.data);
            assert_eq!(object_from_api.namespace_id, object.namespace_id);
            assert_eq!(object_from_api.hubuum_class_id, object.hubuum_class_id);
            object_from_api.delete(&context.pool).await.unwrap();
        } else {
            let error_message: serde_json::Value = test::read_body_json(resp).await;
            let error_text = error_message["error"].as_str().unwrap().to_lowercase();
            assert!(
                error_text.contains("validation error"),
                "Expected 'validation error', got: {error_text}"
            );
        }

        namespace.cleanup().await.unwrap();
    }

    #[rstest]
    #[case::sorted_id_default("id", &[0, 1, 2])]
    #[case::sorted_id_explicit_asc("id.asc", &[0, 1, 2])]
    #[case::sorted_id_descending("id.desc", &[3, 2, 1])]
    #[case::sorted_name_asc("name.asc", &[0, 1, 2])]
    #[case::sorted_name_desc("name.desc", &[3, 2, 1])]
    #[case::sorted_created_at_asc("created_at.asc", &[0, 1, 2])]
    #[case::sorted_created_at_desc("created_at.desc", &[3, 2, 1])]
    #[case::sorted_namespace_and_id_asc("namespace_id.asc,id.asc", &[0, 1, 2])]
    #[actix_web::test]
    async fn test_api_objects_sorted(
        #[case] sort_order: &str,
        #[case] expected_id_order: &[usize],
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let created_objects = create_test_objects(
            &context,
            &format!("api_objects_sorted_{sort_order}_{expected_id_order:?}"),
            4,
        )
        .await;
        let namespace_id = created_objects.namespace_id();

        let sort_order = if sort_order.is_empty() {
            ""
        } else {
            &format!("&sort={sort_order}")
        };

        let class_id = created_objects.class_id();
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{OBJECT_ENDPOINT}/{class_id}/?namespaces={namespace_id}{sort_order}"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let objects: Vec<HubuumObject> = test::read_body_json(resp).await;
        assert_eq!(objects.len(), created_objects.len());
        assert_eq!(objects[0].id, created_objects[expected_id_order[0]].id);
        assert_eq!(objects[1].id, created_objects[expected_id_order[1]].id);
        assert_eq!(objects[2].id, created_objects[expected_id_order[2]].id);
        created_objects.cleanup().await.unwrap();
    }

    #[rstest]
    #[case::limit_2(2)]
    #[case::limit_5(5)]
    #[case::limit_7(6)]
    #[actix_web::test]
    async fn test_api_objects_limit(
        #[case] limit: usize,
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let created_objects =
            create_test_objects(&context, &format!("api_objects_limit_{limit}"), 6).await;
        let namespace_id = created_objects.namespace_id();
        let class_id = created_objects.class_id();

        // Limit to 2 results
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{OBJECT_ENDPOINT}/{class_id}/?namespaces={namespace_id}&limit={limit}&sort=id"
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let objects: Vec<HubuumObject> = test::read_body_json(resp).await;
        assert_eq!(objects.len(), limit);
        created_objects.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_objects_cursor_pagination(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let created_objects = create_test_objects(&context, "api_objects_cursor", 6).await;
        let namespace_id = created_objects.namespace_id();
        let class_id = created_objects.class_id();

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{OBJECT_ENDPOINT}/{class_id}/?namespaces={namespace_id}&limit=2&sort=id"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let next_cursor = header_value(&resp, NEXT_CURSOR_HEADER);
        let objects: Vec<HubuumObject> = test::read_body_json(resp).await;

        assert_eq!(objects.len(), 2);
        assert_eq!(objects[0].id, created_objects[0].id);
        assert_eq!(objects[1].id, created_objects[1].id);
        assert!(next_cursor.is_some());

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{OBJECT_ENDPOINT}/{class_id}/?namespaces={namespace_id}&limit=2&sort=id&cursor={}",
                next_cursor.unwrap()
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let objects: Vec<HubuumObject> = test::read_body_json(resp).await;

        assert_eq!(objects.len(), 2);
        assert_eq!(objects[0].id, created_objects[2].id);
        assert_eq!(objects[1].id, created_objects[3].id);
        created_objects.cleanup().await.unwrap();
    }
}
