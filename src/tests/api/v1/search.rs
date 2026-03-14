#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test};
    use rstest::rstest;

    use crate::models::{NewHubuumClass, NewHubuumObject, UnifiedSearchResponse};
    use crate::tests::api_operations::get_request;
    use crate::tests::asserts::assert_response_status;
    use crate::tests::{TestContext, test_context};
    use crate::traits::CanSave;

    const SEARCH_ENDPOINT: &str = "/api/v1/search";
    const SEARCH_STREAM_ENDPOINT: &str = "/api/v1/search/stream";

    #[rstest]
    #[actix_web::test]
    async fn test_api_search_grouped_results_and_kind_filter(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let namespace = context.namespace_fixture("unified_search_server").await;

        let class = NewHubuumClass {
            name: "server".to_string(),
            namespace_id: namespace.namespace.id,
            json_schema: None,
            validate_schema: Some(false),
            description: "server inventory".to_string(),
        }
        .save(&context.pool)
        .await
        .unwrap();

        NewHubuumObject {
            name: "server-object".to_string(),
            namespace_id: namespace.namespace.id,
            hubuum_class_id: class.id,
            data: serde_json::json!({"hostname": "server-object"}),
            description: "server object description".to_string(),
        }
        .save(&context.pool)
        .await
        .unwrap();

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{SEARCH_ENDPOINT}?q=server&kinds=namespace,class,object"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let search: UnifiedSearchResponse = test::read_body_json(resp).await;

        assert!(!search.results.namespaces.is_empty());
        assert_eq!(search.results.classes.len(), 1);
        assert_eq!(search.results.objects.len(), 1);

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{SEARCH_ENDPOINT}?q=server&kinds=class"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let search: UnifiedSearchResponse = test::read_body_json(resp).await;
        assert!(search.results.namespaces.is_empty());
        assert_eq!(search.results.classes.len(), 1);
        assert!(search.results.objects.is_empty());

        namespace.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_search_class_schema_toggle(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let namespace = context.namespace_fixture("unified_schema_toggle").await;

        let class = NewHubuumClass {
            name: "ordinary-class".to_string(),
            namespace_id: namespace.namespace.id,
            json_schema: Some(serde_json::json!({
                "type": "object",
                "properties": { "role": { "type": "string", "description": "schemaonlyneedle" } }
            })),
            validate_schema: Some(false),
            description: "generic class".to_string(),
        }
        .save(&context.pool)
        .await
        .unwrap();

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{SEARCH_ENDPOINT}?q=schemaonlyneedle&kinds=class"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let search: UnifiedSearchResponse = test::read_body_json(resp).await;
        assert!(search.results.classes.is_empty());

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{SEARCH_ENDPOINT}?q=schemaonlyneedle&kinds=class&search_class_schema=true"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let search: UnifiedSearchResponse = test::read_body_json(resp).await;
        assert_eq!(search.results.classes.len(), 1);
        assert_eq!(search.results.classes[0].id, class.id);

        namespace.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_search_object_data_toggle(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let namespace = context
            .namespace_fixture("unified_object_data_toggle")
            .await;

        let class = NewHubuumClass {
            name: "search-data-class".to_string(),
            namespace_id: namespace.namespace.id,
            json_schema: None,
            validate_schema: Some(false),
            description: "class for data search".to_string(),
        }
        .save(&context.pool)
        .await
        .unwrap();

        let object = NewHubuumObject {
            name: "ordinary-object".to_string(),
            namespace_id: namespace.namespace.id,
            hubuum_class_id: class.id,
            data: serde_json::json!({"owner": "jsononlyneedle", "ignored_key": "value"}),
            description: "generic object".to_string(),
        }
        .save(&context.pool)
        .await
        .unwrap();

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{SEARCH_ENDPOINT}?q=jsononlyneedle&kinds=object"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let search: UnifiedSearchResponse = test::read_body_json(resp).await;
        assert!(search.results.objects.is_empty());

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{SEARCH_ENDPOINT}?q=jsononlyneedle&kinds=object&search_object_data=true"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let search: UnifiedSearchResponse = test::read_body_json(resp).await;
        assert_eq!(search.results.objects.len(), 1);
        assert_eq!(search.results.objects[0].id, object.id);

        namespace.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_search_per_kind_cursor_pagination(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let namespace = context.namespace_fixture("unified_cursor_page").await;

        for idx in 0..3 {
            NewHubuumClass {
                name: format!("cursorclass{idx}"),
                namespace_id: namespace.namespace.id,
                json_schema: None,
                validate_schema: Some(false),
                description: "cursorclass description".to_string(),
            }
            .save(&context.pool)
            .await
            .unwrap();
        }

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{SEARCH_ENDPOINT}?q=cursorclass&kinds=class&limit_per_kind=2"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let first_page: UnifiedSearchResponse = test::read_body_json(resp).await;
        assert_eq!(first_page.results.classes.len(), 2);
        assert!(first_page.next.classes.is_some());

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{SEARCH_ENDPOINT}?q=cursorclass&kinds=class&limit_per_kind=2&cursor_classes={}",
                first_page.next.classes.clone().unwrap()
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let second_page: UnifiedSearchResponse = test::read_body_json(resp).await;
        assert_eq!(second_page.results.classes.len(), 1);
        assert!(second_page.next.classes.is_none());

        namespace.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_search_permission_scoping(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let visible = context.namespace_fixture("permneedle_visible").await;
        let hidden = context.namespace_fixture("permneedle_hidden").await;

        visible
            .owner_group
            .add_member(&context.pool, &context.normal_user)
            .await
            .unwrap();

        for namespace in [&visible, &hidden] {
            let class = NewHubuumClass {
                name: format!("permneedle-class-{}", namespace.namespace.id),
                namespace_id: namespace.namespace.id,
                json_schema: None,
                validate_schema: Some(false),
                description: "permneedle class".to_string(),
            }
            .save(&context.pool)
            .await
            .unwrap();

            NewHubuumObject {
                name: format!("permneedle-object-{}", namespace.namespace.id),
                namespace_id: namespace.namespace.id,
                hubuum_class_id: class.id,
                data: serde_json::json!({"tag": "permneedle"}),
                description: "permneedle object".to_string(),
            }
            .save(&context.pool)
            .await
            .unwrap();
        }

        let resp = get_request(
            &context.pool,
            &context.normal_token,
            &format!("{SEARCH_ENDPOINT}?q=permneedle&search_object_data=true"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let search: UnifiedSearchResponse = test::read_body_json(resp).await;

        assert_eq!(search.results.namespaces.len(), 1);
        assert!(
            search
                .results
                .namespaces
                .iter()
                .all(|namespace| namespace.id == visible.namespace.id)
        );
        assert!(
            search
                .results
                .classes
                .iter()
                .all(|class| class.namespace.id == visible.namespace.id)
        );
        assert!(
            search
                .results
                .objects
                .iter()
                .all(|object| object.namespace_id == visible.namespace.id)
        );

        visible.cleanup().await.unwrap();
        hidden.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_search_invalid_requests(#[future(awt)] test_context: TestContext) {
        let context = test_context;

        let resp = get_request(&context.pool, &context.admin_token, SEARCH_ENDPOINT).await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{SEARCH_ENDPOINT}?q=server&search_object_data=maybe"),
        )
        .await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{SEARCH_ENDPOINT}?q=server&kinds=banana"),
        )
        .await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_search_stream_events(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let namespace = context.namespace_fixture("streamneedle").await;

        let class = NewHubuumClass {
            name: "streamneedle-class".to_string(),
            namespace_id: namespace.namespace.id,
            json_schema: None,
            validate_schema: Some(false),
            description: "streamneedle class".to_string(),
        }
        .save(&context.pool)
        .await
        .unwrap();

        NewHubuumObject {
            name: "streamneedle-object".to_string(),
            namespace_id: namespace.namespace.id,
            hubuum_class_id: class.id,
            data: serde_json::json!({"label": "streamneedle"}),
            description: "streamneedle object".to_string(),
        }
        .save(&context.pool)
        .await
        .unwrap();

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{SEARCH_STREAM_ENDPOINT}?q=streamneedle&kinds=class,object&search_object_data=true"
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let body = String::from_utf8(test::read_body(resp).await.to_vec()).unwrap();

        assert!(body.contains("event: started"));
        assert!(body.contains("event: batch"));
        assert!(body.contains("\"kind\":\"classes\""));
        assert!(body.contains("\"kind\":\"objects\""));
        assert!(body.contains("event: done"));

        namespace.cleanup().await.unwrap();
    }
}
