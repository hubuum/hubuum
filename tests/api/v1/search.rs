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
        let needle = context.scoped_name("unifiedsearchserver").replace('_', "");
        let collection = context.collection_fixture(&needle).await;

        let class = NewHubuumClass {
            name: needle.clone(),
            collection_id: collection.collection.id,
            json_schema: None,
            validate_schema: Some(false),
            description: format!("{needle} inventory"),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();

        NewHubuumObject {
            name: format!("{needle}-object"),
            collection_id: collection.collection.id,
            hubuum_class_id: class.id,
            data: serde_json::json!({"hostname": format!("{needle}-object")}),
            description: format!("{needle} object description"),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{SEARCH_ENDPOINT}?q={needle}&kinds=collection,class,object"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let search: UnifiedSearchResponse = test::read_body_json(resp).await;

        assert!(!search.results.collections.is_empty());
        assert_eq!(search.results.classes.len(), 1);
        assert_eq!(search.results.objects.len(), 1);

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{SEARCH_ENDPOINT}?q={needle}&kinds=class"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let search: UnifiedSearchResponse = test::read_body_json(resp).await;
        assert!(search.results.collections.is_empty());
        assert_eq!(search.results.classes.len(), 1);
        assert!(search.results.objects.is_empty());

        collection.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_search_class_schema_toggle(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let collection = context.collection_fixture("unified_schema_toggle").await;

        let class = NewHubuumClass {
            name: "ordinary-class".to_string(),
            collection_id: collection.collection.id,
            json_schema: Some(serde_json::json!({
                "type": "object",
                "properties": { "role": { "type": "string", "description": "schemaonlyneedle" } }
            })),
            validate_schema: Some(false),
            description: "generic class".to_string(),
        }
        .save_without_events(&context.pool)
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

        collection.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_search_object_data_toggle(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let collection = context
            .collection_fixture("unified_object_data_toggle")
            .await;

        let class = NewHubuumClass {
            name: "search-data-class".to_string(),
            collection_id: collection.collection.id,
            json_schema: None,
            validate_schema: Some(false),
            description: "class for data search".to_string(),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();

        let object = NewHubuumObject {
            name: "ordinary-object".to_string(),
            collection_id: collection.collection.id,
            hubuum_class_id: class.id,
            data: serde_json::json!({"owner": "jsononlyneedle", "ignored_key": "value"}),
            description: "generic object".to_string(),
        }
        .save_without_events(&context.pool)
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

        collection.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_search_per_kind_cursor_pagination(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let collection = context.collection_fixture("unified_cursor_page").await;

        for idx in 0..3 {
            NewHubuumClass {
                name: format!("cursorclass{idx}"),
                collection_id: collection.collection.id,
                json_schema: None,
                validate_schema: Some(false),
                description: "cursorclass description".to_string(),
            }
            .save_without_events(&context.pool)
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

        collection.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_search_permission_scoping(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let visible = context.collection_fixture("permneedle_visible").await;
        let hidden = context.collection_fixture("permneedle_hidden").await;

        visible
            .owner_group
            .add_member_without_events(&context.pool, &context.normal_user)
            .await
            .unwrap();

        for collection in [&visible, &hidden] {
            let class = NewHubuumClass {
                name: format!("permneedle-class-{}", collection.collection.id),
                collection_id: collection.collection.id,
                json_schema: None,
                validate_schema: Some(false),
                description: "permneedle class".to_string(),
            }
            .save_without_events(&context.pool)
            .await
            .unwrap();

            NewHubuumObject {
                name: format!("permneedle-object-{}", collection.collection.id),
                collection_id: collection.collection.id,
                hubuum_class_id: class.id,
                data: serde_json::json!({"tag": "permneedle"}),
                description: "permneedle object".to_string(),
            }
            .save_without_events(&context.pool)
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

        assert_eq!(search.results.collections.len(), 1);
        assert!(
            search
                .results
                .collections
                .iter()
                .all(|collection| collection.id == visible.collection.id)
        );
        assert!(
            search
                .results
                .classes
                .iter()
                .all(|class| class.collection.id == visible.collection.id)
        );
        assert!(
            search
                .results
                .objects
                .iter()
                .all(|object| object.collection_id == visible.collection.id)
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
        let collection = context.collection_fixture("streamneedle").await;

        let class = NewHubuumClass {
            name: "streamneedle-class".to_string(),
            collection_id: collection.collection.id,
            json_schema: None,
            validate_schema: Some(false),
            description: "streamneedle class".to_string(),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();

        NewHubuumObject {
            name: "streamneedle-object".to_string(),
            collection_id: collection.collection.id,
            hubuum_class_id: class.id,
            data: serde_json::json!({"label": "streamneedle"}),
            description: "streamneedle object".to_string(),
        }
        .save_without_events(&context.pool)
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

        collection.cleanup().await.unwrap();
    }
}
