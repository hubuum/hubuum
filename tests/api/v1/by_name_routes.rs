#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test};
    use rstest::rstest;

    use crate::errors::ApiError;
    use crate::events::EventContext;
    use crate::models::traits::{
        CreateObjectInResolvedClass, ResolveClassTarget, ResolveObjectTarget, UpdateResolvedClass,
        UpdateResolvedObject,
    };
    use crate::models::{
        ClassSelector, HubuumClassExpanded, HubuumClassID, HubuumObject, NewHubuumClass,
        NewHubuumObject, ObjectSelector, RelatedClassGraph, RelatedObjectGraph, UpdateHubuumClass,
        UpdateHubuumObject,
    };
    use crate::tests::api_operations::{delete_request, get_request, patch_request, post_request};
    use crate::tests::asserts::assert_response_status;
    use crate::tests::{ObjectFixture, TestContext, test_context};
    use crate::traits::{CanSave, SelfAccessors};

    fn encode_path_segment(value: &str) -> String {
        percent_encoding::utf8_percent_encode(value, percent_encoding::NON_ALPHANUMERIC).to_string()
    }

    fn class_by_name_path(class_name: &str) -> String {
        format!(
            "/api/v1/classes/by-name/{}",
            encode_path_segment(class_name)
        )
    }

    fn object_by_name_path(class_name: &str, object_name: &str) -> String {
        format!(
            "{}/objects/by-name/{}",
            class_by_name_path(class_name),
            encode_path_segment(object_name)
        )
    }

    async fn fixture(context: &TestContext, label: &str) -> ObjectFixture {
        context
            .object_fixture(
                label,
                NewHubuumClass {
                    collection_id: 0,
                    name: context.scoped_name("by-name class"),
                    description: "by-name class".to_string(),
                    json_schema: None,
                    validate_schema: Some(false),
                },
                vec![
                    NewHubuumObject {
                        collection_id: 0,
                        hubuum_class_id: 0,
                        name: context.scoped_name("by-name object"),
                        description: "first object".to_string(),
                        data: serde_json::json!({"ordinal": 1}),
                    },
                    NewHubuumObject {
                        collection_id: 0,
                        hubuum_class_id: 0,
                        name: context.scoped_name("by-name decoy object"),
                        description: "second object".to_string(),
                        data: serde_json::json!({"ordinal": 2}),
                    },
                ],
            )
            .await
            .unwrap()
    }

    #[rstest]
    #[actix_web::test]
    async fn explicit_name_routes_cover_current_resource_reads_and_writes(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "name route coverage").await;
        let decoy_class = NewHubuumClass {
            collection_id: fixture.collection_id(),
            name: test_context.scoped_name("numeric class id decoy"),
            description: "numeric class id decoy".to_string(),
            json_schema: None,
            validate_schema: Some(false),
        }
        .save_without_events(&test_context.pool)
        .await
        .unwrap();

        let mut named_class = fixture.class.clone();
        named_class.name = decoy_class.id.to_string();
        named_class = named_class
            .save_without_events(&test_context.pool)
            .await
            .unwrap();
        let mut named_object = fixture.objects[0].clone();
        named_object.name = fixture.objects[1].id.to_string();
        named_object = named_object
            .save_without_events(&test_context.pool)
            .await
            .unwrap();

        let class_path = class_by_name_path(&named_class.name);
        let object_path = object_by_name_path(&named_class.name, &named_object.name);

        let response =
            get_request(&test_context.pool, &test_context.admin_token, &class_path).await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let class: HubuumClassExpanded = test::read_body_json(response).await;
        assert_eq!(class.id, named_class.id);
        assert_ne!(class.id, decoy_class.id);

        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!("{class_path}/objects"),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let objects: Vec<HubuumObject> = test::read_body_json(response).await;
        assert!(objects.iter().any(|object| object.id == named_object.id));

        let response =
            get_request(&test_context.pool, &test_context.admin_token, &object_path).await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let object: HubuumObject = test::read_body_json(response).await;
        assert_eq!(object.id, named_object.id);
        assert_ne!(object.id, fixture.objects[1].id);

        for suffix in ["permissions", "related/classes", "related/relations"] {
            let response = get_request(
                &test_context.pool,
                &test_context.admin_token,
                &format!("{class_path}/{suffix}"),
            )
            .await;
            assert_response_status(response, StatusCode::OK).await;
        }
        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!("{class_path}/related/graph"),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let graph: RelatedClassGraph = test::read_body_json(response).await;
        assert_eq!(graph.classes[0].id, named_class.id);

        for suffix in ["related/objects", "related/relations"] {
            let response = get_request(
                &test_context.pool,
                &test_context.admin_token,
                &format!("{object_path}/{suffix}"),
            )
            .await;
            assert_response_status(response, StatusCode::OK).await;
        }
        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!("{object_path}/related/graph"),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let graph: RelatedObjectGraph = test::read_body_json(response).await;
        assert_eq!(graph.objects[0].id, named_object.id);

        let response = patch_request(
            &test_context.pool,
            &test_context.admin_token,
            &class_path,
            serde_json::json!({"description": "updated by name"}),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let class: HubuumClassExpanded = test::read_body_json(response).await;
        assert_eq!(class.description, "updated by name");

        let response = patch_request(
            &test_context.pool,
            &test_context.admin_token,
            &object_path,
            serde_json::json!({"description": "updated by name"}),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let object: HubuumObject = test::read_body_json(response).await;
        assert_eq!(object.description, "updated by name");

        let created_name = test_context.scoped_name("body needs no ids");
        let response = post_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!("{class_path}/objects"),
            serde_json::json!({
                "name": created_name,
                "description": "created without redundant IDs",
                "data": {"created": true}
            }),
        )
        .await;
        let response = assert_response_status(response, StatusCode::CREATED).await;
        let created: HubuumObject = test::read_body_json(response).await;
        assert_eq!(created.hubuum_class_id, named_class.id);
        assert_eq!(created.collection_id, named_class.collection_id);

        let created_path = object_by_name_path(&named_class.name, &created.name);
        let response =
            delete_request(&test_context.pool, &test_context.admin_token, &created_path).await;
        assert_response_status(response, StatusCode::NO_CONTENT).await;
        let response =
            get_request(&test_context.pool, &test_context.admin_token, &created_path).await;
        assert_response_status(response, StatusCode::NOT_FOUND).await;

        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn class_delete_by_name_deletes_the_named_class(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "class delete by name").await;
        let class_path = class_by_name_path(&fixture.class.name);

        let response =
            delete_request(&test_context.pool, &test_context.admin_token, &class_path).await;
        assert_response_status(response, StatusCode::NO_CONTENT).await;
        let error = HubuumClassID::new(fixture.class.id)
            .unwrap()
            .instance(&test_context.pool)
            .await
            .unwrap_err();
        assert!(matches!(error, ApiError::NotFound(_)));

        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn stale_name_targets_do_not_follow_concurrent_renames(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "stale name targets").await;
        let event_context = EventContext::user(test_context.admin_user.id, None, None);
        let stale_class_target = ClassSelector::by_name(fixture.class.name.clone())
            .resolve_class_target(&test_context.pool)
            .await
            .unwrap();

        let mut renamed_class = fixture.class.clone();
        renamed_class.name = test_context.scoped_name("renamed class");
        renamed_class = renamed_class
            .save_without_events(&test_context.pool)
            .await
            .unwrap();

        let error = UpdateHubuumClass {
            name: None,
            collection_id: None,
            json_schema: None,
            validate_schema: None,
            description: Some("must not update".to_string()),
        }
        .update_resolved_class(&test_context.pool, &stale_class_target, &event_context)
        .await
        .unwrap_err();
        assert!(matches!(error, ApiError::NotFound(_)));

        let error = NewHubuumObject {
            name: test_context.scoped_name("must not be created"),
            collection_id: renamed_class.collection_id,
            hubuum_class_id: renamed_class.id,
            data: serde_json::json!({}),
            description: "must not be created".to_string(),
        }
        .create_object_in_resolved_class(&test_context.pool, &stale_class_target, &event_context)
        .await
        .unwrap_err();
        assert!(matches!(error, ApiError::NotFound(_)));

        let stale_object_target =
            ObjectSelector::by_name(renamed_class.name.clone(), fixture.objects[0].name.clone())
                .resolve_object_target(&test_context.pool)
                .await
                .unwrap();
        let mut renamed_object = fixture.objects[0].clone();
        renamed_object.name = test_context.scoped_name("renamed object");
        renamed_object
            .save_without_events(&test_context.pool)
            .await
            .unwrap();

        let error = UpdateHubuumObject {
            name: None,
            collection_id: None,
            hubuum_class_id: None,
            data: None,
            description: Some("must not update".to_string()),
        }
        .update_resolved_object(&test_context.pool, &stale_object_target, &event_context)
        .await
        .unwrap_err();
        assert!(matches!(error, ApiError::NotFound(_)));

        fixture.cleanup().await.unwrap();
    }
}
