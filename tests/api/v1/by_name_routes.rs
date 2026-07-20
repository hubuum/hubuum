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
        ClassSelector, HubuumClassExpanded, HubuumClassID, HubuumObject, HubuumObjectID,
        NewHubuumClass, NewHubuumObject, ObjectSelector, RelatedClassGraph, RelatedObjectGraph,
        UpdateHubuumClass, UpdateHubuumObject,
    };
    use crate::tests::api_operations::{delete_request, get_request, patch_request, post_request};
    use crate::tests::asserts::assert_response_status;
    use crate::tests::{ObjectFixture, TestContext, test_context};
    use crate::traits::{CanSave, SelfAccessors};

    #[derive(Clone, Copy)]
    enum SelectorAddressing {
        Id,
        Name,
    }

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

    fn class_selector(addressing: SelectorAddressing, fixture: &ObjectFixture) -> ClassSelector {
        match addressing {
            SelectorAddressing::Id => {
                ClassSelector::by_id(HubuumClassID::new(fixture.class.id).unwrap())
            }
            SelectorAddressing::Name => ClassSelector::by_name(fixture.class.name.clone()),
        }
    }

    fn object_selector(addressing: SelectorAddressing, fixture: &ObjectFixture) -> ObjectSelector {
        match addressing {
            SelectorAddressing::Id => ObjectSelector::by_id(
                HubuumClassID::new(fixture.class.id).unwrap(),
                HubuumObjectID::new(fixture.objects[0].id).unwrap(),
            ),
            SelectorAddressing::Name => {
                ObjectSelector::by_name(fixture.class.name.clone(), fixture.objects[0].name.clone())
            }
        }
    }

    fn class_description_update() -> UpdateHubuumClass {
        UpdateHubuumClass {
            name: None,
            collection_id: None,
            json_schema: None,
            validate_schema: None,
            description: Some("must not update".to_string()),
        }
    }

    fn object_description_update() -> UpdateHubuumObject {
        UpdateHubuumObject {
            name: None,
            collection_id: None,
            hubuum_class_id: None,
            data: None,
            description: Some("must not update".to_string()),
        }
    }

    #[rstest]
    #[actix_web::test]
    async fn numeric_looking_class_name_resolves_as_a_name(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "numeric class name").await;
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
        let class_path = class_by_name_path(&named_class.name);

        let response =
            get_request(&test_context.pool, &test_context.admin_token, &class_path).await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let class: HubuumClassExpanded = test::read_body_json(response).await;
        assert_eq!(class.id, named_class.id);
        assert_ne!(class.id, decoy_class.id);

        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn class_name_object_listing_is_scoped_to_the_named_class(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "class name object listing").await;
        let class_path = class_by_name_path(&fixture.class.name);

        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!("{class_path}/objects"),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let objects: Vec<HubuumObject> = test::read_body_json(response).await;
        assert!(
            objects
                .iter()
                .any(|object| object.id == fixture.objects[0].id)
        );

        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn numeric_looking_object_name_resolves_as_a_name(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "numeric object name").await;
        let mut named_object = fixture.objects[0].clone();
        named_object.name = fixture.objects[1].id.to_string();
        named_object = named_object
            .save_without_events(&test_context.pool)
            .await
            .unwrap();
        let object_path = object_by_name_path(&fixture.class.name, &named_object.name);

        let response =
            get_request(&test_context.pool, &test_context.admin_token, &object_path).await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let object: HubuumObject = test::read_body_json(response).await;
        assert_eq!(object.id, named_object.id);
        assert_ne!(object.id, fixture.objects[1].id);

        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[case::permissions("permissions")]
    #[case::related_classes("related/classes")]
    #[case::related_relations("related/relations")]
    #[actix_web::test]
    async fn class_related_name_alias_is_mounted(
        #[future(awt)] test_context: TestContext,
        #[case] suffix: &str,
    ) {
        let fixture = fixture(&test_context, "class related alias").await;
        let class_path = class_by_name_path(&fixture.class.name);

        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!("{class_path}/{suffix}"),
        )
        .await;
        assert_response_status(response, StatusCode::OK).await;

        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn class_graph_name_alias_uses_the_named_class(#[future(awt)] test_context: TestContext) {
        let fixture = fixture(&test_context, "class graph alias").await;
        let class_path = class_by_name_path(&fixture.class.name);

        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!("{class_path}/related/graph"),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let graph: RelatedClassGraph = test::read_body_json(response).await;
        assert_eq!(graph.classes[0].id, fixture.class.id);

        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[case::related_objects("related/objects")]
    #[case::related_relations("related/relations")]
    #[actix_web::test]
    async fn object_related_name_alias_is_mounted(
        #[future(awt)] test_context: TestContext,
        #[case] suffix: &str,
    ) {
        let fixture = fixture(&test_context, "object related alias").await;
        let object_path = object_by_name_path(&fixture.class.name, &fixture.objects[0].name);

        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!("{object_path}/{suffix}"),
        )
        .await;
        assert_response_status(response, StatusCode::OK).await;

        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn object_graph_name_alias_uses_the_named_object(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "object graph alias").await;
        let object_path = object_by_name_path(&fixture.class.name, &fixture.objects[0].name);

        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!("{object_path}/related/graph"),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let graph: RelatedObjectGraph = test::read_body_json(response).await;
        assert_eq!(graph.objects[0].id, fixture.objects[0].id);

        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn class_update_by_name_updates_the_named_class(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "class update by name").await;
        let class_path = class_by_name_path(&fixture.class.name);

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

        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn object_update_by_name_updates_the_named_object(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "object update by name").await;
        let object_path = object_by_name_path(&fixture.class.name, &fixture.objects[0].name);

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

        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn object_create_by_class_name_does_not_require_redundant_ids(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "object create by class name").await;
        let class_path = class_by_name_path(&fixture.class.name);

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
        assert_eq!(created.hubuum_class_id, fixture.class.id);
        assert_eq!(created.collection_id, fixture.class.collection_id);

        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn object_delete_by_name_deletes_the_named_object(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "object delete by name").await;
        let created = &fixture.objects[0];
        let created_path = object_by_name_path(&fixture.class.name, &created.name);

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
    async fn class_name_alias_uses_the_same_permissions_as_the_id_route(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "class name alias permissions").await;
        fixture
            .collection
            .owner_group
            .add_member_without_events(&test_context.pool, &test_context.normal_user)
            .await
            .unwrap();

        let response = get_request(
            &test_context.pool,
            &test_context.normal_token,
            &class_by_name_path(&fixture.class.name),
        )
        .await;
        assert_response_status(response, StatusCode::OK).await;

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
    #[case::by_id(SelectorAddressing::Id)]
    #[case::by_name(SelectorAddressing::Name)]
    async fn resolved_class_target_rejects_a_concurrent_rename(
        #[future(awt)] test_context: TestContext,
        #[case] addressing: SelectorAddressing,
    ) {
        let fixture = fixture(&test_context, "stale class rename").await;
        let event_context = EventContext::user(test_context.admin_user.id, None, None);
        let stale_class_target = class_selector(addressing, &fixture)
            .resolve_class_target(&test_context.pool)
            .await
            .unwrap();

        let mut renamed_class = fixture.class.clone();
        renamed_class.name = test_context.scoped_name("renamed class");
        renamed_class
            .save_without_events(&test_context.pool)
            .await
            .unwrap();

        let error = class_description_update()
            .update_resolved_class(&test_context.pool, &stale_class_target, &event_context)
            .await
            .unwrap_err();
        assert!(matches!(error, ApiError::NotFound(_)));

        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn resolved_class_name_target_rejects_object_creation_after_a_rename(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = fixture(&test_context, "stale class create").await;
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

        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[case::by_id(SelectorAddressing::Id)]
    #[case::by_name(SelectorAddressing::Name)]
    #[actix_web::test]
    async fn resolved_class_target_rejects_a_concurrent_collection_move(
        #[future(awt)] test_context: TestContext,
        #[case] addressing: SelectorAddressing,
    ) {
        let fixture = fixture(&test_context, "stale class collection").await;
        let destination = test_context
            .collection_fixture("class move destination")
            .await;
        let event_context = EventContext::user(test_context.admin_user.id, None, None);
        let stale_class_target = class_selector(addressing, &fixture)
            .resolve_class_target(&test_context.pool)
            .await
            .unwrap();
        let mut moved_class = fixture.class.clone();
        moved_class.collection_id = destination.collection_id();
        moved_class
            .save_without_events(&test_context.pool)
            .await
            .unwrap();

        let error = class_description_update()
            .update_resolved_class(&test_context.pool, &stale_class_target, &event_context)
            .await
            .unwrap_err();
        assert!(matches!(error, ApiError::NotFound(_)));

        fixture.cleanup().await.unwrap();
        destination.cleanup().await.unwrap();
    }

    #[rstest]
    #[case::by_id(SelectorAddressing::Id)]
    #[case::by_name(SelectorAddressing::Name)]
    #[actix_web::test]
    async fn resolved_object_target_rejects_a_concurrent_rename(
        #[future(awt)] test_context: TestContext,
        #[case] addressing: SelectorAddressing,
    ) {
        let fixture = fixture(&test_context, "stale object rename").await;
        let event_context = EventContext::user(test_context.admin_user.id, None, None);
        let stale_object_target = object_selector(addressing, &fixture)
            .resolve_object_target(&test_context.pool)
            .await
            .unwrap();
        let mut renamed_object = fixture.objects[0].clone();
        renamed_object.name = test_context.scoped_name("renamed object");
        renamed_object
            .save_without_events(&test_context.pool)
            .await
            .unwrap();

        let error = object_description_update()
            .update_resolved_object(&test_context.pool, &stale_object_target, &event_context)
            .await
            .unwrap_err();
        assert!(matches!(error, ApiError::NotFound(_)));

        fixture.cleanup().await.unwrap();
    }
}
