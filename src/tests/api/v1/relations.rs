#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test};
    use rstest::rstest;

    use crate::db::DbPool;
    use crate::models::{
        HubuumClass, HubuumClassRelation, HubuumClassWithPath, HubuumObject, HubuumObjectRelation,
        HubuumObjectWithPath, NamespaceID, NewHubuumClass, NewHubuumClassRelation,
        NewHubuumClassRelationFromClass, NewHubuumObject, NewHubuumObjectRelation, Permissions,
        RelatedClassGraph, RelatedObjectGraph,
    };
    use crate::pagination::NEXT_CURSOR_HEADER;
    use crate::traits::{CanSave, PermissionController, SelfAccessors};
    use crate::{assert_contains_all, assert_contains_same_ids};

    use crate::tests::api_operations::{delete_request, get_request, post_request};
    use crate::tests::asserts::{assert_response_status, header_value};
    use crate::tests::{
        ClassFixture, TestContext, create_class_fixture, create_test_group, ensure_normal_user,
        test_context,
    };
    // use crate::{assert_contains_all, assert_contains_same_ids};

    use crate::tests::api::v1::classes::tests::{cleanup, create_test_classes};

    const CLASS_RELATIONS_ENDPOINT: &str = "/api/v1/relations/classes";
    const OBJECT_RELATIONS_ENDPOINT: &str = "/api/v1/relations/objects";

    fn related_objects_endpoint(class_id: i32, object_id: i32) -> String {
        format!("/api/v1/classes/{class_id}/objects/{object_id}/related/objects")
    }

    fn related_classes_endpoint(class_id: i32) -> String {
        format!("/api/v1/classes/{class_id}/related/classes")
    }

    fn related_class_relations_endpoint(class_id: i32) -> String {
        format!("/api/v1/classes/{class_id}/related/relations")
    }

    fn related_class_graph_endpoint(class_id: i32) -> String {
        format!("/api/v1/classes/{class_id}/related/graph")
    }

    fn related_relations_endpoint(class_id: i32, object_id: i32) -> String {
        format!("/api/v1/classes/{class_id}/objects/{object_id}/related/relations")
    }

    fn related_graph_endpoint(class_id: i32, object_id: i32) -> String {
        format!("/api/v1/classes/{class_id}/objects/{object_id}/related/graph")
    }

    fn relation_endpoint(relation_id: i32) -> String {
        format!("{CLASS_RELATIONS_ENDPOINT}/{relation_id}")
    }

    async fn create_relation(
        pool: &DbPool,
        from_class: &HubuumClass,
        to_class: &HubuumClass,
    ) -> HubuumClassRelation {
        let relation = NewHubuumClassRelation {
            from_hubuum_class_id: from_class.id,
            to_hubuum_class_id: to_class.id,
        };

        relation.save(pool).await.unwrap()
    }

    async fn create_object_relation(
        pool: &DbPool,
        from_object: &HubuumObject,
        to_object: &HubuumObject,
        relation: &HubuumClassRelation,
    ) -> HubuumObjectRelation {
        let relation = NewHubuumObjectRelation {
            from_hubuum_object_id: from_object.id,
            to_hubuum_object_id: to_object.id,
            class_relation_id: relation.id,
        };

        relation.save(pool).await.unwrap()
    }

    async fn create_classes_and_relations(
        context: &TestContext,
        prefix: &str,
    ) -> (ClassFixture, Vec<HubuumClassRelation>) {
        let classes = create_test_classes(context, prefix).await;

        let relations = vec![
            create_relation(&context.pool, &classes[0], &classes[1]).await,
            create_relation(&context.pool, &classes[1], &classes[2]).await,
            create_relation(&context.pool, &classes[2], &classes[3]).await,
            create_relation(&context.pool, &classes[3], &classes[4]).await,
            create_relation(&context.pool, &classes[4], &classes[5]).await,
        ];

        (classes, relations)
    }

    async fn create_hidden_classes(context: &TestContext, prefix: &str) -> ClassFixture {
        create_class_fixture(
            &context.pool,
            context
                .scope
                .namespace_fixture(&format!("{prefix}_hidden_namespace"))
                .await,
            vec![
                NewHubuumClass {
                    namespace_id: 0,
                    name: format!("{prefix}_class_1"),
                    description: format!("{prefix}_class_1"),
                    json_schema: None,
                    validate_schema: Some(false),
                },
                NewHubuumClass {
                    namespace_id: 0,
                    name: format!("{prefix}_class_2"),
                    description: format!("{prefix}_class_2"),
                    json_schema: None,
                    validate_schema: Some(false),
                },
            ],
        )
        .await
        .unwrap()
    }

    async fn create_objects_in_classes(
        pool: &DbPool,
        classes: &[HubuumClass],
    ) -> Vec<HubuumObject> {
        let mut objects = Vec::new();
        for (index, class) in classes.iter().enumerate() {
            let data = match index {
                0 => serde_json::json!({
                    "role": "source-root",
                    "hostname": "root-01",
                    "env": "prod",
                    "service": "gateway"
                }),
                1 => serde_json::json!({
                    "role": "service-api",
                    "hostname": "api-01",
                    "env": "prod",
                    "service": "api"
                }),
                2 => serde_json::json!({
                    "role": "service-db",
                    "hostname": "db-01",
                    "env": "prod",
                    "service": "db"
                }),
                3 => serde_json::json!({
                    "role": "service-worker",
                    "hostname": "worker-01",
                    "env": "stage",
                    "service": "worker"
                }),
                4 => serde_json::json!({
                    "role": "service-cache",
                    "hostname": "cache-01",
                    "env": "stage",
                    "service": "cache"
                }),
                _ => serde_json::json!({
                    "role": format!("service-{index}"),
                    "hostname": format!("node-{index:02}"),
                    "env": "stage",
                    "service": "misc"
                }),
            };

            let object = NewHubuumObject {
                hubuum_class_id: class.id,
                namespace_id: class.namespace_id,
                name: format!("object_in_{}", class.name),
                description: format!("Object in class {}", class.description),
                data,
            };

            objects.push(object.save(pool).await.unwrap());
        }

        objects
    }

    #[rstest]
    #[actix_web::test]
    async fn test_get_class_relations_list(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let (classes, relations) =
            create_classes_and_relations(&context, "get_class_relations_list").await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            CLASS_RELATIONS_ENDPOINT,
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let relations_fetched_all: Vec<HubuumClassRelation> = test::read_body_json(resp).await;

        // Filter only on relations created from this test.
        let relations_in_namespace: Vec<HubuumClassRelation> = relations_fetched_all
            .iter()
            .filter(|r| {
                classes
                    .iter()
                    .any(|c| c.id == r.from_hubuum_class_id || c.id == r.to_hubuum_class_id)
            })
            .cloned()
            .collect();

        assert_contains_same_ids!(&relations, &relations_in_namespace);
        assert_contains_all!(&relations, &relations_in_namespace);

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_admin_can_list_class_relations_without_direct_owner_group_membership(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let classes = create_hidden_classes(&context, "admin_lists_hidden_class_relations").await;
        let relation = create_relation(&context.pool, &classes[0], &classes[1]).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{CLASS_RELATIONS_ENDPOINT}?from_classes={}&to_classes={}",
                classes[0].id, classes[1].id
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let relations: Vec<HubuumClassRelation> = test::read_body_json(resp).await;

        assert_eq!(relations.len(), 1);
        assert_eq!(relations[0].id, relation.id);

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_get_class_relations_sorted_and_limited(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let (classes, relations) =
            create_classes_and_relations(&context, "get_class_relations_sorted_and_limited").await;

        let class_ids = classes
            .iter()
            .map(|class| class.id.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let sorted_url =
            format!("{CLASS_RELATIONS_ENDPOINT}?from_classes={class_ids}&sort=id.desc");
        let resp = get_request(&context.pool, &context.admin_token, &sorted_url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let sorted_relations: Vec<HubuumClassRelation> = test::read_body_json(resp).await;

        assert_eq!(sorted_relations.len(), relations.len());
        assert_eq!(sorted_relations[0].id, relations[4].id);
        assert_eq!(sorted_relations[1].id, relations[3].id);
        assert_eq!(sorted_relations[2].id, relations[2].id);

        let limited_url =
            format!("{CLASS_RELATIONS_ENDPOINT}?from_classes={class_ids}&sort=id&limit=2");
        let resp = get_request(&context.pool, &context.admin_token, &limited_url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let limited_relations: Vec<HubuumClassRelation> = test::read_body_json(resp).await;

        assert_eq!(limited_relations.len(), 2);
        assert_eq!(limited_relations[0].id, relations[0].id);
        assert_eq!(limited_relations[1].id, relations[1].id);

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_get_class_relations_cursor_pagination(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let (classes, _relations) =
            create_classes_and_relations(&context, "get_class_relations_cursor").await;
        let class_ids = classes
            .iter()
            .map(|class| class.id.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{CLASS_RELATIONS_ENDPOINT}?from_classes={class_ids}&limit=2&sort=id"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let next_cursor = header_value(&resp, NEXT_CURSOR_HEADER);
        let relations: Vec<HubuumClassRelation> = test::read_body_json(resp).await;

        assert_eq!(relations.len(), 2);
        assert!(next_cursor.is_some());

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{CLASS_RELATIONS_ENDPOINT}?from_classes={class_ids}&limit=2&sort=id&cursor={}",
                next_cursor.unwrap()
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let relations: Vec<HubuumClassRelation> = test::read_body_json(resp).await;
        assert!(!relations.is_empty());

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_related_classes_bidirectional(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let classes = create_test_classes(&context, "related_classes_bidirectional").await;
        let relation_ab = create_relation(&context.pool, &classes[0], &classes[1]).await;
        let relation_bc = create_relation(&context.pool, &classes[1], &classes[2]).await;
        let relation_bd = create_relation(&context.pool, &classes[1], &classes[3]).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{}?sort=class_id", related_classes_endpoint(classes[2].id)),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let classes_fetched: Vec<HubuumClassWithPath> = test::read_body_json(resp).await;

        assert_eq!(
            classes_fetched
                .iter()
                .map(|class| class.id)
                .collect::<Vec<_>>(),
            vec![classes[0].id, classes[1].id, classes[3].id]
        );

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &related_class_relations_endpoint(classes[2].id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let relations_fetched: Vec<HubuumClassRelation> = test::read_body_json(resp).await;

        assert_eq!(
            relations_fetched
                .iter()
                .map(|relation| relation.id)
                .collect::<Vec<_>>(),
            vec![relation_bc.id]
        );

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{}?depth__lte=2&sort=class_id",
                related_class_graph_endpoint(classes[2].id)
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let graph: RelatedClassGraph = test::read_body_json(resp).await;

        assert_eq!(graph.classes[0].id, classes[2].id);
        assert_eq!(graph.classes[0].path, vec![classes[2].id]);
        assert_eq!(
            graph
                .classes
                .iter()
                .map(|class| class.id)
                .collect::<Vec<_>>(),
            vec![classes[2].id, classes[0].id, classes[1].id, classes[3].id]
        );
        assert_eq!(
            graph
                .relations
                .iter()
                .map(|relation| relation.id)
                .collect::<Vec<_>>(),
            vec![relation_ab.id, relation_bc.id, relation_bd.id]
        );

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_old_directional_class_routes_removed(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let classes = create_test_classes(&context, "old_directional_class_routes_removed").await;
        let _ = create_relation(&context.pool, &classes[0], &classes[1]).await;

        for endpoint in [
            format!("/api/v1/classes/{}/relations", classes[0].id),
            format!("/api/v1/classes/{}/relations/transitive/", classes[0].id),
            format!(
                "/api/v1/classes/{}/relations/transitive/class/{}",
                classes[0].id, classes[1].id
            ),
        ] {
            let resp = get_request(&context.pool, &context.admin_token, &endpoint).await;
            let _ = assert_response_status(resp, StatusCode::NOT_FOUND).await;
        }

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_get_class_relation(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let (classes, relations) =
            create_classes_and_relations(&context, "get_class_relation").await;
        let relation = &relations[0];

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &relation_endpoint(relation.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;

        let relation_response: HubuumClassRelation = test::read_body_json(resp).await;
        assert_eq!(relation_response.id, relation.id);

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_deleting_class_relation_from_global(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let (classes, relations) =
            create_classes_and_relations(&context, "deleting_class_relation_from_global").await;
        let relation = &relations[0];

        let resp = delete_request(
            &context.pool,
            &context.admin_token,
            &relation_endpoint(relation.id),
        )
        .await;
        let _ = assert_response_status(resp, StatusCode::NO_CONTENT).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &relation_endpoint(relation.id),
        )
        .await;
        let _ = assert_response_status(resp, StatusCode::NOT_FOUND).await;

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_deleting_class_relation_from_class(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let (classes, relations) =
            create_classes_and_relations(&context, "deleting_class_relation_from_class").await;
        let relation = &relations[0];

        let endpoint = format!(
            "/api/v1/classes/{}/relations/{}",
            classes[0].id, relation.id
        );
        let resp = delete_request(&context.pool, &context.admin_token, &endpoint).await;
        let _ = assert_response_status(resp, StatusCode::NO_CONTENT).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &relation_endpoint(relation.id),
        )
        .await;
        let _ = assert_response_status(resp, StatusCode::NOT_FOUND).await;

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_deleting_class_relation_from_class_with_wrong_relation(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let (classes, relations) = create_classes_and_relations(
            &context,
            "deleting_class_relation_from_class_with_wrong_relation",
        )
        .await;
        let relation = &relations[1];

        let endpoint = format!(
            "/api/v1/classes/{}/relations/{}",
            classes[0].id, relation.id
        );
        let resp = delete_request(&context.pool, &context.admin_token, &endpoint).await;
        let _ = assert_response_status(resp, StatusCode::BAD_REQUEST).await;

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_creating_class_relation_from_class(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let classes = create_test_classes(&context, "creating_class_relation_from_class").await;

        let content = NewHubuumClassRelationFromClass {
            to_hubuum_class_id: classes[1].id,
        };

        let endpoint = format!("/api/v1/classes/{}/relations/", classes[0].id);
        let resp = post_request(&context.pool, &context.admin_token, &endpoint, &content).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let relation_response: HubuumClassRelation = test::read_body_json(resp).await;

        assert_eq!(relation_response.from_hubuum_class_id, classes[0].id);
        assert_eq!(relation_response.to_hubuum_class_id, classes[1].id);

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &relation_endpoint(relation_response.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let relation_response_from_global: HubuumClassRelation = test::read_body_json(resp).await;
        assert_eq!(relation_response, relation_response_from_global);

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_creating_class_relation_from_class_reverse_duplicate_conflicts(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let classes =
            create_test_classes(&context, "creating_class_relation_from_class_reverse").await;

        let forward_content = NewHubuumClassRelationFromClass {
            to_hubuum_class_id: classes[1].id,
        };
        let reverse_content = NewHubuumClassRelationFromClass {
            to_hubuum_class_id: classes[0].id,
        };

        let forward_endpoint = format!("/api/v1/classes/{}/relations/", classes[0].id);
        let reverse_endpoint = format!("/api/v1/classes/{}/relations/", classes[1].id);

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &forward_endpoint,
            &forward_content,
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let relation_response: HubuumClassRelation = test::read_body_json(resp).await;

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &reverse_endpoint,
            &reverse_content,
        )
        .await;
        let _ = assert_response_status(resp, StatusCode::CONFLICT).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &relation_endpoint(relation_response.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let relation_response_from_global: HubuumClassRelation = test::read_body_json(resp).await;
        assert_eq!(relation_response, relation_response_from_global);

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_get_class_relation_with_permissions(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let user = ensure_normal_user(&context.pool).await;
        let token = user.create_token(&context.pool).await.unwrap().get_token();
        let group = create_test_group(&context.pool).await;

        group.add_member(&context.pool, &user).await.unwrap();

        let (classes, relations) =
            create_classes_and_relations(&context, "get_class_relation_with_permissions").await;
        let namespace = NamespaceID(classes[0].namespace_id)
            .instance(&context.pool)
            .await
            .unwrap();

        let relation = &relations[0];

        // No permissions so far.
        let resp = get_request(&context.pool, &token, CLASS_RELATIONS_ENDPOINT).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;

        let relations_fetched_all: Vec<HubuumClassRelation> = test::read_body_json(resp).await;
        assert!(relations_fetched_all.is_empty());

        let resp = get_request(&context.pool, &token, &relation_endpoint(relation.id)).await;
        let _ = assert_response_status(resp, StatusCode::FORBIDDEN).await;

        // Grant permissions to the group (and indirectly to the user).
        namespace
            .grant_one(&context.pool, group.id, Permissions::ReadClassRelation)
            .await
            .unwrap();

        let resp = get_request(&context.pool, &token, CLASS_RELATIONS_ENDPOINT).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;

        let relations_fetched_all: Vec<HubuumClassRelation> = test::read_body_json(resp).await;
        assert_eq!(relations_fetched_all.len(), relations.len());
        assert_contains_all!(&relations, &relations_fetched_all);
        assert_contains_same_ids!(&relations, &relations_fetched_all);

        let resp = get_request(&context.pool, &token, &relation_endpoint(relation.id)).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;

        let relation_response: HubuumClassRelation = test::read_body_json(resp).await;
        assert_eq!(relation_response.id, relation.id);

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_admin_can_list_object_relations_without_direct_owner_group_membership(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let classes = create_hidden_classes(&context, "admin_lists_hidden_object_relations").await;
        let class_relation = create_relation(&context.pool, &classes[0], &classes[1]).await;

        let from_object = NewHubuumObject {
            hubuum_class_id: classes[0].id,
            namespace_id: classes[0].namespace_id,
            name: "hidden relation source".to_string(),
            description: "hidden relation source".to_string(),
            data: serde_json::json!({"role": "source"}),
        }
        .save(&context.pool)
        .await
        .unwrap();

        let to_object = NewHubuumObject {
            hubuum_class_id: classes[1].id,
            namespace_id: classes[1].namespace_id,
            name: "hidden relation target".to_string(),
            description: "hidden relation target".to_string(),
            data: serde_json::json!({"role": "target"}),
        }
        .save(&context.pool)
        .await
        .unwrap();

        let relation =
            create_object_relation(&context.pool, &from_object, &to_object, &class_relation).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{OBJECT_RELATIONS_ENDPOINT}?from_objects={}&to_objects={}",
                from_object.id, to_object.id
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let relations: Vec<HubuumObjectRelation> = test::read_body_json(resp).await;

        assert_eq!(relations.len(), 1);
        assert_eq!(relations[0].id, relation.id);

        cleanup(&classes).await;
    }

    // classidx of obj1, obj1_idx, obj2_idx, relation_idx, exists
    #[rstest]
    #[case::relation_12_rel_true(0, 0, 1, 0, true)]
    #[case::relation_12_rel_false_c1(1, 0, 1, 0, false)]
    #[case::relation_21_rel_true(1, 1, 0, 0, true)]
    #[case::relation_32_true(2, 2, 1, 1, true)]
    #[case::relation_15_true(0, 0, 4, 2, true)]
    #[case::relation_34_false(2, 2, 3, 0, false)]
    #[case::relation_45_false_r0(3, 3, 4, 0, false)]
    #[case::relation_45_false_r1(3, 3, 4, 1, false)]
    #[case::relation_45_false_r2(3, 3, 4, 2, false)]
    #[actix_web::test]
    async fn test_get_object_relation_param(
        #[case] class_index: usize,
        #[case] from_index: usize,
        #[case] to_index: usize,
        #[case] relation_index: usize,
        #[case] exists: bool,
        #[future(awt)] test_context: TestContext,
    ) {
        let unique =
            format!("get_object_relation_param_{from_index}_{to_index}_{relation_index}_{exists}");
        let context = test_context;
        let (classes, relations) = create_classes_and_relations(&context, &unique).await;
        let objects = create_objects_in_classes(&context.pool, &classes).await;

        // Create relations as in the original test
        let relation_12 =
            create_object_relation(&context.pool, &objects[0], &objects[1], &relations[0]).await;
        let relation_23 =
            create_object_relation(&context.pool, &objects[1], &objects[2], &relations[1]).await;
        let class_relation_15 = create_relation(&context.pool, &classes[0], &classes[4]).await;
        let relation_15 =
            create_object_relation(&context.pool, &objects[0], &objects[4], &class_relation_15)
                .await;

        let relations = vec![relation_12, relation_23, relation_15];

        let endpoint = format!(
            "/api/v1/classes/{}/{}/relations/{}/{}",
            classes[class_index].id,
            objects[from_index].id,
            objects[to_index].hubuum_class_id,
            objects[to_index].id
        );

        let resp = get_request(&context.pool, &context.admin_token, &endpoint).await;

        if exists {
            let resp = assert_response_status(resp, StatusCode::OK).await;
            let relation_response: HubuumObjectRelation = test::read_body_json(resp).await;

            assert_eq!(
                relation_response.id, relations[relation_index].id,
                "{endpoint}: Relation index: {relation_index} ({relation_response:?} in {relations:?})"
            );
            if from_index > to_index {
                assert_eq!(
                    relation_response.from_hubuum_object_id,
                    objects[to_index].id
                );
                assert_eq!(
                    relation_response.to_hubuum_object_id,
                    objects[from_index].id
                );
            } else {
                assert_eq!(
                    relation_response.from_hubuum_object_id,
                    objects[from_index].id
                );
                assert_eq!(relation_response.to_hubuum_object_id, objects[to_index].id);
            }
        } else if !(resp.status() == StatusCode::NOT_FOUND
            || resp.status() == StatusCode::BAD_REQUEST)
        {
            panic!(
                "Expected NOT_FOUND/BAD_REQUEST from {}, got {:?} ({:?})",
                endpoint,
                resp.status(),
                test::read_body(resp).await
            );
        }

        cleanup(&classes).await;
    }

    #[rstest]
    #[case::forward_order(false)]
    #[case::reverse_order(true)]
    #[actix_web::test]
    async fn test_delete_object_relation_from_class_endpoint(
        #[case] reverse_order: bool,
        #[future(awt)] test_context: TestContext,
    ) {
        let unique = format!("delete_object_relation_from_class_endpoint_{reverse_order}");
        let context = test_context;
        let (classes, relations) = create_classes_and_relations(&context, &unique).await;
        let objects = create_objects_in_classes(&context.pool, &classes).await;

        let object_relation =
            create_object_relation(&context.pool, &objects[0], &objects[1], &relations[0]).await;

        let (from_class, from_object, to_class, to_object) = if reverse_order {
            (&classes[1], &objects[1], &classes[0], &objects[0])
        } else {
            (&classes[0], &objects[0], &classes[1], &objects[1])
        };

        let endpoint = format!(
            "/api/v1/classes/{}/{}/relations/{}/{}",
            from_class.id, from_object.id, to_class.id, to_object.id
        );

        let resp = delete_request(&context.pool, &context.admin_token, &endpoint).await;
        let _ = assert_response_status(resp, StatusCode::NO_CONTENT).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{OBJECT_RELATIONS_ENDPOINT}/{}", object_relation.id),
        )
        .await;
        let _ = assert_response_status(resp, StatusCode::NOT_FOUND).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &relation_endpoint(relations[0].id),
        )
        .await;
        let _ = assert_response_status(resp, StatusCode::OK).await;

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_create_object_relation_from_class_endpoint_reverse_duplicate_conflicts(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let (classes, relations) =
            create_classes_and_relations(&context, "create_object_relation_reverse_duplicate")
                .await;
        let objects = create_objects_in_classes(&context.pool, &classes).await;

        let forward_endpoint = format!(
            "/api/v1/classes/{}/{}/relations/{}/{}",
            classes[0].id, objects[0].id, classes[1].id, objects[1].id
        );
        let reverse_endpoint = format!(
            "/api/v1/classes/{}/{}/relations/{}/{}",
            classes[1].id, objects[1].id, classes[0].id, objects[0].id
        );

        let resp = post_request(&context.pool, &context.admin_token, &forward_endpoint, &()).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let relation_response: HubuumObjectRelation = test::read_body_json(resp).await;

        assert_eq!(relation_response.class_relation_id, relations[0].id);

        let resp = post_request(&context.pool, &context.admin_token, &reverse_endpoint, &()).await;
        let _ = assert_response_status(resp, StatusCode::CONFLICT).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{OBJECT_RELATIONS_ENDPOINT}/{}", relation_response.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let relation_response_from_global: HubuumObjectRelation = test::read_body_json(resp).await;
        assert_eq!(relation_response, relation_response_from_global);

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_get_object_relations_sorted_and_limited(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let (classes, class_relations) =
            create_classes_and_relations(&context, "get_object_relations_sorted_and_limited").await;
        let objects = create_objects_in_classes(&context.pool, &classes).await;

        let rel_1 =
            create_object_relation(&context.pool, &objects[0], &objects[1], &class_relations[0])
                .await;
        let rel_2 =
            create_object_relation(&context.pool, &objects[1], &objects[2], &class_relations[1])
                .await;
        let rel_3 =
            create_object_relation(&context.pool, &objects[2], &objects[3], &class_relations[2])
                .await;
        let object_relations = [rel_1, rel_2, rel_3];

        let class_relation_ids = class_relations[0..3]
            .iter()
            .map(|relation| relation.id.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let sorted_url =
            format!("{OBJECT_RELATIONS_ENDPOINT}?class_relation={class_relation_ids}&sort=id.desc");
        let resp = get_request(&context.pool, &context.admin_token, &sorted_url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let sorted_relations: Vec<HubuumObjectRelation> = test::read_body_json(resp).await;
        assert_eq!(sorted_relations.len(), object_relations.len());
        assert_eq!(sorted_relations[0].id, object_relations[2].id);
        assert_eq!(sorted_relations[1].id, object_relations[1].id);
        assert_eq!(sorted_relations[2].id, object_relations[0].id);

        let limited_url = format!(
            "{OBJECT_RELATIONS_ENDPOINT}?class_relation={class_relation_ids}&sort=id&limit=2"
        );
        let resp = get_request(&context.pool, &context.admin_token, &limited_url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let limited_relations: Vec<HubuumObjectRelation> = test::read_body_json(resp).await;
        assert_eq!(limited_relations.len(), 2);
        assert_eq!(limited_relations[0].id, object_relations[0].id);
        assert_eq!(limited_relations[1].id, object_relations[1].id);

        cleanup(&classes).await;
    }

    // class_idx object_idx, expected_code, filter, expected_object_ids
    // TODO: Add tests against _classes / _namespaces / _object
    // Note that <int> in the filter will be replaced with the object id with that index.
    #[rstest]
    #[case::rel_0_0_empty(0, 0, StatusCode::OK, "", vec![1, 2, 4])]
    #[case::rel_0_0_from_name(0, 0, StatusCode::OK, "?from_name__contains=0", vec![1, 2, 4])]
    #[case::rel_0_0_to_name(0, 0, StatusCode::OK, "?to_name__endswith=api_class_2", vec![1])]
    #[case::rel_0_0_to_desc(
        0,
        0,
        StatusCode::OK,
        "?to_description__endswith=api_description_2",
        vec![1]
    )]
    #[case::rel_0_0_depth_eq(0, 0, StatusCode::OK, "?depth=1", vec![1, 4])]
    #[case::rel_0_0_depth_gt(0, 0, StatusCode::OK, "?depth__gt=1", vec![2])]
    #[case::rel_0_0_depth_lt(0, 0, StatusCode::OK, "?depth__lt=1", vec![])]
    #[case::rel_0_0_path_equals_0_1(0, 0, StatusCode::OK, "?path=<0>,<1>", vec![1])]
    #[case::rel_0_0_path_equals_0_2(0, 0, StatusCode::OK, "?path=<0>,<1>,<2>", vec![2])]
    #[case::rel_0_0_path_contains(0, 0, StatusCode::OK, "?path__contains=<1>", vec![1, 2])]
    #[case::rel_1_1_empty(1, 1, StatusCode::OK, "", vec![0, 4, 2])]
    #[case::rel_4_4_empty(4, 4, StatusCode::OK, "", vec![0, 1, 2])]
    #[case::rel_0_0_invalid_key(0, 0, StatusCode::BAD_REQUEST, "?nosuchkey=foo", vec![])]
    #[case::rel_0_0_invalid_op(0, 0, StatusCode::BAD_REQUEST, "?from_name__foo=bar", vec![])]
    #[case::rel_0_1_wrong_class(0, 1, StatusCode::NOT_FOUND, "", vec![])]
    #[actix_web::test]
    async fn test_filter_related_objects(
        #[case] class_index: usize,
        #[case] object_index: usize,
        #[case] status: StatusCode,
        #[case] filter: &str,
        #[case] expected_object_ids: Vec<usize>,
        #[future(awt)] test_context: TestContext,
    ) {
        use regex::Regex;

        let unique =
            format!("filter_related_objects_{class_index}_{object_index}_{status}_{filter}")
                .replace(&['=', '&', '?', ' ', '<', '>'][..], "_");
        let context = test_context;
        let (classes, relations) = create_classes_and_relations(&context, &unique).await;
        let objects = create_objects_in_classes(&context.pool, &classes).await;

        let _ =
            create_object_relation(&context.pool, &objects[0], &objects[1], &relations[0]).await;
        let _ =
            create_object_relation(&context.pool, &objects[1], &objects[2], &relations[1]).await;
        let class_relation_15 = create_relation(&context.pool, &classes[0], &classes[4]).await;
        let _ = create_object_relation(&context.pool, &objects[0], &objects[4], &class_relation_15)
            .await;

        // replace <int> in the filter with the object id with that index.
        let re = Regex::new(r"<(\d+)>").unwrap();
        let filter = re.replace_all(filter, |caps: &regex::Captures| {
            let index = caps[1].parse::<usize>().unwrap();
            objects[index].id.to_string()
        });

        let endpoint = format!(
            "{}{}",
            related_objects_endpoint(classes[class_index].id, objects[object_index].id),
            filter
        );

        let resp = get_request(&context.pool, &context.admin_token, &endpoint).await;
        let resp = assert_response_status(resp, status).await;

        if status == StatusCode::OK {
            let body = test::read_body(resp).await;
            let objects_fetched: Vec<HubuumObjectWithPath> = serde_json::from_slice(&body).unwrap();
            let expected_ids: Vec<i32> = expected_object_ids
                .iter()
                .map(|i| objects[*i].id)
                .collect::<Vec<_>>();
            let fetched_ids = objects_fetched.iter().map(|o| o.id).collect::<Vec<_>>();

            assert_eq!(
                fetched_ids,
                expected_ids,
                "{} -> Expected: {:?}, got: {:?}\nAll objects: {:?}",
                endpoint,
                expected_object_ids
                    .iter()
                    .map(|i| objects[*i].id)
                    .collect::<Vec<_>>(),
                objects_fetched.iter().map(|o| o.id).collect::<Vec<_>>(),
                objects
            );
        }

        cleanup(&classes).await;
    }

    // Covers docs/relationship_endpoints.md "Querying related objects" (`from_json_data` and `to_json_data`).
    #[rstest]
    #[case::docs_from_json_data_matches_ancestor(
        "?from_json_data__equals=role=source-root",
        vec![1, 2, 4]
    )]
    #[case::docs_from_json_data_does_not_match_descendant_fields(
        "?from_json_data__equals=hostname=api-01",
        vec![]
    )]
    #[case::docs_to_json_data_matches_descendants(
        "?to_json_data__equals=env=prod",
        vec![1, 2]
    )]
    #[case::docs_to_json_data_does_not_match_ancestor_fields(
        "?to_json_data__equals=role=source-root",
        vec![]
    )]
    #[actix_web::test]
    async fn docs_api_related_objects_filter_json_data_examples(
        #[case] filter: &str,
        #[case] expected_object_ids: Vec<usize>,
        #[future(awt)] test_context: TestContext,
    ) {
        let unique = format!("docs_related_objects_json_{}", filter)
            .replace(&['=', '&', '?', ' ', '<', '>'][..], "_");
        let context = test_context;
        let (classes, relations) = create_classes_and_relations(&context, &unique).await;
        let objects = create_objects_in_classes(&context.pool, &classes).await;

        let _ =
            create_object_relation(&context.pool, &objects[0], &objects[1], &relations[0]).await;
        let _ =
            create_object_relation(&context.pool, &objects[1], &objects[2], &relations[1]).await;
        let class_relation_15 = create_relation(&context.pool, &classes[0], &classes[4]).await;
        let _ = create_object_relation(&context.pool, &objects[0], &objects[4], &class_relation_15)
            .await;

        let endpoint = format!(
            "{}{}",
            related_objects_endpoint(classes[0].id, objects[0].id),
            filter
        );

        let resp = get_request(&context.pool, &context.admin_token, &endpoint).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let objects_fetched: Vec<HubuumObjectWithPath> = test::read_body_json(resp).await;

        let expected_ids = expected_object_ids
            .iter()
            .map(|i| objects[*i].id)
            .collect::<Vec<_>>();
        let fetched_ids = objects_fetched
            .iter()
            .map(|object| object.id)
            .collect::<Vec<_>>();

        assert_eq!(fetched_ids, expected_ids, "{endpoint}");

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_old_related_objects_route_removed(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let (classes, relations) =
            create_classes_and_relations(&context, "old_related_objects_route_removed").await;
        let objects = create_objects_in_classes(&context.pool, &classes).await;
        let _ =
            create_object_relation(&context.pool, &objects[0], &objects[1], &relations[0]).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "/api/v1/classes/{}/{}/relations",
                classes[0].id, objects[0].id
            ),
        )
        .await;
        let _ = assert_response_status(resp, StatusCode::NOT_FOUND).await;

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_get_related_object_relations(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let (classes, relations) =
            create_classes_and_relations(&context, "get_related_object_relations").await;
        let objects = create_objects_in_classes(&context.pool, &classes).await;

        let relation_12 =
            create_object_relation(&context.pool, &objects[0], &objects[1], &relations[0]).await;
        let relation_23 =
            create_object_relation(&context.pool, &objects[1], &objects[2], &relations[1]).await;
        let class_relation_15 = create_relation(&context.pool, &classes[0], &classes[4]).await;
        let relation_15 =
            create_object_relation(&context.pool, &objects[0], &objects[4], &class_relation_15)
                .await;

        let endpoint = related_relations_endpoint(classes[0].id, objects[0].id);
        let resp = get_request(&context.pool, &context.admin_token, &endpoint).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let fetched: Vec<HubuumObjectRelation> = test::read_body_json(resp).await;

        let fetched_ids = fetched
            .iter()
            .map(|relation| relation.id)
            .collect::<Vec<_>>();
        assert_eq!(fetched_ids, vec![relation_12.id, relation_15.id]);
        assert!(!fetched_ids.contains(&relation_23.id));

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_related_objects_ignore_self_class_defaults_true_and_can_be_disabled(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let classes = create_test_classes(&context, "related_objects_ignore_self_class").await;

        let machine_to_jack = create_relation(&context.pool, &classes[0], &classes[1]).await;
        let jack_to_room = create_relation(&context.pool, &classes[1], &classes[2]).await;

        let machine_a = NewHubuumObject {
            hubuum_class_id: classes[0].id,
            namespace_id: classes[0].namespace_id,
            name: "machine_a".to_string(),
            description: "machine_a".to_string(),
            data: serde_json::json!({"role": "machine_a"}),
        }
        .save(&context.pool)
        .await
        .unwrap();
        let jack = NewHubuumObject {
            hubuum_class_id: classes[1].id,
            namespace_id: classes[1].namespace_id,
            name: "jack".to_string(),
            description: "jack".to_string(),
            data: serde_json::json!({"role": "jack"}),
        }
        .save(&context.pool)
        .await
        .unwrap();
        let machine_b = NewHubuumObject {
            hubuum_class_id: classes[0].id,
            namespace_id: classes[0].namespace_id,
            name: "machine_b".to_string(),
            description: "machine_b".to_string(),
            data: serde_json::json!({"role": "machine_b"}),
        }
        .save(&context.pool)
        .await
        .unwrap();
        let room = NewHubuumObject {
            hubuum_class_id: classes[2].id,
            namespace_id: classes[2].namespace_id,
            name: "room".to_string(),
            description: "room".to_string(),
            data: serde_json::json!({"role": "room"}),
        }
        .save(&context.pool)
        .await
        .unwrap();

        let _ = create_object_relation(&context.pool, &machine_a, &jack, &machine_to_jack).await;
        let _ = create_object_relation(&context.pool, &machine_b, &jack, &machine_to_jack).await;
        let _ = create_object_relation(&context.pool, &jack, &room, &jack_to_room).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &related_objects_endpoint(classes[0].id, machine_a.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let objects: Vec<HubuumObjectWithPath> = test::read_body_json(resp).await;

        assert_eq!(
            objects.iter().map(|object| object.id).collect::<Vec<_>>(),
            vec![jack.id, room.id]
        );

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{}?ignore_self_class=false",
                related_objects_endpoint(classes[0].id, machine_a.id)
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let objects: Vec<HubuumObjectWithPath> = test::read_body_json(resp).await;

        assert_eq!(
            objects.iter().map(|object| object.id).collect::<Vec<_>>(),
            vec![jack.id, machine_b.id, room.id]
        );

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_related_objects_ignore_classes_filters_results_but_keeps_deeper_paths(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let classes = create_test_classes(&context, "related_objects_ignore_classes").await;

        let machine_to_jack = create_relation(&context.pool, &classes[0], &classes[1]).await;
        let jack_to_room = create_relation(&context.pool, &classes[1], &classes[2]).await;

        let machine = NewHubuumObject {
            hubuum_class_id: classes[0].id,
            namespace_id: classes[0].namespace_id,
            name: "machine".to_string(),
            description: "machine".to_string(),
            data: serde_json::json!({"role": "machine"}),
        }
        .save(&context.pool)
        .await
        .unwrap();
        let jack = NewHubuumObject {
            hubuum_class_id: classes[1].id,
            namespace_id: classes[1].namespace_id,
            name: "jack".to_string(),
            description: "jack".to_string(),
            data: serde_json::json!({"role": "jack"}),
        }
        .save(&context.pool)
        .await
        .unwrap();
        let room = NewHubuumObject {
            hubuum_class_id: classes[2].id,
            namespace_id: classes[2].namespace_id,
            name: "room".to_string(),
            description: "room".to_string(),
            data: serde_json::json!({"role": "room"}),
        }
        .save(&context.pool)
        .await
        .unwrap();

        let _ = create_object_relation(&context.pool, &machine, &jack, &machine_to_jack).await;
        let _ = create_object_relation(&context.pool, &jack, &room, &jack_to_room).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{}?ignore_classes={}&ignore_self_class=false",
                related_objects_endpoint(classes[0].id, machine.id),
                classes[1].id
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let objects: Vec<HubuumObjectWithPath> = test::read_body_json(resp).await;

        assert_eq!(
            objects.iter().map(|object| object.id).collect::<Vec<_>>(),
            vec![room.id]
        );

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_related_objects_ignore_self_class_rejects_invalid_boolean(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let (classes, _) =
            create_classes_and_relations(&context, "related_objects_invalid_ignore_self_class")
                .await;
        let objects = create_objects_in_classes(&context.pool, &classes).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{}?ignore_self_class=maybe",
                related_objects_endpoint(classes[0].id, objects[0].id)
            ),
        )
        .await;
        let _ = assert_response_status(resp, StatusCode::BAD_REQUEST).await;

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_related_objects_ignore_self_class_rejects_duplicate_values(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let (classes, _) =
            create_classes_and_relations(&context, "related_objects_duplicate_ignore_self_class")
                .await;
        let objects = create_objects_in_classes(&context.pool, &classes).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{}?ignore_self_class=true&ignore_self_class=false",
                related_objects_endpoint(classes[0].id, objects[0].id)
            ),
        )
        .await;
        let _ = assert_response_status(resp, StatusCode::BAD_REQUEST).await;

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_related_object_graph(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let (classes, relations) =
            create_classes_and_relations(&context, "related_object_graph").await;
        let objects = create_objects_in_classes(&context.pool, &classes).await;

        let relation_12 =
            create_object_relation(&context.pool, &objects[0], &objects[1], &relations[0]).await;
        let relation_23 =
            create_object_relation(&context.pool, &objects[1], &objects[2], &relations[1]).await;
        let class_relation_15 = create_relation(&context.pool, &classes[0], &classes[4]).await;
        let relation_15 =
            create_object_relation(&context.pool, &objects[0], &objects[4], &class_relation_15)
                .await;

        let endpoint = format!(
            "{}?depth__lte=1",
            related_graph_endpoint(classes[0].id, objects[0].id)
        );
        let resp = get_request(&context.pool, &context.admin_token, &endpoint).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let graph: RelatedObjectGraph = test::read_body_json(resp).await;

        let object_ids = graph
            .objects
            .iter()
            .map(|object| object.id)
            .collect::<Vec<_>>();
        let relation_ids = graph
            .relations
            .iter()
            .map(|relation| relation.id)
            .collect::<Vec<_>>();

        assert_eq!(
            object_ids,
            vec![objects[0].id, objects[1].id, objects[4].id]
        );
        assert_eq!(graph.objects[0].path, vec![objects[0].id]);
        assert_eq!(relation_ids, vec![relation_12.id, relation_15.id]);
        assert!(!relation_ids.contains(&relation_23.id));

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_related_endpoints_require_relation_permission_for_neighbors(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let group = create_test_group(&context.pool).await;
        group
            .add_member(&context.pool, &context.normal_user)
            .await
            .unwrap();

        let (classes, relations) = create_classes_and_relations(
            &context,
            "related_endpoints_require_relation_permission_for_neighbors",
        )
        .await;
        let objects = create_objects_in_classes(&context.pool, &classes).await;

        let _ =
            create_object_relation(&context.pool, &objects[0], &objects[1], &relations[0]).await;
        let class_relation_15 = create_relation(&context.pool, &classes[0], &classes[4]).await;
        let _ = create_object_relation(&context.pool, &objects[0], &objects[4], &class_relation_15)
            .await;

        let namespace = NamespaceID(classes[0].namespace_id)
            .instance(&context.pool)
            .await
            .unwrap();
        namespace
            .grant_one(&context.pool, group.id, Permissions::ReadObject)
            .await
            .unwrap();

        let related_objects_resp = get_request(
            &context.pool,
            &context.normal_token,
            &related_objects_endpoint(classes[0].id, objects[0].id),
        )
        .await;
        let related_objects_resp =
            assert_response_status(related_objects_resp, StatusCode::OK).await;
        let related_objects: Vec<HubuumObjectWithPath> =
            test::read_body_json(related_objects_resp).await;
        assert!(related_objects.is_empty());

        let related_relations_resp = get_request(
            &context.pool,
            &context.normal_token,
            &related_relations_endpoint(classes[0].id, objects[0].id),
        )
        .await;
        let related_relations_resp =
            assert_response_status(related_relations_resp, StatusCode::OK).await;
        let related_relations: Vec<HubuumObjectRelation> =
            test::read_body_json(related_relations_resp).await;
        assert!(related_relations.is_empty());

        let graph_resp = get_request(
            &context.pool,
            &context.normal_token,
            &related_graph_endpoint(classes[0].id, objects[0].id),
        )
        .await;
        let graph_resp = assert_response_status(graph_resp, StatusCode::OK).await;
        let graph: RelatedObjectGraph = test::read_body_json(graph_resp).await;

        assert_eq!(graph.objects.len(), 1);
        assert_eq!(graph.objects[0].id, objects[0].id);
        assert_eq!(graph.objects[0].path, vec![objects[0].id]);
        assert!(graph.relations.is_empty());

        group.delete(&context.pool).await.unwrap();
        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_related_endpoints_forbid_hidden_root_object(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let (classes, relations) =
            create_classes_and_relations(&context, "related_endpoints_forbid_hidden_root").await;
        let objects = create_objects_in_classes(&context.pool, &classes).await;

        let _ =
            create_object_relation(&context.pool, &objects[0], &objects[1], &relations[0]).await;

        for endpoint in [
            related_objects_endpoint(classes[0].id, objects[0].id),
            related_relations_endpoint(classes[0].id, objects[0].id),
            related_graph_endpoint(classes[0].id, objects[0].id),
        ] {
            let resp = get_request(&context.pool, &context.normal_token, &endpoint).await;
            let _ = assert_response_status(resp, StatusCode::FORBIDDEN).await;
        }

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_related_endpoints_hide_cross_namespace_relations_without_permission(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let group = create_test_group(&context.pool).await;
        group
            .add_member(&context.pool, &context.normal_user)
            .await
            .unwrap();

        let visible_classes =
            create_test_classes(&context, "related_endpoints_cross_namespace_visible").await;
        let hidden_classes =
            create_hidden_classes(&context, "related_endpoints_cross_namespace").await;

        let visible_relation =
            create_relation(&context.pool, &visible_classes[0], &visible_classes[1]).await;
        let cross_namespace_relation =
            create_relation(&context.pool, &visible_classes[0], &hidden_classes[0]).await;

        let visible_objects = create_objects_in_classes(&context.pool, &visible_classes[..2]).await;
        let hidden_objects = create_objects_in_classes(&context.pool, &hidden_classes[..1]).await;

        let visible_object_relation = create_object_relation(
            &context.pool,
            &visible_objects[0],
            &visible_objects[1],
            &visible_relation,
        )
        .await;
        let hidden_object_relation = create_object_relation(
            &context.pool,
            &visible_objects[0],
            &hidden_objects[0],
            &cross_namespace_relation,
        )
        .await;

        let visible_namespace = NamespaceID(visible_classes[0].namespace_id)
            .instance(&context.pool)
            .await
            .unwrap();
        visible_namespace
            .grant_one(&context.pool, group.id, Permissions::ReadObject)
            .await
            .unwrap();
        visible_namespace
            .grant_one(&context.pool, group.id, Permissions::ReadObjectRelation)
            .await
            .unwrap();

        let related_objects_resp = get_request(
            &context.pool,
            &context.normal_token,
            &related_objects_endpoint(visible_classes[0].id, visible_objects[0].id),
        )
        .await;
        let related_objects_resp =
            assert_response_status(related_objects_resp, StatusCode::OK).await;
        let related_objects: Vec<HubuumObjectWithPath> =
            test::read_body_json(related_objects_resp).await;
        assert_eq!(
            related_objects
                .iter()
                .map(|object| object.id)
                .collect::<Vec<_>>(),
            vec![visible_objects[1].id]
        );

        let related_relations_resp = get_request(
            &context.pool,
            &context.normal_token,
            &related_relations_endpoint(visible_classes[0].id, visible_objects[0].id),
        )
        .await;
        let related_relations_resp =
            assert_response_status(related_relations_resp, StatusCode::OK).await;
        let related_relations: Vec<HubuumObjectRelation> =
            test::read_body_json(related_relations_resp).await;
        assert_eq!(
            related_relations
                .iter()
                .map(|relation| relation.id)
                .collect::<Vec<_>>(),
            vec![visible_object_relation.id]
        );
        assert!(
            !related_relations
                .iter()
                .any(|relation| relation.id == hidden_object_relation.id)
        );

        let graph_resp = get_request(
            &context.pool,
            &context.normal_token,
            &related_graph_endpoint(visible_classes[0].id, visible_objects[0].id),
        )
        .await;
        let graph_resp = assert_response_status(graph_resp, StatusCode::OK).await;
        let graph: RelatedObjectGraph = test::read_body_json(graph_resp).await;

        assert_eq!(
            graph
                .objects
                .iter()
                .map(|object| object.id)
                .collect::<Vec<_>>(),
            vec![visible_objects[0].id, visible_objects[1].id]
        );
        assert_eq!(
            graph
                .relations
                .iter()
                .map(|relation| relation.id)
                .collect::<Vec<_>>(),
            vec![visible_object_relation.id]
        );
        assert!(
            !graph
                .relations
                .iter()
                .any(|relation| relation.id == hidden_object_relation.id)
        );

        group.delete(&context.pool).await.unwrap();
        cleanup(&visible_classes).await;
        cleanup(&hidden_classes).await;
    }
}
