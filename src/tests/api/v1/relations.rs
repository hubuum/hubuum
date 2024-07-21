#[cfg(test)]
mod tests {
    use crate::models::{
        HubuumClass, HubuumClassRelation, HubuumClassRelationTransitive, NamespaceID,
        NewHubuumClassRelation, NewHubuumClassRelationFromClass, Permissions,
    };
    use crate::traits::{CanSave, PermissionController, SelfAccessors};
    use crate::{assert_contains_all, assert_contains_same_ids};
    use actix_web::{http::StatusCode, test};

    use crate::tests::api_operations::{delete_request, get_request, post_request};
    use crate::tests::asserts::assert_response_status;
    use crate::tests::{create_test_group, ensure_normal_user, setup_pool_and_tokens};
    // use crate::{assert_contains_all, assert_contains_same_ids};

    use crate::tests::api::v1::classes::tests::{cleanup, create_test_classes};

    const RELATIONS_ENDPOINT: &str = "/api/v1/relations";

    fn relation_endpoint(relation_id: i32) -> String {
        format!("{}/{}", RELATIONS_ENDPOINT, relation_id)
    }

    async fn create_relation(
        pool: &crate::db::DbPool,
        from_class: &HubuumClass,
        to_class: &HubuumClass,
    ) -> HubuumClassRelation {
        let relation = NewHubuumClassRelation {
            from_hubuum_class_id: from_class.id,
            to_hubuum_class_id: to_class.id,
        };

        relation.save(pool).await.unwrap()
    }

    async fn create_classes_and_relations(
        pool: &crate::db::DbPool,
        prefix: &str,
    ) -> (Vec<HubuumClass>, Vec<HubuumClassRelation>) {
        let classes = create_test_classes(prefix).await;

        let relations = vec![
            create_relation(pool, &classes[0], &classes[1]).await,
            create_relation(pool, &classes[1], &classes[2]).await,
            create_relation(pool, &classes[2], &classes[3]).await,
            create_relation(pool, &classes[3], &classes[4]).await,
            create_relation(pool, &classes[4], &classes[5]).await,
        ];

        (classes, relations)
    }

    #[actix_web::test]
    async fn test_get_class_relations_list() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let (classes, relations) =
            create_classes_and_relations(&pool, "get_class_relations_list").await;

        let resp = get_request(&pool, &admin_token, RELATIONS_ENDPOINT).await;
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

    #[actix_web::test]
    async fn test_get_class_relation_list_via_class() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let (classes, _) =
            create_classes_and_relations(&pool, "get_class_relation_list_via_class").await;

        let class = &classes[0];

        // Check direct relations.
        let endpoint = format!("/api/v1/classes/{}/relations/", class.id);
        let resp = get_request(&pool, &admin_token, &endpoint).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let relations_fetched: Vec<HubuumClassRelation> = test::read_body_json(resp).await;

        assert_eq!(relations_fetched.len(), 1);
        assert_eq!(relations_fetched[0].from_hubuum_class_id, class.id);
        assert_eq!(relations_fetched[0].to_hubuum_class_id, classes[1].id);

        // Check transitive results.
        // We should have links from 1->2, 2->3, 3->4, 4->5, 5->6.
        // So for the first class, we should have 5 relations.
        let endpoint = format!("/api/v1/classes/{}/relations/transitive/", class.id);

        let resp = get_request(&pool, &admin_token, &endpoint).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let relations_fetched: Vec<HubuumClassRelationTransitive> =
            test::read_body_json(resp).await;

        assert_eq!(relations_fetched.len(), 5);
        for (i, relation) in relations_fetched.iter().enumerate() {
            assert_eq!(relation.ancestor_class_id, classes[0].id);
            assert_eq!(relation.descendant_class_id, classes[i + 1].id);
            assert_eq!(relation.depth, i as i32 + 1);
            assert_eq!(relation.path.len(), i as usize + 2);
            // The path should contain the ancestor and descendant classes, so all the classes up to
            // the current one.
            let expected_path = classes.iter().take(i + 2).map(|c| c.id).collect::<Vec<_>>();
            assert_eq!(relation.path.len(), expected_path.len());
            for i in 0..expected_path.len() {
                assert_eq!(relation.path[i], Some(expected_path[i]));
            }
        }

        cleanup(&classes).await;
    }

    #[actix_web::test]
    async fn test_get_class_relation() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let (classes, relations) = create_classes_and_relations(&pool, "get_class_relation").await;
        let relation = &relations[0];

        let resp = get_request(&pool, &admin_token, &relation_endpoint(relation.id)).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;

        let relation_response: HubuumClassRelation = test::read_body_json(resp).await;
        assert_eq!(relation_response.id, relation.id);

        cleanup(&classes).await;
    }

    #[actix_web::test]
    async fn test_deleting_class_relation_from_global() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let (classes, relations) =
            create_classes_and_relations(&pool, "deleting_class_relation_from_global").await;
        let relation = &relations[0];

        let resp = delete_request(&pool, &admin_token, &relation_endpoint(relation.id)).await;
        let _ = assert_response_status(resp, StatusCode::NO_CONTENT).await;

        let resp = get_request(&pool, &admin_token, &relation_endpoint(relation.id)).await;
        let _ = assert_response_status(resp, StatusCode::NOT_FOUND).await;

        cleanup(&classes).await;
    }

    #[actix_web::test]
    async fn test_deleting_class_relation_from_class() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let (classes, relations) =
            create_classes_and_relations(&pool, "deleting_class_relation_from_class").await;
        let relation = &relations[0];

        let endpoint = format!(
            "/api/v1/classes/{}/relations/{}",
            classes[0].id, relation.id
        );
        let resp = delete_request(&pool, &admin_token, &endpoint).await;
        let _ = assert_response_status(resp, StatusCode::NO_CONTENT).await;

        let resp = get_request(&pool, &admin_token, &relation_endpoint(relation.id)).await;
        let _ = assert_response_status(resp, StatusCode::NOT_FOUND).await;

        cleanup(&classes).await;
    }

    #[actix_web::test]
    async fn test_deleting_class_relation_from_class_with_wrong_relation() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let (classes, relations) = create_classes_and_relations(
            &pool,
            "deleting_class_relation_from_class_with_wrong_relation",
        )
        .await;
        let relation = &relations[1];

        let endpoint = format!(
            "/api/v1/classes/{}/relations/{}",
            classes[0].id, relation.id
        );
        let resp = delete_request(&pool, &admin_token, &endpoint).await;
        let _ = assert_response_status(resp, StatusCode::BAD_REQUEST).await;

        cleanup(&classes).await;
    }

    #[actix_web::test]
    async fn test_creating_class_relation_from_class() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let classes = create_test_classes("creating_class_relation_from_class").await;

        let content = NewHubuumClassRelationFromClass {
            to_hubuum_class_id: classes[1].id,
        };

        let endpoint = format!("/api/v1/classes/{}/relations/", classes[0].id);
        let resp = post_request(&pool, &admin_token, &endpoint, &content).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let relation_response: HubuumClassRelation = test::read_body_json(resp).await;

        assert_eq!(relation_response.from_hubuum_class_id, classes[0].id);
        assert_eq!(relation_response.to_hubuum_class_id, classes[1].id);

        let resp = get_request(
            &pool,
            &admin_token,
            &relation_endpoint(relation_response.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let relation_response_from_global: HubuumClassRelation = test::read_body_json(resp).await;
        assert_eq!(relation_response, relation_response_from_global);

        cleanup(&classes).await;
    }

    #[actix_web::test]
    async fn test_get_class_relation_with_permissions() {
        let (pool, _, _) = setup_pool_and_tokens().await;
        let user = ensure_normal_user(&pool).await;
        let token = user.create_token(&pool).await.unwrap().get_token();
        let group = create_test_group(&pool).await;

        group.add_member(&pool, &user).await.unwrap();

        let (classes, relations) =
            create_classes_and_relations(&pool, "get_class_relation_with_permissions").await;
        let namespace = NamespaceID(classes[0].namespace_id)
            .instance(&pool)
            .await
            .unwrap();

        let relation = &relations[0];

        // No permissions so far.
        let resp = get_request(&pool, &token, RELATIONS_ENDPOINT).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;

        let relations_fetched_all: Vec<HubuumClassRelation> = test::read_body_json(resp).await;
        assert!(relations_fetched_all.is_empty());

        let resp = get_request(&pool, &token, &relation_endpoint(relation.id)).await;
        let _ = assert_response_status(resp, StatusCode::FORBIDDEN).await;

        // Grant permissions to the group (and indirectly to the user).
        namespace
            .grant_one(&pool, group.id, Permissions::ReadClassRelation)
            .await
            .unwrap();

        let resp = get_request(&pool, &token, RELATIONS_ENDPOINT).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;

        let relations_fetched_all: Vec<HubuumClassRelation> = test::read_body_json(resp).await;
        assert_eq!(relations_fetched_all.len(), relations.len());
        assert_contains_all!(&relations, &relations_fetched_all);
        assert_contains_same_ids!(&relations, &relations_fetched_all);

        let resp = get_request(&pool, &token, &relation_endpoint(relation.id)).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;

        let relation_response: HubuumClassRelation = test::read_body_json(resp).await;
        assert_eq!(relation_response.id, relation.id);

        cleanup(&classes).await;
    }
}
