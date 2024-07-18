#[cfg(test)]
mod tests {
    use crate::models::{
        HubuumClass, HubuumClassRelation, NamespaceID, NewHubuumClassRelation, Permissions,
    };
    use crate::traits::{CanSave, PermissionController, SelfAccessors};
    use crate::{assert_contains_all, assert_contains_same_ids};
    use actix_web::{http::StatusCode, test};

    use crate::tests::api_operations::get_request;
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
