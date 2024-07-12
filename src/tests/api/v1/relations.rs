#[cfg(test)]
mod tests {
    use crate::models::{HubuumClass, HubuumClassRelation, NewHubuumClassRelation};
    use crate::traits::CanSave;
    use crate::{assert_contains_all, assert_contains_same_ids};
    use actix_web::{http::StatusCode, test};

    use crate::tests::api_operations::{delete_request, get_request, patch_request, post_request};
    use crate::tests::asserts::assert_response_status;
    use crate::tests::{create_namespace, setup_pool_and_tokens};
    // use crate::{assert_contains_all, assert_contains_same_ids};

    use crate::tests::api::v1::classes::tests::{cleanup, create_test_classes};

    const RELATIONS_ENDPOINT: &str = "/api/v1/relations";

    fn relation_endpoint(relation_id: i32) -> String {
        format!("{}/{}", RELATIONS_ENDPOINT, relation_id)
    }

    #[actix_web::test]
    async fn test_get_class_relations_list() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let classes = create_test_classes("get_class_relations_list").await;

        let from_class = &classes[0];
        let to_class = &classes[5];
        let relation = NewHubuumClassRelation {
            from_hubuum_class_id: from_class.id,
            to_hubuum_class_id: to_class.id,
        };
        let relation = relation.save(&pool).await.unwrap();

        let resp = get_request(&pool, &admin_token, RELATIONS_ENDPOINT).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;

        let relations: Vec<HubuumClassRelation> = test::read_body_json(resp).await;

        cleanup(&classes).await;
    }

    #[actix_web::test]
    async fn test_get_class_relation() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let classes = create_test_classes("get_class_relation").await;

        let from_class = &classes[0];
        let to_class = &classes[5];
        let relation = NewHubuumClassRelation {
            from_hubuum_class_id: from_class.id,
            to_hubuum_class_id: to_class.id,
        };

        let relation = relation.save(&pool).await.unwrap();

        let resp = get_request(&pool, &admin_token, &relation_endpoint(relation.id)).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;

        let relation_response: HubuumClassRelation = test::read_body_json(resp).await;
        assert_eq!(relation_response.id, relation.id);

        cleanup(&classes).await;
    }
}
