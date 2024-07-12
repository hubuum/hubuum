#[cfg(test)]
mod tests {
    use crate::models::{HubuumClass, HubuumClassRelation, NewHubuumClassRelation};
    use crate::traits::CanSave;
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
        let classes = create_test_classes("get_class_relations").await;

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
        assert_eq!(relations.len(), 1);
        assert_eq!(relations[0].id, relation.id);

        if from_class.id > to_class.id {
            assert_eq!(relations[0].from_hubuum_class_id, to_class.id);
            assert_eq!(relations[0].to_hubuum_class_id, from_class.id);
        } else {
            assert_eq!(relations[0].from_hubuum_class_id, from_class.id);
            assert_eq!(relations[0].to_hubuum_class_id, to_class.id);
        }

        cleanup(&classes).await;
    }
}
