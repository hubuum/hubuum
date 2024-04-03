#[cfg(test)]
mod tests {
    use crate::models::{HubuumObject, NewHubuumObject, UpdateHubuumObject};
    use crate::traits::CanSave;
    use actix_web::{http::StatusCode, test};

    use crate::tests::api_operations::{delete_request, get_request, patch_request, post_request};
    use crate::tests::asserts::assert_response_status;
    use crate::tests::{create_namespace, setup_pool_and_tokens};
    use crate::{assert_contains_all, assert_contains_same_ids};

    use crate::tests::api::v1::classes::tests::{cleanup, create_test_classes};

    const OBJECT_ENDPOINT: &str = "/api/v1/classes";

    fn object_in_class_endpoint(class_id: i32, object_id: i32) -> String {
        format!("{}/{}/{}", OBJECT_ENDPOINT, class_id, object_id)
    }

    #[actix_rt::test]
    async fn get_patch_and_delete_objects_in_class() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;

        let namespace = create_namespace(&pool, "get_objects_in_class")
            .await
            .unwrap();
        let classes = create_test_classes("get_objects_in_class").await;

        let class = &classes[0];

        let object = NewHubuumObject {
            namespace_id: namespace.id,
            hubuum_class_id: classes[0].id,
            data: serde_json::json!({"test": "data"}),
            name: "test object".to_string(),
            description: "test object description".to_string(),
        };

        let object = object.save(&pool).await.unwrap();

        let resp = get_request(
            &pool,
            &admin_token,
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
            &pool,
            &admin_token,
            &object_in_class_endpoint(class.id, object.id),
            updated_object,
        )
        .await;

        let resp = assert_response_status(resp, StatusCode::OK).await;
        let updated_object_from_req: HubuumObject = test::read_body_json(resp).await;
        assert_eq!(updated_object_from_req.name, "updated object");

        let resp = get_request(
            &pool,
            &admin_token,
            &object_in_class_endpoint(class.id, object.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let updated_object_from_api: HubuumObject = test::read_body_json(resp).await;

        assert_eq!(updated_object_from_api, updated_object_from_req);

        let resp = delete_request(
            &pool,
            &admin_token,
            &object_in_class_endpoint(class.id, object.id),
        )
        .await;

        assert_response_status(resp, StatusCode::NO_CONTENT).await;

        let resp = get_request(
            &pool,
            &admin_token,
            &object_in_class_endpoint(class.id, object.id),
        )
        .await;
        assert_response_status(resp, StatusCode::NOT_FOUND).await;

        cleanup(&classes).await;
    }

    #[actix_rt::test]
    async fn create_object_in_class() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;

        let namespace = create_namespace(&pool, "create_object_in_class")
            .await
            .unwrap();
        let classes = create_test_classes("create_object_in_class").await;

        let class = &classes[0];

        let object = NewHubuumObject {
            namespace_id: namespace.id,
            hubuum_class_id: classes[0].id,
            data: serde_json::json!({"test": "data"}),
            name: "test create object".to_string(),
            description: "test create object description".to_string(),
        };

        let resp = post_request(
            &pool,
            &admin_token,
            &format!("{}/{}/", OBJECT_ENDPOINT, class.id),
            &object,
        )
        .await;

        let resp = assert_response_status(resp, StatusCode::CREATED).await;
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
