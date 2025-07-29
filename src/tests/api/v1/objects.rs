#[cfg(test)]
mod tests {
    use yare::parameterized;

    use crate::models::{HubuumObject, NewHubuumClass, NewHubuumObject, UpdateHubuumObject};
    use crate::traits::{CanDelete, CanSave};
    use actix_web::{http::StatusCode, test};

    use crate::tests::api_operations::{delete_request, get_request, patch_request, post_request};
    use crate::tests::asserts::assert_response_status;
    use crate::tests::constants::{get_schema, SchemaType};
    use crate::tests::{create_namespace, setup_pool_and_tokens};
    // use crate::{assert_contains_all, assert_contains_same_ids};

    use crate::tests::api::v1::classes::tests::{cleanup, create_test_classes};

    const OBJECT_ENDPOINT: &str = "/api/v1/classes";

    fn object_in_class_endpoint(class_id: i32, object_id: i32) -> String {
        format!("{OBJECT_ENDPOINT}/{class_id}/{object_id}")
    }

    fn objects_in_class_endpoint(class_id: i32) -> String {
        format!("{OBJECT_ENDPOINT}/{class_id}/")
    }

    async fn create_test_objects(prefix: &str, count: usize) -> Vec<HubuumObject> {
        let (pool, _, _) = setup_pool_and_tokens().await;

        let namespace = create_namespace(&pool, prefix).await.unwrap();
        let class = NewHubuumClass {
            namespace_id: namespace.id,
            name: format!("test class {prefix}"),
            description: "Test class description".to_string(),
            json_schema: None,
            validate_schema: None,
        }
        .save(&pool)
        .await
        .unwrap();

        let mut objects = Vec::new();

        for i in 0..count {
            let object = NewHubuumObject {
                namespace_id: namespace.id,
                hubuum_class_id: class.id,
                data: serde_json::json!({"test": format!("data_{i}")}),
                name: format!("{prefix} test object {i}"),
                description: format!("{prefix} test object description {i}"),
            };
            objects.push(object.save(&pool).await.unwrap());
        }
        objects
    }

    #[actix_rt::test]
    async fn get_patch_and_delete_objects_in_class() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;

        let namespace = create_namespace(&pool, "get_patch_and_delete_objects_in_class")
            .await
            .unwrap();
        let classes = create_test_classes("get_patch_and_delete_objects_in_class").await;

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

    // This will create objects with the same name but potentially in differnet classes.
    // This is to test that the name is unique within the class.
    // [class_idx1, class_idx2] [expected_status1, expected_status2]
    #[parameterized(
        class_0_0_conflict = {[0, 0], [StatusCode::CREATED, StatusCode::CONFLICT]},
        class_0_1_ok = {[0, 1], [StatusCode::CREATED, StatusCode::CREATED]},
        class_0_2_ok = {[0, 2], [StatusCode::CREATED, StatusCode::CREATED]},
        class_1_1_conflict = {[1, 1], [StatusCode::CREATED, StatusCode::CONFLICT]},
        class_2_2_conflict = {[2, 2], [StatusCode::CREATED, StatusCode::CONFLICT]},

    )]
    #[test_macro(actix_web::test)]
    async fn create_object_in_class(class_ids: [i32; 2], expected_statuses: [StatusCode; 2]) {
        let literal = format!(
            "create_object_in_class_{}",
            class_ids
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<String>>()
                .join("_")
        );

        let (pool, admin_token, _) = setup_pool_and_tokens().await;

        let namespace = create_namespace(&pool, &literal).await.unwrap();
        let classes = create_test_classes(&literal).await;

        for (class_id, expected_status) in class_ids.iter().zip(expected_statuses.iter()) {
            let class = &classes[*class_id as usize];

            let object = NewHubuumObject {
                namespace_id: namespace.id,
                hubuum_class_id: class.id,
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

            let resp = assert_response_status(resp, *expected_status).await;

            if expected_status == &StatusCode::CREATED {
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
        namespace.delete(&pool).await.unwrap();
    }

    #[actix_rt::test]
    async fn get_objects_in_class() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;

        let namespace = create_namespace(&pool, "get_objects_in_class")
            .await
            .unwrap();
        let classes = create_test_classes("get_objects_in_class").await;

        let class = &classes[0];

        let mut objects = vec![];

        for i in 0..5 {
            let object = NewHubuumObject {
                namespace_id: namespace.id,
                hubuum_class_id: classes[0].id,
                data: serde_json::json!({"test": format!("data_{i}")}),
                name: format!("test get objects {i}"),
                description: format!("test object description {i}"),
            };
            objects.push(object.save(&pool).await.unwrap());
        }

        let resp = get_request(&pool, &admin_token, &objects_in_class_endpoint(class.id)).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let objects_from_api: Vec<HubuumObject> = test::read_body_json(resp).await;

        assert_eq!(objects_from_api.len(), objects.len());
    }

    #[parameterized(
        ok_40_74 = { r#"{"latitude": 40.7128, "longitude": -74.0060}"#, true },
        failed_91_74 = { r#"{"latitude": 91, "longitude": 200}"#, false },
        failed_neg91_74 = { r#"{"latitude": -91, "longitude": 200}"#, false },
        failed_40_181 = { r#"{"latitude": 40.7128, "longitude": 181}"#, false },
        failed_40_neg181 = { r#"{"latitude": 40.7128, "longitude": -181}"#, false },
        failed_100_200 = { r#"{"latitude": 100, "longitude": 200}"#, false },
        failed_lat_missing = { r#"{"longitude": 0}"#, false },
        failed_long_missing = { r#"{"latitude": 0}"#, false },
        ok_extra_fields = { r#"{"latitude": 40.7128, "longitude": -74.0060, "extra_field": "value"}"#, true },
    )]
    #[test_macro(actix_web::test)]
    async fn create_objects_in_class_failing_validation(json_data: &str, expected: bool) {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;

        let unique_name = format!("{json_data}_create_objects_in_class_failing_validation");

        let namespace = create_namespace(&pool, &unique_name).await.unwrap();

        let schema = get_schema(SchemaType::Geo);
        let class = NewHubuumClass {
            name: unique_name.clone(),
            namespace_id: namespace.id,
            description: "Test class".to_string(),
            json_schema: Some(schema.clone()),
            validate_schema: Some(true),
        }
        .save(&pool)
        .await
        .unwrap();

        let object = NewHubuumObject {
            name: unique_name.clone(),
            namespace_id: namespace.id,
            hubuum_class_id: class.id,
            data: serde_json::from_str(json_data).unwrap(),
            description: "Test object".to_string(),
        };

        let resp = post_request(
            &pool,
            &admin_token,
            &format!("{}/{}/", OBJECT_ENDPOINT, class.id),
            &object,
        )
        .await;

        let resp = assert_response_status(
            resp,
            if expected {
                StatusCode::CREATED
            } else {
                StatusCode::NOT_ACCEPTABLE
            },
        )
        .await;

        if expected {
            let object_from_api: HubuumObject = test::read_body_json(resp).await;
            assert_eq!(object_from_api.name, object.name);
            assert_eq!(object_from_api.description, object.description);
            assert_eq!(object_from_api.data, object.data);
            assert_eq!(object_from_api.namespace_id, object.namespace_id);
            assert_eq!(object_from_api.hubuum_class_id, object.hubuum_class_id);
            object_from_api.delete(&pool).await.unwrap();
        } else {
            let error_message: serde_json::Value = test::read_body_json(resp).await;
            let error_text = error_message["error"].as_str().unwrap().to_lowercase();
            assert!(
                error_text.contains("validation error"),
                "Expected 'validation error', got: {error_text}"
            );
        }
    }

    #[parameterized(
        unsorted = { "", &[0, 1, 2] },
        sorted_id_default = { "id", &[0, 1, 2] },
        sorted_id_explicit_asc = { "id.asc", &[0, 1, 2] },
        sorted_id_descending = { "id.desc", &[3, 2, 1] },
        sorted_name_asc = { "name.asc", &[0, 1, 2] },
        sorted_name_desc = { "name.desc", &[3, 2, 1] },
        sorted_created_at_asc = { "created_at.asc", &[0, 1, 2] },
        sorted_created_at_desc = { "created_at.desc", &[3, 2, 1] },
        sorted_namespace_and_id_asc = { "namespace_id.asc,id.asc", &[0, 1, 2] },

    )]
    #[test_macro(actix_web::test)]
    async fn test_api_objects_sorted(sort_order: &str, expected_id_order: &[usize]) {
        let created_objects = create_test_objects(
            &format!("api_objects_sorted_{sort_order}_{expected_id_order:?}"),
            4,
        )
        .await;

        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let namespace_id = created_objects[0].namespace_id;

        let sort_order = if sort_order.is_empty() {
            ""
        } else {
            &format!("&sort={sort_order}")
        };

        let class_id = created_objects[0].hubuum_class_id;
        let resp = get_request(
            &pool,
            &admin_token,
            &format!("{OBJECT_ENDPOINT}/{class_id}/?namespaces={namespace_id}{sort_order}"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let objects: Vec<HubuumObject> = test::read_body_json(resp).await;
        assert_eq!(objects.len(), created_objects.len());
        assert_eq!(objects[0].id, created_objects[expected_id_order[0]].id);
        assert_eq!(objects[1].id, created_objects[expected_id_order[1]].id);
        assert_eq!(objects[2].id, created_objects[expected_id_order[2]].id);
    }

    #[parameterized(
        limit_2 = { 2 },
        limit_5 = { 5 },
        limit_7 = { 6 } // Max possible hits
    )]
    #[test_macro(actix_web::test)]
    async fn test_api_objects_limit(limit: usize) {
        let created_objects = create_test_objects(&format!("api_objects_limit_{limit}"), 6).await;

        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let namespace_id = created_objects[0].namespace_id;
        let class_id = created_objects[0].hubuum_class_id;

        // Limit to 2 results
        let resp = get_request(
            &pool,
            &admin_token,
            &format!(
                "{OBJECT_ENDPOINT}/{class_id}/?namespaces={namespace_id}&limit={limit}&sort=id"
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let objects: Vec<HubuumObject> = test::read_body_json(resp).await;
        assert_eq!(objects.len(), limit);
    }
}
