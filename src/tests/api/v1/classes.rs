#[cfg(test)]
pub mod tests {
    use crate::models::{HubuumClass, NamespaceID, NewHubuumClass};
    use crate::traits::{CanDelete, CanSave};
    use actix_web::{http::StatusCode, test};

    use crate::tests::api_operations::{delete_request, get_request, patch_request, post_request};
    use crate::tests::asserts::assert_response_status;
    use crate::tests::constants::{get_schema, SchemaType};
    use crate::tests::{create_namespace, setup_pool_and_tokens};
    use crate::{assert_contains_all, assert_contains_same_ids};

    const CLASSES_ENDPOINT: &str = "/api/v1/classes";

    pub async fn create_test_classes(prefix: &str) -> Vec<crate::models::class::HubuumClass> {
        let (pool, _, _) = setup_pool_and_tokens().await;

        let ns = create_namespace(&pool, &format!("{}_{}", prefix, "api_create_test_classes"))
            .await
            .unwrap();

        let mut created_classes = vec![];

        for i in 1..7 {
            let schema = if i == 6 {
                get_schema(SchemaType::Geo).clone()
            } else if i > 3 {
                get_schema(SchemaType::Address).clone()
            } else {
                get_schema(SchemaType::Blog).clone()
            };

            let class = NewHubuumClass {
                name: format!("{}_api_class_{}", prefix, i),
                description: format!("{}_api_description_{}", prefix, i),
                namespace_id: ns.id,
                json_schema: schema,
                validate_schema: false,
            };

            created_classes.push(class.save(&pool).await.unwrap());
        }
        created_classes
    }

    pub async fn cleanup(classes: &Vec<HubuumClass>) {
        let (pool, _, _) = setup_pool_and_tokens().await;
        let namespaces = classes
            .iter()
            .map(|c| NamespaceID(c.namespace_id))
            .collect::<Vec<NamespaceID>>();

        for ns in namespaces {
            ns.delete(&pool).await.unwrap();
        }
    }

    async fn api_get_classes_with_query_string(query_string: &str) -> Vec<HubuumClass> {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let resp = get_request(
            &pool,
            &admin_token,
            &format!("{}?{}", CLASSES_ENDPOINT, query_string),
        )
        .await;

        let resp = assert_response_status(resp, StatusCode::OK).await;
        let classes: Vec<HubuumClass> = test::read_body_json(resp).await;
        classes
    }

    #[actix_web::test]
    async fn test_api_classes_get() {
        let created_classes = create_test_classes("get").await;

        let (pool, admin_token, _) = setup_pool_and_tokens().await;

        let resp = get_request(&pool, &admin_token, CLASSES_ENDPOINT).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let classes: Vec<HubuumClass> = test::read_body_json(resp).await;

        // We can't do
        // assert_eq!(classes.len(), created_classes.len());
        // As we may have other classes generated by other tests (and we're explicitly not filtering on
        // a given namespace)
        assert_contains_all!(&classes, &created_classes);

        // Check that we can do api/v1/classes/ as well as api/v1/classes
        let resp = get_request(&pool, &admin_token, &format!("{}/", CLASSES_ENDPOINT)).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let classes: Vec<HubuumClass> = test::read_body_json(resp).await;
        assert_contains_all!(&classes, &created_classes);
        cleanup(&created_classes).await;
    }

    #[actix_web::test]
    async fn test_api_classes_get_filtered_name_equals() {
        let created_classes = create_test_classes("get_filtered_name_equals").await;
        let query_string = format!("name={}", created_classes[0].name);
        let classes = api_get_classes_with_query_string(&query_string).await;
        assert_eq!(classes.len(), 1);
        assert_eq!(classes[0].name, created_classes[0].name);
        cleanup(&created_classes).await;
    }

    #[actix_web::test]
    async fn test_api_classes_get_filtered_name_contains() {
        let created_classes = create_test_classes("get_filtered_name_contains").await;
        let query_string = "name__contains=get_filtered_name_contains";
        let classes = api_get_classes_with_query_string(&query_string).await;
        assert_contains_same_ids!(&classes, &created_classes);
        cleanup(&created_classes).await;
    }

    #[actix_web::test]
    async fn test_api_classes_get_filtered_description_contains() {
        let created_classes = create_test_classes("get_filtered_description_contains").await;
        let query_string = "description__contains=get_filtered_description_contains";
        let classes = api_get_classes_with_query_string(&query_string).await;
        assert_contains_same_ids!(&classes, &created_classes);
        cleanup(&created_classes).await;
    }

    #[actix_web::test]
    async fn test_api_classes_get_filtered_description_and_not_name_contains() {
        let created_classes =
            create_test_classes("get_filtered_description_and_not_name_contains").await;
        let query_string =
            "description__contains=get_filtered_description_and_not_name_contains&name__not_contains=1";
        let classes = api_get_classes_with_query_string(&query_string).await;
        assert_eq!(classes.len(), 5);
        cleanup(&created_classes).await;
    }

    #[actix_web::test]
    async fn test_api_classes_get_filtered_namespaces_equals() {
        let created_classes = create_test_classes("get_filtered_namespaces_equals").await;
        let query_string = format!("namespaces={}", created_classes[0].namespace_id);
        let classes = api_get_classes_with_query_string(&query_string).await;
        assert_contains_same_ids!(&classes, &created_classes);
        cleanup(&created_classes).await;
    }

    fn combine_query_string(prefix: &String, query_string: &str) -> String {
        format!("{}&{}", prefix, query_string)
    }

    #[actix_web::test]
    async fn test_api_classes_get_filtered_json_schema() {
        let prefix = "get_filtered_classes_json_schema";
        let base_filter = format!("name__contains={}", prefix);

        // We have 6 classes, 3 with blog (0,1,2), 2 with address (3,4) and 1 with geo (5)
        let created_classes = create_test_classes("get_filtered_classes_json_schema").await;

        let query_string = combine_query_string(&base_filter, "json_schema__contains=$id=blog");
        let classes = api_get_classes_with_query_string(&query_string).await;
        assert_eq!(classes.len(), 3);
        assert_contains_same_ids!(&classes, &created_classes[0..3]);

        let query_string =
            combine_query_string(&base_filter, "json_schema__contains=description=blog");
        let classes = api_get_classes_with_query_string(&query_string).await;
        assert_eq!(classes.len(), 3);
        assert_contains_same_ids!(&classes, &created_classes[0..3]);

        let query_string =
            combine_query_string(&base_filter, "json_schema__not_contains=description=blog");
        let classes = api_get_classes_with_query_string(&query_string).await;
        assert_eq!(classes.len(), 3);
        assert_contains_same_ids!(&classes, &created_classes[3..6]);

        let query_string = combine_query_string(
            &base_filter,
            "json_schema__lt=properties,latitude,minimum=0",
        );
        let classes = api_get_classes_with_query_string(&query_string).await;
        assert_eq!(classes.len(), 1);
        assert_eq!(&classes[0], &created_classes[5]);

        let query_string =
            combine_query_string(&base_filter, "json_schema=properties,latitude,minimum=-90");
        let classes = api_get_classes_with_query_string(&query_string).await;
        assert_eq!(classes.len(), 1);
        assert_eq!(&classes[0], &created_classes[5]);

        let query_string =
            combine_query_string(&base_filter, "json_schema__contains=required=region");
        let classes = api_get_classes_with_query_string(&query_string).await;
        assert_eq!(classes.len(), 2);
        assert_contains_same_ids!(&classes, &created_classes[3..5]);
        cleanup(&created_classes).await;
    }

    #[actix_web::test]
    async fn test_api_classes_get_by_id() {
        let created_classes = create_test_classes("api_classes_get_by_id").await;

        let (pool, admin_token, _) = setup_pool_and_tokens().await;

        for class in &created_classes {
            let resp = get_request(
                &pool,
                &admin_token,
                &format!("{}/{}", CLASSES_ENDPOINT, class.id),
            )
            .await;
            let resp = assert_response_status(resp, StatusCode::OK).await;
            let returned_class: HubuumClass = test::read_body_json(resp).await;
            assert_eq!(class, &returned_class);
        }
        cleanup(&created_classes).await;
    }

    #[actix_web::test]
    async fn test_api_classes_get_failure() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;

        // It'd be really nice if we could garantee that this id doesn't exist...
        for id in 999990..1000000 {
            let resp =
                get_request(&pool, &admin_token, &format!("{}/{}", CLASSES_ENDPOINT, id)).await;
            assert_response_status(resp, StatusCode::NOT_FOUND).await;
        }
    }

    #[actix_web::test]
    async fn test_api_classes_create() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;

        let ns = create_namespace(&pool, "api_create_test_classes")
            .await
            .unwrap();

        let new_class = NewHubuumClass {
            name: "api_create_test_classes".to_string(),
            description: "api_create_test_classes".to_string(),
            namespace_id: ns.id,
            json_schema: get_schema(SchemaType::Blog).clone(),
            validate_schema: false,
        };

        let resp = post_request(
            &pool,
            &admin_token,
            &format!("{}", CLASSES_ENDPOINT),
            &new_class,
        )
        .await;

        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let headers = resp.headers().clone();
        let created_class_from_create: HubuumClass = test::read_body_json(resp).await;
        let created_class_url = headers.get("Location").unwrap().to_str().unwrap();

        let resp = get_request(&pool, &admin_token, created_class_url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let created_class: HubuumClass = test::read_body_json(resp).await;

        // Validate that the location is what we expect
        assert_eq!(
            created_class_url,
            &format!("{}/{}", CLASSES_ENDPOINT, created_class.id)
        );

        assert_eq!(created_class, created_class_from_create);
        ns.delete(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_api_classes_patch() {
        use crate::models::UpdateHubuumClass;

        let (pool, admin_token, _) = setup_pool_and_tokens().await;

        let ns = create_namespace(&pool, "api_patch_test_classes")
            .await
            .unwrap();

        let new_class = NewHubuumClass {
            name: "api_patch_test_classes".to_string(),
            description: "api_patch_test_classes_desc".to_string(),
            namespace_id: ns.id,
            json_schema: get_schema(SchemaType::Blog).clone(),
            validate_schema: false,
        };
        let created_class = new_class.save(&pool).await.unwrap();

        let update_class = UpdateHubuumClass {
            name: Some("api_patch_test_classes_2".to_string()),
            namespace_id: None,
            json_schema: None,
            validate_schema: None,
            description: None,
        };

        let resp = patch_request(
            &pool,
            &admin_token,
            &format!("{}/{}", CLASSES_ENDPOINT, created_class.id),
            &update_class,
        )
        .await;

        let resp = assert_response_status(resp, StatusCode::OK).await;
        let updated_class_from_patch: HubuumClass = test::read_body_json(resp).await;
        let resp = get_request(
            &pool,
            &admin_token,
            &format!("{}/{}", CLASSES_ENDPOINT, created_class.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let updated_class_from_get: HubuumClass = test::read_body_json(resp).await;

        assert_eq!(updated_class_from_patch, updated_class_from_get);
        assert_ne!(created_class, updated_class_from_patch);

        assert_eq!(updated_class_from_patch.name, "api_patch_test_classes_2");
        assert_eq!(
            updated_class_from_patch.description,
            created_class.description
        );
        assert_eq!(
            updated_class_from_patch.namespace_id,
            created_class.namespace_id
        );
        assert_eq!(
            updated_class_from_patch.json_schema,
            created_class.json_schema
        );
        assert_eq!(updated_class_from_patch.validate_schema, false);
    }

    #[actix_web::test]
    async fn test_api_classes_delete() {
        let created_classes = create_test_classes("api_classes_delete").await;

        for class in &created_classes {
            let (pool, admin_token, _) = setup_pool_and_tokens().await;
            let resp = delete_request(
                &pool,
                &admin_token,
                &format!("{}/{}", CLASSES_ENDPOINT, class.id),
            )
            .await;
            assert_response_status(resp, StatusCode::NO_CONTENT).await;

            let resp = get_request(
                &pool,
                &admin_token,
                &format!("{}/{}", CLASSES_ENDPOINT, class.id),
            )
            .await;
            assert_response_status(resp, StatusCode::NOT_FOUND).await;
        }
    }
}
