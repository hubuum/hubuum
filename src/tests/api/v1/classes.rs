#[cfg(test)]
mod tests {
    use crate::models::class::{HubuumClass, NewHubuumClass};
    use crate::traits::CanSave;
    use actix_web::{http, test};

    use crate::tests::api_operations::get_request;
    use crate::tests::asserts::assert_response_status;
    use crate::tests::constants::{get_schema, SchemaType};
    use crate::tests::{create_namespace, setup_pool_and_tokens};
    use crate::{assert_contains_all, assert_contains_same_ids};

    const CLASSES_ENDPOINT: &str = "/api/v1/classes";

    async fn create_test_classes(prefix: &str) -> Vec<crate::models::class::HubuumClass> {
        let (pool, _, _) = setup_pool_and_tokens().await;

        let ns = create_namespace(&pool, &format!("{}_{}", prefix, "api_create_test_classes"))
            .await
            .unwrap();

        let mut created_classes = vec![];

        for i in 1..6 {
            let schema = if i == 5 {
                get_schema(SchemaType::Geo).clone()
            } else if i > 2 {
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

    async fn api_get_classes_with_query_string(query_string: &str) -> Vec<HubuumClass> {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let resp = get_request(
            &pool,
            &admin_token,
            &format!("{}?{}", CLASSES_ENDPOINT, query_string),
        )
        .await;

        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let classes: Vec<HubuumClass> = test::read_body_json(resp).await;
        classes
    }

    #[actix_web::test]
    async fn test_api_classes_get() {
        let created_classes = create_test_classes("get").await;

        let (pool, admin_token, _) = setup_pool_and_tokens().await;

        let resp = get_request(&pool, &admin_token, CLASSES_ENDPOINT).await;

        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let classes: Vec<HubuumClass> = test::read_body_json(resp).await;

        // We can't do
        // assert_eq!(classes.len(), created_classes.len());
        // As we may have other classes generated by other tests
        assert_contains_all!(&classes, &created_classes);
    }

    #[actix_web::test]
    async fn test_api_classes_get_filtered_name_equals() {
        let created_classes = create_test_classes("get_filtered_name_equals").await;
        let query_string = format!("name={}", created_classes[0].name);
        let classes = api_get_classes_with_query_string(&query_string).await;
        assert_eq!(classes.len(), 1);
        assert_eq!(classes[0].name, created_classes[0].name);
    }

    #[actix_web::test]
    async fn test_api_classes_get_filtered_name_contains() {
        let created_classes = create_test_classes("get_filtered_name_contains").await;
        let query_string = "name__contains=get_filtered_name_contains";
        let classes = api_get_classes_with_query_string(&query_string).await;
        assert_contains_same_ids!(&classes, &created_classes);
    }

    #[actix_web::test]
    async fn test_api_classes_get_filtered_description_contains() {
        let created_classes = create_test_classes("get_filtered_description_contains").await;
        let query_string = "description__contains=get_filtered_description_contains";
        let classes = api_get_classes_with_query_string(&query_string).await;
        assert_contains_same_ids!(&classes, &created_classes);
    }

    #[actix_web::test]
    async fn test_api_classes_get_filtered_description_and_not_name_contains() {
        create_test_classes("get_filtered_description_and_not_name_contains").await;
        let query_string =
            "description__contains=get_filtered_description_and_not_name_contains&name__not_contains=1";
        let classes = api_get_classes_with_query_string(&query_string).await;
        assert_eq!(classes.len(), 4);
    }

    #[actix_web::test]
    async fn test_api_classes_get_filtered_namespaces_equals() {
        let created_classes = create_test_classes("get_filtered_namespaces_equals").await;
        let query_string = format!("namespaces={}", created_classes[0].namespace_id);
        let classes = api_get_classes_with_query_string(&query_string).await;
        assert_contains_same_ids!(&classes, &created_classes);
    }

    fn combine_query_string(prefix: &String, query_string: &str) -> String {
        format!("{}&{}", prefix, query_string)
    }

    #[actix_web::test]
    async fn test_api_classes_get_filtered_json_schema() {
        let prefix = "get_filtered_classes_json_schema";
        let base_filter = format!("name__contains={}", prefix);
        let created_classes = create_test_classes("get_filtered_classes_json_schema").await;
        let query_string =
            combine_query_string(&base_filter, "json_schema__contains=description=blog");
        let classes = api_get_classes_with_query_string(&query_string).await;
        assert_eq!(classes.len(), 2);
        assert_contains_same_ids!(&classes, &created_classes[0..2]);

        let query_string =
            combine_query_string(&base_filter, "json_schema__not_contains=description=blog");
        let classes = api_get_classes_with_query_string(&query_string).await;
        assert_eq!(classes.len(), 3);
        assert_contains_same_ids!(&classes, &created_classes[2..5]);

        let query_string = combine_query_string(
            &base_filter,
            "json_schema__lt=properties,latitude,minimum=0",
        );
        let classes = api_get_classes_with_query_string(&query_string).await;
        assert_eq!(classes.len(), 1);
        assert_eq!(&classes[0], &created_classes[4]);

        let query_string =
            combine_query_string(&base_filter, "json_schema=properties,latitude,minimum=-90");
        let classes = api_get_classes_with_query_string(&query_string).await;
        assert_eq!(classes.len(), 1);
        assert_eq!(&classes[0], &created_classes[4]);
    }
}
