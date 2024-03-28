#[cfg(test)]
mod tests {
    use crate::models::class::NewHubuumClass;
    use crate::traits::CanSave;
    use actix_web::{http, test};

    use crate::assert_contains_all;
    use crate::tests::api_operations::get_request;
    use crate::tests::asserts::assert_response_status;
    use crate::tests::constants::{get_schema, SchemaType};
    use crate::tests::{create_namespace, setup_pool_and_tokens};

    const CLASSES_ENDPOINT: &str = "/api/v1/classes";

    async fn create_test_classes() -> Vec<crate::models::class::HubuumClass> {
        let (pool, _, _) = setup_pool_and_tokens().await;

        let ns = create_namespace(&pool, "api_create_test_classes")
            .await
            .unwrap();

        let mut created_classes = vec![];

        for i in 0..5 {
            let class = NewHubuumClass {
                name: format!("class{}", i),
                description: format!("description{}", i),
                namespace_id: ns.id,
                json_schema: get_schema(SchemaType::Blog).clone(),
                validate_schema: false,
            };

            created_classes.push(class.save(&pool).await.unwrap());
        }
        created_classes
    }

    #[actix_web::test]
    async fn test_api_get_classes() {
        let created_classes = create_test_classes().await;

        let (pool, admin_token, _) = setup_pool_and_tokens().await;

        let resp = get_request(&pool, &admin_token, CLASSES_ENDPOINT).await;

        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let classes: Vec<crate::models::class::HubuumClass> = test::read_body_json(resp).await;

        assert_eq!(classes.len(), created_classes.len());
        assert_contains_all!(&created_classes, &classes);
    }
}
