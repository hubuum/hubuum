#[cfg(test)]
mod tests {
    use std::vec;

    use crate::tests::api_operations::send_get_request;
    use crate::tests::asserts::{assert_contains, assert_contains_all, assert_response_status};
    use crate::tests::{create_namespace, setup_pool_and_admin_user};
    use actix_web::{http, test};

    const NAMESPACE_ENDPOINT: &str = "/api/v1/namespaces";

    #[actix_web::test]
    async fn test_looking_up_namespaces() {
        let (pool, token_string) = setup_pool_and_admin_user().await;

        let resp = send_get_request(&pool, "", NAMESPACE_ENDPOINT).await;
        let _ = assert_response_status(resp, http::StatusCode::UNAUTHORIZED).await;

        let created_namespace1 = create_namespace(&pool, "test_namespace_lookup1").unwrap();
        let resp = send_get_request(&pool, &token_string, NAMESPACE_ENDPOINT).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let namespaces: Vec<crate::models::namespace::Namespace> = test::read_body_json(resp).await;
        assert_contains(&namespaces, &created_namespace1);

        let created_namespace2 = create_namespace(&pool, "test_namespace_lookup2").unwrap();
        let resp = send_get_request(&pool, &token_string, NAMESPACE_ENDPOINT).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let updated_namespaces: Vec<crate::models::namespace::Namespace> =
            test::read_body_json(resp).await;
        assert_contains_all(
            &updated_namespaces,
            &vec![created_namespace1, created_namespace2],
        );
    }
}
