#[cfg(test)]
mod tests {
    use std::vec;

    use crate::models::namespace::{Namespace, NewNamespaceRequest, UpdateNamespace};

    use crate::tests::api_operations::{delete_request, get_request, patch_request, post_request};
    use crate::tests::asserts::{assert_contains, assert_contains_all, assert_response_status};
    use crate::tests::{create_namespace, ensure_admin_user, setup_pool_and_tokens};
    use actix_web::{http, test};

    const NAMESPACE_ENDPOINT: &str = "/api/v1/namespaces";

    #[actix_web::test]
    async fn test_looking_up_namespaces() {
        let (pool, admin_token, _normal_token) = setup_pool_and_tokens().await;

        let resp = get_request(&pool, "", NAMESPACE_ENDPOINT).await;
        let _ = assert_response_status(resp, http::StatusCode::UNAUTHORIZED).await;

        let created_namespace1 = create_namespace(&pool, "test_namespace_lookup1")
            .await
            .unwrap();
        let resp = get_request(&pool, &admin_token, NAMESPACE_ENDPOINT).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let namespaces: Vec<crate::models::namespace::Namespace> = test::read_body_json(resp).await;
        assert_contains(&namespaces, &created_namespace1);

        let created_namespace2 = create_namespace(&pool, "test_namespace_lookup2")
            .await
            .unwrap();
        let resp = get_request(&pool, &admin_token, NAMESPACE_ENDPOINT).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let updated_namespaces: Vec<crate::models::namespace::Namespace> =
            test::read_body_json(resp).await;
        assert_contains_all(
            &updated_namespaces,
            &vec![created_namespace1, created_namespace2],
        );
    }

    #[actix_web::test]
    async fn test_create_patch_delete_namespace() {
        let (pool, admin_token, normal_token) = setup_pool_and_tokens().await;

        let resp = get_request(&pool, "", NAMESPACE_ENDPOINT).await;
        let _ = assert_response_status(resp, http::StatusCode::UNAUTHORIZED).await;

        let content = NewNamespaceRequest {
            name: "test_namespace_create".to_string(),
            description: "test namespace create description".to_string(),
            assign_to_user_id: Some(ensure_admin_user(&pool).await.id),
            assign_to_group_id: None,
        };

        let resp = post_request(&pool, &normal_token, NAMESPACE_ENDPOINT, &content).await;
        let _ = assert_response_status(resp, http::StatusCode::FORBIDDEN).await;

        let resp = post_request(&pool, &admin_token, NAMESPACE_ENDPOINT, &content).await;
        let resp = assert_response_status(resp, http::StatusCode::CREATED).await;

        let headers = resp.headers().clone();
        let created_ns_url = headers.get("Location").unwrap().to_str().unwrap();
        let created_ns_from_create: Namespace = test::read_body_json(resp).await;

        let resp = get_request(&pool, &admin_token, &created_ns_url).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;

        let created_ns_from_get: Namespace = test::read_body_json(resp).await;
        assert_eq!(created_ns_from_get.name, content.name);
        assert_eq!(created_ns_from_get.description, content.description);
        assert_eq!(created_ns_from_create, created_ns_from_get);

        let patch_content = UpdateNamespace {
            name: Some("test_namespace_patch".to_string()),
            description: Some("test namespace patch description".to_string()),
        };

        let resp = patch_request(&pool, &normal_token, created_ns_url, &patch_content).await;
        let _ = assert_response_status(resp, http::StatusCode::NOT_FOUND).await;

        let resp = patch_request(&pool, &admin_token, created_ns_url, &patch_content).await;
        let _ = assert_response_status(resp, http::StatusCode::ACCEPTED).await;

        let resp = get_request(&pool, &admin_token, created_ns_url).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;

        let patched_ns: Namespace = test::read_body_json(resp).await;
        assert_eq!(patched_ns.name, patch_content.name.unwrap());
        assert_eq!(patched_ns.description, patch_content.description.unwrap());

        let resp = delete_request(&pool, &normal_token, created_ns_url).await;
        let _ = assert_response_status(resp, http::StatusCode::NOT_FOUND).await;

        let resp = delete_request(&pool, &admin_token, created_ns_url).await;
        let _ = assert_response_status(resp, http::StatusCode::NO_CONTENT).await;

        let resp = get_request(&pool, &admin_token, created_ns_url).await;
        let _ = assert_response_status(resp, http::StatusCode::NOT_FOUND).await;
    }
}
