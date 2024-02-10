#[cfg(test)]
mod tests {
    use std::vec;

    use crate::models::namespace::{Namespace, NewNamespaceWithAssignee, UpdateNamespace};
    use crate::models::output::GroupNamespacePermission;

    use crate::tests::api_operations::{delete_request, get_request, patch_request, post_request};
    use crate::tests::asserts::{assert_contains, assert_contains_all, assert_response_status};
    use crate::tests::{create_namespace, ensure_admin_group, setup_pool_and_tokens};
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
        let admin_group = ensure_admin_group(&pool).await;

        let resp = get_request(&pool, "", NAMESPACE_ENDPOINT).await;
        let _ = assert_response_status(resp, http::StatusCode::UNAUTHORIZED).await;

        let content = NewNamespaceWithAssignee {
            name: "test_namespace_create".to_string(),
            description: "test namespace create description".to_string(),
            group_id: admin_group.id,
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

    #[actix_web::test]
    async fn test_api_namespace_permissions() {
        let (pool, admin_token, normal_token) = setup_pool_and_tokens().await;
        let admin_group = ensure_admin_group(&pool).await;

        let content = NewNamespaceWithAssignee {
            name: "test_namespace_permissions".to_string(),
            description: "test namespace permissions description".to_string(),
            group_id: admin_group.id,
        };

        let resp = post_request(&pool, &admin_token, NAMESPACE_ENDPOINT, &content).await;
        let resp = assert_response_status(resp, http::StatusCode::CREATED).await;

        let headers = resp.headers().clone();
        let created_ns_url = headers.get("Location").unwrap().to_str().unwrap();

        let resp = get_request(&pool, &admin_token, created_ns_url).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let created_ns: Namespace = test::read_body_json(resp).await;

        let resp = get_request(&pool, &normal_token, created_ns_url).await;
        let _ = assert_response_status(resp, http::StatusCode::NOT_FOUND).await;

        let resp = get_request(
            &pool,
            &admin_token,
            &format!("{}/permissions", created_ns_url),
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let permissions: Vec<GroupNamespacePermission> = test::read_body_json(resp).await;
        assert_eq!(permissions.len(), 1);
        let np = permissions[0].namespace_permission;
        assert_eq!(np.group_id, admin_group.id);
        assert_eq!(np.namespace_id, created_ns.id);
        assert_eq!(np.has_create_object, true);
        assert_eq!(np.has_create_class, true);
        assert_eq!(np.has_read_namespace, true);
        assert_eq!(np.has_update_namespace, true);
        assert_eq!(np.has_delete_namespace, true);
        assert_eq!(np.has_delegate_namespace, true);
        assert_eq!(permissions[0].group.id, admin_group.id);

        // Revoke create object permission
        let endpoint = &format!(
            "{}/permissions/group/{}/CreateObject",
            created_ns_url, admin_group.id
        );
        let resp = delete_request(&pool, &admin_token, endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::NO_CONTENT).await;

        let resp = get_request(
            &pool,
            &admin_token,
            &format!("{}/permissions", created_ns_url),
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let permissions: Vec<GroupNamespacePermission> = test::read_body_json(resp).await;
        assert_eq!(permissions.len(), 1);
        let np = permissions[0].namespace_permission;
        assert_eq!(np.group_id, admin_group.id);
        assert_eq!(np.namespace_id, created_ns.id);
        assert_eq!(np.has_create_object, false);
        assert_eq!(np.has_create_class, true);
        assert_eq!(np.has_read_namespace, true);
        assert_eq!(np.has_update_namespace, true);
        assert_eq!(np.has_delete_namespace, true);
        assert_eq!(np.has_delegate_namespace, true);
        assert_eq!(permissions[0].group.id, admin_group.id);
    }
}
