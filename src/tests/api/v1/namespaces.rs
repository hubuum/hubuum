#[cfg(test)]
mod tests {
    use std::vec;

    use crate::models::namespace::{Namespace, NewNamespaceWithAssignee, UpdateNamespace};
    use crate::models::output::GroupNamespacePermission;
    use crate::models::permissions::{NamespacePermission, NamespacePermissions};

    use crate::tests::api_operations::{delete_request, get_request, patch_request, post_request};
    use crate::tests::asserts::{assert_contains, assert_contains_all, assert_response_status};
    use crate::tests::{
        create_namespace, create_test_group, create_test_user, ensure_admin_group,
        setup_pool_and_tokens,
    };
    use crate::traits::CanDelete;
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
        let _ = assert_response_status(resp, http::StatusCode::FORBIDDEN).await;

        let resp = patch_request(&pool, &admin_token, created_ns_url, &patch_content).await;
        let _ = assert_response_status(resp, http::StatusCode::ACCEPTED).await;

        let resp = get_request(&pool, &admin_token, created_ns_url).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;

        let patched_ns: Namespace = test::read_body_json(resp).await;
        assert_eq!(patched_ns.name, patch_content.name.unwrap());
        assert_eq!(patched_ns.description, patch_content.description.unwrap());

        let resp = delete_request(&pool, &normal_token, created_ns_url).await;
        let _ = assert_response_status(resp, http::StatusCode::FORBIDDEN).await;

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
        let _ = assert_response_status(resp, http::StatusCode::FORBIDDEN).await;

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

        created_ns.delete(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_api_namespace_permissions_grant_and_delete_all() {
        let (pool, admin_token, _normal_token) = setup_pool_and_tokens().await;
        let _admin_group = ensure_admin_group(&pool).await;

        let ns = create_namespace(&pool, "test_namespace_permissions_grant")
            .await
            .unwrap();

        let normal_group = create_test_group(&pool).await;

        // Check that normal group has no permissions
        let resp = get_request(
            &pool,
            &admin_token,
            &format!(
                "{}/{}/permissions/group/{}",
                NAMESPACE_ENDPOINT, ns.id, normal_group.id
            ),
        )
        .await;
        let _ = assert_response_status(resp, http::StatusCode::NOT_FOUND).await;

        // Grant read permission to normal group
        let endpoint = &format!(
            "{}/{}/permissions/group/{}/ReadCollection",
            NAMESPACE_ENDPOINT, ns.id, normal_group.id
        );

        let resp = post_request(&pool, &admin_token, endpoint, &()).await;
        let _ = assert_response_status(resp, http::StatusCode::CREATED).await;

        // Check that normal group has read permission
        let resp = get_request(
            &pool,
            &admin_token,
            &format!(
                "{}/{}/permissions/group/{}",
                NAMESPACE_ENDPOINT, ns.id, normal_group.id
            ),
        )
        .await;

        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let np: NamespacePermission = test::read_body_json(resp).await;
        assert_eq!(np.group_id, normal_group.id);
        assert_eq!(np.namespace_id, ns.id);
        assert_eq!(np.has_read_namespace, true);
        assert_eq!(np.has_update_namespace, false);
        assert_eq!(np.has_delete_namespace, false);
        assert_eq!(np.has_delegate_namespace, false);
        assert_eq!(np.has_create_object, false);
        assert_eq!(np.has_create_class, false);

        // Delete all permissions for normal group
        let resp = delete_request(
            &pool,
            &admin_token,
            &format!(
                "{}/{}/permissions/group/{}",
                NAMESPACE_ENDPOINT, ns.id, normal_group.id
            ),
        )
        .await;

        let _ = assert_response_status(resp, http::StatusCode::NO_CONTENT).await;

        // Check that normal group has no permissions
        let resp = get_request(
            &pool,
            &admin_token,
            &format!(
                "{}/{}/permissions/group/{}",
                NAMESPACE_ENDPOINT, ns.id, normal_group.id
            ),
        )
        .await;
        let _ = assert_response_status(resp, http::StatusCode::NOT_FOUND).await;

        ns.delete(&pool).await.unwrap();
        normal_group.delete(&pool).await.unwrap();
    }

    /// Test that after granting a permission to a group, the API allows us to perform
    /// the action that the permission grants.
    #[actix_web::test]
    async fn test_api_namespace_grants_work() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let ns = create_namespace(&pool, "test_namespace_grants")
            .await
            .unwrap();
        let test_group = create_test_group(&pool).await;
        let test_user = create_test_user(&pool).await;

        test_group.add_member(&pool, &test_user).await.unwrap();
        let token = test_user.create_token(&pool).await.unwrap().get_token();

        let ns_endpoint = &format!("{}/{}", NAMESPACE_ENDPOINT, ns.id);
        // First, let us verify that test_user can't read the namespace.
        let resp = get_request(&pool, &token, &ns_endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::FORBIDDEN).await;

        // We can verify this by checking the permissions for the user
        let user_perm_endpoint = &format!(
            "{}/{}/permissions/user/{}",
            NAMESPACE_ENDPOINT, ns.id, test_user.id
        );
        let resp = get_request(&pool, &admin_token, &user_perm_endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::NOT_FOUND).await;

        // Now, let us grant test_group read permission to the namespace
        let np_read = NamespacePermissions::ReadCollection;
        ns.grant(&pool, test_group.id, vec![np_read]).await.unwrap();

        // Let's try reading the namespace again
        let resp = get_request(&pool, &token, &ns_endpoint).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let ns_fetched: Namespace = test::read_body_json(resp).await;
        assert_eq!(ns, ns_fetched);

        // We can verify this by checking the permissions for the user, as the user.
        let resp = get_request(&pool, &token, &user_perm_endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::OK).await;

        // Now, let us grant test_group update permission to the namespace
        let np_update = NamespacePermissions::UpdateCollection;
        ns.grant(&pool, test_group.id, vec![np_update])
            .await
            .unwrap();

        // Let's try updating the namespace
        let update_content = UpdateNamespace {
            name: Some("test_namespace_grants_update".to_string()),
            description: Some("test namespace grants update description".to_string()),
        };

        let resp = patch_request(&pool, &token, &ns_endpoint, &update_content).await;
        let _ = assert_response_status(resp, http::StatusCode::ACCEPTED).await;

        // We can verify this by fetching the namespace again
        let resp = get_request(&pool, &token, &ns_endpoint).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let ns_fetched: Namespace = test::read_body_json(resp).await;
        assert_eq!(ns_fetched.name, update_content.name.unwrap());
        assert_eq!(ns_fetched.description, update_content.description.unwrap());

        // Verify that the user doesn't have permission to delete the namespace
        let resp = delete_request(&pool, &token, &ns_endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::FORBIDDEN).await;

        // Grant test_group delegate permission to the namespace
        let np_delegate = NamespacePermissions::DelegateCollection;
        ns.grant(&pool, test_group.id, vec![np_delegate])
            .await
            .unwrap();

        // And now give ourselves permission to delete the namespace
        let grant_endpoint = &format!(
            "{}/{}/permissions/group/{}/DeleteCollection",
            NAMESPACE_ENDPOINT, ns.id, test_group.id
        );
        let resp = post_request(&pool, &token, &grant_endpoint, &()).await;
        let _ = assert_response_status(resp, http::StatusCode::CREATED).await;

        // Let's try deleting the namespace
        let resp = delete_request(&pool, &token, &ns_endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::NO_CONTENT).await;

        // Verify that the namespace is gone
        let resp = get_request(&pool, &admin_token, &ns_endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::NOT_FOUND).await;

        test_group.delete(&pool).await.unwrap();
        test_user.delete(&pool).await.unwrap();
    }
}
