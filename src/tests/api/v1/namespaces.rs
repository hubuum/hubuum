#[cfg(test)]
mod tests {
    use crate::models::{
        GroupPermission, Namespace, NewNamespaceWithAssignee, Permission, Permissions,
        UpdateNamespace,
    };

    use crate::tests::api_operations::{
        delete_request, get_request, patch_request, post_request, put_request,
    };
    use crate::tests::asserts::assert_response_status;
    use crate::tests::{
        create_namespace, create_test_group, create_test_user, ensure_admin_group,
        setup_pool_and_tokens,
    };
    use crate::traits::{CanDelete, PermissionController};
    use crate::{assert_contains, assert_contains_all};
    use actix_web::{http, test};
    use yare::parameterized;

    const NAMESPACE_ENDPOINT: &str = "/api/v1/namespaces";

    async fn create_namespaces(prefix: &str, count: usize) -> Vec<Namespace> {
        let (pool, _, _) = setup_pool_and_tokens().await;
        let mut namespaces = Vec::new();
        for i in 0..count {
            let namespace = create_namespace(&pool, &format!("{prefix}_{i}"))
                .await
                .unwrap();
            namespaces.push(namespace);
        }
        namespaces
    }

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
        assert_contains!(&namespaces, &created_namespace1);

        let created_namespace2 = create_namespace(&pool, "test_namespace_lookup2")
            .await
            .unwrap();
        let resp = get_request(&pool, &admin_token, &format!("{NAMESPACE_ENDPOINT}/")).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let updated_namespaces: Vec<crate::models::namespace::Namespace> =
            test::read_body_json(resp).await;

        assert_contains_all!(
            &updated_namespaces,
            &[created_namespace1, created_namespace2]
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

        let resp = get_request(&pool, &admin_token, created_ns_url).await;
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
            &format!("{created_ns_url}/permissions"),
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let permissions: Vec<GroupPermission> = test::read_body_json(resp).await;
        assert_eq!(permissions.len(), 1);
        let np = permissions[0].permission;
        assert_eq!(np.group_id, admin_group.id);
        assert_eq!(np.namespace_id, created_ns.id);
        assert!(np.has_read_namespace);
        assert!(np.has_update_namespace);
        assert!(np.has_delete_namespace);
        assert!(np.has_delegate_namespace);
        assert!(np.has_create_class);
        assert!(np.has_read_class);
        assert!(np.has_update_class);
        assert!(np.has_delete_class);
        assert!(np.has_create_object);
        assert!(np.has_read_object);
        assert!(np.has_update_object);
        assert!(np.has_delete_object);
        assert_eq!(permissions[0].group, admin_group);

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
            &format!("{created_ns_url}/permissions"),
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let permissions: Vec<GroupPermission> = test::read_body_json(resp).await;
        assert_eq!(permissions.len(), 1);
        let np = permissions[0].permission;
        assert_eq!(np.group_id, admin_group.id);
        assert_eq!(np.namespace_id, created_ns.id);
        assert!(np.has_read_namespace);
        assert!(np.has_update_namespace);
        assert!(np.has_delete_namespace);
        assert!(np.has_delegate_namespace);
        assert!(np.has_create_class);
        assert!(np.has_read_class);
        assert!(np.has_update_class);
        assert!(np.has_delete_class);
        assert!(!np.has_create_object);
        assert!(np.has_read_object);
        assert!(np.has_update_object);
        assert!(np.has_delete_object);
        assert_eq!(permissions[0].group, admin_group);

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
        let np: Permission = test::read_body_json(resp).await;
        assert_eq!(np.group_id, normal_group.id);
        assert_eq!(np.namespace_id, ns.id);
        assert!(np.has_read_namespace);
        assert!(!np.has_update_namespace);
        assert!(!np.has_delete_namespace);
        assert!(!np.has_delegate_namespace);
        assert!(!np.has_create_class);
        assert!(!np.has_read_class);
        assert!(!np.has_update_class);
        assert!(!np.has_delete_class);
        assert!(!np.has_create_object);
        assert!(!np.has_read_object);
        assert!(!np.has_update_object);
        assert!(!np.has_delete_object);

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

    #[actix_web::test]
    async fn test_api_namespace_permissions_put_empty_is_bad_request() {
        let (pool, admin_token, _normal_token) = setup_pool_and_tokens().await;
        let _admin_group = ensure_admin_group(&pool).await;

        let ns = create_namespace(&pool, "test_namespace_permissions_put_empty")
            .await
            .unwrap();
        let normal_group = create_test_group(&pool).await;

        let endpoint = &format!(
            "{}/{}/permissions/group/{}",
            NAMESPACE_ENDPOINT, ns.id, normal_group.id
        );

        let resp = put_request(&pool, &admin_token, endpoint, Vec::<Permissions>::new()).await;
        let _ = assert_response_status(resp, http::StatusCode::BAD_REQUEST).await;

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

        let ns_endpoint = &format!("{NAMESPACE_ENDPOINT}/{}", ns.id);
        // First, let us verify that test_user can't read the namespace.
        let resp = get_request(&pool, &token, ns_endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::FORBIDDEN).await;

        // We can verify this by checking the permissions for the user
        let user_perm_endpoint = &format!(
            "{NAMESPACE_ENDPOINT}/{}/permissions/user/{}",
            ns.id, test_user.id
        );
        let resp = get_request(&pool, &admin_token, user_perm_endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::NOT_FOUND).await;

        // Now, let us grant test_group read permission to the namespace
        let np_read = Permissions::ReadCollection;
        ns.grant_one(&pool, test_group.id, np_read).await.unwrap();

        // Let's try reading the namespace again
        let resp = get_request(&pool, &token, ns_endpoint).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let ns_fetched: Namespace = test::read_body_json(resp).await;
        assert_eq!(ns, ns_fetched);

        // We can verify this by checking the permissions for the user, as the user.
        let resp = get_request(&pool, &token, user_perm_endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::OK).await;

        // Now, let us grant test_group update permission to the namespace
        let np_update = Permissions::UpdateCollection;
        ns.grant_one(&pool, test_group.id, np_update).await.unwrap();

        // Let's try updating the namespace
        let update_content = UpdateNamespace {
            name: Some("test_namespace_grants_update".to_string()),
            description: Some("test namespace grants update description".to_string()),
        };

        let resp = patch_request(&pool, &token, ns_endpoint, &update_content).await;
        let _ = assert_response_status(resp, http::StatusCode::ACCEPTED).await;

        // We can verify this by fetching the namespace again
        let resp = get_request(&pool, &token, ns_endpoint).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let ns_fetched: Namespace = test::read_body_json(resp).await;
        assert_eq!(ns_fetched.name, update_content.name.unwrap());
        assert_eq!(ns_fetched.description, update_content.description.unwrap());

        // Verify that the user doesn't have permission to delete the namespace
        let resp = delete_request(&pool, &token, ns_endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::FORBIDDEN).await;

        // Grant test_group delegate permission to the namespace
        let np_delegate = Permissions::DelegateCollection;
        ns.grant_one(&pool, test_group.id, np_delegate)
            .await
            .unwrap();

        // And now give ourselves permission to delete the namespace
        let grant_endpoint = &format!(
            "{}/{}/permissions/group/{}/DeleteCollection",
            NAMESPACE_ENDPOINT, ns.id, test_group.id
        );
        let resp = post_request(&pool, &token, grant_endpoint, &()).await;
        let _ = assert_response_status(resp, http::StatusCode::CREATED).await;

        // Let's try deleting the namespace
        let resp = delete_request(&pool, &token, ns_endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::NO_CONTENT).await;

        // Verify that the namespace is gone
        let resp = get_request(&pool, &admin_token, ns_endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::NOT_FOUND).await;

        test_group.delete(&pool).await.unwrap();
        test_user.delete(&pool).await.unwrap();
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

    )]
    #[test_macro(actix_web::test)]
    async fn test_api_namespaces_sorted(sort_order: &str, expected_id_order: &[usize]) {
        let created_namespaces = create_namespaces(
            &format!("api_namespaces_sorted_{sort_order}_{expected_id_order:?}"),
            4,
        )
        .await;

        let (pool, admin_token, _) = setup_pool_and_tokens().await;

        let sort_order = if sort_order.is_empty() {
            ""
        } else {
            &format!("&sort={sort_order}")
        };

        let comma_separated_ids = created_namespaces
            .iter()
            .map(|ns| ns.id.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let resp = get_request(
            &pool,
            &admin_token,
            &format!("{NAMESPACE_ENDPOINT}/?id={comma_separated_ids}{sort_order}"),
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let objects: Vec<Namespace> = test::read_body_json(resp).await;
        assert_eq!(objects.len(), created_namespaces.len());
        assert_eq!(objects[0].id, created_namespaces[expected_id_order[0]].id);
        assert_eq!(objects[1].id, created_namespaces[expected_id_order[1]].id);
        assert_eq!(objects[2].id, created_namespaces[expected_id_order[2]].id);

        for i in created_namespaces {
            i.delete(&pool).await.unwrap();
        }
    }

    #[parameterized(
        limit_2 = { 2 },
        limit_5 = { 5 },
        limit_7 = { 6 } // Max possible hits
    )]
    #[test_macro(actix_web::test)]
    async fn test_api_namespaces_limit(limit: usize) {
        let created_namespaces =
            create_namespaces(&format!("api_namespaces_limit_{limit}"), 6).await;

        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let comma_separated_ids = created_namespaces
            .iter()
            .map(|ns| ns.id.to_string())
            .collect::<Vec<_>>()
            .join(",");

        // Limit to 2 results
        let resp = get_request(
            &pool,
            &admin_token,
            &format!("{NAMESPACE_ENDPOINT}/?id={comma_separated_ids}&limit={limit}&sort=id"),
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let objects: Vec<Namespace> = test::read_body_json(resp).await;
        assert_eq!(objects.len(), limit);

        for i in created_namespaces {
            i.delete(&pool).await.unwrap();
        }
    }
}
