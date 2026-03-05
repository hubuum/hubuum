#[cfg(test)]
mod tests {
    use crate::models::{
        GroupPermission, Namespace, NewGroup, NewNamespaceWithAssignee, Permission, Permissions,
        UpdateNamespace,
    };

    use crate::pagination::NEXT_CURSOR_HEADER;
    use crate::tests::api_operations::{
        delete_request, get_request, patch_request, post_request, put_request,
    };
    use crate::tests::asserts::assert_response_status;
    use crate::tests::asserts::header_value;
    use crate::tests::{
        NamespaceFixture, TestContext, create_test_group, create_test_user, ensure_admin_group,
        test_context,
    };
    use crate::traits::{CanDelete, PermissionController};
    use crate::{assert_contains, assert_contains_all};
    use actix_web::{http, test};
    use rstest::rstest;

    const NAMESPACE_ENDPOINT: &str = "/api/v1/namespaces";

    async fn create_namespaces(
        context: &TestContext,
        prefix: &str,
        count: usize,
    ) -> Vec<NamespaceFixture> {
        context.namespace_fixtures(prefix, count).await
    }

    #[rstest]
    #[actix_web::test]
    async fn test_looking_up_namespaces(#[future(awt)] test_context: TestContext) {
        let context = test_context;

        let resp = get_request(&context.pool, "", NAMESPACE_ENDPOINT).await;
        let _ = assert_response_status(resp, http::StatusCode::UNAUTHORIZED).await;

        let created_namespace1 = context.namespace_fixture("test_namespace_lookup1").await;
        let resp = get_request(&context.pool, &context.admin_token, NAMESPACE_ENDPOINT).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let namespaces: Vec<crate::models::namespace::Namespace> = test::read_body_json(resp).await;
        assert_contains!(&namespaces, &created_namespace1.namespace);

        let created_namespace2 = context.namespace_fixture("test_namespace_lookup2").await;
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{NAMESPACE_ENDPOINT}/"),
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let updated_namespaces: Vec<crate::models::namespace::Namespace> =
            test::read_body_json(resp).await;

        assert_contains_all!(
            &updated_namespaces,
            &[
                created_namespace1.namespace.clone(),
                created_namespace2.namespace.clone()
            ]
        );

        created_namespace1.cleanup().await.unwrap();
        created_namespace2.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_create_patch_delete_namespace(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let admin_group = ensure_admin_group(&context.pool).await;

        let resp = get_request(&context.pool, "", NAMESPACE_ENDPOINT).await;
        let _ = assert_response_status(resp, http::StatusCode::UNAUTHORIZED).await;

        let content = NewNamespaceWithAssignee {
            name: "test_namespace_create".to_string(),
            description: "test namespace create description".to_string(),
            group_id: admin_group.id,
        };

        let resp = post_request(
            &context.pool,
            &context.normal_token,
            NAMESPACE_ENDPOINT,
            &content,
        )
        .await;
        let _ = assert_response_status(resp, http::StatusCode::FORBIDDEN).await;

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            NAMESPACE_ENDPOINT,
            &content,
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::CREATED).await;

        let headers = resp.headers().clone();
        let created_ns_url = headers.get("Location").unwrap().to_str().unwrap();
        let created_ns_from_create: Namespace = test::read_body_json(resp).await;

        let resp = get_request(&context.pool, &context.admin_token, created_ns_url).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;

        let created_ns_from_get: Namespace = test::read_body_json(resp).await;
        assert_eq!(created_ns_from_get.name, content.name);
        assert_eq!(created_ns_from_get.description, content.description);
        assert_eq!(created_ns_from_create, created_ns_from_get);

        let patch_content = UpdateNamespace {
            name: Some("test_namespace_patch".to_string()),
            description: Some("test namespace patch description".to_string()),
        };

        let resp = patch_request(
            &context.pool,
            &context.normal_token,
            created_ns_url,
            &patch_content,
        )
        .await;
        let _ = assert_response_status(resp, http::StatusCode::FORBIDDEN).await;

        let resp = patch_request(
            &context.pool,
            &context.admin_token,
            created_ns_url,
            &patch_content,
        )
        .await;
        let _ = assert_response_status(resp, http::StatusCode::ACCEPTED).await;

        let resp = get_request(&context.pool, &context.admin_token, created_ns_url).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;

        let patched_ns: Namespace = test::read_body_json(resp).await;
        assert_eq!(patched_ns.name, patch_content.name.unwrap());
        assert_eq!(patched_ns.description, patch_content.description.unwrap());

        let resp = delete_request(&context.pool, &context.normal_token, created_ns_url).await;
        let _ = assert_response_status(resp, http::StatusCode::FORBIDDEN).await;

        let resp = delete_request(&context.pool, &context.admin_token, created_ns_url).await;
        let _ = assert_response_status(resp, http::StatusCode::NO_CONTENT).await;

        let resp = get_request(&context.pool, &context.admin_token, created_ns_url).await;
        let _ = assert_response_status(resp, http::StatusCode::NOT_FOUND).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_namespace_permissions(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let admin_group = ensure_admin_group(&context.pool).await;

        let content = NewNamespaceWithAssignee {
            name: "test_namespace_permissions".to_string(),
            description: "test namespace permissions description".to_string(),
            group_id: admin_group.id,
        };

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            NAMESPACE_ENDPOINT,
            &content,
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::CREATED).await;

        let headers = resp.headers().clone();
        let created_ns_url = headers.get("Location").unwrap().to_str().unwrap();

        let resp = get_request(&context.pool, &context.admin_token, created_ns_url).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let created_ns: Namespace = test::read_body_json(resp).await;

        let resp = get_request(&context.pool, &context.normal_token, created_ns_url).await;
        let _ = assert_response_status(resp, http::StatusCode::FORBIDDEN).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
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
        let resp = delete_request(&context.pool, &context.admin_token, endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::NO_CONTENT).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
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

        created_ns.delete(&context.pool).await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_namespace_permissions_grant_and_delete_all(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let _admin_group = ensure_admin_group(&context.pool).await;

        let ns = context
            .namespace_fixture("test_namespace_permissions_grant")
            .await;

        let normal_group = create_test_group(&context.pool).await;

        // Check that normal group has no permissions
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{}/{}/permissions/group/{}",
                NAMESPACE_ENDPOINT, ns.namespace.id, normal_group.id
            ),
        )
        .await;
        let _ = assert_response_status(resp, http::StatusCode::NOT_FOUND).await;

        // Grant read permission to normal group
        let endpoint = &format!(
            "{}/{}/permissions/group/{}/ReadCollection",
            NAMESPACE_ENDPOINT, ns.namespace.id, normal_group.id
        );

        let resp = post_request(&context.pool, &context.admin_token, endpoint, &()).await;
        let _ = assert_response_status(resp, http::StatusCode::CREATED).await;

        // Check that normal group has read permission
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{}/{}/permissions/group/{}",
                NAMESPACE_ENDPOINT, ns.namespace.id, normal_group.id
            ),
        )
        .await;

        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let np: Permission = test::read_body_json(resp).await;
        assert_eq!(np.group_id, normal_group.id);
        assert_eq!(np.namespace_id, ns.namespace.id);
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
            &context.pool,
            &context.admin_token,
            &format!(
                "{}/{}/permissions/group/{}",
                NAMESPACE_ENDPOINT, ns.namespace.id, normal_group.id
            ),
        )
        .await;

        let _ = assert_response_status(resp, http::StatusCode::NO_CONTENT).await;

        // Check that normal group has no permissions
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{}/{}/permissions/group/{}",
                NAMESPACE_ENDPOINT, ns.namespace.id, normal_group.id
            ),
        )
        .await;
        let _ = assert_response_status(resp, http::StatusCode::NOT_FOUND).await;

        ns.cleanup().await.unwrap();
        normal_group.delete(&context.pool).await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_namespace_permissions_put_empty_is_bad_request(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let _admin_group = ensure_admin_group(&context.pool).await;

        let ns = context
            .namespace_fixture("test_namespace_permissions_put_empty")
            .await;
        let normal_group = create_test_group(&context.pool).await;

        let endpoint = &format!(
            "{}/{}/permissions/group/{}",
            NAMESPACE_ENDPOINT, ns.namespace.id, normal_group.id
        );

        let resp = put_request(
            &context.pool,
            &context.admin_token,
            endpoint,
            Vec::<Permissions>::new(),
        )
        .await;
        let _ = assert_response_status(resp, http::StatusCode::BAD_REQUEST).await;

        ns.cleanup().await.unwrap();
        normal_group.delete(&context.pool).await.unwrap();
    }

    /// Test that after granting a permission to a group, the API allows us to perform
    /// the action that the permission grants.
    #[rstest]
    #[actix_web::test]
    async fn test_api_namespace_grants_work(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let ns = context.namespace_fixture("test_namespace_grants").await;
        let test_group = create_test_group(&context.pool).await;
        let test_user = create_test_user(&context.pool).await;

        test_group
            .add_member(&context.pool, &test_user)
            .await
            .unwrap();
        let token = test_user
            .create_token(&context.pool)
            .await
            .unwrap()
            .get_token();

        let ns_endpoint = &format!("{NAMESPACE_ENDPOINT}/{}", ns.namespace.id);
        // First, let us verify that test_user can't read the namespace.
        let resp = get_request(&context.pool, &token, ns_endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::FORBIDDEN).await;

        // We can verify this by checking the permissions for the user
        let user_perm_endpoint = &format!(
            "{NAMESPACE_ENDPOINT}/{}/permissions/user/{}",
            ns.namespace.id, test_user.id
        );
        let resp = get_request(&context.pool, &context.admin_token, user_perm_endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::NOT_FOUND).await;

        // Now, let us grant test_group read permission to the namespace
        let np_read = Permissions::ReadCollection;
        ns.namespace
            .grant_one(&context.pool, test_group.id, np_read)
            .await
            .unwrap();

        // Let's try reading the namespace again
        let resp = get_request(&context.pool, &token, ns_endpoint).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let ns_fetched: Namespace = test::read_body_json(resp).await;
        assert_eq!(ns.namespace, ns_fetched);

        // We can verify this by checking the permissions for the user, as the user.
        let resp = get_request(&context.pool, &token, user_perm_endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::OK).await;

        // Now, let us grant test_group update permission to the namespace
        let np_update = Permissions::UpdateCollection;
        ns.namespace
            .grant_one(&context.pool, test_group.id, np_update)
            .await
            .unwrap();

        // Let's try updating the namespace
        let update_content = UpdateNamespace {
            name: Some("test_namespace_grants_update".to_string()),
            description: Some("test namespace grants update description".to_string()),
        };

        let resp = patch_request(&context.pool, &token, ns_endpoint, &update_content).await;
        let _ = assert_response_status(resp, http::StatusCode::ACCEPTED).await;

        // We can verify this by fetching the namespace again
        let resp = get_request(&context.pool, &token, ns_endpoint).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let ns_fetched: Namespace = test::read_body_json(resp).await;
        assert_eq!(ns_fetched.name, update_content.name.unwrap());
        assert_eq!(ns_fetched.description, update_content.description.unwrap());

        // Verify that the user doesn't have permission to delete the namespace
        let resp = delete_request(&context.pool, &token, ns_endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::FORBIDDEN).await;

        // Grant test_group delegate permission to the namespace
        let np_delegate = Permissions::DelegateCollection;
        ns.namespace
            .grant_one(&context.pool, test_group.id, np_delegate)
            .await
            .unwrap();

        // And now give ourselves permission to delete the namespace
        let grant_endpoint = &format!(
            "{}/{}/permissions/group/{}/DeleteCollection",
            NAMESPACE_ENDPOINT, ns.namespace.id, test_group.id
        );
        let resp = post_request(&context.pool, &token, grant_endpoint, &()).await;
        let _ = assert_response_status(resp, http::StatusCode::CREATED).await;

        // Let's try deleting the namespace
        let resp = delete_request(&context.pool, &token, ns_endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::NO_CONTENT).await;

        // Verify that the namespace is gone
        let resp = get_request(&context.pool, &context.admin_token, ns_endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::NOT_FOUND).await;

        test_group.delete(&context.pool).await.unwrap();
        test_user.delete(&context.pool).await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_namespace_permissions_sorted_and_limited(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let ns = context
            .namespace_fixture("test_namespace_permissions_sorted_and_limited")
            .await;

        let group_one = NewGroup {
            groupname: format!("test_namespace_permissions_sorted_{}_a", ns.namespace.id),
            description: Some(format!(
                "test_namespace_permissions_sorted_{}_description_a",
                ns.namespace.id
            )),
        }
        .save(&context.pool)
        .await
        .unwrap();
        let group_two = NewGroup {
            groupname: format!("test_namespace_permissions_sorted_{}_b", ns.namespace.id),
            description: Some(format!(
                "test_namespace_permissions_sorted_{}_description_b",
                ns.namespace.id
            )),
        }
        .save(&context.pool)
        .await
        .unwrap();

        ns.namespace
            .grant_one(&context.pool, group_one.id, Permissions::ReadCollection)
            .await
            .unwrap();
        ns.namespace
            .grant_one(&context.pool, group_two.id, Permissions::ReadCollection)
            .await
            .unwrap();

        let sorted_endpoint = format!(
            "{NAMESPACE_ENDPOINT}/{}/permissions?permissions=ReadCollection&sort=id.desc",
            ns.namespace.id
        );
        let resp = get_request(&context.pool, &context.admin_token, &sorted_endpoint).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let permissions: Vec<GroupPermission> = test::read_body_json(resp).await;

        assert!(permissions.len() >= 2);
        assert!(permissions[0].permission.id > permissions[1].permission.id);
        assert!(permissions.iter().any(|p| p.group.id == group_one.id));
        assert!(permissions.iter().any(|p| p.group.id == group_two.id));

        let filtered_endpoint = format!(
            "{NAMESPACE_ENDPOINT}/{}/permissions?groupname__contains={}&sort=id",
            ns.namespace.id, group_one.groupname
        );
        let resp = get_request(&context.pool, &context.admin_token, &filtered_endpoint).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let filtered_permissions: Vec<GroupPermission> = test::read_body_json(resp).await;
        assert_eq!(filtered_permissions.len(), 1);
        assert_eq!(filtered_permissions[0].group.id, group_one.id);

        let limited_endpoint = format!(
            "{NAMESPACE_ENDPOINT}/{}/permissions?permissions=ReadCollection&sort=id&limit=1",
            ns.namespace.id
        );
        let resp = get_request(&context.pool, &context.admin_token, &limited_endpoint).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let limited_permissions: Vec<GroupPermission> = test::read_body_json(resp).await;
        assert_eq!(limited_permissions.len(), 1);

        group_one.delete(&context.pool).await.unwrap();
        group_two.delete(&context.pool).await.unwrap();
        ns.cleanup().await.unwrap();
    }

    #[rstest]
    #[case::sorted_id_default("id", &[0, 1, 2])]
    #[case::sorted_id_explicit_asc("id.asc", &[0, 1, 2])]
    #[case::sorted_id_descending("id.desc", &[3, 2, 1])]
    #[case::sorted_name_asc("name.asc", &[0, 1, 2])]
    #[case::sorted_name_desc("name.desc", &[3, 2, 1])]
    #[case::sorted_created_at_asc("created_at.asc", &[0, 1, 2])]
    #[case::sorted_created_at_desc("created_at.desc", &[3, 2, 1])]
    #[actix_web::test]
    async fn test_api_namespaces_sorted(
        #[case] sort_order: &str,
        #[case] expected_id_order: &[usize],
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let created_namespaces = create_namespaces(
            &context,
            &format!("api_namespaces_sorted_{sort_order}_{expected_id_order:?}"),
            4,
        )
        .await;

        let sort_order = if sort_order.is_empty() {
            ""
        } else {
            &format!("&sort={sort_order}")
        };

        let comma_separated_ids = created_namespaces
            .iter()
            .map(|fixture| fixture.namespace.id.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{NAMESPACE_ENDPOINT}/?id={comma_separated_ids}{sort_order}"),
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let objects: Vec<Namespace> = test::read_body_json(resp).await;
        assert_eq!(objects.len(), created_namespaces.len());
        assert_eq!(
            objects[0].id,
            created_namespaces[expected_id_order[0]].namespace.id
        );
        assert_eq!(
            objects[1].id,
            created_namespaces[expected_id_order[1]].namespace.id
        );
        assert_eq!(
            objects[2].id,
            created_namespaces[expected_id_order[2]].namespace.id
        );

        NamespaceFixture::cleanup_all(&created_namespaces)
            .await
            .unwrap();
    }

    #[rstest]
    #[case::limit_2(2)]
    #[case::limit_5(5)]
    #[case::limit_7(6)]
    #[actix_web::test]
    async fn test_api_namespaces_limit(
        #[case] limit: usize,
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let created_namespaces =
            create_namespaces(&context, &format!("api_namespaces_limit_{limit}"), 6).await;
        let comma_separated_ids = created_namespaces
            .iter()
            .map(|fixture| fixture.namespace.id.to_string())
            .collect::<Vec<_>>()
            .join(",");

        // Limit to 2 results
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{NAMESPACE_ENDPOINT}/?id={comma_separated_ids}&limit={limit}&sort=id"),
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let objects: Vec<Namespace> = test::read_body_json(resp).await;
        assert_eq!(objects.len(), limit);

        NamespaceFixture::cleanup_all(&created_namespaces)
            .await
            .unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_namespaces_cursor_pagination(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let created_namespaces = create_namespaces(&context, "api_namespaces_cursor", 6).await;
        let comma_separated_ids = created_namespaces
            .iter()
            .map(|fixture| fixture.namespace.id.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{NAMESPACE_ENDPOINT}/?id={comma_separated_ids}&limit=2&sort=id"),
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let next_cursor = header_value(&resp, NEXT_CURSOR_HEADER);
        let objects: Vec<Namespace> = test::read_body_json(resp).await;

        assert_eq!(objects.len(), 2);
        assert_eq!(objects[0].id, created_namespaces[0].namespace.id);
        assert_eq!(objects[1].id, created_namespaces[1].namespace.id);
        assert!(next_cursor.is_some());

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{NAMESPACE_ENDPOINT}/?id={comma_separated_ids}&limit=2&sort=id&cursor={}",
                next_cursor.unwrap()
            ),
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let objects: Vec<Namespace> = test::read_body_json(resp).await;
        assert_eq!(objects.len(), 2);
        assert_eq!(objects[0].id, created_namespaces[2].namespace.id);
        assert_eq!(objects[1].id, created_namespaces[3].namespace.id);

        NamespaceFixture::cleanup_all(&created_namespaces)
            .await
            .unwrap();
    }
}
