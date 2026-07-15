#[cfg(test)]
mod tests {
    use crate::models::{
        Collection, GroupPermission, GroupResponse, NewCollectionWithAssignee, NewGroup,
        Permission, Permissions, UpdateCollection,
    };

    use crate::pagination::{NEXT_CURSOR_HEADER, TOTAL_COUNT_HEADER};
    use crate::tests::api_operations::{
        delete_request, get_request, patch_request, post_request, put_request,
    };
    use crate::tests::asserts::{
        assert_paginated_collection_total_count, assert_response_status, header_value,
    };
    use crate::tests::{
        CollectionFixture, TestContext, create_test_group, create_test_user, ensure_admin_group,
        test_context,
    };
    use crate::traits::{CanDelete, PermissionController};
    use crate::{assert_contains, assert_contains_all};
    use actix_web::{http, test};
    use rstest::rstest;

    const COLLECTION_ENDPOINT: &str = "/api/v1/collections";

    async fn create_collections(
        context: &TestContext,
        prefix: &str,
        count: usize,
    ) -> Vec<CollectionFixture> {
        context.collection_fixtures(prefix, count).await
    }

    #[rstest]
    #[actix_web::test]
    async fn test_looking_up_collections(#[future(awt)] test_context: TestContext) {
        let context = test_context;

        let resp = get_request(&context.pool, "", COLLECTION_ENDPOINT).await;
        let _ = assert_response_status(resp, http::StatusCode::UNAUTHORIZED).await;

        let created_collection1 = context.collection_fixture("test_collection_lookup1").await;
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{COLLECTION_ENDPOINT}?name__equals={}",
                created_collection1.collection.name
            ),
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let collections: Vec<crate::models::collection::Collection> =
            test::read_body_json(resp).await;
        assert_contains!(&collections, &created_collection1.collection);

        let created_collection2 = context.collection_fixture("test_collection_lookup2").await;
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{COLLECTION_ENDPOINT}/?name__contains=test_collection_lookup"),
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let updated_collections: Vec<crate::models::collection::Collection> =
            test::read_body_json(resp).await;

        assert_contains_all!(
            &updated_collections,
            &[
                created_collection1.collection.clone(),
                created_collection2.collection.clone()
            ]
        );

        created_collection1.cleanup().await.unwrap();
        created_collection2.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_admin_can_list_collections_without_direct_owner_group_membership(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let detached_collection = context
            .scope
            .collection_fixture("admin_lists_hidden_collection")
            .await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{COLLECTION_ENDPOINT}?id={}",
                detached_collection.collection.id
            ),
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let collections: Vec<Collection> = test::read_body_json(resp).await;

        assert_eq!(collections, vec![detached_collection.collection.clone()]);

        detached_collection.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_create_patch_delete_collection(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let admin_group = ensure_admin_group(&context.pool).await;

        let resp = get_request(&context.pool, "", COLLECTION_ENDPOINT).await;
        let _ = assert_response_status(resp, http::StatusCode::UNAUTHORIZED).await;

        let content = NewCollectionWithAssignee {
            name: "test_collection_create".to_string(),
            description: "test collection create description".to_string(),
            group_id: admin_group.id,
            parent_collection_id: None,
        };

        let resp = post_request(
            &context.pool,
            &context.normal_token,
            COLLECTION_ENDPOINT,
            &content,
        )
        .await;
        let _ = assert_response_status(resp, http::StatusCode::FORBIDDEN).await;

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            COLLECTION_ENDPOINT,
            &content,
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::CREATED).await;

        let headers = resp.headers().clone();
        let created_collection_url = headers.get("Location").unwrap().to_str().unwrap();
        let created_collection_from_create: Collection = test::read_body_json(resp).await;

        let resp = get_request(&context.pool, &context.admin_token, created_collection_url).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;

        let created_collection_from_get: Collection = test::read_body_json(resp).await;
        assert_eq!(created_collection_from_get.name, content.name);
        assert_eq!(created_collection_from_get.description, content.description);
        assert_eq!(created_collection_from_create, created_collection_from_get);

        let patch_content = UpdateCollection {
            name: Some("test_collection_patch".to_string()),
            description: Some("test collection patch description".to_string()),
        };

        let resp = patch_request(
            &context.pool,
            &context.normal_token,
            created_collection_url,
            &patch_content,
        )
        .await;
        let _ = assert_response_status(resp, http::StatusCode::FORBIDDEN).await;

        let resp = patch_request(
            &context.pool,
            &context.admin_token,
            created_collection_url,
            &patch_content,
        )
        .await;
        let _ = assert_response_status(resp, http::StatusCode::ACCEPTED).await;

        let resp = get_request(&context.pool, &context.admin_token, created_collection_url).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;

        let patched_collection: Collection = test::read_body_json(resp).await;
        assert_eq!(patched_collection.name, patch_content.name.unwrap());
        assert_eq!(
            patched_collection.description,
            patch_content.description.unwrap()
        );

        let resp =
            delete_request(&context.pool, &context.normal_token, created_collection_url).await;
        let _ = assert_response_status(resp, http::StatusCode::FORBIDDEN).await;

        let resp =
            delete_request(&context.pool, &context.admin_token, created_collection_url).await;
        let _ = assert_response_status(resp, http::StatusCode::NO_CONTENT).await;

        let resp = get_request(&context.pool, &context.admin_token, created_collection_url).await;
        let _ = assert_response_status(resp, http::StatusCode::NOT_FOUND).await;
    }

    // Invalid collection ids are refused during path extraction (`CollectionID`'s validating
    // `Deserialize` plus the `PathConfig` error handler), so the request is rejected at the edge
    // with a `400` rather than surfacing as a confusing lookup miss further in. Covers
    // non-positive values rejected by `new` and non-integer segments rejected while parsing `i32`.
    #[rstest]
    #[case::zero("0")]
    #[case::negative_one("-1")]
    #[case::i32_min("-2147483648")]
    #[case::non_numeric("abc")]
    #[case::non_integer("1.5")]
    #[actix_web::test]
    async fn test_invalid_collection_id_in_path_is_rejected(
        #[future(awt)] test_context: TestContext,
        #[case] invalid_id: &str,
    ) {
        let context = test_context;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{COLLECTION_ENDPOINT}/{invalid_id}"),
        )
        .await;
        let _ = assert_response_status(resp, http::StatusCode::BAD_REQUEST).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_invalid_parent_collection_id_in_body_is_rejected(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let admin_group = ensure_admin_group(&context.pool).await;
        let collection = context
            .collection_fixture("invalid_parent_collection_id_body")
            .await;

        let create_body = serde_json::json!({
            "name": context.scoped_name("invalid_parent_collection_id_create"),
            "description": "invalid parent collection id",
            "group_id": admin_group.id,
            "parent_collection_id": 0
        });
        let resp = post_request(
            &context.pool,
            &context.admin_token,
            COLLECTION_ENDPOINT,
            &create_body,
        )
        .await;
        let _ = assert_response_status(resp, http::StatusCode::BAD_REQUEST).await;

        let move_body = serde_json::json!({
            "parent_collection_id": 0
        });
        let resp = put_request(
            &context.pool,
            &context.admin_token,
            &format!("{COLLECTION_ENDPOINT}/{}/parent", collection.collection.id),
            &move_body,
        )
        .await;
        let _ = assert_response_status(resp, http::StatusCode::BAD_REQUEST).await;

        collection.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_collection_permissions(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let admin_group = ensure_admin_group(&context.pool).await;

        let content = NewCollectionWithAssignee {
            name: "test_collection_permissions".to_string(),
            description: "test collection permissions description".to_string(),
            group_id: admin_group.id,
            parent_collection_id: None,
        };

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            COLLECTION_ENDPOINT,
            &content,
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::CREATED).await;

        let headers = resp.headers().clone();
        let created_collection_url = headers.get("Location").unwrap().to_str().unwrap();

        let resp = get_request(&context.pool, &context.admin_token, created_collection_url).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let created_collection: Collection = test::read_body_json(resp).await;

        let resp = get_request(&context.pool, &context.normal_token, created_collection_url).await;
        let _ = assert_response_status(resp, http::StatusCode::FORBIDDEN).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{created_collection_url}/permissions"),
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let permissions: Vec<GroupPermission> = test::read_body_json(resp).await;
        assert_eq!(permissions.len(), 1);
        let np = permissions[0].permission;
        assert_eq!(np.group_id, admin_group.id);
        assert_eq!(np.collection_id, created_collection.id);
        assert!(np.has_read_collection);
        assert!(np.has_update_collection);
        assert!(np.has_delete_collection);
        assert!(np.has_delegate_collection);
        assert!(np.has_create_class);
        assert!(np.has_read_class);
        assert!(np.has_update_class);
        assert!(np.has_delete_class);
        assert!(np.has_create_object);
        assert!(np.has_read_object);
        assert!(np.has_update_object);
        assert!(np.has_delete_object);
        assert!(np.has_read_template);
        assert!(np.has_create_template);
        assert!(np.has_update_template);
        assert!(np.has_delete_template);
        assert_eq!(permissions[0].group, admin_group);

        // Revoke create object permission
        let endpoint = &format!(
            "{}/permissions/group/{}/CreateObject",
            created_collection_url, admin_group.id
        );
        let resp = delete_request(&context.pool, &context.admin_token, endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::NO_CONTENT).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{created_collection_url}/permissions"),
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let permissions: Vec<GroupPermission> = test::read_body_json(resp).await;
        assert_eq!(permissions.len(), 1);
        let np = permissions[0].permission;
        assert_eq!(np.group_id, admin_group.id);
        assert_eq!(np.collection_id, created_collection.id);
        assert!(np.has_read_collection);
        assert!(np.has_update_collection);
        assert!(np.has_delete_collection);
        assert!(np.has_delegate_collection);
        assert!(np.has_create_class);
        assert!(np.has_read_class);
        assert!(np.has_update_class);
        assert!(np.has_delete_class);
        assert!(!np.has_create_object);
        assert!(np.has_read_object);
        assert!(np.has_update_object);
        assert!(np.has_delete_object);
        assert!(np.has_read_template);
        assert!(np.has_create_template);
        assert!(np.has_update_template);
        assert!(np.has_delete_template);
        assert_eq!(permissions[0].group, admin_group);

        created_collection
            .delete_without_events(&context.pool)
            .await
            .unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_collection_permissions_grant_and_delete_all(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let _admin_group = ensure_admin_group(&context.pool).await;

        let collection_fixture = context
            .collection_fixture("test_collection_permissions_grant")
            .await;

        let normal_group = create_test_group(&context.pool).await;

        // Check that normal group has no permissions
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{}/{}/permissions/group/{}",
                COLLECTION_ENDPOINT, collection_fixture.collection.id, normal_group.id
            ),
        )
        .await;
        let _ = assert_response_status(resp, http::StatusCode::NOT_FOUND).await;

        // Grant read permission to normal group
        let endpoint = &format!(
            "{}/{}/permissions/group/{}/ReadCollection",
            COLLECTION_ENDPOINT, collection_fixture.collection.id, normal_group.id
        );

        let resp = post_request(&context.pool, &context.admin_token, endpoint, &()).await;
        let _ = assert_response_status(resp, http::StatusCode::CREATED).await;

        // Check that normal group has read permission
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{}/{}/permissions/group/{}",
                COLLECTION_ENDPOINT, collection_fixture.collection.id, normal_group.id
            ),
        )
        .await;

        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let np: Permission = test::read_body_json(resp).await;
        assert_eq!(np.group_id, normal_group.id);
        assert_eq!(np.collection_id, collection_fixture.collection.id);
        assert!(np.has_read_collection);
        assert!(!np.has_update_collection);
        assert!(!np.has_delete_collection);
        assert!(!np.has_delegate_collection);
        assert!(!np.has_create_class);
        assert!(!np.has_read_class);
        assert!(!np.has_update_class);
        assert!(!np.has_delete_class);
        assert!(!np.has_create_object);
        assert!(!np.has_read_object);
        assert!(!np.has_update_object);
        assert!(!np.has_delete_object);
        assert!(!np.has_read_template);
        assert!(!np.has_create_template);
        assert!(!np.has_update_template);
        assert!(!np.has_delete_template);

        // Delete all permissions for normal group
        let resp = delete_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{}/{}/permissions/group/{}",
                COLLECTION_ENDPOINT, collection_fixture.collection.id, normal_group.id
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
                COLLECTION_ENDPOINT, collection_fixture.collection.id, normal_group.id
            ),
        )
        .await;
        let _ = assert_response_status(resp, http::StatusCode::NOT_FOUND).await;

        collection_fixture.cleanup().await.unwrap();
        normal_group
            .delete_without_events(&context.pool)
            .await
            .unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_collection_permissions_put_empty_is_bad_request(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let _admin_group = ensure_admin_group(&context.pool).await;

        let collection_fixture = context
            .collection_fixture("test_collection_permissions_put_empty")
            .await;
        let normal_group = create_test_group(&context.pool).await;

        let endpoint = &format!(
            "{}/{}/permissions/group/{}",
            COLLECTION_ENDPOINT, collection_fixture.collection.id, normal_group.id
        );

        let resp = put_request(
            &context.pool,
            &context.admin_token,
            endpoint,
            Vec::<Permissions>::new(),
        )
        .await;
        let _ = assert_response_status(resp, http::StatusCode::BAD_REQUEST).await;

        collection_fixture.cleanup().await.unwrap();
        normal_group
            .delete_without_events(&context.pool)
            .await
            .unwrap();
    }

    /// Test that after granting a permission to a group, the API allows us to perform
    /// the action that the permission grants.
    #[rstest]
    #[actix_web::test]
    async fn test_api_collection_grants_work(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let collection_fixture = context.collection_fixture("test_collection_grants").await;
        let test_group = create_test_group(&context.pool).await;
        let test_user = create_test_user(&context.pool).await;

        test_group
            .add_member_without_events(&context.pool, &test_user)
            .await
            .unwrap();
        let token = test_user
            .create_token(&context.pool)
            .await
            .unwrap()
            .get_token();

        let collection_endpoint =
            &format!("{COLLECTION_ENDPOINT}/{}", collection_fixture.collection.id);
        // First, let us verify that test_user can't read the collection.
        let resp = get_request(&context.pool, &token, collection_endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::FORBIDDEN).await;

        // We can verify this by checking the permissions for the user
        let user_perm_endpoint = &format!(
            "{COLLECTION_ENDPOINT}/{}/permissions/principal/{}",
            collection_fixture.collection.id, test_user.id
        );
        let resp = get_request(&context.pool, &context.admin_token, user_perm_endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::NOT_FOUND).await;

        // Now, let us grant test_group read permission to the collection
        let np_read = Permissions::ReadCollection;
        collection_fixture
            .collection
            .grant_one(&context.pool, test_group.id, np_read)
            .await
            .unwrap();

        // Let's try reading the collection again
        let resp = get_request(&context.pool, &token, collection_endpoint).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let collection_fetched: Collection = test::read_body_json(resp).await;
        assert_eq!(collection_fixture.collection, collection_fetched);

        // We can verify this by checking the permissions for the user, as the user.
        let resp = get_request(&context.pool, &token, user_perm_endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::OK).await;

        // Now, let us grant test_group update permission to the collection
        let np_update = Permissions::UpdateCollection;
        collection_fixture
            .collection
            .grant_one(&context.pool, test_group.id, np_update)
            .await
            .unwrap();

        // Let's try updating the collection
        let update_content = UpdateCollection {
            name: Some("test_collection_grants_update".to_string()),
            description: Some("test collection grants update description".to_string()),
        };

        let resp = patch_request(&context.pool, &token, collection_endpoint, &update_content).await;
        let _ = assert_response_status(resp, http::StatusCode::ACCEPTED).await;

        // We can verify this by fetching the collection again
        let resp = get_request(&context.pool, &token, collection_endpoint).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let collection_fetched: Collection = test::read_body_json(resp).await;
        assert_eq!(collection_fetched.name, update_content.name.unwrap());
        assert_eq!(
            collection_fetched.description,
            update_content.description.unwrap()
        );

        // Verify that the user doesn't have permission to delete the collection
        let resp = delete_request(&context.pool, &token, collection_endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::FORBIDDEN).await;

        // Grant test_group delegate permission to the collection
        let np_delegate = Permissions::DelegateCollection;
        collection_fixture
            .collection
            .grant_one(&context.pool, test_group.id, np_delegate)
            .await
            .unwrap();

        // And now give ourselves permission to delete the collection
        let grant_endpoint = &format!(
            "{}/{}/permissions/group/{}/DeleteCollection",
            COLLECTION_ENDPOINT, collection_fixture.collection.id, test_group.id
        );
        let resp = post_request(&context.pool, &token, grant_endpoint, &()).await;
        let _ = assert_response_status(resp, http::StatusCode::CREATED).await;

        // Let's try deleting the collection
        let resp = delete_request(&context.pool, &token, collection_endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::NO_CONTENT).await;

        // Verify that the collection is gone
        let resp = get_request(&context.pool, &context.admin_token, collection_endpoint).await;
        let _ = assert_response_status(resp, http::StatusCode::NOT_FOUND).await;

        test_group
            .delete_without_events(&context.pool)
            .await
            .unwrap();
        test_user
            .delete_without_events(&context.pool)
            .await
            .unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_collection_permissions_sorted_and_limited(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let collection_fixture = context
            .collection_fixture("test_collection_permissions_sorted_and_limited")
            .await;

        let group_one = NewGroup {
            identity_scope: None,
            groupname: format!(
                "test_collection_permissions_sorted_{}_a",
                collection_fixture.collection.id
            ),
            description: Some(format!(
                "test_collection_permissions_sorted_{}_description_a",
                collection_fixture.collection.id
            )),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();
        let group_two = NewGroup {
            identity_scope: None,
            groupname: format!(
                "test_collection_permissions_sorted_{}_b",
                collection_fixture.collection.id
            ),
            description: Some(format!(
                "test_collection_permissions_sorted_{}_description_b",
                collection_fixture.collection.id
            )),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();

        collection_fixture
            .collection
            .grant_one(&context.pool, group_one.id, Permissions::ReadCollection)
            .await
            .unwrap();
        collection_fixture
            .collection
            .grant_one(&context.pool, group_two.id, Permissions::ReadCollection)
            .await
            .unwrap();

        let sorted_endpoint = format!(
            "{COLLECTION_ENDPOINT}/{}/permissions?permissions=ReadCollection&sort=id.desc",
            collection_fixture.collection.id
        );
        let resp = get_request(&context.pool, &context.admin_token, &sorted_endpoint).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let permissions: Vec<GroupPermission> = test::read_body_json(resp).await;

        assert!(permissions.len() >= 2);
        assert!(permissions[0].permission.id > permissions[1].permission.id);
        assert!(permissions.iter().any(|p| p.group.id == group_one.id));
        assert!(permissions.iter().any(|p| p.group.id == group_two.id));

        let filtered_endpoint = format!(
            "{COLLECTION_ENDPOINT}/{}/permissions?groupname__contains={}&sort=id",
            collection_fixture.collection.id, group_one.groupname
        );
        let resp = get_request(&context.pool, &context.admin_token, &filtered_endpoint).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let filtered_permissions: Vec<GroupPermission> = test::read_body_json(resp).await;
        assert_eq!(filtered_permissions.len(), 1);
        assert_eq!(filtered_permissions[0].group.id, group_one.id);

        let limited_endpoint = format!(
            "{COLLECTION_ENDPOINT}/{}/permissions?permissions=ReadCollection&sort=id&limit=1",
            collection_fixture.collection.id
        );
        let resp = get_request(&context.pool, &context.admin_token, &limited_endpoint).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let limited_permissions: Vec<GroupPermission> = test::read_body_json(resp).await;
        assert_eq!(limited_permissions.len(), 1);

        group_one
            .delete_without_events(&context.pool)
            .await
            .unwrap();
        group_two
            .delete_without_events(&context.pool)
            .await
            .unwrap();
        collection_fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_collection_permission_listings_total_count_match_paginated_results(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let collection_fixture = context
            .collection_fixture("collection_permissions_total_count")
            .await;
        let group_one = NewGroup {
            identity_scope: None,
            groupname: format!(
                "collection-total-count-group-a-{}",
                collection_fixture.collection.id
            ),
            description: Some("group a".to_string()),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();
        let group_two = NewGroup {
            identity_scope: None,
            groupname: format!(
                "collection-total-count-group-b-{}",
                collection_fixture.collection.id
            ),
            description: Some("group b".to_string()),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();
        let group_three = NewGroup {
            identity_scope: None,
            groupname: format!(
                "collection-total-count-group-c-{}",
                collection_fixture.collection.id
            ),
            description: Some("group c".to_string()),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();
        let user = create_test_user(&context.pool).await;

        group_one
            .add_member_without_events(&context.pool, &user)
            .await
            .unwrap();
        group_two
            .add_member_without_events(&context.pool, &user)
            .await
            .unwrap();

        for group in [&group_one, &group_two, &group_three] {
            collection_fixture
                .collection
                .grant_one(&context.pool, group.id, Permissions::ReadCollection)
                .await
                .unwrap();
        }

        let (permissions, permissions_total): (Vec<GroupPermission>, i64) =
            assert_paginated_collection_total_count(
            &context.pool,
            &context.admin_token,
            10,
            |cursor| match cursor {
                Some(cursor) => format!(
                    "{COLLECTION_ENDPOINT}/{}/permissions?permissions=ReadCollection&groupname__contains=collection-total-count-group&sort=id&limit=2&cursor={cursor}",
                    collection_fixture.collection.id
                ),
                None => format!(
                    "{COLLECTION_ENDPOINT}/{}/permissions?permissions=ReadCollection&groupname__contains=collection-total-count-group&sort=id&limit=2",
                    collection_fixture.collection.id
                ),
            },
        )
        .await;
        assert_eq!(permissions_total, 3);
        assert_eq!(permissions.len(), 3);

        let (user_permissions, user_permissions_total): (Vec<GroupPermission>, i64) =
            assert_paginated_collection_total_count(
            &context.pool,
            &context.admin_token,
            10,
            |cursor| match cursor {
                Some(cursor) => format!(
                    "{COLLECTION_ENDPOINT}/{}/permissions/principal/{}?sort=id&limit=1&cursor={cursor}",
                    collection_fixture.collection.id, user.id
                ),
                None => format!(
                    "{COLLECTION_ENDPOINT}/{}/permissions/principal/{}?sort=id&limit=1",
                    collection_fixture.collection.id, user.id
                ),
            },
        )
        .await;
        assert_eq!(user_permissions_total, 2);
        assert_eq!(user_permissions.len(), 2);

        let (groups, groups_total): (Vec<GroupResponse>, i64) =
            assert_paginated_collection_total_count(
            &context.pool,
            &context.admin_token,
            10,
            |cursor| match cursor {
                Some(cursor) => format!(
                    "{COLLECTION_ENDPOINT}/{}/has_permissions/ReadCollection?groupname__contains=collection-total-count-group&sort=id&limit=2&cursor={cursor}",
                    collection_fixture.collection.id
                ),
                None => format!(
                    "{COLLECTION_ENDPOINT}/{}/has_permissions/ReadCollection?groupname__contains=collection-total-count-group&sort=id&limit=2",
                    collection_fixture.collection.id
                ),
            },
            )
            .await;
        assert_eq!(groups_total, 3);
        assert_eq!(groups.len(), 3);

        group_one
            .delete_without_events(&context.pool)
            .await
            .unwrap();
        group_two
            .delete_without_events(&context.pool)
            .await
            .unwrap();
        group_three
            .delete_without_events(&context.pool)
            .await
            .unwrap();
        user.delete_without_events(&context.pool).await.unwrap();
        collection_fixture.cleanup().await.unwrap();
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
    async fn test_api_collections_sorted(
        #[case] sort_order: &str,
        #[case] expected_id_order: &[usize],
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let created_collections = create_collections(
            &context,
            &format!("api_collections_sorted_{sort_order}_{expected_id_order:?}"),
            4,
        )
        .await;

        let sort_order = if sort_order.is_empty() {
            ""
        } else {
            &format!("&sort={sort_order}")
        };

        let comma_separated_ids = created_collections
            .iter()
            .map(|fixture| fixture.collection.id.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{COLLECTION_ENDPOINT}/?id={comma_separated_ids}{sort_order}"),
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let objects: Vec<Collection> = test::read_body_json(resp).await;
        assert_eq!(objects.len(), created_collections.len());
        assert_eq!(
            objects[0].id,
            created_collections[expected_id_order[0]].collection.id
        );
        assert_eq!(
            objects[1].id,
            created_collections[expected_id_order[1]].collection.id
        );
        assert_eq!(
            objects[2].id,
            created_collections[expected_id_order[2]].collection.id
        );

        CollectionFixture::cleanup_all(&created_collections)
            .await
            .unwrap();
    }

    #[rstest]
    #[case::limit_2(2)]
    #[case::limit_5(5)]
    #[case::limit_7(6)]
    #[actix_web::test]
    async fn test_api_collections_limit(
        #[case] limit: usize,
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let created_collections =
            create_collections(&context, &format!("api_collections_limit_{limit}"), 6).await;
        let comma_separated_ids = created_collections
            .iter()
            .map(|fixture| fixture.collection.id.to_string())
            .collect::<Vec<_>>()
            .join(",");

        // Limit to 2 results
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{COLLECTION_ENDPOINT}/?id={comma_separated_ids}&limit={limit}&sort=id"),
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let objects: Vec<Collection> = test::read_body_json(resp).await;
        assert_eq!(objects.len(), limit);

        CollectionFixture::cleanup_all(&created_collections)
            .await
            .unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_collections_cursor_pagination(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let created_collections = create_collections(&context, "api_collections_cursor", 6).await;
        let comma_separated_ids = created_collections
            .iter()
            .map(|fixture| fixture.collection.id.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{COLLECTION_ENDPOINT}/?id={comma_separated_ids}&limit=2&sort=id"),
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let next_cursor = header_value(&resp, NEXT_CURSOR_HEADER);
        let objects: Vec<Collection> = test::read_body_json(resp).await;

        assert_eq!(objects.len(), 2);
        assert_eq!(objects[0].id, created_collections[0].collection.id);
        assert_eq!(objects[1].id, created_collections[1].collection.id);
        assert!(next_cursor.is_some());

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{COLLECTION_ENDPOINT}/?id={comma_separated_ids}&limit=2&sort=id&cursor={}",
                next_cursor.unwrap()
            ),
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let objects: Vec<Collection> = test::read_body_json(resp).await;
        assert_eq!(objects.len(), 2);
        assert_eq!(objects[0].id, created_collections[2].collection.id);
        assert_eq!(objects[1].id, created_collections[3].collection.id);

        CollectionFixture::cleanup_all(&created_collections)
            .await
            .unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_collections_can_skip_exact_total_count(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let created = create_collections(&context, "api_collections_skip_total", 3).await;
        let ids = created
            .iter()
            .map(|fixture| fixture.collection.id.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{COLLECTION_ENDPOINT}/?id={ids}&limit=2&sort=id&include_total=false"),
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        assert!(resp.headers().get(TOTAL_COUNT_HEADER).is_none());
        assert!(resp.headers().get(NEXT_CURSOR_HEADER).is_some());
        let page: Vec<Collection> = test::read_body_json(resp).await;
        assert_eq!(page.len(), 2);

        CollectionFixture::cleanup_all(&created).await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_collection_history_list_and_as_of(#[future(awt)] test_context: TestContext) {
        use crate::models::UpdateCollection;
        use crate::traits::CanUpdate;

        let context = test_context;
        let collection_fixture = context.collection_fixture("collection_history_api").await;
        let event_context = hubuum_events_core::EventContext::system();

        // Create then update so there are two versions.
        let created = collection_fixture.collection.clone();
        UpdateCollection {
            name: None,
            description: Some("v2".to_string()),
        }
        .update(&context.pool, created.id, &event_context)
        .await
        .unwrap();

        // List history newest-first.
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{}/{}/history", COLLECTION_ENDPOINT, created.id),
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let body: Vec<serde_json::Value> = test::read_body_json(resp).await;
        assert_eq!(body.len(), 2, "expected two versions");
        assert_eq!(body[0]["op"], "U");
        assert_eq!(body[0]["description"], "v2");
        assert_eq!(body[1]["op"], "I");
        assert!(
            body[0].get("actor_username").is_some(),
            "actor_username key present"
        );

        // as-of just after the insert (before the update) -> v1.
        let v1_from = body[1]["valid_from"].as_str().unwrap().to_string();
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{}/{}/history/as-of?at={}",
                COLLECTION_ENDPOINT, created.id, v1_from
            ),
        )
        .await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;
        let snap: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(snap["description"], created.description);

        collection_fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_collection_history_404_for_missing(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{}/2147483647/history", COLLECTION_ENDPOINT),
        )
        .await;
        assert_response_status(resp, http::StatusCode::NOT_FOUND).await;
    }
}
