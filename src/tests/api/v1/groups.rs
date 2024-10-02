#[cfg(test)]
mod tests {
    use crate::models::group::{Group, NewGroup, UpdateGroup};
    use crate::models::user::User;
    use actix_web::{http::StatusCode, test};

    use crate::tests::api_operations::{delete_request, get_request, patch_request, post_request};
    use crate::tests::asserts::assert_response_status;
    use crate::tests::{
        create_test_admin, create_test_group, create_test_user, setup_pool_and_tokens,
    };

    const GROUPS_ENDPOINT: &str = "/api/v1/iam/groups";

    async fn check_show_group(target: &Group, requester: &User, expected_status: StatusCode) {
        let (pool, _, _) = setup_pool_and_tokens().await;
        let token = requester.create_token(&pool).await.unwrap().get_token();

        let resp = get_request(
            &pool,
            &token,
            &format!("{}/{}", GROUPS_ENDPOINT, &target.id),
        )
        .await;
        let resp = assert_response_status(resp, expected_status).await;

        if resp.status() == expected_status {
            let returned_group: Group = test::read_body_json(resp).await;
            assert_eq!(target, &returned_group);
        }
    }

    #[actix_web::test]
    async fn test_show_group() {
        let (pool, _, _) = setup_pool_and_tokens().await;
        let test_user = create_test_user(&pool).await;
        let test_admin = create_test_admin(&pool).await;

        let test_group = create_test_group(&pool).await;
        test_group.add_member(&pool, &test_user).await.unwrap();

        let test_admin_group = create_test_group(&pool).await;

        // The format here is (target, requester, expected_status).
        // Check that anyone can see every group.
        check_show_group(&test_group, &test_user, StatusCode::OK).await;
        check_show_group(&test_admin_group, &test_user, StatusCode::OK).await;
        check_show_group(&test_admin_group, &test_admin, StatusCode::OK).await;
        check_show_group(&test_group, &test_admin, StatusCode::OK).await;
    }

    #[actix_web::test]
    async fn test_create_and_delete_group() {
        let (pool, admin_token, normal_token) = setup_pool_and_tokens().await;

        let new_group = NewGroup {
            groupname: "test_create_group_endpoint".to_string(),
            description: Some("Test group".to_string()),
        };

        // Just checking that only admins can create groups...
        let resp = post_request(&pool, &normal_token, GROUPS_ENDPOINT, &new_group).await;
        let _ = assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let resp = post_request(&pool, &admin_token, GROUPS_ENDPOINT, &new_group).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;

        let headers = resp.headers().clone();
        let created_group_url = headers.get("Location").unwrap().to_str().unwrap();
        let created_group_from_create: Group = test::read_body_json(resp).await;
        let resp = get_request(&pool, &admin_token, created_group_url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let created_group: Group = test::read_body_json(resp).await;

        // Validate that the location is what we expect
        assert_eq!(
            created_group_url,
            &format!("{}/{}", GROUPS_ENDPOINT, created_group.id)
        );

        assert_eq!(created_group, created_group_from_create);
        assert_eq!(new_group.groupname, created_group_from_create.groupname);
        assert_eq!(new_group.description, Some(created_group.description));

        // And only admins can delete groups...
        let resp = delete_request(&pool, &normal_token, created_group_url).await;
        let _ = assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let resp = delete_request(&pool, &admin_token, created_group_url).await;
        let _ = assert_response_status(resp, StatusCode::NO_CONTENT).await;

        let resp = get_request(&pool, &admin_token, created_group_url).await;
        let _ = assert_response_status(resp, StatusCode::NOT_FOUND).await;
    }

    #[actix_web::test]
    async fn test_patch_group() {
        let (pool, admin_token, group_token) = setup_pool_and_tokens().await;

        // Test setting a new password
        let updated_group = UpdateGroup {
            groupname: Some("newgroupname".to_string()),
        };

        let test_group = create_test_group(&pool).await;
        let patch_url = format!("{}/{}", GROUPS_ENDPOINT, test_group.id);

        // Only admins can patch groups...
        let resp = patch_request(&pool, &group_token, &patch_url, &updated_group).await;
        let _ = assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let resp = patch_request(&pool, &admin_token, &patch_url, &updated_group).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let patched_group: Group = test::read_body_json(resp).await;

        let resp = get_request(&pool, &admin_token, &patch_url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let refetched_group: Group = test::read_body_json(resp).await;

        assert_eq!(patched_group.groupname, updated_group.groupname.unwrap());
        assert_eq!(patched_group, refetched_group);
    }
}
