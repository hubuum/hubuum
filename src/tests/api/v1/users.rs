#[cfg(test)]
mod tests {
    use crate::models::user::{NewUser, UpdateUser, User};
    use actix_web::{http::StatusCode, test};

    use crate::tests::api_operations::{delete_request, get_request, patch_request, post_request};
    use crate::tests::asserts::assert_response_status;
    use crate::tests::{create_test_admin, create_test_user, setup_pool_and_tokens};

    const USERS_ENDPOINT: &str = "/api/v1/iam/users";

    async fn check_show_user(target: &User, requester: &User, expected_status: StatusCode) {
        let (pool, _, _) = setup_pool_and_tokens().await;
        let token = requester.create_token(&pool).await.unwrap().get_token();

        let resp = get_request(&pool, &token, &format!("{}/{}", USERS_ENDPOINT, &target.id)).await;
        let resp = assert_response_status(resp, expected_status).await;

        if resp.status() == expected_status {
            let returned_user: User = test::read_body_json(resp).await;
            assert_eq!(target, &returned_user);
        }
    }

    async fn check_show_user_tokens(target: &User, requester: &User, expected_status: StatusCode) {
        let (pool, _, _) = setup_pool_and_tokens().await;
        let token = requester.create_token(&pool).await.unwrap().get_token();

        let resp = get_request(
            &pool,
            &token,
            &format!("{}/{}/tokens", USERS_ENDPOINT, &target.id),
        )
        .await;
        let _ = assert_response_status(resp, expected_status).await;
    }

    #[actix_web::test]
    async fn test_show_user() {
        let (pool, _, _) = setup_pool_and_tokens().await;
        let test_user = create_test_user(&pool).await;
        let test_admin_user = create_test_admin(&pool).await;

        // The format here is (target, requester, expected_status).
        check_show_user(&test_user, &test_user, StatusCode::OK).await;
        check_show_user(&test_admin_user, &test_user, StatusCode::OK).await;
        check_show_user(&test_user, &test_admin_user, StatusCode::OK).await;

        // Tokens are admin_or_self. Note that the format is (target, requester, expected_status).
        check_show_user_tokens(&test_user, &test_user, StatusCode::OK).await;
        check_show_user_tokens(&test_admin_user, &test_user, StatusCode::FORBIDDEN).await;
        check_show_user_tokens(&test_user, &test_admin_user, StatusCode::OK).await;
    }

    #[actix_web::test]
    async fn test_create_and_delete_user() {
        let (pool, admin_token, normal_token) = setup_pool_and_tokens().await;

        let new_user = NewUser {
            username: "test_create_user_endpoint".to_string(),
            password: "testpassword".to_string(),
            email: None,
        };

        // Just checking that only admins can create users...
        let resp = post_request(&pool, &normal_token, USERS_ENDPOINT, &new_user).await;
        let _ = assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let resp = post_request(&pool, &admin_token, USERS_ENDPOINT, &new_user).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;

        let created_user_url = resp.headers().get("Location").unwrap().to_str().unwrap();
        let resp = get_request(&pool, &admin_token, created_user_url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let created_user: User = test::read_body_json(resp).await;

        // Validate that the location is what we expect
        assert_eq!(
            created_user_url,
            &format!("{}/{}", USERS_ENDPOINT, created_user.id)
        );

        // And only admins can delete users...
        let resp = delete_request(&pool, &normal_token, &created_user_url).await;
        let _ = assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let resp = delete_request(&pool, &admin_token, &created_user_url).await;
        let _ = assert_response_status(resp, StatusCode::NO_CONTENT).await;

        let resp = get_request(&pool, &admin_token, created_user_url).await;
        let _ = assert_response_status(resp, StatusCode::NOT_FOUND).await;
    }

    #[actix_web::test]
    async fn test_patch_user() {
        let (pool, admin_token, user_token) = setup_pool_and_tokens().await;

        // Test setting a new password
        let updated_user = UpdateUser {
            username: None,
            password: Some("newpassword".to_string()),
            email: None,
        };

        let test_user = create_test_user(&pool).await;
        let patch_url = format!("{}/{}", USERS_ENDPOINT, test_user.id);

        // Only admins can patch users...
        let resp = patch_request(&pool, &user_token, &patch_url, &updated_user).await;
        let _ = assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let resp = patch_request(&pool, &admin_token, &patch_url, &updated_user).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let patched_user: User = test::read_body_json(resp).await;

        assert_eq!(patched_user.username, test_user.username);
        assert_ne!(patched_user.password, test_user.password);
        assert_eq!(patched_user.email, test_user.email);
    }
}
