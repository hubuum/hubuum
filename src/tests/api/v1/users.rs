#[cfg(test)]
mod tests {
    use crate::models::user::{NewUser, User};
    use actix_web::{http::StatusCode, test};

    use crate::tests::api_operations::{send_delete_request, send_get_request, send_post_request};
    use crate::tests::asserts::assert_response_status;
    use crate::tests::{create_test_admin, create_test_user, setup_pool_and_admin_user};

    const USERS_ENDPOINT: &str = "/api/v1/iam/users";

    async fn check_show_user(
        pool: &crate::db::connection::DbPool,
        target: &User,
        requester: &User,
        expected_status: StatusCode,
    ) {
        let token = requester.add_token(&pool).unwrap().get_token();

        let (pool, _) = setup_pool_and_admin_user().await;
        let resp =
            send_get_request(&pool, &token, &format!("{}/{}", USERS_ENDPOINT, &target.id)).await;
        let resp = assert_response_status(resp, expected_status).await;

        if resp.status() == expected_status {
            let returned_user: User = test::read_body_json(resp).await;
            assert_eq!(target, &returned_user);
        }
    }

    async fn check_show_user_tokens(
        pool: &crate::db::connection::DbPool,
        target: &User,
        requester: &User,
        expected_status: StatusCode,
    ) {
        let token = requester.add_token(&pool).unwrap().get_token();

        let (pool, _) = setup_pool_and_admin_user().await;
        let resp = send_get_request(
            &pool,
            &token,
            &format!("{}/{}/tokens", USERS_ENDPOINT, &target.id),
        )
        .await;
        let _ = assert_response_status(resp, expected_status).await;
    }

    #[actix_web::test]
    async fn test_show_user() {
        let (pool, _) = setup_pool_and_admin_user().await;

        let test_user = create_test_user(&pool);
        let test_admin_user = create_test_admin(&pool);

        // The format here is (target, requester, expected_status).
        check_show_user(&pool, &test_user, &test_user, StatusCode::OK).await;
        check_show_user(&pool, &test_admin_user, &test_user, StatusCode::OK).await;
        check_show_user(&pool, &test_user, &test_admin_user, StatusCode::OK).await;

        // Tokens are admin_or_self. Note that the format is (target, requester, expected_status).
        check_show_user_tokens(&pool, &test_user, &test_user, StatusCode::OK).await;
        check_show_user_tokens(&pool, &test_admin_user, &test_user, StatusCode::FORBIDDEN).await;
        check_show_user_tokens(&pool, &test_user, &test_admin_user, StatusCode::OK).await;
    }

    #[actix_web::test]
    async fn test_create_and_delete_user() {
        let (pool, admin_token) = setup_pool_and_admin_user().await;

        let normal_token = create_test_user(&pool)
            .add_token(&pool)
            .unwrap()
            .get_token();

        let new_user = NewUser {
            username: "test_create_user_endpoint".to_string(),
            password: "testpassword".to_string(),
            email: None,
        };

        // Just checking that only admins can create users...
        let resp = send_post_request(&pool, &normal_token, USERS_ENDPOINT, &new_user).await;
        let _ = assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let resp = send_post_request(&pool, &admin_token, USERS_ENDPOINT, &new_user).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;

        let created_user_url = resp.headers().get("Location").unwrap().to_str().unwrap();
        let resp = send_get_request(&pool, &admin_token, created_user_url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let created_user: User = test::read_body_json(resp).await;

        // Validate that the location is what we expect
        assert_eq!(
            created_user_url,
            &format!("{}/{}", USERS_ENDPOINT, created_user.id)
        );

        // And only admins can delete users...
        let resp = send_delete_request(&pool, &normal_token, &created_user_url).await;
        let _ = assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let resp = send_delete_request(&pool, &admin_token, &created_user_url).await;
        let _ = assert_response_status(resp, StatusCode::NO_CONTENT).await;

        let resp = send_get_request(&pool, &admin_token, created_user_url).await;
        let _ = assert_response_status(resp, StatusCode::NOT_FOUND).await;
    }
}
