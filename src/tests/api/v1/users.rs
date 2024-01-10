#[cfg(test)]
mod tests {
    use crate::config::get_config;
    use crate::db::connection::init_pool;
    use crate::models::user::User;
    use actix_web::{http::StatusCode, test, web, App};

    use crate::tests::{create_test_admin, create_test_user};

    use crate::api;

    const USERS_ENDPOINT: &str = "/api/v1/iam/users";

    fn assert_users_are_equal(user1: &User, user2: &User) {
        assert_eq!(user1.id, user2.id);
        assert_eq!(user1.email, user2.email);
        assert_eq!(user1.username, user2.username);
    }

    async fn check_show_user(
        pool: &crate::db::connection::DbPool,
        target: &User,
        requester: &User,
        expected_status: StatusCode,
    ) {
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(pool.clone()))
                .configure(api::config),
        )
        .await;

        let token = requester.add_token(&pool).unwrap().get_token();

        let resp = test::TestRequest::get()
            .uri(&format!("{}/{}", USERS_ENDPOINT, &target.id))
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .send_request(&app)
            .await;

        assert_eq!(
            resp.status(),
            expected_status,
            "User '{}' tried to access user '{}'. Expected status code {:?} instead of {:?}",
            &requester.username,
            &target.username,
            resp.status(),
            expected_status
        );

        if expected_status == StatusCode::OK {
            let returned_user: User = test::read_body_json(resp).await;
            assert_users_are_equal(&target, &returned_user);
        }
    }

    async fn check_show_user_tokens(
        pool: &crate::db::connection::DbPool,
        target: &User,
        requester: &User,
        expected_status: StatusCode,
    ) {
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(pool.clone()))
                .configure(api::config),
        )
        .await;

        let token = requester.add_token(&pool).unwrap().get_token();

        let resp = test::TestRequest::get()
            .uri(&format!("{}/{}/tokens", USERS_ENDPOINT, &target.id))
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .send_request(&app)
            .await;

        assert_eq!(
            resp.status(),
            expected_status,
            "User '{}' tried to access tokens for user '{}'. Expected status code {:?} instead of {:?}",
            &requester.username,
            &target.username,
            resp.status(),
            expected_status
        );
    }

    #[actix_web::test]
    async fn test_show_user() {
        let config = get_config().await;
        let pool = init_pool(&config.database_url, config.db_pool_size);

        let test_user = create_test_user(&pool);
        let test_admin_user = create_test_admin(&pool);

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(pool.clone()))
                .configure(api::config),
        )
        .await;

        // The format here is (target, requester, expected_status).
        check_show_user(&pool, &test_user, &test_user, StatusCode::OK).await;
        check_show_user(&pool, &test_admin_user, &test_user, StatusCode::OK).await;
        check_show_user(&pool, &test_user, &test_admin_user, StatusCode::OK).await;

        let resp = test::TestRequest::get()
            .uri(&format!("{}/{}", USERS_ENDPOINT, &test_user.id))
            .send_request(&app)
            .await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        // Tokens are admin_or_self. Note that the format is (target, requester, expected_status).
        check_show_user_tokens(&pool, &test_user, &test_user, StatusCode::OK).await;
        check_show_user_tokens(&pool, &test_admin_user, &test_user, StatusCode::FORBIDDEN).await;
        check_show_user_tokens(&pool, &test_user, &test_admin_user, StatusCode::OK).await;

        for user in vec![&test_user, &test_admin_user] {
            let resp = test::TestRequest::get()
                .uri(&format!("{}/{}/tokens", USERS_ENDPOINT, &user.id))
                .send_request(&app)
                .await;

            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        }
    }
}
