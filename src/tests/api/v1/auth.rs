#[cfg(test)]
mod tests {
    use crate::api::handlers::auth::login;
    use crate::config::get_config;
    use crate::db::connection::init_pool;
    use crate::models::user::{LoginUser, NewUser};
    use crate::utilities::auth::hash_password;
    use crate::utilities::iam::add_user;
    use actix_web::{http::StatusCode, test, web, App};
    use diesel::prelude::*;

    #[actix_web::test]
    async fn test_valid_login() {
        let config = get_config();
        let pool = init_pool(&config.database_url, config.db_pool_size);
        let mut conn = pool.get().expect("Failed to get db connection");

        // Create a test user
        let hashed_password = hash_password("testpassword").unwrap();
        let new_user = NewUser {
            username: "testuser".to_string(),
            email: Some("test@foo.com".to_string()),
            password: hashed_password,
        };
        add_user(&mut conn, &new_user).expect("Failed to create test user");

        let new_user_id = users
            .filter(username.eq(&new_user.username))
            .select(id)
            .first::<i32>(&mut conn)
            .expect("Failed to get user id");

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(pool.clone()))
                .service(login),
        )
        .await;

        // Test wrong password
        let login_info = web::Form(LoginUser {
            username: "testuser".to_string(),
            password: "wrongpassword".to_string(),
        });

        // Perform login request
        let resp = test::TestRequest::post()
            .uri("/login")
            .set_form(&login_info)
            .send_request(&app)
            .await;

        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "{:?}",
            test::read_body(resp).await
        );

        let login_info_ok = web::Form(LoginUser {
            username: "testuser".to_string(),
            password: "testpassword".to_string(),
        });

        // Perform login request
        let resp = test::TestRequest::post()
            .uri("/login")
            .set_form(&login_info_ok)
            .send_request(&app)
            .await;

        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "{:?}",
            test::read_body(resp).await
        );

        // Check Content Type
        let content_type_header = resp
            .headers()
            .get("Content-Type")
            .unwrap()
            .to_str()
            .unwrap();

        assert!(
            content_type_header.contains("application/json"),
            "Content type is not JSON"
        );

        let body = test::read_body(resp).await;
        let body_json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert!(
            body_json.get("token").is_some(),
            "Response does not contain token"
        );

        let token_value = body_json
            .get("token")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();

        // Verify token in database and that it belongs to the user
        use crate::models::token::Token;
        use crate::schema::tokens::dsl::*;
        let token_exists = tokens
            .filter(token.eq(&token_value))
            .filter(user_id.eq(new_user_id))
            .first::<Token>(&mut conn)
            .is_ok();

        assert!(token_exists, "Token not found in database");

        // Cleanup
        use crate::schema::users::dsl::*;
        diesel::delete(users.find(new_user_id))
            .execute(&mut conn)
            .expect("Failed to clean up user");
    }

    #[actix_web::test]
    async fn test_invalid_login_credentials() {
        let config = get_config();
        let pool = init_pool(&config.database_url, config.db_pool_size);
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(pool.clone()))
                .service(login),
        )
        .await;

        let login_info = web::Form(LoginUser {
            username: "nosuchuser".to_string(),
            password: "nosuchpassword".to_string(),
        });

        // Perform login request
        let resp = test::TestRequest::post()
            .uri("/login")
            .set_form(&login_info)
            .send_request(&app)
            .await;

        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "{:?}",
            test::read_body(resp).await
        );
    }

    #[actix_web::test]
    async fn test_invalid_login_parameters() {
        let config = get_config();
        let pool = init_pool(&config.database_url, config.db_pool_size);

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(pool.clone()))
                .service(login),
        )
        .await;

        #[derive(Debug, serde::Deserialize, serde::Serialize)]
        struct NoPassword {
            username: String,
        }

        #[derive(Debug, serde::Deserialize, serde::Serialize)]
        struct NoUsername {
            password: String,
        }

        // Perform login request with username but no password element
        let login_info_no_password = web::Form(NoPassword {
            username: "testuser".to_string(),
        });

        let resp = test::TestRequest::post()
            .uri("/login")
            .set_form(&login_info_no_password)
            .send_request(&app)
            .await;

        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "{:?}",
            test::read_body(resp).await
        );

        let login_info_no_username = web::Form(NoUsername {
            password: "password".to_string(),
        });

        let resp = test::TestRequest::post()
            .uri("/login")
            .set_form(&login_info_no_username)
            .send_request(&app)
            .await;

        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "{:?}",
            test::read_body(resp).await
        );
    }
}
