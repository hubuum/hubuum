use serde::Serialize;

#[derive(Serialize)]
struct NoData {}

#[derive(PartialEq, Serialize)]
enum AccessLevel {
    Open,
    User,
    Admin,
}

#[derive(Serialize)]
enum TestDataForEndpoint {
    NoData,
    LoginUser(crate::models::user::LoginUser),
}

#[actix_web::test]
async fn test_endpoint_access() {
    use crate::config::get_config;
    use crate::db::connection::init_pool;

    use crate::models::user::LoginUser;

    use actix_web::{http::Method, test, web, App};

    let config = get_config().await;
    let pool = init_pool(&config.database_url, config.db_pool_size);

    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .configure(crate::api::config),
    )
    .await;

    let normal_user = crate::tests::create_test_user(&pool);
    let admin_user = crate::tests::create_test_admin(&pool);

    let endpoints = vec![
        ("/api/v0/auth/logout", Method::GET, AccessLevel::User, None),
        (
            "/api/v0/auth/logout_all",
            Method::GET,
            AccessLevel::User,
            None,
        ),
        (
            "/api/v0/auth/login",
            Method::POST,
            AccessLevel::Open,
            Some(TestDataForEndpoint::LoginUser(LoginUser {
                username: normal_user.username.clone(),
                password: "testpassword".to_string(),
            })),
        ),
    ];

    let access_levels = vec![AccessLevel::Open, AccessLevel::User, AccessLevel::Admin];

    for (endpoint, method, required_access, data) in endpoints {
        for access_level in &access_levels {
            let mut req = test::TestRequest::with_uri(endpoint).method(method.clone());

            // Adding data if necessary
            if let Some(TestDataForEndpoint::LoginUser(ref login_user)) = data {
                req = req.set_json(login_user);
            }

            let normal_token = normal_user.add_token(&pool).unwrap().get_token();
            let admin_token = admin_user.add_token(&pool).unwrap().get_token();

            // Adding auth token based on access level
            match access_level {
                AccessLevel::User => {
                    req = req.insert_header(("Authorization", format!("Bearer {}", normal_token)))
                }
                AccessLevel::Admin => {
                    req = req.insert_header(("Authorization", format!("Bearer {}", admin_token)))
                }
                _ => {}
            }

            let req = req.to_request();
            let resp = test::call_service(&app, req).await;

            match required_access {
                AccessLevel::Open => assert!(
                    !resp.status().is_client_error(),
                    "Open endpoint {} returned {}, expected {} for Open access ({:?})",
                    endpoint,
                    resp.status(),
                    *access_level == AccessLevel::Open,
                    test::read_body(resp).await
                ),
                AccessLevel::User => assert_eq!(
                    resp.status().is_client_error(),
                    *access_level == AccessLevel::Open,
                    "User endpoint {} returned {}, expected {} for User access ({:?})",
                    endpoint,
                    resp.status(),
                    *access_level == AccessLevel::Open,
                    test::read_body(resp).await
                ),
                AccessLevel::Admin => assert_eq!(
                    resp.status().is_client_error(),
                    *access_level != AccessLevel::Admin,
                    "Admin endpoint {} returned {}, expected {} for non-Admin access ({:?})",
                    endpoint,
                    resp.status(),
                    *access_level == AccessLevel::Open,
                    test::read_body(resp).await
                ),
            }
        }
    }
}
