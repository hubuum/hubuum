#[cfg(test)]
mod tests {
    use crate::api;
    use crate::config::get_config;
    use crate::db::connection::init_pool;
    use crate::tests::{create_namespace, create_test_admin};
    use actix_web::{http, test, web, App};

    const NAMESPACE_ENDPOINT: &str = "/api/v1/namespaces";

    async fn setup_app_and_user() -> (web::Data<crate::db::connection::DbPool>, String) {
        let config = get_config().await;
        let pool = web::Data::new(init_pool(&config.database_url, config.db_pool_size));
        let new_user = create_test_admin(&pool.get_ref());

        let token_string = new_user
            .add_token(&pool.get_ref())
            .expect("Failed to add token to user")
            .get_token();

        (pool, token_string)
    }

    async fn send_request(
        app: &web::Data<crate::db::connection::DbPool>,
        token: &str,
    ) -> actix_web::dev::ServiceResponse {
        let mut app =
            test::init_service(App::new().app_data(app.clone()).configure(api::config)).await;

        test::TestRequest::get()
            .insert_header((http::header::AUTHORIZATION, format!("Bearer {}", token)))
            .uri(NAMESPACE_ENDPOINT)
            .send_request(&mut app)
            .await
    }

    async fn assert_response_status(
        resp: actix_web::dev::ServiceResponse,
        expected_status: http::StatusCode,
    ) -> actix_web::dev::ServiceResponse {
        assert_eq!(
            resp.status(),
            expected_status,
            "Unexpected response status: {:?}",
            test::read_body(resp).await
        );
        resp
    }

    #[actix_web::test]
    async fn test_looking_up_namespaces() {
        let (pool, token_string) = setup_app_and_user().await;

        let resp = send_request(&pool, "").await;
        let _ = assert_response_status(resp, http::StatusCode::UNAUTHORIZED).await;

        let resp = send_request(&pool, &token_string).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;

        let namespaces: Vec<crate::models::namespace::Namespace> = test::read_body_json(resp).await;
        assert_eq!(namespaces.len(), 0);

        let _ = create_namespace(&pool, "test_namespace_lookup");

        let resp = send_request(&pool, &token_string).await;
        let resp = assert_response_status(resp, http::StatusCode::OK).await;

        let namespaces: Vec<crate::models::namespace::Namespace> = test::read_body_json(resp).await;
        assert_eq!(namespaces.len(), 1);
    }
}
