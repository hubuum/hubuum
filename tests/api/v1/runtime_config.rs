#[cfg(test)]
mod tests {
    use actix_web::{App, http::StatusCode, test, web::Data};
    use rstest::rstest;
    use serde_json::Value;

    use crate::api;
    use crate::config::running::RunningConfig;
    use crate::test_support::integration_test_config;
    use crate::tests::{TestContext, test_context};

    async fn request(context: &TestContext, token: &str) -> actix_web::dev::ServiceResponse {
        let config = integration_test_config().unwrap();
        let app = test::init_service(
            App::new()
                .app_data(context.pool.clone())
                .app_data(crate::tests::app_context(&context.pool))
                .app_data(Data::new(RunningConfig::from(config)))
                .configure(api::config),
        )
        .await;

        test::TestRequest::get()
            .insert_header(("Authorization", format!("Bearer {token}")))
            .uri("/api/v1/admin/config")
            .send_request(&app)
            .await
            .map_into_boxed_body()
    }

    #[rstest]
    #[actix_web::test]
    async fn admin_can_inspect_redacted_running_config(#[future(awt)] test_context: TestContext) {
        let response = request(&test_context, &test_context.admin_token).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = test::read_body_json(response).await;
        let serialized = serde_json::to_string(&body).unwrap();

        assert_eq!(body["database"]["url"]["configured"], true);
        assert!(body["database"]["pool_size"].is_number());
        assert!(!serialized.contains("postgres://"));
    }

    #[rstest]
    #[actix_web::test]
    async fn non_admin_cannot_inspect_running_config(#[future(awt)] test_context: TestContext) {
        let response = request(&test_context, &test_context.normal_token).await;

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
}
