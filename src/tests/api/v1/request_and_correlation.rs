#[cfg(test)]
mod tests {
    use rstest::rstest;

    use actix_web::{
        App, HttpRequest, HttpResponse,
        http::{StatusCode, header::HeaderValue},
        test, web,
    };
    use serde_json::json;
    use tracing_subscriber::layer::SubscriberExt;

    use crate::events::RequestProvenance;
    use crate::logger::{HubuumLoggingFormat, test_support::JsonLogWriter};
    use crate::middlewares::TracingMiddleware;
    use crate::middlewares::actor_context;
    use crate::middlewares::tracing::record_principal_on_current_span;
    use crate::tests::api_operations::get_request_with_correlation;
    use crate::tests::asserts::assert_response_status;
    use crate::tests::{TestContext, test_context};

    const ENDPOINT: &str = "/api/v1/classes/";

    #[rstest]
    #[case::with_correlation_id(Some("test-correlation-id"))]
    #[case::with_empty_correlation_id(Some(""))]
    #[case::with_long_correlation_id(Some(
        "test-correlation-id-long with spaces & weird characters"
    ))]
    #[case::without_correlation_id(None)]
    #[actix_web::test]
    async fn test_with_correlation_id(
        #[case] correlation_target: Option<&str>,
        #[future(awt)] test_context: TestContext,
    ) {
        let resp = get_request_with_correlation(
            &test_context.pool,
            &test_context.admin_token,
            ENDPOINT,
            correlation_target,
        )
        .await;

        let resp = assert_response_status(resp, StatusCode::OK).await;

        match correlation_target {
            Some(correlation_id) => {
                assert_eq!(
                    resp.headers().get("x-correlation-id"),
                    Some(&HeaderValue::from_str(correlation_id).unwrap())
                );
            }
            None => {
                assert!(resp.headers().get("x-correlation-id").is_none());
            }
        }

        assert!(
            resp.headers().get("x-request-id").is_some(),
            "Expected x-request-id header to be present"
        );
    }

    #[actix_web::test]
    async fn tracing_middleware_exposes_request_provenance_to_handlers() {
        async fn handler(req: HttpRequest) -> HttpResponse {
            let provenance = RequestProvenance::from_request(&req).expect("request provenance");
            HttpResponse::Ok().json(json!({
                "request_id": provenance.request_id().to_string(),
                "correlation_id": provenance.correlation_id(),
            }))
        }

        let app = test::init_service(
            App::new()
                .wrap(TracingMiddleware::new())
                .route("/probe", web::get().to(handler)),
        )
        .await;

        let resp = test::TestRequest::get()
            .insert_header(("x-correlation-id", "context-correlation"))
            .uri("/probe")
            .send_request(&app)
            .await;
        let response_request_id = resp
            .headers()
            .get("x-request-id")
            .expect("response request id")
            .to_str()
            .expect("valid request id header")
            .to_string();
        let body: serde_json::Value = test::read_body_json(resp).await;

        assert_eq!(body["request_id"], response_request_id);
        assert_eq!(body["correlation_id"], "context-correlation");
    }

    fn capture_request_logs() -> (JsonLogWriter, tracing::dispatcher::DefaultGuard) {
        let writer = JsonLogWriter::default();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(writer.clone())
                .event_format(HubuumLoggingFormat),
        );
        let guard = tracing::subscriber::set_default(subscriber);
        (writer, guard)
    }

    async fn ok_handler() -> HttpResponse {
        HttpResponse::Ok().finish()
    }

    async fn bad_request_handler() -> HttpResponse {
        HttpResponse::BadRequest().finish()
    }

    async fn server_error_handler() -> HttpResponse {
        HttpResponse::InternalServerError().finish()
    }

    async fn principal_handler() -> HttpResponse {
        record_principal_on_current_span(77);
        HttpResponse::Ok().finish()
    }

    #[rstest]
    #[case::success("/ok", StatusCode::OK, "INFO")]
    #[case::client_error("/bad-request", StatusCode::BAD_REQUEST, "WARN")]
    #[case::server_error("/server-error", StatusCode::INTERNAL_SERVER_ERROR, "ERROR")]
    #[actix_web::test]
    async fn tracing_middleware_logs_request_completion_by_status(
        #[case] path: &str,
        #[case] expected_status: StatusCode,
        #[case] expected_severity: &str,
    ) {
        let (writer, _guard) = capture_request_logs();
        let app = test::init_service(
            App::new()
                .wrap(TracingMiddleware::new())
                .route("/ok", web::get().to(ok_handler))
                .route("/bad-request", web::get().to(bad_request_handler))
                .route("/server-error", web::get().to(server_error_handler)),
        )
        .await;

        let resp = test::TestRequest::get()
            .insert_header(("x-correlation-id", "request-log-correlation"))
            .uri(path)
            .send_request(&app)
            .await;
        assert_eq!(resp.status(), expected_status);

        let logs = writer.output();
        let event = logs
            .iter()
            .find(|event| event["message"] == "request complete")
            .expect("request completion log");
        assert_eq!(event["severity"], expected_severity);
        assert_eq!(event["method"], "GET");
        assert_eq!(event["path"], path);
        assert_eq!(event["status"], expected_status.as_u16());
        assert_eq!(event["correlation_id"], "request-log-correlation");
        assert!(event["request_id"].as_str().is_some());
        assert!(event["elapsed_ms"].as_u64().is_some());
    }

    #[actix_web::test]
    async fn tracing_middleware_includes_recorded_principal_on_request_logs() {
        let (writer, _guard) = capture_request_logs();
        let app = test::init_service(
            App::new()
                .wrap(TracingMiddleware::new())
                .route("/principal", web::get().to(principal_handler)),
        )
        .await;

        let resp = test::TestRequest::get()
            .uri("/principal")
            .send_request(&app)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);

        let logs = writer.output();
        let event = logs
            .iter()
            .find(|event| event["message"] == "request complete")
            .expect("request completion log");
        assert_eq!(event["principal"], 77);
    }

    #[actix_web::test]
    async fn production_middleware_stack_records_authenticated_principal_on_request_logs() {
        let test_context = TestContext::new().await;
        let (writer, _guard) = capture_request_logs();
        let app = test::init_service(
            App::new()
                .wrap(actix_web::middleware::from_fn(actor_context))
                .wrap(TracingMiddleware::new())
                .app_data(test_context.pool.clone())
                .route("/principal", web::get().to(ok_handler)),
        )
        .await;

        let resp = test::TestRequest::get()
            .insert_header((
                actix_web::http::header::AUTHORIZATION,
                format!("Bearer {}", test_context.admin_token),
            ))
            .uri("/principal")
            .send_request(&app)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);

        let logs = writer.output();
        let event = logs
            .iter()
            .find(|event| event["message"] == "request complete")
            .expect("request completion log");
        assert_eq!(event["principal"], test_context.admin_user.id);
    }
}
